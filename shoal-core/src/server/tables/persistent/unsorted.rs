//! An unsorted table in Shoal where each partition contains a single row

use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{hash_map, BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use tracing::Span;
use tracing::{event, instrument, Level};
use uuid::Uuid;

use crate::server::database::ShoalDatabase;
use crate::server::messages::{Answer, LoadedPartition, QueryMetadata, SealReply, ServerMsg};
use crate::server::replication::digest::{hash_partition, PartitionDigest, PendingDigest};
use crate::server::replication::{CommandResult, ResultKind};
use crate::server::stage_profile::{StageOp, StageStamps};
use crate::server::tables::partitions::UnsortedPartition;
use crate::server::tables::persistent::{adjust_memory_usage, open, ApplyStep, RowSink};
use crate::server::tables::persistent::{
    apply_failure, corrupt_archive, eviction_totals, settle_resident_read, PartitionLoad,
    PendingGets,
};
use crate::server::tables::storage::StorageSupport;
use crate::server::{Conf, ServerError};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::Command;
use crate::shared::queries::{UnsortedExists, UnsortedGet, UnsortedQuery, UnsortedUpdate};
use crate::shared::responses::{Response, ResponseAction, ResponseError};
use crate::shared::traits::{
    RkyvSupport, ShoalProjection, ShoalTableSupport, ShoalUnsortedTable, TableNameSupport,
};
use crate::storage::{
    link_released, FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse,
    RecoveryStats, ShouldPrune,
};
use crate::tables::partitions::{ArchivedMaybeRow, MaybeLoaded, MaybeRow, ValidatedArchive};

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(u8)]
pub enum UnsortedIntents<T: ShoalUnsortedTable + RkyvSupport> {
    Insert(T),
    Delete { partition_key: u64 },
    Update(UnsortedUpdate<T>),
}

impl<T: ShoalUnsortedTable> UnsortedIntents<T>
where
    for<'a> <<T as ShoalTableSupport>::UpdateData as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// build an insert intent
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(row: T) -> Self {
        UnsortedIntents::Insert(row)
    }

    /// build an delete intent
    ///
    /// # Arguments
    ///
    /// * `partition_key`- The partition key for the row that is being deleted
    pub fn delete(partition_key: u64) -> Self {
        UnsortedIntents::Delete { partition_key }
    }

    /// build an update intent
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply
    pub fn update(update: UnsortedUpdate<T>) -> Self {
        UnsortedIntents::Update(update)
    }
}

impl<T: ShoalUnsortedTable> RkyvSupport for UnsortedIntents<T> {}

/// A table that stores data both in memory and on disk
#[derive(Debug)]
pub struct PersistentUnsortedTable<R: ShoalUnsortedTable, S: StorageSupport, N: TableNameSupport> {
    /// The name of this table
    table_name: N,
    /// The rows in this table
    pub partitions: HashMap<u64, MaybeLoaded<UnsortedPartition<R>>>,
    /// The storage engine backing this table
    storage: S,
    /// The generation this tables intent log is currently accepting writes in
    generation: u64,
    /// The newest generation whose intent log has been compacted into an archive
    ///
    /// A partition may only be evicted once its own generation is covered by this,
    /// since anything newer exists only in an intent log that has not been applied
    /// to an archive yet. Generations start at 1 so 0 means nothing is compacted.
    flushed_generation: u64,
    /// The commits that are still pending storage confirmation
    pending: PendingResponse<R>,
    /// The responses for queries that have been flushed to disk
    flushed: Vec<(Uuid, Uuid, Span, StageStamps, Response<R>)>,
    /// The channel to send loader jobs on
    loader_tx: AsyncSender<LoaderMsg<N>>,
    /// A map of queries blocked on partitions being loaded from disk
    blocked: HashMap<u64, Vec<(QueryMetadata, UnsortedQuery<R>)>>,
    /// The partitions a replicated apply asked to have read, and is waiting on
    ///
    /// Kept apart from `blocked`, which holds queries: an apply parks nothing here, the shard
    /// loop holds its batch, and this only stops the same read being asked for twice
    /// ([F40](../../../docs/src/features/replication.md)).
    loading: HashSet<u64>,
    /// The partitions whose in-flight read was made stale by a snapshot install
    ///
    /// A read asked for before an install lands after it with the old generation's bytes; one
    /// of these is asked for again from the new map entry instead of being kept
    /// ([F43](../../../../docs/src/features/node-recovery.md)).
    stale: HashSet<u64>,
    /// The response data for gets that needed partitions to be loaded from disk
    pending_data: PendingGets,
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<GxHasher>>>>,
    /// What replaying this tables intent logs had to discard
    recovery: RecoveryStats,
}

#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<R: ShoalUnsortedTable + 'static, S: StorageSupport, N: TableNameSupport>
    PersistentUnsortedTable<R, S, N>
where
    for<'a> <R as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Fold a shard's intent logs of this table into its archives, with no table built
    ///
    /// The rehome's first step ([F47](../../../../docs/src/features/local-rehome.md)): what a
    /// start would replay and then compact, done without the replay, so a vanished executor's
    /// data is archives and a map before it is copied.
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard whose logs are folded
    /// * `shard_table_name` - The table, as the shard's messages name it
    /// * `conf` - The Shoal config
    pub async fn fold_intents(
        shard_name: &str,
        shard_table_name: <S::Database as ShoalDatabase>::TableNames,
        conf: &Conf,
    ) -> Result<u64, ServerError>
    where
        <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalTableSupport>::UpdateData,
            Strategy<Pool, rkyv::rancor::Error>,
        >,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<UnsortedPartition<R> as IntentReadSupport<R>>::Intent as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // the engine does the folding; this only names the partition and row types
        S::fold_intents::<UnsortedPartition<R>, R>(shard_name, shard_table_name, conf).await
    }

    /// Write every archived partition some shards hold of this table as one snapshot file
    ///
    /// The export of a standalone node, after a fold
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)); an ephemeral table
    /// refuses, since nothing of it is on disk. Returns the manifest of the file written.
    ///
    /// # Arguments
    ///
    /// * `shard_names` - The shards whose archives are exported
    /// * `conf` - The Shoal config
    /// * `path` - The file to write
    /// * `provenance` - Where the export is made and which file format it is written in
    /// * `group` - The group the file is written under, which a restore ignores
    /// * `schema_id` - The schema's fingerprint
    pub async fn export_archives(
        shard_names: &[String],
        conf: &Conf,
        path: &std::path::Path,
        provenance: &crate::server::replication::snapshot::SnapshotProvenance,
        group: crate::shared::identity::GroupId,
        schema_id: u64,
    ) -> Result<crate::server::replication::snapshot::SnapshotManifest, ServerError> {
        // the engine does the export; this only names the row type
        S::export_archives::<R>(shard_names, conf, path, provenance, group, schema_id).await
    }

    /// Create a persistent shoal table
    ///
    /// The double TableNames is strange but its an easy way to work around
    /// an issue where I could have cyclical generics (The table type requires
    /// itself as its own generic).
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(
        name = "PersistentTable::new",
        skip_all,
        fields(shard_name, table_name, memory_usage),
        err(Debug)
    )]
    pub async fn new(
        shard_name: &str,
        table_name: N,
        shard_table_name: <S::Database as ShoalDatabase>::TableNames,
        shard_archive_map: &FullArchiveMap<N>,
        loader_channels: &mut HashMap<
            Loaders,
            (AsyncSender<LoaderMsg<N>>, AsyncReceiver<LoaderMsg<N>>),
        >,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        memory_usage: &Arc<RefCell<usize>>,
        lru: &Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<GxHasher>>>>,
        shard_local_tx: &AsyncSender<ServerMsg<S::Database>>,
    ) -> Result<Self, ServerError>
    where
        <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalTableSupport>::UpdateData,
            Strategy<Pool, rkyv::rancor::Error>,
        >,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<UnsortedPartition<R> as IntentReadSupport<R>>::Intent as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // make sure we have a loader channel for filesystems
        let (loader_tx, _) = loader_channels
            .entry(S::loader_kind())
            .or_insert_with(|| kanal::unbounded_async());
        // build our table
        let mut table = Self {
            table_name,
            partitions: HashMap::with_capacity(1000),
            storage: S::new::<UnsortedPartition<R>, R, N>(
                shard_name,
                table_name,
                shard_table_name,
                shard_archive_map,
                conf,
                medium_priority,
                shard_local_tx,
            )
            .await?,
            // this matches the generation our storage engine starts in
            generation: 1,
            flushed_generation: 0,
            pending: PendingResponse::<R>::with_capacity(100),
            flushed: Vec::with_capacity(1000),
            loader_tx: loader_tx.clone(),
            blocked: HashMap::with_capacity(1000),
            loading: HashSet::new(),
            stale: HashSet::new(),
            pending_data: PendingGets::with_capacity(500),
            memory_usage: memory_usage.clone(),
            lru: lru.clone(),
            // nothing has been replayed yet so nothing has been discarded
            recovery: RecoveryStats::default(),
        };
        // load our intent log, keeping what replaying it had to discard
        table.recovery = table
            .storage
            .read_intents(
                conf,
                table.generation,
                &mut table.partitions,
                &mut table.memory_usage,
            )
            .await?;
        // compact our intent log
        let progress = table.storage.compact_if_needed::<R>(true).await?;
        // that rotation sealed the generation we just replayed, so our new writes
        // belong to the generation it opened rather than the one we replayed into
        table.generation = progress.generation;
        Ok(table)
    }

    /// Get the storage engine kind
    pub fn loader_kind(&self) -> Loaders {
        S::loader_kind()
    }

    /// Get what replaying this tables intent logs had to discard
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.recovery
    }

    /// The channel this table's compactor takes jobs on, if it has one
    pub fn compaction_sink(&self) -> Option<AsyncSender<crate::storage::CompactionJob>> {
        self.storage.compaction_sink()
    }

    /// Spawn the loader for this storage engine type
    pub async fn spawn_loader(
        &self,
        table_map: &FullArchiveMap<<S::Database as ShoalDatabase>::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<<S::Database as ShoalDatabase>::TableNames>>,
        shard_local_tx: &AsyncSender<ServerMsg<S::Database>>,
    ) -> Result<(), ServerError> {
        // spawn the loader for our storage engine
        self.storage
            .spawn_loader(&table_map, loader_rx, shard_local_tx)
            .await
    }

    /// Load this partition from disk if needed
    ///
    /// The generation returned alongside any unblocked queries is the generation our
    /// caller will mark this partition evictable at. That has to be the newest
    /// compacted generation and not the one we are writing in: the queries we are
    /// about to release can modify this partition, and their intents would land in
    /// an intent log that has not been compacted yet.
    ///
    /// This is where an archives bytes are validated, once, so a corrupt archive fails the
    /// read that produced it rather than the first query that happens to touch it.
    pub async fn load_partition(
        &mut self,
        loaded: LoadedPartition,
    ) -> Result<PartitionLoad<UnsortedQuery<R>>, ServerError> {
        // remember which partition this is, since the load is consumed below
        let partition_id = loaded.partition_id;
        // a read an apply asked for has landed, whatever else was waiting on it
        self.loading.remove(&partition_id);
        // and the span of the read that produced it, for the same reason
        let read_span = loaded.span.clone();
        // a read a snapshot install made stale is asked for again from the new map entry, and
        // whatever waits on it keeps waiting; a partition the install removed releases them
        // to answer without a read ([F43](../../../../docs/src/features/node-recovery.md))
        if self.stale.remove(&partition_id) {
            if self
                .storage
                .load_partition(self.table_name, partition_id, &read_span, &self.loader_tx)
                .await?
            {
                self.loading.insert(partition_id);
                return Ok(PartitionLoad::Idle);
            }
            return Ok(PartitionLoad::Failed(
                self.fail_partition(partition_id, &read_span, None)
                    .unwrap_or_default(),
            ));
        }
        // a resident copy of this partition always wins over what we just read
        match self.partitions.entry(loaded.partition_id) {
            hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                // an archive resident already is another copy of the extent we just read, from
                // a second read that raced the first, so keep it and drop ours
                //
                // this used to replace the resident copy and charge the read's size without
                // taking the replaced copy's off, so every such read inflated the counter by a
                // whole partition ([Resolved #120](../../../../docs/src/appendix/resolved/resident-copy-collision.md))
                MaybeLoaded::Accessible(resident) => {
                    // compare the two, saying so if they disagree; nothing is kept or charged
                    settle_resident_read(self.table_name, partition_id, resident, &loaded.data);
                }
                // a loaded partition is either newer than what we read or a tombstone
                // shadowing it, and in both cases our freshly read data is stale
                MaybeLoaded::Loaded { .. } => (),
            },
            // this partition does not have any already loaded data
            hash_map::Entry::Vacant(entry) => {
                // validate this archive once, here, instead of on every query that reads it
                let validated = hotpath::measure_block!("ValidatedArchive::new", {
                    ValidatedArchive::new(loaded.data)
                });
                // a corrupt archive releases the queries parked on it rather than propagating,
                // for the same reason the merge path above does
                let archive = match validated {
                    Ok(archive) => archive,
                    Err(error) => {
                        event!(
                            Level::ERROR,
                            msg = "A loaded partition failed validation",
                            table = %self.table_name,
                            partition_id,
                            error = ?error,
                        );
                        return Ok(PartitionLoad::Failed(
                            self.fail_partition(
                                partition_id,
                                &read_span,
                                Some(&corrupt_archive(self.table_name, partition_id)),
                            )
                            .unwrap_or_default(),
                        ));
                    }
                };
                // get the size of our data, only once it is known to be good
                let size = archive.len();
                // wrap our raw data so that we can access it only when needed
                let wrapped = MaybeLoaded::Accessible(archive);
                // insert our newly loaded and wrapped data
                entry.insert(wrapped);
                // increment our memory usage
                *self.memory_usage.borrow_mut() += size;
                // remove this partition from our cache until any blocked queries have completed
                self.lru
                    .borrow_mut()
                    .pop(&(self.table_name, loaded.partition_id));
            }
        }
        // get the queries that were blocked on this partition
        Ok(match self.blocked.remove(&partition_id) {
            Some(unblocked) => {
                // put every query this read released in the same trace as the read
                link_released(&unblocked, &read_span);
                PartitionLoad::Loaded(unblocked, self.flushed_generation)
            }
            None => PartitionLoad::Idle,
        })
    }

    /// Release the queries parked on a partition that could not be read
    ///
    /// A load completing is the only thing that drains `blocked`, so without this a failed
    /// read leaves every query it was carrying parked forever and their clients waiting on
    /// responses that nothing will ever produce.
    ///
    /// Each released query is marked to answer without that read, because the entry it failed
    /// on is still in the archive map for every failure except a pruned partition - so a replay
    /// that consulted disk again would park on the same failure and never terminate.
    ///
    /// # Arguments
    ///
    /// * `partition_id` - The partition that could not be read
    /// * `read` - The span of the read that gave up, which the queries it releases are linked to
    /// * `error` - What the released queries should answer with, if this was a failure at all
    #[instrument(name = "PersistentTable::fail_partition", skip(self, read, error))]
    pub fn fail_partition(
        &mut self,
        partition_id: u64,
        read: &Span,
        error: Option<&ResponseError>,
    ) -> Option<Vec<(QueryMetadata, UnsortedQuery<R>)>> {
        // a read an apply asked for gave up, whatever else was waiting on it
        self.loading.remove(&partition_id);
        // take the queries that were parked on this partition
        let mut blocked = self.blocked.remove(&partition_id)?;
        // log how many queries this failure released
        event!(
            Level::WARN,
            msg = "Releasing queries parked on a partition that could not be read",
            table = %self.table_name,
            partition_id,
            released = blocked.len(),
        );
        // mark each of them to answer without the read that just failed
        for (meta, _) in &mut blocked {
            meta.skip_disk = Some(partition_id);
            // and to answer with the failure rather than with what they can still see
            //
            // a partition that was pruned carries no failure, because a query replayed
            // against one really has found everything there is to find
            meta.failed = error.cloned();
        }
        // put every query this failure released in the same trace as the read that failed
        link_released(&blocked, read);
        Some(blocked)
    }

    /// Block a query on a partition being loaded from disk
    ///
    /// Returns true if this query was parked and false if this partition has no
    /// data on disk to wait for, in which case the caller should answer now.
    ///
    /// A read already in flight for this partition - one a parked query asked for, or one a
    /// replicated apply asked for through `request_load` - is waited on rather than asked for
    /// again, so at most one read of a partition is ever outstanding.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key of the partition this query needs
    /// * `meta` - The metadata for the query to park
    /// * `query` - The query to replay once this partition has been loaded
    #[instrument(name = "PersistentTable::block_on_load", skip_all)]
    async fn block_on_load(
        &mut self,
        partition_key: u64,
        meta: &QueryMetadata,
        query: UnsortedQuery<R>,
    ) -> bool {
        // if this partition already has blocked queries then a load is in flight
        // for it, so queue behind that load rather than requesting it again
        if let Some(entry) = self.blocked.get_mut(&partition_key) {
            // park this query behind the load we have already requested
            entry.push((meta.clone(), query));
            return true;
        }
        // a query released by a failed load answers without the read that just failed,
        // since asking for it again would only park this query on the same failure
        if meta.skip_disk == Some(partition_key) {
            return false;
        }
        // a read a replicated apply asked for is in flight too, and its landing drains
        // `blocked` like any other, so park behind it rather than asking for a second read
        // that would land on the copy the first one made resident
        // ([Resolved #121](../../../../docs/src/appendix/resolved/resident-copy-collision.md))
        if self.loading.contains(&partition_key) {
            // park this query behind the read the apply already requested
            self.blocked
                .entry(partition_key)
                .or_default()
                .push((meta.clone(), query));
            return true;
        }
        // try to load this partition from disk if it exists
        let will_load = self
            .storage
            .load_partition(
                self.table_name,
                partition_key,
                // the read this asks for belongs in the trace of the query that parks on it
                &meta.span,
                &self.loader_tx,
            )
            .await
            .unwrap();
        // if this partition has no data on disk then there is nothing to wait for
        if !will_load {
            return false;
        }
        // park this query until its partition has been loaded from disk
        self.blocked
            .entry(partition_key)
            .or_default()
            .push((meta.clone(), query));
        true
    }

    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `archived` - The archived query to execute
    #[instrument(name = "PersistentTable::handle", skip(self, query))]
    pub async fn handle<P: ShoalProjection<Row = R>>(
        &mut self,
        mut meta: QueryMetadata,
        query: UnsortedQuery<R>,
        seal: SealReply<P>,
    ) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)>
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        for<'a> <R as ShoalTableSupport>::Filters: rkyv::Serialize<
            Strategy<
                rkyv::ser::Serializer<AlignedVec, ArenaHandle<'a>, Share>,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <<R as ShoalTableSupport>::Filters as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalTableSupport>::UpdateData,
            Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    {
        // tag this record with what kind of query it came from
        //
        // this is the only layer that knows for certain which op ran, and the kinds are kept
        // apart because pooling them makes a percentile report where the boundary between
        // two distributions landed rather than anything about either one
        meta.stamps.set_op(match &query {
            UnsortedQuery::Insert { .. } => StageOp::Insert,
            UnsortedQuery::Get(_) => StageOp::Get,
            UnsortedQuery::Delete { .. } => StageOp::Delete,
            UnsortedQuery::Update(_) => StageOp::Update,
            UnsortedQuery::Exists(_) => StageOp::Exists,
        });
        // note how this tables intent log is made durable, since a table acknowledging on a
        // landed write has no fdatasync stage and a report showing one would be fiction
        meta.stamps.set_durability(self.storage.durability());
        // keep the failure this query was released with, if a read it was parked on gave up
        let failed = meta.failed.take();
        // execute the correct query type
        let answered = match query {
            // insert a row into this partition
            UnsortedQuery::Insert { row, .. } => open(self.insert(meta, row).await),
            // get a row from this partition
            //
            // the only query that can answer with rows, and so the only one that can answer
            // with rows it did not have to copy first
            UnsortedQuery::Get(get) => self.get(meta, get, seal).await,
            // delete a row from this partition
            UnsortedQuery::Delete { key } => open(self.delete(meta, key).await),
            // update a row in this partition
            UnsortedQuery::Update(update) => open(self.update(meta, update).await),
            // check if data exists in this partition
            UnsortedQuery::Exists(exists) => open(self.exists(meta, &exists).await),
        };
        // swap the answer this execution produced for the failure it was released with
        //
        // this is done in one place rather than at every site that builds a response, so the
        // `end` flag and the index stay exactly what this query would have answered with. a
        // query still parked on another partition produces nothing here and carries the
        // failure onward, which is what keeps it to exactly one response per index
        apply_failure(answered, failed)
    }

    /// Insert some data into a partition in this shards table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `row` - The row to insert
    #[instrument(name = "PersistentTable::insert", skip_all)]
    async fn insert<P>(
        &mut self,
        mut meta: QueryMetadata,
        row: R,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)>
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get our partition key
        let key = row.get_partition_key();
        // wrap our row in an insert intent
        let intent = UnsortedIntents::insert(row);
        // persist this new row to storage
        let pos = self.storage.commit(&intent).await.unwrap();
        // record that this writes synchronous work is finished
        //
        // a write returns nothing to the shard, so it stamps this itself rather than having
        // `handle_query` do it. Everything `commit` blocked on - the write behind
        // backpressure in particular - lands in this stage rather than in a durability one.
        meta.stamps.mark_exec_done();
        // extract our row from our intent
        let row = match intent {
            UnsortedIntents::Insert(row) => row,
            // SAFETY we just wrapped this in an insert intent before
            _ => unsafe { std::hint::unreachable_unchecked() },
        };
        // build a new partition for this row
        let partition = UnsortedPartition::new(key, row);
        // get the size of our new partition
        let new_size = partition.size;
        // wrap our new partition that we have loaded
        let wrapped = MaybeLoaded::Loaded {
            partition,
            generation: self.generation,
        };
        // insert our row and get the change in memory usage
        let size_diff = match self.partitions.insert(key, wrapped) {
            // we had an old row calculate the size diff
            Some(old_row) => new_size.cast_signed() - old_row.size().cast_signed(),
            None => new_size.cast_signed(),
        };
        // add this action to our pending queue
        self.pending.add(meta, pos, ResponseAction::Insert(true));
        // do a saturating add on our memory usage
        let new_size = self.memory_usage.borrow().saturating_add_signed(size_diff);
        // adjust our total shards memory usage
        *self.memory_usage.borrow_mut() = new_size;
        // remove this partition from our lru cache as its no longer evictable
        self.lru.borrow_mut().pop(&(self.table_name, key));
        // An insert never returns anything immediately
        None
    }

    /// Get some rows from some partitions
    ///
    /// The rows come back in the order this get named its partitions, which a partition read
    /// back from disk would otherwise break by answering after the ones already in memory.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `get` - The get parameters to use
    #[instrument(name = "PersistentTable::get", skip_all)]
    async fn get<P: ShoalProjection<Row = R>>(
        &mut self,
        meta: QueryMetadata,
        get: UnsortedGet<R>,
        seal: SealReply<P>,
    ) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)> {
        // a get this shard can answer out of the rows it is already holding is serialized where
        // they lie, rather than copied into a response and serialized from that
        if self.can_answer_in_place(&meta, &get.partition_keys) {
            return Some(self.get_sealed::<P>(meta, &get, seal));
        }
        // pick this get up where its last execution left off, or start it fresh
        let mut pending =
            self.pending_data
                .resume::<P>(&(meta.id, meta.index), &get.partition_keys, get.limit);
        // check each of the partition keys this execution was handed
        for partition_key in &get.partition_keys {
            // find where this partitions row belongs in the answer
            let Some(rank) = pending.rank(*partition_key) else {
                // we have already read this partition, so it has nothing left to give
                continue;
            };
            // once the partitions named before this one hold every row this get asked for,
            // nothing this one holds can reach the answer, so it is not worth reading at all
            if pending.filled_before(rank) {
                pending.fill(rank, Vec::new());
                continue;
            }
            // try to get the partition for this key
            let mut rows = RowSink::default();
            match self.partitions.get(partition_key) {
                // this partition is loaded into memory
                Some(partition) => {
                    // get this partitions data
                    if partition.get(&get, &mut rows) {
                        // mark this partition as recently used in our lru cache
                        self.lru
                            .borrow_mut()
                            .promote(&(self.table_name, *partition_key));
                    }
                    // this get has to outlive the scan that found these rows, so they are taken
                    // as owned rows here rather than pointed at
                    pending.fill(rank, rows.into_owned());
                }
                // this partition isn't loaded so lets try and load it from disk
                None => {
                    // build a query for just this blocked partition
                    let blocked_get = UnsortedQuery::Get(get.to_blocked(*partition_key));
                    // block this query if this partition has data on disk to load
                    if !self.block_on_load(*partition_key, &meta, blocked_get).await {
                        // the requested partition doesn't exist so it has no row to give
                        pending.fill(rank, rows.into_owned());
                    }
                }
            }
        }
        // hold this get until every partition it named has been read
        if pending.is_pending() {
            // remember what we have found so far for the replay to carry on from
            self.pending_data.park((meta.id, meta.index), pending);
            // we have blocked partitions so return None
            return None;
        }
        // fold our slots into the rows this get answers with, keeping which partition each
        // run came from so the gather never has to ask a row where it belongs
        let data = pending.finish();
        // add this data to our response
        let action = if data.is_empty() {
            // this query did not find data
            ResponseAction::Get(None)
        } else {
            // this query found data
            ResponseAction::Get(Some(data))
        };
        // cast this action to a response
        let response = Response {
            id: meta.id,
            index: meta.index,
            data: action,
            end: meta.end,
        };
        Some((meta.client, meta.id, meta.stamps, Answer::Open(response)))
    }

    /// Whether this get can be answered out of the rows this shard is already holding
    ///
    /// Three things stop it, and each of them for the same reason: the rows would have to
    /// outlive the scan that found them.
    ///
    /// * a **share of a split get** travels to the shard collecting it, to be merged there;
    /// * a **parked get** is picked back up on a later execution, carrying what it already found;
    /// * a get naming a partition that **is not resident** is about to park on reading it.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the get being executed
    /// * `keys` - The partition keys this get named
    fn can_answer_in_place(&self, meta: &QueryMetadata, keys: &[u64]) -> bool {
        // a share of a split get is owed to another shard rather than to a client
        if meta.gather.is_some() {
            return false;
        }
        // a get that has already parked holds rows from an execution that has ended
        if self.pending_data.is_parked(&(meta.id, meta.index)) {
            return false;
        }
        // and every partition it names has to be one we can read without going to disk
        keys.iter().all(|key| self.partitions.contains_key(key))
    }

    /// Answer a get out of the rows this shard is already holding
    ///
    /// The rows never become owned values: they are pointed at where the partitions hold them,
    /// and the reply is serialized from those pointers while the scan's borrow is still alive.
    /// That is [O2](../../../docs/src/appendix/optimizations.md)'s resident half — a row a get
    /// returns is copied once, into the buffer that goes to the client, rather than once into a
    /// response and again into that buffer.
    ///
    /// Only [`Self::can_answer_in_place`] may send a get here, and it is the reason nothing in
    /// this function can park, block or take `&mut self`.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the get being answered
    /// * `get` - The get to answer
    /// * `seal` - How to serialize the reply into the response kind this table answers in
    fn get_sealed<P: ShoalProjection<Row = R>>(
        &self,
        mut meta: QueryMetadata,
        get: &UnsortedGet<R>,
        seal: SealReply<P>,
    ) -> (Uuid, Uuid, StageStamps, Answer<Response<P>>) {
        // point at the rows each named partition holds, in the order they were named
        let mut sink = RowSink::default();
        for partition_key in &get.partition_keys {
            // once we hold every row this get asked for, nothing later can reach the answer
            if get.limit_reached(sink.len()) {
                break;
            }
            // a partition we do not hold cannot be here - can_answer_in_place checked that
            if let Some(partition) = self.partitions.get(partition_key) {
                // point at this partitions row, if it has one this get wants
                if partition.get(get, &mut sink) {
                    // mark this partition as recently used in our lru cache
                    self.lru
                        .borrow_mut()
                        .promote(&(self.table_name, *partition_key));
                }
            }
            // close this partitions run, whether or not it gave anything
            sink.close_group(*partition_key);
        }
        // build the answer out of what we found, still where we found it
        let mut rows = sink.rows();
        if let Some(limit) = get.limit {
            rows.truncate(limit);
        }
        let action = if rows.is_empty() {
            ResponseAction::Get(None)
        } else {
            ResponseAction::Get(Some(rows))
        };
        // this get's synchronous work ends here, before the serialize rather than after it
        meta.stamps.mark_exec_done();
        // serialize the reply while the rows are still where this scan found them
        let sealed = seal(Response {
            id: meta.id,
            index: meta.index,
            data: action,
            end: meta.end,
        });
        // record what serializing this response cost, the same stage `Shard::reply` stamps
        meta.stamps.mark_replied();
        // a reply that cannot be serialized is answered as a failure rather than dropped
        let answer = match sealed {
            Ok(bytes) => Answer::Sealed(bytes),
            Err(error) => Answer::Open(Response {
                id: meta.id,
                index: meta.index,
                data: ResponseAction::Error(ResponseError::new(
                    ErrorCode::Internal,
                    format!("a response could not be serialized: {error}"),
                )),
                end: meta.end,
            }),
        };
        (meta.client, meta.id, meta.stamps, answer)
    }

    /// Check if data exists in this partition
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this exists query
    /// * `exists_query` - The exists parameters to use
    #[instrument(name = "PersistentTable::exists", skip_all)]
    async fn exists<P>(
        &mut self,
        meta: QueryMetadata,
        exists_query: &UnsortedExists<R>,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // try to get the partition for this key
        match self.partitions.get(&exists_query.partition_key) {
            // this partition is loaded into memory
            Some(partition) => {
                // check if this partition is deserialized or accessible
                let exists = match partition {
                    // this partition is deserialized
                    MaybeLoaded::Loaded { partition, .. } => match &partition.row {
                        // this partitions row still exists so check any filters
                        MaybeRow::Row(row) => match &exists_query.filters {
                            // return true if this partition matches our filter
                            Some(filters) => R::is_filtered(filters, row),
                            // no filters, data exists
                            None => true,
                        },
                        // this partitions row has been deleted
                        MaybeRow::Tombstone => false,
                    },
                    MaybeLoaded::Accessible(read) => {
                        // access our data
                        let access = read.archived();
                        // check if this archived row still exists
                        match &access.row {
                            // this partitions row still exists so check any filters
                            ArchivedMaybeRow::Row(row) => match &exists_query.filters {
                                // return true if this partition matches our filter
                                Some(filters) => R::is_filtered_archived(filters, row),
                                // no filters, data exists
                                None => true,
                            },
                            // this partitions row has been deleted
                            ArchivedMaybeRow::Tombstone => false,
                        }
                    }
                };
                // mark this partition as recently used in our lru cache
                self.lru
                    .borrow_mut()
                    .promote(&(self.table_name, exists_query.partition_key));
                // build the response
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: ResponseAction::Exists(exists),
                    end: meta.end,
                };
                Some((meta.client, meta.id, meta.stamps, response))
            }
            // this partition isn't loaded so lets try and load it from disk
            None => {
                // build the query to replay once this partition has been loaded
                let blocked = UnsortedQuery::Exists(exists_query.clone());
                // block this query if this partition has data on disk to load
                if self
                    .block_on_load(exists_query.partition_key, &meta, blocked)
                    .await
                {
                    // return None since we don't yet have a response for this query
                    None
                } else {
                    // the partition doesn't exist so data doesn't exist
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data: ResponseAction::Exists(false),
                        end: meta.end,
                    };
                    Some((meta.client, meta.id, meta.stamps, response))
                }
            }
        }
    }

    /// Delete a row from this table
    ///
    /// Deletes only succeed if the row exists. If the partition is not resident
    /// then it is loaded from disk first and this query is replayed once it
    /// arrives. If the partition does not exist on disk either, returns false.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this delete query
    /// * `key` - The key to the partition to dlete data from
    #[instrument(name = "PersistentTable::delete", skip_all)]
    async fn delete<P>(
        &mut self,
        mut meta: QueryMetadata,
        key: u64,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)>
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get the partition we want to delete
        match self.partitions.get_mut(&key) {
            Some(partition) => {
                // a partition that has already been deleted has nothing left to delete
                if !partition.is_tombstoned() {
                    // get the size of the partition we are about to delete
                    let old_size = partition.size();
                    // wrap our key in a delete intent
                    let intent = UnsortedIntents::<R>::delete(key);
                    // wite this delete to our intent log
                    let pos = self.storage.commit(&intent).await.unwrap();
                    // record that this writes synchronous work is finished
                    //
                    // a write returns nothing to the shard, so it stamps this itself rather than having
                    // `handle_query` do it. Everything `commit` blocked on - the write behind
                    // backpressure in particular - lands in this stage rather than in a durability one.
                    meta.stamps.mark_exec_done();
                    // replace this partition with a tombstone rather than dropping it,
                    // since a pre-delete copy may still be sitting in an archive and
                    // any later read would load it back
                    *partition = MaybeLoaded::Loaded {
                        partition: UnsortedPartition::tombstone(key),
                        generation: self.generation,
                    };
                    // get the difference in size between our tombstone and our row
                    let diff = partition.size().cast_signed() - old_size.cast_signed();
                    // add this action to our pending queue
                    self.pending.add(meta, pos, ResponseAction::Delete(true));
                    // do a saturating add on our memory usage
                    let new_size = self.memory_usage.borrow().saturating_add_signed(diff);
                    // adjust this shards total memory usage
                    *self.memory_usage.borrow_mut() = new_size;
                    // remove this partition from our lru cache as its no longer evictable
                    self.lru.borrow_mut().pop(&(self.table_name, key));
                    // wait for this delete to get flushed to disk
                    return None;
                }
                // this rows already been deleted and so can't be deleted again
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: ResponseAction::Delete(false),
                    end: meta.end,
                };
                Some((meta.client, meta.id, meta.stamps, response))
            }
            None => {
                // this partition may still be on disk so check there before
                // telling our client it doesn't exist
                if self
                    .block_on_load(key, &meta, UnsortedQuery::Delete { key })
                    .await
                {
                    // wait for this partition to be loaded and this query replayed
                    return None;
                }
                // cast this action to a response
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: ResponseAction::Delete(false),
                    end: meta.end,
                };
                // theres nothing to delete so return our response
                Some((meta.client, meta.id, meta.stamps, response))
            }
        }
    }

    /// Update a row in this table
    ///
    /// Updates only succeed if the row exists. If the partition is not resident
    /// then it is loaded from disk first and this query is replayed once it
    /// arrives. If the partition does not exist on disk either, returns false.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `update` - The update to apply to a row in this table
    #[instrument(name = "PersistentTable::update", skip_all)]
    async fn update<P>(
        &mut self,
        mut meta: QueryMetadata,
        update: UnsortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)>
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get this rows partition
        match self.partitions.get_mut(&update.partition_key) {
            Some(partition) => {
                // a partition whose row has been deleted has nothing to update
                if !partition.is_tombstoned() {
                    // get our old partition size
                    let old_size = partition.size();
                    // update this paritions data
                    if let Some(loaded) = partition.update(&update) {
                        // replace our accessible partition with our loaded one
                        *partition = MaybeLoaded::Loaded {
                            partition: loaded,
                            generation: self.generation,
                        };
                    }
                    // get our partition key so we can remove this from our lru cache later
                    let key = update.partition_key;
                    // wrap our row in an delete intent
                    let intent = UnsortedIntents::<R>::update(update);
                    // write this update to storage
                    let pos = self.storage.commit(&intent).await.unwrap();
                    // record that this writes synchronous work is finished
                    //
                    // a write returns nothing to the shard, so it stamps this itself rather than having
                    // `handle_query` do it. Everything `commit` blocked on - the write behind
                    // backpressure in particular - lands in this stage rather than in a durability one.
                    meta.stamps.mark_exec_done();
                    // we updated some data
                    let action = ResponseAction::Update(true);
                    // add this action to our pending queue
                    self.pending.add(meta, pos, action);
                    // get the difference in size
                    let diff = partition.size().cast_signed() - old_size.cast_signed();
                    // do a saturating add on our memory usage
                    let new_size = self.memory_usage.borrow().saturating_add_signed(diff);
                    // adjust our total shards memory usage
                    *self.memory_usage.borrow_mut() = new_size;
                    // remove this partition from our lru cache as its no longer evictable
                    self.lru.borrow_mut().pop(&(self.table_name, key));
                    // wait for this delete to get flushed to disk
                    return None;
                }
                // this row has been deleted so theres nothing to update
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: ResponseAction::Update(false),
                    end: meta.end,
                };
                Some((meta.client, meta.id, meta.stamps, response))
            }
            None => {
                // get this updates partition key before we hand our query off
                let partition_key = update.partition_key;
                // this partition may still be on disk so check there before
                // telling our client it doesn't exist
                if self
                    .block_on_load(partition_key, &meta, UnsortedQuery::Update(update))
                    .await
                {
                    // wait for this partition to be loaded and this query replayed
                    return None;
                }
                // we didn't find any data to update
                let action = ResponseAction::Update(false);
                // cast this action to a response
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: action,
                    end: meta.end,
                };
                // theres nothing to update so return our response
                Some((meta.client, meta.id, meta.stamps, response))
            }
        }
    }

    /// The intent a write proposes through its tablet group, serialized once
    ///
    /// A read has no intent and proposes nothing.
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    #[must_use]
    pub fn build_intent(&self, query: &UnsortedQuery<R>) -> Option<(u64, Vec<u8>)>
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalTableSupport>::UpdateData,
            Strategy<Pool, rkyv::rancor::Error>,
        >,
    {
        let (key, intent) = match query {
            UnsortedQuery::Insert { row, .. } => (
                row.get_partition_key(),
                UnsortedIntents::Insert(row.clone()),
            ),
            UnsortedQuery::Delete { key } => (*key, UnsortedIntents::<R>::delete(*key)),
            UnsortedQuery::Update(update) => (
                update.partition_key,
                UnsortedIntents::<R>::update(update.clone()),
            ),
            UnsortedQuery::Get(_) | UnsortedQuery::Exists(_) => return None,
        };
        Some((key, RkyvSupport::serialize(&intent).to_vec()))
    }

    /// Apply a committed command to this table, deriving its result from the state it finds
    ///
    /// The replicated twin of `insert`, `delete` and `update`: the same mutation of the same
    /// partition, the same `bool` computed the same way, with nothing committed to storage,
    /// since the shard's WAL already holds the command and the group already committed it
    /// ([F40](../../../docs/src/features/replication.md)). A partition a delete or an update
    /// needs from disk is asked for through [`Self::request_load`] by the caller, which applies
    /// again with `skip_disk` set once the read lands or turns out to have nothing.
    ///
    /// # Arguments
    ///
    /// * `command` - The command
    /// * `generation` - The WAL generation the command's frame is in
    /// * `skip_disk` - Whether to answer without reading, because a read was already tried
    pub fn apply(&mut self, command: &Command, generation: u64, skip_disk: bool) -> ApplyStep
    where
        for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalTableSupport>::UpdateData,
            Strategy<Pool, rkyv::rancor::Error>,
        >,
    {
        // the generation the partition is stamped with is the WAL's
        self.generation = generation;
        self.storage.observe_generation(generation);
        // an archive is accessed at its alignment, which a command's payload does not promise
        let mut aligned = AlignedVec::<16>::with_capacity(command.payload.len());
        aligned.extend_from_slice(&command.payload);
        // validated: the bytes crossed a process boundary
        let intent = match UnsortedIntents::<R>::access(&aligned) {
            Ok(intent) => intent,
            Err(error) => {
                return ApplyStep::Refused(format!(
                    "the command's payload is not an unsorted intent: {error}"
                ))
            }
        };
        match intent {
            ArchivedUnsortedIntents::Insert(archived) => {
                let row: R = match RkyvSupport::deserialize(archived) {
                    Ok(row) => row,
                    Err(error) => {
                        return ApplyStep::Refused(format!(
                            "the command's row does not decode: {error}"
                        ))
                    }
                };
                let key = row.get_partition_key();
                let partition = UnsortedPartition::new(key, row);
                let new_size = partition.size;
                let wrapped = MaybeLoaded::Loaded {
                    partition,
                    generation,
                };
                let diff = match self.partitions.insert(key, wrapped) {
                    Some(old) => new_size.cast_signed() - old.size().cast_signed(),
                    None => new_size.cast_signed(),
                };
                adjust_memory_usage(&self.memory_usage, diff);
                self.lru.borrow_mut().pop(&(self.table_name, key));
                ApplyStep::Done(CommandResult {
                    kind: ResultKind::Insert,
                    ok: true,
                })
            }
            ArchivedUnsortedIntents::Delete { partition_key } => {
                let key = partition_key.to_native();
                let deleted = match self.partitions.get_mut(&key) {
                    Some(partition) => {
                        if partition.is_tombstoned() {
                            false
                        } else {
                            let before = partition.size();
                            *partition = MaybeLoaded::Loaded {
                                partition: UnsortedPartition::tombstone(key),
                                generation,
                            };
                            adjust_memory_usage(
                                &self.memory_usage,
                                partition.size().cast_signed() - before.cast_signed(),
                            );
                            true
                        }
                    }
                    None if !skip_disk => return ApplyStep::NeedsLoad(key),
                    None => false,
                };
                if deleted {
                    self.lru.borrow_mut().pop(&(self.table_name, key));
                }
                ApplyStep::Done(CommandResult {
                    kind: ResultKind::Delete,
                    ok: deleted,
                })
            }
            ArchivedUnsortedIntents::Update(archived) => {
                let update =
                    match rkyv::deserialize::<UnsortedUpdate<R>, rkyv::rancor::Error>(archived) {
                        Ok(update) => update,
                        Err(error) => {
                            return ApplyStep::Refused(format!(
                                "the command's update does not decode: {error}"
                            ))
                        }
                    };
                let key = update.partition_key;
                let updated = match self.partitions.get_mut(&key) {
                    Some(partition) => {
                        if partition.is_tombstoned() {
                            false
                        } else {
                            let before = partition.size();
                            if let Some(loaded) = partition.update(&update) {
                                *partition = MaybeLoaded::Loaded {
                                    partition: loaded,
                                    generation,
                                };
                            } else if let MaybeLoaded::Loaded {
                                generation: stamped,
                                ..
                            } = partition
                            {
                                *stamped = generation;
                            }
                            adjust_memory_usage(
                                &self.memory_usage,
                                partition.size().cast_signed() - before.cast_signed(),
                            );
                            true
                        }
                    }
                    None if !skip_disk => return ApplyStep::NeedsLoad(key),
                    None => false,
                };
                if updated {
                    self.lru.borrow_mut().pop(&(self.table_name, key));
                }
                ApplyStep::Done(CommandResult {
                    kind: ResultKind::Update,
                    ok: updated,
                })
            }
        }
    }

    /// Ask for a partition a replicated apply needs, saying whether a read is coming
    ///
    /// Returns true if the partition is on disk and a read was requested, or is already in
    /// flight; false if storage has nothing for it.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition
    /// * `span` - The span the read hangs off
    pub async fn request_load(
        &mut self,
        partition_key: u64,
        span: &Span,
    ) -> Result<bool, ServerError> {
        // a read already asked for, by an apply or by a query, is one to wait on
        if self.loading.contains(&partition_key) || self.blocked.contains_key(&partition_key) {
            return Ok(true);
        }
        let will_load = self
            .storage
            .load_partition(self.table_name, partition_key, span, &self.loader_tx)
            .await?;
        if will_load {
            self.loading.insert(partition_key);
        }
        Ok(will_load)
    }

    /// Hash the applied state, for a fixture comparing replicas at a common boundary
    ///
    /// Every live row hashed as its partition key and its archived bytes, and the hashes
    /// summed, so the digest is of the set of rows and not of their order or of which shard
    /// holds them: a table dealt over two executors and the same rows on one digest the same,
    /// which is what an import is judged by
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)). A resident copy and one
    /// still in its archive hash the same. A partition that is not resident is read from its
    /// archive, so a replica that holds nothing in memory - one just restarted, or one that
    /// just installed a snapshot - hashes the same state as one that applied every write
    /// itself ([F43](../../../../docs/src/features/node-recovery.md)); a resident copy shadows
    /// the archived one, since it is the newer of the two. Returns how many rows there are and
    /// the hash.
    ///
    /// # Errors
    ///
    /// Fails if an archived partition cannot be read.
    pub async fn digest(&self) -> Result<(u64, u64), ServerError> {
        // every key, resident or archived, in one order on every replica
        let mut keys: Vec<u64> = self.partitions.keys().copied().collect();
        keys.extend(self.storage.archived_keys());
        keys.sort_unstable();
        keys.dedup();
        let mut rows = 0u64;
        let mut acc = 0u64;
        for key in keys {
            // a resident row as it is, an archived one read back
            let bytes = match self.partitions.get(&key) {
                Some(MaybeLoaded::Loaded { partition, .. }) => match &partition.row {
                    MaybeRow::Row(row) => RkyvSupport::serialize(row),
                    MaybeRow::Tombstone => continue,
                },
                Some(MaybeLoaded::Accessible(read)) => match &read.archived().row {
                    ArchivedMaybeRow::Row(row) => match <R as RkyvSupport>::deserialize(row) {
                        Ok(row) => RkyvSupport::serialize(&row),
                        Err(_) => continue,
                    },
                    ArchivedMaybeRow::Tombstone => continue,
                },
                // not resident: the archive's copy is the state
                None => {
                    let Some(read) = self.storage.load_partition_direct(key).await? else {
                        continue;
                    };
                    let archived = <UnsortedPartition<R> as RkyvSupport>::access(&read)?;
                    match &archived.row {
                        ArchivedMaybeRow::Row(row) => match <R as RkyvSupport>::deserialize(row) {
                            Ok(row) => RkyvSupport::serialize(&row),
                            Err(_) => continue,
                        },
                        ArchivedMaybeRow::Tombstone => continue,
                    }
                }
            };
            rows += 1;
            // the row's own hash, summed: the order and the shard are not in it
            let mut fold = Vec::with_capacity(8 + bytes.len());
            fold.extend_from_slice(&key.to_le_bytes());
            fold.extend_from_slice(&bytes);
            acc = acc.wrapping_add(gxhash::gxhash64(&fold, 0));
        }
        Ok((rows, acc))
    }

    /// Take a canonical cut of some tablets: hash what is resident, collect what is archived
    ///
    /// The loop's half of a scrub ([F44](../../../../docs/src/features/repair.md)): every
    /// resident partition of the tablets is hashed here, as its key, its row count and its row
    /// re-serialized, and every archived partition that is not resident is collected with a
    /// handle to read it by. A resident copy shadows the archived one, since it is the newer.
    /// Unlike [`Self::digest`] this reads nothing from disk on the loop and never sees archive
    /// bytes as such, so two replicas with the same rows agree however their archives lie.
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    ///
    /// # Errors
    ///
    /// Fails if an archive a collected record is in cannot be opened.
    pub async fn canonical_cut(&self, tablets: &[u16]) -> Result<PendingDigest, ServerError> {
        let mut resident_keys = HashSet::new();
        let mut resident = BTreeMap::new();
        // every resident partition of the tablets, hashed as the row it holds
        for (key, entry) in &self.partitions {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            if !tablets.contains(&tablet) {
                continue;
            }
            // a resident copy is the state whatever the archive holds
            resident_keys.insert(*key);
            let bytes = match entry {
                MaybeLoaded::Loaded { partition, .. } => match &partition.row {
                    MaybeRow::Row(row) => Some(RkyvSupport::serialize(row)),
                    MaybeRow::Tombstone => None,
                },
                MaybeLoaded::Accessible(read) => match &read.archived().row {
                    ArchivedMaybeRow::Row(row) => <R as RkyvSupport>::deserialize(row)
                        .ok()
                        .map(|row| RkyvSupport::serialize(&row)),
                    ArchivedMaybeRow::Tombstone => None,
                },
            };
            if let Some(digest) = hash_partition(*key, bytes.iter().map(|bytes| bytes.as_slice())) {
                resident.insert(*key, digest);
            }
        }
        // what is archived and not resident, with the handles to read it by
        let archived = self.storage.archived_cut(tablets, &resident_keys).await?;
        Ok(PendingDigest {
            resident,
            archived,
            hasher: Self::hash_archived,
        })
    }

    /// Hash one archived partition's bytes canonically
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `bytes` - The record's payload, verified
    fn hash_archived(key: u64, bytes: &[u8]) -> Result<Option<PartitionDigest>, ServerError> {
        // the record is read back as the row it holds and re-serialized, never hashed as it lies
        let archived = <UnsortedPartition<R> as RkyvSupport>::access(bytes)?;
        let row = match &archived.row {
            ArchivedMaybeRow::Row(row) => Some(RkyvSupport::serialize(
                &<R as RkyvSupport>::deserialize(row)?,
            )),
            ArchivedMaybeRow::Tombstone => None,
        };
        Ok(hash_partition(
            key,
            row.iter().map(|bytes| bytes.as_slice()),
        ))
    }

    /// Every resident partition of some tablets, as its key and archived bytes
    ///
    /// What a volatile group's snapshot is cut from
    /// ([F43](../../../../docs/src/features/node-recovery.md)): a tombstone is a deleted row
    /// and is not there, and an archived copy is read back as the row it holds.
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    #[must_use]
    pub fn snapshot_partitions(&self, tablets: &[u16]) -> Vec<(u64, Vec<u8>)> {
        let mut records = Vec::new();
        for (key, entry) in &self.partitions {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            if !tablets.contains(&tablet) {
                continue;
            }
            let bytes = match entry {
                MaybeLoaded::Loaded { partition, .. } => match &partition.row {
                    MaybeRow::Row(_) => {
                        rkyv::to_bytes::<rkyv::rancor::Error>(partition).map(|bytes| bytes.to_vec())
                    }
                    MaybeRow::Tombstone => continue,
                },
                MaybeLoaded::Accessible(read) => match &read.archived().row {
                    ArchivedMaybeRow::Row(_) => Ok(read.as_bytes().to_vec()),
                    ArchivedMaybeRow::Tombstone => continue,
                },
            };
            match bytes {
                Ok(bytes) => records.push((*key, bytes)),
                Err(error) => {
                    event!(Level::WARN, msg = "a partition could not be serialized for a snapshot", key, %error)
                }
            }
        }
        records.sort_by_key(|(key, _)| *key);
        records
    }

    /// Drop every resident partition of some tablets, whatever generation it is stamped with
    ///
    /// What a snapshot install ends with on a persistent table: the archives now hold the
    /// installed generation, and every resident copy of the covered tablets is the old one,
    /// so all of them go and the next read of any of them comes from the archive. A read in
    /// flight for one of them is marked stale, so it is asked for again when it lands rather
    /// than kept ([F43](../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    pub fn evict_tablets(&mut self, tablets: &[u16]) {
        let owned = |key: u64| {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(key) as u16;
            tablets.contains(&tablet)
        };
        let victims: Vec<u64> = self
            .partitions
            .keys()
            .copied()
            .filter(|key| owned(*key))
            .collect();
        for victim in &victims {
            if let Some(partition) = self.partitions.remove(victim) {
                let size = partition.size();
                let decreased = self.memory_usage.borrow().saturating_sub(size);
                *self.memory_usage.borrow_mut() = decreased;
                self.lru.borrow_mut().pop(&(self.table_name, *victim));
            }
        }
        // a read in flight for a covered partition would land with the old generation
        let in_flight: Vec<u64> = self
            .loading
            .iter()
            .chain(self.blocked.keys())
            .copied()
            .filter(|key| owned(*key))
            .collect();
        self.stale.extend(in_flight);
        event!(
            Level::INFO,
            msg = "evicted the resident partitions of installed tablets",
            evicted = victims.len(),
            stale = self.stale.len()
        );
    }

    /// Replace every resident partition of some tablets with a snapshot's records
    ///
    /// What a snapshot install is on an ephemeral table, which has no archive: the covered
    /// tablets' partitions are dropped, and each record is put in their place as a loaded
    /// partition of the current generation ([F43](../../../../docs/src/features/node-recovery.md)).
    /// A record that is not a partition of this table's row type is refused whole.
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    /// * `records` - The partitions, as their keys and archived bytes
    ///
    /// # Errors
    ///
    /// Fails if a record does not validate as a partition, before anything is replaced.
    pub fn install_partitions(
        &mut self,
        tablets: &[u16],
        records: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), ServerError> {
        // every record validated and deserialized first, so a bad file changes nothing
        let mut partitions = Vec::with_capacity(records.len());
        for (key, bytes) in records {
            let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(&bytes);
            let archived = <UnsortedPartition<R> as RkyvSupport>::access(&aligned)?;
            let partition: UnsortedPartition<R> =
                <UnsortedPartition<R> as RkyvSupport>::deserialize(archived)?;
            partitions.push((key, partition));
        }
        self.evict_tablets(tablets);
        let generation = self.generation;
        for (key, partition) in partitions {
            // a record with nothing live in it is absence, not a partition
            if !matches!(partition.row, MaybeRow::Row(_)) {
                continue;
            }
            let size = crate::tables::partitions::PartitionSupport::size(&partition);
            self.partitions.insert(
                key,
                MaybeLoaded::Loaded {
                    partition,
                    generation,
                },
            );
            *self.memory_usage.borrow_mut() += size;
        }
        Ok(())
    }

    /// Mark partitions as evictable if they are no longer in the intent log
    ///
    /// The generation we are given names an intent log that has been compacted into
    /// an archive, so it also tells us how far our own data has been made durable.
    ///
    /// # Arguments
    ///
    /// * `generation` - The newest generation that has been compacted
    /// * `partitions` - The partitions to consider marking as evictable
    #[instrument(name = "PersistentTable::mark_evictable", skip(self, partitions), fields(partition_count = partitions.len()))]
    pub fn mark_evictable(&mut self, generation: u64, partitions: Vec<u64>) {
        let mut marked = 0;
        // track how far our data has been compacted
        self.flushed_generation = self.flushed_generation.max(generation);
        // check each partition that we find might be evictable now
        for partition in partitions {
            // try to get this partition
            if let Some(maybe_loaded) = self.partitions.get(&partition) {
                // check if this partition is now evictable
                if maybe_loaded.is_evictable(generation) {
                    // get this partitions size
                    let size = maybe_loaded.size();
                    // insert this partition into our lru cache
                    self.lru
                        .borrow_mut()
                        .put((self.table_name, partition), size);
                    marked += size;
                }
            }
        }
        event!(
            Level::INFO,
            marked,
            flushed_generation = self.flushed_generation
        );
    }

    /// Evict partitions from memory
    #[instrument(name = "PersistentTable::evict", skip_all, fields(victim_count = victims.len()))]
    pub fn evict(&mut self, victims: Vec<u64>) {
        // get our current memory usage
        let pre = *self.memory_usage.borrow();
        // track the total size of the partitions we actually dropped
        let mut removed = 0;
        // step over and remove all of our victim partitions
        for victim in victims {
            // remove this partition if it exists
            if let Some(partition) = self.partitions.remove(&victim) {
                // get the size this partition was accounted for at
                let size = partition.size();
                // get our new memory usage amount with this partition removed
                let decreased = self.memory_usage.borrow().saturating_sub(size);
                // update our memory usage
                *self.memory_usage.borrow_mut() = decreased;
                // track what this pass freed independently of the shards counter
                removed += size;
            }
        }
        // get our post eviction memory usage
        let post = *self.memory_usage.borrow();
        // summarize this pass without assuming our counter is consistent
        let (reclaimed, drift) = eviction_totals(pre, post, removed);
        // log the change in memory usage
        event!(
            Level::INFO,
            pre,
            post,
            removed,
            reclaimed,
            drift,
            partitions = self.partitions.len(),
            evictable = self.lru.borrow().len(),
        );
    }

    /// Flush all pending writes to disk
    pub async fn flush(&mut self) -> Result<(), ServerError> {
        self.storage.flush().await
    }

    /// Check if this tables intent log is due to be rotated
    ///
    /// Asked by the shard before it sweeps its tables, so it stays synchronous.
    pub fn compaction_due(&self) -> bool {
        self.storage.compaction_due()
    }

    /// Get all flushed response actions
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed actions to return
    pub async fn get_flushed(
        &mut self,
    ) -> Result<&mut Vec<(Uuid, Uuid, Span, StageStamps, Response<R>)>, ServerError> {
        // check if our current intent log should be compacted
        let progress = self.storage.compact_if_needed::<R>(false).await?;
        // update our current generation
        self.generation = progress.generation;
        // release the responses whose data is now durable
        if progress.rotated {
            // rotation fdatasynced everything in the old log and restarted our
            // positions at 0, so every pending response is durable and none of
            // their positions can be compared against the new files watermark
            self.pending.drain_all(&mut self.flushed);
        } else {
            // get all of the responses whose data has been flushed to disk
            self.pending.get(progress.durable_pos, &mut self.flushed);
        }
        // fill in the durability phases for everything we just released
        //
        // these are intervals of the intent log rather than properties of a query, so they
        // are looked up by the offset each response parked at. `self.flushed` is drained by
        // the shard on every sweep, so everything in it now is newly released.
        #[cfg(feature = "stage-profile")]
        for (_, _, _, stamps, _) in self.flushed.iter_mut() {
            self.storage.fill_durability(stamps);
        }
        // return a ref to our flushed responses
        Ok(&mut self.flushed)
    }

    /// Shutdown this table
    #[instrument(name = "PersistentTable::shutdown", skip_all)]
    pub async fn shutdown(self) -> Result<(), ServerError> {
        // shutdown our storage engine
        self.storage.shutdown().await
    }
}

impl<T: ShoalUnsortedTable + RkyvSupport> IntentReadSupport<T> for UnsortedPartition<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::UpdateData as Archive>::Archived:
        rkyv::Deserialize<T::UpdateData, Strategy<Pool, rkyv::rancor::Error>>,
    for<'a> <T as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <<T as ShoalTableSupport>::UpdateData as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// A tombstone: a partition whose one row was deleted
    fn erased(key: u64) -> Self {
        UnsortedPartition::tombstone(key)
    }

    /// The intent type to use
    type Intent = UnsortedIntents<T>;

    /// Name the partitions an intent needs loaded before it can be replayed
    ///
    /// # Arguments
    ///
    /// * `read` - The intent to name the partitions of
    /// * `to_load` - The set of partition keys to add to
    fn scan_keys(read: &ReadResult, to_load: &mut HashSet<u64>) -> Result<(), ServerError> {
        // access our data
        let intent = UnsortedIntents::<T>::access(read)?;
        // only an update needs its base row to already be there
        match intent {
            // we don't need to load inserts or deletes to replay its intent
            ArchivedUnsortedIntents::Insert(_) | ArchivedUnsortedIntents::Delete { .. } => (),
            // we need the data loaded in order to update it
            ArchivedUnsortedIntents::Update(update) => {
                to_load.insert(update.partition_key.to_native());
            }
        }
        Ok(())
    }

    /// Load a intent from a read and insert it into our map
    ///
    /// # Arguments
    ///
    /// * `read` - The archived intent to load
    /// * `generation` - The generation to load these intents as
    /// * `partitions` - The map to load our intents into
    /// * `memory_usage` - The total memory usage of of this shard
    /// * `stats` - The counts of what this recovery has discarded
    fn replay(
        read: &ReadResult,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
        stats: &mut RecoveryStats,
    ) -> Result<(), ServerError> {
        // access our data
        let intent = UnsortedIntents::<T>::access(read)?;
        // add this intent to our btreemap
        match intent {
            ArchivedUnsortedIntents::Insert(archived) => {
                // deserialize this row
                let row: T = RkyvSupport::deserialize(archived)?;
                // get the size of our row
                let size = row.deep_size_of();
                // get the partition key for this row
                let key = row.get_partition_key();
                // build a new partition for this row
                let partition = UnsortedPartition::new(key, row);
                // insert this new partition
                match partitions.insert(
                    key,
                    MaybeLoaded::Loaded {
                        partition,
                        generation,
                    },
                ) {
                    // if we had an existing partition then get the difference in size
                    Some(old) => {
                        // calculate the change in size
                        let size_diff = size.cast_signed() - old.size().cast_signed();
                        // do a saturating add on our memory usage
                        let new_size = memory_usage.borrow().saturating_add_signed(size_diff);
                        // adjust our memory usage correctly
                        *memory_usage.borrow_mut() = new_size;
                    }
                    // we did not have an existing partition so just increment our sizes
                    None => *memory_usage.borrow_mut() += size,
                }
            }
            ArchivedUnsortedIntents::Delete { partition_key } => {
                // convert our partition key to its native endianess
                let partition_key = partition_key.to_native();
                // build the tombstone for this deleted partition, since the pre-delete
                // copy may still be in an archive that has not been compacted yet
                let tombstone = UnsortedPartition::tombstone(partition_key);
                // get the size of our tombstone
                let size = tombstone.size;
                // wrap our tombstone as a loaded partition
                let wrapped = MaybeLoaded::Loaded {
                    partition: tombstone,
                    generation,
                };
                // replace this partition with its tombstone
                match partitions.insert(partition_key, wrapped) {
                    // if we had an existing partition then get the difference in size
                    Some(old) => {
                        // calculate the change in size
                        let size_diff = size.cast_signed() - old.size().cast_signed();
                        // do a saturating add on our memory usage
                        let new_size = memory_usage.borrow().saturating_add_signed(size_diff);
                        // adjust our memory usage correctly
                        *memory_usage.borrow_mut() = new_size;
                    }
                    // we did not have an existing partition so just increment our sizes
                    None => *memory_usage.borrow_mut() += size,
                }
            }
            ArchivedUnsortedIntents::Update(archived) => {
                // deserialize this row's update
                let update = rkyv::deserialize::<UnsortedUpdate<T>, rkyv::rancor::Error>(archived)?;
                // try to get the partition containing our target row
                match partitions.get_mut(&update.partition_key) {
                    // update this row
                    Some(partition) => {
                        // an update onto a deleted row is dropped, but that is the
                        // delete working rather than data going missing
                        if partition.is_tombstoned() {
                            // count this as a drop that cost us nothing
                            stats.updates_after_delete += 1;
                        }
                        // get the size of not yet updated partition
                        let old_size = partition.size();
                        // update our row in place if its loaded or by replacement if its not
                        if let Some(loaded) = partition.update(&update) {
                            // replace our old partition with its updated data
                            *partition = MaybeLoaded::Loaded {
                                partition: loaded,
                                generation,
                            };
                        }
                        // calculate the change in size
                        let size_diff = partition.size().cast_signed() - old_size.cast_signed();
                        // do a saturating add on our memory usage
                        let new_size = memory_usage.borrow().saturating_add_signed(size_diff);
                        // adjust our total memory usage based on our newly updated row
                        *memory_usage.borrow_mut() = new_size;
                    }
                    // This updates base row is neither resident nor on disk, so the
                    // insert it was built on is gone. Skip it rather than crashing a
                    // shard that would otherwise start.
                    None => {
                        tracing::warn!(
                            "Skipping update intent for missing partition {}",
                            update.partition_key
                        );
                        // this one really is data we no longer have
                        stats.orphaned_updates += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply an intent to this partition
    ///
    /// # Arguments
    ///
    /// * `loaded` - The partitions loaded from their current archives
    /// * `key` - The key of the partition to apply intents to
    /// * `intents` - The intents to apply to this partition
    /// * `stats` - The counts of what this compaction has discarded
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
        stats: &mut RecoveryStats,
    ) -> ShouldPrune {
        // start from this partitions current archive copy if it has one, since an
        // update can target a row whose insert was compacted generations ago
        let mut maybe_partition = loaded.remove(&key);
        // a delete here drops the partition outright rather than tombstoning it, so
        // without remembering that this batch is what dropped it an update that
        // correctly lost its row looks exactly like one whose insert we lost
        let mut deleted_here = false;
        // apply all of our intents to this partition
        for intent in intents {
            // apply this intent to our partition
            match intent {
                UnsortedIntents::Insert(row) => {
                    // insert a new partition
                    maybe_partition = Some(Self::new(key, row));
                    // this partition is live again, so a later miss is not a delete
                    deleted_here = false;
                }
                UnsortedIntents::Delete { .. } => {
                    maybe_partition = None;
                    // remember that this batch is what took this partition
                    deleted_here = true;
                }
                UnsortedIntents::Update(update) => {
                    // apply this update if we have a partition
                    match &mut maybe_partition {
                        Some(partition) => {
                            // a false here means this partition is a tombstone, which
                            // is the delete working rather than data going missing
                            if !partition.update(&update) {
                                // count this as a drop that cost us nothing
                                stats.updates_after_delete += 1;
                            }
                        }
                        // a delete in this batch took this partition, so dropping
                        // this update is that delete working rather than a loss
                        None if deleted_here => stats.updates_after_delete += 1,
                        // this updates base row is gone, so there is nothing to
                        // apply it to and nothing we can do but drop it
                        None => {
                            tracing::warn!("Skipping update intent for missing partition {}", key);
                            // this one really is data we no longer have
                            stats.orphaned_updates += 1;
                        }
                    }
                }
            }
        }
        // only insert this partition if we ended with live row data
        match maybe_partition {
            // a tombstone is never written to an archive, the partition is pruned instead
            Some(partition) if !partition.is_tombstoned() => {
                // insert this partition
                loaded.insert(key, partition);
                // we have a partition still so this partition should not be pruned
                ShouldPrune::No
            }
            // we do not have a partition so prune it
            _ => ShouldPrune::Yes,
        }
    }

    /// Get the partition key for a specific intent
    fn partition_key_and_intent(read: &ReadResult) -> Result<(u64, UnsortedIntents<T>), ServerError>
    where
        for<'a> ArchivedUnsortedIntents<T>: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // try to deserialize this row from our intent log
        let archived = <Self::Intent as RkyvSupport>::access(&read)?;
        // deserialize this intent
        let intent = rkyv::deserialize::<UnsortedIntents<T>, rkyv::rancor::Error>(archived)?;
        // get this intent entries partition key
        let partition_key = match &intent {
            UnsortedIntents::Insert(row) => row.get_partition_key(),
            UnsortedIntents::Delete { partition_key, .. } => *partition_key,
            UnsortedIntents::Update(update) => update.partition_key,
        };
        Ok((partition_key, intent))
    }

    /// Get the partition key for an intent that arrived as bytes, validating them first
    fn partition_key_and_intent_checked(
        bytes: &[u8],
    ) -> Result<(u64, UnsortedIntents<T>), ServerError>
    where
        for<'a> ArchivedUnsortedIntents<T>: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // an archive is accessed at its alignment, which a command's payload does not promise
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        // validated, since the bytes crossed a process boundary
        let archived = <Self::Intent as RkyvSupport>::access(&aligned)?;
        let intent = rkyv::deserialize::<UnsortedIntents<T>, rkyv::rancor::Error>(archived)?;
        let partition_key = match &intent {
            UnsortedIntents::Insert(row) => row.get_partition_key(),
            UnsortedIntents::Delete { partition_key, .. } => *partition_key,
            UnsortedIntents::Update(update) => update.partition_key,
        };
        Ok((partition_key, intent))
    }
}
