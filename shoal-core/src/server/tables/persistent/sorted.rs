//! A sorted table in  Shoal where each partition can contain multiple sorted rows

use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::{hash_map, HashSet};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::BuildHasherDefault;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::server::database::ShoalDatabase;
use crate::server::messages::{Answer, LoadedPartition, QueryMetadata, SealReply, ServerMsg};
use crate::server::replication::digest::{hash_partition, PartitionDigest, PendingDigest};
use crate::server::replication::{CommandResult, ResultKind};
use crate::server::stage_profile::{StageOp, StageStamps};
use crate::server::tables::partitions::SortedPartition;
use crate::server::tables::persistent::{
    adjust_memory_usage, apply_failure, corrupt_archive, eviction_totals, settle_resident_read,
    PartitionLoad, PendingGets,
};
use crate::server::tables::persistent::{
    open, read_not_asked, refuse, shed, storage_write, unreadable, ApplyStep, ParkedQueries,
    Parking, RowSink,
};
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::Command;
use crate::shared::queries::{SortedExists, SortedGet, SortedQuery};
use crate::shared::queries::{SortedUpdate, UnsortedGet};
use crate::shared::responses::{Response, ResponseAction, ResponseError};
use crate::shared::traits::{
    RkyvSupport, ShoalProjection, ShoalSortedTable, ShoalTableSupport, TableNameSupport,
};
use crate::storage::{
    link_released, FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse,
    RecoveryStats, ShouldPrune, StorageSupport,
};
use crate::tables::partitions::{MaybeLoaded, MaybeRow, PartitionSupport, ValidatedArchive};
use rkyv::util::AlignedVec;

/// Apply an update to a partition during recovery and count it if it was dropped
///
/// [`SortedPartition::update`] answers `None` both for a row a delete already
/// tombstoned and for a row that was never there. Those are very different things -
/// the first is the delete working and the second is data we no longer have - so the
/// miss is classified here instead of being collapsed into one number.
///
/// # Arguments
///
/// * `partition` - The partition to apply this update to
/// * `update` - The update to apply
/// * `stats` - The counts of what this recovery has discarded
pub(crate) fn replay_update<T: ShoalSortedTable>(
    partition: &mut SortedPartition<T>,
    update: &SortedUpdate<T>,
    stats: &mut RecoveryStats,
) -> isize {
    // apply the update to the target row
    match partition.update(update) {
        // our update landed on a live row
        Some(diff) => diff,
        None => {
            // tell a row a delete already took apart from one that is simply gone
            match partition.rows.get(&update.sort_key) {
                // a delete already took this row, so dropping this update costs nothing
                Some(MaybeRow::Tombstone) => stats.updates_after_delete += 1,
                // this rows insert is gone, so this update is data we no longer have
                _ => {
                    tracing::warn!(
                        "Skipping update intent for missing row in partition {}",
                        update.partition_key
                    );
                    stats.orphaned_updates += 1;
                }
            }
            // nothing was updated so this partition did not change size
            0
        }
    }
}

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(u8)]
pub enum SortedIntents<T: ShoalSortedTable + RkyvSupport> {
    Insert(T),
    Delete {
        partition_key: u64,
        sort_key: T::Sort,
    },
    Update(SortedUpdate<T>),
}

impl<T: ShoalSortedTable> SortedIntents<T> {
    /// build an insert intent
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(row: T) -> Self {
        SortedIntents::Insert(row)
    }

    /// build an delete intent
    ///
    /// # Arguments
    ///
    /// * `partition_key`- The partition key for the row that is being deleted
    /// * `sort_key` - The sort key for the row that is being deleted
    pub fn delete(partition_key: u64, sort_key: T::Sort) -> Self {
        SortedIntents::Delete {
            partition_key,
            sort_key,
        }
    }

    /// build an update intent
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply
    pub fn update(update: SortedUpdate<T>) -> Self {
        SortedIntents::Update(update)
    }
}

impl<T: ShoalSortedTable> RkyvSupport for SortedIntents<T> {}

/// A table that stores data both in memory and on disk
#[derive(Debug)]
pub struct PersistentSortedTable<R: ShoalSortedTable, S: StorageSupport, N: TableNameSupport>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
{
    /// The name of this table
    table_name: N,
    /// The rows in this table
    pub partitions: HashMap<u64, MaybeLoaded<SortedPartition<R>>>,
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
    /// The response data for gets that needed partitions to be loaded from disk
    pending_data: PendingGets,
    /// The partitions each exists query is still waiting to have loaded from disk
    ///
    /// An exists answers with a bool rather than rows, so it only needs to know which of its
    /// partitions it has yet to hear about.
    pending_exists: HashMap<(Uuid, usize), Vec<u64>>,
    /// The responses for queries that have been flushed to disk
    flushed: Vec<(Uuid, Uuid, Span, StageStamps, Response<R>)>,
    /// The channel to send loader jobs on
    loader_tx: AsyncSender<LoaderMsg<N>>,
    /// A map of queries blocked on partitions being loaded from disk
    blocked: ParkedQueries<SortedQuery<R>>,
    /// The most writes this table holds waiting to be made durable before it sheds one
    max_pending_writes: usize,
    /// The most queries this table holds parked on partition reads before it sheds one
    max_parked_queries: usize,
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
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<GxHasher>>>>,
    /// What replaying this tables intent logs had to discard
    recovery: RecoveryStats,
}

#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<R: ShoalSortedTable + 'static, S: StorageSupport, N: TableNameSupport>
    PersistentSortedTable<R, S, N>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
    <<R as ShoalTableSupport>::UpdateData as Archive>::Archived: rkyv::Deserialize<
        <R as ShoalTableSupport>::UpdateData,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    <<R as ShoalSortedTable>::Sort as Archive>::Archived:
        rkyv::Deserialize<<R as ShoalSortedTable>::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
    for<'a> <<R as ShoalSortedTable>::Sort as Archive>::Archived: rkyv::bytecheck::CheckBytes<
        Strategy<
            rkyv::validation::Validator<
                rkyv::validation::archive::ArchiveValidator<'a>,
                rkyv::validation::shared::SharedValidator,
            >,
            rkyv::rancor::Error,
        >,
    >,
    for<'a> <R as Archive>::Archived: rkyv::bytecheck::CheckBytes<
        Strategy<
            rkyv::validation::Validator<
                rkyv::validation::archive::ArchiveValidator<'a>,
                rkyv::validation::shared::SharedValidator,
            >,
            rkyv::rancor::Error,
        >,
    >,
    for<'a> <<R as ShoalTableSupport>::UpdateData as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
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
    ) -> Result<u64, ServerError> {
        // the engine does the folding; this only names the partition and row types
        S::fold_intents::<SortedPartition<R>, R>(shard_name, shard_table_name, conf).await
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
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(name = "PersistentTable::new", skip_all, fields(shard_name, table_name = ?table_name), err(Debug))]
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
    ) -> Result<Self, ServerError> {
        // make sure we have a loader channel for filesystems
        let (loader_tx, _) = loader_channels
            .entry(S::loader_kind())
            .or_insert_with(|| kanal::unbounded_async());
        // build our table
        let mut table = Self {
            table_name,
            partitions: HashMap::with_capacity(1000),
            storage: S::new::<SortedPartition<R>, R, N>(
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
            pending_data: PendingGets::with_capacity(500),
            pending_exists: HashMap::with_capacity(500),
            flushed: Vec::with_capacity(1000),
            loader_tx: loader_tx.clone(),
            blocked: ParkedQueries::with_capacity(1000),
            max_pending_writes: conf.networking.max_pending_writes,
            max_parked_queries: conf.networking.max_parked_queries,
            loading: HashSet::new(),
            stale: HashSet::new(),
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
    ) -> Result<PartitionLoad<SortedQuery<R>>, ServerError> {
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
        // overlay any existing loaded partition data on this newly loaded partition
        match self.partitions.entry(loaded.partition_id) {
            hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                // an archive resident already is another copy of the extent we just read, from a
                // second read that raced the first, so keep it and drop ours - both arms are
                // named, since a pattern that quietly did not match is how this was once silent
                // ([Resolved #30](../../../../docs/src/appendix/resolved/resident-copy-collision.md))
                MaybeLoaded::Accessible(resident) => {
                    // compare the two, saying so if they disagree; nothing is kept or charged
                    settle_resident_read(self.table_name, partition_id, resident, &loaded.data);
                }
                // if this partition is loaded then insert its current rows ontop of this loaded data
                MaybeLoaded::Loaded { partition, .. } => {
                    // get the current size of this partition
                    let old_size = partition.size();
                    // access and deserialize our loaded partitions data
                    //
                    // this is where a corrupt archive shows up on the merge path, and it has to
                    // release the queries parked on it rather than propagate: an error out of
                    // here ends the shard, and every one of those queries is parked on a
                    // `blocked` entry that only a completed load ever drains
                    let merged = SortedPartition::<R>::access(&loaded.data)
                        .and_then(|accessed| SortedPartition::<R>::deserialize(&accessed));
                    let new = match merged {
                        Ok(new) => new,
                        Err(error) => {
                            event!(
                                Level::ERROR,
                                msg = "A loaded partition could not be read back",
                                table = %self.table_name,
                                partition_id,
                                error = ?error,
                            );
                            // answer the queries parked on this read with the failure
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
                    // replay our in memory rows ontop of the copy we just read from disk
                    partition.merge_from_disk(new);
                    // calculate the difference in our partition size
                    let diff = partition.size().cast_signed() - old_size.cast_signed();
                    // merging in the disk copy can only grow this partition since our in
                    // memory rows win every collision, but apply the diff signed anyway so
                    // drift in the delta maintained sizes can never wrap our counter
                    adjust_memory_usage(&self.memory_usage, diff);
                    // remove this partition from our cache until any blocked queries have completed
                    self.lru
                        .borrow_mut()
                        .pop(&(self.table_name, loaded.partition_id));
                }
            },
            // this partition does not have any already loaded data
            hash_map::Entry::Vacant(entry) => {
                // validate this archive once, here, instead of on every query that reads it
                let validated = hotpath::measure_block!("ValidatedArchive::new", {
                    ValidatedArchive::new(loaded.data)
                });
                // a corrupt archive releases the queries parked on it rather than propagating
                //
                // returning the error here would end the shard, and would leave every query
                // parked on this partition in `blocked`, which only a completed load drains
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
                        // answer the queries parked on this read with the failure
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
                // get the size of our data, which is only charged once it is known to be good
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
        Ok(match self.blocked.take(&partition_id) {
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
    ) -> Option<Vec<(QueryMetadata, SortedQuery<R>)>> {
        // a read an apply asked for gave up, whatever else was waiting on it
        self.loading.remove(&partition_id);
        // take the queries that were parked on this partition
        let mut blocked = self.blocked.take(&partition_id)?;
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
    /// Says whether this query was parked, whether this partition has no data on disk to wait
    /// for, in which case the caller should answer now, or whether it was shed because this
    /// table already holds `max_parked_queries` parked queries. A query is only ever shed
    /// before anything was pushed or asked for, and only when `may_shed` says no part of it
    /// is parked already ([Resolved #15](../../../../docs/src/appendix/resolved/backlog-bounds.md)).
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
    /// * `may_shed` - Whether this query has parked nothing yet, so it may still be shed
    #[instrument(name = "PersistentTable::block_on_load", skip_all)]
    async fn block_on_load(
        &mut self,
        partition_key: u64,
        meta: &QueryMetadata,
        query: SortedQuery<R>,
        may_shed: bool,
    ) -> Parking {
        // a query released by a failed load answers without the read that just failed, since
        // asking for it again would only park this query on the same failure - unless another
        // read of it is already in flight, which it queues behind like any other query
        if meta.skip_disk == Some(partition_key) && !self.blocked.contains_key(&partition_key) {
            return Parking::Absent;
        }
        // a table holding as many parked queries as it may sheds this one before it parks,
        // rather than growing the parked set by arrival rate times read latency
        if may_shed && self.blocked.len() >= self.max_parked_queries {
            return Parking::Shed;
        }
        // if this partition already has blocked queries then a load is in flight
        // for it, so queue behind that load rather than requesting it again
        let Err(query) = self.blocked.join(partition_key, meta, query) else {
            return Parking::Parked;
        };
        // a read a replicated apply asked for is in flight too, and its landing drains
        // `blocked` like any other, so park behind it rather than asking for a second read
        // that would land on the copy the first one made resident
        // ([Resolved #121](../../../../docs/src/appendix/resolved/resident-copy-collision.md))
        if self.loading.contains(&partition_key) {
            // park this query behind the read the apply already requested
            self.blocked.park(partition_key, meta, query);
            return Parking::Parked;
        }
        // try to load this partition from disk if it exists
        let will_load = match self
            .storage
            .load_partition(
                self.table_name,
                partition_key,
                // the read this asks for belongs in the trace of the query that parks on it
                &meta.span,
                &self.loader_tx,
            )
            .await
        {
            Ok(will_load) => will_load,
            // the loader could not take the request, so nothing was parked and nothing will land
            Err(error) => return read_not_asked(self.table_name, partition_key, &error),
        };
        // if this partition has no data on disk then there is nothing to wait for
        if !will_load {
            // and nothing will be until this shard writes it, at which point what it writes
            // is what this partition is already holding - so remember this answer instead of
            // asking again on every query that touches this partition
            self.mark_absent_from_disk(partition_key);
            return Parking::Absent;
        }
        // park this query until its partition has been loaded from disk
        self.blocked.park(partition_key, meta, query);
        Parking::Parked
    }

    /// Shed a query that would have parked past this table's bound
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the query being shed
    fn shed_parked<P>(
        &self,
        meta: QueryMetadata,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // say which table and which bound, never anything about the partition
        shed(
            meta,
            format!(
                "{} holds {} queries parked on partition reads and this one was not parked behind them",
                self.table_name, self.max_parked_queries
            ),
        )
    }

    /// Shed a write the device has not kept up with
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the write being shed
    fn shed_pending<P>(
        &self,
        meta: QueryMetadata,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // say which table and which bound
        shed(
            meta,
            format!(
                "{} holds {} writes waiting to be made durable and this one was not committed behind them",
                self.table_name, self.max_pending_writes
            ),
        )
    }

    /// Record that storage has told us a partition has nothing on disk
    ///
    /// A sorted partition starts out assuming it may have rows in an archive nobody has read
    /// yet, and only a completed read ever cleared that. A read that found no archive at all
    /// cleared nothing, so the partition asked again on every get - and, because a partition
    /// that might have rows on disk can never be answered in place, never took the borrowing
    /// path [F27](../../../docs/src/features/grouped-responses.md) built for it
    /// ([Resolved #80](../../../docs/src/appendix/resolved/never-flushed-partitions.md)).
    ///
    /// This is only sound because **every row that reaches an archive passed through this copy
    /// first**: an archive is built by compacting this shard's own intent log over this
    /// partition's previous archive, and every intent in that log was applied to the partition
    /// in memory when it was accepted. A partition dropped from memory takes this answer with
    /// it, since eviction removes the whole entry and the next write rebuilds it from
    /// [`SortedPartition::new`], which assumes disk again.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition storage had nothing for
    fn mark_absent_from_disk(&mut self, partition_key: u64) {
        // a partition we are not holding has nowhere to record this
        //
        // an accessible partition is already the copy from disk and never asks in the first
        // place, so a loaded one is the only kind that can be told this
        if let Some(MaybeLoaded::Loaded { partition, .. }) = self.partitions.get_mut(&partition_key)
        {
            // nothing is on disk, so nothing is left for a read to find
            partition.check_disk = false;
        }
    }

    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `query` - The query to execute
    #[instrument(name = "PersistentTable::handle", skip(self, query))]
    pub async fn handle<P: ShoalProjection<Row = R>>(
        &mut self,
        mut meta: QueryMetadata,
        query: SortedQuery<R>,
        seal: SealReply<P>,
    ) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)> {
        // tag this record with what kind of query it came from
        //
        // this is the only layer that knows for certain which op ran, and the kinds are kept
        // apart because pooling them makes a percentile report where the boundary between
        // two distributions landed rather than anything about either one
        meta.stamps.set_op(match &query {
            SortedQuery::Insert { .. } => StageOp::Insert,
            SortedQuery::Get(_) => StageOp::Get,
            SortedQuery::Delete { .. } => StageOp::Delete,
            SortedQuery::Update(_) => StageOp::Update,
            SortedQuery::Exists(_) => StageOp::Exists,
        });
        // note how this tables intent log is made durable, since a table acknowledging on a
        // landed write has no fdatasync stage and a report showing one would be fiction
        meta.stamps.set_durability(self.storage.durability());
        // keep the failure this query was released with, if a read it was parked on gave up
        let failed = meta.failed.take();
        // a write the device has not kept up with is shed before it is committed, rather than
        // growing the pending queue by arrival rate times fsync latency. A write released from a
        // parked read is judged again here, which is sound: it has committed nothing yet
        // ([Resolved #15](../../../../docs/src/appendix/resolved/backlog-bounds.md))
        if matches!(
            query,
            SortedQuery::Insert { .. } | SortedQuery::Delete { .. } | SortedQuery::Update(_)
        ) && self.pending.len() >= self.max_pending_writes
        {
            return apply_failure(open(self.shed_pending(meta)), failed);
        }
        // execute the correct query type
        let answered = match query {
            // insert a row into this partition
            SortedQuery::Insert { row, .. } => open(self.insert(meta, row).await),
            // get a row from this partition
            //
            // the only query that can answer with rows, and so the only one that can answer
            // with rows it did not have to copy first
            SortedQuery::Get(get) => self.get(meta, &get, seal).await,
            // delete a row from this partition
            SortedQuery::Delete { key, sort_key } => open(self.delete(meta, key, sort_key).await),
            // update a row in this partition
            SortedQuery::Update(update) => open(self.update(meta, update).await),
            // check if data exists in this partition
            SortedQuery::Exists(exists) => open(self.exists(meta, &exists).await),
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
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // get our partition key
        let key = row.get_partition_key();
        // wrap our row in an insert intent
        let intent = SortedIntents::Insert(row);
        // find the partition this row joins, once, and hold it across the commit
        let slot = self.partitions.entry(key);
        // a partition resident only as its archive has to become rows before this row can join
        // it, and that happens before the commit so a copy that cannot be read commits nothing
        let archived = match &slot {
            Entry::Occupied(occupied) => match occupied.get() {
                MaybeLoaded::Accessible(read) => {
                    match SortedPartition::<R>::deserialize(read.archived()) {
                        Ok(partition) => Some(partition),
                        Err(error) => return unreadable(meta, self.table_name, key, &error),
                    }
                }
                MaybeLoaded::Loaded { .. } => None,
            },
            Entry::Vacant(_) => None,
        };
        // persist this new row to storage, or refuse it having changed nothing
        let pos = match self.storage.commit(&intent).await {
            Ok(pos) => pos,
            Err(error) => return storage_write(meta, self.table_name, key, &error),
        };
        // record that this writes synchronous work is finished
        //
        // a write returns nothing to the shard, so it stamps this itself rather than having
        // `handle_query` do it. Everything `commit` blocked on - the write behind
        // backpressure in particular - lands in this stage rather than in a durability one.
        meta.stamps.mark_exec_done();
        // extract our row from our intent
        let row = match intent {
            SortedIntents::Insert(row) => row,
            // SAFETY we just wrapped this in an insert intent before
            _ => unsafe { std::hint::unreachable_unchecked() },
        };
        // insert this row into its partition, starting one if this is the partitions first row
        let (size_diff, action) = match (slot, archived) {
            // the archives rows, read above, become the partition this row joins
            (Entry::Occupied(mut occupied), Some(mut partition)) => {
                // since this partition was accessible it was the full partition from disk, so
                // we don't need to check disk again - the flag an archive carries is whatever
                // the compactor happened to write and says nothing about this copy
                partition.check_disk = false;
                // insert this new row into our loaded partition
                let inserted = partition.insert(row);
                // replace our accessible partition with the loaded one
                occupied.insert(MaybeLoaded::Loaded {
                    partition,
                    generation: self.generation,
                });
                inserted
            }
            // a partition already holding its rows takes this one as it is
            (Entry::Occupied(mut occupied), None) => match occupied.get_mut() {
                MaybeLoaded::Loaded {
                    partition,
                    generation,
                } => {
                    // this partitions newest data is now in the log we are writing to, so
                    // it can't be evicted until that log has been compacted
                    *generation = self.generation;
                    partition.insert(row)
                }
                // an accessible partition always has its archive read above
                MaybeLoaded::Accessible(_) => {
                    unreachable!("an accessible partition was read before its commit")
                }
            },
            // a partition we have never seen starts with this row
            (Entry::Vacant(vacant), _) => {
                // start an empty partition and insert this row into it
                let mut partition = SortedPartition::new(key);
                let inserted = partition.insert(row);
                vacant.insert(MaybeLoaded::Loaded {
                    partition,
                    generation: self.generation,
                });
                inserted
            }
        };
        // add this action to our pending queue
        self.pending.add(meta, pos, action);
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
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `get` - The get parameters to use
    #[instrument(name = "PersistentTable::get", skip_all)]
    async fn get<P: ShoalProjection<Row = R>>(
        &mut self,
        meta: QueryMetadata,
        get: &SortedGet<R>,
        seal: SealReply<P>,
    ) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)> {
        // a get this shard can answer out of the rows it is already holding is serialized where
        // they lie, rather than copied into a response and serialized from that
        if self.can_answer_in_place(&meta, get) {
            return Some(self.get_sealed::<P>(meta, get, seal));
        }
        // a get that parked on an earlier execution is never shed, since part of it is held
        //
        // this is read before `resume`, which takes the parked state out
        let fresh = !self.pending_data.is_parked(&(meta.id, meta.index));
        // whether this execution has parked this get on any partition yet
        let mut parked_any = false;
        // pick this get up where its last execution left off, or start it fresh
        //
        // a get blocked on a partition is replayed once that partition has been read, so the
        // rows it already found have to outlive the execution that found them
        let mut pending = match self.pending_data.resume::<P>(
            &(meta.id, meta.index),
            &get.partition_keys,
            get.limit,
        ) {
            Ok(pending) => pending,
            // another get parked under this id and index, and it is left to finish
            Err(error) => return open(refuse(meta, error)),
        };
        // the archived forms of this gets keys, built the first time a partition of it is
        // being read in place - a get every one of whose partitions is resident builds none
        let mut seek = None;
        // check each of the partition keys this execution was handed
        for partition_key in &get.partition_keys {
            // find where this partitions rows belong in the answer
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
            // work out whether we hold this partition and whether it might have rows on disk
            let (resident, check_disk) = match self.partitions.get(partition_key) {
                // a loaded partition may still have rows in an archive we have not read yet
                Some(MaybeLoaded::Loaded { partition, .. }) => (true, partition.check_disk),
                // an accessible partition is already the copy from disk
                Some(MaybeLoaded::Accessible(_)) => (true, false),
                // a partition we have never seen may still be on disk
                None => (false, true),
            };
            // read this partition from disk if it might hold rows we do not have
            if check_disk {
                // build a query for just this blocked partition
                let blocked_get = SortedQuery::Get(get.to_blocked(*partition_key));
                // park this get if this partition has to be read from disk first
                //
                // a partition being read fills its own slot when this get is replayed for it, and
                // a get is only shed while no part of it is parked
                let may_shed = fresh && !parked_any;
                match self
                    .block_on_load(*partition_key, &meta, blocked_get, may_shed)
                    .await
                {
                    // leave this slot empty for the replay to fill
                    Parking::Parked => {
                        parked_any = true;
                        continue;
                    }
                    // nothing of this get is parked, so what it found so far is dropped
                    Parking::Shed => return open(self.shed_parked(meta)),
                    // this partition could not be read, which this get answers with once
                    // nothing of it is parked
                    Parking::Failed(error) => {
                        pending.fail(rank, error);
                        continue;
                    }
                    Parking::Absent => (),
                }
            }
            // get the rows this get asked for from this partition
            //
            // these rows are this partitions alone, so the limit stops each partition after
            // its own first `limit` rows rather than stopping the get at the first partition
            // that happens to fill it
            let mut rows = RowSink::default();
            // a partition we have never seen and that is not on disk has no rows to give
            if resident {
                // SAFETY: we looked this partition up above and have not touched the map since
                if let Some(partition) = self.partitions.get(partition_key) {
                    partition.get(get, &mut seek, &mut rows);
                }
            }
            // this get has to outlive the scan that found these rows, so they are taken as owned
            // rows here rather than pointed at
            pending.fill(rank, rows.into_owned());
        }
        // hold this get until every partition it named has been read
        if pending.is_pending() {
            // remember what we have found so far for the replay to carry on from
            self.pending_data.park((meta.id, meta.index), pending);
            // we have blocked partitions so return None
            return None;
        }
        // a partition this get named could not be read, so it answers with why
        if let Some(error) = pending.take_failure() {
            return open(refuse(meta, error));
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
    /// Four things stop it, and each of them for the same reason: the rows would have to outlive
    /// the scan that found them.
    ///
    /// * a **share of a split get** travels to the shard collecting it, to be merged there;
    /// * a **parked get** is picked back up on a later execution, carrying what it already found;
    /// * a get naming a partition that **is not resident** is about to park on reading it;
    /// * so is one naming a resident partition that **may still have rows on disk**.
    ///
    /// The last two are read out of the map with the same expression the scan below uses, so the
    /// decision and the scan cannot come to different conclusions about the same partition.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the get being executed
    /// * `get` - The get being executed
    fn can_answer_in_place(&self, meta: &QueryMetadata, get: &SortedGet<R>) -> bool {
        // a share of a split get is owed to another shard rather than to a client
        if meta.gather.is_some() {
            return false;
        }
        // a get that has already parked holds rows from an execution that has ended
        if self.pending_data.is_parked(&(meta.id, meta.index)) {
            return false;
        }
        // and none of the partitions it names may still have rows we have not read
        get.partition_keys
            .iter()
            .all(|key| !self.partition_needs_disk(key))
    }

    /// Whether a partition this get named might still have rows on disk
    ///
    /// The one place either path decides this, so that
    /// [`Self::can_answer_in_place`] and the scan it guards cannot disagree about a partition.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition to judge
    fn partition_needs_disk(&self, partition_key: &u64) -> bool {
        match self.partitions.get(partition_key) {
            // a loaded partition may still have rows in an archive we have not read yet
            Some(MaybeLoaded::Loaded { partition, .. }) => partition.check_disk,
            // an accessible partition is already the copy from disk
            Some(MaybeLoaded::Accessible(_)) => false,
            // a partition we have never seen may still be on disk
            None => true,
        }
    }

    /// Answer a get out of the rows this shard is already holding
    ///
    /// The rows never become owned values: they are pointed at where the partitions hold them,
    /// and the reply is serialized from those pointers while the scan's borrow is still alive.
    /// That is [O2](../../../docs/src/appendix/optimizations.md)'s resident half — a row a get
    /// returns is copied once, into the buffer that goes to the client, rather than once into a
    /// response and again into that buffer.
    ///
    /// A partition that is still an archive is the exception, and the half of that entry this
    /// does not close: what it holds is `Archived<R>`, and there is no `R` to point at, so those
    /// rows are built as they always were. A get mixing the two answers with the rows it can
    /// borrow and the rows it had to build, side by side.
    ///
    /// Only [`Self::can_answer_in_place`] may send a get here, and it is the reason nothing in
    /// this function can park, block or take `&mut self`.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata of the get being answered
    /// * `get` - The get to answer
    /// * `seal` - How to serialize the reply into the response kind this table answers in
    fn get_sealed<'a, P: ShoalProjection<Row = R>>(
        &'a self,
        mut meta: QueryMetadata,
        get: &'a SortedGet<R>,
        seal: SealReply<P>,
    ) -> (Uuid, Uuid, StageStamps, Answer<Response<P>>) {
        // the archived forms of this gets keys, built the first time one is needed
        let mut seek = None;
        // point at the rows each named partition holds, in the order they were named
        let mut sink = RowSink::default();
        for partition_key in &get.partition_keys {
            // once we hold every row this get asked for, nothing later can reach the answer
            if get.limit_reached(sink.len()) {
                break;
            }
            // a partition that needs a disk read cannot be here - can_answer_in_place checked
            if let Some(partition) = self.partitions.get(partition_key) {
                partition.get(get, &mut seek, &mut sink);
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

    /// Check if data exists in some partitions
    ///
    /// The rows are checked by `MaybeLoaded::exists`, which covers both of the ways a
    /// partition can be held, so this only has to decide which partitions to read. An
    /// exists naming sort keys asks about those rows alone; one naming none asks whether
    /// its partitions hold any row at all.
    ///
    /// A partition that might still have rows on disk is read before it is answered
    /// about, even when this exists names sort keys. A named row missing from the copy in
    /// memory says nothing about the copy in an archive.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this exists query
    /// * `exists_query` - The exists parameters to use
    #[instrument(name = "PersistentTable::exists", skip_all)]
    async fn exists<P>(
        &mut self,
        meta: QueryMetadata,
        exists_query: &SortedExists<R>,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // pick up the partitions this exists is still waiting on, or start it fresh
        // an exists that parked on an earlier execution is never shed, since part of it is held
        let fresh = !self.pending_exists.contains_key(&(meta.id, meta.index));
        let mut blocked = match self.pending_exists.remove(&(meta.id, meta.index)) {
            // carry on with the partitions this exists has yet to read
            Some(blocked) => blocked,
            // this query has never been executed before so instance sane defaults
            None => Vec::default(),
        };
        // the archived forms of this exists keys, built the first time a partition of it is
        // being read in place - an exists whose partitions are all resident builds none
        let mut seek = None;
        // check each of the partition keys this execution was handed
        for partition_key in &exists_query.partition_keys {
            // work out whether we hold this partition and whether it might have rows on disk
            let (resident, check_disk) = match self.partitions.get(partition_key) {
                // a loaded partition may still have rows in an archive we have not read yet
                Some(MaybeLoaded::Loaded { partition, .. }) => (true, partition.check_disk),
                // an accessible partition is already the copy from disk
                Some(MaybeLoaded::Accessible(_)) => (true, false),
                // a partition we have never seen may still be on disk
                None => (false, true),
            };
            // read this partition from disk if it might hold rows we do not have
            if check_disk {
                // build a query for just this blocked partition
                let blocked_exists = SortedQuery::Exists(exists_query.to_blocked(*partition_key));
                // park this exists if this partition has to be read from disk first
                //
                // a partition being read is answered about when this exists is replayed for it
                // and it is only shed while no part of it is parked
                let may_shed = fresh && blocked.is_empty();
                match self
                    .block_on_load(*partition_key, &meta, blocked_exists, may_shed)
                    .await
                {
                    // remember that we are still waiting on this partition
                    Parking::Parked => {
                        blocked.push(*partition_key);
                        continue;
                    }
                    // nothing of this exists is parked, so it is answered as shed
                    Parking::Shed => return self.shed_parked(meta),
                    // the read could not be asked for, so this query answers with why
                    Parking::Failed(error) => return refuse(meta, error),
                    Parking::Absent => (),
                }
            }
            // this partition has been read so it is no longer one we are waiting on
            blocked.retain(|key| key != partition_key);
            // a partition we have never seen and that is not on disk has no rows to check
            if resident {
                // SAFETY: we looked this partition up above and have not touched the map since
                if let Some(partition) = self.partitions.get(partition_key) {
                    // check whether this partition holds any of the rows we were asked about
                    if partition.exists(exists_query, &mut seek) {
                        // this partition holds a row this exists named
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Exists(true),
                            end: meta.end,
                        };
                        return Some((meta.client, meta.id, meta.stamps, response));
                    }
                }
            }
        }
        // hold this exists until every partition it named has been read
        if !blocked.is_empty() {
            // remember what we are still waiting on for the replay to carry on from
            self.pending_exists.insert((meta.id, meta.index), blocked);
            return None;
        }
        // none of the partitions we read held any of the rows this exists named
        let response = Response {
            id: meta.id,
            index: meta.index,
            data: ResponseAction::Exists(false),
            end: meta.end,
        };
        Some((meta.client, meta.id, meta.stamps, response))
    }

    /// Delete a row from this table
    ///
    /// Deletes only succeed if the row exists. If the partition is loaded but
    /// has `check_disk` set to true and the row isn't found, the partition will
    /// be loaded from disk first. If the row doesn't exist after loading, returns false.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this delete query
    /// * `key` - The key to the partition to delete data from
    /// * `sort` - The sort key to delete
    #[instrument(name = "PersistentTable::delete", skip_all)]
    async fn delete<P>(
        &mut self,
        mut meta: QueryMetadata,
        key: u64,
        sort: R::Sort,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // get the partition we want to delete from
        match self.partitions.get_mut(&key) {
            Some(maybe_loaded) => {
                // check if this partition is fully loaded in memory or not
                match maybe_loaded {
                    // the partition is at least partially deserialized and loaded into memory
                    MaybeLoaded::Loaded {
                        partition,
                        generation,
                    } => {
                        // try to remove the target row
                        if let Some((size_diff, removed)) = partition.remove(&sort) {
                            // we were able to delete this row so build the delete intent
                            let intent = SortedIntents::<R>::delete(key, sort);
                            // commit it to the intent to the intent log
                            let pos = match self.storage.commit(&intent).await {
                                Ok(pos) => pos,
                                Err(error) => {
                                    // nothing of this delete was committed, so its row goes back
                                    let sort = match intent {
                                        SortedIntents::Delete { sort_key, .. } => sort_key,
                                        // SAFETY we just wrapped this in a delete intent before
                                        _ => unsafe { std::hint::unreachable_unchecked() },
                                    };
                                    partition.restore(sort, removed, size_diff);
                                    return storage_write(meta, self.table_name, key, &error);
                                }
                            };
                            // record that this writes synchronous work is finished
                            //
                            // a write returns nothing to the shard, so it stamps this itself rather than having
                            // `handle_query` do it. Everything `commit` blocked on - the write behind
                            // backpressure in particular - lands in this stage rather than in a durability one.
                            meta.stamps.mark_exec_done();
                            // we were able to delete data
                            let action = ResponseAction::Delete(true);
                            // add this to the pending query until its commit is flushed
                            self.pending.add(meta, pos, action);
                            // subtract this deleted rows memory usage from our total usage
                            let new_size = self.memory_usage.borrow().saturating_sub(size_diff);
                            // update the current memory usage
                            *self.memory_usage.borrow_mut() = new_size;
                            // the tombstone we just wrote is the only thing shadowing this
                            // rows archived copy, so this partition can't be evicted until
                            // the log holding our delete intent has been compacted
                            *generation = self.generation;
                            // remove from LRU cache since partition was just modified
                            self.lru.borrow_mut().pop(&(self.table_name, key));
                            // we can't acknowledge this delete until its intent is flushed
                            return None;
                        } else if partition.check_disk {
                            // we couldn't find the row to delete but it may be on on disk
                            let blocked_delete = SortedQuery::Delete {
                                key,
                                sort_key: sort,
                            };
                            // park this delete if this partition has to be read from disk
                            // first, so it can be retried once we hold the archived copy
                            match self.block_on_load(key, &meta, blocked_delete, true).await {
                                // theres nothing to respond with yet
                                Parking::Parked => return None,
                                // too many queries are parked on this table to park another
                                Parking::Shed => return self.shed_parked(meta),
                                // the read could not be asked for, so this query answers with why
                                Parking::Failed(error) => return refuse(meta, error),
                                Parking::Absent => (),
                            }
                        }
                        // Tthis row doesn't exist and so can't be deleted
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Delete(false),
                            end: meta.end,
                        };
                        Some((meta.client, meta.id, meta.stamps, response))
                    }
                    // this partition is loaded from disk but not deserialized
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can modify it
                        let mut partition = match SortedPartition::<R>::deserialize(accessible) {
                            Ok(partition) => partition,
                            Err(error) => return unreadable(meta, self.table_name, key, &error),
                        };
                        // since this partition is accessible it must have been the full partition from disk
                        // so we don't need to check disk again
                        partition.check_disk = false;
                        // try to remove the target row
                        if let Some((size_diff, _)) = partition.remove(&sort) {
                            // we were able to delete this row so build the delete intent
                            let intent = SortedIntents::<R>::delete(key, sort);
                            // commit it to the intent to the intent log, or refuse it with the
                            // accessible copy untouched, since only our deserialized one changed
                            let pos = match self.storage.commit(&intent).await {
                                Ok(pos) => pos,
                                Err(error) => {
                                    return storage_write(meta, self.table_name, key, &error)
                                }
                            };
                            // record that this writes synchronous work is finished
                            //
                            // a write returns nothing to the shard, so it stamps this itself rather than having
                            // `handle_query` do it. Everything `commit` blocked on - the write behind
                            // backpressure in particular - lands in this stage rather than in a durability one.
                            meta.stamps.mark_exec_done();
                            // we were able to delete data
                            let action = ResponseAction::Delete(true);
                            // add this to the pending query until its commit is flushed
                            self.pending.add(meta, pos, action);
                            // subtract this deleted rows memory usage from our total usage
                            let new_size = self.memory_usage.borrow().saturating_sub(size_diff);
                            // update the current memory usage
                            *self.memory_usage.borrow_mut() = new_size;
                            // remove from LRU cache since partition was just modified
                            self.lru.borrow_mut().pop(&(self.table_name, key));
                            // convert to Loaded state since we've deserialized it
                            *maybe_loaded = MaybeLoaded::Loaded {
                                partition,
                                generation: self.generation,
                            };
                            // we can't acknowledge this delete until its intent is flushed
                            None
                        } else {
                            // row wasn't found but we deserialized this partition so keep it
                            *maybe_loaded = MaybeLoaded::Loaded {
                                partition,
                                generation: self.generation,
                            };
                            // build the failed delete response
                            let response = Response {
                                id: meta.id,
                                index: meta.index,
                                data: ResponseAction::Delete(false),
                                end: meta.end,
                            };
                            Some((meta.client, meta.id, meta.stamps, response))
                        }
                    }
                }
            }
            None => {
                // we don't have this partition loaded so try to load it
                let blocked_delete = SortedQuery::Delete {
                    key,
                    sort_key: sort,
                };
                // this partition exists and is being loaded
                match self.block_on_load(key, &meta, blocked_delete, true).await {
                    Parking::Parked => None,
                    // too many queries are parked on this table to park another
                    Parking::Shed => self.shed_parked(meta),
                    // the read could not be asked for, so this query answers with why
                    Parking::Failed(error) => refuse(meta, error),
                    Parking::Absent => {
                        // build the failed delete response
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Delete(false),
                            end: meta.end,
                        };
                        Some((meta.client, meta.id, meta.stamps, response))
                    }
                }
            }
        }
    }

    /// Update a row in this table
    ///
    /// Updates only succeed if the row exists. If the partition is loaded but
    /// has `check_disk` set to true and the row isn't found, the partition will
    /// be loaded from disk first. If the row doesn't exist after loading, returns false.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this update query
    /// * `update` - The update to apply to a row in this table
    #[instrument(name = "PersistentTable::update", skip_all)]
    async fn update<P>(
        &mut self,
        mut meta: QueryMetadata,
        update: SortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, StageStamps, Response<P>)> {
        // get the partition we want to update
        match self.partitions.get_mut(&update.partition_key) {
            Some(maybe_loaded) => {
                // check if this partition is fully loaded in memory or not
                match maybe_loaded {
                    // the partition is at least partialy deserialzied and loaded into memory
                    MaybeLoaded::Loaded {
                        partition,
                        generation,
                    } => {
                        // find the target row if its loaded, leaving it as it is until its
                        // update has been committed
                        if let Some(row) = partition.live_row_mut(&update.sort_key) {
                            // we were able to find this row so get its partitions key
                            let key = update.partition_key;
                            // wrap our update in an update intent
                            let intent = SortedIntents::<R>::update(update);
                            // commit this intent to storage, or refuse it with the row untouched
                            let pos = match self.storage.commit(&intent).await {
                                Ok(pos) => pos,
                                Err(error) => {
                                    return storage_write(meta, self.table_name, key, &error)
                                }
                            };
                            // take our update back out of its intent
                            let update = match intent {
                                SortedIntents::Update(update) => update,
                                // SAFETY we just wrapped this in an update intent before
                                _ => unsafe { std::hint::unreachable_unchecked() },
                            };
                            // apply the committed update to its row
                            let diff = SortedPartition::update_row(row, &update);
                            // and add the change in its size to its partition
                            partition.resize(diff);
                            // record that this writes synchronous work is finished
                            //
                            // a write returns nothing to the shard, so it stamps this itself rather than having
                            // `handle_query` do it. Everything `commit` blocked on - the write behind
                            // backpressure in particular - lands in this stage rather than in a durability one.
                            meta.stamps.mark_exec_done();
                            // we were able to update data
                            let action = ResponseAction::Update(true);
                            // add this to our pending queries until its commit is flushed
                            self.pending.add(meta, pos, action);
                            // this partitions newest data is only in the log we are writing
                            // to, so it can't be evicted until that log has been compacted
                            *generation = self.generation;
                            // remove this partition from our lru cache as its no longer evictable
                            self.lru.borrow_mut().pop(&(self.table_name, key));
                            // do a saturating add on our memory usage
                            let new_size = self.memory_usage.borrow().saturating_add_signed(diff);
                            // adjust our total shards memory usage
                            *self.memory_usage.borrow_mut() = new_size;
                            return None;
                        } else if partition.check_disk {
                            // we don't have this partition loaded so try to load it from disk
                            let partition_key = update.partition_key;
                            // park this update if this partition has to be read from disk first
                            match self
                                .block_on_load(
                                    partition_key,
                                    &meta,
                                    SortedQuery::Update(update),
                                    true,
                                )
                                .await
                            {
                                // wait for this partition to get loaded
                                Parking::Parked => return None,
                                // too many queries are parked on this table to park another
                                Parking::Shed => return self.shed_parked(meta),
                                // the read could not be asked for, so this query answers with why
                                Parking::Failed(error) => return refuse(meta, error),
                                Parking::Absent => (),
                            }
                        }
                        // this partition doesn't exist in disk or in memory
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Update(false),
                            end: meta.end,
                        };
                        Some((meta.client, meta.id, meta.stamps, response))
                    }
                    // this partition is loaded from disk but not deserialized
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can update it
                        let mut partition = match SortedPartition::<R>::deserialize(accessible) {
                            Ok(partition) => partition,
                            Err(error) => {
                                return unreadable(
                                    meta,
                                    self.table_name,
                                    update.partition_key,
                                    &error,
                                )
                            }
                        };
                        // since this partition is accessible it must have been the full partition
                        // from disk so we don't need to check disk again
                        partition.check_disk = false;
                        // try to update this partitions data
                        if let Some(diff) = partition.update(&update) {
                            // we were able to update this partition so get its key
                            let key = update.partition_key;
                            // wrap our update in an update intent
                            let intent = SortedIntents::<R>::update(update);
                            // commit this intent to storage, or refuse it with the accessible
                            // copy untouched, since only our deserialized one changed
                            let pos = match self.storage.commit(&intent).await {
                                Ok(pos) => pos,
                                Err(error) => {
                                    return storage_write(meta, self.table_name, key, &error)
                                }
                            };
                            // record that this writes synchronous work is finished
                            //
                            // a write returns nothing to the shard, so it stamps this itself rather than having
                            // `handle_query` do it. Everything `commit` blocked on - the write behind
                            // backpressure in particular - lands in this stage rather than in a durability one.
                            meta.stamps.mark_exec_done();
                            // we were able to update data
                            let action = ResponseAction::Update(true);
                            // add this to our pending queries until its commit is flushed
                            self.pending.add(meta, pos, action);
                            // remove this partition from our lru cache as its no longer evictable
                            self.lru.borrow_mut().pop(&(self.table_name, key));
                            // do a saturating add on our memory usage
                            let new_size = self.memory_usage.borrow().saturating_add_signed(diff);
                            // adjust our total shards memory usage
                            *self.memory_usage.borrow_mut() = new_size;
                            // convert to Loaded state since we've deserialized it
                            *maybe_loaded = MaybeLoaded::Loaded {
                                partition,
                                generation: self.generation,
                            };
                            None
                        } else {
                            // this row wasn't found but we deserialized this row so keep it
                            // to avoid future deserialization costs
                            *maybe_loaded = MaybeLoaded::Loaded {
                                partition,
                                generation: self.generation,
                            };
                            // build the failed update response
                            let response = Response {
                                id: meta.id,
                                index: meta.index,
                                data: ResponseAction::Update(false),
                                end: meta.end,
                            };
                            Some((meta.client, meta.id, meta.stamps, response))
                        }
                    }
                }
            }
            None => {
                // we don't have this partition loaded so try to load it
                let partition_key = update.partition_key;
                // this partition exists and is being loaded
                match self
                    .block_on_load(partition_key, &meta, SortedQuery::Update(update), true)
                    .await
                {
                    Parking::Parked => None,
                    // too many queries are parked on this table to park another
                    Parking::Shed => self.shed_parked(meta),
                    // the read could not be asked for, so this query answers with why
                    Parking::Failed(error) => refuse(meta, error),
                    Parking::Absent => {
                        // Partition doesn't exist - update fails
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Update(false),
                            end: meta.end,
                        };
                        // wait for this partition to get loaded
                        Some((meta.client, meta.id, meta.stamps, response))
                    }
                }
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
    ///
    /// # Errors
    ///
    /// Fails if the intent cannot be archived, which fails this write and nothing else.
    pub fn build_intent(
        &self,
        query: &SortedQuery<R>,
    ) -> Result<Option<(u64, Vec<u8>)>, ServerError> {
        let (key, intent) = match query {
            SortedQuery::Insert { row, .. } => {
                (row.get_partition_key(), SortedIntents::Insert(row.clone()))
            }
            SortedQuery::Delete { key, sort_key } => {
                (*key, SortedIntents::<R>::delete(*key, sort_key.clone()))
            }
            SortedQuery::Update(update) => (
                update.partition_key,
                SortedIntents::<R>::update(update.clone()),
            ),
            SortedQuery::Get(_) | SortedQuery::Exists(_) => return Ok(None),
        };
        // archive it once, for every replica's log and state machine
        let payload = RkyvSupport::serialize(&intent)?;
        Ok(Some((key, payload.to_vec())))
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
    pub fn apply(&mut self, command: &Command, generation: u64, skip_disk: bool) -> ApplyStep {
        // the generation the partition is stamped with is the WAL's
        self.generation = generation;
        self.storage.observe_generation(generation);
        // an archive is accessed at its alignment, which a command's payload does not promise
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(command.payload.len());
        aligned.extend_from_slice(&command.payload);
        // validated: the bytes crossed a process boundary
        let intent = match SortedIntents::<R>::access(&aligned) {
            Ok(intent) => intent,
            Err(error) => {
                return ApplyStep::Refused(format!(
                    "the command's payload is not a sorted intent: {error}"
                ))
            }
        };
        match intent {
            ArchivedSortedIntents::Insert(archived) => {
                let row: R = match RkyvSupport::deserialize(archived) {
                    Ok(row) => row,
                    Err(error) => {
                        return ApplyStep::Refused(format!(
                            "the command's row does not decode: {error}"
                        ))
                    }
                };
                let key = row.get_partition_key();
                let entry = self
                    .partitions
                    .entry(key)
                    .or_insert_with(|| MaybeLoaded::Loaded {
                        partition: SortedPartition::new(key),
                        generation,
                    });
                let (size_diff, _) = match entry {
                    MaybeLoaded::Loaded {
                        partition,
                        generation: stamped,
                    } => {
                        *stamped = generation;
                        partition.insert(row)
                    }
                    MaybeLoaded::Accessible(read) => {
                        let mut partition = match SortedPartition::<R>::deserialize(read.archived())
                        {
                            Ok(partition) => partition,
                            Err(error) => {
                                return ApplyStep::Refused(format!(
                                    "the resident archive does not decode: {error}"
                                ))
                            }
                        };
                        partition.check_disk = false;
                        let outcome = partition.insert(row);
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        outcome
                    }
                };
                adjust_memory_usage(&self.memory_usage, size_diff);
                self.lru.borrow_mut().pop(&(self.table_name, key));
                ApplyStep::Done(CommandResult {
                    kind: ResultKind::Insert,
                    ok: true,
                })
            }
            ArchivedSortedIntents::Delete {
                partition_key,
                sort_key,
            } => {
                let key = partition_key.to_native();
                let sort = match rkyv::deserialize::<R::Sort, rkyv::rancor::Error>(sort_key) {
                    Ok(sort) => sort,
                    Err(error) => {
                        return ApplyStep::Refused(format!(
                            "the command's sort key does not decode: {error}"
                        ))
                    }
                };
                let deleted = match self.partitions.get_mut(&key) {
                    Some(MaybeLoaded::Loaded {
                        partition,
                        generation: stamped,
                    }) => match partition.remove(&sort) {
                        Some((size, _)) => {
                            adjust_memory_usage(&self.memory_usage, -(size.cast_signed()));
                            *stamped = generation;
                            true
                        }
                        None if partition.check_disk && !skip_disk => {
                            return ApplyStep::NeedsLoad(key)
                        }
                        None => false,
                    },
                    Some(entry @ MaybeLoaded::Accessible(_)) => {
                        let MaybeLoaded::Accessible(read) = &*entry else {
                            unreachable!()
                        };
                        let mut partition = match SortedPartition::<R>::deserialize(read.archived())
                        {
                            Ok(partition) => partition,
                            Err(error) => {
                                return ApplyStep::Refused(format!(
                                    "the resident archive does not decode: {error}"
                                ))
                            }
                        };
                        partition.check_disk = false;
                        let removed = partition.remove(&sort);
                        let before = entry.size();
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        adjust_memory_usage(
                            &self.memory_usage,
                            entry.size().cast_signed() - before.cast_signed(),
                        );
                        removed.is_some()
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
            ArchivedSortedIntents::Update(archived) => {
                let update =
                    match rkyv::deserialize::<SortedUpdate<R>, rkyv::rancor::Error>(archived) {
                        Ok(update) => update,
                        Err(error) => {
                            return ApplyStep::Refused(format!(
                                "the command's update does not decode: {error}"
                            ))
                        }
                    };
                let key = update.partition_key;
                let updated = match self.partitions.get_mut(&key) {
                    Some(MaybeLoaded::Loaded {
                        partition,
                        generation: stamped,
                    }) => match partition.update(&update) {
                        Some(diff) => {
                            adjust_memory_usage(&self.memory_usage, diff);
                            *stamped = generation;
                            true
                        }
                        None if partition.check_disk && !skip_disk => {
                            return ApplyStep::NeedsLoad(key)
                        }
                        None => false,
                    },
                    Some(entry @ MaybeLoaded::Accessible(_)) => {
                        let MaybeLoaded::Accessible(read) = &*entry else {
                            unreachable!()
                        };
                        let mut partition = match SortedPartition::<R>::deserialize(read.archived())
                        {
                            Ok(partition) => partition,
                            Err(error) => {
                                return ApplyStep::Refused(format!(
                                    "the resident archive does not decode: {error}"
                                ))
                            }
                        };
                        partition.check_disk = false;
                        let updated = partition.update(&update).is_some();
                        let before = entry.size();
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        adjust_memory_usage(
                            &self.memory_usage,
                            entry.size().cast_signed() - before.cast_signed(),
                        );
                        updated
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
    /// flight; false if storage has nothing for it, in which case the partition is marked so
    /// nothing asks again.
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
        } else {
            self.mark_absent_from_disk(partition_key);
        }
        Ok(will_load)
    }

    /// Hash the applied state, for a fixture comparing replicas at a common boundary
    ///
    /// Every live row hashed as its partition key and its archived bytes, and the hashes
    /// summed, so the digest is of the set of rows and not of their order or of which shard
    /// holds them: a table dealt over two executors and the same rows on one digest the same,
    /// which is what an import is judged by
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)). Tombstones and
    /// archives alike are folded in as rows, so a resident copy and one still in its archive
    /// hash the same. A partition that is not resident is read from its archive, so a replica
    /// holding nothing in memory hashes the same state as one that applied every write itself
    /// ([F43](../../../../docs/src/features/node-recovery.md)). Returns how many rows there
    /// are and the hash.
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
            // a resident partition as it is, an archived one read back whole
            let owned;
            let partition: &SortedPartition<R> = match self.partitions.get(&key) {
                Some(MaybeLoaded::Loaded { partition, .. }) => partition,
                Some(MaybeLoaded::Accessible(read)) => {
                    owned = SortedPartition::<R>::deserialize(read.archived()).ok();
                    match &owned {
                        Some(partition) => partition,
                        None => continue,
                    }
                }
                // not resident: the archive's copy is the state
                None => {
                    let Some(read) = self.storage.load_partition_direct(key).await? else {
                        continue;
                    };
                    let archived = <SortedPartition<R> as RkyvSupport>::access(&read)?;
                    owned = SortedPartition::<R>::deserialize(archived).ok();
                    match &owned {
                        Some(partition) => partition,
                        None => continue,
                    }
                }
            };
            for (_, row) in &partition.rows {
                if let MaybeRow::Row(row) = row {
                    rows += 1;
                    let bytes = RkyvSupport::serialize(row)?;
                    // the row's own hash, summed: the order and the shard are not in it
                    let mut fold = Vec::with_capacity(8 + bytes.len());
                    fold.extend_from_slice(&key.to_le_bytes());
                    fold.extend_from_slice(&bytes);
                    acc = acc.wrapping_add(gxhash::gxhash64(&fold, 0));
                }
            }
        }
        Ok((rows, acc))
    }

    /// Take a canonical cut of some tablets: hash what is resident, collect what is archived
    ///
    /// The loop's half of a scrub ([F44](../../../../docs/src/features/repair.md)): every
    /// resident partition of the tablets is hashed here, as its key, its live row count and
    /// every live row re-serialized in sort-key order, and every archived partition that is not
    /// resident is collected with a handle to read it by. A resident copy shadows the archived
    /// one, since it is the newer. Unlike [`Self::digest`] this reads nothing from disk on the
    /// loop and never sees archive bytes as such, so two replicas with the same rows agree
    /// however their archives lie.
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
        // every resident partition of the tablets, hashed as the rows it holds in sort order
        for (key, entry) in &self.partitions {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            if !tablets.contains(&tablet) {
                continue;
            }
            // a resident copy is the state whatever the archive holds
            resident_keys.insert(*key);
            let owned;
            let partition: &SortedPartition<R> = match entry {
                MaybeLoaded::Loaded { partition, .. } => partition,
                MaybeLoaded::Accessible(read) => {
                    owned = SortedPartition::<R>::deserialize(read.archived()).ok();
                    match &owned {
                        Some(partition) => partition,
                        None => continue,
                    }
                }
            };
            if let Some(digest) = Self::hash_rows(*key, partition)? {
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

    /// Hash a partition's live rows canonically, in sort-key order
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `partition` - The partition
    fn hash_rows(
        key: u64,
        partition: &SortedPartition<R>,
    ) -> Result<Option<PartitionDigest>, ServerError> {
        // every live row re-serialized, in the order the partition keeps them
        let rows: Vec<AlignedVec> = partition
            .rows
            .values()
            .filter_map(|row| match row {
                MaybeRow::Row(row) => Some(RkyvSupport::serialize(row)),
                MaybeRow::Tombstone => None,
            })
            .collect::<Result<_, _>>()?;
        Ok(hash_partition(
            key,
            rows.iter().map(|bytes| bytes.as_slice()),
        ))
    }

    /// Hash one archived partition's bytes canonically
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `bytes` - The record's payload, verified
    fn hash_archived(key: u64, bytes: &[u8]) -> Result<Option<PartitionDigest>, ServerError> {
        // the record is read back as the rows it holds, never hashed as it lies
        let archived = <SortedPartition<R> as RkyvSupport>::access(bytes)?;
        let partition = SortedPartition::<R>::deserialize(archived)?;
        Self::hash_rows(key, &partition)
    }

    /// Every resident partition of some tablets, as its key and archived bytes
    ///
    /// What a volatile group's snapshot is cut from
    /// ([F43](../../../../docs/src/features/node-recovery.md)); a partition with no live row
    /// is not there.
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
                MaybeLoaded::Loaded { partition, .. } => {
                    if !partition
                        .rows
                        .values()
                        .any(|row| matches!(row, MaybeRow::Row(_)))
                    {
                        continue;
                    }
                    rkyv::to_bytes::<rkyv::rancor::Error>(partition).map(|bytes| bytes.to_vec())
                }
                MaybeLoaded::Accessible(read) => Ok(read.as_bytes().to_vec()),
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
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(&bytes);
            let archived = <SortedPartition<R> as RkyvSupport>::access(&aligned)?;
            let partition: SortedPartition<R> =
                <SortedPartition<R> as RkyvSupport>::deserialize(archived)?;
            partitions.push((key, partition));
        }
        self.evict_tablets(tablets);
        let generation = self.generation;
        for (key, partition) in partitions {
            // a record with nothing live in it is absence, not a partition
            if !partition
                .rows
                .values()
                .any(|row| matches!(row, MaybeRow::Row(_)))
            {
                continue;
            }
            let size = partition.size();
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
    #[instrument(name = "PersistentSortedTable::mark_evictable", skip(self, partitions), fields(partition_count = partitions.len()))]
    pub fn mark_evictable(&mut self, generation: u64, partitions: Vec<u64>) {
        let mut marked = 0;
        let mut swept = 0;
        // track how far our data has been compacted
        self.flushed_generation = self.flushed_generation.max(generation);
        // check each partition that we find might be evictable now
        for partition in partitions {
            // try to get this partition
            if let Some(maybe_loaded) = self.partitions.get_mut(&partition) {
                // check if this partition is now evictable
                if maybe_loaded.is_evictable(generation) {
                    // this partitions deletes have been applied to its archive copy, so
                    // any tombstones it still holds have nothing left to shadow
                    if let MaybeLoaded::Loaded { partition, .. } = maybe_loaded {
                        swept += partition.drop_tombstones();
                    }
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
            swept,
            flushed_generation = self.flushed_generation
        );
    }

    /// Evict partitions from memory
    #[instrument(name = "PersistentSortedTable::evict", skip_all, fields(victim_count = victims.len()))]
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

impl<T: ShoalSortedTable + RkyvSupport> IntentReadSupport<T> for SortedPartition<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Sort as Archive>::Archived: rkyv::Deserialize<T::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <T::UpdateData as Archive>::Archived:
        rkyv::Deserialize<T::UpdateData, Strategy<Pool, rkyv::rancor::Error>>,
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
    for<'a> <<T as ShoalTableSupport>::UpdateData as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <<T as ShoalSortedTable>::Sort as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <T as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// A partition with no rows at all
    fn erased(key: u64) -> Self {
        SortedPartition::new(key)
    }

    /// The intent type to use
    type Intent = SortedIntents<T>;

    /// Name the partitions an intent needs loaded before it can be replayed
    ///
    /// # Arguments
    ///
    /// * `read` - The intent to name the partitions of
    /// * `to_load` - The set of partition keys to add to
    fn scan_keys(read: &ReadResult, to_load: &mut HashSet<u64>) -> Result<(), ServerError> {
        // access our data
        let intent = SortedIntents::<T>::access(read)?;
        // only an update needs its base row to already be there
        match intent {
            // we don't need to load inserts or deletes to replay its intent
            ArchivedSortedIntents::Insert(_) | ArchivedSortedIntents::Delete { .. } => (),
            // we need the data loaded in order to update it
            ArchivedSortedIntents::Update(update) => {
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
        let intent = SortedIntents::<T>::access(read)?;
        // add this intent to our btreemap
        let diff = match intent {
            ArchivedSortedIntents::Insert(archived) => {
                // deserialize this row
                let row: T = RkyvSupport::deserialize(archived)?;
                // get the partition key for this row
                let partition_key = row.get_partition_key();
                // apply this intent to the target partition
                let entry =
                    partitions
                        .entry(partition_key)
                        .or_insert_with(|| MaybeLoaded::Loaded {
                            partition: SortedPartition::new(partition_key),
                            generation,
                        });
                match entry {
                    MaybeLoaded::Loaded {
                        partition,
                        generation: partition_gen,
                    } => {
                        // update this loaded partitions generation
                        *partition_gen = generation;
                        // insert this new row
                        let (diff, _) = partition.insert(row);
                        // return the change in memory usage
                        diff
                    }
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can insert this row
                        let mut partition = SortedPartition::<T>::deserialize(accessible)?;
                        // partitions that come from reads never have to go back to disk
                        partition.check_disk = false;
                        //  insert this new row
                        let (diff, _) = partition.insert(row);
                        // update this partition entry
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        // return the change in memory usage
                        diff
                    }
                }
            }
            ArchivedSortedIntents::Delete {
                partition_key,
                sort_key,
            } => {
                // convert our partition key to its native endianess
                let partition_key = partition_key.to_native();
                // deserialize this rows sort key
                let sort_key = rkyv::deserialize::<T::Sort, rkyv::rancor::Error>(sort_key)?;
                // get or create the partition so the tombstone is preserved
                // even if the archive data hasn't been loaded yet
                let entry =
                    partitions
                        .entry(partition_key)
                        .or_insert_with(|| MaybeLoaded::Loaded {
                            partition: SortedPartition::new(partition_key),
                            generation,
                        });
                match entry {
                    MaybeLoaded::Loaded {
                        partition,
                        generation: partition_gen,
                    } => {
                        // update this loaded partitions generation
                        *partition_gen = generation;
                        // insert a tombstone unconditionally so it overlays disk data later
                        partition.tombstone(&sort_key)
                    }
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can tombstone this row
                        let mut partition = SortedPartition::<T>::deserialize(accessible)?;
                        // partitions that come from reads never have to go back to disk
                        partition.check_disk = false;
                        // insert a tombstone unconditionally so it overlays disk data later
                        let diff = partition.tombstone(&sort_key);
                        // update this partition entry
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        // return the change in memory usage
                        diff
                    }
                }
            }
            ArchivedSortedIntents::Update(archived) => {
                // deserialize this row's update
                let update = rkyv::deserialize::<SortedUpdate<T>, rkyv::rancor::Error>(archived)?;
                // try to get the partition containing our target row
                let entry =
                    partitions
                        .entry(update.partition_key)
                        .or_insert_with(|| MaybeLoaded::Loaded {
                            partition: SortedPartition::new(update.partition_key),
                            generation,
                        });
                match entry {
                    MaybeLoaded::Loaded {
                        partition,
                        generation: partition_gen,
                    } => {
                        // update this loaded partitions generation
                        *partition_gen = generation;
                        // apply the update to the target row
                        replay_update(partition, &update, stats)
                    }
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can update this row
                        let mut partition = SortedPartition::<T>::deserialize(accessible)?;
                        // partitions that come from reads never have to go back to disk
                        partition.check_disk = false;
                        // apply the update to the target row
                        let diff = replay_update(&mut partition, &update, stats);
                        // update this partition entry
                        *entry = MaybeLoaded::Loaded {
                            partition,
                            generation,
                        };
                        diff
                    }
                }
            }
        };
        // do a saturating add on our memory usage
        let new_size = memory_usage.borrow().saturating_add_signed(diff);
        // adjust our memory usage correctly
        *memory_usage.borrow_mut() = new_size;
        Ok(())
    }

    /// Apply an intent to this partition
    ///
    /// # Arguments
    ///
    /// * `intent` - The intent to apply to this partition
    /// * `stats` - The counts of what this compaction has discarded
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
        stats: &mut RecoveryStats,
    ) -> ShouldPrune {
        // get this partitions current data or start with an empty one
        let entry = loaded.entry(key).or_insert_with(|| Self::new(key));
        // a delete here removes a row outright rather than tombstoning it, so without
        // remembering which rows this batch deleted an update that correctly lost its
        // row to one of them looks exactly like an update whose insert we lost
        let mut deleted_here = BTreeSet::new();
        // apply each intent to this partition
        for intent in intents {
            match intent {
                SortedIntents::Insert(row) => {
                    // this row is live again, so a later miss on it is not a delete
                    deleted_here.remove(&row.get_sort());
                    entry.insert(row);
                }
                SortedIntents::Delete { sort_key, .. } => {
                    // truly remove during compaction - no tombstone needed since
                    // the new archive won't contain the deleted row
                    entry.rows.remove(&sort_key);
                    // remember that this batch is what took this row
                    deleted_here.insert(sort_key);
                }
                SortedIntents::Update(update) => {
                    // apply this update, and work out what a miss meant if it missed
                    if entry.update(&update).is_none() {
                        if deleted_here.contains(&update.sort_key) {
                            // a delete in this batch took this row, so this costs nothing
                            stats.updates_after_delete += 1;
                        } else {
                            tracing::warn!(
                                "Skipping update intent for missing row in partition {key}"
                            );
                            // this rows insert is gone, so this update is data we lost
                            stats.orphaned_updates += 1;
                        }
                    }
                }
            }
        }
        // if our entry is empty then prune this partition
        if entry.is_empty() {
            // This partition is empty so prune it
            ShouldPrune::Yes
        } else {
            // This partition is not empty so don't prune it
            ShouldPrune::No
        }
    }

    /// Get the partition key for a specific intent
    fn partition_key_and_intent(read: &ReadResult) -> Result<(u64, SortedIntents<T>), ServerError> {
        // try to deserialize this row from our intent log
        let archived = unsafe { rkyv::access_unchecked::<ArchivedSortedIntents<T>>(&read[..]) };
        // deserialize this intent
        let intent = rkyv::deserialize::<SortedIntents<T>, rkyv::rancor::Error>(archived)?;
        // get this intent entries partition key
        let partition_key = match &intent {
            SortedIntents::Insert(row) => row.get_partition_key(),
            SortedIntents::Delete { partition_key, .. } => *partition_key,
            SortedIntents::Update(update) => update.partition_key,
        };
        Ok((partition_key, intent))
    }

    /// Get the partition key for an intent that arrived as bytes, validating them first
    fn partition_key_and_intent_checked(
        bytes: &[u8],
    ) -> Result<(u64, SortedIntents<T>), ServerError>
    where
        for<'a> ArchivedSortedIntents<T>: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // an archive is accessed at its alignment, which a command's payload does not promise
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        // validated, since the bytes crossed a process boundary
        let archived = <Self::Intent as RkyvSupport>::access(&aligned)?;
        let intent = rkyv::deserialize::<SortedIntents<T>, rkyv::rancor::Error>(archived)?;
        let partition_key = match &intent {
            SortedIntents::Insert(row) => row.get_partition_key(),
            SortedIntents::Delete { partition_key, .. } => *partition_key,
            SortedIntents::Update(update) => update.partition_key,
        };
        Ok((partition_key, intent))
    }
}
