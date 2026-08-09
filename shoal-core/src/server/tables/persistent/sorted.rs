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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::{hash_map, HashSet};
use std::hash::BuildHasherDefault;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::server::messages::{LoadedPartition, QueryMetadata, ServerMsg};
use crate::server::tables::partitions::SortedPartition;
use crate::server::tables::persistent::{adjust_memory_usage, eviction_totals, PendingGets};
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::queries::{SortedExists, SortedGet, SortedQuery};
use crate::shared::queries::{SortedUpdate, UnsortedGet};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{
    RkyvSupport, ShoalDatabase, ShoalProjection, ShoalSortedTable, ShoalTableSupport,
    TableNameSupport,
};
use crate::storage::{
    FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse, RecoveryStats,
    ShouldPrune, StorageSupport,
};
use crate::tables::partitions::{MaybeLoaded, MaybeRow, PartitionSupport, ValidatedArchive};

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
    flushed: Vec<(Uuid, Uuid, Span, Response<R>)>,
    /// The channel to send loader jobs on
    loader_tx: AsyncSender<LoaderMsg<N>>,
    /// A map of queries blocked on partitions being loaded from disk
    blocked: HashMap<u64, Vec<(QueryMetadata, SortedQuery<R>)>>,
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
            blocked: HashMap::with_capacity(1000),
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
    ) -> Result<Option<(Vec<(QueryMetadata, SortedQuery<R>)>, u64)>, ServerError> {
        // overlay any existing loaded partition data on this newly loaded partition
        match self.partitions.entry(loaded.partition_id) {
            hash_map::Entry::Occupied(mut entry) => {
                // if this partition is loaded then insert its current rows ontop of this loaded data
                if let MaybeLoaded::Loaded { partition, .. } = entry.get_mut() {
                    // get the current size of this partition
                    let old_size = partition.size();
                    // access our loaded partitions data
                    let accessed = SortedPartition::<R>::access(&loaded.data).unwrap();
                    // deserialize this partition
                    let new = SortedPartition::<R>::deserialize(&accessed).unwrap();
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
            }
            // this partition does not have any already loaded data
            hash_map::Entry::Vacant(entry) => {
                // validate this archive once, here, instead of on every query that reads it
                let archive = hotpath::measure_block!("ValidatedArchive::new", {
                    ValidatedArchive::new(loaded.data)
                })?;
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
        Ok(self
            .blocked
            .remove(&loaded.partition_id)
            .map(|unblocked| (unblocked, self.flushed_generation)))
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
        meta: QueryMetadata,
        query: SortedQuery<R>,
    ) -> Option<(Uuid, Uuid, Response<P>)> {
        // execute the correct query type
        match query {
            // insert a row into this partition
            SortedQuery::Insert { row, .. } => self.insert(meta, row).await,
            // get a row from this partition
            SortedQuery::Get(get) => self.get(meta, &get).await,
            // delete a row from this partition
            SortedQuery::Delete { key, sort_key } => self.delete(meta, key, sort_key).await,
            // update a row in this partition
            SortedQuery::Update(update) => self.update(meta, update).await,
            // check if data exists in this partition
            SortedQuery::Exists(exists) => self.exists(meta, &exists).await,
        }
    }

    /// Insert some data into a partition in this shards table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `row` - The row to insert
    #[instrument(name = "PersistentTable::insert", skip_all)]
    async fn insert<P>(&mut self, meta: QueryMetadata, row: R) -> Option<(Uuid, Uuid, Response<P>)> {
        // get our partition key
        let key = row.get_partition_key();
        // wrap our row in an insert intent
        let intent = SortedIntents::Insert(row);
        // persist this new row to storage
        let pos = self.storage.commit(&intent).await.unwrap();
        // extract our row from our intent
        let row = match intent {
            SortedIntents::Insert(row) => row,
            // SAFETY we just wrapped this in an insert intent before
            _ => unsafe { std::hint::unreachable_unchecked() },
        };
        // get our current partition or start with an empty one
        let entry = self
            .partitions
            .entry(key)
            .or_insert_with(|| MaybeLoaded::Loaded {
                partition: SortedPartition::new(key),
                generation: self.generation,
            });
        // check if we need to convert this to a loaded partition or not
        let (size_diff, action) = match entry {
            MaybeLoaded::Loaded {
                partition,
                generation,
            } => {
                // this partitions newest data is now in the log we are writing to, so
                // it can't be evicted until that log has been compacted
                *generation = self.generation;
                partition.insert(row)
            }
            MaybeLoaded::Accessible(read) => {
                // convert this read to a accessible partition
                let accessable = read.archived();
                // deserialize our accessible partition
                let mut partition = SortedPartition::<R>::deserialize(accessable).unwrap();
                // insert this new row into our loaded partition
                let (size_diff, action) = partition.insert(row);
                // replace our loaded partition
                *entry = MaybeLoaded::Loaded {
                    partition,
                    generation: self.generation,
                };
                (size_diff, action)
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
    ) -> Option<(Uuid, Uuid, Response<P>)> {
        // pick this get up where its last execution left off, or start it fresh
        //
        // a get blocked on a partition is replayed once that partition has been read, so the
        // rows it already found have to outlive the execution that found them
        let mut pending =
            self.pending_data
                .resume::<P>(&(meta.id, meta.index), &get.partition_keys, get.limit);
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
                // try to load this partition from disk if it exists
                let will_load = if self.blocked.contains_key(partition_key) {
                    // a load is already in flight for this partition so queue behind it
                    // instead of reading the same data again
                    true
                } else {
                    self.storage
                        .load_partition(self.table_name, *partition_key, &self.loader_tx)
                        .await
                        .unwrap()
                };
                // a partition being read fills its own slot when this get is replayed for it
                if will_load {
                    // add this query to the list of ones waiting on this partition
                    let entry = self.blocked.entry(*partition_key).or_default();
                    // build a query for just this blocked partition
                    let blocked_get = get.to_blocked(*partition_key);
                    // add our blocked query and its metadata to this partitions blocked list
                    entry.push((meta.clone(), SortedQuery::Get(blocked_get)));
                    // leave this slot empty for the replay to fill
                    continue;
                }
            }
            // get the rows this get asked for from this partition
            //
            // these rows are this partitions alone, so the limit stops each partition after
            // its own first `limit` rows rather than stopping the get at the first partition
            // that happens to fill it
            let mut rows = Vec::default();
            // a partition we have never seen and that is not on disk has no rows to give
            if resident {
                // SAFETY: we looked this partition up above and have not touched the map since
                if let Some(partition) = self.partitions.get(partition_key) {
                    partition.get(get, &mut seek, &mut rows);
                }
            }
            pending.fill(rank, rows);
        }
        // hold this get until every partition it named has been read
        if pending.is_pending() {
            // remember what we have found so far for the replay to carry on from
            self.pending_data.park((meta.id, meta.index), pending);
            // we have blocked partitions so return None
            return None;
        }
        // flatten our slots back into the order this get named its partitions
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
        Some((meta.client, meta.id, response))
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
    ) -> Option<(Uuid, Uuid, Response<P>)> {
        // pick up the partitions this exists is still waiting on, or start it fresh
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
                // try to load this partition from disk if it exists
                let will_load = if self.blocked.contains_key(partition_key) {
                    // a load is already in flight for this partition so queue behind it
                    // instead of reading the same data again
                    true
                } else {
                    self.storage
                        .load_partition(self.table_name, *partition_key, &self.loader_tx)
                        .await
                        .unwrap()
                };
                // a partition being read is answered about when this exists is replayed for it
                if will_load {
                    // add this query to the list of ones waiting on this partition
                    let entry = self.blocked.entry(*partition_key).or_default();
                    // build a query for just this blocked partition
                    let blocked_exists = exists_query.to_blocked(*partition_key);
                    // add our blocked query and its metadata to this partitions blocked list
                    entry.push((meta.clone(), SortedQuery::Exists(blocked_exists)));
                    // remember that we are still waiting on this partition
                    blocked.push(*partition_key);
                    continue;
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
                        return Some((meta.client, meta.id, response));
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
        Some((meta.client, meta.id, response))
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
        meta: QueryMetadata,
        key: u64,
        sort: R::Sort,
    ) -> Option<(Uuid, Uuid, Response<P>)> {
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
                        if let Some((size_diff, _)) = partition.remove(&sort) {
                            // we were able to delete this row so build the delete intent
                            let intent = SortedIntents::<R>::delete(key, sort);
                            // commit it to the intent to the intent log
                            let pos = self.storage.commit(&intent).await.unwrap();
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
                            let will_load = if self.blocked.contains_key(&key) {
                                // a load is already in flight for this partition so queue
                                // behind it instead of reading the same data again
                                true
                            } else {
                                self.storage
                                    .load_partition(self.table_name, key, &self.loader_tx)
                                    .await
                                    .unwrap()
                            };
                            // check if this partition has any on disk data to load
                            if will_load {
                                // this partition has on disk data so block this query
                                // until its loaded and then retry
                                let entry = self.blocked.entry(key).or_default();
                                // add this query to our blocked queries
                                entry.push((
                                    meta,
                                    SortedQuery::Delete {
                                        key,
                                        sort_key: sort,
                                    },
                                ));
                                // theres nothing to respond with yet
                                return None;
                            }
                        }
                        // Tthis row doesn't exist and so can't be deleted
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Delete(false),
                            end: meta.end,
                        };
                        Some((meta.client, meta.id, response))
                    }
                    // this partition is loaded from disk but not deserialized
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can modify it
                        let mut partition = SortedPartition::<R>::deserialize(accessible).unwrap();
                        // since this partition is accessible it must have been the full partition from disk
                        // so we don't need to check disk again
                        partition.check_disk = false;
                        // try to remove the target row
                        if let Some((size_diff, _)) = partition.remove(&sort) {
                            // we were able to delete this row so build the delete intent
                            let intent = SortedIntents::<R>::delete(key, sort);
                            // commit it to the intent to the intent log
                            let pos = self.storage.commit(&intent).await.unwrap();
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
                            Some((meta.client, meta.id, response))
                        }
                    }
                }
            }
            None => {
                // we don't have this partition loaded so try to load it
                let will_load = if self.blocked.contains_key(&key) {
                    // a load is already in flight for this partition so queue behind it
                    // instead of reading the same data again
                    true
                } else {
                    self.storage
                        .load_partition(self.table_name, key, &self.loader_tx)
                        .await
                        .unwrap()
                };
                // this partition exists and is being loaded
                if will_load {
                    // get an entry to this partitions blocked queries
                    let entry = self.blocked.entry(key).or_default();
                    // add this to our blocked queries
                    entry.push((
                        meta,
                        SortedQuery::Delete {
                            key,
                            sort_key: sort,
                        },
                    ));
                    None
                } else {
                    // build the failed delete response
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data: ResponseAction::Delete(false),
                        end: meta.end,
                    };
                    Some((meta.client, meta.id, response))
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
        meta: QueryMetadata,
        update: SortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, Response<P>)> {
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
                        // update the target row if its loaded
                        if let Some(diff) = partition.update(&update) {
                            // we were able to update this partition so get its key
                            let key = update.partition_key;
                            // wrap our update in an update intent
                            let intent = SortedIntents::<R>::update(update);
                            // commit this intent to storage
                            let pos = self.storage.commit(&intent).await.unwrap();
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
                            let will_load = if self.blocked.contains_key(&update.partition_key) {
                                // a load is already in flight for this partition so queue
                                // behind it instead of reading the same data again
                                true
                            } else {
                                self.storage
                                    .load_partition(
                                        self.table_name,
                                        update.partition_key,
                                        &self.loader_tx,
                                    )
                                    .await
                                    .unwrap()
                            };
                            // if we are going to load it from disk add this query to our blocked queries
                            if will_load {
                                // get an entry to this partitions blocked queries
                                let entry = self.blocked.entry(update.partition_key).or_default();
                                // add this to our blocked queries
                                entry.push((meta, SortedQuery::Update(update)));
                                // wait for this partition to get loaded
                                return None;
                            }
                        }
                        // this partition doesn't exist in disk or in memory
                        let response = Response {
                            id: meta.id,
                            index: meta.index,
                            data: ResponseAction::Update(false),
                            end: meta.end,
                        };
                        Some((meta.client, meta.id, response))
                    }
                    // this partition is loaded from disk but not deserialized
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = read.archived();
                        // deserialize our partition so we can update it
                        let mut partition = SortedPartition::<R>::deserialize(accessible).unwrap();
                        // try to update this partitions data
                        if let Some(diff) = partition.update(&update) {
                            // we were able to update this partition so get its key
                            let key = update.partition_key;
                            // wrap our update in an update intent
                            let intent = SortedIntents::<R>::update(update);
                            // commit this intent to storage
                            let pos = self.storage.commit(&intent).await.unwrap();
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
                            Some((meta.client, meta.id, response))
                        }
                    }
                }
            }
            None => {
                // we don't have this partition loaded so try to load it
                let will_load = if self.blocked.contains_key(&update.partition_key) {
                    // a load is already in flight for this partition so queue behind it
                    // instead of reading the same data again
                    true
                } else {
                    self.storage
                        .load_partition(self.table_name, update.partition_key, &self.loader_tx)
                        .await
                        .unwrap()
                };
                // this partition exists and is being loaded
                if will_load {
                    // get an entry to this partitions blocked queries
                    let entry = self.blocked.entry(update.partition_key).or_default();
                    // add this to our blocked queries
                    entry.push((meta, SortedQuery::Update(update)));
                    None
                } else {
                    // Partition doesn't exist - update fails
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data: ResponseAction::Update(false),
                        end: meta.end,
                    };
                    // wait for this partition to get loaded
                    Some((meta.client, meta.id, response))
                }
            }
        }
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
    ) -> Result<&mut Vec<(Uuid, Uuid, Span, Response<R>)>, ServerError> {
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
                        let mut partition = SortedPartition::<T>::deserialize(accessible).unwrap();
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
                        let mut partition = SortedPartition::<T>::deserialize(accessible).unwrap();
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
                        let mut partition = SortedPartition::<T>::deserialize(accessible).unwrap();
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
}
