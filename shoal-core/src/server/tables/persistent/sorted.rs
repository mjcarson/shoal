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
use std::hash::BuildHasherDefault;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::server::messages::{LoadedPartition, QueryMetadata, ServerMsg};
use crate::server::tables::partitions::SortedPartition;
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::queries::{SortedExists, SortedGet, SortedQuery};
use crate::shared::queries::{SortedUpdate, UnsortedGet};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{
    RkyvSupport, ShoalDatabase, ShoalSortedTable, ShoalTableSupport, TableNameSupport,
};
use crate::storage::{
    FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse, ShouldPrune,
    StorageSupport,
};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};

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
    /// The current generation of flushed data
    generation: u64,
    /// The commits that are still pending storage confirmation
    pending: PendingResponse<R>,
    /// The response data for queries that needed partitions to be loaded from disk
    pending_data: HashMap<(Uuid, usize), (Vec<R>, Vec<u64>)>,
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
}

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
    #[instrument(name = "PersistentTable::new", skip(conf), err(Debug))]
    pub async fn new<D: ShoalDatabase>(
        shard_name: &str,
        table_name: N,
        shard_table_name: D::TableNames,
        shard_archive_map: &FullArchiveMap<N>,
        loader_channels: &mut HashMap<
            Loaders,
            (AsyncSender<LoaderMsg<N>>, AsyncReceiver<LoaderMsg<N>>),
        >,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        memory_usage: &Arc<RefCell<usize>>,
        lru: &Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<GxHasher>>>>,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
    ) -> Result<Self, ServerError> {
        // make sure we have a loader channel for filesystems
        let (loader_tx, _) = loader_channels
            .entry(S::loader_kind())
            .or_insert_with(|| kanal::unbounded_async());
        // build our table
        let mut table = Self {
            table_name,
            partitions: HashMap::with_capacity(1000),
            storage: S::new::<SortedPartition<R>, R, N, D>(
                shard_name,
                table_name,
                shard_table_name,
                shard_archive_map,
                conf,
                medium_priority,
                shard_local_tx,
            )
            .await?,
            generation: 0,
            pending: PendingResponse::<R>::with_capacity(100),
            pending_data: HashMap::with_capacity(500),
            flushed: Vec::with_capacity(1000),
            loader_tx: loader_tx.clone(),
            blocked: HashMap::with_capacity(1000),
            memory_usage: memory_usage.clone(),
            lru: lru.clone(),
        };
        // load our intent log
        table
            .storage
            .read_intents(
                conf,
                table.generation,
                &mut table.partitions,
                &mut table.memory_usage,
            )
            .await?;
        // compact our intent log
        table.storage.compact_if_needed::<R>(true).await?;
        Ok(table)
    }

    /// Get the storage engine kind
    pub fn loader_kind(&self) -> Loaders {
        S::loader_kind()
    }

    /// Spawn the loader for this storage engine type
    pub async fn spawn_loader<D: ShoalDatabase>(
        &self,
        table_map: &FullArchiveMap<D::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
    ) -> Result<(), ServerError> {
        // spawn the loader for our storage engine
        self.storage
            .spawn_loader(&table_map, loader_rx, shard_local_tx)
            .await
    }

    /// Load this partition from disk if needed
    pub async fn load_partition(
        &mut self,
        loaded: LoadedPartition,
    ) -> Option<(Vec<(QueryMetadata, SortedQuery<R>)>, u64)> {
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
                    let mut new = SortedPartition::<R>::deserialize(&accessed).unwrap();
                    // swap our loaded partition with our existing one so we can repaly it ontop
                    std::mem::swap(&mut new, partition);
                    // replay any rows from our current partition onto our loaded one
                    partition.rows.extend(new.rows.into_iter());
                    // mark this partition as no longer needing to check disk since we just loaded it
                    partition.check_disk = false;
                    // calculate the difference in our partition size
                    let diff = partition.size() as isize - old_size as isize;
                    // increment or decrement our memory usage
                    if diff.is_positive() {
                        // our partition got larger so increase our memory usage
                        *self.memory_usage.borrow_mut() += diff as usize;
                    } else {
                        // our partition got smaller so decrease our memory usage
                        let new_mem_usage =
                            self.memory_usage.borrow().saturating_sub(diff as usize);
                        // updat our memory usage
                        *self.memory_usage.borrow_mut() = new_mem_usage;
                    }
                    // remove this partition from our cache until any blocked queries have completed
                    self.lru
                        .borrow_mut()
                        .pop(&(self.table_name, loaded.partition_id));
                }
            }
            // this partition does not have any already loaded data
            hash_map::Entry::Vacant(entry) => {
                // get the size of our dat
                let size = loaded.data.len();
                // wrap our raw data so that we can access it only when needed
                let wrapped = MaybeLoaded::Accessible(loaded.data);
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
        self.blocked
            .remove(&loaded.partition_id)
            .map(|unblocked| (unblocked, self.generation))
    }

    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `query` - The query to execute
    #[instrument(name = "PersistentTable::handle", skip(self, query))]
    pub async fn handle(
        &mut self,
        meta: QueryMetadata,
        query: SortedQuery<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
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
    async fn insert(&mut self, meta: QueryMetadata, row: R) -> Option<(Uuid, Uuid, Response<R>)> {
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
            MaybeLoaded::Loaded { partition, .. } => partition.insert(row),
            MaybeLoaded::Accessible(read) => {
                // convert this read to a accessible partition
                let accessable = SortedPartition::<R>::access(&read).unwrap();
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
    async fn get(
        &mut self,
        meta: QueryMetadata,
        get: &SortedGet<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        // get any dat from previously executed/blocked queries
        let (mut data, mut blocked) = match self.pending_data.remove(&(meta.id, meta.index)) {
            // use our existing data/blocked queries
            Some((data, blocked)) => (data, blocked),
            // this query has never been executed before so instance sane defaults
            None => (Vec::with_capacity(get.partition_keys.len()), Vec::default()),
        };
        // check each of the specified partition keys
        for partition_key in &get.partition_keys {
            // try to get the partition for this key
            match self.partitions.get(partition_key) {
                // this partition may be loaded into memory
                Some(partition) => {
                    // if this partition is accessible then we don't need to check disk
                    match partition {
                        MaybeLoaded::Loaded { partition, .. } => {
                            // load this partitions data from disk if needed
                            if partition.check_disk {
                                // try to load this partition from disk if it exists
                                let will_load = self
                                    .storage
                                    .load_partition(
                                        self.table_name,
                                        *partition_key,
                                        &self.loader_tx,
                                    )
                                    .await
                                    .unwrap();
                                // if this query was blocked then add it to our blocked list
                                if will_load {
                                    // if we need to load this then add this query to a map of queries
                                    // that are blocked on partitions being loaded from disk
                                    // get an entry to our partitions blocked queries
                                    let entry = self.blocked.entry(*partition_key).or_default();
                                    // build a query for just this blocked partition
                                    let blocked_get = get.to_blocked(*partition_key);
                                    // add our blocked query and its metadata for this partitions blocked query list
                                    entry.push((meta.clone(), SortedQuery::Get(blocked_get)));
                                    // add this partition to our blocked partition list
                                    blocked.push(*partition_key);
                                    // continue to the next partition key
                                    continue;
                                }
                            }
                            // get the live rows from our partition (tombstones are skipped)
                            for row in partition.live_row_values() {
                                // check if we are supposed to filter our rows
                                if let Some(filters) = &get.filters {
                                    // check if this row should be returned
                                    if !R::is_filtered(filters, row) {
                                        // skip this row as it doesn't match our filters
                                        continue;
                                    }
                                }
                                // add this row to our response
                                data.push(row.clone());
                            }
                        }
                        MaybeLoaded::Accessible(read) => {
                            // This partition is accessible so it must have come from disk
                            // we don't need to check it just access it
                            let partition = SortedPartition::<R>::access(&read).unwrap();
                            // get the live rows from our partition (tombstones are skipped)
                            for row in partition.live_row_values() {
                                // check if we are supposed to filter our rows
                                if let Some(filters) = &get.filters {
                                    // check if this row should be returned
                                    if !R::is_filtered_archived(filters, row) {
                                        // skip this row as it doesn't match our filters
                                        continue;
                                    }
                                }
                                // convert this to a loaded row
                                let loaded_row = R::deserialize(row).unwrap();
                                // add this row to our response
                                data.push(loaded_row);
                            }
                        }
                    }
                    // remove this partition from our blocked queries
                    blocked.retain(|key| key != partition_key);
                }
                // this partition is not loaded into memory
                // check if this partition exist and load it if it does
                None => {
                    // try to load this partition from disk if it exists
                    let will_load = self
                        .storage
                        .load_partition(self.table_name, *partition_key, &self.loader_tx)
                        .await
                        .unwrap();
                    // if this query was blocked then add it to our blocket list
                    if will_load {
                        // if we need to load this then add this query to a map of queries
                        // that are blocked on partitions being loaded from disk
                        // get an entry to our partitions blocked queries
                        let entry = self.blocked.entry(*partition_key).or_default();
                        // build a query for just this blocked partition
                        let blocked_get = get.to_blocked(*partition_key);
                        // add our blocked query and its metadata for this partitions
                        // blocked query list
                        entry.push((meta.clone(), SortedQuery::Get(blocked_get)));
                        // add this partition to our blocked partition list
                        blocked.push(*partition_key);
                    }
                }
            }
        }
        // if we have any blocked queries then add this to our pending data map
        if !blocked.is_empty() {
            // get an entry to this queries pending data
            self.pending_data
                .insert((meta.id, meta.index), (data, blocked));
            // we have blocked queries so return None
            None
        } else {
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
    }

    /// Check if data exists in some partitions
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this exists query
    /// * `exists` - The exists parameters to use
    #[instrument(name = "PersistentTable::exists", skip_all)]
    async fn exists(
        &mut self,
        meta: QueryMetadata,
        exists_query: &SortedExists<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        // get any data from previously executed/blocked queries
        let mut blocked = match self.pending_data.remove(&(meta.id, meta.index)) {
            // use our existing blocked queries
            Some((_, blocked)) => blocked,
            // this query has never been executed before so instance sane defaults
            None => Vec::default(),
        };
        println!("## Exists {exists_query:?} -> {blocked:?}");
        // check each of the specified partition keys
        for partition_key in &exists_query.partition_keys {
            // try to get the partition for this key
            match self.partitions.get(partition_key) {
                // this partition may be loaded into memory
                Some(partition) => {
                    println!("SOME -> {partition:?}");
                    // if this partition is accessible then we don't need to check disk
                    match partition {
                        MaybeLoaded::Loaded { partition, .. } => {
                            // load this partitions data from disk if needed
                            if partition.check_disk {
                                // try to load this partition from disk if it exists
                                let will_load = self
                                    .storage
                                    .load_partition(
                                        self.table_name,
                                        *partition_key,
                                        &self.loader_tx,
                                    )
                                    .await
                                    .unwrap();
                                // if this query was blocked then add it to our blocked list
                                if will_load {
                                    let entry = self.blocked.entry(*partition_key).or_default();
                                    let blocked_exists = exists_query.to_blocked(*partition_key);
                                    entry.push((meta.clone(), SortedQuery::Exists(blocked_exists)));
                                    blocked.push(*partition_key);
                                    continue;
                                }
                            }
                            // check the rows from our partition
                            for row in partition.live_row_values() {
                                // check if we are supposed to filter our rows
                                if let Some(filters) = &exists_query.filters {
                                    if !R::is_filtered(filters, row) {
                                        continue;
                                    }
                                }
                                // found a matching row - data exists
                                let response = Response {
                                    id: meta.id,
                                    index: meta.index,
                                    data: ResponseAction::Exists(true),
                                    end: meta.end,
                                };
                                println!("%% partition_lo -> {partition:#?}");
                                println!("@@ exists {exists_query:?} -> true");
                                return Some((meta.client, meta.id, response));
                            }
                        }
                        MaybeLoaded::Accessible(read) => {
                            let partition = SortedPartition::<R>::access(&read).unwrap();
                            for row in partition.live_row_values() {
                                if let Some(filters) = &exists_query.filters {
                                    if !R::is_filtered_archived(filters, row) {
                                        continue;
                                    }
                                }
                                // found a matching row - data exists
                                let response = Response {
                                    id: meta.id,
                                    index: meta.index,
                                    data: ResponseAction::Exists(true),
                                    end: meta.end,
                                };
                                println!("@@ exists {exists_query:?} -> true");
                                return Some((meta.client, meta.id, response));
                            }
                        }
                    }
                    // remove this partition from our blocked queries
                    blocked.retain(|key| key != partition_key);
                }
                // this partition is not loaded into memory
                None => {
                    let will_load = self
                        .storage
                        .load_partition(self.table_name, *partition_key, &self.loader_tx)
                        .await
                        .unwrap();
                    if will_load {
                        let entry = self.blocked.entry(*partition_key).or_default();
                        let blocked_exists = exists_query.to_blocked(*partition_key);
                        entry.push((meta.clone(), SortedQuery::Exists(blocked_exists)));
                        blocked.push(*partition_key);
                    }
                }
            }
        }
        // if we have any blocked queries then add this to our pending data map
        if !blocked.is_empty() {
            println!("## Still have blocked! -> {blocked:?}");
            self.pending_data
                .insert((meta.id, meta.index), (Vec::new(), blocked));
            None
        } else {
            // no data found in any partition
            let response = Response {
                id: meta.id,
                index: meta.index,
                data: ResponseAction::Exists(false),
                end: meta.end,
            };
            println!("@@ exists {exists_query:?} -> false");
            Some((meta.client, meta.id, response))
        }
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
    async fn delete(
        &mut self,
        meta: QueryMetadata,
        key: u64,
        sort: R::Sort,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        println!("~~ DELETING {key:?} / {sort:?}");
        // get the partition we want to delete from
        match self.partitions.get_mut(&key) {
            Some(maybe_loaded) => {
                println!("DEL SOM?");
                // check if this partition is fully loaded in memory or not
                match maybe_loaded {
                    // the partition is at least partially deserialized and loaded into memory
                    MaybeLoaded::Loaded { partition, .. } => {
                        println!("DEL MAYBE_LOADED?");
                        // try to remove the target row
                        if let Some((size_diff, _)) = partition.remove(&sort) {
                            println!("DEL MAYBE_LOADED SOME? -> {partition:#?}");
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
                            // we can't acknowledge this delete until its intent is flushed
                            return None;
                        } else if partition.check_disk {
                            println!("DEL MAYBE_LOADED CHECK_DISK?");
                            // we couldn't find the row to delete but it may be on on disk
                            let will_load = self
                                .storage
                                .load_partition(self.table_name, key, &self.loader_tx)
                                .await
                                .unwrap();
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
                        println!("~~ DELETING POST_LO: {partition:#?}");
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
                        println!("~~ DELETING {key:?} / {sort:?} - ACCESIBLE");
                        // access this partitions data
                        let accessible = SortedPartition::<R>::access(&read).unwrap();
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
                            println!("^^ partition -> {partition:#?}");
                            // convert to Loaded state since we've deserialized it
                            *maybe_loaded = MaybeLoaded::Loaded {
                                partition,
                                generation: self.generation,
                            };
                            // we can't acknowledge this delete until its intent is flushed
                            println!("~~ DELETING {key:?}  - NEED FLUSH!");
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
                            println!("~~ DELETING {key:?} / {sort:?} - NOTHING TO DO");
                            Some((meta.client, meta.id, response))
                        }
                    }
                }
            }
            None => {
                // we don't have this partition loaded so try to load it
                let will_load = self
                    .storage
                    .load_partition(self.table_name, key, &self.loader_tx)
                    .await
                    .unwrap();
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
                    println!("~~ DELETING EMPTY?");
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
    async fn update(
        &mut self,
        meta: QueryMetadata,
        update: SortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        // get the partition we want to update
        match self.partitions.get_mut(&update.partition_key) {
            Some(maybe_loaded) => {
                // check if this partition is fully loaded in memory or not
                match maybe_loaded {
                    // the partition is at least partialy deserialzied and loaded into memory
                    MaybeLoaded::Loaded { partition, .. } => {
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
                            // remove this partition from our lru cache as its no longer evictable
                            self.lru.borrow_mut().pop(&(self.table_name, key));
                            // do a saturating add on our memory usage
                            let new_size = self.memory_usage.borrow().saturating_add_signed(diff);
                            // adjust our total shards memory usage
                            *self.memory_usage.borrow_mut() = new_size;
                            return None;
                        } else if partition.check_disk {
                            // we don't have this partition loaded so try to load it from disk
                            let will_load = self
                                .storage
                                .load_partition(
                                    self.table_name,
                                    update.partition_key,
                                    &self.loader_tx,
                                )
                                .await
                                .unwrap();
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
                        let accessible = SortedPartition::<R>::access(&read).unwrap();
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
                let will_load = self
                    .storage
                    .load_partition(self.table_name, update.partition_key, &self.loader_tx)
                    .await
                    .unwrap();
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
    #[instrument(name = "PersistentSortedTable::mark_evictable", skip(self, partitions), fields(partition_count = partitions.len()))]
    pub fn mark_evictable(&mut self, generation: u64, partitions: Vec<u64>) {
        let mut marked = 0;
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
        event!(Level::INFO, marked);
    }

    /// Evict partitions from memory
    #[instrument(name = "PersistentSortedTable::evict", skip_all, fields(victim_count = victims.len()))]
    pub fn evict(&mut self, victims: Vec<u64>) {
        // get our current memory usage
        let pre = *self.memory_usage.borrow();
        // step over and remove all of our victim partitions
        for victim in victims {
            // remove this partition if it exists
            if let Some(partition) = self.partitions.remove(&victim) {
                // get our new memory usage amount with this partition removed
                let decreased = self.memory_usage.borrow().saturating_sub(partition.size());
                // update our memory usage
                *self.memory_usage.borrow_mut() = decreased;
            }
        }
        // get our post eviction memory usage
        let post = *self.memory_usage.borrow();
        // log the change in memory usage
        event!(
            Level::INFO,
            pre,
            post,
            diff = pre - post,
            partitions = self.partitions.len(),
            evictable = self.lru.borrow().len(),
        );
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<(), ServerError> {
        self.storage.flush().await
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
        let (flushed_pos, generation) = self.storage.compact_if_needed::<R>(false).await?;
        // update our current generation
        self.generation = generation;
        // get all of the responses whose data has been flushed to disk
        self.pending.get(flushed_pos, &mut self.flushed);
        // return a ref to our flushed responses
        Ok(&mut self.flushed)
    }

    /// Shutdown this table
    #[instrument(name = "PersistentTable::shutdown", skip_all)]
    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
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

    /// Load an intent logs partition from disk if its needed to replay this intent log
    ///
    /// # Arguments
    ///
    /// * `read` - The intent to scan for partitions to load
    /// * `storage` - The storage engine to load data from
    /// * `partitions` - The partition map to load our partitions into
    /// * `memory_usage` - The current memory usage for this shard
    async fn scan<S: StorageSupport>(
        read: &ReadResult,
        storage: &S,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<(), ServerError> {
        // access our data
        let intent = SortedIntents::<T>::access(read)?;
        // build a set of partitions to load from disk
        let mut to_load = HashSet::with_capacity(1000);
        // we only need to load partitions for delete intents
        match intent {
            // we don't need to load inserts or deletes to replay its intent
            ArchivedSortedIntents::Insert(_) | ArchivedSortedIntents::Delete { .. } => (),
            // we need the data loaded in order to update it
            ArchivedSortedIntents::Update(update) => {
                to_load.insert(update.partition_key.to_native());
            }
        }
        // load all of our partitions
        for partition_key in to_load {
            // get this partitions data
            if let Some(partition_read) = storage.load_partition_direct(partition_key).await? {
                // update the memory usage for this partition
                *memory_usage.borrow_mut() += partition_read.len();
                // wrap this partition as being accessible
                let wrapped = MaybeLoaded::Accessible(partition_read);
                // load this partition
                partitions.insert(partition_key, wrapped);
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
    fn replay(
        read: &ReadResult,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<(), ServerError> {
        // access our data
        let intent = SortedIntents::<T>::access(read)?;
        println!("pre_load -> {partitions:#?}");
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
                        let accessible = SortedPartition::<T>::access(&read).unwrap();
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
                        let accessible = SortedPartition::<T>::access(&read).unwrap();
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
                        partition.update(&update).unwrap_or(0)
                    }
                    MaybeLoaded::Accessible(read) => {
                        // access this partitions data
                        let accessible = SortedPartition::<T>::access(&read).unwrap();
                        // deserialize our partition so we can update this row
                        let mut partition = SortedPartition::<T>::deserialize(accessible).unwrap();
                        // partitions that come from reads never have to go back to disk
                        partition.check_disk = false;
                        // apply the update to the target row
                        let diff = partition.update(&update).unwrap_or(0);
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
        println!("post_load -> {partitions:#?}");
        Ok(())
    }

    /// Apply an intent to this partition
    ///
    /// # Arguments
    ///
    /// * `intent` - The intent to apply to this partition
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
    ) -> ShouldPrune {
        // get this partitions current data or start with an empty one
        let entry = loaded.entry(key).or_insert_with(|| Self::new(key));
        // apply each intent to this partition
        for intent in intents {
            match intent {
                SortedIntents::Insert(row) => {
                    entry.insert(row);
                }
                SortedIntents::Delete { sort_key, .. } => {
                    // truly remove during compaction - no tombstone needed since
                    // the new archive won't contain the deleted row
                    entry.rows.remove(&sort_key);
                }
                SortedIntents::Update(update) => {
                    entry.update(&update);
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
