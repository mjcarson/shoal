//! A sorted table in  Shoal where each partition can contain multiple sorted rows

use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
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
use std::collections::hash_map;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::server::messages::{LoadedPartition, QueryMetadata, ServerMsg};
use crate::server::tables::partitions::SortedPartition;
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::queries::{SortedGet, SortedQuery};
use crate::shared::queries::{SortedUpdate, UnsortedGet};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{RkyvSupport, ShoalDatabase, ShoalSortedTable, TableNameSupport};
use crate::storage::{
    FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse, ShouldPrune,
    StorageSupport,
};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(u8)]
pub enum SortedIntents<T: ShoalSortedTable> {
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
    lru: Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<Xxh3>>>>,
}

impl<R: ShoalSortedTable + 'static, S: StorageSupport, N: TableNameSupport>
    PersistentSortedTable<R, S, N>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
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
    <<R as ShoalSortedTable>::Update as Archive>::Archived:
        rkyv::Deserialize<<R as ShoalSortedTable>::Update, Strategy<Pool, rkyv::rancor::Error>>,
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
    for<'a> <<SortedPartition<R> as IntentReadSupport<R>>::Intent as Archive>::Archived:
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
        lru: &Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<Xxh3>>>>,
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
        S::read_intents::<SortedPartition<R>, R>(
            shard_name,
            conf,
            table.generation,
            &mut table.partitions,
            &mut table.memory_usage,
        )
        .await?;
        // compact our intent log
        table.storage.compact_if_needed::<R>(true).await?;
        Ok(table)
        //// build our table
        //let mut table = Self {
        //    partitions: HashMap::default(),
        //    storage: S::new::<SortedPartition<R>, R, N>(shard_name, conf, medium_priority).await?,
        //    pending: PendingResponse::<R>::with_capacity(100),
        //    memory_usage: 0,
        //    flushed: Vec::with_capacity(1000),
        //};
        //// load our intent log
        //S::read_intents::<SortedPartition<R>, R>(shard_name, conf, &mut table.partitions).await?;
        //Ok(table)
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
                                }
                            } else {
                                // get the rows from our partition
                                for (_, row) in &partition.rows {
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
                        }
                        MaybeLoaded::Accessible(read) => {
                            // This partition is accessible so it must have come from disk
                            // we don't need to check it just access it
                            let partition = SortedPartition::<R>::access(&read).unwrap();
                            // get the rows from our partition
                            for (_, row) in partition.rows.iter() {
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

    /// Delete a row from this table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this delete query
    /// * `key` - The key to the partition to dlete data from
    /// * `sort` - The sort key to delete
    #[instrument(name = "PersistentTable::delete", skip_all)]
    async fn delete(
        &mut self,
        meta: QueryMetadata,
        key: u64,
        sort: R::Sort,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        unimplemented!("DELETE NEEDS TOMBSTONES OR SOMETHING SIMILAR!");
        //// get the partition we are deleting data from
        //match self.partitions.entry(key) {
        //    // we have some of this partition loaded
        //    Entry::Occupied(mut entry) => {
        //        // get a mutable ref to this partitions data
        //        let value = entry.get_mut();
        //        // handle loaded or accessible partitions
        //        match value {
        //            MaybeLoaded::Loaded { partition, .. } => {
        //                // try to remove the target row
        //                match partition.remove(&sort) {
        //                    Some(diff) => panic!("GOT DIFF"),
        //                    // we don't have this partition loaded so just write
        //                    // a
        //                    None =>
        //                }
        //            }
        //            MaybeLoaded::Accessible(read) => panic!("read"),
        //        }
        //    }
        //    Entry::Vacant(vacant) => panic!("Vacant"),
        //};
        ////Some(MaybeLoaded::Loaded { partition, .. }) => partition.remove(&sort),
        ////{
        ////    // try remove the target row from this partition
        ////    if let Some((size_diff, _)) = partition.remove(&sort) {
        ////        // wrap our row in an delete intent
        ////        let intent = SortedIntents::<R>::delete(key, sort);
        ////        // wite this delete to our intent log
        ////        let pos = self.storage.commit(&intent).await.unwrap();
        ////        // build the pending action to store
        ////        let action = ResponseAction::Delete(true);
        ////        // add this action to our pending queue
        ////        self.pending.add(meta, pos, action);
        ////        // do a saturating add on our memory usage
        ////        let new_size = self.memory_usage.borrow().saturating_sub(size_diff);
        ////        // adjust our total shards memory usage
        ////        *self.memory_usage.borrow_mut() = new_size;
        ////        // remove this partition from our lru cache as its no longer evictable
        ////        self.lru.borrow_mut().pop(&(self.table_name, key));
        ////        // wait for this delete to get flushed to disk
        ////        return None;
        ////    }
        ////}
        ////Some(MaybeLoaded::Accessible(read)) => {
        ////    // convert this read to a accessible partition
        ////    let accessable = SortedPartition::<R>::access(&read).unwrap();
        ////    // deserialize our accessible partition
        ////    let mut partition = SortedPartition::<R>::deserialize(accessable).unwrap();
        ////    // try to remove this data from this partition
        ////    let diff = partition.remove(&sort);
        ////    // insert
        ////    //// try remove the target row from this partition
        ////    //if let Some((size_diff, _)) = partition.remove(&sort) {
        ////    //    // wrap our row in an delete intent
        ////    //    let intent = SortedIntents::<R>::delete(key, sort);
        ////    //    // wite this delete to our intent log
        ////    //    let pos = self.storage.commit(&intent).await.unwrap();
        ////    //    // build the pending action to store
        ////    //    let action = ResponseAction::Delete(true);
        ////    //    // add this action to our pending queue
        ////    //    self.pending.add(meta, pos, action);
        ////    //    // adjust this shards total memory usage
        ////    //    self.memory_usage = self.memory_usage.saturating_sub(size_diff);
        ////    //    // wait for this delete to get flushed to disk
        ////    //    return None;
        ////    //}
        ////}
        ////None => {
        ////    //// we didn't find any data to delete
        ////    //let action = ResponseAction::Delete(false);
        ////    //// cast this action to a response
        ////    //let response = Response {
        ////    //    id: meta.id,
        ////    //    index: meta.index,
        ////    //    data: action,
        ////    //    end: meta.end,
        ////    //};
        ////    //Some((meta.client, response))
        ////}
        ////}
        //// get this rows partition
        //if let Some(partition) = self.partitions.get_mut(&key) {
        //    // try remove the target row from this partition
        //    if let Some((size_diff, _)) = partition.remove(&sort) {
        //        // wrap our row in an delete intent
        //        let intent = SortedIntents::<R>::delete(key, sort);
        //        // wite this delete to our intent log
        //        let pos = self.storage.commit(&intent).await.unwrap();
        //        // build the pending action to store
        //        let action = ResponseAction::Delete(true);
        //        // add this action to our pending queue
        //        self.pending.add(meta, pos, action);
        //        // adjust this shards total memory usage
        //        self.memory_usage = self.memory_usage.saturating_sub(size_diff);
        //        // wait for this delete to get flushed to disk
        //        return None;
        //    }
        //}
        //// we didn't find any data to delete
        //let action = ResponseAction::Delete(false);
        //// cast this action to a response
        //let response = Response {
        //    id: meta.id,
        //    index: meta.index,
        //    data: action,
        //    end: meta.end,
        //};
        //Some((meta.client, response))
    }

    /// Update a row in this table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `update` - The update to apply to a row in this table
    #[instrument(name = "PersistentTable::update", skip_all)]
    async fn update(
        &mut self,
        meta: QueryMetadata,
        update: SortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        unimplemented!("Need update support")
        //// get this rows partition
        //if let Some(partition) = self.partitions.get_mut(&update.partition_key) {
        //    if partition.update(&update) {
        //        // wrap our update in an intent
        //        let intent = SortedIntents::update(update);
        //        // write this update to storage
        //        let pos = self.storage.commit(&intent).await.unwrap();
        //        // we didn't find any data to update
        //        let action = ResponseAction::Update(false);
        //        // add this action to our pending queue
        //        self.pending.add(meta, pos, action);
        //        // TODO: adjust this shards memory usage?
        //        // wait for this delete to get flushed to disk
        //        return None;
        //    }
        //}
        //// we didn't find any data to update
        //let action = ResponseAction::Update(false);
        //// cast this action to a response
        //let response = Response {
        //    id: meta.id,
        //    index: meta.index,
        //    data: action,
        //    end: meta.end,
        //};
        //Some((meta.client, response))
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
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
{
    /// The intent type to use
    type Intent = SortedIntents<T>;

    fn load(
        read: &ReadResult,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<(), ServerError> {
        // try to deserialize this row from our intent log
        let intent = unsafe { rkyv::access_unchecked::<ArchivedSortedIntents<T>>(&read[..]) };
        // add this intent to our btreemap
        match intent {
            ArchivedSortedIntents::Insert(archived) => {
                // deserialize this row
                let row: T = RkyvSupport::deserialize(archived)?;
                // get the partition key for this row
                let key = row.get_partition_key();
                // get this rows partition
                todo!("USE A REAL GENEATION VALUE NOT 0");
                //let entry = partitions
                //    .entry(key)
                //    .or_insert_with(|| MaybeLoaded::Loaded { SortedPartition::new(key), 0 });
                // insert this row
                // TODO support this
                //entry.insert(row);
            }
            ArchivedSortedIntents::Delete {
                partition_key,
                sort_key,
            } => {
                // convert our partition key to its native endianess
                let partition_key = partition_key.to_native();
                // get the partition to delete a row from
                if let Some(partition) = partitions.get_mut(&partition_key) {
                    // deserialize this rows sort key
                    let sort_key = rkyv::deserialize::<T::Sort, rkyv::rancor::Error>(sort_key)?;
                    // TODO this
                    // remove the sort key from this partition
                    //partition.remove(&sort_key);
                }
            }
            ArchivedSortedIntents::Update(archived) => {
                // deserialize this row's update
                let update = rkyv::deserialize::<SortedUpdate<T>, rkyv::rancor::Error>(archived)?;
                // try to get the partition containing our target row
                if let Some(partition) = partitions.get_mut(&update.partition_key) {
                    // TODO this
                    //// find our target row
                    //if !partition.update(&update) {
                    //    panic!("Missing row update?");
                    //}
                }
            }
        }
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
                    entry.remove(&sort_key);
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
