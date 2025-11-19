//! An unsorted table in Shoal where each partition contains a single row

use bytes::Bytes;
use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::rend::u64_le;
use rkyv::rend::unaligned::u64_ule;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::hash_map;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{event, instrument, Level};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::server::messages::{LoadedPartition, QueryMetadata, ShardMsg};
use crate::server::tables::partitions::UnsortedPartition;
use crate::server::tables::storage::StorageSupport;
use crate::server::{Conf, ServerError};
use crate::shared::queries::{
    ArchivedUnsortedGet, ArchivedUnsortedQuery, ArchivedUnsortedUpdate, UnsortedGet, UnsortedQuery,
    UnsortedUpdate,
};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{RkyvSupport, ShoalDatabase, ShoalUnsortedTable, TableNameSupport};
use crate::storage::{
    FullArchiveMap, IntentReadSupport, LoaderMsg, Loaders, PendingResponse, ShouldPrune,
};
use crate::tables::partitions::MaybeLoaded;

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
    for<'a> <<T as ShoalUnsortedTable>::Update as Archive>::Archived:
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

enum HandledKinds<R: ShoalUnsortedTable> {
    /// The query was successful
    Success {
        client_id: Uuid,
        query_id: Uuid,
        response: Response<R>,
    },
    /// This query was successful but is pending until data is flushed
    Pending { action: ResponseAction<R>, pos: u64 },
    /// This query needs a partition loaded to be executed
    NeedsLoad(u64),
}

/// A table that stores data both in memory and on disk
#[derive(Debug)]
pub struct PersistentUnsortedTable<R: ShoalUnsortedTable, S: StorageSupport, N: TableNameSupport> {
    /// The name of this table
    table_name: N,
    /// The rows in this table
    pub partitions: HashMap<u64, MaybeLoaded<UnsortedPartition<R>>>,
    /// The storage engine backing this table
    storage: S,
    /// The current generation of flushed data
    generation: u64,
    /// The commits that are still pending storage confirmation
    pending: PendingResponse<R>,
    /// The responses for queries that have been flushed to disk
    flushed: Vec<(Uuid, Uuid, Response<R>)>,
    /// The channel to send loader jobs on
    loader_tx: AsyncSender<LoaderMsg<N>>,
    /// A map of queries blocked on partitions being loaded from disk
    blocked: HashMap<u64, Vec<(QueryMetadata, UnsortedQuery<R>)>>,
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<Xxh3>>>>,
}

impl<R: ShoalUnsortedTable + 'static, S: StorageSupport, N: TableNameSupport>
    PersistentUnsortedTable<R, S, N>
where
    for<'a> <R as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
{
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
        shard_local_tx: &AsyncSender<ShardMsg<D>>,
    ) -> Result<Self, ServerError>
    where
        <<R as ShoalUnsortedTable>::Update as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalUnsortedTable>::Update,
            Strategy<Pool, rkyv::rancor::Error>,
        >,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<UnsortedPartition<R> as IntentReadSupport<R>>::Intent as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
        for<'a> <<R as ShoalUnsortedTable>::Update as Archive>::Archived: CheckBytes<
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
            storage: S::new::<UnsortedPartition<R>, R, N, D>(
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
            flushed: Vec::with_capacity(1000),
            loader_tx: loader_tx.clone(),
            blocked: HashMap::with_capacity(1000),
            memory_usage: memory_usage.clone(),
            lru: lru.clone(),
        };
        // load our intent log
        S::read_intents::<UnsortedPartition<R>, R>(
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
        shard_local_tx: &AsyncSender<ShardMsg<D>>,
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
    ) -> Option<(Vec<(QueryMetadata, UnsortedQuery<R>)>, u64)> {
        // if we have an existing loaded partition then do not use our newly loaded data
        // as that should be older
        match self.partitions.entry(loaded.partition_id) {
            hash_map::Entry::Occupied(mut entry) => {
                // if this partition contains already loaded data then we should use that
                if let &mut MaybeLoaded::Accessible(_) = entry.get_mut() {
                    // get the size of our dat
                    let size = loaded.data.len();
                    // wrap our raw data so that we can access it only when needed
                    let wrapped = MaybeLoaded::Accessible(loaded.data);
                    // overwrite our data with newly loaded data
                    entry.insert(wrapped);
                    // increment our memory usage
                    *self.memory_usage.borrow_mut() += size;
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
        let unblocked = self
            .blocked
            .remove(&loaded.partition_id)
            .map(|unblocked| (unblocked, self.generation));
        if let Some((unblocked, _)) = &unblocked {
            for (meta, _) in unblocked {
                println!("S3: UNBLOCKED: {}", meta.index);
            }
        }
        unblocked
    }

    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `archived` - The archived query to execute
    #[instrument(name = "PersistentTable::handle", skip(self, query))]
    pub async fn handle(
        &mut self,
        meta: QueryMetadata,
        query: UnsortedQuery<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)>
    where
        for<'a> <<R as ShoalUnsortedTable>::Update as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        for<'a> <R as ShoalUnsortedTable>::Filters: rkyv::Serialize<
            Strategy<
                rkyv::ser::Serializer<AlignedVec, ArenaHandle<'a>, Share>,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <<R as ShoalUnsortedTable>::Filters as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        <<R as ShoalUnsortedTable>::Update as Archive>::Archived: rkyv::Deserialize<
            <R as ShoalUnsortedTable>::Update,
            Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    {
        println!("S2: HANDLE -> {}", meta.index);
        // execute the correct query type
        match query {
            // insert a row into this partition
            UnsortedQuery::Insert { row, .. } => self.insert(meta, row).await,
            // get a row from this partition
            UnsortedQuery::Get(get) => self.get(meta, get).await,
            // delete a row from this partition
            UnsortedQuery::Delete { key } => self.delete(meta, key).await,
            // update a row in this partition
            UnsortedQuery::Update(update) => self.update(meta, update).await,
        }
        //match handled {
        //    HandledKinds::Success {
        //        client_id,
        //        query_id,
        //        response,
        //    } => Some((client_id, query_id, response)),
        //    HandledKinds::Pending { action, pos } => {
        //        // add this action to our pending queue
        //        self.pending.add(meta, pos, action);
        //        // return none since we have pending intent commits still
        //        None
        //    }
        //    HandledKinds::NeedsLoad(partition_key) => {
        //        // if we need to load this then add this query to a map of queries
        //        // are blocked on patitions being loaded from disk
        //        // get an entry to our partitions blocked queries
        //        let entry = self.blocked.entry(partition_key).or_default();
        //        // add our blocked query for this partitions blocked query list
        //        entry.push((meta, ));
        //        // return none since we need a partition to be loaded
        //        None
        //    }
        //}
    }

    /// Insert some data into a partition in this shards table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `row` - The row to insert
    #[instrument(name = "PersistentTable::insert", skip_all)]
    async fn insert(&mut self, meta: QueryMetadata, row: R) -> Option<(Uuid, Uuid, Response<R>)>
    where
        for<'a> <<R as ShoalUnsortedTable>::Update as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get our partition key
        let key = row.get_partition_key();
        // wrap our row in an insert intent
        let intent = UnsortedIntents::insert(row);
        // persist this new row to storage
        let pos = self.storage.commit(&intent).await.unwrap();
        // extract our row from our intent
        let row = match intent {
            UnsortedIntents::Insert(row) => row,
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
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `get` - The get parameters to use
    /// * `was_blocked` - Whether this query was blocked before being executed
    #[instrument(name = "PersistentTable::get", skip_all)]
    async fn get(
        &mut self,
        meta: QueryMetadata,
        get: UnsortedGet<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)> {
        // build a vec for the data we found
        let mut data = Vec::new();
        // try to get the partition for this key
        match self.partitions.get(&get.partition_key) {
            Some(partition) => {
                // get this partitions data
                let action = if partition.get(&get, &mut data) {
                    // mark this partition as recently used in our lru cache
                    self.lru
                        .borrow_mut()
                        .promote(&(self.table_name, get.partition_key));
                    // this query found data
                    ResponseAction::Get(Some(data))
                } else {
                    // this query did not find data
                    ResponseAction::Get(None)
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
            // this partition isn't loaded so lets try and load it from disk
            None => {
                // try to load this partition from disk if it exists
                let will_load = self
                    .storage
                    .load_partition(self.table_name, get.partition_key, &self.loader_tx)
                    .await
                    .unwrap();
                // if we aren't going to load data then return that this partition doesn't exist
                if will_load {
                    // if we need to load this then add this query to a map of queries
                    // are blocked on patitions being loaded from disk
                    // get an entry to our partitions blocked queries
                    let entry = self.blocked.entry(get.partition_key).or_default();
                    // add our blocked query for this partitions blocked query list
                    entry.push((meta, UnsortedQuery::Get(get)));
                    // return None since we don't yet have a response for this query
                    None
                } else {
                    // cast this action to a response
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data: ResponseAction::Get(None),
                        end: meta.end,
                    };
                    // the requested partition doesn't exist
                    Some((meta.client, meta.id, response))
                }
            }
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
    async fn delete(&mut self, meta: QueryMetadata, key: u64) -> Option<(Uuid, Uuid, Response<R>)>
    where
        for<'a> <<R as ShoalUnsortedTable>::Update as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // remove this partition
        match self.partitions.remove(&key) {
            Some(old) => {
                // wrap our row in an insert intent
                let intent = UnsortedIntents::<R>::delete(key);
                // wite this delete to our intent log
                let pos = self.storage.commit(&intent).await.unwrap();
                // build the pending action to store
                let action = ResponseAction::Delete(true);
                // add this action to our pending queue
                self.pending.add(meta, pos, action);
                // adjust this shards total memory usage
                *self.memory_usage.borrow_mut() = old.size();
                // remove this partition from our lru cache as its no longer evictable
                self.lru.borrow_mut().pop(&(self.table_name, key));
                // wait for this delete to get flushed to disk
                None
            }
            None => {
                // cast this action to a response
                let response = Response {
                    id: meta.id,
                    index: meta.index,
                    data: ResponseAction::Delete(false),
                    end: meta.end,
                };
                // theres nothing to delete so return our response
                Some((meta.client, meta.id, response))
            }
        }
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
        update: UnsortedUpdate<R>,
    ) -> Option<(Uuid, Uuid, Response<R>)>
    where
        for<'a> <<R as ShoalUnsortedTable>::Update as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get this rows partition
        match self.partitions.get_mut(&update.partition_key) {
            Some(partition) => {
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
                // we updated some data
                let action = ResponseAction::Update(true);
                // add this action to our pending queue
                self.pending.add(meta, pos, action);
                // remove this partition from our lru cache as its no longer evictable
                self.lru.borrow_mut().pop(&(self.table_name, key));
                // wait for this delete to get flushed to disk
                None
            }
            None => {
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
                Some((meta.client, meta.id, response))
            }
        }
    }

    /// Mark partitions as evictable if they are no longer in the intent log
    #[instrument(name = "PersistentTable::mark_evictable", skip(self, partitions), fields(partition_count = partitions.len()))]
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
    #[instrument(name = "PersistentTable::evict", skip_all, fields(victim_count = victims.len()))]
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
    pub async fn flush(&mut self) -> Result<(), ServerError> {
        self.storage.flush().await
    }

    /// Get all flushed response actions
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed actions to return
    pub async fn get_flushed(
        &mut self,
    ) -> Result<&mut Vec<(Uuid, Uuid, Response<R>)>, ServerError> {
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

impl<T: ShoalUnsortedTable + RkyvSupport> IntentReadSupport<T> for UnsortedPartition<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
    for<'a> <T as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <<T as ShoalUnsortedTable>::Update as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// The intent type to use
    type Intent = UnsortedIntents<T>;

    fn load(
        read: &ReadResult,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
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
                // try to delete this partition
                if let Some(removed) = partitions.remove(&partition_key) {
                    // we are removing a loaded partition so adjust our memory usage
                    *memory_usage.borrow_mut() -= removed.size();
                }
            }
            ArchivedUnsortedIntents::Update(archived) => {
                // deserialize this row's update
                let update = rkyv::deserialize::<UnsortedUpdate<T>, rkyv::rancor::Error>(archived)?;
                // try to get the partition containing our target row
                match partitions.get_mut(&update.partition_key) {
                    // update this row
                    Some(partition) => {
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
                    // TODO handling a row missing
                    None => panic!("Missing row?"),
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
        // start with no partition
        let mut maybe_partition = None;
        // apply all of our intents to this partition
        for intent in intents {
            // apply this intent to our partition
            match intent {
                UnsortedIntents::Insert(row) => {
                    // insert a new partition
                    maybe_partition = Some(Self::new(key, row));
                }
                UnsortedIntents::Delete { .. } => maybe_partition = None,
                UnsortedIntents::Update(update) => {
                    // apply this update if we have a partition
                    match &mut maybe_partition {
                        Some(partition) => partition.update(&update),
                        None => panic!("Applying update to no partition?"),
                    }
                }
            }
        }
        // only insert this partition if we ended with one
        if let Some(partition) = maybe_partition {
            // insert this partition
            loaded.insert(key, partition);
            // we have a partition still so this partition should not be pruned
            ShouldPrune::No
        } else {
            // we do not have a partition so prune it
            ShouldPrune::Yes
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
}
