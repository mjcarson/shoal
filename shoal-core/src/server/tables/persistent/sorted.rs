//! A sorted table in  Shoal where each partition can contain multiple sorted rows

use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::instrument;

use crate::server::messages::QueryMetadata;
use crate::server::tables::partitions::SortedPartition;
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::queries::SortedUpdate;
use crate::shared::queries::{SortedGet, SortedQuery};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{RkyvSupport, ShoalSortedTable};
use crate::storage::{IntentReadSupport, PendingResponse, StorageSupport};

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
pub struct PersistentSortedTable<R: ShoalSortedTable, S: StorageSupport> {
    /// The rows in this table
    pub partitions: HashMap<u64, SortedPartition<R>>,
    /// The storage engine backing this table
    storage: S,
    /// The commits that are still pending storage confirmation
    pending: PendingResponse<R>,
    /// The total size of all data on this shard
    memory_usage: usize,
    /// The responses for queries that have been flushed to disk
    flushed: Vec<(SocketAddr, Response<R>)>,
}

impl<R: ShoalSortedTable + 'static, S: StorageSupport> PersistentSortedTable<R, S> {
    /// Create a persistent shoal table
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(name = "PersistentTable::new", skip(conf), err(Debug))]
    pub async fn new(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError>
    where
        <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
        <<R as ShoalSortedTable>::Update as Archive>::Archived:
            rkyv::Deserialize<<R as ShoalSortedTable>::Update, Strategy<Pool, rkyv::rancor::Error>>,
        <<R as ShoalSortedTable>::Sort as Archive>::Archived:
            rkyv::Deserialize<<R as ShoalSortedTable>::Sort, Strategy<Pool, rkyv::rancor::Error>>,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
    {
        // build our table
        let mut table = Self {
            partitions: HashMap::default(),
            storage: S::new::<SortedPartition<R>, R>(shard_name, conf, medium_priority).await?,
            pending: PendingResponse::<R>::with_capacity(100),
            memory_usage: 0,
            flushed: Vec::with_capacity(1000),
        };
        // load our intent log
        S::read_intents::<SortedPartition<R>, R>(shard_name, conf, &mut table.partitions).await?;
        Ok(table)
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
    ) -> Option<(SocketAddr, Response<R>)> {
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
    async fn insert(&mut self, meta: QueryMetadata, row: R) -> Option<(SocketAddr, Response<R>)> {
        // get our partition key
        let key = row.get_partition_key();
        // get our partition
        let partition = self
            .partitions
            .entry(key)
            .or_insert_with(|| SortedPartition::new(key));
        // wrap our row in an insert intent
        let intent = SortedIntents::Insert(row);
        // persist this new row to storage
        let pos = self.storage.commit(&intent).await.unwrap();
        // extract our row from our intent
        let row = match intent {
            SortedIntents::Insert(row) => row,
            _ => panic!("TODO NOT HAVE THIS POINTLESS MATCH!"),
        };
        // insert this row into this partition
        let (size_diff, action) = partition.insert(row);
        // add this action to our pending queue
        self.pending.add(meta, pos, action);
        // adjust our total shards memory usage
        self.memory_usage = self.memory_usage.saturating_add_signed(size_diff);
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
    ) -> Option<(SocketAddr, Response<R>)> {
        // build a vec for the data we found
        let mut data = Vec::new();
        // build the sort key
        for key in &get.partition_keys {
            // get the partition for this key
            if let Some(partition) = self.partitions.get(key) {
                // get rows from this partition
                partition.get(get, &mut data);
            }
        }
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
        Some((meta.addr, response))
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
    ) -> Option<(SocketAddr, Response<R>)> {
        // get this rows partition
        if let Some(partition) = self.partitions.get_mut(&key) {
            // try remove the target row from this partition
            if let Some((size_diff, _)) = partition.remove(&sort) {
                // wrap our row in an delete intent
                let intent = SortedIntents::<R>::delete(key, sort);
                // wite this delete to our intent log
                let pos = self.storage.commit(&intent).await.unwrap();
                // build the pending action to store
                let action = ResponseAction::Delete(true);
                // add this action to our pending queue
                self.pending.add(meta, pos, action);
                // adjust this shards total memory usage
                self.memory_usage = self.memory_usage.saturating_sub(size_diff);
                // wait for this delete to get flushed to disk
                return None;
            }
        }
        // we didn't find any data to delete
        let action = ResponseAction::Delete(false);
        // cast this action to a response
        let response = Response {
            id: meta.id,
            index: meta.index,
            data: action,
            end: meta.end,
        };
        Some((meta.addr, response))
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
    ) -> Option<(SocketAddr, Response<R>)> {
        // get this rows partition
        if let Some(partition) = self.partitions.get_mut(&update.partition_key) {
            if partition.update(&update) {
                // wrap our update in an intent
                let intent = SortedIntents::update(update);
                // write this update to storage
                let pos = self.storage.commit(&intent).await.unwrap();
                // we didn't find any data to update
                let action = ResponseAction::Update(false);
                // add this action to our pending queue
                self.pending.add(meta, pos, action);
                // TODO: adjust this shards memory usage?
                // wait for this delete to get flushed to disk
                return None;
            }
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
        Some((meta.addr, response))
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
    ) -> Result<&mut Vec<(SocketAddr, Response<R>)>, ServerError> {
        // check if our current intent log should be compacted
        let flushed_pos = self.storage.compact_if_needed::<R>().await?;
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

impl<T: ShoalSortedTable + RkyvSupport> IntentReadSupport for SortedPartition<T>
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
        read: &glommio::io::ReadResult,
        partitions: &mut HashMap<u64, Self>,
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
                let entry = partitions
                    .entry(key)
                    .or_insert_with(|| SortedPartition::new(key));
                // insert this row
                entry.insert(row);
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
                    // remove the sort key from this partition
                    partition.remove(&sort_key);
                }
            }
            ArchivedSortedIntents::Update(archived) => {
                // deserialize this row's update
                let update = rkyv::deserialize::<SortedUpdate<T>, rkyv::rancor::Error>(archived)?;
                // try to get the partition containing our target row
                if let Some(partition) = partitions.get_mut(&update.partition_key) {
                    // find our target row
                    if !partition.update(&update) {
                        panic!("Missing row update?");
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
    /// * `intent` - The intent to apply to this partition
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
    ) -> bool {
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
        false
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
