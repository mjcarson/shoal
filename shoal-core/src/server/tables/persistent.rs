//! Persistent tables cache hot data in memory while also storing data on disk.
//!
//! This means that data is retained through restarts at the cost of speed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::instrument;
use uuid::Uuid;

use super::partitions::Partition;
use super::storage::Intents;
use super::storage::ShoalStorage;
use crate::server::messages::QueryMetadata;
use crate::server::Conf;
use crate::server::ServerError;
use crate::shared::queries::Update;
use crate::shared::queries::{Get, Query};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::ShoalTable;

/// A table that stores data both in memory and on disk
#[derive(Debug)]
pub struct PersistentTable<T: ShoalTable, S: ShoalStorage<T>> {
    /// The rows in this table
    pub partitions: HashMap<u64, Partition<T>>,
    /// The storage engine backing this table
    storage: S,
    /// The total size of all data on this shard
    memory_usage: usize,
    /// The responses for queries that have been flushed to disk
    flushed: Vec<(SocketAddr, Response<T>)>,
}

impl<T: ShoalTable, S: ShoalStorage<T>> PersistentTable<T, S> {
    /// Create a persistent shoal table
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(name = "PersistentTable::new", skip(conf), err(Debug))]
    pub async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError> {
        // build our table
        let mut table = Self {
            partitions: HashMap::default(),
            storage: S::new(shard_name, conf).await?,
            memory_usage: 0,
            flushed: Vec::with_capacity(1000),
        };
        // build the path to this shards intent log
        let path = PathBuf::from_str(&format!("/opt/shoal/Intents/{shard_name}-active"))?;
        // load our intent log
        S::read_intents(&path, &mut table.partitions).await?;
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
        query: Query<T>,
    ) -> Option<(SocketAddr, Response<T>)> {
        // execute the correct query type
        match query {
            // insert a row into this partition
            Query::Insert { row, .. } => self.insert(meta, row).await,
            // get a row from this partition
            Query::Get(get) => self.get(meta, &get).await,
            // delete a row from this partition
            Query::Delete { key, sort_key } => self.delete(meta, key, sort_key).await,
            // update a row in this partition
            Query::Update(update) => self.update(meta, update).await,
        }
    }

    /// Insert some data into a partition in this shards table
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about this insert query
    /// * `row` - The row to insert
    #[instrument(name = "PersistentTable::insert", skip_all)]
    async fn insert(&mut self, meta: QueryMetadata, row: T) -> Option<(SocketAddr, Response<T>)> {
        // get our partition key
        let key = row.get_partition_key();
        // get our partition
        let partition = self.partitions.entry(key).or_default();
        // wrap our row in an insert intent
        let intent = Intents::Insert(row);
        // persist this new row to storage
        let pos = self.storage.insert(&intent).await.unwrap();
        // extract our row from our intent
        let row = match intent {
            Intents::Insert(row) => row,
            _ => panic!("TODO NOT HAVE THIS POINTLESS MATCH!"),
        };
        // insert this row into this partition
        let (size_diff, action) = partition.insert(row);
        // add this action to our pending queue
        self.storage.add_pending(meta, pos, action);
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
        get: &Get<T>,
    ) -> Option<(SocketAddr, Response<T>)> {
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
        sort: T::Sort,
    ) -> Option<(SocketAddr, Response<T>)> {
        // get this rows partition
        if let Some(partition) = self.partitions.get_mut(&key) {
            // try remove the target row from this partition
            if let Some((size_diff, _)) = partition.remove(&sort) {
                // wite this delete to our intent log
                let pos = self.storage.delete(key, sort).await.unwrap();
                // build the pending action to store
                let action = ResponseAction::Delete(true);
                // add this action to our pending queue
                self.storage.add_pending(meta, pos, action);
                // adjust this shards total memory usage
                self.memory_usage = self.memory_usage.saturating_sub(size_diff);
                // wait for this delete to get flushed to disk
                return None;
            }
        }
        // we didn't find any data to delete
        let action = ResponseAction::Delete(true);
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
        update: Update<T>,
    ) -> Option<(SocketAddr, Response<T>)> {
        // get this rows partition
        if let Some(partition) = self.partitions.get_mut(&update.partition_key) {
            if partition.update(&update) {
                // write this update to storage
                let pos = self.storage.update(update).await.unwrap();
                // we didn't find any data to update
                let action = ResponseAction::Update(false);
                // add this action to our pending queue
                self.storage.add_pending(meta, pos, action);
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
    pub fn get_flushed(&mut self) -> &mut Vec<(SocketAddr, Response<T>)> {
        // check if we have any flushed actions to return
        self.storage.get_flushed(&mut self.flushed);
        // return a ref to our flushed responses
        &mut self.flushed
    }

    /// Shutdown this table
    #[instrument(name = "PersistentTable::shutdown", skip_all)]
    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
        // flush any remaining writes to disk
        self.storage.flush().await
    }
}
