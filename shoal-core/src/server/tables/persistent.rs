//! Persistent tables cache hot data in memory while also storing data on disk.
//!
//! This means that data is retained through restarts at the cost of speed.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::instrument;
use uuid::Uuid;

use super::partitions::Partition;
use super::storage::Intents;
use super::storage::ShoalStorage;
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
    /// * `query` - The query to execute
    #[instrument(name = "PersistentTable::handle", skip(self, query))]
    pub async fn handle(
        &mut self,
        id: Uuid,
        index: usize,
        query: Query<T>,
        end: bool,
    ) -> Response<T> {
        // execute the correct query type
        let data = match query {
            // insert a row into this partition
            Query::Insert { row, .. } => self.insert(row).await,
            // get a row from this partition
            Query::Get(get) => self.get(&get).await,
            // delete a row from this partition
            Query::Delete { key, sort_key } => self.delete(key, sort_key).await,
            // update a row in this partition
            Query::Update(update) => self.update(update).await,
        };
        // build the response for this query
        Response {
            id,
            index,
            data,
            end,
        }
    }

    /// Insert some data into a partition in this shards table
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    #[instrument(name = "PersistentTable::insert", skip_all)]
    async fn insert(&mut self, row: T) -> ResponseAction<T> {
        // get our partition key
        let key = row.get_partition_key();
        // get our partition
        let partition = self.partitions.entry(key).or_default();
        // wrap our row in an insert intent
        let intent = Intents::Insert(row);
        // persist this new row to storage
        self.storage.insert(&intent).await;
        // extract our row from our intent
        let row = match intent {
            Intents::Insert(row) => row,
            _ => panic!("TODO NOT HAVE THIS POINTLESS MATCH!"),
        };
        // insert this row into this partition
        partition.insert(row)
    }

    /// Get some rows from some partitions
    ///
    /// # Arguments
    ///
    /// * `get` - The get parameters to use
    /// * `responses` - The response object to use
    #[instrument(name = "PersistentTable::get", skip_all)]
    async fn get(&mut self, get: &Get<T>) -> ResponseAction<T> {
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
        if data.is_empty() {
            // this query did not find data
            ResponseAction::Get(None)
        } else {
            // this query found data
            ResponseAction::Get(Some(data))
        }
    }

    /// Delete a row from this table
    ///
    /// # Arguments
    ///
    /// * `key` - The key to the partition to dlete data from
    /// * `sort` - The sort key to delete
    #[instrument(name = "PersistentTable::delete", skip_all)]
    async fn delete(&mut self, key: u64, sort: T::Sort) -> ResponseAction<T> {
        // get this rows partition
        let removed = match self.partitions.get_mut(&key) {
            Some(partition) => {
                // try remove the target row from this partition
                if partition.remove(&sort).is_some() {
                    // wite this delete to our intent log
                    self.storage.delete(key, sort).await;
                    true
                } else {
                    // we didn't find the row to delete
                    false
                }
            }
            // this partition doesn't exist and so the row can't exist
            None => false,
        };
        ResponseAction::Delete(removed)
    }

    /// Update a row in this table
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a row in this table
    #[instrument(name = "PersistentTable::update", skip_all)]
    async fn update(&mut self, update: Update<T>) -> ResponseAction<T> {
        // get this rows partition
        let updated = match self.partitions.get_mut(&update.partition_key) {
            Some(partition) => {
                if partition.update(&update) {
                    // write this update to storage
                    self.storage.update(update).await;
                    true
                } else {
                    false
                }
            }
            None => false,
        };
        ResponseAction::Update(updated)
    }

    /// Shutdown this table
    #[instrument(name = "PersistentTable::shutdown", skip_all)]
    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
        // flush any remaining writes to disk
        self.storage.flush().await
    }
}
