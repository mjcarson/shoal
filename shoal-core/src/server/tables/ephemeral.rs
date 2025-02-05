//! Ephemeral tables are fully in memory and are never persisted to disk
//!
//! This means while they are the fastest when it comes to writes they will
//! not retain data through restarts.

use std::collections::BTreeMap;
use uuid::Uuid;

use super::partitions::Partition;
use crate::server::Conf;
use crate::shared::queries::{Get, Query, Update};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::ShoalTable;

/// A Table that stores all data only in memory
#[derive(Debug)]
pub struct EphemeralTable<T: ShoalTable> {
    /// The rows in this table
    pub partitions: BTreeMap<u64, Partition<T>>,
    /// The total size of all data on this shard
    memory_usage: usize,
}

impl<T: ShoalTable> Default for EphemeralTable<T> {
    /// Build a default empty table
    fn default() -> Self {
        Self {
            partitions: BTreeMap::default(),
            memory_usage: 0,
        }
    }
}

impl<T: ShoalTable> EphemeralTable<T> {
    /// Create an ephemeral shoal table
    ///
    /// # Arguments
    ///
    /// * `conf` - The Shoal config
    pub fn new(_conf: &Conf) -> Self {
        Self::default()
    }
    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute
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
            Query::Delete { key, sort_key } => self.delete(key, &sort_key).await,
            // Update a row in a target partition
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
    async fn insert(&mut self, row: T) -> ResponseAction<T> {
        // get our partition key
        let key = row.get_partition_key().clone();
        // get our partition
        let partition = self
            .partitions
            .entry(key)
            .or_insert_with(|| Partition::new(key));
        // insert this row into this partition
        let (size_diff, action) = partition.insert(row);
        // adjust our total shards memory usage
        self.memory_usage = self.memory_usage.saturating_add_signed(size_diff);
        action
    }

    /// Get some rows from some partitions
    ///
    /// # Arguments
    ///
    /// * `get` - The get parameters to use
    /// * `responses` - The response object to use
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

    /// Delete a row from this partition
    ///
    /// # Arguments
    ///
    /// * `key` - The key to the partition to dlete data from
    /// * `sort` - The sort key to delete
    async fn delete(&mut self, key: u64, sort: &T::Sort) -> ResponseAction<T> {
        // get this rows partition
        let removed = match self.partitions.get_mut(&key) {
            Some(partition) => {
                match partition.remove(sort) {
                    Some((diff, _)) => {
                        // adjust this shards total memory usage
                        self.memory_usage = self.memory_usage.saturating_sub(diff);
                        // return our removed row
                        true
                    }
                    None => false,
                }
            }
            None => false,
        };
        ResponseAction::Delete(removed)
    }

    /// Update a row in this table
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a row in this table
    async fn update(&mut self, update: Update<T>) -> ResponseAction<T> {
        // get this rows partition
        let updated = match self.partitions.get_mut(&update.partition_key) {
            Some(partition) => partition.update(&update),
            None => false,
        };
        ResponseAction::Update(updated)
    }
}
