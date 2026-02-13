//! Ephemeral tables are fully in memory and are never persisted to disk
//!
//! This means while they are the fastest when it comes to writes they will
//! not retain data through restarts.

use std::collections::BTreeMap;
use uuid::Uuid;

use super::partitions::SortedPartition;
use crate::server::Conf;
use crate::shared::queries::{SortedExists, SortedGet, SortedQuery, SortedUpdate};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::ShoalSortedTable;

/// A Table that stores all data only in memory
#[derive(Debug)]
pub struct EphemeralTable<T: ShoalSortedTable> {
    /// The rows in this table
    pub partitions: BTreeMap<u64, SortedPartition<T>>,
    /// The total size of all data on this shard
    memory_usage: usize,
}

impl<T: ShoalSortedTable> Default for EphemeralTable<T> {
    /// Build a default empty table
    fn default() -> Self {
        Self {
            partitions: BTreeMap::default(),
            memory_usage: 0,
        }
    }
}

impl<T: ShoalSortedTable> EphemeralTable<T> {
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
        query: SortedQuery<T>,
        end: bool,
    ) -> Response<T> {
        // execute the correct query type
        let data = match query {
            // insert a row into this partition
            SortedQuery::Insert { row, .. } => self.insert(row).await,
            // get a row from this partition
            SortedQuery::Get(get) => self.get(&get).await,
            // delete a row from this partition
            SortedQuery::Delete { key, sort_key } => self.delete(key, &sort_key).await,
            // Update a row in a target partition
            SortedQuery::Update(update) => self.update(update).await,
            // Check if data exists in a partition
            SortedQuery::Exists(exists) => self.exists(&exists).await,
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
            .or_insert_with(|| SortedPartition::new(key));
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
    async fn get(&mut self, get: &SortedGet<T>) -> ResponseAction<T> {
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
    async fn update(&mut self, update: SortedUpdate<T>) -> ResponseAction<T> {
        // get this rows partition
        let updated = match self.partitions.get_mut(&update.partition_key) {
            Some(partition) => match partition.update(&update) {
                Some(diff) => {
                    self.memory_usage = self.memory_usage.saturating_add_signed(diff);
                    true
                }
                None => false,
            },
            None => false,
        };
        ResponseAction::Update(updated)
    }

    /// Check if data exists in some partitions
    ///
    /// # Arguments
    ///
    /// * `exists` - The exists parameters to use
    async fn exists(&mut self, exists: &SortedExists<T>) -> ResponseAction<T> {
        // check each of the specified partition keys
        for key in &exists.partition_keys {
            // get the partition for this key
            if let Some(partition) = self.partitions.get(key) {
                // check rows in this partition
                for row in partition.live_row_values() {
                    // check if we are supposed to filter our rows
                    if let Some(filters) = &exists.filters {
                        if !T::is_filtered(filters, row) {
                            continue;
                        }
                    }
                    // found a matching row - data exists
                    return ResponseAction::Exists(true);
                }
            }
        }
        // no data found in any partition
        ResponseAction::Exists(false)
    }
}
