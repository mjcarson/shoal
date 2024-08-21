//! Ephemeral tables are fully in memory and are never persisted to disk
//!
//! This means while they are the fastest when it comes to writes they will
//! not retain data through restarts.

use std::collections::BTreeMap;
use uuid::Uuid;

use super::partitions::Partition;
use crate::server::Conf;
use crate::shared::queries::{Get, Query};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::ShoalTable;

/// A Table that stores all data only in memory
#[derive(Debug)]
pub struct EphemeralTable<R: ShoalTable> {
    /// The rows in this table
    pub partitions: BTreeMap<R::Sort, Partition<R>>,
}

impl<R: ShoalTable> Default for EphemeralTable<R> {
    /// Build a default empty table
    fn default() -> Self {
        Self {
            partitions: BTreeMap::default(),
        }
    }
}

impl<D: ShoalTable> EphemeralTable<D> {
    /// Create an ephemeral shoal table
    ///
    /// # Arguments
    ///
    /// * `conf` - The Shoal config
    pub fn new(_conf: &Conf) -> Self {
        Self {
            partitions: BTreeMap::default(),
        }
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
        query: Query<D>,
        end: bool,
    ) -> Response<D> {
        // execute the correct query type
        let data = match query {
            // insert a row into this partition
            Query::Insert { row, .. } => self.insert(row).await,
            // get a row from this partition
            Query::Get(get) => self.get(&get).await,
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
    async fn insert(&mut self, row: D) -> ResponseAction<D> {
        // get our partition key
        let key = row.get_sort().clone();
        // get our partition
        let partition = self.partitions.entry(key).or_default();
        // insert this row into this partition
        partition.insert(row)
    }

    /// Get some rows from some partitions
    ///
    /// # Arguments
    ///
    /// * `get` - The get parameters to use
    /// * `responses` - The response object to use
    async fn get(&mut self, get: &Get<D>) -> ResponseAction<D> {
        // build a vec for the data we found
        let mut data = Vec::new();
        // build the sort key
        for key in &get.partitions {
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
}
