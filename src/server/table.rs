//! A table is a view of data accessible by a primary key

use std::collections::BTreeMap;
use uuid::Uuid;

pub mod partition;

use self::partition::Partition;
use crate::shared::queries::{Get, Query};
use crate::shared::responses::{Response, ResponseKinds};
use crate::shared::traits::ShoalTable;

/// A Table containing some data for Shoal
#[derive(Debug)]
pub struct Table<R: ShoalTable> {
    /// The rows in this table
    pub partitions: BTreeMap<R::Sort, Partition<R>>,
}

impl<R: ShoalTable> Default for Table<R> {
    /// Build a default empty table
    fn default() -> Self {
        Self {
            partitions: BTreeMap::default(),
        }
    }
}

impl<D: ShoalTable> Table<D> {
    /// Cast and handle a serialized query
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute
    pub fn handle(&mut self, id: Uuid, index: usize, query: Query<D>, end: bool) -> Response<D> {
        // execute the correct query type
        let data = match query {
            // insert a row into this partition
            Query::Insert { row, .. } => self.insert(row),
            // get a row from this partition
            Query::Get(get) => self.get(&get),
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
    fn insert(&mut self, row: D) -> ResponseKinds<D> {
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
    fn get(&mut self, get: &Get<D>) -> ResponseKinds<D> {
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
            ResponseKinds::Get(None)
        } else {
            // this query found data
            ResponseKinds::Get(Some(data))
        }
    }
}
