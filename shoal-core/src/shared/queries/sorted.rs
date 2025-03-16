//! The query types for a sorted table in Shoal. This is a table where the
//! partitions contain multiple rows in a sorted structure.

use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::shared::traits::ShoalSortedTable;

/// The different types of queries for a single datatype
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum SortedQuery<T: ShoalSortedTable + std::fmt::Debug> {
    /// Insert a row into shoal
    Insert { key: u64, row: T },
    /// Get some data from shoal
    Get(SortedGet<T>),
    /// Delete a row from shoal
    Delete { key: u64, sort_key: T::Sort },
    /// Update a row in a shoal
    Update(SortedUpdate<T>),
}

impl<T: ShoalSortedTable + std::fmt::Debug> SortedQuery<T> {
    // sort our queries by shard
    pub fn find_shard<'a>(&self, ring: &'a Ring, tmp: &mut Vec<&'a ShardInfo>) {
        // get the correct shard for this query
        match self {
            SortedQuery::Insert { key, .. } | SortedQuery::Delete { key, .. } => {
                tmp.push(ring.find_shard(*key))
            }
            SortedQuery::Get(get) => {
                for key in &get.partition_keys {
                    tmp.push(ring.find_shard(*key))
                }
            }
            SortedQuery::Update(update) => tmp.push(ring.find_shard(update.partition_key)),
        }
    }
}

/// A single query tagged with client info
pub struct TaggedSortedQuery<R: ShoalSortedTable> {
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// The query to execute
    pub query: SortedQuery<R>,
}

impl<R: ShoalSortedTable> TaggedSortedQuery<R> {
    /// Create a new tagged query
    ///
    /// # Arguments
    ///
    /// * `id` - The id for this query's bundle
    /// * `index` - The index for this query in its parent bundle
    /// * `query` - The query to execute
    pub fn new(id: Uuid, index: usize, query: SortedQuery<R>) -> Self {
        Self { id, index, query }
    }
}

/// A get query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortedGet<R: ShoalSortedTable> {
    /// The partition keys to get data from
    pub partition_keys: Vec<u64>,
    /// The sort keys to get data from
    pub sort_keys: Vec<R::Sort>,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
}

/// An update query for a single row in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortedUpdate<T: ShoalSortedTable> {
    /// The key to the partition to update data in
    pub partition_key: u64,
    /// The sort key to apply updates too
    pub sort_key: T::Sort,
    /// The updates to apply
    pub update: T::Update,
}
