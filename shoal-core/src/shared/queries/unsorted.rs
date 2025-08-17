//! The queries for an unsorted table in shoal. This is a table where each
//! partition contains only a single row.

use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::shared::traits::{RkyvSupport, ShoalUnsortedTable};

/// The different types of queries for a single datatype
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum UnsortedQuery<T: ShoalUnsortedTable + std::fmt::Debug> {
    /// Insert a row into shoal
    Insert { key: u64, row: T },
    /// Get some data from shoal
    Get(UnsortedGet<T>),
    /// Delete a row from shoal
    Delete { key: u64 },
    /// Update a row in a shoal
    Update(UnsortedUpdate<T>),
}

impl<T: ShoalUnsortedTable + std::fmt::Debug> UnsortedQuery<T> {
    // sort our queries by shard
    pub fn find_shard<'a>(&self, ring: &'a Ring, tmp: &mut Vec<&'a ShardInfo>) {
        // get the correct shard for this query
        match self {
            UnsortedQuery::Insert { key, .. } | UnsortedQuery::Delete { key, .. } => {
                tmp.push(ring.find_shard(*key))
            }
            UnsortedQuery::Get(get) => tmp.push(ring.find_shard(get.partition_key)),
            UnsortedQuery::Update(update) => tmp.push(ring.find_shard(update.partition_key)),
        }
    }
}

/// A single query tagged with client info
pub struct TaggedUnsortedQuery<R: ShoalUnsortedTable> {
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// The query to execute
    pub query: UnsortedQuery<R>,
}

impl<R: ShoalUnsortedTable> TaggedUnsortedQuery<R> {
    /// Create a new tagged query
    ///
    /// # Arguments
    ///
    /// * `id` - The id for this query's bundle
    /// * `index` - The index for this query in its parent bundle
    /// * `query` - The query to execute
    pub fn new(id: Uuid, index: usize, query: UnsortedQuery<R>) -> Self {
        Self { id, index, query }
    }
}

/// A get query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct UnsortedGet<R: ShoalUnsortedTable> {
    /// The partition key to get data from
    pub partition_key: u64,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
}

/// An update query for a single row in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct UnsortedUpdate<T: ShoalUnsortedTable + RkyvSupport> {
    /// The key to the partition to update data in
    pub partition_key: u64,
    /// The updates to apply
    pub update: T::Update,
}
