//! The types needed for querying the database

use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use super::traits::ShoalDatabase;

use super::traits::ShoalTable;
use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;

/// A bundle of different query kinds
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Queries<S: ShoalDatabase> {
    /// The id for this query
    pub id: Uuid,
    /// The individual queries to execute
    pub queries: Vec<S::QueryKinds>,
}

impl<S: ShoalDatabase> Queries<S> {
    /// Add a new query onto this queries bundle
    ///
    /// # Arguments
    ///
    /// * `query` - The query to add
    #[must_use]
    pub fn add<Q: Into<S::QueryKinds>>(mut self, query: Q) -> Self {
        // add our query
        self.queries.push(query.into());
        self
    }
}

impl<S: ShoalDatabase> Default for Queries<S> {
    fn default() -> Self {
        Queries {
            id: Uuid::new_v4(),
            queries: Vec::default(),
        }
    }
}

/// The different types of queries for a single datatype
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
pub enum Query<T: ShoalTable + std::fmt::Debug> {
    /// Insert a row into shoal
    Insert { key: u64, row: T },
    /// Get some data from shoal
    Get(Get<T>),
    /// Delete a row from shoal
    Delete { key: u64, sort_key: T::Sort },
    /// Update a row in a shoal
    Update(Update<T>),
}

impl<T: ShoalTable + std::fmt::Debug> Query<T> {
    // sort our queries by shard
    pub fn find_shard<'a>(&self, ring: &'a Ring, tmp: &mut Vec<&'a ShardInfo>) {
        // get the correct shard for this query
        match self {
            Query::Insert { key, .. } | Query::Delete { key, .. } => {
                tmp.push(ring.find_shard(*key))
            }
            Query::Get(get) => {
                for key in &get.partition_keys {
                    tmp.push(ring.find_shard(*key))
                }
            }
            Query::Update(update) => tmp.push(ring.find_shard(update.partition_key)),
        }
    }
}

/// A single query tagged with client info
pub struct TaggedQuery<R: ShoalTable> {
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// The query to execute
    pub query: Query<R>,
}

impl<R: ShoalTable> TaggedQuery<R> {
    /// Create a new tagged query
    ///
    /// # Arguments
    ///
    /// * `id` - The id for this query's bundle
    /// * `index` - The index for this query in its parent bundle
    /// * `query` - The query to execute
    pub fn new(id: Uuid, index: usize, query: Query<R>) -> Self {
        Self { id, index, query }
    }
}

/// A get query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
//#[archive_attr(derive(Debug))]
pub struct Get<R: ShoalTable> {
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
#[archive(check_bytes)]
pub struct Update<T: ShoalTable> {
    /// The key to the partition to update data in
    pub partition_key: u64,
    /// The sort key to apply updates too
    pub sort_key: T::Sort,
    /// The updates to apply
    pub update: T::Update,
}

//impl<T: ShoalTable> Update<T> {
//    /// Get this updates sort key
//    pub fn get_sort_key(&self) -> T::Sort {
//        self.sort_key
//    }
//}
