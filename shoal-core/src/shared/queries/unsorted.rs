//! The queries for an unsorted table in shoal. This is a table where each
//! partition contains only a single row.

use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::shared::queries::group_by_shard;
use crate::shared::traits::{RkyvSupport, ShoalTableSupport, ShoalUnsortedTable};

/// The different types of queries for a single datatype
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum UnsortedQuery<T: ShoalUnsortedTable + std::fmt::Debug + RkyvSupport> {
    /// Insert a row into shoal
    Insert { key: u64, row: T },
    /// Get some data from shoal
    Get(UnsortedGet<T>),
    /// Delete a row from shoal
    Delete { key: u64 },
    /// Update a row in a shoal
    Update(UnsortedUpdate<T>),
    /// Check if data exists in shoal
    Exists(UnsortedExists<T>),
}

impl<T: ShoalUnsortedTable + std::fmt::Debug> UnsortedQuery<T> {
    /// Split this query into the per shard queries that answer it
    ///
    /// Every unsorted query but a get names exactly one partition, so it goes to a single
    /// shard with nothing to narrow. A get may name several, so it is narrowed to each
    /// shards own keys the same way a sorted get is.
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard queries we found for this query
    pub fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>) {
        // get the correct shards for this query
        let shard = match self {
            UnsortedQuery::Insert { key, .. } | UnsortedQuery::Delete { key, .. } => {
                ring.find_shard(*key)
            }
            UnsortedQuery::Get(get) => {
                // narrow this get to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &get.partition_keys) {
                    found.push((shard, UnsortedQuery::Get(get.for_partitions(keys))));
                }
                return;
            }
            UnsortedQuery::Update(update) => ring.find_shard(update.partition_key),
            UnsortedQuery::Exists(exists) => ring.find_shard(exists.partition_key),
        };
        found.push((shard, self.clone()));
    }

    /// Get the subset of each rows fields this query asked to be answered with
    ///
    /// Only a get returns rows, so every other query answers with the whole row it would have
    /// carried. That is the same answer a get that named no projection gives, which is what
    /// lets the database dispatch on this one value rather than on the query kind as well.
    pub fn projection(&self) -> T::Projection {
        // only a get returns rows there is a subset of
        match self {
            UnsortedQuery::Get(get) => get.projection,
            _ => T::Projection::default(),
        }
    }

    /// Get the most rows this query asked for, if it set a limit
    pub fn limit(&self) -> Option<usize> {
        // only a get returns rows that a limit could apply to
        match self {
            UnsortedQuery::Get(get) => get.limit,
            _ => None,
        }
    }

    /// Get the partitions this query named, in the order it named them
    ///
    /// This is the order the rows come back in, so the shard collecting the shares of a
    /// split query uses it to put them back together. Only a get returns rows there is an
    /// order to, so every other query names none.
    pub fn partition_keys(&self) -> &[u64] {
        // only a get returns rows whose order this could describe
        match self {
            UnsortedQuery::Get(get) => &get.partition_keys,
            _ => &[],
        }
    }
}

impl<T: ShoalUnsortedTable> RkyvSupport for UnsortedQuery<T> where
    for<'a> <T as ShoalTableSupport>::Filters: rkyv::Serialize<
        Strategy<rkyv::ser::Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >
{
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
    /// The partition keys to get data from
    pub partition_keys: Vec<u64>,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
    /// The subset of each rows fields this get is asking to be answered with
    pub projection: R::Projection,
}

impl<R: ShoalUnsortedTable> UnsortedGet<R> {
    /// Create a get for just some of this gets partitions
    ///
    /// Everything but the partition keys is copied as is, including the limit. The limit is
    /// deliberately not divided up, because each shard keeps the rows of the partitions this
    /// get named first, and the shard collecting their shares trims their union back down.
    ///
    /// # Arguments
    ///
    /// * `partition_keys` - The keys of the partitions this get should cover
    pub fn for_partitions(&self, partition_keys: Vec<u64>) -> Self {
        UnsortedGet {
            partition_keys,
            filters: self.filters.clone(),
            limit: self.limit,
            // a narrowed get answers with the same rows the get it came from asked for
            projection: self.projection,
        }
    }

    /// Create a single partition get from another get
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key of the partition that needs to be loaded from disk
    pub fn to_blocked(&self, partition_key: u64) -> Self {
        self.for_partitions(vec![partition_key])
    }

    /// Check if we have already found every row this get asked for
    ///
    /// An unsorted partition holds at most one row, so this only ever bites on a get naming
    /// several partitions, or on a limit of zero.
    ///
    /// The rows are counted rather than inspected, so this is generic in what a get is being
    /// answered with: a projected get fills its limit with the same number of rows an
    /// unprojected one does.
    ///
    /// # Arguments
    ///
    /// * `found` - The rows this get has found so far
    pub fn limit_reached<P>(&self, found: &[P]) -> bool {
        // check whether this get was given a limit at all
        match self.limit {
            // we are done once we hold as many rows as we were asked for
            Some(limit) => found.len() >= limit,
            // a get with no limit can never fill
            None => false,
        }
    }
}

/// An exists query to check if data exists
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct UnsortedExists<R: ShoalUnsortedTable> {
    /// The partition keys to check for data in
    pub partition_key: u64,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
}

impl<R: ShoalUnsortedTable> UnsortedExists<R> {
    /// Create a single partition exists from another exists
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key of the partition that needs to be loaded from disk
    pub fn to_blocked(&self, partition_key: u64) -> Self {
        UnsortedExists {
            partition_key,
            filters: self.filters.clone(),
        }
    }
}

/// An update query for a single row in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct UnsortedUpdate<T: ShoalUnsortedTable + RkyvSupport> {
    /// The key to the partition to update data in
    pub partition_key: u64,
    /// The updates to apply
    pub update: T::UpdateData,
}

impl<T: ShoalUnsortedTable> RkyvSupport for UnsortedUpdate<T> {}
