//! Splitting a query into the per shard queries that answer it
//!
//! This is the half of a query's behaviour that only a server has. Its signature names [`Ring`]
//! and [`ShardInfo`], which are placement — and placement is something a server decides, not
//! something a client is told. It lived on `ShoalQuerySupport` until
//! [F15](../../../docs/src/features/client-server-split.md), which is why every `shoalctl` built
//! before then linked io_uring: a trait the client's query enum had to implement named two types
//! that only exist inside the engine.
//!
//! The queries themselves are wire types and live in the protocol crate. Only the routing does,
//! which is why the impls here are on foreign types.

use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::shared::queries::{SortedQuery, UnsortedQuery};
use crate::shared::traits::{ShoalSortedTable, ShoalUnsortedTable};

/// Group a queries partition keys by the shard that owns each of them
///
/// The keys are kept in the order they were asked for within each shard, so a narrowed query
/// reads its partitions in the same order the whole query would have. That is what lets the
/// shard collecting the shares put the rows back into the order the query named them in, and
/// what makes each shards own share of a limit the right rows to keep.
///
/// A key named twice is grouped once. Reading a partition twice would hand back each of its
/// rows twice, and every table below this counts on a key naming exactly one of its partitions.
///
/// # Arguments
///
/// * `ring` - The shard ring to check against
/// * `partition_keys` - The partition keys to group
fn group_by_shard<'a>(ring: &'a Ring, partition_keys: &[u64]) -> Vec<(&'a ShardInfo, Vec<u64>)> {
    // build the per shard groups we find
    let mut grouped: Vec<(&ShardInfo, Vec<u64>)> = Vec::with_capacity(1);
    // place each partition key with the shard that owns it
    for key in partition_keys {
        // a key we have already placed names a partition we are already reading
        if grouped.iter().any(|(_, keys)| keys.contains(key)) {
            continue;
        }
        // find the shard that owns this key
        let shard = ring.find_shard(*key);
        // add this key to that shards group, or start a group for it
        match grouped
            .iter_mut()
            .find(|(found, _)| found.mesh_id() == shard.mesh_id())
        {
            // this shard already owns one of our keys so add this one to it
            Some((_, keys)) => keys.push(*key),
            // this is the first key we have found for this shard
            None => grouped.push((shard, vec![*key])),
        }
    }
    grouped
}

/// Splitting a query into the per shard queries that answer it
///
/// Implemented by the generated `QueryKinds` of every `#[shoal::db]` schema, and by the two
/// query enums it wraps. A `#[shoal::db(client)]` schema implements none of them, which is what
/// lets a client be built without the engine.
pub trait ShardRouting: Sized {
    /// Split this query into the per shard queries that answer it
    ///
    /// A query naming several partition keys is only answerable by the shards that own
    /// those keys, so it is narrowed to each shards own keys rather than sent whole to
    /// every one of them. Shards are deduplicated too, so a shard owning two of the
    /// keys gets one query naming both instead of the same query twice.
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard queries we found for this query
    fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>);
}

impl<T: ShoalSortedTable + std::fmt::Debug> ShardRouting for SortedQuery<T> {
    /// Split this sorted query into the per shard queries that answer it
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard queries we found for this query
    fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>) {
        // get the correct shards for this query
        match self {
            SortedQuery::Insert { key, .. } | SortedQuery::Delete { key, .. } => {
                // a write names a single partition so it goes to a single shard
                found.push((ring.find_shard(*key), self.clone()));
            }
            SortedQuery::Get(get) => {
                // put this gets sort keys in the order the rows they name come back in
                let sort_select = get.sort_select.normalized();
                // narrow this get to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &get.partition_keys) {
                    let narrowed = get.for_partitions(keys, sort_select.clone());
                    found.push((shard, SortedQuery::Get(narrowed)));
                }
            }
            SortedQuery::Exists(exists) => {
                // drop any sort key this exists named more than once
                let sort_select = exists.sort_select.normalized();
                // narrow this exists to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &exists.partition_keys) {
                    let narrowed = exists.for_partitions(keys, sort_select.clone());
                    found.push((shard, SortedQuery::Exists(narrowed)));
                }
            }
            SortedQuery::Update(update) => {
                // an update names a single partition so it goes to a single shard
                found.push((ring.find_shard(update.partition_key), self.clone()));
            }
        }
    }
}

impl<T: ShoalUnsortedTable + std::fmt::Debug> ShardRouting for UnsortedQuery<T> {
    /// Split this unsorted query into the per shard queries that answer it
    ///
    /// Every unsorted query but a get names exactly one partition, so it goes to a single
    /// shard with nothing to narrow. A get may name several, so it is narrowed to each
    /// shards own keys the same way a sorted get is.
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard queries we found for this query
    fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>) {
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
}
