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

use rkyv::vec::ArchivedVec;

use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::shared::queries::{
    ArchivedSortedQuery, ArchivedUnsortedQuery, SortedQuery, UnsortedQuery,
};
use crate::shared::traits::{ShoalSortedTable, ShoalUnsortedTable};

/// Read a queries partition keys out of the archive they arrived in
///
/// A `u64` is stored little endian in an archive whatever the host is, so there is no slice of
/// native keys in there to borrow and each one has to be read out. They are scalars sitting
/// inline in the buffer, so this walks bytes the coordinator has already touched and allocates
/// once for the whole set - which is what makes routing a bundle of megabyte rows cost the
/// keys rather than the rows.
///
/// # Arguments
///
/// * `keys` - The archived partition keys to read
fn native_keys(keys: &ArchivedVec<rkyv::rend::u64_le>) -> Vec<u64> {
    // read each key back into the endianness this host works in
    keys.iter().map(|key| key.to_native()).collect()
}

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

/// Routing a query that is still in the buffer it arrived in
///
/// This is the live routing path. [`ShardRouting`] describes the same decision over a
/// deserialized query and is kept as the reference implementation the tests check this
/// against, but the coordinator no longer builds one: it validates the bundle, reads the
/// partition keys straight out of the archive, and hands each shard the shared buffer plus
/// the keys it owns ([F26](../../../docs/src/features/archive-routed-requests.md)).
///
/// Nothing here touches a row, a filter or a sort key. Every field it reads is a `u64` or an
/// `Option<usize>` sitting inline in the archive, which is the whole reason the coordinator
/// can route a bundle of megabyte rows without allocating.
pub trait ArchivedShardRouting: rkyv::Archive + Sized {
    /// Find the shards that answer this query, and the keys each of them owns
    ///
    /// A shard is pushed with `Some(keys)` when the query was narrowed to a subset of the
    /// partitions it named, and with `None` when the shard answers the query as it stands.
    /// `None` is not the same as "every key": it means the executing shard must not narrow,
    /// which is what every write needs, since a write names its partition in a field the
    /// narrowing does not touch.
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard shares we found for this query
    fn route_archived<'a>(
        archived: &<Self as rkyv::Archive>::Archived,
        ring: &'a Ring,
        found: &mut Vec<(&'a ShardInfo, Option<Vec<u64>>)>,
    );

    /// Get the partitions this query named, in the order it named them
    ///
    /// The owned form hands back a borrowed slice, but there is no slice of `u64` in an
    /// archive to borrow - the keys are stored little endian and have to be read out one at a
    /// time - so this allocates. It is only called for a query that really was split, which is
    /// the only case whose shares have to be put back into the order the query named.
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_partition_keys(archived: &<Self as rkyv::Archive>::Archived) -> Vec<u64>;

    /// Get the most rows this query asked for, if it set a limit
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_limit(archived: &<Self as rkyv::Archive>::Archived) -> Option<usize>;

    /// Narrow this query to the partitions the shard executing it owns
    ///
    /// This is the other half of [`ArchivedShardRouting::route_archived`], run on the shard
    /// that will answer rather than on the coordinator. Splitting the decision from the
    /// narrowing is what lets the query be deserialized once, on the shard that needs it,
    /// instead of once on the coordinator and again per shard it was split to.
    ///
    /// # Arguments
    ///
    /// * `keys` - The partition keys this shard owns
    fn narrow_to(self, keys: Vec<u64>) -> Self;
}

impl<T: ShoalSortedTable + std::fmt::Debug> ArchivedShardRouting for SortedQuery<T> {
    /// Find the shards that answer this sorted query, and the keys each of them owns
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard shares we found for this query
    fn route_archived<'a>(
        archived: &<Self as rkyv::Archive>::Archived,
        ring: &'a Ring,
        found: &mut Vec<(&'a ShardInfo, Option<Vec<u64>>)>,
    ) {
        // get the correct shards for this query
        match archived {
            ArchivedSortedQuery::Insert { key, .. } | ArchivedSortedQuery::Delete { key, .. } => {
                // a write names a single partition so it goes to a single shard, unnarrowed
                found.push((ring.find_shard(key.to_native()), None));
            }
            ArchivedSortedQuery::Get(get) => {
                // narrow this get to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &native_keys(&get.partition_keys)) {
                    found.push((shard, Some(keys)));
                }
            }
            ArchivedSortedQuery::Exists(exists) => {
                // narrow this exists to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &native_keys(&exists.partition_keys)) {
                    found.push((shard, Some(keys)));
                }
            }
            ArchivedSortedQuery::Update(update) => {
                // an update names a single partition so it goes to a single shard
                found.push((ring.find_shard(update.partition_key.to_native()), None));
            }
        }
    }

    /// Get the partitions this sorted query named, in the order it named them
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_partition_keys(archived: &<Self as rkyv::Archive>::Archived) -> Vec<u64> {
        // only a get returns rows whose order this could describe
        match archived {
            ArchivedSortedQuery::Get(get) => native_keys(&get.partition_keys),
            _ => Vec::new(),
        }
    }

    /// Get the most rows this sorted query asked for, if it set a limit
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_limit(archived: &<Self as rkyv::Archive>::Archived) -> Option<usize> {
        // only a get returns rows that a limit could apply to
        match archived {
            ArchivedSortedQuery::Get(get) => get.limit.as_ref().map(|limit| limit.to_native() as usize),
            _ => None,
        }
    }

    /// Narrow this sorted query to the partitions the shard executing it owns
    ///
    /// The sort key selection is normalized here rather than on the coordinator, since the
    /// coordinator never deserializes one. That is once per shard a get was split to instead
    /// of once per get, and it is the one thing this design pays for rather than saves.
    ///
    /// # Arguments
    ///
    /// * `keys` - The partition keys this shard owns
    fn narrow_to(self, keys: Vec<u64>) -> Self {
        // only the two multi partition queries have anything to narrow
        match self {
            SortedQuery::Get(get) => {
                // put this gets sort keys in the order the rows they name come back in
                let sort_select = get.sort_select.normalized();
                SortedQuery::Get(get.for_partitions(keys, sort_select))
            }
            SortedQuery::Exists(exists) => {
                // drop any sort key this exists named more than once
                let sort_select = exists.sort_select.normalized();
                SortedQuery::Exists(exists.for_partitions(keys, sort_select))
            }
            // a write named one partition and was routed by it, so there is nothing to narrow
            other => other,
        }
    }
}

impl<T: ShoalUnsortedTable + std::fmt::Debug> ArchivedShardRouting for UnsortedQuery<T> {
    /// Find the shards that answer this unsorted query, and the keys each of them owns
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    /// * `ring` - The shard ring to check against
    /// * `found` - The per shard shares we found for this query
    fn route_archived<'a>(
        archived: &<Self as rkyv::Archive>::Archived,
        ring: &'a Ring,
        found: &mut Vec<(&'a ShardInfo, Option<Vec<u64>>)>,
    ) {
        // get the correct shard for this query, or push every shard a get was split to
        let shard = match archived {
            ArchivedUnsortedQuery::Insert { key, .. }
            | ArchivedUnsortedQuery::Delete { key, .. } => ring.find_shard(key.to_native()),
            ArchivedUnsortedQuery::Get(get) => {
                // narrow this get to each shards own partition keys
                for (shard, keys) in group_by_shard(ring, &native_keys(&get.partition_keys)) {
                    found.push((shard, Some(keys)));
                }
                return;
            }
            ArchivedUnsortedQuery::Update(update) => {
                ring.find_shard(update.partition_key.to_native())
            }
            ArchivedUnsortedQuery::Exists(exists) => {
                ring.find_shard(exists.partition_key.to_native())
            }
        };
        found.push((shard, None));
    }

    /// Get the partitions this unsorted query named, in the order it named them
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_partition_keys(archived: &<Self as rkyv::Archive>::Archived) -> Vec<u64> {
        // only a get returns rows whose order this could describe
        match archived {
            ArchivedUnsortedQuery::Get(get) => native_keys(&get.partition_keys),
            _ => Vec::new(),
        }
    }

    /// Get the most rows this unsorted query asked for, if it set a limit
    ///
    /// # Arguments
    ///
    /// * `archived` - The query, still in the buffer it arrived in
    fn archived_limit(archived: &<Self as rkyv::Archive>::Archived) -> Option<usize> {
        // only a get returns rows that a limit could apply to
        match archived {
            ArchivedUnsortedQuery::Get(get) => {
                get.limit.as_ref().map(|limit| limit.to_native() as usize)
            }
            _ => None,
        }
    }

    /// Narrow this unsorted query to the partitions the shard executing it owns
    ///
    /// # Arguments
    ///
    /// * `keys` - The partition keys this shard owns
    fn narrow_to(self, keys: Vec<u64>) -> Self {
        // only a get names more than one partition, so only a get has anything to narrow
        match self {
            UnsortedQuery::Get(get) => UnsortedQuery::Get(get.for_partitions(keys)),
            // every other unsorted query named one partition and was routed by it
            other => other,
        }
    }
}
