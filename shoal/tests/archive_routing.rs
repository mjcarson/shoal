//! Tests for routing a query without deserializing it
//!
//! The coordinator no longer builds a query to route it
//! ([F26](../../docs/src/features/archive-routed-requests.md)). It validates the bundle, reads
//! the partition keys straight out of the archive, and hands each shard the buffer plus the keys
//! it owns; the shard that executes the query is the one that turns it back into a query and
//! narrows it.
//!
//! That splits one function into three - `route_archived`, `deserialize_query` and `narrow_to` -
//! and the risk is entirely in whether the three of them together still say what
//! `split_by_shard` said on its own. So `split_by_shard` is kept as the reference implementation
//! and every test here checks the new path against it rather than against a hardcoded answer. A
//! change to how keys are placed moves both sides and these still pass, which is the point: they
//! are about agreement, not about placement.
//!
//! Nothing here starts a server. `Ring`, the routing traits and rkyv are all pure CPU over plain
//! data, the same property `shoal/benches/routing.rs` relies on.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::ring::Ring;
use shoal::shared::queries::{
    ArchivedSortedQuery, ArchivedUnsortedQuery, SortSelect, SortedGet, SortedQuery, UnsortedGet,
    UnsortedQuery,
};
use shoal::tables::{EphemeralSortedTable, EphemeralUnsortedTable};
use shoal::{ArchivedShardRouting, ShardRouting};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};

/// A row in the unsorted table these tests route against
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "RouteDb")]
pub struct FlatRow {
    /// The partition this row lands in
    #[shoal(partition)]
    pub key: u64,
    /// A payload no routing decision ever reads
    #[shoal(update)]
    pub value: String,
}

/// A row in the sorted table these tests route against
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "RouteDb")]
pub struct TieredRow {
    /// The partition this row lands in
    #[shoal(partition)]
    pub key: u64,
    /// The key this row is sorted by within its partition
    #[shoal(sort)]
    pub sort_key: String,
    /// A payload no routing decision ever reads
    #[shoal(update)]
    pub value: String,
}

/// A schema with one table of each kind, so both query enums are routed
#[db]
pub struct RouteDb {
    /// The unsorted table, where a partition holds one row
    pub flat: EphemeralUnsortedTable<FlatRow>,
    /// The sorted table, where a partition holds many
    pub tiered: EphemeralSortedTable<TieredRow>,
}

/// The shard count every test here routes against
///
/// Twelve is what the benchmark host's `shoal.yml` resolves to, so a get naming a handful of
/// keys really does land on several shards rather than trivially on one.
const SHARDS: usize = 12;

/// The bound `rkyv::to_bytes` needs, spelled once
///
/// `RkyvSupport` carries the same one, but its impl for the two query enums is conditional on
/// the row's filter set and does not hold for every row a schema can declare - so the archives
/// here are taken through rkyv directly rather than through a trait that may not be there.
trait Archivable:
    for<'a> Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >
{
}

impl<T> Archivable for T where
    T: for<'a> Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >
{
}

/// Archive a query the way a client does
///
/// # Arguments
///
/// * `query` - The query to archive
fn archive<Q: Archivable>(query: &Q) -> Vec<u8> {
    // archive it the way the wire does
    rkyv::to_bytes::<rkyv::rancor::Error>(query)
        .expect("a query archives")
        .to_vec()
}

/// Compare two sets of narrowed queries by what they would put on the wire
///
/// A query has no `PartialEq` - it holds a row, a filter set and a projection, none of which
/// are required to be comparable - so the two paths are compared by their archives instead.
/// Two queries with the same bytes are the same query by every definition that matters here,
/// and this is stricter than a field by field check rather than looser.
///
/// # Arguments
///
/// * `left` - One set of narrowed queries
/// * `right` - The other set
fn same_queries<Q: Archivable>(left: &[Q], right: &[Q]) -> bool {
    // the same number of shares, each archiving to the same bytes
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(one, other)| archive(one) == archive(other))
}

/// The shards a routing decision named, in the order it named them
///
/// Reduced to mesh ids because that is what the two paths have in common: one hands back
/// narrowed queries and the other hands back key sets, and the thing being compared is which
/// shards were chosen and in what order.
///
/// # Arguments
///
/// * `found` - The per shard shares a routing decision produced
fn shards_of<T>(found: &[(&shoal::server::shard::ShardInfo, T)]) -> Vec<usize> {
    // keep only which shard each share went to
    found.iter().map(|(shard, _)| shard.mesh_id()).collect()
}

/// Route one unsorted query both ways and hand back what each said
///
/// # Arguments
///
/// * `ring` - The ring to route against
/// * `query` - The query to route
fn both_ways_unsorted(
    ring: &Ring,
    query: &UnsortedQuery<FlatRow>,
) -> (Vec<usize>, Vec<UnsortedQuery<FlatRow>>, Vec<usize>, Vec<UnsortedQuery<FlatRow>>) {
    // route it the old way, which narrows as it splits
    let mut split = Vec::new();
    query.split_by_shard(ring, &mut split);
    let split_shards = shards_of(&split);
    let split_queries: Vec<_> = split.into_iter().map(|(_, query)| query).collect();
    // route it the way the coordinator does, off the archive alone
    let bytes = archive(query);
    let archived = rkyv::access::<ArchivedUnsortedQuery<FlatRow>, rkyv::rancor::Error>(&bytes)
        .expect("an archived query is readable");
    let mut routed = Vec::new();
    UnsortedQuery::<FlatRow>::route_archived(archived, ring, &mut routed);
    let routed_shards = shards_of(&routed);
    // narrow each share the way the shard executing it does
    let routed_queries: Vec<_> = routed
        .into_iter()
        .map(|(_, keys)| {
            // the query is deserialized on the shard that answers it, then narrowed
            let query: UnsortedQuery<FlatRow> =
                rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("a query deserializes");
            match keys {
                Some(keys) => query.narrow_to(keys),
                None => query,
            }
        })
        .collect();
    (split_shards, split_queries, routed_shards, routed_queries)
}

/// Route one sorted query both ways and hand back what each said
///
/// # Arguments
///
/// * `ring` - The ring to route against
/// * `query` - The query to route
fn both_ways_sorted(
    ring: &Ring,
    query: &SortedQuery<TieredRow>,
) -> (Vec<usize>, Vec<SortedQuery<TieredRow>>, Vec<usize>, Vec<SortedQuery<TieredRow>>) {
    // route it the old way, which narrows as it splits
    let mut split = Vec::new();
    query.split_by_shard(ring, &mut split);
    let split_shards = shards_of(&split);
    let split_queries: Vec<_> = split.into_iter().map(|(_, query)| query).collect();
    // route it the way the coordinator does, off the archive alone
    let bytes = archive(query);
    let archived = rkyv::access::<ArchivedSortedQuery<TieredRow>, rkyv::rancor::Error>(&bytes)
        .expect("an archived query is readable");
    let mut routed = Vec::new();
    SortedQuery::<TieredRow>::route_archived(archived, ring, &mut routed);
    let routed_shards = shards_of(&routed);
    // narrow each share the way the shard executing it does
    let routed_queries: Vec<_> = routed
        .into_iter()
        .map(|(_, keys)| {
            // the query is deserialized on the shard that answers it, then narrowed
            let query: SortedQuery<TieredRow> =
                rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("a query deserializes");
            match keys {
                Some(keys) => query.narrow_to(keys),
                None => query,
            }
        })
        .collect();
    (split_shards, split_queries, routed_shards, routed_queries)
}

/// Build an unsorted get naming some number of distinct partition keys
///
/// # Arguments
///
/// * `count` - How many partitions this get should name
fn flat_get(count: u64) -> UnsortedQuery<FlatRow> {
    UnsortedQuery::Get(UnsortedGet {
        partition_keys: (0..count).collect(),
        filters: None,
        limit: None,
        projection: FlatRowProjection::default(),
    })
}

/// Build a sorted get naming some partitions and some rows of each
///
/// # Arguments
///
/// * `count` - How many partitions this get should name
/// * `sort_select` - Which rows of each partition it asks for
fn tiered_get(count: u64, sort_select: SortSelect<String>) -> SortedQuery<TieredRow> {
    SortedQuery::Get(SortedGet {
        partition_keys: (0..count).collect(),
        sort_select,
        filters: None,
        limit: None,
        projection: TieredRowProjection::default(),
    })
}

#[test]
/// A get routed off its archive lands on the same shards, carrying the same keys
///
/// This is the whole property the change rests on. `split_by_shard` narrowed as it split;
/// `route_archived` decides and `narrow_to` narrows, on two different cores, and the two halves
/// together have to be indistinguishable from the one function they replaced.
fn a_get_routes_the_same_way_off_its_archive() {
    let ring = Ring::new(SHARDS).expect("a twelve shard ring is placeable");
    // walk key counts from one partition up past the shard count, so gets that land on one
    // shard and gets that land on all of them are both covered
    for count in [1, 2, 4, 12, 16, 64] {
        let query = flat_get(count);
        let (split_shards, split_queries, routed_shards, routed_queries) =
            both_ways_unsorted(&ring, &query);
        // the same shards answer, in the same order
        assert_eq!(
            split_shards, routed_shards,
            "a get naming {count} keys chose different shards off its archive"
        );
        // and each of them was handed the same query
        assert!(
            same_queries(&split_queries, &routed_queries),
            "a get naming {count} keys was narrowed differently off its archive"
        );
    }
}

#[test]
/// A sorted get routed off its archive lands on the same shards, carrying the same rows
fn a_sorted_get_routes_the_same_way_off_its_archive() {
    let ring = Ring::new(SHARDS).expect("a twelve shard ring is placeable");
    // every arm of the row selection, since only one of them is normalized
    let selections = [
        SortSelect::All,
        SortSelect::Keys(vec!["b".to_string(), "a".to_string()]),
    ];
    for sort_select in selections {
        for count in [1, 4, 16, 64] {
            let query = tiered_get(count, sort_select.clone());
            let (split_shards, split_queries, routed_shards, routed_queries) =
                both_ways_sorted(&ring, &query);
            assert_eq!(
                split_shards, routed_shards,
                "a sorted get naming {count} keys chose different shards off its archive"
            );
            assert!(
                same_queries(&split_queries, &routed_queries),
                "a sorted get naming {count} keys was narrowed differently off its archive"
            );
        }
    }
}

#[test]
/// A get whose sort keys arrive unsorted and repeated is still normalized
///
/// The normalization moved from the coordinator to the shard that executes the query, since the
/// coordinator never deserializes one. It has to still happen, and it has to happen exactly
/// once - a key named twice would otherwise seek its row twice and answer it twice.
fn sort_keys_are_still_normalized_after_narrowing() {
    let ring = Ring::new(SHARDS).expect("a twelve shard ring is placeable");
    // a selection naming its keys out of order, with one of them named twice
    let messy = SortSelect::Keys(vec![
        "c".to_string(),
        "a".to_string(),
        "c".to_string(),
        "b".to_string(),
    ]);
    let query = tiered_get(16, messy);
    let (_, _, _, routed_queries) = both_ways_sorted(&ring, &query);
    // every share this get was split into carries the keys in sort order, once each
    for narrowed in &routed_queries {
        let SortedQuery::Get(get) = narrowed else {
            panic!("a get narrowed into something that is not a get");
        };
        assert_eq!(
            get.sort_select.keys(),
            Some(["a".to_string(), "b".to_string(), "c".to_string()].as_slice()),
            "a narrowed get did not normalize its sort keys"
        );
    }
}

#[test]
/// Every write routes to one shard and is handed on unnarrowed
///
/// A write names its partition in a field the narrowing does not reach, so `route_archived` has
/// to answer `None` rather than a key set. Answering `Some` would send it through `narrow_to`,
/// which for a write is a no-op today - so this is checking the shape of the message rather than
/// an observable difference, which is exactly the kind of thing that stops being true quietly.
fn a_write_is_routed_to_one_shard_and_never_narrowed() {
    let ring = Ring::new(SHARDS).expect("a twelve shard ring is placeable");
    // one of every unsorted query that is not a get
    let writes = [
        UnsortedQuery::Insert {
            key: 7,
            row: FlatRow {
                key: 7,
                value: "payload".to_string(),
            },
        },
        UnsortedQuery::Delete { key: 7 },
    ];
    for query in &writes {
        let bytes = archive(query);
        let archived = rkyv::access::<ArchivedUnsortedQuery<FlatRow>, rkyv::rancor::Error>(&bytes)
            .expect("an archived query is readable");
        let mut routed = Vec::new();
        UnsortedQuery::<FlatRow>::route_archived(archived, &ring, &mut routed);
        // exactly one shard answers a write
        assert_eq!(routed.len(), 1, "a write was routed to more than one shard");
        // and it is handed the query as it stands
        assert!(
            routed[0].1.is_none(),
            "a write was routed with keys to narrow to"
        );
        // which is the shard the old path chose too
        let mut split = Vec::new();
        query.split_by_shard(&ring, &mut split);
        assert_eq!(shards_of(&split), shards_of(&routed));
    }
}

#[test]
/// A query's limit and partition order survive being read off the archive
///
/// These are the two things the coordinator still reads out of a query, and it reads them for
/// the gather - the record of what a split query is owed. Getting either wrong reorders or
/// truncates a client's rows rather than failing anything, so they are checked against the
/// deserialized query they are supposed to agree with.
fn a_gets_limit_and_partition_order_read_the_same_off_the_archive() {
    // a get naming its partitions in an order that is not sorted, with a limit set
    let query = UnsortedQuery::<FlatRow>::Get(UnsortedGet {
        partition_keys: vec![9, 3, 7, 1],
        filters: None,
        limit: Some(11),
        projection: FlatRowProjection::default(),
    });
    let bytes = archive(&query);
    let archived = rkyv::access::<ArchivedUnsortedQuery<FlatRow>, rkyv::rancor::Error>(&bytes)
        .expect("an archived query is readable");
    // the order the query named its partitions in is the order the rows come back in
    assert_eq!(
        UnsortedQuery::<FlatRow>::archived_partition_keys(archived),
        vec![9, 3, 7, 1],
        "the partition order read off the archive is not the order the get named"
    );
    // and the limit is what the shard collecting the shares trims their union down to
    assert_eq!(
        UnsortedQuery::<FlatRow>::archived_limit(archived),
        Some(11),
        "the limit read off the archive is not the limit the get set"
    );
}
