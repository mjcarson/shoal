//! Micro benchmarks for the routing layer
//!
//! Routing is the hop between a bundle arriving on a socket and the shards that can answer it,
//! and it is the one layer of the query path with no benchmark of any kind. `docs/src/appendix/todos.md`
//! has asked for this file since [F3](../../docs/src/features/performance-harness.md) and twice
//! recorded that nothing blocks it: `Ring::find_shard` and `split_by_shard` are pure CPU over plain
//! data, so unlike the table layer they need no server, no `LocalExecutor` and no storage backend.
//!
//! Two entries in `docs/src/appendix/optimizations.md` live here and neither could be adjudicated
//! before this file existed:
//!
//! * [O39] — `group_by_shard` deduplicates by scanning every key it has already placed, so routing
//!   a get is O(n²) in the partition keys it names. `split_by_shard/get` is that curve, and
//!   `split_by_shard/write` beside it is the control.
//! * [O20] — a sort key get reading a partition it may not need, which is decided one layer above
//!   `find_shard` but is priced against it.
//!
//! **`macro/fanout/{resident,evicted}/n` sweeps the same *n* and cannot replace this.** Every
//! sample there includes the wire, the tables, the response merge and the client, so a bend in that
//! curve has two candidate causes — this layer and [O13] — and no way to separate them. The key
//! counts below are deliberately the fanout workload's own, so the two can be read side by side.
//!
//! Unlike `partitions`, this reaches nothing crate private: `server::ring` and `ShardRouting` are
//! both public API. So it builds and runs with no features at all, the same way `wire` does.
//!
//! [O39]: ../../docs/src/appendix/optimizations.md
//! [O20]: ../../docs/src/appendix/optimizations.md
//! [O13]: ../../docs/src/appendix/optimizations.md

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use std::hint::black_box;

use shoal::server::ring::Ring;
use shoal::shared::queries::{ArchivedUnsortedQuery, UnsortedGet, UnsortedQuery};
use shoal::{
    ArchivedShardRouting, FileSystem, PersistentUnsortedTable, ShardRouting, ShoalUnsortedTable,
};

/// The shard counts every ring benchmark is run at
///
/// One is the degenerate ring, twelve is what `shoal.yml` resolves to on the benchmark host, and
/// the outer two bracket it. A tablet ring answers in constant time
/// ([Resolved #11/#12/#37](../../docs/src/appendix/resolved/tablet-ring.md)), so a curve that is
/// flat across these is the ring working and a curve that is not is a regression to the old
/// `BTreeMap`.
const SHARDS: [usize; 4] = [1, 4, 12, 64];

/// The partition key counts every split benchmark is run at
///
/// These are `macro/fanout/{resident,evicted}/n`'s own values, so the isolated cost measured here
/// and the end to end curve measured there describe the same points and can be read together.
const KEYS: [usize; 6] = [1, 2, 4, 16, 64, 256];

/// A row shaped like the narrowest thing a get can name
///
/// Routing never looks at a row's fields — only at its partition keys — so the row is kept as
/// small as it can be. A wide row here would measure the `clone` in the write arm rather than the
/// routing the benchmark is about.
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "RoutingDb")]
pub struct RoutedRow {
    /// The partition this row lands in
    #[shoal(partition)]
    pub key: u64,
    /// A payload the routing layer never reads
    pub value: u64,
}

/// The schema these benchmarks build their queries against
#[shoal::db]
pub struct RoutingDb {
    /// The one unsorted table these benchmarks use
    pub routed: PersistentUnsortedTable<RoutedRow, FileSystem>,
}

/// Build a get naming some number of distinct partition keys
///
/// The keys are distinct, which is the case `group_by_shard`'s dedup scan cannot short circuit
/// on — a get naming the same key *n* times bails on the first comparison every time and would
/// measure the opposite of what this is for.
///
/// # Arguments
///
/// * `count` - The number of partition keys this get should name
fn get(count: usize) -> UnsortedGet<RoutedRow> {
    UnsortedGet {
        partition_keys: (0..count as u64).collect(),
        filters: None,
        limit: None,
        projection: RoutedRowProjection::default(),
    }
}

/// Measure finding the shard that owns one partition key
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_find_shard(c: &mut Criterion) {
    // build the group every shard count is measured under
    let mut group = c.benchmark_group("routing/find_shard");
    // measure the lookup at each shard count
    for shards in SHARDS {
        // build a ring of this many shards
        let ring = Ring::new(shards).expect("a ring of this size is placeable");
        // one lookup answers one key, whatever the ring holds
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(shards), &shards, |b, _| {
            b.iter(|| black_box(ring.find_shard(black_box(0x9E37_79B9_7F4A_7C15))));
        });
    }
    group.finish();
}

/// Measure building a ring
///
/// This is what [Resolved #11/#12/#37](../../docs/src/appendix/resolved/tablet-ring.md) replaced —
/// a 1000×N `BTreeMap` became a 4096 entry tablet map — and it runs once per server start, so it
/// is here to bound a startup cost rather than because anything is waiting on it.
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_ring_new(c: &mut Criterion) {
    // build the group every shard count is measured under
    let mut group = c.benchmark_group("routing/ring_new");
    // measure construction at each shard count
    for shards in SHARDS {
        group.bench_with_input(BenchmarkId::from_parameter(shards), &shards, |b, &shards| {
            b.iter(|| black_box(Ring::new(black_box(shards)).expect("placeable")));
        });
    }
    group.finish();
}

/// Measure splitting a get across the shards that own its partitions
///
/// **This is the curve [O39] is about.** `group_by_shard` checks whether it has already placed a
/// key by scanning every key it has placed so far, so the cost should grow as the square of the
/// key count rather than linearly in it. Read it against `split_by_shard/write` below, which takes
/// the same code path with a single partition and therefore cannot bend.
///
/// [O39]: ../../docs/src/appendix/optimizations.md
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_split_get(c: &mut Criterion) {
    // build the group every key count is measured under
    let mut group = c.benchmark_group("routing/split_by_shard/get");
    // a twelve shard ring, which is what the benchmark host's shoal.yml resolves to
    let ring = Ring::new(12).expect("a twelve shard ring is placeable");
    // measure the split at each key count
    for keys in KEYS {
        // build the query this split is measured over
        let query = UnsortedQuery::<RoutedRow>::Get(get(keys));
        // the work is per key, so this is what makes the per element cost readable
        group.throughput(Throughput::Elements(keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |b, _| {
            // reuse one buffer across iterations, the way the shard loop does
            let mut found = Vec::with_capacity(12);
            b.iter(|| {
                // the caller clears the buffer rather than allocating a new one
                found.clear();
                query.split_by_shard(&ring, &mut found);
                black_box(found.len())
            });
        });
    }
    group.finish();
}

/// Measure splitting a write, which names exactly one partition
///
/// **The control.** A write takes `split_by_shard` down a branch that calls `find_shard` once and
/// never enters `group_by_shard`, so its cost is flat in everything the get arm varies. If this
/// moves when the get arm moves, the difference is not the dedup scan and the get arm is not
/// evidence about it — which is the check [O24](../../docs/src/appendix/optimizations.md) exists
/// to make somebody do.
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_split_write(c: &mut Criterion) {
    // build the group this control is measured under
    let mut group = c.benchmark_group("routing/split_by_shard/write");
    // the same twelve shard ring the get arm splits against
    let ring = Ring::new(12).expect("a twelve shard ring is placeable");
    // the same key counts, none of which a write can actually name more than one of
    for keys in KEYS {
        // a write names one partition however many the get beside it named
        let query = UnsortedQuery::<RoutedRow>::Insert {
            key: 0,
            row: RoutedRow { key: 0, value: 0 },
        };
        // one partition is one element, which is the point of the control
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |b, _| {
            // the same reused buffer the get arm uses
            let mut found = Vec::with_capacity(12);
            b.iter(|| {
                // the caller clears the buffer rather than allocating a new one
                found.clear();
                query.split_by_shard(&ring, &mut found);
                black_box(found.len())
            });
        });
    }
    group.finish();
}

/// Measure routing a get without deserializing it
///
/// **This is the live path**, and `routing/split_by_shard/get` above is the same decision over a
/// query that has already been deserialized. The coordinator no longer builds one
/// ([F26](../../docs/src/features/archive-routed-requests.md)): it reads the partition keys
/// straight out of the archive and hands each shard the buffer plus the keys it owns. So the
/// difference between the two groups at a given key count is what routing from the archive is
/// worth per query, before the deserialize this moved off the coordinator is counted at all.
///
/// The same O(n²) `group_by_shard` sits under both, so [O39] bends this curve exactly as it bends
/// the one above. That is deliberate — a change to the dedup scan has to show in both.
///
/// [O39]: ../../docs/src/appendix/optimizations.md
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_route_archived_get(c: &mut Criterion) {
    // build the group every key count is measured under
    let mut group = c.benchmark_group("routing/route_archived/get");
    // the same twelve shard ring the split arms measure against
    let ring = Ring::new(12).expect("a twelve shard ring is placeable");
    // measure routing at each key count
    for keys in KEYS {
        // build the query this routing is measured over, and archive it the way the wire does
        let query = UnsortedQuery::<RoutedRow>::Get(get(keys));
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("a query archives");
        // read it back the way a shard does, which is the form this benchmark is about
        let archived =
            rkyv::access::<ArchivedUnsortedQuery<RoutedRow>, rkyv::rancor::Error>(&bytes)
                .expect("an archived query is readable");
        // the work is per key, so this is what makes the per element cost readable
        group.throughput(Throughput::Elements(keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |b, _| {
            // reuse one buffer across iterations, the way the routing loop does
            let mut found = Vec::with_capacity(12);
            b.iter(|| {
                // the caller clears the buffer rather than allocating a new one
                found.clear();
                UnsortedQuery::<RoutedRow>::route_archived(archived, &ring, &mut found);
                black_box(found.len())
            });
        });
    }
    group.finish();
}

/// Measure routing a write without deserializing it
///
/// **The control**, for the same reason `routing/split_by_shard/write` is one: a write names a
/// single partition and never enters `group_by_shard`, so it is flat in everything the get arm
/// varies. Against that write arm it is also the cleanest reading of what this change is worth —
/// both route one key to one shard, and the difference is that this one never touched the row.
///
/// # Arguments
///
/// * `c` - The criterion harness to add this benchmark to
fn bench_route_archived_write(c: &mut Criterion) {
    // build the group this control is measured under
    let mut group = c.benchmark_group("routing/route_archived/write");
    // the same twelve shard ring every other arm here measures against
    let ring = Ring::new(12).expect("a twelve shard ring is placeable");
    // the same key counts, none of which a write can actually name more than one of
    for keys in KEYS {
        // a write names one partition however many the get beside it named
        let query = UnsortedQuery::<RoutedRow>::Insert {
            key: 0,
            row: RoutedRow { key: 0, value: 0 },
        };
        // archive it and read it back, the way the wire and then the coordinator do
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("a query archives");
        let archived =
            rkyv::access::<ArchivedUnsortedQuery<RoutedRow>, rkyv::rancor::Error>(&bytes)
                .expect("an archived query is readable");
        // one partition is one element, which is the point of the control
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |b, _| {
            // the same reused buffer every other arm uses
            let mut found = Vec::with_capacity(12);
            b.iter(|| {
                // the caller clears the buffer rather than allocating a new one
                found.clear();
                UnsortedQuery::<RoutedRow>::route_archived(archived, &ring, &mut found);
                black_box(found.len())
            });
        });
    }
    group.finish();
}

criterion_group!(
    routing,
    bench_find_shard,
    bench_ring_new,
    bench_split_get,
    bench_split_write,
    bench_route_archived_get,
    bench_route_archived_write,
);
criterion_main!(routing);
