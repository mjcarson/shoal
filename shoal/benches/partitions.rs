//! Micro benchmarks for the partition layer
//!
//! This is the hottest CPU bound code in Shoal and the code most of the entries in
//! `docs/src/appendix/optimizations.md` are about. It is benchmarked here rather than through
//! the `tmdb` example because the end to end harness cannot resolve anything smaller than
//! about a third of its own wall clock, which is far larger than any of those entries claim
//! to be worth.
//!
//! These reach `SortedPartition` through `shoal::server::tables::bench_exports`, which
//! only exists under the `bench` feature. See `docs/src/features/performance-harness.md`.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::hint::black_box;
use std::ops::Bound;

use shoal::shared::queries::{SortRange, SortSelect, SortedExists, SortedGet};
use shoal::shared::traits::RkyvSupport;
use shoal::traits::ShoalProjection;
use shoal::{FileSystem, PersistentSortedTable, ShoalSortedTable};
use shoal::server::tables::bench_exports::{
    MaybeLoaded, RowSink, SeekBytes, SortedPartition, ValidatedArchive,
};

/// The partition sizes every scan benchmark is run at
///
/// These bracket what the `tmdb` workload actually produces. A keyword partition there holds
/// tens to low thousands of titles, and the shape of a scan changes with that count: at 16 the
/// cost is dominated by the call, at 4096 by the walk.
const SIZES: [usize; 4] = [16, 256, 1024, 4096];

/// A row shaped like the sorted table the `tmdb` benchmark writes most of its rows to
///
/// `MovieByKeyword` is roughly 88% of the inserts in that workload, so a micro benchmark that
/// does not look like it is measuring the wrong thing. Two short strings, sorted by the second.
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "BenchDb")]
pub struct TitleByKeyword {
    /// The keyword this title is filed under, which is its partition
    #[shoal(partition)]
    pub keyword: String,
    /// The title itself, which is what the partition is sorted by
    #[shoal(sort)]
    pub title: String,
}

/// A projection that names every field its row has
///
/// This is not a projection anybody would write. It exists to measure what an archived get cost
/// **before** [F28](../../docs/src/features/rearchived-rows.md): a projection has no archived
/// value of its own to point at, so it is materialized per row however its partition is held,
/// which is what every archived get used to do. Naming every field makes the row it builds the
/// same row `get_all` now points at, so the two arms differ in the copy and in nothing else.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, shoal::ShoalProjection, PartialEq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "TitleByKeyword")]
pub struct WholeRow {
    /// The keyword this title is filed under, which is its partition
    #[shoal(partition)]
    pub keyword: String,
    /// The title itself, which is what the partition is sorted by
    pub title: String,
}

/// The schema these benchmarks build their queries against
#[shoal::db]
pub struct BenchDb {
    /// The one sorted table these benchmarks use
    #[shoal(projections(WholeRow))]
    pub title_by_keyword: PersistentSortedTable<TitleByKeyword, FileSystem>,
}

/// Build a sort key that sorts in the same order as the number it came from
///
/// Zero padding matters. `"9"` sorts after `"10"`, so an unpadded key would make the range
/// benchmarks walk a different number of rows than their bounds suggest.
///
/// # Arguments
///
/// * `index` - The position this key should sort at
fn sort_key(index: usize) -> String {
    format!("title-{index:08}")
}

/// Build a row at a position in the sort order
///
/// # Arguments
///
/// * `index` - The position this rows sort key should sort at
fn row(index: usize) -> TitleByKeyword {
    TitleByKeyword {
        keyword: "keyword".to_owned(),
        title: sort_key(index),
    }
}

/// Build a partition already holding a number of rows
///
/// # Arguments
///
/// * `size` - The number of rows to fill this partition with
fn filled(size: usize) -> SortedPartition<TitleByKeyword> {
    // start from an empty partition under a fixed key
    let mut partition = SortedPartition::new(0);
    // fill it in sort order
    for index in 0..size {
        partition.insert(row(index));
    }
    partition
}

/// Build a get that selects rows a particular way
///
/// # Arguments
///
/// * `select` - How this get should select its rows
fn get_for(select: SortSelect<String>) -> SortedGet<TitleByKeyword> {
    SortedGet {
        partition_keys: vec![0],
        sort_select: select,
        filters: None,
        limit: None,
        projection: TitleByKeywordProjection::default(),
    }
}

/// Build an exists that asks about rows a particular way
///
/// # Arguments
///
/// * `select` - Which rows this exists should ask about
fn exists_for(select: SortSelect<String>) -> SortedExists<TitleByKeyword> {
    SortedExists {
        partition_keys: vec![0],
        sort_select: select,
        filters: None,
    }
}

/// Wrap a filled partition the way an evicted one is held after it is read back
///
/// The buffer is an [`AlignedVec`] rather than the glommio `ReadResult` a real read
/// produces, because a `ReadResult` can only come from a live reactor. That is the whole
/// reason [`MaybeLoaded`] carries its buffer as a type parameter - see the note on
/// [`bench_maybe_loaded`].
///
/// # Arguments
///
/// * `size` - The number of rows the partition should hold
fn accessible(size: usize) -> MaybeLoaded<SortedPartition<TitleByKeyword>, AlignedVec> {
    // archive the partition the way a compaction would have written it
    let raw = <SortedPartition<TitleByKeyword> as RkyvSupport>::serialize(&filled(size));
    // hold it as an archive rather than as rows, which is what an evicted partition is
    //
    // validating here rather than at every read is the whole of F4, and it is why this
    // is the helper the group below shares: the cost it moved is now paid once, out here
    MaybeLoaded::Accessible(ValidatedArchive::new(raw).unwrap())
}

/// Measure inserting a row into partitions of different sizes
///
/// This is the single hottest call in a write heavy workload. It is timed with a fresh
/// partition per iteration, because inserting into the same partition repeatedly would
/// measure a `BTreeMap` that grows without bound rather than one of a fixed size.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_insert(c: &mut Criterion) {
    // build a group so every size shares an axis
    let mut group = c.benchmark_group("partition_sorted/insert");
    // measure an insert into a partition of each size
    for size in SIZES {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched_ref(
                // each iteration gets its own partition of this size
                || filled(size),
                // insert one more row past the end of it
                |partition| black_box(partition.insert(row(size))),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Measure a get that names one sort key against partitions of different sizes
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_get_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/get_key");
    for size in SIZES {
        // seek a key in the middle so the walk is not trivially short at either end
        let get = get_for(SortSelect::Keys(vec![sort_key(size / 2)]));
        let partition = filled(size);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                // collect into a fresh vec so we are not measuring a growing allocation
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                partition.get(black_box(&get), &mut found);
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Measure a get that walks a whole partition
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_get_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/get_all");
    for size in SIZES {
        let get = get_for(SortSelect::All);
        let partition = filled(size);
        // every row is visited and returned, so the row count is the unit of work
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                partition.get(black_box(&get), &mut found);
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Measure a get bounded by a range of sort keys
///
/// This is what [F1](../../docs/src/features/sort-key-ranges.md) added, and the claim it rests
/// on is that paging a partition costs a page rather than a partition. The range here is a
/// fixed 64 rows whatever the partition holds, so that claim is visible as a flat line across
/// the sizes rather than something that has to be argued from the source.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_get_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/get_range_64");
    for size in SIZES {
        // take a fixed width window out of the middle of the partition
        let start = size / 4;
        let get = get_for(SortSelect::Range(SortRange {
            start: Bound::Included(sort_key(start)),
            end: Bound::Excluded(sort_key(start + 64)),
        }));
        let partition = filled(size);
        group.throughput(Throughput::Elements(64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                partition.get(black_box(&get), &mut found);
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Measure the archived read path against the in memory one
///
/// A partition that has been evicted is read where it lies rather than deserialized, so this
/// is the cost a get pays for a cold partition once the read itself has landed. Holding it
/// next to `get_key` and `get_all` is the point: the pairs say what eviction costs a reader.
///
/// This goes through `ArchivedSortedPartition` by hand rather than through
/// `MaybeLoaded::Accessible`, which [`bench_maybe_loaded`] now does. Both are kept, and the
/// split between them is the point: this group calls `RkyvSupport::access` directly, so it
/// measures the codec and nothing the table layer does around it. That makes it the
/// **control** for any change to how a partition is held — if this moves, the machine moved.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_archived_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/archived");
    for size in SIZES {
        // archive a partition of this size, as a compaction would have written it
        let raw = <SortedPartition<TitleByKeyword> as RkyvSupport>::serialize(&filled(size));
        // accessing an archive and projecting one row out, which is what a keyed cold get does
        //
        // this does not stay flat as the partition grows, and it is worth being clear about
        // why: `access` validates the whole buffer before anything can be sought in it, so a
        // cold get pays for the size of the partition it landed in even when it wants one row
        // of it. The seek that follows is the cheap half. See `codec/access` for the split.
        group.bench_with_input(BenchmarkId::new("access_and_one_row", size), &size, |b, _| {
            b.iter(|| {
                // access the archive the way a cold read does
                let access =
                    <SortedPartition<TitleByKeyword> as RkyvSupport>::access(black_box(&raw))
                        .unwrap();
                // walk to the one row this get named and project it out of the archive
                let mut found: Vec<TitleByKeyword> = Vec::with_capacity(1);
                for row in access.live_row_values() {
                    found.push(<TitleByKeyword as ShoalProjection>::from_archived(row));
                    break;
                }
                black_box(found)
            });
        });
        // walking a whole archive, which is what an unbounded cold get does
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("walk_all", size), &size, |b, &size| {
            b.iter(|| {
                let access =
                    <SortedPartition<TitleByKeyword> as RkyvSupport>::access(black_box(&raw))
                        .unwrap();
                // deserialize every row out of the archive, as an unprojected get does
                let mut found: Vec<TitleByKeyword> = Vec::with_capacity(size);
                for row in access.live_row_values() {
                    found.push(<TitleByKeyword as ShoalProjection>::from_archived(row));
                }
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Measure a get and an exists against a partition that is still an archive
///
/// This is the archived read path as a query actually reaches it: through
/// [`MaybeLoaded`], with the enum dispatch, the `SeekBytes` build, and whatever the variant
/// does before it can look at a row. `bench_archived_scan` above mimics this by hand; this
/// group *is* it, which is what makes it the group an entry about how a partition is held
/// has to be judged on.
///
/// It became reachable when `MaybeLoaded` took its buffer as a type parameter. A real
/// `ReadResult` can only come from a live glommio reactor, so before that this arm could not
/// be constructed outside the server at all.
///
/// A fresh `Option<SeekBytes>` per iteration is deliberate: production builds one per query
/// execution, not one per partition, so carrying it across iterations would measure a cache
/// that does not exist.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_maybe_loaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/maybe_loaded");
    for size in SIZES {
        // hold this partition both ways so the two arms can be read against each other
        let archived = accessible(size);
        let loaded = MaybeLoaded::<SortedPartition<TitleByKeyword>, AlignedVec>::Loaded {
            partition: filled(size),
            generation: 0,
        };
        // seek a key in the middle so the walk is not trivially short at either end
        let keyed = get_for(SortSelect::Keys(vec![sort_key(size / 2)]));
        // a cold get that names one row, which is the shape most reads have
        group.bench_with_input(BenchmarkId::new("get_key", size), &size, |b, _| {
            b.iter(|| {
                let mut seek = None;
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                archived.get(black_box(&keyed), &mut seek, &mut found);
                black_box(found)
            });
        });
        // the same get against the same rows held in memory, as a null control
        //
        // nothing about how an archive is held can reach this arm, so it should not move.
        // if it does, the run is contaminated and the archived numbers beside it are noise
        group.bench_with_input(BenchmarkId::new("loaded_get_key", size), &size, |b, _| {
            b.iter(|| {
                let mut seek = None;
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                loaded.get(black_box(&keyed), &mut seek, &mut found);
                black_box(found)
            });
        });
        // an exists over the same key, which returns a bool and deserializes nothing
        //
        // that is what makes it the cleanest signal on this page: everything it does except
        // the seek is overhead the partition imposes rather than work the answer needed
        let exists = exists_for(SortSelect::Keys(vec![sort_key(size / 2)]));
        group.bench_with_input(BenchmarkId::new("exists_key", size), &size, |b, _| {
            b.iter(|| {
                let mut seek = None;
                black_box(archived.exists(black_box(&exists), &mut seek))
            });
        });
        // a cold get that walks the whole archive, where the walk is the larger half
        let all = get_for(SortSelect::All);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("get_all", size), &size, |b, &size| {
            b.iter(|| {
                let mut seek = None;
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                archived.get(black_box(&all), &mut seek, &mut found);
                black_box(found)
            });
        });
        // the same walk, materializing every row, which is what this cost before F28
        //
        // it reaches the archive through the same `MaybeLoaded` dispatch and the same scan as
        // `get_all` above, and differs from it only in that a projection has no archived value
        // to point at and has to be built. The before and the after therefore come out of one
        // build on one machine, rather than out of two captures taken weeks apart
        let all_projected = SortedGet::<TitleByKeyword> {
            partition_keys: vec![0],
            sort_select: SortSelect::All,
            filters: None,
            limit: None,
            projection: <WholeRow as ShoalProjection>::PROJECTION,
        };
        group.bench_with_input(BenchmarkId::new("build_all", size), &size, |b, _| {
            b.iter(|| {
                let mut seek = None;
                let mut found: RowSink<'_, WholeRow> = RowSink::default();
                archived.get(black_box(&all_projected), &mut seek, &mut found);
                black_box(found)
            });
        });
        // what a partition costs to take on, which is where the validation went
        //
        // this id exists so that a capture shows the cost having *moved* rather than having
        // vanished. It is paid once per read from disk, against once per query before F4
        let raw = <SortedPartition<TitleByKeyword> as RkyvSupport>::serialize(&filled(size));
        group.throughput(Throughput::Bytes(raw.len() as u64));
        group.bench_with_input(BenchmarkId::new("validate_once", size), &size, |b, _| {
            b.iter_batched(
                // each iteration needs its own buffer, since the constructor takes it
                || raw.clone(),
                |raw| black_box(ValidatedArchive::<SortedPartition<TitleByKeyword>, _>::new(raw)),
                BatchSize::SmallInput,
            );
        });
        // a cold get bounded to a fixed window, which is what paging an archive costs
        let start = size / 4;
        let ranged = get_for(SortSelect::Range(SortRange {
            start: Bound::Included(sort_key(start)),
            end: Bound::Excluded(sort_key(start + 64)),
        }));
        group.throughput(Throughput::Elements(64));
        group.bench_with_input(BenchmarkId::new("get_range_64", size), &size, |b, _| {
            b.iter(|| {
                let mut seek = None;
                let mut found: RowSink<'_, TitleByKeyword> = RowSink::default();
                archived.get(black_box(&ranged), &mut seek, &mut found);
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Measure archiving the keys a query names
///
/// [`SeekBytes`] is built once per query that touches an archived partition, whatever the
/// partition holds, so its cost is paid per query rather than per row. That makes it a
/// fixed overhead on every cold read and worth knowing the size of.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_seek_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("seek_bytes/new");
    // a query naming a single key, which is the common case
    let one = SortSelect::Keys(vec![sort_key(1)]);
    group.bench_function("one_key", |b| {
        b.iter(|| black_box(SeekBytes::new(black_box(&one))));
    });
    // a query naming many keys, where the cost scales with how many were named
    let many = SortSelect::Keys((0..64).map(sort_key).collect());
    group.throughput(Throughput::Elements(64));
    group.bench_function("sixty_four_keys", |b| {
        b.iter(|| black_box(SeekBytes::new(black_box(&many))));
    });
    // a bounded range, which archives at most two values however wide it is
    let range = SortSelect::Range(SortRange {
        start: Bound::Included(sort_key(0)),
        end: Bound::Excluded(sort_key(64)),
    });
    group.bench_function("range", |b| {
        b.iter(|| black_box(SeekBytes::new(black_box(&range))));
    });
    group.finish();
}

/// Measure archiving and reading back a whole partition
///
/// This is what a compaction writes and what a cold read accesses, so it bounds both.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_partition_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_sorted/codec");
    for size in SIZES {
        let partition = filled(size);
        let archived = <SortedPartition<TitleByKeyword> as RkyvSupport>::serialize(&partition);
        group.throughput(Throughput::Bytes(archived.len() as u64));
        // serializing is what a compaction pays per partition it rewrites
        group.bench_with_input(
            BenchmarkId::new("serialize", size),
            &size,
            |b, _| {
                b.iter(|| {
                    black_box(<SortedPartition<TitleByKeyword> as RkyvSupport>::serialize(
                        black_box(&partition),
                    ))
                });
            },
        );
        // accessing is what a cold read pays before it can seek at all
        group.bench_with_input(BenchmarkId::new("access", size), &size, |b, _| {
            b.iter(|| {
                black_box(
                    <SortedPartition<TitleByKeyword> as RkyvSupport>::access(black_box(&archived))
                        .unwrap(),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_get_key,
    bench_get_all,
    bench_get_range,
    bench_archived_scan,
    bench_maybe_loaded,
    bench_seek_bytes,
    bench_partition_codec,
);
criterion_main!(benches);
