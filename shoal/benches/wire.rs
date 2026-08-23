//! Micro benchmarks for the wire codec
//!
//! This is the benchmark [D2](../../docs/src/direction/framing.md) called `wire_codec` and asked
//! for. It answers three separate questions that happen to share a fixture:
//!
//! - **What the header costs.** Encoding and decoding eight bytes, isolated from everything they
//!   travel with. This is the only cost the framing change adds, and it is here so that the claim
//!   that it is not measurable can be checked rather than asserted.
//! - **What validating an arriving bundle costs**
//!   ([O1](../../docs/src/appendix/optimizations.md)). The request decode group runs the validated
//!   `access` and the unchecked `access_unchecked` as separate functions, so the difference
//!   between them is the price of `bytecheck` on the request path.
//! - **What building a response costs** ([O2](../../docs/src/appendix/optimizations.md)). The
//!   response groups run at the same row counts as `partitions.rs`, so the two benchmarks can be
//!   read against each other.
//!
//! What this does **not** answer is whether the payload is still aligned. A criterion benchmark
//! measures nanoseconds, and a misaligned rkyv access is a `bytecheck` failure or undefined
//! behaviour rather than a slowdown. The test that catches that is
//! `the_response_payload_lands_on_a_sixteen_byte_boundary`, in `shoal-core/src/client.rs`.
//!
//! Unlike `partitions.rs` this reaches nothing crate private — the protocol module is public — so
//! it builds and runs with no features at all, which is what somebody debugging a framing change
//! wants.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::hint::black_box;
use uuid::Uuid;

use shoal::shared::protocol::{self, Flags, Header, MessageType};
use shoal::shared::queries::Queries;
use shoal::shared::traits::RkyvSupport;
use shoal::{
    FileSystem, PersistentSortedTable, PersistentUnsortedTable, ShoalSortedTable, ShoalUnsortedTable,
};

/// The bundle sizes the request groups are run at
///
/// The driver in `shoal-bench` sends a hundred queries per bundle, so this brackets what a real
/// request frame carries: at one the cost is the call, at a hundred it is the payload.
const BUNDLE_SIZES: [usize; 3] = [1, 10, 100];

/// The row counts the response groups are run at
///
/// These are the sizes `partitions.rs` uses, so a response of a given size can be read against
/// the scan that produced it.
const ROW_COUNTS: [usize; 4] = [16, 256, 1024, 4096];

/// The row widths the width groups are run at
///
/// The axis the rest of this file does not have. `BUNDLE_SIZES` sweeps how many queries a frame
/// carries and `ROW_COUNTS` sweeps how many rows a response answers with, both over a row of about
/// thirty bytes - so nothing here ever varied how wide a *row* is, and the per-byte half of
/// [O1](../../docs/src/appendix/optimizations.md) and
/// [O2](../../docs/src/appendix/optimizations.md) was unmeasured at the micro layer entirely.
///
/// Five widths spanning ten doublings. The top is 64 KiB rather than the grid's four megabytes
/// because these run inside criterion's sampling loop: at 64 KiB a response of
/// [`WIDTH_ROWS`] rows is a megabyte, and every sample builds one.
const WIDTHS: [usize; 5] = [64, 256, 1024, 8 * 1024, 64 * 1024];

/// How many queries a bundle carries while the width is being swept
///
/// Held fixed so that width is the only axis. Ten rather than one because a bundle of one measures
/// the call as much as the payload, and rather than a hundred because a hundred 64 KiB rows is a
/// six megabyte frame per sample.
const WIDTH_BUNDLE: usize = 10;

/// How many rows a response carries while the width is being swept
///
/// Held fixed for the same reason, and the smallest of [`ROW_COUNTS`] so that the widest arm's
/// response is a megabyte rather than sixteen of them.
const WIDTH_ROWS: usize = 16;

/// A frame bound large enough that no benchmark here ever trips it
const MAX_FRAME: u32 = protocol::DEFAULT_MAX_FRAME_BYTES;

/// A row shaped like the one the macro workloads write most of
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WireDb")]
pub struct TitleByKeyword {
    /// The keyword this title is filed under, which is its partition
    #[shoal(partition)]
    pub keyword: String,
    /// The title itself, which is what the partition is sorted by
    #[shoal(sort)]
    pub title: String,
}

/// A row whose width is whatever the benchmark asks for
///
/// A second row type rather than a payload field on [`TitleByKeyword`], and the reason is the join
/// key: every existing `wire_codec` identifier names a measurement taken over a thirty byte row, so
/// widening that row would keep all thirty nine names and change what every one of them measured.
/// The frozen baseline would then be compared against numbers describing something else.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WireDb")]
pub struct WideRow {
    /// Which partition this row belongs to
    #[shoal(partition)]
    pub id: u64,
    /// The payload, which is the whole point of this table and the only thing that varies
    pub payload: String,
}

/// The schema these benchmarks build their frames against
#[shoal::db]
pub struct WireDb {
    /// The one sorted table these benchmarks use
    pub title_by_keyword: PersistentSortedTable<TitleByKeyword, FileSystem>,
    /// The table whose rows are as wide as a benchmark asks for
    pub wide_row: PersistentUnsortedTable<WideRow, FileSystem>,
}

/// Build a row at a position in the sort order
///
/// # Arguments
///
/// * `index` - The position this row's sort key should sort at
fn row(index: usize) -> TitleByKeyword {
    TitleByKeyword {
        keyword: "keyword".to_owned(),
        title: format!("title-{index:08}"),
    }
}

/// Build a row of a given width
///
/// The payload is a single repeated byte rather than anything generated, because rkyv copies a
/// `String` as bytes and what is in them cannot change what that costs.
///
/// # Arguments
///
/// * `index` - Which partition this row belongs to
/// * `width` - How many bytes of payload it carries
fn wide(index: usize, width: usize) -> WideRow {
    WideRow {
        id: index as u64,
        payload: "x".repeat(width),
    }
}

/// Build a bundle of inserts of rows of a given width
///
/// # Arguments
///
/// * `width` - How many bytes of payload each row carries
fn wide_bundle(width: usize) -> Queries<WireDbClient> {
    // the same shape as `bundle`, at a fixed count and a varying width rather than the reverse
    let mut queries = Queries::default();
    for index in 0..WIDTH_BUNDLE {
        queries = queries.add(wide(index, width));
    }
    queries
}

/// Build a response holding [`WIDTH_ROWS`] rows of a given width
///
/// # Arguments
///
/// * `width` - How many bytes of payload each row carries
fn wide_response(width: usize) -> WireDbResponseKinds {
    let found = (0..WIDTH_ROWS).map(|index| wide(index, width)).collect::<Vec<_>>();
    WireDbResponseKinds::WideRow(shoal::shared::responses::Response {
        id: Uuid::new_v4(),
        index: 0,
        data: shoal::shared::responses::ResponseAction::Get(Some(found)),
        end: true,
    })
}

/// Build a bundle of inserts of a given size
///
/// # Arguments
///
/// * `size` - The number of queries to put in this bundle
fn bundle(size: usize) -> Queries<WireDbClient> {
    // start from an empty bundle and fill it with inserts
    let mut queries = Queries::default();
    for index in 0..size {
        queries = queries.add(row(index));
    }
    queries
}

/// How many bytes a bundle of ten inserts archives to
///
/// Pinned as of [F22](../../docs/src/features/row-size-benchmarks.md), which added a second table to
/// `WireDb`. See [`check_original_archive_sizes`].
const BUNDLE_10_BYTES: usize = 812;

/// How many bytes a response of 256 rows archives to
const RESPONSE_256_BYTES: usize = 7724;

/// Fails the run if the archives the original groups measure have changed size
///
/// `WireDbQueryKinds` and `WireDbResponseKinds` are enums whose archived size is the largest of
/// their variants, so **adding a table to `WireDb` can change what every benchmark in this file
/// measures while changing none of their names** — and those names are joined against
/// `docs/perf/baselines/B1-performance.json`, which is frozen. That is the failure mode
/// [F22](../../docs/src/features/row-size-benchmarks.md) went out of its way to avoid when it added
/// `WideRow` as a second row type rather than widening `TitleByKeyword`; it added the table anyway,
/// so this is what says the avoidance worked. Both figures are unchanged by it.
///
/// This is an assertion rather than a test because a criterion target has no test harness -
/// `cargo test --bench wire` runs each benchmark once instead - so the only place a check can live
/// is inside a benchmark. It runs once per process, before anything is timed.
///
/// **If this fires**, a schema change moved the layout. That is not necessarily wrong, and it does
/// mean every `wire_codec` number captured before it is measuring something else. Update the
/// constants, say so on [Optimizations](../../docs/src/appendix/optimizations.md), and expect the
/// comparison against the frozen baseline to be meaningless for this target until a new one exists.
fn check_original_archive_sizes() {
    let bundle = rkyv::to_bytes::<rkyv::rancor::Error>(&bundle(10)).expect("failed to archive");
    assert_eq!(
        bundle.len(),
        BUNDLE_10_BYTES,
        "a bundle of ten inserts changed size, so every wire_codec/request/* identifier now \
         measures something else"
    );
    let response = rkyv::to_bytes::<rkyv::rancor::Error>(&response(256)).expect("failed to archive");
    assert_eq!(
        response.len(),
        RESPONSE_256_BYTES,
        "a response of 256 rows changed size, so every wire_codec/response/* identifier now \
         measures something else"
    );
}

/// Measure encoding and decoding the frame header on its own
///
/// This is the whole cost the header adds, with nothing else in the way.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_header(c: &mut Criterion) {
    // before anything is timed, since a schema change here silently redefines every identifier
    check_original_archive_sizes();
    let mut group = c.benchmark_group("wire_codec/header");
    // a header of the shape every request frame carries
    let header = Header::new(MessageType::Queries, Flags::NONE, 4096, MAX_FRAME)
        .expect("failed to build a header");
    let encoded = header.encode();
    group.throughput(Throughput::Bytes(protocol::HEADER_LEN as u64));
    group.bench_function("encode", |b| {
        b.iter(|| black_box(black_box(&header).encode()));
    });
    group.bench_function("decode", |b| {
        b.iter(|| black_box(Header::decode(black_box(&encoded), MAX_FRAME).unwrap()));
    });
    // and the two preamble builders, which are what the call sites actually reach for
    let query_id = Uuid::new_v4();
    group.bench_function("request_preamble", |b| {
        b.iter(|| black_box(protocol::request_preamble(black_box(4096), MAX_FRAME).unwrap()));
    });
    group.bench_function("response_preamble", |b| {
        b.iter(|| {
            black_box(
                protocol::response_preamble(black_box(&query_id), black_box(4096), MAX_FRAME)
                    .unwrap(),
            )
        });
    });
    let response = protocol::response_preamble(&query_id, 4096, MAX_FRAME)
        .expect("failed to build a response preamble");
    group.bench_function("decode_response", |b| {
        b.iter(|| black_box(protocol::decode_response(black_box(&response), MAX_FRAME).unwrap()));
    });
    group.finish();
}

/// Measure archiving a bundle of queries and framing it
///
/// This is everything `Shoal::send` does before it touches a socket.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_request_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/request/encode");
    for size in BUNDLE_SIZES {
        let queries = bundle(size);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&queries).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        // the serialize on its own, which is what dominates
        group.bench_with_input(BenchmarkId::new("serialize", size), &size, |b, _| {
            b.iter(|| {
                black_box(rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&queries)).unwrap())
            });
        });
        // and the serialize with the framing on top, which is what a send costs
        group.bench_with_input(BenchmarkId::new("framed", size), &size, |b, _| {
            b.iter(|| {
                let archived = rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&queries)).unwrap();
                let preamble = protocol::request_preamble(archived.len(), MAX_FRAME).unwrap();
                black_box((preamble, archived))
            });
        });
    }
    group.finish();
}

/// Measure reading a bundle of queries back off the wire
///
/// The validated and unchecked accesses are separate functions on purpose. The difference between
/// them is exactly what `bytecheck` costs on the request path, which is the question
/// `O1` is blocked on.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_request_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/request/decode");
    for size in BUNDLE_SIZES {
        let queries = bundle(size);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&queries).expect("failed to archive");
        let preamble =
            protocol::request_preamble(archived.len(), MAX_FRAME).expect("failed to frame");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        // the header decode, which is what the read relay pays per frame
        group.bench_with_input(BenchmarkId::new("header", size), &size, |b, _| {
            b.iter(|| {
                black_box(protocol::decode_request(black_box(&preamble), MAX_FRAME).unwrap())
            });
        });
        // the validated access, which is what the server actually does today
        group.bench_with_input(BenchmarkId::new("access", size), &size, |b, _| {
            b.iter(|| {
                black_box(Queries::<WireDbClient>::access(black_box(&archived[..])).unwrap())
            });
        });
        // the unchecked access, whose difference from the above is what validation costs
        group.bench_with_input(BenchmarkId::new("access_unchecked", size), &size, |b, _| {
            b.iter(|| {
                black_box(unsafe {
                    rkyv::access_unchecked::<<Queries<WireDbClient> as Archive>::Archived>(
                        black_box(&archived[..]),
                    )
                })
            });
        });
        // and the full deserialize, which is what turns an archive into owned queries
        group.bench_with_input(BenchmarkId::new("deserialize", size), &size, |b, _| {
            b.iter(|| {
                let accessed = Queries::<WireDbClient>::access(black_box(&archived[..])).unwrap();
                black_box(<Queries<WireDbClient> as RkyvSupport>::deserialize(accessed).unwrap())
            });
        });
    }
    group.finish();
}

/// Build a response holding a number of rows
///
/// # Arguments
///
/// * `rows` - The number of rows this response should carry
fn response(rows: usize) -> WireDbResponseKinds {
    // build the rows this response answers with
    let found = (0..rows).map(row).collect::<Vec<_>>();
    WireDbResponseKinds::TitleByKeyword(shoal::shared::responses::Response {
        id: Uuid::new_v4(),
        index: 0,
        data: shoal::shared::responses::ResponseAction::Get(Some(found)),
        end: true,
    })
}

/// Measure archiving a response and framing it
///
/// This is everything a shard does to a response before it reaches the write relay, and it runs
/// once per query rather than once per bundle — which is the argument for the length field being
/// a `u32` rather than a `u64`.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_response_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/response/encode");
    let query_id = Uuid::new_v4();
    for rows in ROW_COUNTS {
        let response = response(rows);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        group.bench_with_input(BenchmarkId::new("serialize", rows), &rows, |b, _| {
            b.iter(|| {
                black_box(rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&response)).unwrap())
            });
        });
        group.bench_with_input(BenchmarkId::new("framed", rows), &rows, |b, _| {
            b.iter(|| {
                let archived = rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&response)).unwrap();
                let preamble =
                    protocol::response_preamble(&query_id, archived.len(), MAX_FRAME).unwrap();
                black_box((preamble, archived))
            });
        });
    }
    group.finish();
}

/// Measure reading a response back off the wire, into the aligned buffer the client uses
///
/// The copy into an `AlignedVec<16>` is included because that is what the client actually does,
/// and because leaving it out would make the read look cheaper than the path it stands for.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/response/decode");
    for rows in ROW_COUNTS {
        let response = response(rows);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        // the validated access, which is what a response costs the client before it reads a row
        group.bench_with_input(BenchmarkId::new("access", rows), &rows, |b, _| {
            b.iter(|| black_box(WireDbResponseKinds::access(black_box(&archived[..])).unwrap()));
        });
        // the copy into the aligned buffer the read path allocates, which is what makes the
        // access above a pointer cast rather than a parse
        group.bench_with_input(BenchmarkId::new("into_aligned", rows), &rows, |b, _| {
            b.iter(|| {
                let mut buff = AlignedVec::<16>::with_capacity(archived.len());
                buff.extend_from_slice(black_box(&archived[..]));
                black_box(buff)
            });
        });
    }
    group.finish();
}

/// Measure archiving and framing a request, against the width of the rows in it
///
/// The bundle size is held at [`WIDTH_BUNDLE`] so that the payload width is the only thing moving.
/// Read against `wire_codec/request/encode`, which moves the count and holds the width.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_width_request_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/width/request/encode");
    for width in WIDTHS {
        let queries = wide_bundle(width);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&queries).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        group.bench_with_input(BenchmarkId::new("serialize", width), &width, |b, _| {
            b.iter(|| {
                black_box(rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&queries)).unwrap())
            });
        });
        group.bench_with_input(BenchmarkId::new("framed", width), &width, |b, _| {
            b.iter(|| {
                let archived = rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&queries)).unwrap();
                let preamble = protocol::request_preamble(archived.len(), MAX_FRAME).unwrap();
                black_box((preamble, archived))
            });
        });
    }
    group.finish();
}

/// Measure decoding an arriving request, against the width of the rows in it
///
/// This is the per-byte half of [O1](../../docs/src/appendix/optimizations.md), which the rest of
/// this file could not see. `access` validates the archive and `deserialize` copies every `String`
/// out of a buffer that already holds it in a readable layout, so the gap between the two grows in
/// the payload width while the gap between `access` and `access_unchecked` should not.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_width_request_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/width/request/decode");
    for width in WIDTHS {
        let queries = wide_bundle(width);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&queries).expect("failed to archive");
        let preamble =
            protocol::request_preamble(archived.len(), MAX_FRAME).expect("failed to frame");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        // the header decode, which is the control: it is the same eight bytes at every width, so a
        // curve here would mean the axis is measuring something other than the payload
        group.bench_with_input(BenchmarkId::new("header", width), &width, |b, _| {
            b.iter(|| {
                black_box(protocol::decode_request(black_box(&preamble), MAX_FRAME).unwrap())
            });
        });
        group.bench_with_input(BenchmarkId::new("access", width), &width, |b, _| {
            b.iter(|| {
                black_box(Queries::<WireDbClient>::access(black_box(&archived[..])).unwrap())
            });
        });
        group.bench_with_input(BenchmarkId::new("access_unchecked", width), &width, |b, _| {
            b.iter(|| {
                black_box(unsafe {
                    rkyv::access_unchecked::<<Queries<WireDbClient> as Archive>::Archived>(
                        black_box(&archived[..]),
                    )
                })
            });
        });
        // and the full deserialize, which is the entry O1 is about
        group.bench_with_input(BenchmarkId::new("deserialize", width), &width, |b, _| {
            b.iter(|| {
                let accessed = Queries::<WireDbClient>::access(black_box(&archived[..])).unwrap();
                black_box(<Queries<WireDbClient> as RkyvSupport>::deserialize(accessed).unwrap())
            });
        });
    }
    group.finish();
}

/// Measure archiving and framing a response, against the width of the rows in it
///
/// The per-byte half of [O2](../../docs/src/appendix/optimizations.md). The row count is held at
/// [`WIDTH_ROWS`], so this and `wire_codec/response/encode` sweep the two ways a response can grow.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_width_response_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/width/response/encode");
    let query_id = Uuid::new_v4();
    for width in WIDTHS {
        let response = wide_response(width);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        group.bench_with_input(BenchmarkId::new("serialize", width), &width, |b, _| {
            b.iter(|| {
                black_box(rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&response)).unwrap())
            });
        });
        group.bench_with_input(BenchmarkId::new("framed", width), &width, |b, _| {
            b.iter(|| {
                let archived = rkyv::to_bytes::<rkyv::rancor::Error>(black_box(&response)).unwrap();
                let preamble =
                    protocol::response_preamble(&query_id, archived.len(), MAX_FRAME).unwrap();
                black_box((preamble, archived))
            });
        });
    }
    group.finish();
}

/// Measure reading a response back, against the width of the rows in it
///
/// `access` is a pointer cast and a validation pass, and `into_aligned` is a copy of the whole
/// response into the buffer the client allocates. The second is O(bytes) by construction, so the
/// two together say how much of a wide response's client-side cost is the copy alone.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_width_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_codec/width/response/decode");
    for width in WIDTHS {
        let response = wide_response(width);
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("failed to archive");
        group.throughput(Throughput::Bytes(archived.len() as u64));
        group.bench_with_input(BenchmarkId::new("access", width), &width, |b, _| {
            b.iter(|| black_box(WireDbResponseKinds::access(black_box(&archived[..])).unwrap()));
        });
        group.bench_with_input(BenchmarkId::new("into_aligned", width), &width, |b, _| {
            b.iter(|| {
                let mut buff = AlignedVec::<16>::with_capacity(archived.len());
                buff.extend_from_slice(black_box(&archived[..]));
                black_box(buff)
            });
        });
    }
    group.finish();
}

criterion_group!(
    wire_codec,
    bench_header,
    bench_request_encode,
    bench_request_decode,
    bench_response_encode,
    bench_response_decode,
    bench_width_request_encode,
    bench_width_request_decode,
    bench_width_response_encode,
    bench_width_response_decode,
);
criterion_main!(wire_codec);

