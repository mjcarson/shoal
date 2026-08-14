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
use shoal::{FileSystem, PersistentSortedTable, ShoalSortedTable};

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

/// The schema these benchmarks build their frames against
#[shoal::db]
pub struct WireDb {
    /// The one sorted table these benchmarks use
    pub title_by_keyword: PersistentSortedTable<TitleByKeyword, FileSystem>,
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

/// Measure encoding and decoding the frame header on its own
///
/// This is the whole cost the header adds, with nothing else in the way.
///
/// # Arguments
///
/// * `c` - The criterion harness to register with
fn bench_header(c: &mut Criterion) {
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

criterion_group!(
    wire_codec,
    bench_header,
    bench_request_encode,
    bench_request_decode,
    bench_response_encode,
    bench_response_decode,
);
criterion_main!(wire_codec);
