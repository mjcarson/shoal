//! Integration test for one query being one trace across the client and the server
//!
//! [Resolved #89](../../docs/src/appendix/resolved/fragmented-query-traces.md) joined every span
//! the *server* opens for a query into one trace and stopped at the socket, because joining the
//! client's half to it is a protocol change. This asserts the property that change buys: the spans
//! `Shoal::send` opens and the spans the shard opens answering it carry **one OpenTelemetry trace
//! id**, and the server's root names the client's span as its parent.
//!
//! **This asserts about exported spans rather than about the registry's span tree**, which is the
//! difference between this file and `tracing_topology.rs`. A trace context on the wire is joined at
//! the OTLP layer: `Shoal::request` is still opened with `parent: None` and is still an explicit
//! root as far as `tracing` is concerned, so a recorder reading `Attributes::parent` sees exactly
//! what it saw before the fix. The join is only visible in the `SpanContext` the OTLP layer
//! resolves, which is what the exporter below records.
//!
//! **This binary holds one test.** The subscriber and the tracer provider behind it are process
//! wide, and a second test running beside this one would mix its spans into the same trace set.
//!
//! **It needs the `otel` feature**, which is what puts a trace context on the wire at all, so it
//! is compiled away without one rather than failing: `cargo test -p shoal --features otel`. A
//! client built without the feature sets no flag bit and is joined to nothing, deliberately, and
//! asserting otherwise would make the feature being off a test failure.
//!
//! A plain `cargo test --workspace` **does** run this, without naming the feature, because
//! `shoal-bench`'s `workloads` feature enables `shoal/otel` and cargo unifies features across a
//! workspace build. That is worth knowing before changing `shoal-bench`'s feature list: dropping
//! `shoal/otel` there would stop this test running, and nothing would fail.

#![cfg(feature = "otel")]

use deepsize2::DeepSizeOf;
use futures::future::BoxFuture;
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::PersistentSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod utils;

use utils::TestError;

/// One exported span, reduced to the four things this test asks about
#[derive(Debug, Clone)]
struct Exported {
    /// The name the span was opened under
    name: String,
    /// The trace this span was exported as part of
    trace_id: TraceId,
    /// The id of the span itself
    span_id: SpanId,
    /// The span this one hangs off, or an invalid id if it is the root of its trace
    parent_span_id: SpanId,
}

/// Every span exported since this process started
static EXPORTED: Mutex<Vec<Exported>> = Mutex::new(Vec::new());

/// An exporter that keeps what it is handed instead of sending it anywhere
///
/// The SDK ships one of these behind a `testing` feature. Implementing it here instead keeps that
/// feature, and the lockfile churn enabling it causes, out of the tree - which is the same choice
/// `server/trace.rs`'s own tests make.
#[derive(Debug, Default, Clone)]
struct RecordingExporter;

impl SpanExporter for RecordingExporter {
    /// Record the identity of every span in this batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The spans being exported
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, OTelSdkResult> {
        // keep what each span in this batch says about which trace it belongs to
        let mut exported = EXPORTED.lock().expect("the span recorder was poisoned");
        for span in batch {
            exported.push(Exported {
                name: span.name.to_string(),
                trace_id: span.span_context.trace_id(),
                span_id: span.span_context.span_id(),
                parent_span_id: span.parent_span_id,
            });
        }
        Box::pin(std::future::ready(Ok(())))
    }
}

/// The spans the client opens for one query, from framing it to handing its rows back
///
/// `Shoal::send_stamped` covers the write. `Shoal::response` and `ShoalResultStream::next` are the
/// return half, which runs in a detached reader task shared by every query on the connection and
/// rejoins the query's trace through the span parked in its `Waiter`.
///
/// `Shoal::send` is deliberately absent: this test sends with `send_one`, which reaches
/// `send_stamped` through `send_one_stamped` and never opens that span.
const CLIENT_PATH: &[&str] = &[
    "Shoal::send_stamped",
    "Shoal::response",
    "ShoalResultStream::next",
];

/// The spans the server opens between the frame arriving and the answer being written
///
/// A get of a partition that is only on disk is the path with the most hops in it, which is why
/// this test takes one. These are a subset of `tracing_topology.rs`'s list: that file asserts they
/// are one *tracing* tree, and this one asserts they are in the client's trace.
const SERVER_PATH: &[&str] = &[
    "Shoal::request",
    "Coordinator::route",
    "Shard::handle_query",
    "loader::read_partition",
    "Shard::reply",
];

/// A sorted row that keeps its rows on disk, so a get after a restart has to read one
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key - orders records within a partition
    #[shoal(sort)]
    pub sort_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A schema with one persistent sorted table
#[db]
pub struct TestDb {
    /// The sorted table that keeps its rows on disk
    pub records: PersistentSortedTable<TestRecord, FileSystem>,
}

/// Describe what was exported, grouped by the trace each span landed in
///
/// A count on its own does not say what fragmented, and which spans went with which is the whole
/// of the diagnosis.
///
/// # Arguments
///
/// * `spans` - The spans on the query path, as exported
fn describe(spans: &[&Exported]) -> String {
    // collect the names in each trace
    let mut traces: HashMap<TraceId, Vec<&str>> = HashMap::new();
    for span in spans {
        traces
            .entry(span.trace_id)
            .or_default()
            .push(span.name.as_str());
    }
    // and write one line per trace
    traces
        .iter()
        .map(|(trace, names)| format!("  trace {trace:?}: {}\n", names.join(", ")))
        .collect()
}

/// One query is one trace across both halves of the system
///
/// The query is a get of a partition that is only on disk, because that is the path with the most
/// hops in it. If the client's spans and the server's resolve to different trace ids then a reader
/// following a slow query in a collector sees two unrelated traces and no way to join them - which
/// is what the system did before a trace context went on the wire.
#[tokio::test]
async fn one_query_spans_the_client_and_the_server() -> Result<(), TestError> {
    // export every span this process closes into the recorder above
    //
    // a simple processor rather than a batched one: this test asserts about spans that closed
    // moments before it reads them, and a batch delay is a race it does not need
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(RecordingExporter)
        .build();
    // install a subscriber whose only layer is the one that resolves trace ids
    //
    // this has to be the OTLP layer rather than a recording `Layer`, because the join this test is
    // about happens inside it - the registry sees `Shoal::request` as a root either way
    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "trace_propagation");
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a server whose intent log rotates every few writes, so our rows reach an archive
    // rather than sitting in a log the next startup would replay straight back into memory
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // write the row this test reads back
    client
        .send_one(TestRecord {
            partition_key: "partition_key".to_owned(),
            sort_key: "sort_key".to_owned(),
            data: "woot".to_owned(),
        })
        .await?;
    // write enough rows after it to rotate the intent log and compact it into an archive
    for index in 0..64 {
        // each in its own partition, so this fills the log rather than one partition
        client
            .send_one(TestRecord {
                partition_key: format!("filler_{index}"),
                sort_key: "sort_key".to_owned(),
                data: "x".repeat(256),
            })
            .await?;
    }
    // shut this server down, which flushes and compacts our rows into an archive
    pool.exit()?;
    // wait for the threads to clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start the server again, so nothing is resident and the get below has to read from disk
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // everything above this line is setup, and its spans are dropped by the filter below
    EXPORTED
        .lock()
        .expect("the span recorder was poisoned")
        .clear();
    // read the row back, which parks on a partition read and is answered when it lands
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_owned()]))
        .await?;
    // the get has to have found the row, or it took a path with no disk read in it
    assert_eq!(
        response.access::<TestRecord>()?.unwrap().len(),
        1,
        "the get did not answer with the row it was given"
    );
    // drop the client before reading any of this, which is what closes its spans
    //
    // the send's span is held by the `Waiter` the response was routed through, and `channel_map`
    // is a `papaya` map - removing an entry does not drop it, it defers reclamation to a later
    // epoch. so the span a query was sent in stays open for a while after the query ends, and
    // dropping the client is the only way a test can be sure it has closed
    drop(response);
    drop(client);
    // stop the server, which closes and exports everything the query left open
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // ship whatever the processor still holds before reading any of it
    provider.force_flush().expect("failed to flush spans");
    // take what was exported
    let exported = EXPORTED
        .lock()
        .expect("the span recorder was poisoned")
        .clone();
    // narrow it to the spans this query walks, on both sides of the socket
    let path: Vec<&Exported> = exported
        .iter()
        .filter(|span| {
            CLIENT_PATH.contains(&span.name.as_str()) || SERVER_PATH.contains(&span.name.as_str())
        })
        .collect();
    // one query, one trace - this is the whole assertion
    let traces: HashSet<TraceId> = path.iter().map(|span| span.trace_id).collect();
    assert_eq!(
        traces.len(),
        1,
        "one query's spans were exported as {} separate traces:\n{}",
        traces.len(),
        describe(&path)
    );
    // every name this asserts about has to have been opened, or the test measured a shorter path
    for name in CLIENT_PATH.iter().chain(SERVER_PATH) {
        assert!(
            path.iter().any(|span| span.name == *name),
            "the query never opened {name}, so this test did not measure the whole path. \
             what was exported: {:?}",
            exported
                .iter()
                .map(|span| span.name.as_str())
                .collect::<Vec<&str>>()
        );
    }
    // and the server's root has to name the client's span as its parent, rather than merely
    // sharing a trace id with it
    //
    // a shared trace id with no parent link is what a propagator that carried the trace id and
    // dropped the span id would produce: one trace, two roots, and no ordering between them
    let request = path
        .iter()
        .find(|span| span.name == "Shoal::request")
        .expect("the server never opened Shoal::request");
    let sender = path
        .iter()
        .find(|span| span.name == "Shoal::send_stamped")
        .expect("the client never opened Shoal::send_stamped");
    assert_eq!(
        request.parent_span_id, sender.span_id,
        "the server's request span is in the client's trace but does not hang off its send"
    );
    Ok(())
}
