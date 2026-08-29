//! Integration test for the shape of the spans one query produces on the server
//!
//! A query that reads a partition off disk touches at least five instrumented functions, spread
//! over three tasks and two OS threads: the coordinator routes it, the owning shard executes it,
//! the table parks it, a loader task reads the partition, and the shard replays and answers it.
//! Every one of those hops crosses an asynchronous channel, which is where `tracing`'s implicit
//! parenting stops working - so the spans either carry their parent across by hand or they do not
//! belong to the same trace at all.
//!
//! This test asserts the property that matters to somebody reading a collector: **every span the
//! query path opens for one query resolves to one root**. It says nothing about the spans'
//! contents, their timings or their order, because none of those are what fragments a trace.
//!
//! Startup, compaction and the writer's flush tasks are deliberately *not* in the set below. They
//! are shared by every query rather than owned by one, so they are their own traces on purpose -
//! see the limitations on the resolved page.
//!
//! **This binary holds one test.** The recorder and the subscriber that feeds it are process wide,
//! and a second test running beside this one would mix its spans into the same topology.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::PersistentSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tracing::span::Attributes;
use tracing::Id;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::util::SubscriberInitExt;

mod utils;

use utils::TestError;

/// One span, reduced to the only three things this test asks about
#[derive(Debug, Clone)]
struct SpanRecord {
    /// The id the registry gave this span
    id: u64,
    /// The name the span was opened under
    name: &'static str,
    /// The span this one hangs off, or `None` if it is the root of a trace
    parent: Option<u64>,
}

/// Every span opened since the recorder was armed
static SPANS: Mutex<Vec<SpanRecord>> = Mutex::new(Vec::new());

/// The name of every span that was entered at least once since the recorder was armed
///
/// A span that is held but never entered is not an error in `tracing` and is a real defect in
/// what gets exported: `tracing-opentelemetry` timestamps a span when it is **exited**
/// (`layer.rs`, `on_exit`), and the SDK resolves a missing end time as
/// `end_time.unwrap_or(start_time)`. So a span that is only ever passed around as a parent
/// exports with **zero duration** - the trace is joined correctly and the root draws as a tick.
static ENTERED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// Whether spans are being recorded yet
///
/// Startup opens dozens of spans that have nothing to do with a query, and recording them would
/// mean filtering them out again at the other end. Arming after the server is up is cheaper and
/// says what the test means.
static ARMED: AtomicBool = AtomicBool::new(false);

/// A tracing layer that records the parent of every span it is shown
///
/// This is the whole of the instrument. A trace is fragmented exactly when a span that should hang
/// off another one has no parent, and `Attributes` is where that is decided - so this reads the
/// same three fields `tracing` itself uses to build the tree, before any exporter has seen them.
struct RecordTopology;

impl<S> Layer<S> for RecordTopology
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    /// Record this span and the span it hangs off
    ///
    /// # Arguments
    ///
    /// * `attrs` - The attributes of the span being opened
    /// * `id` - The id assigned to the span
    /// * `ctx` - The context of the subscriber this layer is part of
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // nothing before the server is up is part of a query
        if !ARMED.load(Ordering::SeqCst) {
            return;
        }
        // work out what this span hangs off, the same three ways tracing does
        let parent = if let Some(parent) = attrs.parent() {
            // an explicit `parent = &span`, which is how every hop across a channel is rejoined
            Some(parent.into_u64())
        } else if attrs.is_contextual() {
            // the ambient span, which only exists within one task on one thread
            ctx.current_span().id().map(Id::into_u64)
        } else {
            // an explicit root, asked for with `parent: None`
            None
        };
        // keep it
        SPANS
            .lock()
            .expect("the topology recorder was poisoned")
            .push(SpanRecord {
                id: id.into_u64(),
                name: attrs.metadata().name(),
                parent,
            });
    }

    /// Record that this span was entered
    ///
    /// # Arguments
    ///
    /// * `id` - The span being entered
    /// * `ctx` - The context of the subscriber this layer is part of
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // nothing before the server is up is part of a query
        if !ARMED.load(Ordering::SeqCst) {
            return;
        }
        // remember what was entered, by name - the ids are already in SPANS
        if let Some(span) = ctx.span(id) {
            ENTERED
                .lock()
                .expect("the topology recorder was poisoned")
                .push(span.name());
        }
    }
}

/// The spans this fix opens itself, which have to be entered to have a duration
///
/// Both are held across channel hops rather than wrapping a function, so neither gets an
/// `#[instrument]`'s enter for free. `Shoal::request` is entered over the body read and
/// `Coordinator::route` over the response write, which is also what makes each of them cover the
/// thing it is named for.
const MUST_BE_ENTERED: &[&str] = &["Shoal::request", "Coordinator::route"];

/// The spans one query opens between arriving on the socket and being answered on it
///
/// A read that misses in memory walks all of these. They live in three tasks and on two threads,
/// which is the whole point - a name here is a name that has to have been carried across a
/// channel to be in the right trace.
const QUERY_PATH: &[&str] = &[
    "Shoal::request",
    "Coordinator::handle_client",
    "Coordinator::route",
    "Shard::handle_query",
    "PersistentTable::block_on_load",
    "Fsloader::spawn_task",
    "loader::read_partition",
    "Shard::handle_released",
    "Shard::reply",
];

/// Walk a span up to the root of its trace
///
/// A span whose parent is not in the recording is treated as a root: it belongs to a trace this
/// test did not see the start of, which is the same failure as having no parent at all.
///
/// # Arguments
///
/// * `id` - The span to walk up from
/// * `parents` - Every recorded span's parent, by id
fn root_of(id: u64, parents: &HashMap<u64, Option<u64>>) -> u64 {
    // start at the span we were asked about
    let mut current = id;
    // climb until something has no parent we can follow
    while let Some(Some(parent)) = parents.get(&current) {
        // a cycle is impossible in a span tree, but a bug that made one would hang this test
        if *parent == current {
            break;
        }
        current = *parent;
    }
    current
}

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

/// Every span one query opens on the server belongs to one trace
///
/// The query is a get of a partition that is only on disk, because that is the path with the most
/// hops in it: the coordinator routes it, the shard parks it, a loader task in another executor
/// reads the partition, and the shard replays and answers it. If the spans of that path resolve to
/// more than one root then a reader following the query in a collector sees several unrelated
/// traces and no way to join them.
#[tokio::test]
async fn one_query_produces_one_trace() -> Result<(), TestError> {
    // record the shape of every span this process opens once we arm it
    tracing_subscriber::registry().with(RecordTopology).init();
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
    // start recording now that everything startup opens is behind us
    ARMED.store(true, Ordering::SeqCst);
    // read the row back, which parks on a partition read and is answered when it lands
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_owned()]))
        .await?;
    // stop recording before the shutdown below opens anything
    ARMED.store(false, Ordering::SeqCst);
    // the get has to have found the row, or it took a path with no disk read in it
    assert_eq!(
        response.access::<TestRecord>()?.unwrap().len(),
        1,
        "the get did not answer with the row it was given"
    );
    // take what we recorded
    let spans = SPANS
        .lock()
        .expect("the topology recorder was poisoned")
        .clone();
    // index every span's parent by its id, so a span can be walked up to its root
    let parents: HashMap<u64, Option<u64>> =
        spans.iter().map(|span| (span.id, span.parent)).collect();
    // and every span's name, so a failure can say which trace is which
    let names: HashMap<u64, &'static str> =
        spans.iter().map(|span| (span.id, span.name)).collect();
    // group the query path's spans by the root each of them resolves to
    let mut roots: HashMap<u64, Vec<&'static str>> = HashMap::new();
    for span in spans.iter().filter(|span| QUERY_PATH.contains(&span.name)) {
        // walk this span up to whatever it hangs off
        roots
            .entry(root_of(span.id, &parents))
            .or_default()
            .push(span.name);
    }
    // every name we expect to see has to have been opened at all, or this test is asserting
    // about a path the query did not take
    for name in QUERY_PATH {
        assert!(
            spans.iter().any(|span| &span.name == name),
            "the query path never opened {name}, so this test did not measure a disk read"
        );
    }
    // write out what we found, since the count on its own does not say what fragmented
    let found = roots
        .iter()
        .map(|(root, under)| {
            format!(
                "  root {} ({}): {}\n",
                root,
                names.get(root).copied().unwrap_or("<not recorded>"),
                under.join(", ")
            )
        })
        .collect::<String>();
    // one query, one trace
    assert_eq!(
        roots.len(),
        1,
        "one query's spans resolved to {} separate traces:\n{found}",
        roots.len()
    );
    // every span this fix opens by hand was entered, or it exports with no duration
    //
    // this is a separate failure from a fragmented trace and looks nothing like one: the trace
    // is joined correctly and the root draws as a zero width tick with its children extending
    // past it, which reads as a broken trace rather than as a timing bug
    let entered = ENTERED
        .lock()
        .expect("the topology recorder was poisoned")
        .clone();
    for name in MUST_BE_ENTERED {
        assert!(
            entered.contains(name),
            "{name} was opened and never entered, so it exports with zero duration"
        );
    }
    // and that trace is rooted where the request arrived, not part way through it
    //
    // a root anywhere else means the spans are joined but the trace still starts after work
    // the request had already paid for - which is the half of this that a count cannot see
    let root = *roots.keys().next().expect("a trace with no root");
    assert_eq!(
        names.get(&root).copied(),
        Some("Shoal::request"),
        "the trace was rooted at {:?} rather than at the socket read",
        names.get(&root)
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}
