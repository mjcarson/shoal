//! Integration tests for the two bounds a table keeps on work that has left the mesh queue
//!
//! The mesh's admission bound counts the messages waiting on a shard's queue, and nothing else. A
//! write that has been handled waits on its fdatasync in the table's pending queue, and a query
//! that needs a partition from disk waits parked on its read - both off the queue, and both
//! growing with nothing counting them: the pending queue by arrival rate times fsync latency, the
//! parked set by arrival rate times read latency ([item 15](../../docs/src/appendix/resolved/backlog-bounds.md)).
//! `networking.max_pending_writes` and `networking.max_parked_queries` bound them, each answering
//! the query it would have exceeded `Shedding` before anything was committed, parked or read.
//!
//! A slow device cannot be made on demand, so these tests build a shard's tables the way a shard
//! does - on a glommio executor, over a directory a real server seeded - and hold what the shard
//! loop would drain: the pending queue is never swept until a test sweeps it, and no loader runs
//! until a test starts one, so every parked query stays parked.
//!
//! See `docs/src/appendix/resolved/backlog-bounds.md`.

use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::glommio::{Latency, LocalExecutorBuilder, Shares};
use shoal::gxhash::GxHasher;
use shoal::kanal::{self, AsyncReceiver, AsyncSender};
use shoal::lru::LruCache;
use shoal::server::conf::{Conf, Networking};
use shoal::server::messages::{Answer, QueryMetadata, ServerMsg};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::queries::{SortSelect, SortedGet, SortedQuery, UnsortedGet, UnsortedQuery};
use shoal::shared::responses::{Response, ResponseAction};
use shoal::shared::row_ref::RowRef;
use shoal::shared::traits::{PartitionKeySupport, ShoalTableSupport};
use shoal::storage::{FileSystem, FullArchiveMap, LoaderMsg, Loaders};
use shoal::tables::{PartitionLoad, PersistentSortedTable, PersistentUnsortedTable};
use shoal::ShoalDatabase;
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tracing::Span;
use uuid::Uuid;

mod utils;

/// A sorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "BacklogDb")]
pub struct SortedRow {
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

/// An unsorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "BacklogDb")]
pub struct UnsortedRow {
    /// The partition key - one row per partition
    #[shoal(partition)]
    pub partition_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A schema with one persistent table of each kind
#[db]
pub struct BacklogDb {
    /// The sorted table
    pub sorted: PersistentSortedTable<SortedRow, FileSystem>,
    /// The unsorted table
    pub unsorted: PersistentUnsortedTable<UnsortedRow, FileSystem>,
}

/// The first partition the seed writes, which a test parks a query on
const FIRST: &str = "first";

/// The second partition the seed writes, which a test tries to park a second query on
const SECOND: &str = "second";

/// The shard a single shard server names its one executor
const SHARD_NAME: &str = "Shard-0";

/// How many harnesses have been opened, for naming their executors
static OPENED: AtomicUsize = AtomicUsize::new(0);

/// Fail a test body with a message rather than panicking
///
/// A panic on a glommio executor thread aborts the whole binary while it unwinds, so the bodies
/// here report a failure as an error and `with_harness` panics with it on the test's own thread.
macro_rules! check {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+));
        }
    };
}

/// Fail a test body if two values differ, saying what both were
macro_rules! check_eq {
    ($left:expr, $right:expr, $msg:literal) => {{
        let (left, right) = (&$left, &$right);
        if left != right {
            return Err(format!("{}\n  left: {:?}\n right: {:?}", $msg, left, right));
        }
    }};
}

/// Flush a table and sweep it until it has released this many responses
///
/// A flush submits what is staged and returns; the fdatasync lands on its own, so the sweep is
/// repeated until everything the test wrote has come back out, for at most ten seconds.
macro_rules! release_all {
    ($table:expr, $expected:expr) => {{
        // submit what is staged
        $table
            .flush()
            .await
            .map_err(|error| format!("failed to flush: {error:?}"))?;
        // then sweep until everything has been released
        let mut released = 0;
        for _ in 0..1000 {
            let flushed = $table
                .get_flushed()
                .await
                .map_err(|error| format!("failed to sweep: {error:?}"))?;
            released += flushed.len();
            flushed.clear();
            if released >= $expected {
                break;
            }
            shoal::glommio::timer::sleep(Duration::from_millis(10)).await;
        }
        check!(
            released >= $expected,
            "only {released} of {} writes were released",
            $expected
        );
    }};
}

/// What a test body answers with
type Outcome = Result<(), String>;

/// The lru cache a shard shares between its tables
type Lru = LruCache<(BacklogDbTableNames, u64), usize, BuildHasherDefault<GxHasher>>;

/// What a table answers a query with, if it answers it at once
type Answered<P> = Option<(
    Uuid,
    Uuid,
    shoal::server::stage_profile::StageStamps,
    Answer<Response<P>>,
)>;

/// A shard's tables, built the way a shard builds them, with the channels a shard would drain
struct Harness {
    /// The tables themselves
    db: BacklogDb,
    /// Every table's archive map
    table_map: FullArchiveMap<BacklogDbTableNames>,
    /// The channel a loader takes read requests on, and the tables send them on
    loader: (
        AsyncSender<LoaderMsg<BacklogDbTableNames>>,
        AsyncReceiver<LoaderMsg<BacklogDbTableNames>>,
    ),
    /// The channel the loader and the compactors answer the shard on
    shard: (
        AsyncSender<ServerMsg<BacklogDb>>,
        AsyncReceiver<ServerMsg<BacklogDb>>,
    ),
    /// The partition key of the first partition in each table
    first: PartitionKeys,
    /// The partition key of the second partition in each table
    second: PartitionKeys,
}

/// One partition's key in each of the two tables
#[derive(Clone, Copy)]
struct PartitionKeys {
    /// Its key in the sorted table
    sorted: u64,
    /// Its key in the unsorted table
    unsorted: u64,
}

/// The two rows a partition key names
///
/// # Arguments
///
/// * `partition_key` - The partition to build rows for
fn rows(partition_key: &str) -> (SortedRow, UnsortedRow) {
    // one row for each table, under the same partition key
    let sorted = SortedRow {
        partition_key: partition_key.to_owned(),
        sort_key: "a".to_owned(),
        data: "backlog".to_owned(),
    };
    let unsorted = UnsortedRow {
        partition_key: partition_key.to_owned(),
        data: "backlog".to_owned(),
    };
    (sorted, unsorted)
}

/// The partition keys a partition's rows hash to in each table
///
/// # Arguments
///
/// * `partition_key` - The partition to hash
fn keys_of(partition_key: &str) -> PartitionKeys {
    // hash the same rows the seed wrote
    let (sorted, unsorted) = rows(partition_key);
    PartitionKeys {
        sorted: sorted.get_partition_key(),
        unsorted: unsorted.get_partition_key(),
    }
}

/// Write both partitions of both tables through a real server and stop it
///
/// # Arguments
///
/// * `conf` - The config to serve the seed with
fn seed(conf: Conf) {
    // the client is tokio's, so the seed runs on a runtime of its own
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build a runtime for the seed");
    runtime.block_on(async move {
        // start a server over the test's directory
        let (client, pool) = utils::start_with_conf::<BacklogDb>(conf)
            .await
            .expect("failed to start the seed server");
        // write one row into each partition of each table
        for partition_key in [FIRST, SECOND] {
            let (sorted, unsorted) = rows(partition_key);
            client
                .send_one(sorted)
                .await
                .expect("failed to seed a sorted row");
            client
                .send_one(unsorted)
                .await
                .expect("failed to seed an unsorted row");
        }
        // stop the server, leaving its intent logs on disk
        pool.exit().expect("the seed server failed");
    });
}

impl Harness {
    /// Build a shard's tables over a seeded directory, with every partition archived and none
    /// of them resident
    ///
    /// # Arguments
    ///
    /// * `conf` - The config the tables are built with, whose bounds are the ones under test
    async fn open(conf: &Conf) -> Result<Harness, String> {
        // the queue a shard's loader and compactors run on
        let medium = shoal::glommio::executor().create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(100)),
            "MediumPriority:table_backlog",
        );
        // everything a shard hands its tables when it builds them
        let table_map = FullArchiveMap::default();
        let mut loader_channels = HashMap::with_capacity(1);
        let memory = Arc::new(RefCell::new(0));
        let lru: Arc<RefCell<Lru>> = Arc::new(RefCell::new(LruCache::unbounded_with_hasher(
            BuildHasherDefault::<GxHasher>::default(),
        )));
        let shard = kanal::unbounded_async();
        // build the tables, which replays the seed and starts compacting it
        let mut db = <BacklogDb as ShoalDatabase>::new(
            SHARD_NAME,
            &table_map,
            &mut loader_channels,
            conf,
            medium,
            &memory,
            &lru,
            &shard.0,
        )
        .await
        .map_err(|error| format!("failed to build the tables: {error:?}"))?;
        // keep the loader channel the tables send their read requests on
        let loader = loader_channels
            .remove(&Loaders::FileSystem)
            .ok_or("the file system tables made no loader channel")?;
        // wait for each table's compactor to archive what was replayed
        let first = keys_of(FIRST);
        let second = keys_of(SECOND);
        let mut archived = 0;
        while archived < 2 {
            if let ServerMsg::MarkEvictable {
                generation,
                partitions,
                ..
            } = recv_within(&shard.1).await?
            {
                // hand the marking to both tables, since only the one it names holds its keys
                db.sorted.mark_evictable(generation, partitions.clone());
                db.unsorted.mark_evictable(generation, partitions);
                archived += 1;
            }
        }
        // drop every partition from memory, so each is on disk and nowhere else
        db.sorted.evict(vec![first.sorted, second.sorted]);
        db.unsorted.evict(vec![first.unsorted, second.unsorted]);
        check!(
            db.sorted.partitions.is_empty() && db.unsorted.partitions.is_empty(),
            "a partition was still resident after eviction"
        );
        Ok(Harness {
            db,
            table_map,
            loader,
            shard,
            first,
            second,
        })
    }

    /// Start the loader, so read requests are served
    async fn start_loader(&self) -> Outcome {
        // one loader serves every file system table, so either table can spawn it
        self.db
            .sorted
            .spawn_loader(&self.table_map, &self.loader.1, &self.shard.0)
            .await
            .map_err(|error| format!("failed to spawn the loader: {error:?}"))
    }

    /// Wait for the next partition the loader read, and land it on the table it belongs to
    ///
    /// Returns how many parked queries the read released.
    async fn land_next_read(&mut self) -> Result<usize, String> {
        // skip whatever else the shard is told while it waits
        let kinds = loop {
            if let ServerMsg::Partition(kinds) = recv_within(&self.shard.1).await? {
                break kinds;
            }
        };
        // land it on the table the read names, since both tables hash a key the same way
        /// How many queries a landed read released, or why it failed to land
        fn released<Q>(
            landed: Result<PartitionLoad<Q>, shoal::server::ServerError>,
        ) -> Result<usize, String> {
            match landed {
                Ok(PartitionLoad::Loaded(released, _)) => Ok(released.len()),
                Ok(_) => Ok(0),
                Err(error) => Err(format!("a read failed to land: {error:?}")),
            }
        }
        let landed = match kinds.table {
            BacklogDbTableNames::SortedRow => {
                released(self.db.sorted.load_partition(kinds.loaded).await)?
            }
            BacklogDbTableNames::UnsortedRow => {
                released(self.db.unsorted.load_partition(kinds.loaded).await)?
            }
        };
        Ok(landed)
    }
}

/// Wait on the shard's channel for at most ten seconds
///
/// # Arguments
///
/// * `rx` - The channel to wait on
async fn recv_within(
    rx: &AsyncReceiver<ServerMsg<BacklogDb>>,
) -> Result<ServerMsg<BacklogDb>, String> {
    // glommio's own timeout, since this runs on no tokio runtime
    shoal::glommio::timer::timeout(Duration::from_secs(10), async { Ok(rx.recv().await) })
        .await
        .map_err(|_| "timed out waiting on the shard's channel".to_owned())?
        .map_err(|error| format!("the shard's channel closed: {error:?}"))
}

/// A test body, handed the harness it runs against
type Body = Box<
    dyn for<'a> FnOnce(
            &'a mut Harness,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + 'a>>
        + Send,
>;

/// Run a test body against a seeded harness on an executor of its own
///
/// # Arguments
///
/// * `networking` - The networking section the tables are built with, which holds the bounds
/// * `body` - The test, handed the harness
fn with_harness(networking: Networking, body: Body) {
    // a directory on a real filesystem, and a single shard so everything lands on it
    let temp_dir: TempDir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    // write the rows through a real server first, under the default bounds
    seed(conf.clone());
    // then build the tables again, under the bounds this test is about, on an executor it owns
    let mut conf = conf;
    conf.networking = networking;
    let name = format!("table_backlog-{}", OPENED.fetch_add(1, Ordering::SeqCst));
    let outcome = LocalExecutorBuilder::default()
        .name(&name)
        .spawn(move || async move {
            let mut harness = Harness::open(&conf).await?;
            let outcome = body(&mut harness).await;
            // keep the directory until the tables are done with it
            drop(harness);
            drop(temp_dir);
            outcome
        })
        .expect("failed to spawn the harness executor")
        .join()
        .expect("the harness executor panicked");
    // fail here, on the test's own thread, where a panic is only this test's
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}

/// A seal no test here ever reaches, since no partition a get names is resident
///
/// # Arguments
///
/// * `_response` - The response it would have serialized
fn never_sealed_sorted(
    _response: Response<RowRef<'_, SortedRow>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error> {
    unreachable!("a query answered in place")
}

/// A seal no test here ever reaches, since no partition a get names is resident
///
/// # Arguments
///
/// * `_response` - The response it would have serialized
fn never_sealed_unsorted(
    _response: Response<RowRef<'_, UnsortedRow>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error> {
    unreachable!("a query answered in place")
}

/// The metadata of a query arriving from a client
fn meta() -> QueryMetadata {
    QueryMetadata::untimed(Uuid::new_v4(), Uuid::new_v4(), 0, true, None, Span::none())
}

/// Say whether a table's answer is a refusal by the bound, rather than anything else
///
/// # Arguments
///
/// * `answered` - What the table answered with
fn is_shed<P>(answered: &Answered<P>) -> bool {
    // only an open answer can carry a failure
    matches!(
        answered,
        Some((_, _, _, Answer::Open(Response { data: ResponseAction::Error(error), .. })))
            if error.code() == ErrorCode::Shedding
    )
}

/// An unsorted get of some partitions
///
/// # Arguments
///
/// * `partition_keys` - The partitions to read
fn unsorted_get(partition_keys: Vec<u64>) -> UnsortedQuery<UnsortedRow> {
    UnsortedQuery::Get(UnsortedGet::<UnsortedRow> {
        partition_keys,
        filters: None,
        limit: None,
        projection: <UnsortedRow as ShoalTableSupport>::Projection::default(),
    })
}

/// A sorted get of some partitions
///
/// # Arguments
///
/// * `partition_keys` - The partitions to read
fn sorted_get(partition_keys: Vec<u64>) -> SortedQuery<SortedRow> {
    SortedQuery::Get(SortedGet::<SortedRow> {
        partition_keys,
        sort_select: SortSelect::All,
        filters: None,
        limit: None,
        projection: <SortedRow as ShoalTableSupport>::Projection::default(),
    })
}

/// A write to an unsorted table past its pending bound is shed, and nothing of it is written
///
/// Two inserts are held waiting on their fdatasync, since nothing sweeps the table, so the third
/// finds the queue at the bound and is answered `Shedding` - its row nowhere in the table. Once
/// the two are made durable and released, the same insert is taken.
#[test]
fn an_unsorted_write_past_the_pending_bound_is_shed_and_never_written() {
    with_harness(
        Networking::default().max_pending_writes(2),
        Box::new(|harness| {
            Box::pin(async move {
                // two inserts wait on their fdatasync, and the third finds the queue full
                let mut answers = Vec::new();
                for key in ["w0", "w1", "w2"] {
                    let (_, row) = rows(key);
                    let query = UnsortedQuery::Insert {
                        key: row.get_partition_key(),
                        row,
                    };
                    answers.push(
                        harness
                            .db
                            .unsorted
                            .handle(meta(), query, never_sealed_unsorted)
                            .await,
                    );
                }
                check!(
                    answers[0].is_none() && answers[1].is_none(),
                    "a write under the bound was answered before it was durable"
                );
                check!(
                    is_shed(&answers[2]),
                    "the write past the pending bound was not shed: {:?}",
                    answers[2].as_ref().map(|(_, _, _, answer)| match answer {
                        Answer::Open(response) => format!("{:?}", response.data),
                        Answer::Sealed(_) => "sealed".to_owned(),
                    })
                );
                // a shed write committed nothing, so its row is nowhere in the table
                let shed_key = rows("w2").1.get_partition_key();
                check!(
                    !harness.db.unsorted.partitions.contains_key(&shed_key),
                    "a shed insert was written anyway"
                );
                // make the two durable, and sweep until both are released
                release_all!(harness.db.unsorted, 2);
                // and the same insert is taken now
                let (_, row) = rows("w2");
                let query = UnsortedQuery::Insert {
                    key: row.get_partition_key(),
                    row,
                };
                let answered = harness
                    .db
                    .unsorted
                    .handle(meta(), query, never_sealed_unsorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a write under the bound was refused after the queue drained"
                );
                check!(
                    harness.db.unsorted.partitions.contains_key(&shed_key),
                    "a write taken after the queue drained was not written"
                );
                Ok(())
            })
        }),
    );
}

/// A write to a sorted table past its pending bound is shed, and nothing of it is written
///
/// The sorted half of the test above.
#[test]
fn a_sorted_write_past_the_pending_bound_is_shed_and_never_written() {
    with_harness(
        Networking::default().max_pending_writes(2),
        Box::new(|harness| {
            Box::pin(async move {
                // two inserts wait on their fdatasync, and the third finds the queue full
                let mut answers = Vec::new();
                for key in ["w0", "w1", "w2"] {
                    let (row, _) = rows(key);
                    let query = SortedQuery::Insert {
                        key: row.get_partition_key(),
                        row,
                    };
                    answers.push(
                        harness
                            .db
                            .sorted
                            .handle(meta(), query, never_sealed_sorted)
                            .await,
                    );
                }
                check!(
                    answers[0].is_none() && answers[1].is_none(),
                    "a write under the bound was answered before it was durable"
                );
                check!(
                    is_shed(&answers[2]),
                    "the write past the pending bound was not shed"
                );
                // a shed write committed nothing, so its row is nowhere in the table
                let shed_key = rows("w2").0.get_partition_key();
                check!(
                    !harness.db.sorted.partitions.contains_key(&shed_key),
                    "a shed insert was written anyway"
                );
                // make the two durable, and sweep until both are released
                release_all!(harness.db.sorted, 2);
                // and the same insert is taken now
                let (row, _) = rows("w2");
                let query = SortedQuery::Insert {
                    key: row.get_partition_key(),
                    row,
                };
                let answered = harness
                    .db
                    .sorted
                    .handle(meta(), query, never_sealed_sorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a write under the bound was refused after the queue drained"
                );
                Ok(())
            })
        }),
    );
}

/// A read of an unsorted partition past the parked bound is shed before it parks
///
/// One get parks on the first partition's read, which no loader serves yet, so the table holds
/// one parked query. A get of the second partition is answered `Shedding` at once and asks for
/// no read. Once the first read lands and releases its get, the second is parked like any other.
#[test]
fn an_unsorted_read_past_the_parked_bound_is_shed_before_it_parks() {
    with_harness(
        Networking::default().max_parked_queries(1),
        Box::new(|harness| {
            Box::pin(async move {
                let (first, second) = (harness.first.unsorted, harness.second.unsorted);
                // the first get parks on its partition's read
                let answered = harness
                    .db
                    .unsorted
                    .handle(meta(), unsorted_get(vec![first]), never_sealed_unsorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a get of a partition on disk did not park"
                );
                check_eq!(harness.loader.1.len(), 1, "the first get asked for no read");
                // the second finds the table at its bound and is shed without asking for a read
                let answered = harness
                    .db
                    .unsorted
                    .handle(meta(), unsorted_get(vec![second]), never_sealed_unsorted)
                    .await;
                check!(
                    is_shed(&answered),
                    "a get past the parked bound was not shed"
                );
                check_eq!(
                    harness.loader.1.len(),
                    1,
                    "a shed get asked for a read anyway"
                );
                // the first read lands and releases its get
                harness.start_loader().await?;
                check_eq!(
                    harness.land_next_read().await?,
                    1,
                    "the first read did not release its get"
                );
                // and with nothing parked, the second get parks like any other
                let answered = harness
                    .db
                    .unsorted
                    .handle(meta(), unsorted_get(vec![second]), never_sealed_unsorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a get under the parked bound was not parked"
                );
                Ok(())
            })
        }),
    );
}

/// A read of a sorted partition past the parked bound is shed before it parks
///
/// The sorted half of the test above.
#[test]
fn a_sorted_read_past_the_parked_bound_is_shed_before_it_parks() {
    with_harness(
        Networking::default().max_parked_queries(1),
        Box::new(|harness| {
            Box::pin(async move {
                let (first, second) = (harness.first.sorted, harness.second.sorted);
                // the first get parks on its partition's read
                let answered = harness
                    .db
                    .sorted
                    .handle(meta(), sorted_get(vec![first]), never_sealed_sorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a get of a partition on disk did not park"
                );
                check_eq!(harness.loader.1.len(), 1, "the first get asked for no read");
                // the second finds the table at its bound and is shed without asking for a read
                let answered = harness
                    .db
                    .sorted
                    .handle(meta(), sorted_get(vec![second]), never_sealed_sorted)
                    .await;
                check!(
                    is_shed(&answered),
                    "a get past the parked bound was not shed"
                );
                check_eq!(
                    harness.loader.1.len(),
                    1,
                    "a shed get asked for a read anyway"
                );
                // the first read lands and releases its get
                harness.start_loader().await?;
                check_eq!(
                    harness.land_next_read().await?,
                    1,
                    "the first read did not release its get"
                );
                // and with nothing parked, the second get parks like any other
                let answered = harness
                    .db
                    .sorted
                    .handle(meta(), sorted_get(vec![second]), never_sealed_sorted)
                    .await;
                check!(
                    answered.is_none(),
                    "a get under the parked bound was not parked"
                );
                Ok(())
            })
        }),
    );
}

/// A get that has parked on one partition is never shed parking on the next
///
/// Shedding it there would leave its first share parked with nobody to answer, so a query is
/// only ever shed before any part of it parks. A get of both partitions under a bound of one
/// parks on both; the next fresh get is the one shed.
#[test]
fn a_get_that_has_parked_is_never_shed() {
    with_harness(
        Networking::default().max_parked_queries(1),
        Box::new(|harness| {
            Box::pin(async move {
                let (first, second) = (harness.first.unsorted, harness.second.unsorted);
                // one get of both partitions parks on both, passing the bound on the second
                let answered = harness
                    .db
                    .unsorted
                    .handle(
                        meta(),
                        unsorted_get(vec![first, second]),
                        never_sealed_unsorted,
                    )
                    .await;
                check!(
                    answered.is_none(),
                    "a get that had parked on one partition was answered rather than parked"
                );
                check_eq!(
                    harness.loader.1.len(),
                    2,
                    "a get that had parked did not ask for its second read"
                );
                // and the next fresh get finds the table past its bound
                let answered = harness
                    .db
                    .unsorted
                    .handle(meta(), unsorted_get(vec![first]), never_sealed_unsorted)
                    .await;
                check!(
                    is_shed(&answered),
                    "a fresh get past the parked bound was not shed"
                );
                Ok(())
            })
        }),
    );
}
