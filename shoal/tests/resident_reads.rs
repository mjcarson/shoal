//! Integration tests for a partition read that lands on a copy already in memory
//!
//! A read from disk can land on a partition that is already resident when two reads of it were
//! asked for at once - a replicated apply's `request_load` and a query's `block_on_load`, which
//! never looked at each other's bookkeeping. The first read to land makes the partition resident
//! and the second lands on it. What the table did then was answered badly in both tables: the
//! sorted one threw the read away without a word (item 30), and the unsorted one replaced the
//! resident copy and charged the shard's memory counter for both (item 120). And the duplicate
//! read should never have been asked for at all (item 121).
//!
//! None of that is reachable from a standalone server, since only a replicated apply ever asks
//! for a read through `request_load`, and a cluster makes the two reads a race. So these tests
//! build a shard's tables the way a shard does - on a glommio executor, against the storage a
//! real server left behind - and drive `load_partition` directly, delivering the same read twice
//! and a read of one partition onto another, which is what a race would do and what a race
//! could never be made to do on demand.
//!
//! See `docs/src/appendix/resolved/resident-copy-collision.md`.

use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::glommio::{Latency, LocalExecutorBuilder, Shares};
use shoal::gxhash::GxHasher;
use shoal::kanal::{self, AsyncReceiver, AsyncSender};
use shoal::lru::LruCache;
use shoal::server::conf::Conf;
use shoal::server::messages::{LoadedPartition, QueryMetadata, ServerMsg};
use shoal::shared::queries::{SortSelect, SortedGet, SortedQuery, UnsortedGet, UnsortedQuery};
use shoal::shared::responses::Response;
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
use tracing::{Event, Level, Span};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use uuid::Uuid;

mod utils;

/// A sorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ResidentDb")]
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
#[shoal_table(db = "ResidentDb")]
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
pub struct ResidentDb {
    /// The sorted table
    pub sorted: PersistentSortedTable<SortedRow, FileSystem>,
    /// The unsorted table
    pub unsorted: PersistentUnsortedTable<UnsortedRow, FileSystem>,
}

/// The partition every test reads twice
const FIRST: &str = "first";

/// The partition whose read is delivered onto the first one in the divergence tests
///
/// Its row is a different length from the first partition's, so its archive cannot be mistaken
/// for the first one's by size alone.
const SECOND: &str = "second";

/// The shard a single shard server names its one executor
const SHARD_NAME: &str = "Shard-0";

thread_local! {
    /// How many error level events this thread's tables have emitted
    ///
    /// Thread local, because each test runs its tables on an executor thread of its own and
    /// installs its counting subscriber there - so tests running beside each other in this
    /// binary do not add to each other's count.
    static ERRORS: RefCell<usize> = const { RefCell::new(0) };
}

/// How many harnesses have been opened, for naming their executors
static OPENED: AtomicUsize = AtomicUsize::new(0);

/// A tracing layer that counts error level events
struct CountErrors;

impl<S: tracing::Subscriber> Layer<S> for CountErrors {
    /// Count this event if it is at error level
    ///
    /// # Arguments
    ///
    /// * `event` - The event being recorded
    /// * `_ctx` - The context of the subscriber this layer is part of
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // only errors count
        if *event.metadata().level() == Level::ERROR {
            // one more error on this thread
            ERRORS.with(|errors| *errors.borrow_mut() += 1);
        }
    }
}

/// The lru cache a shard shares between its tables
type Lru = LruCache<(ResidentDbTableNames, u64), usize, BuildHasherDefault<GxHasher>>;

/// A shard's tables, built the way a shard builds them, with the channels a shard would drain
struct Harness {
    /// The tables themselves
    db: ResidentDb,
    /// Every table's archive map
    table_map: FullArchiveMap<ResidentDbTableNames>,
    /// The channel a loader takes read requests on, and the tables send them on
    loader: (
        AsyncSender<LoaderMsg<ResidentDbTableNames>>,
        AsyncReceiver<LoaderMsg<ResidentDbTableNames>>,
    ),
    /// The channel the loader and the compactors answer the shard on
    shard: (
        AsyncSender<ServerMsg<ResidentDb>>,
        AsyncReceiver<ServerMsg<ResidentDb>>,
    ),
    /// The shard's memory counter, which the tables charge and credit
    memory: Arc<RefCell<usize>>,
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

/// The two rows a partition key names in the seed
///
/// # Arguments
///
/// * `partition_key` - The partition to build rows for
fn rows(partition_key: &str) -> (SortedRow, UnsortedRow) {
    // the second partition's payload is longer, so its archive is a different length
    let data = if partition_key == FIRST {
        "short".to_owned()
    } else {
        "a considerably longer payload".to_owned()
    };
    // one row for each table
    let sorted = SortedRow {
        partition_key: partition_key.to_owned(),
        sort_key: "a".to_owned(),
        data: data.clone(),
    };
    let unsorted = UnsortedRow {
        partition_key: partition_key.to_owned(),
        data,
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
        let (client, pool) = utils::start_with_conf::<ResidentDb>(conf)
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

/// What a test body answers with
type Outcome = Result<(), String>;

impl Harness {
    /// Build a shard's tables over a seeded directory, with every partition archived and none
    /// of them resident
    ///
    /// The tables replay the seed's intent logs and compact them, which is what a shard's start
    /// does; this waits for both compactors to say so, then evicts every partition, so a read is
    /// the only way back in.
    ///
    /// # Arguments
    ///
    /// * `conf` - The config the seed was served with
    async fn open(conf: &Conf) -> Result<Harness, String> {
        // the queue a shard's loader and compactors run on
        let medium = shoal::glommio::executor().create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(100)),
            "MediumPriority:resident_reads",
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
        let mut db = <ResidentDb as ShoalDatabase>::new(
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
        check_eq!(*memory.borrow(), 0, "eviction left memory charged");
        Ok(Harness {
            db,
            table_map,
            loader,
            shard,
            memory,
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

    /// Wait for the next partition the loader read
    async fn next_read(&self) -> Result<LoadedPartition, String> {
        loop {
            // skip whatever else the shard is told while it waits
            if let ServerMsg::Partition(kinds) = recv_within(&self.shard.1).await? {
                return Ok(kinds.loaded);
            }
        }
    }

    /// Read a partition of the sorted table from disk
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition to read
    async fn read_sorted(&mut self, partition_key: u64) -> Result<LoadedPartition, String> {
        // ask for it the way a replicated apply does
        let coming = self
            .db
            .sorted
            .request_load(partition_key, &Span::none())
            .await
            .map_err(|error| format!("failed to ask for a read: {error:?}"))?;
        check!(coming, "an archived partition had nothing to read");
        self.next_read().await
    }

    /// Read a partition of the unsorted table from disk
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition to read
    async fn read_unsorted(&mut self, partition_key: u64) -> Result<LoadedPartition, String> {
        // ask for it the way a replicated apply does
        let coming = self
            .db
            .unsorted
            .request_load(partition_key, &Span::none())
            .await
            .map_err(|error| format!("failed to ask for a read: {error:?}"))?;
        check!(coming, "an archived partition had nothing to read");
        self.next_read().await
    }

    /// What the shard's memory counter says right now
    fn memory(&self) -> usize {
        *self.memory.borrow()
    }
}

/// Wait on the shard's channel for at most ten seconds
///
/// # Arguments
///
/// * `rx` - The channel to wait on
async fn recv_within(
    rx: &AsyncReceiver<ServerMsg<ResidentDb>>,
) -> Result<ServerMsg<ResidentDb>, String> {
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
/// * `body` - The test, handed the harness
fn with_harness(body: Body) {
    // a directory on a real filesystem, and a single shard so everything lands on it
    let temp_dir: TempDir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    // write the rows through a real server first
    seed(conf.clone());
    // then build the tables again on an executor this test owns
    let name = format!("resident_reads-{}", OPENED.fetch_add(1, Ordering::SeqCst));
    let outcome = LocalExecutorBuilder::default()
        .name(&name)
        .spawn(move || async move {
            // count the errors this thread's tables emit, and only this thread's
            let subscriber = tracing_subscriber::registry().with(CountErrors);
            let _guard = tracing::subscriber::set_default(subscriber);
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

/// How many errors this thread has counted
fn errors() -> usize {
    ERRORS.with(|errors| *errors.borrow())
}

/// A seal a get that parks never calls
///
/// # Arguments
///
/// * `_response` - The response it would have serialized
fn never_sealed_sorted(
    _response: Response<RowRef<'_, SortedRow>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error> {
    unreachable!("a get of a partition that is not resident answered in place")
}

/// A seal a get that parks never calls
///
/// # Arguments
///
/// * `_response` - The response it would have serialized
fn never_sealed_unsorted(
    _response: Response<RowRef<'_, UnsortedRow>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error> {
    unreachable!("a get of a partition that is not resident answered in place")
}

/// The metadata of a get arriving from a client
fn get_meta() -> QueryMetadata {
    QueryMetadata::untimed(Uuid::new_v4(), Uuid::new_v4(), 0, true, None, Span::none())
}

/// Land a read on a table, saying what went wrong if it failed
///
/// # Arguments
///
/// * `landed` - The table's answer to the read
fn landed<Q>(
    landed: Result<PartitionLoad<Q>, shoal::server::ServerError>,
) -> Result<PartitionLoad<Q>, String> {
    landed.map_err(|error| format!("a read failed to land: {error:?}"))
}

/// A second read of a sorted partition that is already resident changes nothing
///
/// The copy in memory and the one read are the same archive extent, so the table keeps the
/// one it has and charges nothing for the one it drops - and says nothing, since nothing is
/// wrong.
#[test]
fn a_second_read_of_a_resident_sorted_partition_is_not_charged() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            harness.start_loader().await?;
            // read the partition and land it
            let read = harness.read_sorted(harness.first.sorted).await?;
            landed(harness.db.sorted.load_partition(read.clone()).await)?;
            let charged = harness.memory();
            check!(charged > 0, "a resident archive was not charged");
            // land the same read again, which is what a duplicate read looks like
            landed(harness.db.sorted.load_partition(read).await)?;
            check_eq!(
                harness.memory(),
                charged,
                "a second read of a resident partition changed the memory counter"
            );
            check_eq!(
                errors(),
                0,
                "a read agreeing with the resident copy was reported"
            );
            Ok(())
        })
    }));
}

/// A second read of an unsorted partition that is already resident changes nothing
///
/// Item 120: the unsorted table replaced the resident archive with the read and added the
/// read's size to the counter without taking the replaced archive's off, so every duplicate
/// read charged the shard for a partition it no longer held.
#[test]
fn a_second_read_of_a_resident_unsorted_partition_is_not_charged() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            harness.start_loader().await?;
            // read the partition and land it
            let read = harness.read_unsorted(harness.first.unsorted).await?;
            landed(harness.db.unsorted.load_partition(read.clone()).await)?;
            let charged = harness.memory();
            check!(charged > 0, "a resident archive was not charged");
            // land the same read again, which is what a duplicate read looks like
            landed(harness.db.unsorted.load_partition(read).await)?;
            check_eq!(
                harness.memory(),
                charged,
                "a second read of a resident partition changed the memory counter"
            );
            check_eq!(
                errors(),
                0,
                "a read agreeing with the resident copy was reported"
            );
            Ok(())
        })
    }));
}

/// A read that disagrees with a resident sorted partition is reported, and the resident copy kept
///
/// Item 30: the sorted table's merge arm only matched a partition that had been written to, so
/// a read landing on an archive fell through it without a word. Two copies of one extent never
/// differ today, which is exactly why a difference has to be loud if it ever happens.
#[test]
fn a_divergent_read_of_a_resident_sorted_partition_is_reported() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            harness.start_loader().await?;
            // make the first partition resident
            let first = harness.read_sorted(harness.first.sorted).await?;
            landed(harness.db.sorted.load_partition(first).await)?;
            let digest = harness
                .db
                .sorted
                .digest()
                .await
                .map_err(|e| format!("{e:?}"))?;
            let charged = harness.memory();
            // read the second partition and deliver it as though it were the first
            let mut second = harness.read_sorted(harness.second.sorted).await?;
            second.partition_id = harness.first.sorted;
            landed(harness.db.sorted.load_partition(second).await)?;
            // the table still answers from the copy it had
            let after = harness
                .db
                .sorted
                .digest()
                .await
                .map_err(|e| format!("{e:?}"))?;
            check_eq!(after, digest, "a divergent read replaced the resident copy");
            check_eq!(harness.memory(), charged, "a divergent read was charged");
            // and said that the two copies disagreed
            check_eq!(
                errors(),
                1,
                "a divergent read of a resident partition was not reported"
            );
            Ok(())
        })
    }));
}

/// A read that disagrees with a resident unsorted partition is reported, and the resident copy
/// kept
///
/// The unsorted table overwrote the resident archive with whatever was read, so a divergent
/// read silently changed the row the partition answered with.
#[test]
fn a_divergent_read_of_a_resident_unsorted_partition_is_reported() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            harness.start_loader().await?;
            // make the first partition resident
            let first = harness.read_unsorted(harness.first.unsorted).await?;
            landed(harness.db.unsorted.load_partition(first).await)?;
            let digest = harness
                .db
                .unsorted
                .digest()
                .await
                .map_err(|e| format!("{e:?}"))?;
            let charged = harness.memory();
            // read the second partition and deliver it as though it were the first
            let mut second = harness.read_unsorted(harness.second.unsorted).await?;
            second.partition_id = harness.first.unsorted;
            landed(harness.db.unsorted.load_partition(second).await)?;
            // the table still answers from the copy it had
            let after = harness
                .db
                .unsorted
                .digest()
                .await
                .map_err(|e| format!("{e:?}"))?;
            check_eq!(after, digest, "a divergent read replaced the resident copy");
            check_eq!(harness.memory(), charged, "a divergent read was charged");
            // and said that the two copies disagreed
            check_eq!(
                errors(),
                1,
                "a divergent read of a resident partition was not reported"
            );
            Ok(())
        })
    }));
}

/// A get of a sorted partition an apply is already reading waits on that read
///
/// Item 121: `block_on_load` only looked for queries already parked, not for a read an apply
/// asked for, so a get arriving while an apply's read was in flight asked for a second one -
/// which is the duplicate that lands on a resident copy.
#[test]
fn a_sorted_get_waits_on_an_applys_read_rather_than_asking_again() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            let key = harness.first.sorted;
            // an apply asks for the partition, with no loader running to serve it yet
            let coming = harness
                .db
                .sorted
                .request_load(key, &Span::none())
                .await
                .map_err(|e| format!("{e:?}"))?;
            check!(coming, "an archived partition had nothing to read");
            check_eq!(
                harness.loader.1.len(),
                1,
                "the apply's read was not asked for"
            );
            // a get of the same partition arrives while that read is in flight
            let get = SortedGet::<SortedRow> {
                partition_keys: vec![key],
                sort_select: SortSelect::All,
                filters: None,
                limit: None,
                projection: <SortedRow as ShoalTableSupport>::Projection::default(),
            };
            let answered = harness
                .db
                .sorted
                .handle(get_meta(), SortedQuery::Get(get), never_sealed_sorted)
                .await;
            check!(
                answered.is_none(),
                "a get of a partition on disk did not park"
            );
            check_eq!(
                harness.loader.1.len(),
                1,
                "a get asked for a read an apply had already asked for"
            );
            // the apply's read lands and releases the get with it
            harness.start_loader().await?;
            let read = harness.next_read().await?;
            let PartitionLoad::Loaded(released, _) =
                landed(harness.db.sorted.load_partition(read).await)?
            else {
                return Err("the apply's read released nothing".to_owned());
            };
            check_eq!(
                released.len(),
                1,
                "the get was not released by the apply's read"
            );
            Ok(())
        })
    }));
}

/// A get of an unsorted partition an apply is already reading waits on that read
///
/// The unsorted half of item 121.
#[test]
fn an_unsorted_get_waits_on_an_applys_read_rather_than_asking_again() {
    with_harness(Box::new(|harness| {
        Box::pin(async move {
            let key = harness.first.unsorted;
            // an apply asks for the partition, with no loader running to serve it yet
            let coming = harness
                .db
                .unsorted
                .request_load(key, &Span::none())
                .await
                .map_err(|e| format!("{e:?}"))?;
            check!(coming, "an archived partition had nothing to read");
            check_eq!(
                harness.loader.1.len(),
                1,
                "the apply's read was not asked for"
            );
            // a get of the same partition arrives while that read is in flight
            let get = UnsortedGet::<UnsortedRow> {
                partition_keys: vec![key],
                filters: None,
                limit: None,
                projection: <UnsortedRow as ShoalTableSupport>::Projection::default(),
            };
            let answered = harness
                .db
                .unsorted
                .handle(get_meta(), UnsortedQuery::Get(get), never_sealed_unsorted)
                .await;
            check!(
                answered.is_none(),
                "a get of a partition on disk did not park"
            );
            check_eq!(
                harness.loader.1.len(),
                1,
                "a get asked for a read an apply had already asked for"
            );
            // the apply's read lands and releases the get with it
            harness.start_loader().await?;
            let read = harness.next_read().await?;
            let PartitionLoad::Loaded(released, _) =
                landed(harness.db.unsorted.load_partition(read).await)?
            else {
                return Err("the apply's read released nothing".to_owned());
            };
            check_eq!(
                released.len(),
                1,
                "the get was not released by the apply's read"
            );
            Ok(())
        })
    }));
}
