//! Integration tests for the failures a table's request path used to answer with a panic
//!
//! A query that a table could not serve used to take its shard down with it in three places a
//! running server can reach: a write whose commit failed, a read whose request the loader could
//! not take, and a get whose parked progress was of another projection
//! ([item 16](../../docs/src/appendix/resolved/hot-path-panics.md)). Each is now answered as a
//! failure of that one query, and a write that is refused leaves the table exactly as it found it.
//!
//! A commit cannot be made to fail on demand on a standalone table - its writer records a device
//! error for the sweep rather than returning it - so the write tests build a table the way a
//! cluster node does, whose writes go through a tablet group and whose own log does not exist.
//! Rows are put into it the way a replica puts them, through `apply`, and a write sent to it the
//! way a client's would be is the one whose commit fails.
//!
//! See `docs/src/appendix/resolved/hot-path-panics.md`.

use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::glommio::{Latency, LocalExecutorBuilder, Shares};
use shoal::gxhash::GxHasher;
use shoal::kanal::{self, AsyncReceiver, AsyncSender};
use shoal::lru::LruCache;
use shoal::server::conf::cluster::Cluster;
use shoal::server::conf::Conf;
use shoal::server::messages::{Answer, QueryMetadata, ServerMsg};
use shoal::server::shard::ShardContact;
use shoal::shared::identity::TableId;
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::protocol::peer::{Command, RequestId};
use shoal::shared::queries::{
    SortSelect, SortedExists, SortedGet, SortedQuery, UnsortedGet, UnsortedQuery,
};
use shoal::shared::responses::{Response, ResponseAction};
use shoal::shared::row_ref::RowRef;
use shoal::shared::traits::{PartitionKeySupport, ShoalTableSupport};
use shoal::storage::{FileSystem, FullArchiveMap, LoaderMsg, Loaders};
use shoal::tables::{ApplyStep, PersistentSortedTable, PersistentUnsortedTable};
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
#[shoal_table(db = "HotPathDb")]
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
#[shoal_table(db = "HotPathDb")]
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
pub struct HotPathDb {
    /// The sorted table
    pub sorted: PersistentSortedTable<SortedRow, FileSystem>,
    /// The unsorted table
    pub unsorted: PersistentUnsortedTable<UnsortedRow, FileSystem>,
}

/// The partition every test writes
const PARTITION: &str = "partition";

/// The shard a single shard server names its one executor
const SHARD_NAME: &str = "Shard-0";

/// How many harnesses have been opened, for naming their executors
static OPENED: AtomicUsize = AtomicUsize::new(0);

/// Fail a test body with a message rather than panicking
///
/// A panic on a glommio executor thread aborts the whole binary while it unwinds, so the bodies
/// here report a failure as an error and `with_tables` panics with it on the test's own thread.
macro_rules! check {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+));
        }
    };
}

/// What a test body answers with
type Outcome = Result<(), String>;

/// The lru cache a shard shares between its tables
type Lru = LruCache<(HotPathDbTableNames, u64), usize, BuildHasherDefault<GxHasher>>;

/// What a table answers a query with, if it answers it at once
type Answered<P> = Option<(
    Uuid,
    Uuid,
    shoal::server::stage_profile::StageStamps,
    Answer<Response<P>>,
)>;

/// A shard's tables, built the way a shard builds them, with the channels a shard would drain
struct Tables {
    /// The tables themselves
    db: HotPathDb,
    /// The channel a loader takes read requests on, and the tables send them on
    loader: (
        AsyncSender<LoaderMsg<HotPathDbTableNames>>,
        AsyncReceiver<LoaderMsg<HotPathDbTableNames>>,
    ),
    /// The channel the loader and the compactors answer the shard on, held open
    _shard: (
        AsyncSender<ServerMsg<HotPathDb>>,
        AsyncReceiver<ServerMsg<HotPathDb>>,
    ),
}

impl Tables {
    /// Build a shard's tables over a directory
    ///
    /// # Arguments
    ///
    /// * `conf` - The config the tables are built with
    /// * `seeded` - Whether a server wrote a row to each table first, which is then archived and
    ///   dropped from memory so it is on disk and nowhere else
    async fn open(conf: &Conf, seeded: bool) -> Result<Tables, String> {
        // the queue a shard's loader and compactors run on
        let medium = shoal::glommio::executor().create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(100)),
            "MediumPriority:hot_path_failures",
        );
        // everything a shard hands its tables when it builds them
        let table_map = FullArchiveMap::default();
        let mut loader_channels = HashMap::with_capacity(1);
        let memory = Arc::new(RefCell::new(0));
        let lru: Arc<RefCell<Lru>> = Arc::new(RefCell::new(LruCache::unbounded_with_hasher(
            BuildHasherDefault::<GxHasher>::default(),
        )));
        let shard = kanal::unbounded_async();
        // build the tables, which replays any seed and starts compacting it
        let mut db = <HotPathDb as ShoalDatabase>::new(
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
        // a seeded row is replayed into memory, so wait for it to be archived and drop it
        if seeded {
            // wait for each table's compactor to archive what was replayed
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
            // drop both partitions from memory, so each is on disk and nowhere else
            let (sorted, unsorted) = rows("");
            db.sorted.evict(vec![sorted.get_partition_key()]);
            db.unsorted.evict(vec![unsorted.get_partition_key()]);
            check!(
                db.sorted.partitions.is_empty() && db.unsorted.partitions.is_empty(),
                "a partition was still resident after eviction"
            );
        }
        Ok(Tables {
            db,
            loader,
            _shard: shard,
        })
    }
}

/// Wait on the shard's channel for at most ten seconds
///
/// # Arguments
///
/// * `rx` - The channel to wait on
async fn recv_within(
    rx: &AsyncReceiver<ServerMsg<HotPathDb>>,
) -> Result<ServerMsg<HotPathDb>, String> {
    // glommio's own timeout, since this runs on no tokio runtime
    shoal::glommio::timer::timeout(Duration::from_secs(10), async { Ok(rx.recv().await) })
        .await
        .map_err(|_| "timed out waiting on the shard's channel".to_owned())?
        .map_err(|error| format!("the shard's channel closed: {error:?}"))
}

/// A test body, handed the tables it runs against
type Body = Box<
    dyn for<'a> FnOnce(
            &'a mut Tables,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + 'a>>
        + Send,
>;

/// Run a test body against a shard's tables on an executor of its own
///
/// # Arguments
///
/// * `cluster` - Whether the tables are built the way a cluster node builds them
/// * `seed` - Whether a real server writes a row to each table's partition first
/// * `body` - The test, handed the tables
fn with_tables(cluster: bool, seed: bool, body: Body) {
    // a directory on a real filesystem, and a single shard so everything lands on it
    let temp_dir: TempDir = utils::test_dir();
    let mut conf = utils::build_single_shard_config(&temp_dir);
    // write the rows through a real server first, if this test reads them back from disk
    if seed {
        seed_rows(conf.clone());
    }
    // a cluster node's tables write through their groups, and have no log of their own
    if cluster {
        conf.cluster = Some(Cluster::default());
    }
    let name = format!(
        "hot_path_failures-{}",
        OPENED.fetch_add(1, Ordering::SeqCst)
    );
    let outcome = LocalExecutorBuilder::default()
        .name(&name)
        .spawn(move || async move {
            let mut tables = Tables::open(&conf, seed).await?;
            let outcome = body(&mut tables).await;
            // keep the directory until the tables are done with it
            drop(tables);
            drop(temp_dir);
            outcome
        })
        .expect("failed to spawn the tables' executor")
        .join()
        .expect("the tables' executor panicked");
    // fail here, on the test's own thread, where a panic is only this test's
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}

/// Write one row to each table's partition through a real server, and stop it
///
/// # Arguments
///
/// * `conf` - The config the server runs under
fn seed_rows(conf: Conf) {
    // the client is tokio's, so the seed runs on a runtime of its own
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build a runtime for the seed");
    runtime.block_on(async move {
        // start a server over the test's directory
        let (client, pool) = utils::start_with_conf::<HotPathDb>(conf)
            .await
            .expect("failed to start the seed server");
        // write one row into each table
        let (sorted, unsorted) = rows("seeded");
        client
            .send_one(sorted)
            .await
            .expect("failed to seed a sorted row");
        client
            .send_one(unsorted)
            .await
            .expect("failed to seed an unsorted row");
        // stop the server, leaving its intent logs on disk
        pool.exit().expect("the seed server failed");
    });
}

/// The two rows this test writes, carrying some data
///
/// # Arguments
///
/// * `data` - The data both rows carry
fn rows(data: &str) -> (SortedRow, UnsortedRow) {
    (
        SortedRow {
            partition_key: PARTITION.to_owned(),
            sort_key: "sort".to_owned(),
            data: data.to_owned(),
        },
        UnsortedRow {
            partition_key: PARTITION.to_owned(),
            data: data.to_owned(),
        },
    )
}

/// A seal no test here reaches, since every get is asked as a share of a split one
///
/// # Arguments
///
/// * `_response` - The response it would have serialized
fn never_sealed_sorted(
    _response: Response<RowRef<'_, SortedRow>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error> {
    unreachable!("a query answered in place")
}

/// A seal no test here reaches, since every get is asked as a share of a split one
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

/// The metadata of a share of a split query, which is always answered with owned rows
fn share_meta() -> QueryMetadata {
    QueryMetadata::untimed(
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        true,
        Some(ShardContact::Local(0)),
        Span::none(),
    )
}

/// The code a table answered with, if it answered with a failure
///
/// # Arguments
///
/// * `answered` - What the table answered with
fn error_code<P>(answered: &Answered<P>) -> Option<ErrorCode> {
    // only an open answer can carry a failure
    match answered {
        Some((
            _,
            _,
            _,
            Answer::Open(Response {
                data: ResponseAction::Error(error),
                ..
            }),
        )) => Some(error.code()),
        _ => None,
    }
}

/// Describe what a table answered with, for a failure message
///
/// # Arguments
///
/// * `answered` - What the table answered with
fn describe<P: std::fmt::Debug>(answered: &Answered<P>) -> String {
    match answered {
        Some((_, _, _, Answer::Open(response))) => format!("{:?}", response.data),
        Some((_, _, _, Answer::Sealed(_))) => "sealed".to_owned(),
        None => "nothing yet".to_owned(),
    }
}

/// Wrap an intent in the command a tablet group would have committed
///
/// # Arguments
///
/// * `payload` - The serialized intent the group would have committed
fn command(payload: Vec<u8>) -> Command {
    Command {
        table: TableId(0),
        tablet: 0,
        request: RequestId {
            bundle: Uuid::new_v4().into_bytes(),
            index: 0,
        },
        payload,
    }
}

/// The data of the one row a sorted get found, if it found exactly one
///
/// # Arguments
///
/// * `tables` - The tables to read
async fn sorted_data(tables: &mut Tables) -> Result<Option<String>, String> {
    let key = rows("").0.get_partition_key();
    let get = SortedQuery::Get(SortedGet::<SortedRow> {
        partition_keys: vec![key],
        sort_select: SortSelect::All,
        filters: None,
        limit: None,
        projection: <SortedRow as ShoalTableSupport>::Projection::default(),
    });
    let answered = tables
        .db
        .sorted
        .handle(share_meta(), get, never_sealed_sorted)
        .await;
    match answered {
        Some((
            _,
            _,
            _,
            Answer::Open(Response {
                data: ResponseAction::Get(found),
                ..
            }),
        )) => Ok(found.and_then(|found| found.rows.first().map(|row| row.data.clone()))),
        other => Err(format!("a sorted get answered {}", describe(&other))),
    }
}

/// The data of the one row an unsorted get found, if it found one
///
/// # Arguments
///
/// * `tables` - The tables to read
async fn unsorted_data(tables: &mut Tables) -> Result<Option<String>, String> {
    let key = rows("").1.get_partition_key();
    let get = UnsortedQuery::Get(UnsortedGet::<UnsortedRow> {
        partition_keys: vec![key],
        filters: None,
        limit: None,
        projection: <UnsortedRow as ShoalTableSupport>::Projection::default(),
    });
    let answered = tables
        .db
        .unsorted
        .handle(share_meta(), get, never_sealed_unsorted)
        .await;
    match answered {
        Some((
            _,
            _,
            _,
            Answer::Open(Response {
                data: ResponseAction::Get(found),
                ..
            }),
        )) => Ok(found.and_then(|found| found.rows.first().map(|row| row.data.clone()))),
        other => Err(format!("an unsorted get answered {}", describe(&other))),
    }
}

/// A sorted write whose commit fails is refused, and the table is left as it was
///
/// The row is put in through `apply`. A delete of it and an update of it are then sent the way a
/// client's would be, each commit fails, and each is answered `StorageWrite` with the row still
/// there and still carrying its first data - the delete took the row out before committing, so
/// this is the test that it is put back. An insert of a new row is refused the same way.
#[test]
fn a_sorted_write_a_table_cannot_commit_is_refused_and_changes_nothing() {
    with_tables(
        true,
        false,
        Box::new(|tables| {
            Box::pin(async move {
                // put a row in the way a replica would
                let (row, _) = rows("first");
                let key = row.get_partition_key();
                let insert = SortedQuery::Insert { key, row };
                let (_, payload) = tables
                    .db
                    .sorted
                    .build_intent(&insert)
                    .map_err(|error| format!("failed to build an intent: {error:?}"))?
                    .ok_or("an insert built no intent")?;
                let step = tables.db.sorted.apply(&command(payload), 1, true);
                check!(
                    matches!(step, ApplyStep::Done(_)),
                    "the replicated insert did not apply: {step:?}"
                );
                check!(
                    sorted_data(tables).await? == Some("first".to_owned()),
                    "the applied row was not there"
                );
                // a delete of it commits nothing, so it is refused and the row stays
                let delete = SortedQuery::Delete {
                    key,
                    sort_key: "sort".to_owned(),
                };
                let answered = tables
                    .db
                    .sorted
                    .handle(meta(), delete, never_sealed_sorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "a delete that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    sorted_data(tables).await? == Some("first".to_owned()),
                    "a refused delete removed its row"
                );
                // an update of it commits nothing, so it is refused and the row keeps its data
                let update = HotPathDbQueryKinds::from(SortedRowUpdate {
                    partition_key: PARTITION.to_owned(),
                    sort_key: "sort".to_owned(),
                    data: Some("second".to_owned()),
                });
                let HotPathDbQueryKinds::SortedRow(update) = update else {
                    return Err("a sorted update built another table's query".to_owned());
                };
                let answered = tables
                    .db
                    .sorted
                    .handle(meta(), update, never_sealed_sorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "an update that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    sorted_data(tables).await? == Some("first".to_owned()),
                    "a refused update changed its row"
                );
                // and an insert over it is refused the same way
                let (row, _) = rows("third");
                let answered = tables
                    .db
                    .sorted
                    .handle(
                        meta(),
                        SortedQuery::Insert { key, row },
                        never_sealed_sorted,
                    )
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "an insert that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    sorted_data(tables).await? == Some("first".to_owned()),
                    "a refused insert replaced its row"
                );
                // the refused delete's row is still one an exists can find
                let exists = SortedQuery::Exists(SortedExists::<SortedRow> {
                    partition_keys: vec![key],
                    sort_select: SortSelect::All,
                    filters: None,
                });
                let answered = tables
                    .db
                    .sorted
                    .handle(meta(), exists, never_sealed_sorted)
                    .await;
                check!(
                    matches!(
                        &answered,
                        Some((
                            _,
                            _,
                            _,
                            Answer::Open(Response {
                                data: ResponseAction::Exists(true),
                                ..
                            })
                        ))
                    ),
                    "the row a refused delete put back does not exist: {}",
                    describe(&answered)
                );
                Ok(())
            })
        }),
    );
}

/// An unsorted write whose commit fails is refused, and the table is left as it was
///
/// The unsorted half of the test above.
#[test]
fn an_unsorted_write_a_table_cannot_commit_is_refused_and_changes_nothing() {
    with_tables(
        true,
        false,
        Box::new(|tables| {
            Box::pin(async move {
                // put a row in the way a replica would
                let (_, row) = rows("first");
                let key = row.get_partition_key();
                let insert = UnsortedQuery::Insert { key, row };
                let (_, payload) = tables
                    .db
                    .unsorted
                    .build_intent(&insert)
                    .map_err(|error| format!("failed to build an intent: {error:?}"))?
                    .ok_or("an insert built no intent")?;
                let step = tables.db.unsorted.apply(&command(payload), 1, true);
                check!(
                    matches!(step, ApplyStep::Done(_)),
                    "the replicated insert did not apply: {step:?}"
                );
                check!(
                    unsorted_data(tables).await? == Some("first".to_owned()),
                    "the applied row was not there"
                );
                // a delete of it commits nothing, so it is refused and the row stays
                let answered = tables
                    .db
                    .unsorted
                    .handle(meta(), UnsortedQuery::Delete { key }, never_sealed_unsorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "a delete that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    unsorted_data(tables).await? == Some("first".to_owned()),
                    "a refused delete removed its row"
                );
                // an update of it commits nothing, so it is refused and the row keeps its data
                let update = HotPathDbQueryKinds::from(UnsortedRowUpdate {
                    partition_key: PARTITION.to_owned(),
                    data: Some("second".to_owned()),
                });
                let HotPathDbQueryKinds::UnsortedRow(update) = update else {
                    return Err("an unsorted update built another table's query".to_owned());
                };
                let answered = tables
                    .db
                    .unsorted
                    .handle(meta(), update, never_sealed_unsorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "an update that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    unsorted_data(tables).await? == Some("first".to_owned()),
                    "a refused update changed its row"
                );
                // and an insert over it is refused the same way
                let (_, row) = rows("third");
                let answered = tables
                    .db
                    .unsorted
                    .handle(
                        meta(),
                        UnsortedQuery::Insert { key, row },
                        never_sealed_unsorted,
                    )
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageWrite),
                    "an insert that could not commit answered {}",
                    describe(&answered)
                );
                check!(
                    unsorted_data(tables).await? == Some("first".to_owned()),
                    "a refused insert replaced its row"
                );
                Ok(())
            })
        }),
    );
}

/// A sorted read the loader cannot take is answered, rather than parked or panicked on
///
/// The seeded row is on disk and nowhere else once the tables are rebuilt, so a get of it has
/// to ask the loader for a read. The loader's channel is closed first, so that request fails.
#[test]
fn a_sorted_read_the_loader_cannot_take_is_answered() {
    with_tables(
        false,
        true,
        Box::new(|tables| {
            Box::pin(async move {
                // nothing will ever take a read request again
                tables
                    .loader
                    .1
                    .close()
                    .map_err(|error| format!("{error:?}"))?;
                let key = rows("").0.get_partition_key();
                let get = SortedQuery::Get(SortedGet::<SortedRow> {
                    partition_keys: vec![key],
                    sort_select: SortSelect::All,
                    filters: None,
                    limit: None,
                    projection: <SortedRow as ShoalTableSupport>::Projection::default(),
                });
                let answered = tables
                    .db
                    .sorted
                    .handle(meta(), get, never_sealed_sorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageRead),
                    "a get whose read could not be asked for answered {}",
                    describe(&answered)
                );
                Ok(())
            })
        }),
    );
}

/// An unsorted read the loader cannot take is answered, rather than parked or panicked on
///
/// The unsorted half of the test above.
#[test]
fn an_unsorted_read_the_loader_cannot_take_is_answered() {
    with_tables(
        false,
        true,
        Box::new(|tables| {
            Box::pin(async move {
                // nothing will ever take a read request again
                tables
                    .loader
                    .1
                    .close()
                    .map_err(|error| format!("{error:?}"))?;
                let key = rows("").1.get_partition_key();
                let get = UnsortedQuery::Get(UnsortedGet::<UnsortedRow> {
                    partition_keys: vec![key],
                    filters: None,
                    limit: None,
                    projection: <UnsortedRow as ShoalTableSupport>::Projection::default(),
                });
                let answered = tables
                    .db
                    .unsorted
                    .handle(meta(), get, never_sealed_unsorted)
                    .await;
                check!(
                    error_code(&answered) == Some(ErrorCode::StorageRead),
                    "a get whose read could not be asked for answered {}",
                    describe(&answered)
                );
                Ok(())
            })
        }),
    );
}

/// A message for one shard cannot be copied for a broadcast, and says why rather than panicking
///
/// `ServerMsg` was `Clone`, and its clone panicked on every variant a broadcast never carries, so
/// a routing mistake that broadcast one took the process down. `try_clone` refuses those by name,
/// and a broadcast refuses before any shard is sent anything.
#[test]
fn a_message_for_one_shard_cannot_be_copied_for_a_broadcast() {
    // a failure is asked of one shard, so it has no copy to send another
    let refused = ServerMsg::<HotPathDb>::Fail.try_clone();
    assert!(
        matches!(refused, Err(why) if !why.is_empty()),
        "a message for one shard was copied for a broadcast"
    );
    // and a shutdown is told to every shard, so it copies
    assert!(
        matches!(
            ServerMsg::<HotPathDb>::Shutdown.try_clone(),
            Ok(ServerMsg::Shutdown)
        ),
        "a broadcast message was refused a copy"
    );
}
