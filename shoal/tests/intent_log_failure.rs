//! Integration tests for a table whose intent log fails a write or an fdatasync
//!
//! A write to a table's intent log runs on a detached task, and a device error it hits is recorded
//! for the next sweep to find. That sweep returned it, and the shard loop's `?` ended the shard,
//! and every client it was serving with it
//! ([item 122](../../docs/src/appendix/resolved/intent-log-failure.md)). A failed log now answers
//! the writes it cannot vouch for as outcomes it does not know, refuses every write after them
//! before a byte is staged, keeps serving reads, and replays what was durable at the next start.
//!
//! A device cannot be made to fail on demand, so the tests ask the writer to: the next write or
//! fdatasync reports `EIO` instead of doing its IO, from the same background task and through the
//! same state a real failure is recorded in. The tables are built the way a shard builds them, on
//! a glommio executor of their own, and swept the way the shard sweeps them.
//!
//! See `docs/src/appendix/resolved/intent-log-failure.md`.

use deepsize2::DeepSizeOf;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::glommio::{Latency, LocalExecutorBuilder, Shares};
use shoal::gxhash::GxHasher;
use shoal::kanal::{self, AsyncReceiver, AsyncSender};
use shoal::lru::LruCache;
use shoal::server::conf::Conf;
use shoal::server::messages::{Answer, QueryMetadata, ServerMsg};
use shoal::server::shard::ShardContact;
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::queries::{SortSelect, SortedGet, SortedQuery, UnsortedGet, UnsortedQuery};
use shoal::shared::responses::{Response, ResponseAction};
use shoal::shared::row_ref::RowRef;
use shoal::shared::traits::{PartitionKeySupport, ShoalTableSupport};
use shoal::storage::{FileSystem, FullArchiveMap, LogFault};
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
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
#[shoal_table(db = "LogFailureDb")]
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
#[shoal_table(db = "LogFailureDb")]
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
pub struct LogFailureDb {
    /// The sorted table
    pub sorted: PersistentSortedTable<SortedRow, FileSystem>,
    /// The unsorted table
    pub unsorted: PersistentUnsortedTable<UnsortedRow, FileSystem>,
}

/// The shard a single shard server names its one executor
const SHARD_NAME: &str = "Shard-0";

/// The sort key every sorted row is written under
const SORT_KEY: &str = "sort";

/// How many harnesses have been opened, for naming their executors
static OPENED: AtomicUsize = AtomicUsize::new(0);

/// Fail a test body with a message rather than panicking
///
/// A panic on a glommio executor thread aborts the whole binary while it unwinds, so the bodies
/// here report a failure as an error and `with_conf` panics with it on the test's own thread.
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
type Lru = LruCache<(LogFailureDbTableNames, u64), usize, BuildHasherDefault<GxHasher>>;

/// What a table answers a query with, if it answers it at once
type Answered<P> = Option<(
    Uuid,
    Uuid,
    shoal::server::stage_profile::StageStamps,
    Answer<Response<P>>,
)>;

/// A shard's tables, built the way a shard builds them, with the channel a shard would drain
struct Tables {
    /// The tables themselves
    db: LogFailureDb,
    /// The channel the writers and the compactors wake the shard on
    shard: (
        AsyncSender<ServerMsg<LogFailureDb>>,
        AsyncReceiver<ServerMsg<LogFailureDb>>,
    ),
}

impl Tables {
    /// Build a shard's tables over a directory, replaying whatever is already in it
    ///
    /// # Arguments
    ///
    /// * `conf` - The config the tables are built with
    async fn open(conf: &Conf) -> Result<Tables, String> {
        // the queue a shard's loader and compactors run on
        let medium = shoal::glommio::executor().create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(100)),
            "MediumPriority:intent_log_failure",
        );
        // everything a shard hands its tables when it builds them
        let table_map = FullArchiveMap::default();
        let mut loader_channels = HashMap::with_capacity(1);
        let memory = Arc::new(RefCell::new(0));
        let lru: Arc<RefCell<Lru>> = Arc::new(RefCell::new(LruCache::unbounded_with_hasher(
            BuildHasherDefault::<GxHasher>::default(),
        )));
        let shard = kanal::unbounded_async();
        // build the tables, which replays any log already on disk
        let db = <LogFailureDb as ShoalDatabase>::new(
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
        Ok(Tables { db, shard })
    }

    /// Wait for a writer to wake the shard, for at most ten seconds
    async fn woken(&self) -> Outcome {
        // glommio's own timeout, since this runs on no tokio runtime
        shoal::glommio::timer::timeout(Duration::from_secs(10), async {
            Ok(self.shard.1.recv().await)
        })
        .await
        .map_err(|_| "timed out waiting for a writer to wake the shard".to_owned())?
        .map_err(|error| format!("the shard's channel closed: {error:?}"))?;
        Ok(())
    }

    /// Sweep the unsorted table the way the shard does until it releases a response
    ///
    /// A sweep that fails is what used to end the shard, so it fails the test here.
    async fn settle_unsorted(&mut self) -> Result<Vec<ResponseAction<UnsortedRow>>, String> {
        loop {
            // hand the log's staged tail to the kernel, which is what the shard does when idle
            self.db
                .unsorted
                .flush()
                .await
                .map_err(|error| format!("the flush failed: {error:?}"))?;
            // release whatever is now durable, or answered
            let released = self
                .db
                .unsorted
                .get_flushed()
                .await
                .map_err(|error| format!("the sweep failed, which ends the shard: {error:?}"))?
                .drain(..)
                .map(|(_, _, _, _, response)| response.data)
                .collect::<Vec<_>>();
            if !released.is_empty() {
                return Ok(released);
            }
            // nothing yet, so wait for the next write or sync to land
            self.woken().await?;
        }
    }

    /// Sweep the sorted table the way the shard does until it releases a response
    ///
    /// A sweep that fails is what used to end the shard, so it fails the test here.
    async fn settle_sorted(&mut self) -> Result<Vec<ResponseAction<SortedRow>>, String> {
        loop {
            // hand the log's staged tail to the kernel, which is what the shard does when idle
            self.db
                .sorted
                .flush()
                .await
                .map_err(|error| format!("the flush failed: {error:?}"))?;
            // release whatever is now durable, or answered
            let released = self
                .db
                .sorted
                .get_flushed()
                .await
                .map_err(|error| format!("the sweep failed, which ends the shard: {error:?}"))?
                .drain(..)
                .map(|(_, _, _, _, response)| response.data)
                .collect::<Vec<_>>();
            if !released.is_empty() {
                return Ok(released);
            }
            // nothing yet, so wait for the next write or sync to land
            self.woken().await?;
        }
    }
}

/// Every file under a directory whose name marks it as a rotated intent log
///
/// # Arguments
///
/// * `dir` - The directory to search
fn rotated_logs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    // walk the whole tree, since the intent logs sit a few directories down
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().contains("-inactive-"))
            {
                found.push(path);
            }
        }
    }
    found
}

/// A test body, handed the config its tables are built with
type Body =
    Box<dyn FnOnce(Conf) -> std::pin::Pin<Box<dyn std::future::Future<Output = Outcome>>> + Send>;

/// Run a test body on an executor of its own, over a directory of its own
///
/// # Arguments
///
/// * `body` - The test, handed the config to build its tables with
fn with_conf(body: Body) {
    // a directory on a real filesystem, and a single shard so everything lands on it
    let temp_dir: TempDir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let name = format!(
        "intent_log_failure-{}",
        OPENED.fetch_add(1, Ordering::SeqCst)
    );
    let outcome = LocalExecutorBuilder::default()
        .name(&name)
        .spawn(move || async move {
            let outcome = body(conf).await;
            // keep the directory until the tables are done with it
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

/// The code a table answered with at once, if it answered with a failure
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

/// The code a released response carries, if it is a failure
///
/// # Arguments
///
/// * `action` - What the response said
fn released_code<T>(action: &ResponseAction<T>) -> Option<ErrorCode> {
    match action {
        ResponseAction::Error(error) => Some(error.code()),
        _ => None,
    }
}

/// An unsorted row in a partition of its own
///
/// # Arguments
///
/// * `partition` - The partition, which is also the row's data
fn unsorted_row(partition: &str) -> UnsortedRow {
    UnsortedRow {
        partition_key: partition.to_owned(),
        data: partition.to_owned(),
    }
}

/// A sorted row in a partition of its own
///
/// # Arguments
///
/// * `partition` - The partition, which is also the row's data
fn sorted_row(partition: &str) -> SortedRow {
    SortedRow {
        partition_key: partition.to_owned(),
        sort_key: SORT_KEY.to_owned(),
        data: partition.to_owned(),
    }
}

/// Insert an unsorted row the way a client's insert arrives
///
/// # Arguments
///
/// * `tables` - The tables to write
/// * `partition` - The partition to write, which is also its data
async fn insert_unsorted(tables: &mut Tables, partition: &str) -> Answered<UnsortedRow> {
    let row = unsorted_row(partition);
    let key = row.get_partition_key();
    tables
        .db
        .unsorted
        .handle(
            meta(),
            UnsortedQuery::Insert { key, row },
            never_sealed_unsorted,
        )
        .await
}

/// Insert a sorted row the way a client's insert arrives
///
/// # Arguments
///
/// * `tables` - The tables to write
/// * `partition` - The partition to write, which is also its data
async fn insert_sorted(tables: &mut Tables, partition: &str) -> Answered<SortedRow> {
    let row = sorted_row(partition);
    let key = row.get_partition_key();
    tables
        .db
        .sorted
        .handle(
            meta(),
            SortedQuery::Insert { key, row },
            never_sealed_sorted,
        )
        .await
}

/// The data of an unsorted partition's row, if a get finds one
///
/// # Arguments
///
/// * `tables` - The tables to read
/// * `partition` - The partition to read
async fn unsorted_data(tables: &mut Tables, partition: &str) -> Result<Option<String>, String> {
    let key = unsorted_row(partition).get_partition_key();
    let get = UnsortedQuery::Get(UnsortedGet::<UnsortedRow> {
        partition_keys: vec![key],
        filters: None,
        limit: None,
        projection: <UnsortedRow as ShoalTableSupport>::Projection::default(),
    });
    match tables
        .db
        .unsorted
        .handle(share_meta(), get, never_sealed_unsorted)
        .await
    {
        Some((
            _,
            _,
            _,
            Answer::Open(Response {
                data: ResponseAction::Get(found),
                ..
            }),
        )) => Ok(found.and_then(|found| found.rows.first().map(|row| row.data.clone()))),
        Some((_, _, _, Answer::Open(response))) => {
            Err(format!("an unsorted get answered {:?}", response.data))
        }
        Some((_, _, _, Answer::Sealed(_))) => Err("a share was answered sealed".to_owned()),
        None => Err("an unsorted get of a resident partition parked".to_owned()),
    }
}

/// The data of a sorted partition's row, if a get finds one
///
/// # Arguments
///
/// * `tables` - The tables to read
/// * `partition` - The partition to read
async fn sorted_data(tables: &mut Tables, partition: &str) -> Result<Option<String>, String> {
    let key = sorted_row(partition).get_partition_key();
    let get = SortedQuery::Get(SortedGet::<SortedRow> {
        partition_keys: vec![key],
        sort_select: SortSelect::All,
        filters: None,
        limit: None,
        projection: <SortedRow as ShoalTableSupport>::Projection::default(),
    });
    match tables
        .db
        .sorted
        .handle(share_meta(), get, never_sealed_sorted)
        .await
    {
        Some((
            _,
            _,
            _,
            Answer::Open(Response {
                data: ResponseAction::Get(found),
                ..
            }),
        )) => Ok(found.and_then(|found| found.rows.first().map(|row| row.data.clone()))),
        Some((_, _, _, Answer::Open(response))) => {
            Err(format!("a sorted get answered {:?}", response.data))
        }
        Some((_, _, _, Answer::Sealed(_))) => Err("a share was answered sealed".to_owned()),
        None => Err("a sorted get of a resident partition parked".to_owned()),
    }
}

/// An unsorted table whose log fails a write answers the writes it cannot vouch for, and carries on
///
/// The first insert is made durable and released. A write of the log then fails, and the insert
/// staged in it is answered `OutcomeUnknown` by the sweep rather than the sweep failing and
/// ending the shard. Every write after that is refused `StorageWrite` at once, the durable row is
/// still read, and the table never asks to rotate the log again.
#[test]
fn an_unsorted_log_that_fails_a_write_answers_what_it_cannot_know() {
    with_conf(Box::new(|conf| {
        Box::pin(async move {
            let mut tables = Tables::open(&conf).await?;
            // a write that is made durable, and answered as it was
            check!(
                insert_unsorted(&mut tables, "durable").await.is_none(),
                "an insert answered before it was durable"
            );
            let released = tables.settle_unsorted().await?;
            check!(
                released.len() == 1 && released_code(&released[0]).is_none(),
                "the durable insert was answered {released:?}"
            );
            // the next write of the log fails, and the insert staged in it with it
            tables.db.unsorted.inject_log_fault(LogFault::Write);
            check!(
                insert_unsorted(&mut tables, "lost").await.is_none(),
                "an insert answered before it was durable"
            );
            // the sweep answers it with what it cannot know, rather than failing
            let released = tables.settle_unsorted().await?;
            check!(
                released.len() == 1
                    && released_code(&released[0]) == Some(ErrorCode::OutcomeUnknown),
                "the insert behind the failed write was answered {released:?}"
            );
            // every write after that is refused before it is staged
            let answered = insert_unsorted(&mut tables, "after").await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "an insert to a failed log was not refused"
            );
            let update = LogFailureDbQueryKinds::from(UnsortedRowUpdate {
                partition_key: "durable".to_owned(),
                data: Some("changed".to_owned()),
            });
            let LogFailureDbQueryKinds::UnsortedRow(update) = update else {
                return Err("an unsorted update built another table's query".to_owned());
            };
            let answered = tables
                .db
                .unsorted
                .handle(meta(), update, never_sealed_unsorted)
                .await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "an update to a failed log was not refused"
            );
            let key = unsorted_row("durable").get_partition_key();
            let answered = tables
                .db
                .unsorted
                .handle(meta(), UnsortedQuery::Delete { key }, never_sealed_unsorted)
                .await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "a delete to a failed log was not refused"
            );
            // the durable row is still served, unchanged by the refused update and delete
            check!(
                unsorted_data(&mut tables, "durable").await? == Some("durable".to_owned()),
                "the durable row was not read back as it was written"
            );
            // and the log is never rotated or swept into failing again
            check!(
                !tables.db.unsorted.compaction_due(),
                "a failed log asked to be rotated"
            );
            let released = tables
                .db
                .unsorted
                .get_flushed()
                .await
                .map_err(|error| format!("a later sweep failed: {error:?}"))?
                .len();
            check!(released == 0, "a later sweep released {released} responses");
            Ok(())
        })
    }));
}

/// A sorted table whose log fails an fdatasync answers the writes it cannot vouch for, and carries on
///
/// The sorted twin of the write test, failing the other IO: the write lands and the fdatasync
/// that would have made it durable fails. No later sync may vouch for it, so the insert is
/// answered `OutcomeUnknown` and every write after it refused.
#[test]
fn a_sorted_log_that_fails_an_fdatasync_answers_what_it_cannot_know() {
    with_conf(Box::new(|conf| {
        Box::pin(async move {
            let mut tables = Tables::open(&conf).await?;
            // a write that is made durable, and answered as it was
            check!(
                insert_sorted(&mut tables, "durable").await.is_none(),
                "an insert answered before it was durable"
            );
            let released = tables.settle_sorted().await?;
            check!(
                released.len() == 1 && released_code(&released[0]).is_none(),
                "the durable insert was answered {released:?}"
            );
            // the next fdatasync of the log fails, after the insert staged behind it was written
            tables.db.sorted.inject_log_fault(LogFault::Sync);
            check!(
                insert_sorted(&mut tables, "lost").await.is_none(),
                "an insert answered before it was durable"
            );
            // the sweep answers it with what it cannot know, rather than failing
            let released = tables.settle_sorted().await?;
            check!(
                released.len() == 1
                    && released_code(&released[0]) == Some(ErrorCode::OutcomeUnknown),
                "the insert behind the failed fdatasync was answered {released:?}"
            );
            // every write after that is refused before it is staged
            let answered = insert_sorted(&mut tables, "after").await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "an insert to a failed log was not refused"
            );
            let update = LogFailureDbQueryKinds::from(SortedRowUpdate {
                partition_key: "durable".to_owned(),
                sort_key: SORT_KEY.to_owned(),
                data: Some("changed".to_owned()),
            });
            let LogFailureDbQueryKinds::SortedRow(update) = update else {
                return Err("a sorted update built another table's query".to_owned());
            };
            let answered = tables
                .db
                .sorted
                .handle(meta(), update, never_sealed_sorted)
                .await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "an update to a failed log was not refused"
            );
            // the durable row is still served, unchanged by the refused update
            check!(
                sorted_data(&mut tables, "durable").await? == Some("durable".to_owned()),
                "the durable row was not read back as it was written"
            );
            // and the log is never rotated or swept into failing again
            check!(
                !tables.db.sorted.compaction_due(),
                "a failed log asked to be rotated"
            );
            Ok(())
        })
    }));
}

/// A table whose log failed shuts down cleanly, and its next start replays the durable prefix
///
/// The failed write never reached the file, so the row behind it is not there after the restart:
/// its client was told it could not know, and this is the outcome it could not know. The row made
/// durable before the failure is.
#[test]
fn a_failed_log_shuts_down_and_replays_its_durable_prefix() {
    with_conf(Box::new(|conf| {
        Box::pin(async move {
            let mut tables = Tables::open(&conf).await?;
            // one durable write, then one behind a write of the log that fails
            check!(
                insert_unsorted(&mut tables, "durable").await.is_none(),
                "an insert answered before it was durable"
            );
            tables.settle_unsorted().await?;
            tables.db.unsorted.inject_log_fault(LogFault::Write);
            check!(
                insert_unsorted(&mut tables, "lost").await.is_none(),
                "an insert answered before it was durable"
            );
            let released = tables.settle_unsorted().await?;
            check!(
                released.len() == 1
                    && released_code(&released[0]) == Some(ErrorCode::OutcomeUnknown),
                "the insert behind the failed write was answered {released:?}"
            );
            // a shard shutting down closes the failed log without syncing it
            let Tables { db, shard } = tables;
            db.shutdown().await.map_err(|error| {
                format!("a table with a failed log did not shut down: {error:?}")
            })?;
            drop(shard);
            // the next start replays what was durable, and nothing past it
            let mut tables = Tables::open(&conf).await?;
            check!(
                unsorted_data(&mut tables, "durable").await? == Some("durable".to_owned()),
                "the durable row was not replayed"
            );
            check!(
                unsorted_data(&mut tables, "lost").await?.is_none(),
                "a row whose write never reached the log was replayed"
            );
            // and the log takes writes again
            check!(
                insert_unsorted(&mut tables, "again").await.is_none(),
                "the restarted log did not take a write"
            );
            let released = tables.settle_unsorted().await?;
            check!(
                released.len() == 1 && released_code(&released[0]).is_none(),
                "the restarted log's first write was answered {released:?}"
            );
            let Tables { db, .. } = tables;
            db.shutdown()
                .await
                .map_err(|error| format!("the restarted tables did not shut down: {error:?}"))?;
            Ok(())
        })
    }));
}

/// A rotation whose fdatasync fails fails the log where it is, and renames nothing
///
/// A rotation syncs the log before it renames it and asks for its compaction. That sync failing
/// used to return from the sweep like any other failure; it is now the same failed log, with the
/// write behind it answered as unknown and no rotated log left behind for a compactor or a
/// restart to find in two places.
#[test]
fn a_rotation_whose_fdatasync_fails_fails_the_log_where_it_is() {
    with_conf(Box::new(|mut conf| {
        Box::pin(async move {
            // any byte accepted takes the log past its size, so every sweep rotates it
            conf.storage
                .default
                .filesystem
                .latency_sensitive
                .intent_log_size = 0;
            let root = conf
                .storage
                .default
                .filesystem
                .latency_sensitive
                .path
                .clone();
            let mut tables = Tables::open(&conf).await?;
            // the startup's own rotation left nothing behind
            let before = rotated_logs(&root);
            // a write staged in the log, whose rotation's fdatasync then fails
            check!(
                insert_unsorted(&mut tables, "lost").await.is_none(),
                "an insert answered before it was durable"
            );
            tables.db.unsorted.inject_log_fault(LogFault::Sync);
            // the sweep is due to rotate, and answers the write rather than failing
            let released = tables
                .db
                .unsorted
                .get_flushed()
                .await
                .map_err(|error| format!("the sweep failed, which ends the shard: {error:?}"))?
                .drain(..)
                .map(|(_, _, _, _, response)| response.data)
                .collect::<Vec<_>>();
            check!(
                released.len() == 1
                    && released_code(&released[0]) == Some(ErrorCode::OutcomeUnknown),
                "the insert behind the failed rotation was answered {released:?}"
            );
            // the log was not renamed, and is never due to rotate again
            let after = rotated_logs(&root);
            check!(
                after == before,
                "a failed rotation left a rotated log behind: {after:?}"
            );
            check!(
                !tables.db.unsorted.compaction_due(),
                "a failed log asked to be rotated"
            );
            // and every write after it is refused
            let answered = insert_unsorted(&mut tables, "after").await;
            check!(
                error_code(&answered) == Some(ErrorCode::StorageWrite),
                "an insert to a failed log was not refused"
            );
            let Tables { db, .. } = tables;
            db.shutdown().await.map_err(|error| {
                format!("a table with a failed log did not shut down: {error:?}")
            })?;
            Ok(())
        })
    }));
}
