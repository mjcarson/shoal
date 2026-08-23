//! Integration tests for how many records share one intent log write
//!
//! The intent log stages records into an aligned DMA buffer and flushes it when the next
//! record will not fit. How many records that buffer holds is what the group commit below
//! it has to amortize an fdatasync across, and it is the whole of
//! [O34](../../docs/src/appendix/optimizations.md): a table whose rows exceed the staging
//! buffer used to get one DMA write and one DMA allocation per insert.
//!
//! These tests read the property off disk rather than out of the writer. Every flush that
//! is not already block aligned leaves a pad sentinel behind, so the number of sentinels in
//! a log is the number of partial flushes that built it — and with a batch of inserts
//! arriving in one frame, that is a direct count of how many records shared a write.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::conf::Conf;
use shoal::shared::queries::Queries;
use shoal::storage::FileSystem;
use shoal::traits::PartitionKeySupport;
use shoal::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// The width of the payload every row in these tests carries
///
/// Twice the staging buffer below, so a record can never fit in a buffer of the configured
/// size and the writer has to have grown one to batch anything at all.
const ROW_BYTES: usize = 8 << 10;

/// The staging buffer floor these tests configure
///
/// The value the committed `shoal.yml` uses, so these tests measure the shipped configuration
/// rather than one invented for them.
const BUFFER_FLOOR: usize = 4096;

/// How many rows one bundle inserts
///
/// They go over the wire in a single frame, which is what puts them in the shard's queue
/// together — a shard flushes its tables whenever that queue drains, so records sent one at a
/// time can never share a write however large the buffer is.
const ROWS: usize = 128;

/// The sentinel a partial flush leaves at the start of its pad region
///
/// Spelled out rather than imported, since the constant lives inside `shoal-core`'s storage
/// engine and this is a test of what reaches the disk.
const PAD_SENTINEL: u64 = u64::MAX;

/// A table whose rows are wider than the intent log's staging buffer
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct WideRecord {
    /// The partition key this row is stored under
    #[shoal(partition)]
    pub partition_key: String,
    /// A payload wide enough that a record cannot fit in the configured buffer
    #[shoal(update)]
    pub data: String,
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The wide row table
    pub wide_record: PersistentUnsortedTable<WideRecord, FileSystem>,
}

/// Build a single shard config with a staging buffer narrower than a row
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
fn wide_row_config(temp_dir: &TempDir) -> Conf {
    // one shard, so every row lands in one intent log we can then read
    let mut conf = utils::build_single_shard_config(temp_dir);
    // pin the staging buffer to the shipped floor, which is half of one of our rows
    conf.storage.default.filesystem.latency_sensitive.buffer_size = BUFFER_FLOOR;
    conf
}

/// Insert one bundle of wide rows and return the intent log they landed in
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `conf` - The config to start this server with
async fn write_wide_rows(temp_dir: &TempDir, conf: Conf) -> Result<Vec<u8>, TestError> {
    // start a server on this config and build a client for it
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // build one bundle holding every row, so they reach the shard together
    let mut queries = Queries::<TestDbClient>::default();
    for index in 0..ROWS {
        queries.add_mut(WideRecord {
            partition_key: format!("partition-{index}"),
            data: "a".repeat(ROW_BYTES),
        });
    }
    // send them and drain every response so we know they all committed
    let mut stream = client.send(queries).await?;
    while stream.next().await?.is_some() {}
    // shut down, which writes out whatever is still staged and fdatasyncs it
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // read back the one intent log this shard wrote
    let path = only_intent_log(temp_dir);
    Ok(std::fs::read(&path)?)
}

/// Find the single intent log a one shard server left behind
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
fn only_intent_log(temp_dir: &TempDir) -> PathBuf {
    // the intent logs live under <path>/<table>/intents/<shard>
    let mut dir = temp_dir.path().to_path_buf();
    dir.push(WideRecord::name());
    dir.push("intents");
    // there is one shard, so there is one log
    let mut logs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error:?}", dir.display()))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect();
    logs.sort();
    assert_eq!(logs.len(), 1, "expected one intent log, found {logs:?}");
    logs.remove(0)
}

/// Count the partial flushes that built a log
///
/// Every flush that was not already block aligned starts its pad region with a sentinel, so
/// this is how many writes the records in this log were spread across.
///
/// # Arguments
///
/// * `log` - The raw intent log bytes
fn pad_regions(log: &[u8]) -> usize {
    // a sentinel is always written at an eight byte boundary, since it follows framed records
    log.chunks_exact(8)
        .filter(|chunk| u64::from_le_bytes((*chunk).try_into().unwrap()) == PAD_SENTINEL)
        .count()
}

/// Build the same config with the buffer sizing turned off
///
/// The ceiling pinned to the floor, which is the one field this differs from
/// [`wide_row_config`] in. Every buffer is then the floor, or one record when a record is wider
/// than the floor, which is exactly what the writer did before it sized itself.
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
fn pinned_buffer_config(temp_dir: &TempDir) -> Conf {
    // start from the config the test above uses
    let mut conf = wide_row_config(temp_dir);
    // refuse to grow past the floor
    conf.storage
        .default
        .filesystem
        .latency_sensitive
        .max_buffer_size = BUFFER_FLOOR;
    conf
}

/// A bundle of records wider than the buffer still share aligned writes
///
/// This is [O34](../../docs/src/appendix/optimizations.md). Before the staging buffer sized
/// itself, a record wider than `buffer_size` got a buffer of its own, so a bundle of 128 rows
/// became 128 writes and 128 DMA allocations with nothing left for the group commit to group.
#[tokio::test(flavor = "multi_thread")]
async fn wide_records_share_an_aligned_write() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // write our rows through a buffer half a row wide
    let log = write_wide_rows(&temp_dir, wide_row_config(&temp_dir)).await?;
    // every row has to actually be in this log, or a flush count of zero means nothing
    assert!(
        log.len() >= ROWS * ROW_BYTES,
        "the log is {} bytes, which cannot hold {ROWS} rows",
        log.len()
    );
    // count how many writes those rows were spread across
    let flushes = pad_regions(&log);
    // a buffer sized to batch should hold several records per write rather than one
    //
    // a flush that happens to land on a block boundary leaves no pad region behind, so this
    // counts at most the partial flushes - which is the direction that makes it a safe bound
    assert!(
        flushes <= ROWS / 4,
        "{ROWS} rows were written in at least {flushes} flushes, which is one record per write"
    );
    Ok(())
}

/// Pinning the ceiling to the floor gives back one write per record
///
/// The control for the test above, differing from it in one configuration field and nothing
/// else. It is what makes that test a measurement of the sizing rather than of anything else
/// that happens to batch, and it is the escape hatch an operator with a memory budget has.
#[tokio::test(flavor = "multi_thread")]
async fn a_ceiling_at_the_floor_batches_nothing() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // write the same rows through a buffer that may not grow
    let log = write_wide_rows(&temp_dir, pinned_buffer_config(&temp_dir)).await?;
    // count how many writes those rows were spread across
    let flushes = pad_regions(&log);
    // every record should have had a write to itself, since none of them fit beside another
    assert_eq!(
        flushes, ROWS,
        "a pinned buffer batched {ROWS} rows into {flushes} writes"
    );
    Ok(())
}
