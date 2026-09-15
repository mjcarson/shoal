//! Integration tests for the identity of a Shoal storage directory
//!
//! A partition is owned by the shard its tablet is assigned to, and that assignment comes
//! from the shard count. ~~These tests hold the server to refusing a directory reopened under a
//! different count rather than starting and quietly finding nothing.~~ Since
//! [F47](../../docs/src/features/local-rehome.md) a changed count is a rehome: the files a
//! vanished executor left are moved onto the live ones before a shard starts, and these tests
//! hold the server to finding every row afterwards, at every count it is moved between.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// A simple unsorted table for testing
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The unsorted test table
    pub test_record: PersistentUnsortedTable<TestRecord, FileSystem>,
}

/// How many rows the rehome tests write, enough to land on every tablet's executor
const ROWS: usize = 400;

/// The partition key of a row
///
/// # Arguments
///
/// * `at` - The row
fn key(at: usize) -> String {
    format!("partition-{at}")
}

/// Start a server on this dir with a set shard count, then shut it back down
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `cores` - The number of cores to run this server with
async fn cycle_server(temp_dir: &TempDir, cores: usize) -> Result<(), TestError> {
    // build a config pinned to this dir
    let mut conf = utils::build_config(temp_dir);
    // run it with the shard count we were asked for
    conf.resources.cores = Some(cores);
    // start a shoal server on our existing data
    let (_client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // shut it right back down
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Start a server on this dir with a set core count, read every row back, and shut it down
///
/// Returns the report of the rehome the start ran, if it ran one.
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `cores` - The number of cores to run this server with
/// * `write` - Whether to write the rows first, on the first start
async fn cycle_and_read(
    temp_dir: &TempDir,
    cores: usize,
    write: bool,
) -> Result<Option<shoal::server::RehomeReport>, TestError> {
    // build a config pinned to this dir at this count
    let mut conf = utils::build_config(temp_dir);
    conf.resources.cores = Some(cores);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // the rows, once
    if write {
        for at in 0..ROWS {
            client
                .send_one(TestRecord {
                    partition_key: key(at),
                    data: format!("data-{at}"),
                })
                .await?;
        }
    }
    // every row reads back, whatever count it was written under
    for at in 0..ROWS {
        let found = client.send_one(TestRecordGet::new(vec![key(at)])).await?;
        let rows = found.access::<TestRecord>()?;
        let rows = rows.unwrap_or_else(|| panic!("row {at} did not read back at {cores} cores"));
        assert_eq!(
            rows.len(),
            1,
            "row {at} read back {} rows at {cores} cores",
            rows.len()
        );
        assert_eq!(
            rows[0].data,
            format!("data-{at}"),
            "row {at} read back wrong at {cores} cores"
        );
    }
    // what the start did to the files, and the hosting it runs under
    let report = pool.rehome().cloned();
    assert_eq!(pool.hosting().physical, cores);
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(report)
}

/// A storage dir reopened by the shard count that wrote it has to start
#[tokio::test(flavor = "multi_thread")]
async fn the_same_shard_count_restarts() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // claim it with some number of shards
    cycle_server(&temp_dir, 2).await?;
    // reopening it with that same count is the ordinary restart
    cycle_server(&temp_dir, 2).await?;
    Ok(())
}

/// A storage dir reopened by a different core count is rehomed, and every row reads back
///
/// ~~A storage dir reopened by a different shard count has to be refused.~~ Written at two
/// cores, the directory is reopened at three - a growth, where two live donors deal tablets to
/// a new executor - and then at one, where two executors vanish and everything they held lands
/// on the survivor. Every row reads back at every count, the start that moved files says so
/// in its report, and one that did not says nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_core_count_rehomes_and_reads_back() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // written and read at two cores, which is no rehome at all
    let report = cycle_and_read(&temp_dir, 2, true).await?;
    assert!(
        report.is_none(),
        "a fresh directory ran a rehome: {report:?}"
    );
    // grown to three: two donors deal a third of their tablets to the new executor
    let report = cycle_and_read(&temp_dir, 3, false)
        .await?
        .expect("a growth ran no rehome");
    assert_eq!((report.from, report.to), (2, 3));
    assert!(
        report.tablets_moved > 0,
        "a growth moved no tablets: {report:?}"
    );
    assert!(report.records > 0, "a growth copied no records: {report:?}");
    assert_eq!(report.steps_redone, 0);
    // the same count again is the ordinary restart
    let report = cycle_and_read(&temp_dir, 3, false).await?;
    assert!(
        report.is_none(),
        "a settled directory ran a rehome: {report:?}"
    );
    // shrunk to one: two executors vanish and the survivor holds every row
    let report = cycle_and_read(&temp_dir, 1, false)
        .await?
        .expect("a shrink ran no rehome");
    assert_eq!((report.from, report.to), (3, 1));
    assert!(report.tablets_moved > 0);
    assert!(report.records > 0, "a shrink copied no records: {report:?}");
    // and only the survivor's files remain
    let conf = utils::build_config(&temp_dir);
    assert_eq!(
        shoal::server::rehome::executors_with_files(&conf, &["TestRecord"], 4),
        vec![0],
        "a vanished executor's files were not reclaimed"
    );
    Ok(())
}
