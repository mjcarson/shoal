//! Integration tests for persistent sorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// A simple sorted table for testing
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key - orders records within a partition (must be String for RkyvSupport)
    #[shoal(sort)]
    pub sort_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

impl TestRecord {
    /// Create a new test record
    ///
    /// # Arguments
    ///
    /// * `partition_key`
    /// * `sort_key`
    /// * `data`
    pub fn new<T: Into<String>>(partition_key: T, sort_key: T, data: T) -> Self {
        TestRecord {
            partition_key: partition_key.into(),
            sort_key: sort_key.into(),
            data: data.into(),
        }
    }
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The sorted test table
    pub test_records: PersistentSortedTable<TestRecord, FileSystem>,
}

/// Test setting up and tearing down a db
#[tokio::test]
async fn setup() {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (_client, pool) = utils::start::<TestDb>(&temp_dir)
        .await
        .expect("Failed to setup shoal server/client");
    // Shutdown server
    pool.exit().expect("Failed to shutdown server");
}

/// Test inserting and getting rows from shoal
#[tokio::test]
async fn insert() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test inserting and exists queries work in shoal
#[tokio::test]
async fn exists_true() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure this row no longer exists
    assert!(exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test inserting and exists queries work in shoal
#[tokio::test]
async fn exists_false() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec!["partition_key".to_owned()]))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test deleting rows from shoal
#[tokio::test]
async fn delete() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // now delete this record and make sure it was deleted
    client
        .send_one(TestRecordDelete::new(
            "partition_key".into(),
            "sort_key".into(),
        ))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test deleting rows from shoal
#[tokio::test]
async fn delete_after_restart() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // Shutdown server for the first time
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a shoal server for the last time and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // now delete this record and make sure it was deleted
    client
        .send_one(TestRecordDelete::new(
            "partition_key".into(),
            "sort_key".into(),
        ))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test deleting rows from shoal
#[tokio::test]
async fn delete_survives_restart() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // Shutdown server for the first time
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // restart this server 3 times to make sure intent logs get flushed
    for _ in 0..3 {
        // start a shoal server and build a client
        let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
        // send this query
        let response = client
            .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
            .await?;
        // access our response
        let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
        // deserialize our test record
        let record = TestRecord::deserialize(access).unwrap();
        // make sure this record matches
        assert_eq!(test_data, record);
        // Shutdown server for the first time
        pool.exit()?;
        // wait for threads to fully clean up and port to be released
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // start a shoal server for the last time and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // now delete this record and make sure it was deleted
    client
        .send_one(TestRecordDelete::new(
            "partition_key".into(),
            "sort_key".into(),
        ))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Start a server, do nothing, and shut it back down
///
/// Startup replays the intent log into memory and then force compacts it into an
/// archive, so cycling the server twice after a write leaves that partition on
/// disk only: the third session has an empty log and nothing resident.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this servers data lives in
async fn cycle_server(temp_dir: &TempDir) -> Result<(), TestError> {
    // start and immediately stop a server
    let (_client, pool) = utils::start::<TestDb>(temp_dir).await?;
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Insert a row and leave it on disk with nothing resident in memory
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this servers data lives in
/// * `row` - The row to insert
async fn insert_then_evict_to_disk(temp_dir: &TempDir, row: &TestRecord) -> Result<(), TestError> {
    // start a server and write our row to it
    let (client, pool) = utils::start::<TestDb>(temp_dir).await?;
    client.send_one(row.clone()).await?;
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so this rows insert has been compacted into an archive
    cycle_server(temp_dir).await
}

/// Test deleting a row whose partition is on disk but not in memory
#[tokio::test]
async fn delete_when_not_resident() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test row to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // delete this record, which has to be found on disk first
    //
    // `send_one` fails the query when the server answers Delete(false), so this
    // is also the assertion that our delete actually deleted something
    client
        .send_one(TestRecordDelete::new(
            "partition_key".into(),
            "sort_key".into(),
        ))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // restart this server 3 times so our delete is replayed, then compacted, then
    // forgotten entirely; only a stale archive map entry could bring the row back
    for _ in 0..3 {
        // start a shoal server and build a client
        let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
        // check if this row still exists in shoal
        let exists = client
            .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
            .await?;
        // make sure this row no longer exists
        assert!(!exists);
        // Shutdown server
        pool.exit()?;
        // wait for threads to fully clean up and the port to be released
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Test that a deleted row stays deleted when its tombstone is evicted
///
/// The tombstone is the only thing shadowing the pre delete copy in the archive, so
/// it may not be evicted until the log holding its delete intent has been compacted.
/// This runs the shard under constant memory pressure so that any partition the
/// server marks evictable is dropped on the very next loop iteration.
#[tokio::test]
async fn delete_survives_eviction() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test row to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server that evicts everything it is allowed to evict
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // delete this record, which has to be found on disk first
    client
        .send_one(TestRecordDelete::new(
            "partition_key".into(),
            "sort_key".into(),
        ))
        .await?;
    // give the shard time to evict anything it thinks is durable
    tokio::time::sleep(Duration::from_secs(2)).await;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(vec![test_data.partition_key.clone()]))
        .await?;
    // make sure evicting the tombstone did not resurrect the row
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test that rows written into a resident partition survive eviction
///
/// A sorted partition is mutated in place, so its generation has to be refreshed on
/// every write. If it is not, a partition first loaded generations ago is marked
/// evictable by a compaction that never saw its newest rows, and those rows vanish
/// from reads until the log holding them is compacted.
#[tokio::test]
async fn writes_survive_eviction() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server that evicts everything it is allowed to evict
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // write enough rows into one partition to rotate its intent log several times
    for i in 0..200 {
        // build a row with a unique sort key
        let row = TestRecord::new(
            "partition_key".to_owned(),
            format!("sort-{i:03}"),
            "woot".to_owned(),
        );
        // insert this row
        client.send_one(row).await?;
    }
    // give the shard time to evict anything it thinks is durable
    tokio::time::sleep(Duration::from_secs(2)).await;
    // read this partition back
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_owned()]))
        .await?;
    // access our response
    let rows = response.access::<TestRecord>()?.unwrap();
    // every row we wrote has to still be readable
    assert_eq!(rows.len(), 200);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test updating rows in shoal
#[tokio::test]
async fn update() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "original");
    // send this query
    client.send_one(test_data.clone()).await?;
    // verify the record was inserted
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(test_data, record);
    // now update this record
    client
        .send_one(TestRecordUpdate {
            partition_key: "partition_key".into(),
            sort_key: "sort_key".into(),
            data: Some("updated".into()),
        })
        .await?;
    // verify the record was updated
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    assert_eq!(record.partition_key, "partition_key");
    assert_eq!(record.sort_key, "sort_key");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test update rows from shoal after a restart
#[tokio::test]
async fn update_intent_replay() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "sort_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // Shutdown server for the first time
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a shoal server for the last time and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // now update this record
    client
        .send_one(TestRecordUpdate {
            partition_key: "partition_key".into(),
            sort_key: "sort_key".into(),
            data: Some("updated".into()),
        })
        .await?;
    // verify the record was updated
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    assert_eq!(record.partition_key, "partition_key");
    assert_eq!(record.sort_key, "sort_key");
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a shoal server for the last time and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // verify the record was updated
    let response = client
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    assert_eq!(record.partition_key, "partition_key");
    assert_eq!(record.sort_key, "sort_key");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// The number of rows the crash test writes, enough to span many intent log buffers
const CRASH_ROWS: usize = 200;

/// The child half of [`ack_survives_sigkill`]
///
/// Starts a server, inserts a row, waits for the acknowledgement, announces itself
/// and then hangs so its parent can kill it without a clean shutdown. Ignored by
/// default since it never returns on its own.
#[tokio::test]
#[ignore]
async fn ack_survives_sigkill_child() -> Result<(), TestError> {
    // get the dir and port our parent picked for us
    let dir = std::env::var(utils::CRASH_DIR_VAR).expect("crash test dir not set");
    let port: u16 = std::env::var(utils::CRASH_PORT_VAR)
        .expect("crash test port not set")
        .parse()
        .expect("crash test port not a number");
    // start a shoal server on our parents dir and build a client
    let conf = utils::build_crash_config(std::path::Path::new(&dir), port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // insert enough rows to span many intent log buffers, each with its own pad
    // region, and wait for every one of them to be acknowledged
    for index in 0..CRASH_ROWS {
        let row = TestRecord::new("partition_key", &format!("sort_key_{index:04}"), "survives a kill");
        client.send_one(row).await?;
    }
    // tell our parent our write has been acknowledged so it can kill us
    println!("{}", utils::CRASH_READY_LINE);
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    // hang until our parent kills us, so we never shut down cleanly
    std::future::pending::<()>().await;
    unreachable!()
}

/// Acknowledged writes replay after a SIGKILL with no clean shutdown
///
/// Every other restart test in this file calls `pool.exit()` first, which drains,
/// fdatasyncs and closes the intent log on the way out. This one kills the server
/// outright, so replay has to work from whatever the writer had already put on
/// disk, across many buffers and the pad regions between them.
///
/// **What this does not prove.** It is not a durability test and it does not catch
/// a premature acknowledgement. Two reasons, both verified rather than assumed:
///
/// - SIGKILL kills the process, not the kernel, so writes the kernel has already
///   accepted survive whether or not they were fdatasynced. Only a power loss
///   distinguishes `Durability::Fsync` from `Durability::Async`.
/// - The shard flushes as soon as its queue drains (`shard.rs:651-653`), which is
///   microseconds after an insert is acknowledged, while delivering the kill takes
///   milliseconds. Reverting `commit` to its old hardcoded `Ok(0)` leaves this test
///   passing.
///
/// The guarantee that acknowledgement implies durability rests on the watermark
/// unit tests in `shoal-core` plus the construction of the write path, not on this.
#[tokio::test]
async fn ack_survives_sigkill() -> Result<(), TestError> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    // get a new temp dir and port for this test
    let temp_dir = utils::test_dir();
    let port = 13900;
    // re-run this test binary as a child running only the child half
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ack_survives_sigkill_child", "--ignored", "--nocapture"])
        .env(utils::CRASH_DIR_VAR, temp_dir.path())
        .env(utils::CRASH_PORT_VAR, port.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn crash test child");
    // wait for our child to tell us its write has been acknowledged
    let stdout = child.stdout.take().unwrap();
    let mut ready = false;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("Failed to read child output");
        println!("child: {line}");
        if line.trim() == utils::CRASH_READY_LINE {
            ready = true;
            break;
        }
    }
    // kill our child outright so it never gets to shut down cleanly
    child.kill().expect("Failed to kill crash test child");
    child.wait().expect("Failed to reap crash test child");
    assert!(ready, "Child never acknowledged its write");
    // wait for the port to be released
    tokio::time::sleep(Duration::from_secs(2)).await;
    // start a fresh server over the same storage and look for our row
    let conf = utils::build_crash_config(temp_dir.path(), port + 1);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_string()]))
        .await?;
    let rows = response.access::<TestRecord>()?.unwrap();
    // every acknowledged row must be replayable after an abrupt death
    assert_eq!(
        rows.len(),
        CRASH_ROWS,
        "Acknowledged rows were lost to a SIGKILL"
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}
