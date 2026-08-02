//! Integration tests for persistent sorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::queries::SortRange;
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use std::ops::Bound;
use std::path::PathBuf;
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

/// Find this tables intent log directory under a servers storage path
///
/// The layout is `<path>/<table>/intents`, and the table name is generated by a
/// derive, so this searches for the directory instead of hard coding either half.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this servers data lives in
fn intent_dir(temp_dir: &TempDir) -> PathBuf {
    // every table gets its own directory directly under our storage path
    for table in std::fs::read_dir(temp_dir.path()).expect("Failed to read storage path") {
        // build the path this table would keep its intent logs in
        let intents = table.expect("Failed to read table dir").path().join("intents");
        // this is our table if that directory exists
        if intents.is_dir() {
            return intents;
        }
    }
    panic!("No intent log directory under {:?}", temp_dir.path());
}

/// List the inactive intent logs a server left behind, sorted by name
///
/// # Arguments
///
/// * `intents` - The intent log directory to list
fn inactive_logs(intents: &PathBuf) -> Vec<PathBuf> {
    // crawl over every file in this intent directory
    let mut found: Vec<PathBuf> = std::fs::read_dir(intents)
        .expect("Failed to read intent dir")
        .map(|entry| entry.expect("Failed to read intent dir entry").path())
        .filter(|path| {
            // only rotated logs carry the inactive marker in their name
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("-inactive-"))
        })
        .collect();
    // sort these paths so a failure reads the same way every run
    found.sort();
    found
}

/// Start a single shard server, apply some queries to it, and shut it back down
///
/// Every partition key lands on the one shard, so the intent logs this leaves behind
/// are always `Shard-0`'s rather than whichever shard a key happened to hash to.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this servers data lives in
/// * `queries` - The queries to send before shutting back down
async fn single_shard_session(
    temp_dir: &TempDir,
    queries: Vec<TestDbQueryKinds>,
) -> Result<(), TestError> {
    // start a server that keeps every partition on one shard
    let conf = utils::build_single_shard_config(temp_dir);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // apply each of our queries in order
    for query in queries {
        client.send_one(query).await?;
    }
    // shut this server back down
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test that a later intent log does not discard what an earlier one replayed
///
/// Recovery replays each intent log with its own scan pass, and that scan pass loads
/// a partition an update names straight over whatever is already in the partition
/// map. A partition an earlier log replayed into is therefore replaced by its stale
/// archive copy the moment a later log updates it, and every row the earlier log put
/// there is lost.
///
/// The two logs are built by real servers and then arranged by hand, since the only
/// way to leave an inactive log behind is to interrupt a compaction. `donor` supplies
/// the update and `temp_dir` supplies both the archive and the earlier log.
#[tokio::test]
async fn multi_log_recovery_keeps_earlier_intents() -> Result<(), TestError> {
    // get a temp dir for the server under test and one to take an update log from
    let temp_dir = utils::test_dir();
    let donor = utils::test_dir();
    // build the two rows that share a partition
    let first = TestRecord::new("partition_key", "first", "one");
    let second = TestRecord::new("partition_key", "second", "two");
    // write the first row and cycle twice so it is compacted into an archive
    single_shard_session(&temp_dir, vec![first.clone().into()]).await?;
    single_shard_session(&temp_dir, vec![]).await?;
    // insert the second row, which is all this sessions intent log will hold
    single_shard_session(&temp_dir, vec![second.clone().into()]).await?;
    // do the same in our donor dir, but update the first row instead
    single_shard_session(&donor, vec![first.clone().into()]).await?;
    single_shard_session(&donor, vec![]).await?;
    single_shard_session(
        &donor,
        vec![TestRecordUpdate {
            partition_key: "partition_key".into(),
            sort_key: "first".into(),
            data: Some("one-updated".into()),
        }
        .into()],
    )
    .await?;
    // find the intent log directories both servers left behind
    let under_test = intent_dir(&temp_dir);
    let donated = intent_dir(&donor);
    // stage the insert of our second row as the log an interrupted compaction left
    std::fs::rename(
        under_test.join("Shard-0-active"),
        under_test.join("Shard-0-inactive-1"),
    )
    .expect("Failed to stage the inactive intent log");
    // and stage the update as the active log that recovery replays after it
    std::fs::copy(
        donated.join("Shard-0-active"),
        under_test.join("Shard-0-active"),
    )
    .expect("Failed to stage the active intent log");
    // start a server over both logs and read the partition they share back
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_owned()]))
        .await?;
    // deserialize every row recovery left us
    let rows: Vec<TestRecord> = response
        .access::<TestRecord>()?
        .unwrap()
        .iter()
        .map(|access| TestRecord::deserialize(access).unwrap())
        .collect();
    // the active logs update must not have cost us the inactive logs insert
    assert_eq!(
        rows.len(),
        2,
        "Recovery dropped a row an earlier intent log replayed: {rows:?}"
    );
    // the row the active log updated carries its update
    let updated = rows.iter().find(|row| row.sort_key == "first").unwrap();
    assert_eq!(updated.data, "one-updated");
    // and the row only the inactive log knew about is still here untouched
    let survivor = rows.iter().find(|row| row.sort_key == "second").unwrap();
    assert_eq!(survivor.data, "two");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a rotated intent log with nothing in it is deleted by its compaction
///
/// Startup always forces a rotation, so a table that is never written still hands the
/// compactor a zero length log every restart. That log is compacted to nothing, and if
/// its file is only removed on the path that had partitions to write then it outlives
/// the compaction that consumed it — left on disk to be replayed by the next startup,
/// and reading as the interrupted compaction it is not.
#[tokio::test]
async fn empty_rotated_intent_logs_are_deleted() -> Result<(), TestError> {
    // get a temp dir for the server under test
    let temp_dir = utils::test_dir();
    // write one row so this table has an intent log and an archive at all
    single_shard_session(
        &temp_dir,
        vec![TestRecord::new("partition_key", "sort_key", "data").into()],
    )
    .await?;
    // cycle twice more without writing anything, so both rotations are of empty logs
    single_shard_session(&temp_dir, vec![]).await?;
    single_shard_session(&temp_dir, vec![]).await?;
    // find the intent log directory these sessions left behind
    let intents = intent_dir(&temp_dir);
    // every rotated log was compacted by the session that rotated it, so none are left
    let leftovers = inactive_logs(&intents);
    assert!(
        leftovers.is_empty(),
        "A compacted intent log outlived its compaction: {leftovers:?}"
    );
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

/// Insert several rows into one partition
///
/// # Arguments
///
/// * `client` - The client to insert our rows with
/// * `partition_key` - The partition to insert our rows into
/// * `sort_keys` - The sort keys to build a row for
async fn insert_rows(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_key: &str,
    sort_keys: &[&str],
) -> Result<(), TestError> {
    // insert a row for each sort key we were given
    for sort_key in sort_keys {
        client
            .send_one(TestRecord::new(partition_key, sort_key, "woot"))
            .await?;
    }
    Ok(())
}

/// Test that a get returns no more rows than its limit
///
/// This is the headline case of a limit being parsed, sent, and then discarded: the
/// table inlined its own scan loop with no limit check, so this came back with all
/// five rows.
#[tokio::test]
async fn get_stops_at_its_limit() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // get them back with a limit of two
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_string()]).limit(2))
        .await?;
    let rows = response.access::<TestRecord>()?.unwrap();
    // our limit has to be honoured
    assert_eq!(rows.len(), 2);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit of zero returns nothing at all
///
/// A limit of zero is reached before a single row is read, so the get scans nothing
/// and answers `Get(None)`. `send_one` treats that as a failed query, which is the
/// same thing it does for a get that found nothing - the two are not distinguishable
/// until a response can carry a reason.
#[tokio::test]
async fn get_with_a_zero_limit_returns_nothing() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // get them back with a limit of zero
    let result = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_string()]).limit(0))
        .await;
    // a get that asked for nothing gets nothing
    assert!(matches!(
        result,
        Err(shoal_core::client::Errors::QueryDidNotSucceed {
            kind: shoal_core::shared::responses::ResponseActionNames::Get,
            ..
        })
    ));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit is spread across every partition a get names
///
/// The rows a get finds accumulate into one response, so its limit spans all of its
/// partitions rather than resetting at each one. This runs a single shard so that
/// both partitions are answered by the same shard.
#[tokio::test]
async fn get_spreads_its_limit_across_partitions() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a single shard server so both partitions land on one shard
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write three rows into each of two partitions
    insert_rows(&client, "partition_a", &["a", "b", "c"]).await?;
    insert_rows(&client, "partition_b", &["d", "e", "f"]).await?;
    // get from both partitions with a limit of four
    let response = client
        .send_one(
            TestRecordGet::new(vec!["partition_a".to_string(), "partition_b".to_string()]).limit(4),
        )
        .await?;
    let rows = response.access::<TestRecord>()?.unwrap();
    // our limit spans both partitions instead of allowing four from each
    assert_eq!(rows.len(), 4);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit is honoured when the partition has to be read from disk
///
/// A get whose partition is not resident parks itself, is replayed once the read
/// lands, and picks its earlier rows back up out of `pending_data`. This is also the
/// only path that scans an archive in place rather than a loaded partition.
#[tokio::test]
async fn get_stops_at_its_limit_when_loaded_from_disk() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // write five rows and leave them on disk with nothing resident in memory
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so those inserts have been compacted into an archive
    cycle_server(&temp_dir).await?;
    // start a fresh server, which has to read this partition back off disk
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_string()]).limit(2))
        .await?;
    let rows = response.access::<TestRecord>()?.unwrap();
    // our limit has to survive the trip through the blocked query path
    assert_eq!(rows.len(), 2);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit is honoured for a partition that keeps being evicted
///
/// This runs the shard under constant memory pressure so the partition is dropped
/// and read back rather than answered from memory.
#[tokio::test]
async fn get_stops_at_its_limit_under_eviction() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server that evicts everything it is allowed to evict
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // give the shard time to evict anything it thinks is durable
    tokio::time::sleep(Duration::from_secs(2)).await;
    // get them back with a limit of two
    let response = client
        .send_one(TestRecordGet::new(vec!["partition_key".to_string()]).limit(2))
        .await?;
    let rows = response.access::<TestRecord>()?.unwrap();
    // our limit has to be honoured however the partition happens to be held
    assert_eq!(rows.len(), 2);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// How many partitions the cross shard tests spread their rows over
///
/// Enough that a multi shard ring is certain to own some of them on each side, so the
/// get really is answered in pieces rather than by one shard alone.
const SPREAD_PARTITIONS: usize = 20;

/// How many rows each of those partitions holds
const SPREAD_ROWS_PER_PARTITION: usize = 3;

/// Write `SPREAD_ROWS_PER_PARTITION` rows into each of `SPREAD_PARTITIONS` partitions
///
/// Returns the partition keys that were written to.
///
/// # Arguments
///
/// * `client` - The client to insert our rows with
async fn insert_spread_rows(
    client: &shoal_core::client::Shoal<TestDbClient>,
) -> Result<Vec<String>, TestError> {
    // build a partition key per partition we are spreading over
    let partition_keys = (0..SPREAD_PARTITIONS)
        .map(|index| format!("partition_{index}"))
        .collect::<Vec<_>>();
    // write our rows into each of those partitions
    for partition_key in &partition_keys {
        insert_rows(client, partition_key, &["a", "b", "c"]).await?;
    }
    Ok(partition_keys)
}

/// Read a whole get stream and report how many responses and rows it produced
///
/// # Arguments
///
/// * `stream` - The result stream to drain
async fn drain_get(
    stream: &mut shoal_core::client::ShoalResultStream<TestDbClient>,
) -> Result<(usize, usize), TestError> {
    // count the responses and the rows they carried
    let mut responses = 0;
    let mut rows = 0;
    // drain every response this get produced
    while let Some(response) = stream.next().await? {
        // count this response and the rows it carried
        responses += 1;
        if let Some(found) = response.access::<TestRecord>()? {
            rows += found.len();
        }
    }
    Ok((responses, rows))
}

/// Test that a get spanning several shards returns every row exactly once
///
/// A get naming partitions on several shards is split and answered in pieces. The
/// client tracks one response per query index, so without the shard that split it
/// merging those pieces the extra responses are dropped and the client is handed only
/// whichever shard answered first.
#[tokio::test]
async fn get_across_shards_returns_every_row() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // get from every partition at once with no limit
    let mut stream = client
        .send(client.query().add(TestRecordGet::new(partition_keys)))
        .await?;
    let (responses, rows) = drain_get(&mut stream).await?;
    // the client is owed exactly one response per query
    assert_eq!(responses, 1, "A split query was answered more than once");
    // and that response has to carry what every shard found, not just one of them
    assert_eq!(rows, SPREAD_PARTITIONS * SPREAD_ROWS_PER_PARTITION);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit is applied across shards rather than on each of them
///
/// Each shard applies the limit to its own share as it scans, so their union can
/// still be over it. The shard that split the query trims that union back down, which
/// is the only place a limit spanning shards can be enforced. The limit here is
/// deliberately larger than any one shard's share, so a per shard limit cannot reach
/// it.
#[tokio::test]
async fn get_applies_its_limit_across_shards() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // ask for most of our rows, which is more than any single shard holds
    let limit = SPREAD_PARTITIONS * SPREAD_ROWS_PER_PARTITION - 10;
    // get from every partition at once with that limit
    let mut stream = client
        .send(
            client
                .query()
                .add(TestRecordGet::new(partition_keys).limit(limit)),
        )
        .await?;
    let (responses, rows) = drain_get(&mut stream).await?;
    // the client is owed exactly one response per query
    assert_eq!(responses, 1, "A split query was answered more than once");
    // and our limit spans every shard rather than resetting on each of them
    assert_eq!(rows, limit);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Read a whole get stream and report its rows in the order they came back
///
/// # Arguments
///
/// * `stream` - The result stream to drain
async fn drain_row_keys(
    stream: &mut shoal_core::client::ShoalResultStream<TestDbClient>,
) -> Result<Vec<(String, String)>, TestError> {
    // collect the keys of every row this get answered with
    let mut rows = Vec::new();
    // drain every response this get produced
    while let Some(response) = stream.next().await? {
        // pull the keys out of each row this response carried
        if let Some(found) = response.access::<TestRecord>()? {
            for row in found.iter() {
                rows.push((row.partition_key.to_string(), row.sort_key.to_string()));
            }
        }
    }
    Ok(rows)
}

/// Build the rows a get over these partitions should answer with, in order
///
/// Partitions come back in the order the query named them and their rows in sort key order,
/// so the answer is one flattened out of the other.
///
/// # Arguments
///
/// * `partition_keys` - The partitions the query named, in the order it named them
/// * `sort_keys` - The sort keys each of those partitions holds, in sort order
fn expected_row_keys(partition_keys: &[String], sort_keys: &[&str]) -> Vec<(String, String)> {
    partition_keys
        .iter()
        .flat_map(|partition_key| {
            sort_keys
                .iter()
                .map(move |sort_key| (partition_key.clone(), (*sort_key).to_string()))
        })
        .collect()
}

/// Run a get over these partitions and report the rows it answered with, in order
///
/// # Arguments
///
/// * `client` - The client to send our get with
/// * `partition_keys` - The partitions to read, in the order to read them
async fn get_row_keys(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_keys: Vec<String>,
) -> Result<Vec<(String, String)>, TestError> {
    // get from every one of these partitions at once
    let mut stream = client
        .send(client.query().add(TestRecordGet::new(partition_keys)))
        .await?;
    drain_row_keys(&mut stream).await
}

/// Test that a get spanning several shards returns its partitions in the order it named them
///
/// The shares of a split query used to be merged in whichever order the shards answered in, so
/// the same get could come back with its partitions in a different order each time it ran.
#[tokio::test]
async fn get_returns_partitions_in_query_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // read them all back in the order we wrote them
    let rows = get_row_keys(&client, partition_keys.clone()).await?;
    // every partition should appear in the order the query named it, rows in sort order
    assert_eq!(rows, expected_row_keys(&partition_keys, &["a", "b", "c"]));
    // naming the partitions the other way round turns the answer round with it
    let reversed = partition_keys.iter().rev().cloned().collect::<Vec<_>>();
    let rows = get_row_keys(&client, reversed.clone()).await?;
    assert_eq!(rows, expected_row_keys(&reversed, &["a", "b", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that the same get answers with the same rows in the same order every time
///
/// This is the property that makes paging over several partitions possible without pulling the
/// whole result back and sorting it on the client. It only holds once the shard collecting the
/// shares of a split query orders them by something other than which shard replied first, so a
/// single run proves nothing and this runs the same query many times over.
#[tokio::test]
async fn get_partition_order_is_stable_across_repeats() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // take the answer to the first run as the one every other run has to match
    let first = get_row_keys(&client, partition_keys.clone()).await?;
    // run the same query again and again, since the old order depended on a race
    for run in 1..20 {
        let rows = get_row_keys(&client, partition_keys.clone()).await?;
        assert_eq!(rows, first, "run {run} answered in a different order");
    }
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit takes the rows of the partitions a get named first
///
/// A limit is only meaningful once the order is, so this pins which rows are kept and not just
/// how many of them there are.
#[tokio::test]
async fn get_limit_takes_the_first_partitions() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // ask for fewer rows than the first partition holds
    let mut stream = client
        .send(
            client
                .query()
                .add(TestRecordGet::new(partition_keys.clone()).limit(2)),
        )
        .await?;
    let rows = drain_row_keys(&mut stream).await?;
    // those rows have to come from the partition we named first
    assert_eq!(
        rows,
        expected_row_keys(&partition_keys[..1], &["a", "b"]),
        "a limit kept rows from a partition named later"
    );
    // ask for enough rows to spill into the partition we named second
    let mut stream = client
        .send(
            client
                .query()
                .add(TestRecordGet::new(partition_keys.clone()).limit(4)),
        )
        .await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the spill has to land on the second partition and pick up its first row
    let mut expected = expected_row_keys(&partition_keys[..1], &["a", "b", "c"]);
    expected.push((partition_keys[1].clone(), "a".to_string()));
    assert_eq!(rows, expected);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Write several rows and leave them on disk with nothing resident in memory
///
/// This runs a single shard throughout, since a partition is stored under the shard that owns
/// it and changing the shard count between runs would move where it lives.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this servers data lives in
/// * `rows` - The rows to write
async fn insert_rows_then_evict_to_disk(
    temp_dir: &TempDir,
    rows: &[TestRecord],
) -> Result<(), TestError> {
    // start a single shard server and write our rows to it
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(temp_dir)).await?;
    for row in rows {
        client.send_one(row.clone()).await?;
    }
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so these inserts have been compacted into an archive
    let (_client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(temp_dir)).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test that a partition read back from disk still answers in the place the query named it
///
/// A partition that has to be read from disk is replayed long after the ones already resident,
/// so its rows used to be appended after theirs however the query was written.
#[tokio::test]
async fn get_partition_order_survives_disk_loads() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave one partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write a second partition, which is resident and can answer without any read
    insert_rows(&client, "partition_resident", &["a", "b", "c"]).await?;
    // name the partition that has to be read from disk first
    let names = vec![
        "partition_on_disk".to_string(),
        "partition_resident".to_string(),
    ];
    let rows = get_row_keys(&client, names.clone()).await?;
    // its rows have to come first even though it answered last
    assert_eq!(rows, expected_row_keys(&names, &["a", "b", "c"]));
    // and naming it second puts its rows second
    let reversed = names.iter().rev().cloned().collect::<Vec<_>>();
    let rows = get_row_keys(&client, reversed.clone()).await?;
    assert_eq!(rows, expected_row_keys(&reversed, &["a", "b", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit keeps the rows of a partition still being read from disk
///
/// A get used to stop as soon as the rows it had already found filled its limit, and then drop
/// the partitions it was still waiting on. With the partition it named first on disk and the
/// one it named second in memory, that answered entirely out of the second one.
#[tokio::test]
async fn get_limit_prefers_a_blocked_partition() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave one partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write a second partition, which is resident and can answer without any read
    insert_rows(&client, "partition_resident", &["a", "b", "c"]).await?;
    // ask for fewer rows than the partition on disk holds, naming it first
    let mut stream = client
        .send(
            client.query().add(
                TestRecordGet::new(vec![
                    "partition_on_disk".to_string(),
                    "partition_resident".to_string(),
                ])
                .limit(2),
            ),
        )
        .await?;
    let rows = drain_row_keys(&mut stream).await?;
    // every row has to come from the partition we named first, not the one that was quicker
    assert_eq!(
        rows,
        vec![
            ("partition_on_disk".to_string(), "a".to_string()),
            ("partition_on_disk".to_string(), "b".to_string()),
        ]
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test the reported query end to end, from a SHQL string to ordered rows
///
/// This is the case the whole change came from, with `TestRecord` standing in for
/// `MovieByKeyword`: a partition per keyword, sorted by title. `keyword = 'a' AND keyword = 'b'`
/// used to read as an intersection, answer as a union, and return the union in whichever order
/// the shards replied in.
#[tokio::test]
async fn shql_in_reads_every_partition_in_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a few titles under each of two keywords
    insert_rows(&client, "alien", &["Alien", "Aliens", "Prometheus"]).await?;
    insert_rows(&client, "giant worm", &["Dune", "Tremors"]).await?;
    // read both keywords with the spelling that says what it means
    let query = client
        .query()
        .parse("SELECT * FROM TestRecord WHERE partition_key IN ('giant worm', 'alien') LIMIT 3")
        .expect("failed to parse an IN query");
    let mut stream = client.send(query).await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the limit takes the first three rows of the keyword named first, in title order
    assert_eq!(
        rows,
        vec![
            ("giant worm".to_string(), "Dune".to_string()),
            ("giant worm".to_string(), "Tremors".to_string()),
            ("alien".to_string(), "Alien".to_string()),
        ]
    );
    // naming the keywords the other way round answers out of the other one
    let query = client
        .query()
        .parse("SELECT * FROM TestRecord WHERE partition_key IN ('alien', 'giant worm') LIMIT 3")
        .expect("failed to parse an IN query");
    let mut stream = client.send(query).await?;
    let rows = drain_row_keys(&mut stream).await?;
    assert_eq!(
        rows,
        vec![
            ("alien".to_string(), "Alien".to_string()),
            ("alien".to_string(), "Aliens".to_string()),
            ("alien".to_string(), "Prometheus".to_string()),
        ]
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that the spelling that used to lie is now refused
///
/// `keyword = 'a' AND keyword = 'b'` reads as "rows carrying both" and was answered as "rows
/// carrying either". It is a parse error now, and the error says how to ask for either.
#[tokio::test]
async fn shql_rejects_a_partition_key_constrained_twice() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // the query from the original report never reaches the server
    //
    // `Queries` has no Debug impl, so the error is pulled out by hand rather than with
    // `expect_err`
    let result = client.query().parse(
        "SELECT * FROM TestRecord WHERE partition_key = 'giant worm' \
         AND partition_key = 'alien' LIMIT 2",
    );
    let Err(error) = result else {
        panic!("expected the AND spelling to be refused");
    };
    assert!(
        error.message.contains("'partition_key' is constrained twice by AND"),
        "unexpected message: {}",
        error.message
    );
    // and it names the IN list that was meant, quoting the literals as they were written
    assert!(
        error
            .message
            .contains("partition_key IN ('giant worm', 'alien')"),
        "unexpected message: {}",
        error.message
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Run a get naming some sort keys and report the rows it answered with, in order
///
/// # Arguments
///
/// * `client` - The client to send our get with
/// * `partition_keys` - The partitions to read, in the order to read them
/// * `sort_keys` - The sort keys to select within each of those partitions
async fn get_row_keys_by_sort_key(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_keys: Vec<String>,
    sort_keys: &[&str],
) -> Result<Vec<(String, String)>, TestError> {
    // build a get naming both the partitions and the rows we want out of them
    let get = TestRecordGet::new(partition_keys)
        .sort_keys(sort_keys.iter().map(|key| (*key).to_string()).collect());
    // read every one of those partitions at once
    let mut stream = client.send(client.query().add(get)).await?;
    drain_row_keys(&mut stream).await
}

/// Test that a get naming a sort key returns that row alone
///
/// This is the headline case of a sort key being parsed, sent, and then discarded: the table
/// scanned every live row in the partition, so this came back with all five rows.
#[tokio::test]
async fn get_selects_a_named_sort_key() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // ask for one of them by sort key
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["c"]).await?;
    // only the row we named comes back
    assert_eq!(rows, expected_row_keys(&names, &["c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a get naming several sort keys returns each of their rows in sort order
///
/// The keys are named in reverse, since a sort key list is a set and not an order - the rows
/// still come back in the order the partition holds them.
#[tokio::test]
async fn get_selects_several_sort_keys_in_sort_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // ask for three of them, naming them in the opposite order to the one they sort in
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["d", "a", "b"]).await?;
    // every named row comes back, in sort order rather than the order they were asked for
    assert_eq!(rows, expected_row_keys(&names, &["a", "b", "d"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a get naming a sort key no row carries finds nothing
#[tokio::test]
async fn get_by_sort_key_misses_return_nothing() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    // ask for a row this partition does not hold
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys_by_sort_key(&client, names, &["z"]).await?;
    // a miss is a miss, not the whole partition
    assert!(rows.is_empty(), "a missing sort key answered with {rows:?}");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a sort key selects rows out of a partition that has to be read from disk
///
/// The archived copy of a partition is scanned in place rather than deserialized, so it is a
/// second selection path with its own lookup and its own filter.
#[tokio::test]
async fn get_by_sort_key_reads_from_disk() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave a partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c", "d"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // ask for one of the rows that only exists in an archive
    let names = vec!["partition_on_disk".to_string()];
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["c"]).await?;
    // only the row we named comes back
    assert_eq!(rows, expected_row_keys(&names, &["c"]));
    // and a row that partition never held is still a miss
    let rows = get_row_keys_by_sort_key(&client, names, &["z"]).await?;
    assert!(rows.is_empty(), "a missing sort key answered with {rows:?}");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a sort key selection spans the rows in memory and the rows in an archive
///
/// A named key found in memory says nothing about the other named keys, which may only exist
/// on disk, so a get may never resolve early on the strength of its sort keys. This is the
/// test that pins that: one of the two rows asked for is resident and the other is not.
#[tokio::test]
async fn get_by_sort_key_spans_memory_and_disk() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave two rows of one partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_key", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write a row of that same partition, which is resident and needs no read
    insert_rows(&client, "partition_key", &["b"]).await?;
    // ask for the resident row and one that only exists in the archive
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["b", "c"]).await?;
    // both come back, in sort order
    assert_eq!(rows, expected_row_keys(&names, &["b", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit bounds a sort key selection spanning partitions
#[tokio::test]
async fn get_by_sort_key_stops_at_its_limit() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a single shard server so both partitions are answered by one shard
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write the same three sort keys into each of two partitions
    insert_rows(&client, "first", &["a", "b", "c"]).await?;
    insert_rows(&client, "second", &["a", "b", "c"]).await?;
    // name two rows in each partition but only allow three back
    let get = TestRecordGet::new(vec!["first".to_string(), "second".to_string()])
        .sort_keys(vec!["a".to_string(), "b".to_string()])
        .limit(3);
    let mut stream = client.send(client.query().add(get)).await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the limit takes the first rows of the partition named first
    assert_eq!(
        rows,
        vec![
            ("first".to_string(), "a".to_string()),
            ("first".to_string(), "b".to_string()),
            ("second".to_string(), "a".to_string()),
        ]
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an exists naming a sort key answers for that row and not for its partition
///
/// This is the sharper half of the defect. The old exists answered true on the first live row
/// it walked, so a partition holding any row at all answered true for every sort key, and a
/// caller could not tell "this row is here" from "something is here".
#[tokio::test]
async fn exists_by_sort_key_is_false_for_a_missing_row() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    // a row this partition holds exists
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_key".to_string()])
                .sort_keys(vec!["b".to_string()]),
        )
        .await?;
    assert!(exists, "a row that was written did not exist");
    // a row it does not hold does not, even though the partition is not empty
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_key".to_string()])
                .sort_keys(vec!["z".to_string()]),
        )
        .await?;
    assert!(!exists, "a row that was never written exists");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an exists naming a sort key still consults disk before answering false
#[tokio::test]
async fn exists_by_sort_key_survives_a_disk_load() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave a partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // a row that only exists in an archive still exists
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_on_disk".to_string()])
                .sort_keys(vec!["b".to_string()]),
        )
        .await?;
    assert!(exists, "a row read back from disk did not exist");
    // and a row that partition never held does not
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_on_disk".to_string()])
                .sort_keys(vec!["z".to_string()]),
        )
        .await?;
    assert!(!exists, "a row that was never written exists");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Run a get bounding its rows by a range and report the rows it answered with, in order
///
/// # Arguments
///
/// * `client` - The client to send our get with
/// * `partition_keys` - The partitions to read, in the order to read them
/// * `range` - The range of sort keys to select within each of those partitions
async fn get_row_keys_by_range(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_keys: Vec<String>,
    range: SortRange<String>,
) -> Result<Vec<(String, String)>, TestError> {
    // build a get naming both the partitions and the span of rows we want out of them
    let get = TestRecordGet::new(partition_keys).sort_range(range);
    // read every one of those partitions at once
    let mut stream = client.send(client.query().add(get)).await?;
    drain_row_keys(&mut stream).await
}

/// Build a range over a pair of borrowed sort keys
///
/// # Arguments
///
/// * `start` - The lower bound of the range to build
/// * `end` - The upper bound of the range to build
fn range_of(start: Bound<&str>, end: Bound<&str>) -> SortRange<String> {
    // owning the bound values is what a real query carries over the wire
    let owned = |bound: Bound<&str>| match bound {
        Bound::Included(key) => Bound::Included(key.to_string()),
        Bound::Excluded(key) => Bound::Excluded(key.to_string()),
        Bound::Unbounded => Bound::Unbounded,
    };
    SortRange::new(owned(start), owned(end))
}

/// Test that a range selects the rows between its bounds and no others
#[tokio::test]
async fn get_by_range_selects_its_rows() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // ask for the span between two of them
    let names = vec!["partition_key".to_string()];
    let range = range_of(Bound::Included("b"), Bound::Included("d"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    // the rows inside the range come back in sort order, and nothing outside it does
    assert_eq!(rows, expected_row_keys(&names, &["b", "c", "d"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that each end of a range decides for itself whether it keeps the row it names
#[tokio::test]
async fn get_by_range_honours_each_bound() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write four rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d"]).await?;
    let names = vec!["partition_key".to_string()];
    // an excluded lower bound leaves out the row it names
    let range = range_of(Bound::Excluded("a"), Bound::Excluded("d"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert_eq!(rows, expected_row_keys(&names, &["b", "c"]));
    // and an included one keeps it
    let range = range_of(Bound::Included("a"), Bound::Included("d"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert_eq!(rows, expected_row_keys(&names, &["a", "b", "c", "d"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a range selects rows out of a partition that has to be read from disk
///
/// This is the only cover for the archived range. The archived copy of a partition is walked
/// in place with `ArchivedBTreeMap::range`, which is a wholly separate seek to the in memory
/// one and cannot be reached without a real server.
#[tokio::test]
async fn get_by_range_reads_from_disk() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave a partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // ask for a span of the rows that only exist in an archive
    let names = vec!["partition_on_disk".to_string()];
    let range = range_of(Bound::Excluded("a"), Bound::Included("c"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert_eq!(rows, expected_row_keys(&names, &["b", "c"]));
    // a range past every row that partition holds is still a miss
    let range = range_of(Bound::Excluded("e"), Bound::Unbounded);
    let rows = get_row_keys_by_range(&client, names, range).await?;
    assert!(rows.is_empty(), "a range past the end answered with {rows:?}");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a range spans the rows in memory and the rows in an archive
///
/// A range that matches nothing resident says nothing about what an archive holds, so a get
/// may never resolve early on the strength of its bounds. This is the range twin of the sort
/// key test that pins the same rule.
#[tokio::test]
async fn get_by_range_spans_memory_and_disk() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave two rows of one partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_key", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write a row of that same partition, which is resident and needs no read
    insert_rows(&client, "partition_key", &["b"]).await?;
    // ask for a range covering the resident row and both archived ones
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys_by_range(&client, names.clone(), SortRange::default()).await?;
    // all three come back, in sort order, whichever copy each of them came from
    assert_eq!(rows, expected_row_keys(&names, &["a", "b", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a range that cannot contain a key answers with nothing rather than panicking
///
/// `BTreeMap::range` panics on an inverted range and on one whose ends meet on a key neither
/// includes, so without the guard in front of the seek this takes the shard down.
#[tokio::test]
async fn get_by_an_empty_range_returns_nothing() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    let names = vec!["partition_key".to_string()];
    // a range running backwards holds nothing
    let range = range_of(Bound::Included("c"), Bound::Included("a"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert!(rows.is_empty(), "an inverted range answered with {rows:?}");
    // and neither does one whose ends meet on a key neither of them includes
    let range = range_of(Bound::Excluded("b"), Bound::Excluded("b"));
    let rows = get_row_keys_by_range(&client, names, range).await?;
    assert!(rows.is_empty(), "an empty range answered with {rows:?}");
    // the server is still answering, which is the half of this that matters
    let exists = client
        .exists(TestRecordExists::new(vec!["partition_key".to_string()]))
        .await?;
    assert!(exists, "the shard stopped answering after an empty range");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit bounds a range spanning partitions
///
/// The walk has to stop at the limit rather than at the upper bound, which is what makes a
/// page cost a page rather than a partition.
#[tokio::test]
async fn get_by_range_stops_at_its_limit() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a single shard server so both partitions are answered by one shard
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // write the same three sort keys into each of two partitions
    insert_rows(&client, "first", &["a", "b", "c"]).await?;
    insert_rows(&client, "second", &["a", "b", "c"]).await?;
    // ask for an unbounded range of both partitions but only allow three rows back
    let get = TestRecordGet::new(vec!["first".to_string(), "second".to_string()])
        .sort_range(SortRange::default())
        .limit(3);
    let mut stream = client.send(client.query().add(get)).await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the limit takes the first rows of the partition named first
    assert_eq!(
        rows,
        vec![
            ("first".to_string(), "a".to_string()),
            ("first".to_string(), "b".to_string()),
            ("first".to_string(), "c".to_string()),
        ]
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a partition can be paged through by feeding the last row back as a cursor
///
/// **This is the point of the whole feature.** Before it, the only way to read the tail of a
/// large partition was to fetch all of it: a `LIMIT` always answered with the first rows and
/// there was no way to ask for the next ones. Each page here costs a seek plus its own rows,
/// on whatever page it is.
#[tokio::test]
async fn a_partition_can_be_paged_by_its_sort_key() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write seven rows into one partition, which pages of two do not divide evenly
    let written = ["a", "b", "c", "d", "e", "f", "g"];
    insert_rows(&client, "partition_key", &written).await?;
    // walk the partition two rows at a time, starting from the beginning
    let names = vec!["partition_key".to_string()];
    let mut cursor: Option<String> = None;
    let mut paged: Vec<String> = Vec::new();
    loop {
        // the page after the last row we saw, or the first page if we have not seen one
        let range = match &cursor {
            Some(last) => SortRange::after(last.clone()),
            None => SortRange::default(),
        };
        // read this page
        let get = TestRecordGet::new(names.clone())
            .sort_range(range)
            .limit(2);
        let mut stream = client.send(client.query().add(get)).await?;
        let page = drain_row_keys(&mut stream).await?;
        // a page with nothing in it is the end of the partition
        if page.is_empty() {
            break;
        }
        // the last row of this page is the cursor onto the next one
        cursor = Some(page[page.len() - 1].1.clone());
        paged.extend(page.into_iter().map(|(_, sort_key)| sort_key));
        // a partition of seven rows cannot take more than seven pages, so bail rather than spin
        assert!(paged.len() <= written.len(), "paging did not terminate");
    }
    // every row came back exactly once, in sort order, with no gap and no repeat
    let expected: Vec<String> = written.iter().map(|key| (*key).to_string()).collect();
    assert_eq!(paged, expected);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an exists over a range answers for the rows inside it
#[tokio::test]
async fn exists_by_range_answers_for_its_rows() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    // a range holding one of our rows exists
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_key".to_string()])
                .sort_range(range_of(Bound::Included("b"), Bound::Included("b"))),
        )
        .await?;
    assert!(exists, "a range holding a row did not exist");
    // a range past every row we hold does not, even though the partition is not empty
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_key".to_string()])
                .sort_range(range_of(Bound::Excluded("c"), Bound::Unbounded)),
        )
        .await?;
    assert!(!exists, "a range holding no row exists");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an exists over a range still consults disk before answering false
#[tokio::test]
async fn exists_by_range_survives_a_disk_load() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // leave a partition on disk with nothing resident in memory
    let on_disk: Vec<TestRecord> = ["a", "b", "c"]
        .iter()
        .map(|sort_key| TestRecord::new("partition_on_disk", sort_key, "woot"))
        .collect();
    insert_rows_then_evict_to_disk(&temp_dir, &on_disk).await?;
    // start a fresh server over that same storage
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_single_shard_config(&temp_dir)).await?;
    // a range whose rows only exist in an archive still exists
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_on_disk".to_string()])
                .sort_range(range_of(Bound::Included("b"), Bound::Included("b"))),
        )
        .await?;
    assert!(exists, "a range read back from disk did not exist");
    // and a range that partition never held does not
    let exists = client
        .exists(
            TestRecordExists::new(vec!["partition_on_disk".to_string()])
                .sort_range(range_of(Bound::Excluded("c"), Bound::Unbounded)),
        )
        .await?;
    assert!(!exists, "a range holding no row exists");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a range typed as SHQL reaches the table and narrows the rows it answers with
#[tokio::test]
async fn shql_bounds_rows_by_a_sort_key_range() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // read a page of it the way a human would type it
    //
    // `Queries` has no Debug impl, so the parse result is unwrapped by hand rather than with
    // `expect`
    let Ok(queries) = client.query().parse(
        "SELECT * FROM TestRecord WHERE partition_key = 'partition_key' \
         AND sort_key > 'b' LIMIT 2",
    ) else {
        panic!("a sort key range should parse and bind");
    };
    let mut stream = client.send(queries).await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the rows after the bound come back, stopped by the limit
    let names = vec!["partition_key".to_string()];
    assert_eq!(rows, expected_row_keys(&names, &["c", "d"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}
