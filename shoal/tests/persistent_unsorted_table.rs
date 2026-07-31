//! Integration tests for persistent unsorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// A simple sorted table for testing
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

impl TestRecord {
    /// Create a new test record
    ///
    /// # Arguments
    ///
    /// * `partition_key`
    /// * `data`
    pub fn new<T: Into<String>>(partition_key: T, data: T) -> Self {
        TestRecord {
            partition_key: partition_key.into(),
            data: data.into(),
        }
    }
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The sorted test table
    pub test_record: PersistentUnsortedTable<TestRecord, FileSystem>,
}

/// Start a server, do nothing, and shut it back down
///
/// Startup replays the intent log into memory and then force compacts it into an
/// archive, so cycling the server twice after a write leaves that partition on
/// disk only: the third session has an empty log and nothing resident. That is
/// the state where updates and deletes have to go to disk to find their row.
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
async fn cycle_server(temp_dir: &TempDir) -> Result<(), TestError> {
    // start a shoal server on our existing data
    let (_client, pool) = utils::start::<TestDb>(temp_dir).await?;
    // shut it right back down
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Insert a row and leave it on disk with nothing resident in memory
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `row` - The row to insert
async fn insert_then_evict_to_disk(temp_dir: &TempDir, row: &TestRecord) -> Result<(), TestError> {
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(temp_dir).await?;
    // insert our row
    client.send_one(row.clone()).await?;
    // shut down so our insert is left in the intent log
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle the server so our insert is compacted out of the log and into an archive
    cycle_server(temp_dir).await
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
    let test_data = TestRecord::new("partition_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
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

/// Test deleting rows from shoal
#[tokio::test]
async fn delete() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "woot");
    // send this query
    client.send_one(test_data.clone()).await?;
    // send this query
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    // access our response
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    // deserialize our test record
    let record = TestRecord::deserialize(access).unwrap();
    // make sure this record matches
    assert_eq!(test_data, record);
    // now delete this record and make sure it was deleted
    client
        .send_one(TestRecordDelete::new("partition_key".into()))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(test_data.partition_key.clone()))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
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
    let test_data = TestRecord::new("partition_key", "original");
    // send this query
    client.send_one(test_data.clone()).await?;
    // verify the record was inserted
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(test_data, record);
    // now update this record
    client
        .send_one(TestRecordUpdate {
            partition_key: "partition_key".into(),
            data: Some("updated".into()),
        })
        .await?;
    // verify the record was updated
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    assert_eq!(record.partition_key, "partition_key");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test deleting a row whose partition is on disk but not in memory
#[tokio::test]
async fn delete_when_not_resident() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "woot");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // delete this record, which has to be found on disk first
    //
    // `send_one` fails the query when the server answers Delete(false), so this
    // is also the assertion that our delete actually deleted something
    client
        .send_one(TestRecordDelete::new("partition_key".into()))
        .await?;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(test_data.partition_key.clone()))
        .await?;
    // make sure this row no longer exists
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // restart this server 3 times so our delete is replayed, then compacted, then
    // forgotten entirely; only a stale archive map entry could bring the row back
    for _ in 0..3 {
        // start a shoal server and build a client
        let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
        // check if this row has been resurrected
        let exists = client
            .exists(TestRecordExists::new(test_data.partition_key.clone()))
            .await?;
        // make sure this row is still deleted
        assert!(!exists);
        // Shutdown server
        pool.exit()?;
        // wait for threads to fully clean up and port to be released
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Test that a deleted row stays deleted when its tombstone is evicted
///
/// An unsorted delete replaces the partition with a tombstone, which is the only
/// thing shadowing the pre delete copy in the archive. It may not be evicted until
/// the log holding its delete intent has been compacted, so this runs the shard
/// under constant memory pressure to drop anything the server marks evictable.
#[tokio::test]
async fn delete_survives_eviction() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "woot");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server that evicts everything it is allowed to evict
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // delete this record, which has to be found on disk first
    client
        .send_one(TestRecordDelete::new("partition_key".into()))
        .await?;
    // give the shard time to evict anything it thinks is durable
    tokio::time::sleep(Duration::from_secs(2)).await;
    // check if this row still exists in shoal
    let exists = client
        .exists(TestRecordExists::new(test_data.partition_key.clone()))
        .await?;
    // make sure evicting the tombstone did not resurrect the row
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test updating a row whose partition is on disk but not in memory
#[tokio::test]
async fn update_when_not_resident() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "original");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // update this record, which has to be found on disk first
    //
    // `send_one` fails the query when the server answers Update(false), so this
    // is also the assertion that our update actually updated something
    client
        .send_one(TestRecordUpdate {
            partition_key: "partition_key".into(),
            data: Some("updated".into()),
        })
        .await?;
    // verify the record was updated
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // make sure our update survived being compacted into an archive
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(record.data, "updated");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a delete's tombstone does not shadow a later insert of the same key
#[tokio::test]
async fn insert_after_delete_when_not_resident() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test partition to insert
    let test_data = TestRecord::new("partition_key", "original");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // delete this record, which has to be found on disk first
    client
        .send_one(TestRecordDelete::new("partition_key".into()))
        .await?;
    // build the replacement for the row we just deleted
    let replacement = TestRecord::new("partition_key", "replacement");
    // insert it over the tombstone our delete left behind
    client.send_one(replacement.clone()).await?;
    // verify our replacement is what we get back
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(replacement, record);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // make sure our replacement survived the delete being compacted
    let response = client
        .send_one(TestRecordGet::new(test_data.partition_key.clone()))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(replacement, record);
    // Shutdown server
    pool.exit()?;
    Ok(())
}
