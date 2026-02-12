//! Integration tests for persistent sorted tables in Shoal

use std::time::Duration;

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentSortedTable;
use shoal_derive::{ShoalDB, ShoalSortedTable};
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
#[derive(ShoalDB)]
pub struct TestDb {
    /// The sorted test table
    pub test_records: PersistentSortedTable<TestRecord, FileSystem, TestDbTableNames>,
}

/// Test setting up and tearing down a db
#[tokio::test]
async fn setup() {
    // get a new temp dir for this test
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    tokio::time::sleep(Duration::from_secs(3)).await;
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
#[ignore]
#[tokio::test]
async fn delete_survives_restart() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    tokio::time::sleep(Duration::from_secs(3)).await;
    //// restart this server 3 times to make sure intent logs get flushed
    //for _ in 0..3 {
    //    // start a shoal server and build a client
    //    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    //    // send this query
    //    let response = client
    //        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
    //        .await?;
    //    // access our response
    //    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    //    // deserialize our test record
    //    let record = TestRecord::deserialize(access).unwrap();
    //    // make sure this record matches
    //    assert_eq!(test_data, record);
    //    // Shutdown server for the first time
    //    pool.exit()?;
    //    // wait for threads to fully clean up and port to be released
    //    tokio::time::sleep(Duration::from_secs(3)).await;
    //}
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

/// Test updating rows in shoal
#[tokio::test]
async fn update() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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

#[tokio::test]
pub async fn into_query() {
    // start with an example query
    let query = "select * from test_records";
}
