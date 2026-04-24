//! Integration tests for persistent unsorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_derive::{shoal_db, ShoalUnsortedTable};
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
#[shoal_db]
pub struct TestDb {
    /// The sorted test table
    pub test_record: PersistentUnsortedTable<TestRecord, FileSystem>,
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
