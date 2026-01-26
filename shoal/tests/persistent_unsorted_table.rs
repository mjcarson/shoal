//! Integration tests for persistent sorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_core::{client::QuerySuceededOpts, storage::FileSystem};
use shoal_derive::{ShoalDB, ShoalUnsortedTable};
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
#[derive(ShoalDB)]
pub struct TestDb {
    /// The sorted test table
    pub test_record: PersistentUnsortedTable<TestRecord, FileSystem, TestDbTableNames>,
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
    // build a query to insert some rows into this shoal db
    let insert_query = client.query().add(test_data.clone());
    // send this query
    let mut insert_stream = client.send(insert_query).await?;
    // get the response for this insert stream
    let insert_response = insert_stream
        .next()
        .await?
        .expect("Failed to get response for insert query");
    // check if this response succeeded
    insert_response.suceeded(QuerySuceededOpts::default())?;
    // make sure this data was inserted
    let get_query = client
        .query()
        .add(TestRecordGet::new(test_data.partition_key.clone()));
    // send this query
    let mut result_stream = client.send(get_query).await?;
    // get the first item
    let response = result_stream
        .next()
        .await?
        .expect("Failed to get response for get query");
    // check if this response succeeded
    response.suceeded(QuerySuceededOpts::default())?;
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
