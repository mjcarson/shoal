//! Integration tests for persistent unsorted tables in Shoal

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalProjection, ShoalUnsortedTable};
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

/// A projection of a test record that leaves its payload behind
///
/// An unsorted partition holds one row, so a projection of one carries its partition key and
/// nothing else. That key is what the shard collecting the shares of a split get uses to put
/// the rows back in the order the query named their partitions in.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "TestRecord")]
pub struct TestRecordKey {
    /// The partition this row belonged to
    #[shoal(partition)]
    pub partition_key: String,
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The sorted test table
    #[shoal(projections(TestRecordKey))]
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
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
        .send_one(TestRecordGet::new(vec![test_data.partition_key.clone()]))
        .await?;
    let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
    let record = TestRecord::deserialize(access).unwrap();
    assert_eq!(replacement, record);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit of zero returns nothing at all
///
/// An unsorted partition holds exactly one row, so a limit of zero is the only limit
/// an unsorted get can ever reach. It is honoured so that `LIMIT 0` means the same
/// thing on both kinds of table.
#[tokio::test]
async fn get_with_a_zero_limit_returns_nothing() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row to get back
    client
        .send_one(TestRecord::new("partition_key", "woot"))
        .await?;
    // get it back with a limit of zero
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

/// How many partitions the multi partition tests spread their rows over
///
/// This has to be enough that the consistent hash ring puts some of them on every shard, since
/// a get that never splits proves nothing about how split gets are put back together.
const SPREAD_PARTITIONS: usize = 20;

/// Write one row into each of `SPREAD_PARTITIONS` partitions
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
    // write a row into each of those partitions
    for partition_key in &partition_keys {
        client
            .send_one(TestRecord::new(partition_key.as_str(), "woot"))
            .await?;
    }
    Ok(partition_keys)
}

/// Run a get over these partitions and report the partition keys it answered with, in order
///
/// # Arguments
///
/// * `client` - The client to send our get with
/// * `partition_keys` - The partitions to read, in the order to read them
/// * `limit` - The most rows to ask for, if any
async fn get_partition_keys(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_keys: Vec<String>,
    limit: Option<usize>,
) -> Result<Vec<String>, TestError> {
    // build a get over every one of these partitions, with a limit if we were given one
    let mut get = TestRecordGet::new(partition_keys);
    if let Some(limit) = limit {
        get = get.limit(limit);
    }
    // send it and collect the partitions the rows it answered with came from
    let mut stream = client.send(client.query().add(get)).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<TestRecord>()? {
            for row in rows.iter() {
                found.push(row.partition_key.to_string());
            }
        }
    }
    Ok(found)
}

/// Test that an unsorted get can read several partitions at once
///
/// An unsorted get used to name a single partition, so `id = 1 AND id = 2` bound only the first
/// value and the second was dropped without a word.
#[tokio::test]
async fn get_reads_several_partitions() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // read every one of them back in a single get
    let found = get_partition_keys(&client, partition_keys.clone(), None).await?;
    // each partition should have answered with its own row, exactly once
    assert_eq!(found, partition_keys);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an unsorted get returns its partitions in the order it named them
#[tokio::test]
async fn get_returns_partitions_in_query_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // naming the partitions backwards should turn the answer round with them
    let reversed = partition_keys.iter().rev().cloned().collect::<Vec<_>>();
    let found = get_partition_keys(&client, reversed.clone(), None).await?;
    assert_eq!(found, reversed);
    // and the same query has to answer the same way every time it runs
    for run in 1..10 {
        let repeated = get_partition_keys(&client, reversed.clone(), None).await?;
        assert_eq!(repeated, found, "run {run} answered in a different order");
    }
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit on an unsorted get keeps the partitions it named first
#[tokio::test]
async fn get_limit_takes_the_first_partitions() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // ask for fewer rows than we have partitions
    let found = get_partition_keys(&client, partition_keys.clone(), Some(3)).await?;
    // the rows we keep have to come from the partitions we named first
    assert_eq!(found, partition_keys[..3].to_vec());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Run a projected get over these partitions and report the keys it answered with, in order
///
/// This is the twin of `get_partition_keys` and reads the projections own variant of the
/// response, which is what a projected get answers in.
///
/// # Arguments
///
/// * `client` - The client to send our get with
/// * `partition_keys` - The partitions to read, in the order to read them
/// * `limit` - The most rows to ask for, if this get should set a limit
async fn get_projected_keys(
    client: &shoal_core::client::Shoal<TestDbClient>,
    partition_keys: Vec<String>,
    limit: Option<usize>,
) -> Result<Vec<String>, TestError> {
    // build a projected get over every one of these partitions, with a limit if we have one
    let mut get = TestRecordGet::new(partition_keys).projection::<TestRecordKey>();
    if let Some(limit) = limit {
        get = get.limit(limit);
    }
    // send it and collect the partitions the rows it answered with came from
    let mut stream = client.send(client.query().add(get)).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<TestRecordKey>()? {
            for row in rows.iter() {
                found.push(row.partition_key.to_string());
            }
        }
    }
    Ok(found)
}

/// Test that a projected get on an unsorted table answers with the projection
#[tokio::test]
async fn projection_returns_only_its_own_fields() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row to read back
    client
        .send_one(TestRecord::new("partition_key", "woot"))
        .await?;
    // read it back as the projection instead of as the whole row
    let response = client
        .send_one(
            TestRecordGet::new(vec!["partition_key".to_owned()]).projection::<TestRecordKey>(),
        )
        .await?;
    // the row is in the projections variant, not the tables
    let rows = response.access::<TestRecordKey>()?.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.first().unwrap().partition_key.as_str(), "partition_key");
    // reaching for the row type is the wrong type, not an empty answer
    assert!(response.access::<TestRecord>().is_err());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a projected get spread over several shards keeps its partition order and its limit
///
/// The shard collecting the shares of a split get asks each row which partition it came from,
/// so this is where a projection that could not name its partition would come back shuffled.
#[tokio::test]
async fn projection_orders_rows_across_partitions() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // read them all back as the projection, in the order named here
    let found = get_projected_keys(&client, partition_keys.clone(), None).await?;
    assert_eq!(found, partition_keys);
    // a limit still keeps the partitions this get named first
    let limited = get_projected_keys(&client, partition_keys.clone(), Some(3)).await?;
    assert_eq!(limited, partition_keys[..3].to_vec());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a projection reads an unsorted partition that is still an archive on disk
#[tokio::test]
async fn projection_reads_an_archived_partition() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build a test row to insert
    let test_data = TestRecord::new("partition_key", "woot");
    // leave this row on disk with nothing resident in memory
    insert_then_evict_to_disk(&temp_dir, &test_data).await?;
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // read it back as the projection, which has to find it on disk first
    let response = client
        .send_one(
            TestRecordGet::new(vec!["partition_key".to_owned()]).projection::<TestRecordKey>(),
        )
        .await?;
    // the projection came out of the archive
    let rows = response.access::<TestRecordKey>()?.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.first().unwrap().partition_key.as_str(), "partition_key");
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
