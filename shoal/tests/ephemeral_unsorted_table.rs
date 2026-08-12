//! Integration tests for ephemeral unsorted tables in Shoal
//!
//! An ephemeral table is a persistent table with a storage engine that writes nothing, so the
//! interesting tests here are not the ones proving a get returns a row — those paths are shared
//! with `persistent_unsorted_table.rs` and could not diverge from it. They are the three at the
//! bottom of this file, which pin what makes the table ephemeral at all: nothing reaches the
//! disk, nothing survives a restart, and nothing is ever evicted.
//!
//! The schema is deliberately mixed, with the ephemeral table declared **first**. A table
//! reports which loader its storage engine needs and the shard spawns each kind once; an
//! ephemeral table claiming the filesystem loaders slot without filling it would leave
//! `disk_record` unable to read anything back.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::storage::FileSystem;
use shoal_core::tables::{EphemeralUnsortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalProjection, ShoalUnsortedTable};
use std::time::Duration;

mod utils;

use utils::TestError;

/// A row in the ephemeral table
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
    /// * `partition_key` - The partition to put this row in
    /// * `data` - The payload for this row
    pub fn new<T: Into<String>>(partition_key: T, data: T) -> Self {
        TestRecord {
            partition_key: partition_key.into(),
            data: data.into(),
        }
    }
}

/// A projection of a test record that leaves its payload behind
///
/// A projection is declared on the field holding its table, so this existing at all is the proof
/// that an ephemeral table can carry one — which it could not when it was unreachable from a
/// database struct.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "TestRecord")]
pub struct TestRecordKey {
    /// The partition this row belonged to
    #[shoal(partition)]
    pub partition_key: String,
}

/// A row in the persistent table this schema also holds
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct DiskRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// The test database schema
///
/// The ephemeral table comes first on purpose. See the module doc.
#[db]
pub struct TestDb {
    /// The ephemeral test table
    #[shoal(projections(TestRecordKey))]
    pub test_record: EphemeralUnsortedTable<TestRecord>,
    /// A persistent table sharing the database, so the two can be told apart
    pub disk_record: PersistentUnsortedTable<DiskRecord, FileSystem>,
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

/// Test setting up and tearing down a db holding an ephemeral table
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

/// Test inserting and getting rows from an ephemeral table
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
    // read the row we just wrote back
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

/// Test deleting rows from an ephemeral table
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
    // now delete this record
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

/// Test updating rows in an ephemeral table
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

/// Test that a limit of zero returns nothing at all
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

/// Test that an ephemeral get returns its partitions in the order it named them
///
/// A get is still split across the shards owning its keys and put back together by the shard
/// that split it, none of which the storage engine has anything to do with.
#[tokio::test]
async fn get_returns_partitions_in_query_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // read every one of them back in a single get
    let found = get_partition_keys(&client, partition_keys.clone(), None).await?;
    assert_eq!(found, partition_keys);
    // naming the partitions backwards should turn the answer round with them
    let reversed = partition_keys.iter().rev().cloned().collect::<Vec<_>>();
    let found = get_partition_keys(&client, reversed.clone(), None).await?;
    assert_eq!(found, reversed);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a limit on an ephemeral get keeps the partitions it named first
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

/// Test that a projected get on an ephemeral table answers with the projection
///
/// Projections on ephemeral tables were filed as a todo, because a projection is declared on a
/// database field and an ephemeral table could not be one. This is that todo closing.
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
    assert_eq!(
        rows.first().unwrap().partition_key.as_str(),
        "partition_key"
    );
    // reaching for the row type is the wrong type, not an empty answer
    assert!(response.access::<TestRecord>().is_err());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an ephemeral table writes nothing to the storage directory
///
/// The filesystem engine builds a directory tree named after each tables row type the moment
/// that table is constructed, so the absence of one is the strongest available statement that
/// no storage engine was ever wired up behind this table.
#[tokio::test]
async fn nothing_is_written_to_the_storage_directory() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write rows into both tables, so the two can be told apart by what they left behind
    client
        .send_one(TestRecord::new("partition_key", "woot"))
        .await?;
    client
        .send_one(DiskRecord {
            partition_key: "partition_key".to_owned(),
            data: "woot".to_owned(),
        })
        .await?;
    // Shutdown server, which flushes and compacts everything the persistent table holds
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // the persistent table built itself a tree under its row type name
    assert!(
        temp_dir.path().join("DiskRecord").is_dir(),
        "the persistent table wrote nothing, so this test proves nothing about the ephemeral one"
    );
    // and the ephemeral table did not
    assert!(
        !temp_dir.path().join("TestRecord").exists(),
        "an ephemeral table opened a storage directory"
    );
    Ok(())
}

/// Test that an ephemeral tables rows do not survive a restart
///
/// This is the whole trade. The same temp dir is reused so the server that comes back up would
/// find the rows if they had ever been written down.
#[tokio::test]
async fn data_does_not_survive_a_restart() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // every one of them is readable while this server is up
    let found = get_partition_keys(&client, partition_keys.clone(), None).await?;
    assert_eq!(found, partition_keys);
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a new server over the same storage directory
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // none of those rows came back
    let found = get_partition_keys(&client, partition_keys.clone(), None).await?;
    assert!(
        found.is_empty(),
        "an ephemeral table kept {} rows through a restart",
        found.len()
    );
    // and neither does an exists, which reads the same partitions by a different path
    for partition_key in &partition_keys {
        let exists = client
            .exists(TestRecordExists::new(partition_key.clone()))
            .await?;
        assert!(!exists, "{partition_key} survived a restart");
    }
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that memory pressure never evicts an ephemeral partition
///
/// This is the invariant the whole design rests on. An evicted partition is re-read from disk on
/// the next get, and an ephemeral table has no disk to re-read it from, so an ephemeral
/// partition that could be evicted would be silent data loss rather than a cache miss. It is
/// safe because a partition is only ever evicted after being marked evictable, and the only two
/// things that mark one are the filesystem compactor and the partition load path — neither of
/// which an ephemeral table reaches.
///
/// `build_pressured_config` sets the memory limit to a single byte, so the shard tries to evict
/// on every pass of its loop.
#[tokio::test]
async fn memory_pressure_does_not_evict() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server that evicts everything it is allowed to evict
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // write a row into each of our partitions
    let partition_keys = insert_spread_rows(&client).await?;
    // give the shards plenty of loop iterations to evict anything they think they may
    tokio::time::sleep(Duration::from_secs(2)).await;
    // every row is still there
    let found = get_partition_keys(&client, partition_keys.clone(), None).await?;
    assert_eq!(
        found, partition_keys,
        "memory pressure dropped rows an ephemeral table can never get back"
    );
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Test that a persistent table declared after an ephemeral one still gets its loader
///
/// The shard spawns one loader per storage kind and keeps a set of the kinds it has already
/// spawned. An ephemeral table reporting a kind it does not need would land in that set without
/// anything being spawned for it, and `disk_record` — declared second — would then have no
/// loader to answer a read that has to come off disk. So this writes a row, cycles the server
/// twice to leave it in an archive with nothing resident, and reads it back.
#[tokio::test]
async fn a_persistent_table_declared_after_an_ephemeral_one_still_loads() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // build the row we are going to have to read off disk later
    let test_data = DiskRecord {
        partition_key: "partition_key".to_owned(),
        data: "woot".to_owned(),
    };
    // start a shoal server and write it
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    client.send_one(test_data.clone()).await?;
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle the server so that write is compacted out of the intent log and into an archive
    let (_client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a third server, which holds nothing resident and must read this off disk
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    let response = client
        .send_one(DiskRecordGet::new(vec!["partition_key".to_owned()]))
        .await?;
    let access = response.access::<DiskRecord>()?.unwrap().first().unwrap();
    let record = DiskRecord::deserialize(access).unwrap();
    assert_eq!(test_data, record);
    // Shutdown server
    pool.exit()?;
    Ok(())
}
