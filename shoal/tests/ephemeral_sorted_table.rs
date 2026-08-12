//! Integration tests for ephemeral sorted tables in Shoal
//!
//! A sorted table is the one that makes a partition hold more than one row, so this is where the
//! sort key selections, ranges and SHQL bounds are exercised without a storage engine underneath
//! them. Those paths are shared with `persistent_sorted_table.rs` and could not diverge from it;
//! they are here because "shared" is a claim about a type alias that is worth checking rather
//! than assuming.
//!
//! The three tests at the bottom are the ones that are about this table rather than about sorted
//! tables in general: nothing reaches the disk, nothing survives a restart, and nothing is ever
//! evicted.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::queries::SortRange;
use shoal_core::shared::traits::RkyvSupport;
use shoal_core::tables::EphemeralSortedTable;
use shoal_derive::{db, ShoalProjection, ShoalSortedTable};
use std::ops::Bound;
use std::time::Duration;

mod utils;

use utils::TestError;

/// A row in the ephemeral sorted table
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
    /// * `partition_key` - The partition to put this row in
    /// * `sort_key` - The key this row is sorted by within its partition
    /// * `data` - The payload for this row
    pub fn new<T: Into<String>>(partition_key: T, sort_key: T, data: T) -> Self {
        TestRecord {
            partition_key: partition_key.into(),
            sort_key: sort_key.into(),
            data: data.into(),
        }
    }
}

/// A projection of a test record that leaves its payload behind
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "TestRecord")]
pub struct TestRecordKeys {
    /// The partition this row belonged to
    #[shoal(partition)]
    pub partition_key: String,
    /// The key this row was sorted by within its partition
    pub sort_key: String,
}

/// The test database schema
#[db]
pub struct TestDb {
    /// The ephemeral sorted test table
    #[shoal(projections(TestRecordKeys))]
    pub test_records: EphemeralSortedTable<TestRecord>,
}

/// How many partitions the multi partition tests spread their rows over
///
/// This has to be enough that the consistent hash ring puts some of them on every shard, since
/// a get that never splits proves nothing about how split gets are put back together.
const SPREAD_PARTITIONS: usize = 20;

/// Insert one row per sort key into a partition
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

/// Write three rows into each of `SPREAD_PARTITIONS` partitions
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

/// Run a get selecting named sort keys and report the rows it answered with, in order
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

/// Run a get over a range of sort keys and report the rows it answered with, in order
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

/// Test setting up and tearing down a db holding an ephemeral sorted table
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

/// Test inserting and getting rows from an ephemeral sorted table
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

/// Test that an exists answers true for a row this table holds and false for one it does not
#[tokio::test]
async fn exists() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write a row to ask about
    client
        .send_one(TestRecord::new("partition_key", "sort_key", "woot"))
        .await?;
    // the partition we wrote to holds a row
    let exists = client
        .exists(TestRecordExists::new(vec!["partition_key".to_owned()]))
        .await?;
    assert!(exists);
    // one we never wrote to does not, and says so rather than blocking on a disk read
    let exists = client
        .exists(TestRecordExists::new(vec!["missing".to_owned()]))
        .await?;
    assert!(!exists);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test deleting rows from an ephemeral sorted table
#[tokio::test]
async fn delete() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    // delete the middle one
    client
        .send_one(TestRecordDelete::new("partition_key".into(), "b".into()))
        .await?;
    // the rows either side of it are untouched
    let names = vec!["partition_key".to_string()];
    let rows = get_row_keys(&client, names.clone()).await?;
    assert_eq!(rows, expected_row_keys(&names, &["a", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test updating rows in an ephemeral sorted table
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
    assert_eq!(record.sort_key, "sort_key");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a get returns no more rows than its limit
#[tokio::test]
async fn get_stops_at_its_limit() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    // ask for two of them
    let get = TestRecordGet::new(vec!["partition_key".to_string()]).limit(2);
    let mut stream = client.send(client.query().add(get)).await?;
    let rows = drain_row_keys(&mut stream).await?;
    // the limit takes the first two in sort order
    let names = vec!["partition_key".to_string()];
    assert_eq!(rows, expected_row_keys(&names, &["a", "b"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a get spanning several shards returns its partitions in the order it named them
#[tokio::test]
async fn get_returns_partitions_in_query_order() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client, which runs more than one shard
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // every partition answers with its own rows, in the order the query named them
    let rows = get_row_keys(&client, partition_keys.clone()).await?;
    assert_eq!(rows, expected_row_keys(&partition_keys, &["a", "b", "c"]));
    // naming the partitions backwards turns the answer round with them
    let reversed = partition_keys.iter().rev().cloned().collect::<Vec<_>>();
    let rows = get_row_keys(&client, reversed.clone()).await?;
    assert_eq!(rows, expected_row_keys(&reversed, &["a", "b", "c"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that a get naming sort keys returns those rows alone, in sort order
///
/// The keys are named in reverse, since a sort key list is a set and not an order - the rows
/// still come back in the order the partition holds them.
#[tokio::test]
async fn get_selects_named_sort_keys() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write five rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c", "d", "e"]).await?;
    let names = vec!["partition_key".to_string()];
    // ask for one of them by sort key
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["c"]).await?;
    assert_eq!(rows, expected_row_keys(&names, &["c"]));
    // ask for several, named out of order
    let rows = get_row_keys_by_sort_key(&client, names.clone(), &["d", "a"]).await?;
    assert_eq!(rows, expected_row_keys(&names, &["a", "d"]));
    // Shutdown server
    pool.exit()?;
    Ok(())
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
    let names = vec!["partition_key".to_string()];
    // ask for the span between two of them
    let range = range_of(Bound::Included("b"), Bound::Included("d"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert_eq!(rows, expected_row_keys(&names, &["b", "c", "d"]));
    // each end decides for itself whether it keeps the row it names
    let range = range_of(Bound::Excluded("b"), Bound::Excluded("e"));
    let rows = get_row_keys_by_range(&client, names.clone(), range).await?;
    assert_eq!(rows, expected_row_keys(&names, &["c", "d"]));
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

/// Test that a range typed as SHQL reaches an ephemeral table and narrows what it answers with
///
/// SHQL is parsed into the same query types the builders produce, so an ephemeral table is
/// reachable from it for free - but "for free" is the sort of claim worth one test.
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

/// Test that a projected get on an ephemeral sorted table answers with the projection
#[tokio::test]
async fn projection_returns_only_its_own_fields() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // write three rows into one partition
    insert_rows(&client, "partition_key", &["a", "b", "c"]).await?;
    // read them back as the projection instead of as whole rows
    let response = client
        .send_one(
            TestRecordGet::new(vec!["partition_key".to_owned()]).projection::<TestRecordKeys>(),
        )
        .await?;
    // the rows are in the projections variant, not the tables
    let rows = response.access::<TestRecordKeys>()?.unwrap();
    let keys: Vec<String> = rows.iter().map(|row| row.sort_key.to_string()).collect();
    assert_eq!(
        keys,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    // reaching for the row type is the wrong type, not an empty answer
    assert!(response.access::<TestRecord>().is_err());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Test that an ephemeral sorted table writes nothing to the storage directory
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
    // write enough rows that a persistent table would certainly have rotated something
    insert_spread_rows(&client).await?;
    // Shutdown server, which is where a persistent table flushes and compacts
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // no table directory was ever created
    assert!(
        !temp_dir.path().join("TestRecord").exists(),
        "an ephemeral table opened a storage directory"
    );
    // the only thing in this directory is the marker the pool claims it with
    let left: Vec<String> = std::fs::read_dir(temp_dir.path())
        .expect("failed to read the storage directory")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(left, vec!["shoal-meta.json".to_string()]);
    Ok(())
}

/// Test that an ephemeral sorted tables rows do not survive a restart
///
/// This is the whole trade. The same temp dir is reused so the server that comes back up would
/// find the rows if they had ever been written down.
#[tokio::test]
async fn data_does_not_survive_a_restart() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // start a shoal server and build a client
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // every one of them is readable while this server is up
    let rows = get_row_keys(&client, partition_keys.clone()).await?;
    assert_eq!(rows, expected_row_keys(&partition_keys, &["a", "b", "c"]));
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // start a new server over the same storage directory
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // none of those rows came back
    let rows = get_row_keys(&client, partition_keys.clone()).await?;
    assert!(
        rows.is_empty(),
        "an ephemeral table kept {} rows through a restart",
        rows.len()
    );
    // and neither does an exists, which reads the same partitions by a different path
    let exists = client
        .exists(TestRecordExists::new(partition_keys.clone()))
        .await?;
    assert!(!exists, "a partition survived a restart");
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
    // spread our rows over enough partitions to land on every shard
    let partition_keys = insert_spread_rows(&client).await?;
    // give the shards plenty of loop iterations to evict anything they think they may
    tokio::time::sleep(Duration::from_secs(2)).await;
    // every row is still there
    let rows = get_row_keys(&client, partition_keys.clone()).await?;
    assert_eq!(
        rows,
        expected_row_keys(&partition_keys, &["a", "b", "c"]),
        "memory pressure dropped rows an ephemeral table can never get back"
    );
    // Shutdown server
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
