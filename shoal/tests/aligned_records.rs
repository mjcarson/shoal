//! Integration tests for rows whose archive has to sit at a sixteen byte boundary
//!
//! rkyv lays a `u128` out at a sixteen byte boundary of its buffer, and its validator refuses a
//! buffer whose start is not at one. The intent log framed a record as sixteen bytes of size and
//! checksum then the payload, back to back with the record before it, so a record that followed
//! one whose payload was eight bytes past a multiple of sixteen left its own payload at eight
//! past a boundary; an archive's records were laid out the same way. Every row type the engine
//! had stored was eight byte aligned at most, until an object's entry carried its id as a
//! `u128` ([Resolved #228](../../docs/src/appendix/resolved/aligned-records.md)).
//!
//! A row with a `u128` of its own is sixteen byte aligned at its root, and so is every record of
//! it, whose length is then a multiple of sixteen: it never moves the record after it. The rows
//! that do are the ones whose `u128` is behind a pointer, as an object's entries are: the root is
//! eight byte aligned, the record's length can end eight past a boundary, and the field inside it
//! still needs sixteen. Every row these tests write carries its `u128`s in a `Vec`, with payloads
//! of every length mod sixteen, in one bundle so their records share a staged buffer.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::conf::Conf;
use shoal::shared::queries::Queries;
use shoal::storage::FileSystem;
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// How many rows each table is given, one bundle each
///
/// Enough that every payload length mod sixteen appears several times over, so some record
/// follows one whose length leaves the next eight bytes past a boundary.
const ROWS: u64 = 64;

/// An unsorted row carrying 128 bit fields behind a pointer, as an object's entries carry its id
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "AlignDb")]
pub struct WideId {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// Fields rkyv lays out at a sixteen byte boundary, under a root that is eight byte aligned
    #[shoal(update)]
    pub ids: Vec<u128>,
    /// A payload whose length moves where the next record starts
    #[shoal(update)]
    pub data: String,
}

/// A sorted row carrying 128 bit fields behind a pointer
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "AlignDb")]
pub struct SortedWideId {
    /// The partition key
    #[shoal(partition)]
    pub group: String,
    /// The sort key
    #[shoal(sort)]
    pub key: String,
    /// Fields rkyv lays out at a sixteen byte boundary, under a root that is eight byte aligned
    #[shoal(update)]
    pub ids: Vec<u128>,
    /// A payload whose length moves where the next record starts
    #[shoal(update)]
    pub data: String,
}

/// The test database schema
#[db]
pub struct AlignDb {
    /// Unsorted rows with wide fields
    pub wide_id: PersistentUnsortedTable<WideId, FileSystem>,
    /// Sorted rows with wide fields
    pub sorted_wide_id: PersistentSortedTable<SortedWideId, FileSystem>,
}

/// The unsorted row a key is written as
///
/// # Arguments
///
/// * `key` - The row's key
fn wide(key: u64) -> WideId {
    // a payload of every length up to forty seven bytes, so every residue mod sixteen appears
    WideId {
        key,
        ids: (0..=key % 3)
            .map(|n| u128::from(key) << 64 | u128::from(n))
            .collect(),
        data: "w".repeat((key % 48) as usize),
    }
}

/// The sorted row a key is written as
///
/// Each in a partition of its own, so the archive holds as many records as there are rows.
///
/// # Arguments
///
/// * `key` - The row's key
fn sorted(key: u64) -> SortedWideId {
    // the same spread of payload lengths as the unsorted rows
    SortedWideId {
        group: format!("group-{key}"),
        key: format!("key-{key}"),
        ids: (0..=key % 2)
            .map(|n| u128::from(key) << 64 | u128::from(n) << 8)
            .collect(),
        data: "s".repeat((key * 7 % 48) as usize),
    }
}

/// A single shard config, so every record lands in one log and one archive
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
fn config(temp_dir: &TempDir) -> Conf {
    // one shard, so the records of a bundle are staged into one buffer back to back
    utils::build_single_shard_config(temp_dir)
}

/// Start a server on this storage, waiting for every shard to come up
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
async fn start(
    temp_dir: &TempDir,
) -> Result<
    (
        shoal::client::Shoal<AlignDbClient>,
        shoal::server::ShoalPool<AlignDb>,
    ),
    TestError,
> {
    // a shard that fails its replay fails the start here, with its error
    utils::start_with_conf::<AlignDb>(config(temp_dir)).await
}

/// Stop a server and wait for its port to be released
///
/// # Arguments
///
/// * `pool` - The server to stop
async fn stop(pool: shoal::server::ShoalPool<AlignDb>) -> Result<(), TestError> {
    // shut down, which writes out whatever is still staged and fdatasyncs it
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Write every row of both tables, one bundle a table
///
/// # Arguments
///
/// * `client` - The client to write with
async fn write_rows(client: &shoal::client::Shoal<AlignDbClient>) -> Result<(), TestError> {
    // one bundle of unsorted rows, so they reach the shard together and share a staged buffer
    let mut queries = Queries::<AlignDbClient>::default();
    for key in 0..ROWS {
        queries.add_mut(wide(key));
    }
    let mut stream = client.send(queries).await?;
    while stream.next().await?.is_some() {}
    // and one of sorted rows
    let mut queries = Queries::<AlignDbClient>::default();
    for key in 0..ROWS {
        queries.add_mut(sorted(key));
    }
    let mut stream = client.send(queries).await?;
    while stream.next().await?.is_some() {}
    Ok(())
}

/// Read every row of both tables back and check each is the row written
///
/// # Arguments
///
/// * `client` - The client to read with
async fn check_rows(client: &shoal::client::Shoal<AlignDbClient>) -> Result<(), TestError> {
    // every unsorted row, in one get
    let get = WideIdGet::new((0..ROWS).collect::<Vec<_>>());
    let mut stream = client.send(client.query().add(get)).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<WideId>()? {
            for row in rows.iter() {
                found.push(
                    rkyv::deserialize::<WideId, rkyv::rancor::Error>(row)
                        .expect("a row deserializes"),
                );
            }
        }
    }
    found.sort_by_key(|row| row.key);
    assert_eq!(found, (0..ROWS).map(wide).collect::<Vec<_>>());
    // every sorted row, one partition each
    let groups = (0..ROWS).map(|key| sorted(key).group).collect::<Vec<_>>();
    let get = SortedWideIdGet::new(groups);
    let mut stream = client.send(client.query().add(get)).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<SortedWideId>()? {
            for row in rows.iter() {
                found.push(
                    rkyv::deserialize::<SortedWideId, rkyv::rancor::Error>(row)
                        .expect("a row deserializes"),
                );
            }
        }
    }
    found.sort_by(|a, b| a.group.cmp(&b.group));
    let mut expected = (0..ROWS).map(sorted).collect::<Vec<_>>();
    expected.sort_by(|a, b| a.group.cmp(&b.group));
    assert_eq!(found, expected);
    Ok(())
}

/// Rows with a 128 bit field are replayed from the intent log they were committed to
///
/// The restart replays the log, which validates every record where it lies in the read
/// buffer. Before item 228 a record eight bytes past a boundary failed that validation, and
/// the replay's `?` ended the shard at startup.
#[tokio::test(flavor = "multi_thread")]
async fn wide_rows_replay_from_an_intent_log() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // write every row, then stop so they are left in the intent log
    let (client, pool) = start(&temp_dir).await?;
    write_rows(&client).await?;
    stop(pool).await?;
    // start again, which replays every record of the log
    let (client, pool) = start(&temp_dir).await?;
    check_rows(&client).await?;
    stop(pool).await
}

/// Rows with a 128 bit field are loaded from the archive a compaction wrote them to
///
/// The second session replays the log and compacts it into an archive, so the third has an
/// empty log and nothing resident, and every get loads its partition from an archive record.
#[tokio::test(flavor = "multi_thread")]
async fn wide_rows_load_from_an_archive() -> Result<(), TestError> {
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // write every row, then stop so they are left in the intent log
    let (client, pool) = start(&temp_dir).await?;
    write_rows(&client).await?;
    stop(pool).await?;
    // cycle once, which replays the log and compacts it into an archive
    let (_client, pool) = start(&temp_dir).await?;
    stop(pool).await?;
    // and read every row back off the archive
    let (client, pool) = start(&temp_dir).await?;
    check_rows(&client).await?;
    stop(pool).await
}
