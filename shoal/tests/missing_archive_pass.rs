//! An archive pass that meets an archive whose file is gone deletes what it can (item 361)
//!
//! An archive pass weighs every archive its map knows or counts live records in, least used first,
//! and opened each one it chose with `DmaFile::open(&path).await?`. An archive whose file is gone
//! while the map still names records in it - a device that lost the file, an operator who removed
//! it - failed that open, and the pass with it, before it copied or deleted anything: tried again
//! after its backoff, the next try chose the same archive and failed the same way, so no pass on
//! that shard copied or deleted anything of the table again
//! ([Resolved #361](../../docs/src/appendix/resolved/lost-archive-skipped.md)).
//!
//! Its own binary, so the archives it hides and the passes it waits on are its alone.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Errors;
use shoal::server::conf::Conf;
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::RkyvSupport;
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// How many rows are written, and written again
const ROWS: usize = 2_000;

/// The row whose archive is lost
const LOST: &str = "lost-row";

/// A row of a kibibyte
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "LostDb")]
pub struct LostRow {
    /// The row's name
    #[shoal(partition)]
    pub id: String,
    /// What it holds
    #[shoal(update)]
    pub data: String,
}

/// The test database: one persistent table
#[db]
pub struct LostDb {
    /// The table one of whose archives is lost
    pub lost_row: PersistentUnsortedTable<LostRow, FileSystem>,
}

/// The client this database is spoken to with
type Client = shoal::client::Shoal<LostDbClient>;

/// One shard, and an archive pass after every rotation with no wait between them
///
/// # Arguments
///
/// * `temp_dir` - The server's storage directory
fn conf(temp_dir: &TempDir) -> Conf {
    // a single shard, so every row is in one map and one directory
    let mut conf = utils::build_single_shard_config(temp_dir);
    let throughput = &mut conf.storage.default.filesystem.throughput_sensitive;
    *throughput = throughput.clone().archive_pass_interval(Duration::ZERO);
    conf
}

/// Every row's name but the lost one
fn keys() -> Vec<String> {
    (0..ROWS).map(|at| format!("kept-{at:05}")).collect()
}

/// Write rows with the same contents, a hundred to a bundle
///
/// # Arguments
///
/// * `client` - The client to write through
/// * `keys` - The rows' names
/// * `data` - What every row holds
async fn write_rows(client: &Client, keys: &[String], data: &str) -> Result<(), TestError> {
    for chunk in keys.chunks(100) {
        // one bundle of a hundred inserts, each answered before the next bundle is sent
        let mut query = client.query();
        for key in chunk {
            query = query.add(LostRow {
                id: key.clone(),
                data: data.to_owned(),
            });
        }
        let mut stream = client.send(query).await?;
        while stream.next().await?.is_some() {}
    }
    Ok(())
}

/// Every archive file under the table's archive directory
///
/// # Arguments
///
/// * `root` - The server's storage directory
fn archive_files(root: &Path) -> Vec<PathBuf> {
    // the table's archives, never the map logs' directory beside them
    let dir = root.join("LostRow").join("archives");
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_file())
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}

/// Start a server and stop it once its start compacted what it found
///
/// # Arguments
///
/// * `temp_dir` - The server's storage directory
async fn cycle(temp_dir: &TempDir) -> Result<(), TestError> {
    let (_client, pool) = utils::start_with_conf::<LostDb>(conf(temp_dir)).await?;
    // the start's compaction and the pass behind it
    tokio::time::sleep(Duration::from_secs(2)).await;
    pool.exit()?;
    // the threads gone and the port released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Read one row back, or the error its read was answered with
///
/// # Arguments
///
/// * `client` - The client to read through
/// * `key` - The row's name
async fn read_row(client: &Client, key: &str) -> Result<Option<LostRow>, Errors> {
    let response = client.send_one(LostRowGet::new(vec![key.to_owned()])).await?;
    Ok(response
        .access::<LostRow>()?
        .and_then(|rows| rows.first())
        .map(|row| LostRow::deserialize(row).unwrap()))
}

/// A pass that meets an archive whose file is gone skips it, deletes the dead archives it chose,
/// and leaves the lost archive's rows named, so a read says so and a file put back reads again
#[tokio::test]
async fn a_pass_skips_an_archive_whose_file_is_gone_and_deletes_the_dead_ones() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let original = "o".repeat(1000);
    let rewritten = "r".repeat(1000);
    // the rows, then a start that compacts them into an archive of their own
    let (client, pool) = utils::start_with_conf::<LostDb>(conf(&temp_dir)).await?;
    write_rows(&client, &keys(), &original).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cycle(&temp_dir).await?;
    let first = archive_files(temp_dir.path());
    assert!(!first.is_empty(), "the rows reached no archive");
    // one more row, compacted at the next start into an archive of its own
    let (client, pool) = utils::start_with_conf::<LostDb>(conf(&temp_dir)).await?;
    write_rows(&client, &[LOST.to_owned()], "lost").await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cycle(&temp_dir).await?;
    let lost: Vec<PathBuf> = archive_files(temp_dir.path())
        .into_iter()
        .filter(|path| !first.contains(path))
        .collect();
    assert_eq!(lost.len(), 1, "the one row did not reach one archive of its own: {lost:?}");
    let lost = lost[0].clone();
    // its file gone while the map still names the row in it
    let missing = utils::MissingArchives::one(&temp_dir, "LostRow", &lost);
    // every other row written again, so the first archives hold nothing live once compacted
    let (client, pool) = utils::start_with_conf::<LostDb>(conf(&temp_dir)).await?;
    write_rows(&client, &keys(), &rewritten).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // the start that compacts the rewrites, whose passes must delete the first archives though
    // the lost one is among those they weigh
    let (client, pool) = utils::start_with_conf::<LostDb>(conf(&temp_dir)).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left: Vec<&PathBuf> = first.iter().filter(|path| path.exists()).collect();
        if left.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the dead archives are still on disk 30 s after the start, an archive whose file is gone \
             failing every pass: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // the lost archive was not made again, and the shard serves on
    assert!(missing.recreated().is_empty(), "a pass made the lost archive again: {:?}", missing.recreated());
    assert_eq!(pool.failure(), None);
    // the lost row is named still, and its read says the archive is missing by name
    match read_row(&client, LOST).await {
        Err(Errors::Server { code: ErrorCode::ArchiveMissing, .. }) => (),
        other => panic!("the lost row was read as {other:?}, not as an archive missing"),
    }
    // every other row reads back as last written
    for key in keys() {
        let row = read_row(&client, &key).await?.unwrap_or_else(|| panic!("row {key} is gone"));
        assert_eq!(row.data, rewritten, "row {key}");
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // the file put back, the lost row reads again: the pass kept what the map named
    drop(missing);
    let (client, pool) = utils::start_with_conf::<LostDb>(conf(&temp_dir)).await?;
    let row = read_row(&client, LOST).await?.expect("the lost row is gone once its file came back");
    assert_eq!(row.data, "lost");
    pool.exit()?;
    Ok(())
}
