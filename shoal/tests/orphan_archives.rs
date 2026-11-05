//! An archive a pass took out of its map and never unlinked is removed at the next start (item 340)
//!
//! An archive pass logs an archive's delete, takes it out of the map, settles the map and only
//! then unlinks the file. A crash between the settle and the unlink leaves a file no map names,
//! and since a compactor's start commits its map and begins its log again, the next start leaves
//! a manifest without the archive and a log that never names it: no pass would weigh it again
//! ([Resolved #340](../../docs/src/appendix/resolved/orphan-archives-swept.md)). The pass here is
//! the real one, told by a fault to stop where the crash would (`faults::leave_archive_unlinks`).
//!
//! Its own binary, since the fault is a process-wide static.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::conf::Conf;
use shoal::server::faults;
use shoal::shared::identity::TableId;
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

/// A row of a kibibyte
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "OrphanDb")]
pub struct OrphanRow {
    /// The row's name
    #[shoal(partition)]
    pub id: String,
    /// What it holds
    #[shoal(update)]
    pub data: String,
}

/// The test database: one persistent table
#[db]
pub struct OrphanDb {
    /// The table whose archives a pass empties
    pub orphan_row: PersistentUnsortedTable<OrphanRow, FileSystem>,
}

/// The client this database is spoken to with
type Client = shoal::client::Shoal<OrphanDbClient>;

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

/// Every row's name
fn keys() -> Vec<String> {
    (0..ROWS).map(|at| format!("orphan-{at:05}")).collect()
}

/// Write every row with the same contents, a hundred to a bundle
///
/// # Arguments
///
/// * `client` - The client to write through
/// * `data` - What every row holds
async fn write_rows(client: &Client, data: &str) -> Result<(), TestError> {
    for chunk in keys().chunks(100) {
        // one bundle of a hundred inserts, each answered before the next bundle is sent
        let mut query = client.query();
        for key in chunk {
            query = query.add(OrphanRow {
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
    let dir = root.join("OrphanRow").join("archives");
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
    let (_client, pool) = utils::start_with_conf::<OrphanDb>(conf(temp_dir)).await?;
    // the start's compaction and the pass behind it
    tokio::time::sleep(Duration::from_secs(2)).await;
    pool.exit()?;
    // the threads gone and the port released
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// An archive a pass took out of its map and left on disk is removed at the next start, counted,
/// and nothing a row is in goes with it
#[tokio::test]
async fn an_archive_a_pass_left_on_disk_is_removed_at_the_next_start() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let table = TableId::of("OrphanRow").0;
    let original = "o".repeat(1000);
    let rewritten = "r".repeat(1000);
    // the rows, then a start that compacts them into an archive
    let (client, pool) = utils::start_with_conf::<OrphanDb>(conf(&temp_dir)).await?;
    write_rows(&client, &original).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cycle(&temp_dir).await?;
    let first = archive_files(temp_dir.path());
    assert!(!first.is_empty(), "the rows reached no archive");
    // every row written again, so the first archive holds nothing live once they are compacted,
    // and every pass from here leaves its unlinks undone, as a crash after its settle would
    faults::leave_archive_unlinks(table, true);
    let (client, pool) = utils::start_with_conf::<OrphanDb>(conf(&temp_dir)).await?;
    write_rows(&client, &rewritten).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // the start that compacts the rewrites, whose pass takes the first archive out of the map
    let deadline = Instant::now() + Duration::from_secs(60);
    let (_client, pool) = utils::start_with_conf::<OrphanDb>(conf(&temp_dir)).await?;
    while faults::archives_left().is_empty() {
        assert!(Instant::now() < deadline, "no pass took an archive out of its map");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    faults::leave_archive_unlinks(table, false);
    // the archives the pass left are the ones the rows were first written to, still on disk
    let left = faults::archives_left();
    for path in &left {
        assert!(first.contains(path), "a pass left {} which held the rewrites", path.display());
        assert!(path.exists(), "{} went though its unlink was left", path.display());
    }
    // a start, which on a build before the fix commits a map without them and keeps them for good
    let (client, pool) = utils::start_with_conf::<OrphanDb>(conf(&temp_dir)).await?;
    let still: Vec<&PathBuf> = left.iter().filter(|path| path.exists()).collect();
    assert!(
        still.is_empty(),
        "the archives a pass took out of its map are still on disk after a start: {still:?}"
    );
    // counted as removed, by the start's own report
    let swept = pool.swept().clone();
    assert_eq!(swept.removed, left.len() as u64, "{swept:?}");
    assert!(swept.removed_bytes > 0, "{swept:?}");
    assert!(swept.skipped.is_empty(), "{swept:?}");
    // and every row reads back as last written, from the archives that are left
    for key in keys() {
        let response = client.send_one(OrphanRowGet::new(vec![key.clone()])).await?;
        let row = OrphanRow::deserialize(
            response
                .access::<OrphanRow>()?
                .and_then(|rows| rows.first())
                .unwrap_or_else(|| panic!("row {key} is gone")),
        )
        .unwrap();
        assert_eq!(row.data, rewritten, "row {key}");
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // and a start with nothing left over removes nothing
    let (_client, pool) = utils::start_with_conf::<OrphanDb>(conf(&temp_dir)).await?;
    let again = pool.swept().clone();
    assert_eq!((again.candidates, again.removed), (0, 0), "{again:?}");
    assert!(again.archives > 0, "{again:?}");
    pool.exit()?;
    Ok(())
}
