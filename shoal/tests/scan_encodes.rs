//! A scan page's resident partitions encoded in steps of their shard's loop
//! ([F126](../../docs/src/features/encodes-in-steps.md), M23's fourth step)
//!
//! A planned partition is encoded once the page reaches it, a step of the loop's second inbox at a
//! time, each over a fresh lookup of its key and at most 64 KiB of its rows a step. These rows hold
//! the steps of a page between its plan and its encoding, or between two of its pieces, while a
//! merge compacts, sweeps or evicts the partition under them, through `faults::hold_encode_steps`;
//! the hold and the sweep counter are process wide, so the rows run in a binary of their own, one
//! at a time.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::server::faults;
use shoal::server::shard::walk::{WalkAsk, WalkPage};
use shoal::shared::identity::TableId;
use shoal::shared::placement::tablet_of;
use shoal::shared::protocol::admin::WalkClass;
use shoal::shared::traits::PartitionKeySupport;
use shoal::storage::FileSystem;
use shoal::tables::walk::walked_rows;
use shoal::tables::PersistentSortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

mod utils;
use utils::TestError;

/// A row of a sorted table, a kibibyte of body, so a few hundred of them fill several steps
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "EncodeDb")]
pub struct Entry {
    /// The partition key
    #[shoal(partition)]
    pub group: u64,
    /// The sort key, zero padded so it sorts as its number
    #[shoal(sort)]
    pub seq: String,
    /// The version a test wrote it at
    pub version: u64,
    /// A kibibyte of bytes, so a row has a size
    pub body: String,
}

/// A schema of one sorted table
#[db]
pub struct EncodeDb {
    /// The sorted table the rows walk
    pub entries: PersistentSortedTable<Entry, FileSystem>,
}

/// The group every row walks, one partition of hundreds of rows
const GROUP: u64 = 7;

/// How many rows the group holds
const ROWS: u64 = 400;

/// Held by each row for its whole length: the hold on encode steps and the sweep counter are
/// process wide
static SERIAL: Mutex<()> = Mutex::new(());

/// Take a row's turn, whichever row before it panicked, with no encode step held
fn turn() -> MutexGuard<'static, ()> {
    let turn = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    faults::hold_encode_steps(None);
    faults::hold_evictions(false);
    turn
}

/// One of the group's rows at a version
///
/// # Arguments
///
/// * `seq` - Its place in the group
/// * `version` - The version it is written at
fn entry(seq: u64, version: u64) -> Entry {
    row_of(GROUP, seq, version)
}

/// A row of any group at a version, a kibibyte of body each
///
/// # Arguments
///
/// * `group` - Its group
/// * `seq` - Its place in the group
/// * `version` - The version it is written at
fn row_of(group: u64, seq: u64, version: u64) -> Entry {
    let stem = format!("{group}-{seq}-{version}-");
    Entry {
        group,
        seq: format!("{seq:06}"),
        version,
        body: stem.repeat(1_000 / stem.len() + 1)[..1_000].to_string(),
    }
}

/// The group's tablet
fn tablet() -> u16 {
    // a tablet id is twelve bits, so it fits
    tablet_of(entry(0, 0).get_partition_key()) as u16
}

/// Write rows, five hundred a bundle, every one acknowledged
///
/// # Arguments
///
/// * `client` - The client
/// * `rows` - The rows
async fn write<R: Clone>(client: &Shoal<EncodeDbClient>, rows: &[R]) -> Result<(), TestError>
where
    shoal::shared::queries::Queries<EncodeDbClient>: AddRow<R>,
{
    for chunk in rows.chunks(500) {
        let mut bundle = client.query();
        for row in chunk {
            bundle = bundle.add_row(row.clone());
        }
        let mut answers = client.send(bundle).await?;
        while let Some(answer) = answers.next().await? {
            assert!(answer.error().is_none(), "a write failed");
        }
    }
    Ok(())
}

/// Add one row to a bundle, whatever table it is of
trait AddRow<R> {
    /// The bundle with the row's insert added
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    fn add_row(self, row: R) -> Self;
}

impl AddRow<Entry> for shoal::shared::queries::Queries<EncodeDbClient> {
    /// The bundle with the entry's insert added
    fn add_row(self, row: Entry) -> Self {
        self.add(row)
    }
}

/// Rows of other groups, on other tablets than the walked group's, enough to move the table's
/// intent log on past a few of its segments, so a merge takes the group's writes
///
/// # Arguments
///
/// * `client` - The client
/// * `round` - Which round of filler, so no two rounds write the same groups
async fn fill(client: &Shoal<EncodeDbClient>, round: u64) -> Result<(), TestError> {
    let rows: Vec<Entry> = (0..)
        .map(|at| 1_000 + round * 1_000 + at)
        .filter(|group| tablet_of(row_of(*group, 0, 0).get_partition_key()) as u16 != tablet())
        .take(64)
        .map(|group| row_of(group, 0, 1))
        .collect();
    write(client, &rows).await
}

/// What one page of the group's tablet asks for, of a class
///
/// # Arguments
///
/// * `class` - A scan's page or a driver's
fn ask_of(class: WalkClass) -> WalkAsk {
    let mut ask = WalkAsk::tablet(TableId::of("Entry"), tablet());
    ask.class = class;
    ask.limit = 1_000;
    ask
}

/// One page of the group's tablet, of a class
///
/// # Arguments
///
/// * `pool` - The node
/// * `class` - A scan's page or a driver's
fn page(pool: &ShoalPool<EncodeDb>, class: WalkClass) -> WalkPage {
    match pool.walk(ask_of(class)) {
        Ok(Ok(page)) => page,
        other => panic!("a page was refused: {other:?}"),
    }
}

/// The answer to a page asked before, waited for
///
/// # Arguments
///
/// * `asked` - Where its answer comes
fn answer_of(asked: &shoal::kanal::Receiver<Result<WalkPage, shoal::server::shard::walk::WalkRefusal>>) -> WalkPage {
    match asked.recv_timeout(Duration::from_secs(60)) {
        Ok(Ok(page)) => page,
        other => panic!("a page was refused: {other:?}"),
    }
}

/// The group's rows a page handed out, by their place in it
///
/// # Arguments
///
/// * `page` - The page
fn group_of(page: &WalkPage) -> BTreeMap<String, Entry> {
    // the group is the one partition of its tablet the rows write
    let partition = page
        .partitions
        .iter()
        .find(|partition| partition.key == entry(0, 0).get_partition_key())
        .expect("the page handed out the group");
    walked_rows::<Entry>(partition)
        .expect("the group reads back")
        .into_iter()
        .map(|row| (row.seq.clone(), row))
        .collect()
}

/// Wait until a loop is held from an encode step, or fail the row after thirty seconds
///
/// # Arguments
///
/// * `before` - How many declines had been counted before the page was asked
async fn held(before: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while faults::encode_steps_declined() <= before {
        assert!(Instant::now() < deadline, "no encode step was held");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A large resident partition is encoded a step at a time, no step copying more than 64 KiB and
/// a row, and a page hands out what a driver's capture does; one held as its archive's bytes is
/// shared by one step that copies nothing
/// ([W5](../../docs/src/future-work/scans/tablet-reader.md#acceptance-tests), M23)
#[tokio::test(flavor = "multi_thread")]
async fn a_planned_partition_is_encoded_a_step_at_a_time() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    // resident and unflushed: a scan's page encodes it in pieces, a driver's captures it whole
    let driver = page(&pool, WalkClass::Driver);
    let scan = page(&pool, WalkClass::Scan);
    assert_eq!(scan.planned, 1);
    let steps = (ROWS as usize * 1_000).div_ceil(64 << 10);
    assert!(scan.encode_steps as usize >= steps, "{} steps for {ROWS} rows", scan.encode_steps);
    assert!(scan.largest_step <= (64 << 10) + 2_048, "a step copied {} bytes", scan.largest_step);
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    assert_eq!(group_of(&driver), expected);
    assert_eq!(group_of(&scan), expected);
    pool.exit()?;
    drop(client);
    // after a restart on a node that evicts all it may, the group leaves memory; a get then reads
    // it back held as its archive's bytes, evictions held so it stays, and one step shares it
    let mut conf = conf;
    conf.resources.memory = 1;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while page(&pool, WalkClass::Driver).resident > 0 {
        assert!(Instant::now() < deadline, "the group never left memory");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    faults::hold_evictions(true);
    let got = client.send_one(EntryGet::new(vec![GROUP])).await?;
    assert!(got.error().is_none(), "the group could not be read");
    let scan = page(&pool, WalkClass::Scan);
    faults::hold_evictions(false);
    assert_eq!((scan.planned, scan.encode_steps, scan.largest_step), (1, 1, 0));
    assert_eq!(group_of(&scan), expected);
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A partition evicted between a page's plan and its steps, or between two of its pieces, is
/// handed out from its archive as the map holds it after them, never older than the page's
/// boundary ([W5](../../docs/src/future-work/scans/tablet-reader.md#acceptance-tests), M23)
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_partition_is_read_from_its_archive_at_or_after_the_boundary() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_single_shard_config(&temp_dir);
    // a log that moves on every few kibibytes
    conf.storage.default.filesystem.latency_sensitive.intent_log_size = 4 << 10;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    // the group at its first version, archived by a restart
    let first: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &first).await?;
    pool.exit()?;
    drop(client);
    // every loop evicts whatever its merges have made evictable
    conf.resources.memory = 1;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    // half the group rewritten, resident over its archive and unflushed at the page's boundary
    let second: Vec<Entry> = (0..ROWS).step_by(2).map(|seq| entry(seq, 2)).collect();
    write(&client, &second).await?;
    let mut expected: BTreeMap<String, Entry> = first.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    for row in &second {
        expected.insert(row.seq.clone(), row.clone());
    }
    // a scan's page plans it and takes its first piece; its second step is held
    faults::hold_encode_steps(Some(1));
    let before = faults::encode_steps_declined();
    let asked = pool.ask_walk(ask_of(WalkClass::Scan))?;
    held(before).await;
    // the log moves on, the rewrite is merged into the archive, and the loop evicts the group,
    // which a driver's page, captured on the loop rather than in steps, sees
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut round = 0;
    loop {
        fill(&client, round).await?;
        round += 1;
        if page(&pool, WalkClass::Driver).resident == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "the group was never evicted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // let go, the shard woken by one more write: the rest is read where the merge put it
    faults::hold_encode_steps(None);
    fill(&client, round).await?;
    let scan = answer_of(&asked);
    assert!(scan.encode_steps >= 2, "the page took {} steps", scan.encode_steps);
    assert_eq!(group_of(&scan), expected, "a row older than the page's boundary was handed out");
    pool.exit()?;
    drop(client);
    Ok(())
}

/// An overlay whose tombstones a merge swept between a page's steps is folded over its archive as
/// the map holds it after them, and hides every row deleted before the page
/// ([W5](../../docs/src/future-work/scans/tablet-reader.md#acceptance-tests), M23)
#[tokio::test(flavor = "multi_thread")]
async fn a_swept_overlay_never_brings_a_deleted_row_back() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_single_shard_config(&temp_dir);
    conf.storage.default.filesystem.latency_sensitive.intent_log_size = 4 << 10;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    // the group at its first version, archived by a restart
    let first: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &first).await?;
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    // half the group rewritten into an overlay, and every tenth row of it deleted there, each
    // tombstone shadowing the row the archive still holds
    let second: Vec<Entry> = (0..ROWS).step_by(2).map(|seq| entry(seq, 2)).collect();
    write(&client, &second).await?;
    let mut expected: BTreeMap<String, Entry> = first.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    for row in &second {
        expected.insert(row.seq.clone(), row.clone());
    }
    for seq in (0..ROWS).step_by(20) {
        let deleted = client.send_one(EntryDelete::new(GROUP, format!("{seq:06}"))).await?;
        assert!(deleted.error().is_none(), "a delete failed");
        expected.remove(&format!("{seq:06}"));
    }
    // a scan's page plans it and takes its first piece; its second step is held
    faults::hold_encode_steps(Some(1));
    let before = faults::encode_steps_declined();
    let swept = faults::tombstones_swept();
    let asked = pool.ask_walk(ask_of(WalkClass::Scan))?;
    held(before).await;
    // the log moves on until the deletes are merged and their tombstones swept from the overlay
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut round = 0;
    while faults::tombstones_swept() < swept + (ROWS / 20) {
        assert!(Instant::now() < deadline, "the overlay's tombstones were never swept");
        fill(&client, round).await?;
        round += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // still resident: the merge swept it rather than evicting it
    assert!(page(&pool, WalkClass::Driver).resident > 0, "the group was evicted");
    faults::hold_encode_steps(None);
    fill(&client, round).await?;
    let scan = answer_of(&asked);
    assert!(scan.encode_steps >= 2, "the page took {} steps", scan.encode_steps);
    assert_eq!(group_of(&scan), expected, "a deleted row came back");
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A scan of an archived table leaves nothing resident: on a node that evicts all it may, with
/// evictions held so anything a scan made resident would stay, every partition is read from its
/// archive and no tablet holds a resident partition after
/// ([W5](../../docs/src/future-work/scans/tablet-reader.md#never-resident), M23)
#[tokio::test(flavor = "multi_thread")]
async fn nothing_a_scan_reads_becomes_resident() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    // two hundred groups of five rows, archived by a restart
    let rows: Vec<Entry> = (0..200).flat_map(|group| (0..5).map(move |seq| row_of(10_000 + group, seq, 1))).collect();
    write(&client, &rows).await?;
    pool.exit()?;
    drop(client);
    conf.resources.memory = 1;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let mut tablets: Vec<u16> = rows.iter().map(|row| tablet_of(row.get_partition_key()) as u16).collect();
    tablets.sort_unstable();
    tablets.dedup();
    // every tablet's partitions out of memory, a driver's page captured on the loop seeing none
    let resident = |pool: &ShoalPool<EncodeDb>| -> u32 {
        tablets
            .iter()
            .map(|tablet| {
                let mut ask = WalkAsk::tablet(TableId::of("Entry"), *tablet);
                ask.limit = 1_000;
                match pool.walk(ask) {
                    Ok(Ok(page)) => page.resident,
                    other => panic!("a page was refused: {other:?}"),
                }
            })
            .sum()
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while resident(&pool) > 0 {
        assert!(Instant::now() < deadline, "the table never left memory");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // evictions held, a scan of every tablet reads every partition from its archive
    faults::hold_evictions(true);
    let mut archived = 0u32;
    for tablet in &tablets {
        let mut ask = WalkAsk::tablet(TableId::of("Entry"), *tablet);
        ask.class = WalkClass::Scan;
        ask.limit = 1_000;
        let page = match pool.walk(ask) {
            Ok(Ok(page)) => page,
            other => panic!("a scan's page was refused: {other:?}"),
        };
        assert_eq!(page.resident, 0, "a scan's page found a partition resident");
        archived += page.archived;
    }
    assert_eq!(archived, 200, "the scan did not read every partition from its archive");
    // and left nothing resident behind it
    let left = resident(&pool);
    faults::hold_evictions(false);
    assert_eq!(left, 0, "a scan made {left} partitions resident");
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A partition a scan's page folds is handed out in pieces of at most 64 KiB and a row, the scan
/// queue yielded to between them: a small overlay over a large archive, which one encode step
/// takes, is folded over the archive a piece at a time, and the rows are a driver's page's
/// ([W5](../../docs/src/future-work/scans/tablet-reader.md#5-fold-filter-project-and-encode-in-the-scan-queue), M23)
#[tokio::test(flavor = "multi_thread")]
async fn a_folded_partition_is_handed_out_in_pieces() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    // the group archived by a restart, then a few rows written over it, resident as an overlay
    let first: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &first).await?;
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let late: Vec<Entry> = (0..ROWS).step_by(50).map(|seq| entry(seq, 2)).collect();
    write(&client, &late).await?;
    let mut expected: BTreeMap<String, Entry> = first.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    for row in &late {
        expected.insert(row.seq.clone(), row.clone());
    }
    // a scan's page folds it over its archive a piece at a time
    let scan = page(&pool, WalkClass::Scan);
    let group = scan
        .partitions
        .iter()
        .find(|partition| partition.key == entry(0, 0).get_partition_key())
        .expect("the page handed out the group");
    let pieces: Vec<usize> = group.pieces().map(|piece| piece.len()).collect();
    assert!(pieces.len() >= (ROWS as usize * 1_000).div_ceil(64 << 10), "{} pieces", pieces.len());
    assert!(pieces.iter().all(|piece| *piece <= (64 << 10) + 2_048), "a piece too large: {pieces:?}");
    assert_eq!(group.rows as usize, expected.len());
    assert_eq!(group_of(&scan), expected);
    // and a driver's page, which folds it whole, hands out the same rows
    assert_eq!(group_of(&page(&pool, WalkClass::Driver)), expected);
    pool.exit()?;
    drop(client);
    Ok(())
}
