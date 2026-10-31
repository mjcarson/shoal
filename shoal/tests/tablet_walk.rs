//! Integration tests for the walk of one tablet's rows (F87)
//!
//! A walk hands a driver a tablet's rows a page at a time, in ascending key order, each partition
//! once, never older than the walk's floor ([F87](../../docs/src/features/tablet-walk.md)). These
//! drive it on a standalone node through the in-process seam a driver and the fixture use, and
//! through the admin read a client sends: on every kind of table, with rows resident, archived,
//! and resident over an archive, with writes running between the pages, and across the bounds a
//! page is held to.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::shared::auth::Credentials;
use shoal::server::shard::walk::{WalkAsk, WalkPage};
use shoal::shared::identity::TableId;
use shoal::shared::placement::{tablet_of, tablet_range, TABLET_COUNT};
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest, WalkAnswer, WalkClass};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::PartitionKeySupport;
use shoal::storage::FileSystem;
use shoal::tables::walk::{walked_sorted, walked_unsorted};
use shoal::tables::{
    EphemeralSortedTable, EphemeralUnsortedTable, PersistentSortedTable, PersistentUnsortedTable,
};
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};
use uuid::Uuid;

mod utils;

use utils::TestError;

/// A row of an unsorted table, its version moved by every write a test makes
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WalkDb")]
pub struct Item {
    /// The partition key
    #[shoal(partition)]
    pub id: u64,
    /// Moved by every write
    #[shoal(update)]
    pub version: u64,
    /// Some bytes, so a row has a size
    pub body: String,
}

/// A row of a sorted table
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WalkDb")]
pub struct Entry {
    /// The partition key
    #[shoal(partition)]
    pub group: u64,
    /// The sort key, zero padded so it sorts as its number
    #[shoal(sort)]
    pub seq: String,
    /// Some bytes, so a row has a size
    pub body: String,
}

/// A row of an ephemeral unsorted table
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WalkDb")]
pub struct VolatileItem {
    /// The partition key
    #[shoal(partition)]
    pub id: u64,
    /// Moved by every write
    #[shoal(update)]
    pub version: u64,
}

/// A row of an ephemeral sorted table
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "WalkDb")]
pub struct VolatileEntry {
    /// The partition key
    #[shoal(partition)]
    pub group: u64,
    /// The sort key, zero padded so it sorts as its number
    #[shoal(sort)]
    pub seq: String,
}

/// A schema of one table of each kind
#[db]
pub struct WalkDb {
    /// The persistent unsorted table
    pub items: PersistentUnsortedTable<Item, FileSystem>,
    /// The persistent sorted table
    pub entries: PersistentSortedTable<Entry, FileSystem>,
    /// The ephemeral unsorted table
    pub volatile_items: EphemeralUnsortedTable<VolatileItem>,
    /// The ephemeral sorted table
    pub volatile_entries: EphemeralSortedTable<VolatileEntry>,
}

/// One partition a walk handed out, as a test reads it
#[derive(Debug, Clone)]
struct Walked {
    /// Its key
    key: u64,
    /// Its live rows
    rows: u32,
    /// Its bytes, the whole partition or its first piece
    bytes: Vec<u8>,
    /// The rest of a partition a scan's page handed out in pieces (M23)
    more: Vec<Vec<u8>>,
}

/// A walked partition of a sorted table as its live rows, every piece of it read, of a row type
/// and a partition as the walk handed it out
macro_rules! sorted_of {
    ($row:ty, $walked:expr) => {
        // each piece's rows, the pieces in sort order
        std::iter::once(&$walked.bytes)
            .chain($walked.more.iter())
            .flat_map(|piece| walked_sorted::<$row>(piece).expect("a group reads back"))
            .collect::<Vec<$row>>()
    };
}

/// What a whole walk of a table came to
#[derive(Debug, Default)]
struct Walk {
    /// Every partition, in the order handed out
    partitions: Vec<Walked>,
    /// How many pages it took
    pages: usize,
    /// How many partitions came from memory
    resident: u32,
    /// How many came from an archive
    archived: u32,
}

/// An item of a version
///
/// # Arguments
///
/// * `id` - The item
/// * `version` - Its version
fn item(id: u64, version: u64) -> Item {
    Item {
        id,
        version,
        body: format!("item {id} at {version} {}", "x".repeat((id % 64) as usize)),
    }
}

/// A row of an entry group
///
/// # Arguments
///
/// * `group` - The partition
/// * `seq` - The row's place in it
fn entry(group: u64, seq: u64) -> Entry {
    Entry {
        group,
        seq: format!("{seq:06}"),
        body: format!("entry {group}/{seq}"),
    }
}

/// A row of a volatile entry group
///
/// # Arguments
///
/// * `group` - The partition
/// * `seq` - The row's place in it
fn volatile_entry(group: u64, seq: u64) -> VolatileEntry {
    VolatileEntry {
        group,
        seq: format!("{seq:06}"),
    }
}

/// Insert every row of a list through the client, a bundle at a time
///
/// # Arguments
///
/// * `client` - The client
/// * `rows` - The rows, as the bundle's queries take them
async fn insert_all<R>(client: &Shoal<WalkDbClient>, rows: Vec<R>) -> Result<(), TestError>
where
    R: Clone,
    shoal::shared::queries::Queries<WalkDbClient>: AddRow<R>,
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

/// Every row type of the schema is added to a bundle as its own insert
macro_rules! add_row {
    ($($row:ty),*) => {$(
        impl AddRow<$row> for shoal::shared::queries::Queries<WalkDbClient> {
            /// The bundle with the row's insert added
            fn add_row(self, row: $row) -> Self {
                self.add(row)
            }
        }
    )*};
}
add_row!(Item, Entry, VolatileItem, VolatileEntry);

thread_local! {
    /// The class every page a test on this thread asks is asked as: a driver's, unless a twin
    /// below runs the test with every page a scan's ([F117](../../docs/src/features/scan-class.md))
    static CLASS: std::cell::Cell<WalkClass> = const { std::cell::Cell::new(WalkClass::Driver) };
}

/// Run a walk test with every page it asks asked as a scan's: admitted by the scan class's count,
/// captured from the loop's second inbox and read in the scan queue in steps, and held to the
/// same contract as a driver's (F117)
///
/// # Arguments
///
/// * `test` - The test, which builds its own runtime and node
fn as_scan(test: fn() -> Result<(), TestError>) {
    CLASS.with(|class| class.set(WalkClass::Scan));
    let outcome = test();
    CLASS.with(|class| class.set(WalkClass::Driver));
    outcome.expect("the walk test failed with its pages asked as a scan's");
}

/// Ask a node for one page of a walk, off the test's runtime thread
///
/// # Arguments
///
/// * `pool` - The node
/// * `ask` - What the page asks for
fn page(pool: &ShoalPool<WalkDb>, ask: WalkAsk) -> Result<WalkPage, (ErrorCode, String)> {
    // asked as this thread's class, a driver's unless a twin runs the test as a scan's (F117)
    let ask = WalkAsk {
        class: CLASS.with(std::cell::Cell::get),
        ..ask
    };
    match pool.walk(ask) {
        Ok(Ok(page)) => Ok(page),
        Ok(Err(refusal)) => Err((refusal.code, refusal.msg)),
        Err(error) => Err((ErrorCode::Unknown, format!("{error:?}"))),
    }
}

/// Walk tablets of a table page by page, checking every page's invariants as it goes
///
/// # Arguments
///
/// * `pool` - The node
/// * `table` - The table's name
/// * `tablets` - The tablets to walk, in order
/// * `limit` - The most partitions a page holds
fn walk_tablets(
    pool: &ShoalPool<WalkDb>,
    table: &str,
    tablets: impl Iterator<Item = u16>,
    limit: u32,
) -> Walk {
    let mut walk = Walk::default();
    let mut last: Option<u64> = None;
    for tablet in tablets {
        let mut ask = WalkAsk::tablet(TableId::of(table), tablet);
        ask.limit = limit;
        loop {
            let page = page(pool, ask).unwrap_or_else(|refusal| panic!("a page was refused: {refusal:?}"));
            walk.pages += 1;
            walk.resident += page.resident;
            walk.archived += page.archived;
            // every key ascends, past the cursor, inside its tablet, and once
            for partition in &page.partitions {
                assert!(last.is_none_or(|last| partition.key > last), "{:016x} after {last:?}", partition.key);
                assert!(ask.after.is_none_or(|after| partition.key > after));
                assert_eq!(tablet_of(partition.key), usize::from(tablet));
                assert!(partition.rows > 0, "a partition with no live row was handed out");
                last = Some(partition.key);
                walk.partitions.push(Walked {
                    key: partition.key,
                    rows: partition.rows,
                    bytes: partition.bytes.to_vec(),
                    more: partition.more.iter().map(|piece| piece.to_vec()).collect(),
                });
            }
            assert!(page.partitions.len() <= limit as usize);
            // the cursor is the last key handed out, and a page short of its bound ends the range
            match page.next {
                Some(next) => {
                    assert_eq!(Some(next), page.partitions.last().map(|partition| partition.key));
                    ask.after = Some(next);
                }
                None => break,
            }
        }
    }
    walk
}

/// A scan's page plans its resident partitions on the loop and copies none of them there: each is
/// encoded later in a step of the loop's second inbox, and the page hands out what a driver's
/// captured page does; one bounded by a few partitions selects the smallest keys rather than
/// sorting the tablet's, and goes on from the last of them
/// ([W5](../../docs/src/future-work/scans/tablet-reader.md#2-plan-on-the-loop-serializing-nothing), M23)
#[tokio::test(flavor = "multi_thread")]
async fn a_planned_page_serializes_nothing_on_the_loop() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<WalkDb>(&temp_dir).await?;
    // resident partitions, and the tablet that holds the most of them
    let items: Vec<Item> = (0..2_000).map(|id| item(id, 1)).collect();
    insert_all(&client, items.clone()).await?;
    let mut held: BTreeMap<u16, usize> = BTreeMap::new();
    for row in &items {
        *held.entry(tablet_of(row.get_partition_key()) as u16).or_default() += 1;
    }
    let (tablet, partitions) = held.into_iter().max_by_key(|(_, count)| *count).expect("a tablet");
    assert!(partitions >= 3, "the busiest tablet holds {partitions}");
    // the tablet in one page each way: a driver's captured, a scan's planned
    let ask = |class: WalkClass, limit: u32, after: Option<u64>| {
        let mut ask = WalkAsk::tablet(TableId::of("Item"), tablet);
        ask.class = class;
        ask.limit = limit;
        ask.after = after;
        match pool.walk(ask) {
            Ok(Ok(page)) => page,
            other => panic!("a page was refused: {other:?}"),
        }
    };
    let driver = ask(WalkClass::Driver, 1_000, None);
    let scan = ask(WalkClass::Scan, 1_000, None);
    // the driver's capture copied its partitions off the loop; the scan's plan copied none
    assert!(driver.captured_bytes > 0 && driver.planned == 0, "{} {}", driver.captured_bytes, driver.planned);
    assert_eq!(scan.captured_bytes, 0, "a planned page copied bytes in its plan");
    assert_eq!(scan.planned as usize, partitions, "every resident partition was planned");
    assert_eq!(scan.resident as usize, partitions);
    // and both handed out the same partitions, byte for byte
    let pages = |page: &WalkPage| page.partitions.iter().map(|partition| (partition.key, partition.bytes.to_vec())).collect::<Vec<_>>();
    assert_eq!(pages(&scan), pages(&driver));
    // pages of two partitions select the smallest keys, each going on from the last it handed out
    let mut walked = Vec::new();
    let mut after = None;
    loop {
        let page = ask(WalkClass::Scan, 2, after);
        assert!(page.partitions.len() <= 2);
        walked.extend(page.partitions.iter().map(|partition| partition.key));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(walked, pages(&driver).into_iter().map(|(key, _)| key).collect::<Vec<_>>());
    pool.exit()?;
    drop(client);
    Ok(())
}

/// Every tablet, in order
fn every_tablet() -> impl Iterator<Item = u16> {
    // a tablet id is twelve bits, so every one fits
    (0..TABLET_COUNT).map(|tablet| tablet as u16)
}

/// Every table of every kind walks its tablets in key order, each partition once with its rows
#[tokio::test]
async fn every_kind_of_table_walks_in_key_order() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<WalkDb>(&temp_dir).await?;
    // a few hundred partitions of each table, the sorted ones three rows each
    let items: Vec<Item> = (0..300).map(|id| item(id, 1)).collect();
    let entries: Vec<Entry> = (0..100).flat_map(|group| (0..3).map(move |seq| entry(group, seq))).collect();
    let volatile_items: Vec<VolatileItem> = (0..300).map(|id| VolatileItem { id, version: 1 }).collect();
    let volatile_entries: Vec<VolatileEntry> =
        (0..100).flat_map(|group| (0..3).map(move |seq| volatile_entry(group, seq))).collect();
    insert_all(&client, items.clone()).await?;
    insert_all(&client, entries.clone()).await?;
    insert_all(&client, volatile_items.clone()).await?;
    insert_all(&client, volatile_entries.clone()).await?;
    // the unsorted tables: every row, once, as written
    let walk = walk_tablets(&pool, "Item", every_tablet(), 7);
    let walked: BTreeMap<u64, Item> = walk
        .partitions
        .iter()
        .map(|partition| (partition.key, walked_unsorted::<Item>(&partition.bytes).expect("an item reads back")))
        .collect();
    let expected: BTreeMap<u64, Item> = items.iter().map(|row| (row.get_partition_key(), row.clone())).collect();
    assert_eq!(walk.partitions.len(), expected.len(), "a partition was handed out twice or missed");
    assert_eq!(walked, expected);
    let walk = walk_tablets(&pool, "VolatileItem", every_tablet(), 7);
    let walked: BTreeMap<u64, VolatileItem> = walk
        .partitions
        .iter()
        .map(|partition| (partition.key, walked_unsorted::<VolatileItem>(&partition.bytes).expect("an item reads back")))
        .collect();
    let expected: BTreeMap<u64, VolatileItem> =
        volatile_items.iter().map(|row| (row.get_partition_key(), row.clone())).collect();
    assert_eq!(walked, expected);
    // the sorted tables: every partition once, its rows in sort order
    let walk = walk_tablets(&pool, "Entry", every_tablet(), 5);
    let walked: BTreeMap<u64, Vec<Entry>> = walk
        .partitions
        .iter()
        .map(|partition| {
            assert_eq!(partition.rows, 3);
            (partition.key, sorted_of!(Entry, partition))
        })
        .collect();
    let mut expected: BTreeMap<u64, Vec<Entry>> = BTreeMap::new();
    for row in &entries {
        expected.entry(row.get_partition_key()).or_default().push(row.clone());
    }
    assert_eq!(walked, expected);
    let walk = walk_tablets(&pool, "VolatileEntry", every_tablet(), 5);
    let walked: BTreeMap<u64, Vec<VolatileEntry>> = walk
        .partitions
        .iter()
        .map(|partition| (partition.key, sorted_of!(VolatileEntry, partition)))
        .collect();
    let mut expected: BTreeMap<u64, Vec<VolatileEntry>> = BTreeMap::new();
    for row in &volatile_entries {
        expected.entry(row.get_partition_key()).or_default().push(row.clone());
    }
    assert_eq!(walked, expected);
    pool.exit()?;
    Ok(())
}

/// Walk a table until some of its partitions come from archives, checking the rows each time
///
/// # Arguments
///
/// * `pool` - The node
/// * `expected` - The items the table holds, by key
fn walk_until_archived(pool: &ShoalPool<WalkDb>, expected: &BTreeMap<u64, Item>) -> Walk {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // every walk reads the same rows, wherever they are held
        let walk = walk_tablets(pool, "Item", every_tablet(), 16);
        let walked: BTreeMap<u64, Item> = walk
            .partitions
            .iter()
            .map(|partition| (partition.key, walked_unsorted::<Item>(&partition.bytes).expect("an item reads back")))
            .collect();
        assert_eq!(&walked, expected);
        if walk.archived > 0 || Instant::now() > deadline {
            return walk;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Partitions evicted to their archives are walked from the archives, and again after a restart
#[tokio::test]
async fn an_archived_tablet_walks_from_its_archives() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // every shard loop evicts what has been compacted, and the intent log rotates every 4 KiB
    let conf = utils::build_pressured_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf.clone()).await?;
    let items: Vec<Item> = (0..400).map(|id| item(id, 1)).collect();
    insert_all(&client, items.clone()).await?;
    let expected: BTreeMap<u64, Item> = items.iter().map(|row| (row.get_partition_key(), row.clone())).collect();
    // the rows are walked from their archives once they are evicted
    let walk = walk_until_archived(&pool, &expected);
    assert!(walk.archived > 0, "no partition was walked from an archive");
    pool.exit()?;
    drop(client);
    // and after a restart, from whatever the start left resident and the archives
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf).await?;
    let walk = walk_until_archived(&pool, &expected);
    assert!(walk.archived > 0, "no partition was walked from an archive after a restart");
    pool.exit()?;
    drop(client);
    Ok(())
}

/// Walk a table until none of its partitions is resident, checking every walk holds the same keys
///
/// # Arguments
///
/// * `pool` - The node
/// * `table` - The table
/// * `partitions` - How many partitions it holds
fn walk_until_evicted(pool: &ShoalPool<WalkDb>, table: &str, partitions: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let walk = walk_tablets(pool, table, every_tablet(), 64);
        assert_eq!(walk.partitions.len(), partitions);
        if walk.resident == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "{table} never left memory: {} resident", walk.resident);
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A resident change shadows its archived copy: an update, a delete, and a sorted partition's
/// new rows and deleted rows over the rows its archive holds
#[tokio::test]
async fn resident_changes_shadow_their_archives() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // every shard loop evicts what has been compacted, and the intent log is the default's, so
    // what is written after the restart below stays in its log, resident, until the test ends
    let mut conf = utils::build_config(&temp_dir);
    conf.resources.memory = 1;
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf.clone()).await?;
    let items: Vec<Item> = (0..300).map(|id| item(id, 1)).collect();
    let entries: Vec<Entry> = (0..60).flat_map(|group| (0..4).map(move |seq| entry(group, seq))).collect();
    insert_all(&client, items.clone()).await?;
    insert_all(&client, entries.clone()).await?;
    let mut expected: BTreeMap<u64, Item> = items.iter().map(|row| (row.get_partition_key(), row.clone())).collect();
    // a start compacts every table's intent logs, so after one every row reaches its archive and
    // every shard loop evicts it
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf).await?;
    walk_until_evicted(&pool, "Item", 300);
    walk_until_evicted(&pool, "Entry", 60);
    // then change them: every third item rewritten, every seventh deleted
    let rewritten: Vec<Item> = (0..300).step_by(3).map(|id| item(id, 2)).collect();
    insert_all(&client, rewritten.clone()).await?;
    for row in rewritten {
        expected.insert(row.get_partition_key(), row);
    }
    for id in (0..300).step_by(7) {
        client.send_one(ItemDelete::new(id)).await?;
        expected.remove(&item(id, 1).get_partition_key());
    }
    // every third group gains a row it holds alone in memory, over the rows its archive holds -
    // a partition written before it was read - and the next third loses its first row, which a
    // delete reads the partition for
    let mut groups: BTreeMap<u64, Vec<Entry>> = BTreeMap::new();
    for row in &entries {
        groups.entry(row.get_partition_key()).or_default().push(row.clone());
    }
    for group in (0..60).step_by(3) {
        client.send_one(entry(group, 100)).await?;
        let rows = groups.get_mut(&entry(group, 0).get_partition_key()).expect("a group written");
        rows.push(entry(group, 100));
    }
    for group in (1..60).step_by(3) {
        client.send_one(EntryDelete::new(group, format!("{:06}", 0))).await?;
        let rows = groups.get_mut(&entry(group, 0).get_partition_key()).expect("a group written");
        rows.retain(|row| row.seq != format!("{:06}", 0));
    }
    // the walk reads every change, wherever it is held
    let walk = walk_tablets(&pool, "Item", every_tablet(), 16);
    let walked: BTreeMap<u64, Item> = walk
        .partitions
        .iter()
        .map(|partition| (partition.key, walked_unsorted::<Item>(&partition.bytes).expect("an item reads back")))
        .collect();
    assert_eq!(walked, expected);
    let walk = walk_tablets(&pool, "Entry", every_tablet(), 16);
    let walked: BTreeMap<u64, Vec<Entry>> = walk
        .partitions
        .iter()
        .map(|partition| {
            let rows = sorted_of!(Entry, partition);
            assert_eq!(partition.rows as usize, rows.len());
            (partition.key, rows)
        })
        .collect();
    assert_eq!(walked, groups);
    pool.exit()?;
    Ok(())
}

/// A page holds no more than its bound of partitions or bytes, always one at least, and a walk
/// resumed from any cursor hands out the rest of the range
#[tokio::test]
async fn pages_are_bounded_and_resume_from_their_cursor() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<WalkDb>(&temp_dir).await?;
    // enough items that some tablet holds several, and one sorted partition far past a page's bytes
    let items: Vec<Item> = (0..12_000).map(|id| item(id, 1)).collect();
    insert_all(&client, items.clone()).await?;
    let big: Vec<Entry> = (0..2_000).map(|seq| entry(7, seq)).collect();
    insert_all(&client, big).await?;
    // the tablet holding the most items
    let mut per_tablet: HashMap<u16, usize> = HashMap::new();
    for row in &items {
        *per_tablet.entry(tablet_of(row.get_partition_key()) as u16).or_default() += 1;
    }
    let (&tablet, &held) = per_tablet.iter().max_by_key(|(_, held)| **held).expect("a tablet");
    assert!(held >= 5, "no tablet holds five items");
    // one page of all of it, against pages of one
    let whole = walk_tablets(&pool, "Item", std::iter::once(tablet), 10_000);
    assert_eq!(whole.pages, 1);
    assert_eq!(whole.partitions.len(), held);
    let ones = walk_tablets(&pool, "Item", std::iter::once(tablet), 1);
    assert_eq!(ones.pages, held + 1, "a page of one held more, or the last page was not empty");
    let keys = |walk: &Walk| walk.partitions.iter().map(|partition| partition.key).collect::<Vec<_>>();
    assert_eq!(keys(&ones), keys(&whole));
    // a page bounded at one byte holds one partition
    let mut ask = WalkAsk::tablet(TableId::of("Item"), tablet);
    ask.max_bytes = 1;
    let first = page(&pool, ask).expect("a page");
    assert_eq!(first.partitions.len(), 1);
    // resumed from the middle, the rest of the range and nothing before it
    let middle = keys(&whole)[held / 2];
    let mut ask = WalkAsk::tablet(TableId::of("Item"), tablet);
    ask.after = Some(middle);
    let rest = page(&pool, ask).expect("a page");
    assert_eq!(
        rest.partitions.iter().map(|partition| partition.key).collect::<Vec<_>>(),
        keys(&whole)[held / 2 + 1..].to_vec()
    );
    // a cursor at the range's end leaves nothing
    let (_, high) = tablet_range(tablet);
    let mut ask = WalkAsk::tablet(TableId::of("Item"), tablet);
    ask.after = Some(high);
    let done = page(&pool, ask).expect("a page");
    assert!(done.partitions.is_empty() && done.next.is_none());
    // a narrower range holds only its own keys
    let (low, _) = tablet_range(tablet);
    let cut = keys(&whole)[held / 2];
    let narrow = page(&pool, WalkAsk::range(TableId::of("Item"), tablet, (low, cut))).expect("a page");
    assert_eq!(
        narrow.partitions.iter().map(|partition| partition.key).collect::<Vec<_>>(),
        keys(&whole)[..=held / 2].to_vec()
    );
    // a sorted partition past the page's bytes is still handed out, alone
    let group = entry(7, 0).get_partition_key();
    let mut ask = WalkAsk::tablet(TableId::of("Entry"), tablet_of(group) as u16);
    ask.max_bytes = 1;
    ask.after = group.checked_sub(1);
    let alone = page(&pool, ask).expect("a page");
    assert_eq!(alone.partitions.len(), 1);
    assert_eq!(alone.partitions[0].key, group);
    assert_eq!(alone.partitions[0].rows, 2_000);
    pool.exit()?;
    Ok(())
}

/// While writes run between the pages, every partition live at the floor and never deleted is
/// handed out exactly once, none is handed out after its delete, and none older than its last
/// acknowledged write before its page was asked
#[tokio::test]
async fn the_floor_holds_while_writes_run() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // compactions and evictions run under the walk, so pages meet moved and evicted records
    let conf = utils::build_pressured_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf).await?;
    let initial: Vec<Item> = (0..3_000).map(|id| item(id, 1)).collect();
    insert_all(&client, initial.clone()).await?;
    // what each id was last acknowledged at, and when each was deleted
    let mut acked: HashMap<u64, u64> = initial.iter().map(|row| (row.id, 1)).collect();
    let mut deleted: HashMap<u64, usize> = HashMap::new();
    let key_of = |id: u64| item(id, 0).get_partition_key();
    let by_key: HashMap<u64, u64> = (0..4_000).map(|id| (key_of(id), id)).collect();
    // the first quarter of the tablets, a page of three at a time
    let tablets = 0..(TABLET_COUNT as u16 / 4);
    let mut seen: HashMap<u64, usize> = HashMap::new();
    let mut pages = 0usize;
    let mut next_id = 3_000u64;
    let mut round = 1u64;
    for tablet in tablets.clone() {
        let mut ask = WalkAsk::tablet(TableId::of("Item"), tablet);
        ask.limit = 3;
        loop {
            // the versions acknowledged before this page is asked
            let before = acked.clone();
            let walked = page(&pool, ask).unwrap_or_else(|refusal| panic!("a page was refused: {refusal:?}"));
            pages += 1;
            for partition in &walked.partitions {
                let row = walked_unsorted::<Item>(&partition.bytes).expect("an item reads back");
                let id = by_key[&partition.key];
                assert_eq!(row.id, id);
                // once, and never after its delete
                assert!(seen.insert(id, pages).is_none(), "item {id} handed out twice");
                assert!(!deleted.get(&id).is_some_and(|at| *at < pages), "item {id} handed out after its delete");
                // never older than what was acknowledged before its page
                let floor = before.get(&id).copied().unwrap_or(0);
                assert!(row.version >= floor, "item {id} at {} below {floor}", row.version);
            }
            // writes between the pages: rewrites, deletes and new items, every fourth page
            if pages % 4 == 0 {
                round += 1;
                let mut bundle = client.query();
                for id in (pages as u64 * 7..pages as u64 * 7 + 10).map(|id| id % 3_000) {
                    if !deleted.contains_key(&id) {
                        bundle = bundle.add(item(id, round));
                        acked.insert(id, round);
                    }
                }
                for _ in 0..3 {
                    bundle = bundle.add(item(next_id, 1));
                    acked.insert(next_id, 1);
                    next_id += 1;
                }
                let mut answers = client.send(bundle).await?;
                while let Some(answer) = answers.next().await? {
                    assert!(answer.error().is_none(), "a write failed");
                }
                for id in (pages as u64 * 13..pages as u64 * 13 + 3).map(|id| id % 3_000) {
                    if deleted.contains_key(&id) {
                        continue;
                    }
                    client.send_one(ItemDelete::new(id)).await?;
                    deleted.insert(id, pages);
                    acked.remove(&id);
                }
            }
            match walked.next {
                Some(next) => ask.after = Some(next),
                None => break,
            }
        }
    }
    // every item of these tablets live at the floor and never deleted was handed out
    let walked_tablets: BTreeSet<u16> = tablets.collect();
    for row in &initial {
        let tablet = tablet_of(row.get_partition_key()) as u16;
        if walked_tablets.contains(&tablet) && !deleted.contains_key(&row.id) {
            assert!(seen.contains_key(&row.id), "item {} was live throughout and never handed out", row.id);
        }
    }
    assert!(pages > 1_000, "{pages} pages");
    pool.exit()?;
    Ok(())
}

/// Send an admin walk through a client and read what it came back as
///
/// # Arguments
///
/// * `client` - The client
/// * `kind` - The request, a walk
async fn admin_walk(client: &Shoal<WalkDbClient>, kind: AdminKind) -> Result<WalkAnswer, (ErrorCode, String)> {
    let request = AdminRequest {
        op: Uuid::new_v4(),
        expected_version: 0,
        kind,
    };
    let response = client.admin(&request).await.map_err(|error| (ErrorCode::Unknown, format!("{error:?}")))?;
    match response.outcome {
        Ok(AdminOutcome::Read(json)) => Ok(serde_json::from_value(json).expect("a walk's answer")),
        Ok(other) => Err((ErrorCode::Unknown, format!("{other:?}"))),
        Err(error) => Err((error.code(), error.msg)),
    }
}

/// A walk with every bound left to its default
///
/// # Arguments
///
/// * `table` - The table
/// * `tablet` - The tablet
fn walk_kind(table: &str, tablet: u16) -> AdminKind {
    walk_bounded(table, tablet, None, None)
}

/// A walk with its low key or its limit set, every other bound left to its default
///
/// # Arguments
///
/// * `table` - The table
/// * `tablet` - The tablet
/// * `low` - The first key of the range, if set
/// * `limit` - The most partitions a page holds, if set
fn walk_bounded(table: &str, tablet: u16, low: Option<u64>, limit: Option<u32>) -> AdminKind {
    AdminKind::Walk {
        table: table.to_string(),
        tablet,
        low,
        high: None,
        after: None,
        limit,
        bytes: None,
        at_least: None,
        class: None,
    }
}

/// A standalone node answers a walk as an admin read, the same keys the seam hands out, and
/// refuses by name what is not a walk of one of its tablets
#[tokio::test]
async fn a_standalone_node_answers_a_walk_and_refuses_what_is_not_one() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<WalkDb>(&temp_dir).await?;
    let items: Vec<Item> = (0..2_000).map(|id| item(id, 1)).collect();
    insert_all(&client, items.clone()).await?;
    // every tablet's answer is the seam's
    let mut keys = 0;
    for tablet in every_tablet().step_by(64) {
        let answer = admin_walk(&client, walk_kind("Item", tablet)).await.expect("a walk is answered");
        let seam = page(&pool, WalkAsk::tablet(TableId::of("Item"), tablet)).expect("a page");
        assert_eq!(answer.keys, seam.partitions.iter().map(|partition| partition.key).collect::<Vec<_>>());
        assert_eq!(answer.table, "Item");
        assert!(answer.rows.iter().all(|rows| *rows == 1));
        assert_eq!(answer.boundary, None, "a standalone node has no applied index");
        assert_eq!(answer.next, None);
        keys += answer.keys.len();
    }
    assert!(keys > 0);
    // a tablet past the twelve bits, a range outside the tablet, a table the schema does not
    // have and a page of no partitions are each refused by name
    let refused = |result: Result<WalkAnswer, (ErrorCode, String)>| result.expect_err("refused").0;
    assert_eq!(refused(admin_walk(&client, walk_kind("Item", 4096)).await), ErrorCode::InvalidRequest);
    let outside = walk_bounded("Item", 1, Some(tablet_range(2).0), None);
    assert_eq!(refused(admin_walk(&client, outside).await), ErrorCode::InvalidRequest);
    assert_eq!(refused(admin_walk(&client, walk_kind("Nothing", 1)).await), ErrorCode::UnknownOperation);
    let empty = walk_bounded("Item", 1, None, Some(0));
    assert_eq!(refused(admin_walk(&client, empty).await), ErrorCode::InvalidRequest);
    pool.exit()?;
    Ok(())
}

/// A standalone node names no admins, so an authenticated principal may not walk it
#[tokio::test]
async fn a_walk_needs_no_principal_on_a_standalone_node() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::build_auth_config(&temp_dir, "walker", "walker-password");
    let mut pool = ShoalPool::<WalkDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    let client =
        Shoal::<WalkDbClient>::with_credentials(&addr.to_string(), Credentials::scram("walker", "walker-password")).await?;
    let refusal = admin_walk(&client, walk_kind("Item", 0)).await.expect_err("an authenticated walk was answered");
    assert_eq!(refusal.0, ErrorCode::Unauthorized, "{}", refusal.1);
    // the seam a driver uses holds no principal and is answered
    assert!(page(&pool, WalkAsk::tablet(TableId::of("Item"), 0)).is_ok());
    pool.exit()?;
    Ok(())
}

/// A sorted partition written before it was read is digested with its archived rows
///
/// Item 241, reproduced and not fixed: a sorted partition a write starts before its archive is
/// read holds only its new rows (`check_disk`), and the digest and the scrub's canonical cut take
/// a resident partition as the whole of it, so they leave its archived rows out. The walk merges
/// such a partition over its archive, which is how this was found
/// ([F87](../../docs/src/features/tablet-walk.md)). Ignored until the item is fixed; it fails
/// today, counting the overlay's rows alone.
#[tokio::test]
#[ignore = "item 241: the sorted digest and canonical cut skip a partly read partition's archived rows"]
async fn a_partly_read_sorted_partition_is_digested_whole() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    conf.resources.memory = 1;
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf.clone()).await?;
    // sixty groups of four rows, archived and evicted by a restart
    let entries: Vec<Entry> = (0..60).flat_map(|group| (0..4).map(move |seq| entry(group, seq))).collect();
    insert_all(&client, entries).await?;
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<WalkDb>(conf).await?;
    walk_until_evicted(&pool, "Entry", 60);
    // one more row in each of twenty groups, each held alone in memory over its archive
    for group in 0..20 {
        client.send_one(entry(group, 100)).await?;
    }
    // the walk counts every row the table holds
    let walk = walk_tablets(&pool, "Entry", every_tablet(), 64);
    let walked: u64 = walk.partitions.iter().map(|partition| u64::from(partition.rows)).sum();
    assert_eq!(walked, 260);
    // and so should the digest, summed over the shards
    let answers = pool.replication_verb(shoal::server::replication::ReplicationVerb::Digest {
        table: TableId::of("Entry"),
    })?;
    let digested: u64 = answers
        .into_iter()
        .map(|answer| answer.expect("a shard digests")["rows"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(digested, 260, "the digest left out the archived rows of partly read partitions");
    pool.exit()?;
    Ok(())
}

/// [`every_kind_of_table_walks_in_key_order`] with every page a scan's (F117)
#[test]
fn every_kind_of_table_walks_in_key_order_as_a_scan() {
    as_scan(every_kind_of_table_walks_in_key_order);
}

/// [`an_archived_tablet_walks_from_its_archives`] with every page a scan's (F117)
#[test]
fn an_archived_tablet_walks_from_its_archives_as_a_scan() {
    as_scan(an_archived_tablet_walks_from_its_archives);
}

/// [`resident_changes_shadow_their_archives`] with every page a scan's (F117)
#[test]
fn resident_changes_shadow_their_archives_as_a_scan() {
    as_scan(resident_changes_shadow_their_archives);
}

/// [`pages_are_bounded_and_resume_from_their_cursor`] with every page a scan's (F117)
#[test]
fn pages_are_bounded_and_resume_from_their_cursor_as_a_scan() {
    as_scan(pages_are_bounded_and_resume_from_their_cursor);
}

/// [`the_floor_holds_while_writes_run`] with every page a scan's (F117): its pages captured from
/// the loop's second inbox while the writes keep the main one busy
#[test]
fn the_floor_holds_while_writes_run_as_a_scan() {
    as_scan(the_floor_holds_while_writes_run);
}
