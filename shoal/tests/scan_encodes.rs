//! A scan page's resident partitions encoded in steps of their shard's loop
//! ([F126](../../docs/src/features/encodes-in-steps.md), M23's fourth step)
//!
//! A planned partition is encoded once the page reaches it, a step of the loop's second inbox at a
//! time, each over a fresh lookup of its key and at most 64 KiB of its rows a step. These rows hold
//! the steps of a page between its plan and its encoding, or between two of its pieces, while a
//! merge compacts, sweeps or evicts the partition under them, through `faults::hold_encode_steps`;
//! the hold and the sweep counter are process wide, so the rows run in a binary of their own, one
//! at a time. Since [F130](../../docs/src/features/loop-steps-by-kind.md) a row stretches the steps
//! too, on their thread's cpu or off it, through `faults::stretch_steps`, also process wide, and
//! since [item 350](../../docs/src/appendix/resolved/encode-copy-deadline.md) makes the rows a
//! piece copies cost more, through `faults::slow_piece_rows`; since
//! [item 353](../../docs/src/appendix/resolved/capture-allocations.md)
//! a row stretches one part of a capture, through `faults::stretch_capture_part`.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::server::faults;
use shoal::server::shard::walk::{WalkAsk, WalkPage};
use shoal::shared::identity::TableId;
use shoal::shared::placement::tablet_of;
use shoal::shared::protocol::admin::WalkClass;
use shoal::shared::protocol::stats::{CapturePart, ScanFigures, StepKind};
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

/// Take a row's turn, whichever row before it panicked, with no encode step held and every step
/// sized by what its rows have shown they cost
fn turn() -> MutexGuard<'static, ()> {
    let turn = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    faults::hold_encode_steps(None);
    faults::hold_evictions(false);
    faults::override_node_memory(None);
    faults::fix_encode_row_cost(None);
    faults::stretch_steps(StepKind::Capture, None);
    faults::stretch_steps(StepKind::Encode, None);
    faults::slow_piece_rows(None);
    for part in [CapturePart::Keys, CapturePart::Plan, CapturePart::View] {
        faults::stretch_capture_part(part, None);
    }
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

/// An encode step takes as many of a partition's rows as its step's time holds at what a row of
/// its table costs, never 64 KiB of them whatever they cost: 64 KiB of `Keyword`'s small rows held
/// a Zen1 host's loop up to 570 µs a step, past W12's 100
/// ([item 347](../../docs/src/appendix/resolved/encode-steps-by-rows.md))
#[tokio::test(flavor = "multi_thread")]
async fn an_encode_step_takes_the_rows_its_time_holds() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    // a row said to cost 5 µs: ten a step of 50 µs, where 64 KiB of them is about sixty
    faults::fix_encode_row_cost(Some(Duration::from_micros(5)));
    let scan = page(&pool, WalkClass::Scan);
    faults::fix_encode_row_cost(None);
    assert_eq!(scan.planned, 1);
    assert!(u64::from(scan.encode_steps) >= ROWS / 10, "{} steps for {ROWS} rows", scan.encode_steps);
    assert!(scan.largest_step <= 10 * 1_100, "a step copied {} bytes", scan.largest_step);
    // and the rows it handed out are the partition's, every one
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    assert_eq!(group_of(&scan), expected);
    pool.exit()?;
    drop(client);
    Ok(())
}

/// An encode step ends its piece once its copy has held the loop its share of a step, whatever its
/// table's rows were learned to cost: steps of `Keyword`'s large resident partitions held europa's
/// loop up to 539 µs on M23's exit, past W12's 100, under a bound learned from other steps
/// ([item 350](../../docs/src/appendix/resolved/encode-copy-deadline.md))
#[tokio::test(flavor = "multi_thread")]
async fn an_encode_step_ends_once_its_rows_have_held_the_loop() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    // a row said to cost a nanosecond, as a cost learned from cheaper rows would say, so the rows
    // bound never binds; and every row a piece copies costs 5 µs of its loop's own cpu
    faults::fix_encode_row_cost(Some(Duration::from_nanos(1)));
    faults::slow_piece_rows(Some(Duration::from_micros(5)));
    let before = walks(&pool).scan;
    let scan = page(&pool, WalkClass::Scan);
    let after = walks(&pool).scan;
    faults::slow_piece_rows(None);
    faults::fix_encode_row_cost(None);
    assert_eq!(scan.planned, 1);
    // every piece ended by its copy's deadline: by its eighth row its rows alone held 40 µs, past
    // the copy's 25, where 64 KiB of them is about sixty
    assert!(scan.largest_step <= 8 * 1_100, "a step copied {} bytes", scan.largest_step);
    assert!(u64::from(scan.encode_steps) >= ROWS / 8, "{} steps for {ROWS} rows", scan.encode_steps);
    // so no encode step held its loop past 100 µs on its own cpu
    let longest = after.encodes.longest;
    assert_eq!(
        after.encodes.past_cpu[0] - before.encodes.past_cpu[0],
        0,
        "encode steps past 100 µs on their own cpu, the longest {longest:?}"
    );
    // and the rows it handed out are the partition's, every one
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    assert_eq!(group_of(&scan), expected);
    pool.exit()?;
    drop(client);
    Ok(())
}

/// What every shard of a node's walk pages did since it started, read through each shard
///
/// # Arguments
///
/// * `pool` - The node
fn walks(pool: &ShoalPool<EncodeDb>) -> ScanFigures {
    // each shard's figures, summed as the control thread sums them
    let mut figures = ScanFigures::default();
    for answer in pool
        .read_verb(None, shoal::server::replication::ReadVerb::Walks)
        .expect("the shards answered")
    {
        let shard: ScanFigures =
            serde_json::from_value(answer.expect("a shard's walk figures")).expect("walk figures read back");
        figures.absorb(&shard);
    }
    figures
}

/// A walk page's steps are counted by their kind, a capture apart from its encode steps, each with
/// the keys it looked at, the rows and bytes it copied and its thread's own cpu beside its wall; a
/// step held past 100 µs on its own cpu is counted past it on both, and one held off its cpu, as a
/// preemption holds it, by the wall alone
/// ([F130](../../docs/src/features/loop-steps-by-kind.md))
#[tokio::test(flavor = "multi_thread")]
async fn a_step_is_counted_by_its_kind_with_its_own_cpu() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    // two rows a step, so every step's own work is far under 100 µs whatever the build
    faults::fix_encode_row_cost(Some(Duration::from_micros(25)));
    // a scan's page and a driver's, unstretched: one capture each, an encode step a piece
    let before = walks(&pool);
    let scan = page(&pool, WalkClass::Scan);
    let driver = page(&pool, WalkClass::Driver);
    let after = walks(&pool);
    let steps = u64::from(scan.encode_steps);
    assert!(steps >= ROWS / 2, "{steps} encode steps for {ROWS} rows at two a step");
    for (class, page_steps, now, then) in [
        ("scan", steps, after.scan, before.scan),
        ("driver", u64::from(driver.encode_steps), after.driver, before.driver),
    ] {
        let (captures, encodes) = (now.captures.all, now.encodes.all);
        assert_eq!(captures.steps - then.captures.all.steps, 1, "{class}: one capture a page");
        assert_eq!(encodes.steps - then.encodes.all.steps, page_steps, "{class}: an encode step a piece");
        // the capture looked at the tablet's one resident key and copied nothing; the steps one key
        // each, every row of the group between them, and the bytes the class copied off the loop
        assert_eq!((captures.keys - then.captures.all.keys, captures.rows - then.captures.all.rows), (1, 0));
        assert_eq!(encodes.keys - then.encodes.all.keys, page_steps, "{class}");
        assert_eq!(encodes.rows - then.encodes.all.rows, ROWS, "{class}: every row taken once");
        let copied = now.resident_bytes - then.resident_bytes;
        assert_eq!(encodes.bytes - then.encodes.all.bytes + captures.bytes - then.captures.all.bytes, copied);
        assert!(copied >= ROWS * 1_000, "{class}: {copied} bytes copied of {ROWS} rows of a kibibyte");
        // every step's own cpu inside its wall, and the figures before F130 counting both kinds
        assert!(captures.cpu_ns <= captures.wall_ns && encodes.cpu_ns <= encodes.wall_ns, "{class}");
        assert_eq!(now.steps - then.steps, 1 + page_steps, "{class}");
    }
    // every step held 300 µs on its own cpu: past 100 µs by the wall and by its own cpu, each kind
    faults::stretch_steps(StepKind::Capture, Some((Duration::from_micros(300), faults::Stretch::Spin)));
    faults::stretch_steps(StepKind::Encode, Some((Duration::from_micros(300), faults::Stretch::Spin)));
    let before = walks(&pool).scan;
    let spun = page(&pool, WalkClass::Scan);
    let after = walks(&pool).scan;
    let steps = u64::from(spun.encode_steps);
    assert_eq!(after.captures.past[0] - before.captures.past[0], 1);
    assert_eq!(after.captures.past_cpu[0] - before.captures.past_cpu[0], 1);
    assert_eq!(after.encodes.past[0] - before.encodes.past[0], steps);
    assert_eq!(after.encodes.past_cpu[0] - before.encodes.past_cpu[0], steps);
    assert_eq!(after.encodes.long.steps - before.encodes.long.steps, steps);
    assert!(after.encodes.long.cpu_ns - before.encodes.long.cpu_ns >= steps * 300_000);
    // every step held 1 ms off its cpu, as a preemption holds it: past 100 µs by the wall alone
    faults::stretch_steps(StepKind::Capture, Some((Duration::from_millis(1), faults::Stretch::Sleep)));
    faults::stretch_steps(StepKind::Encode, Some((Duration::from_millis(1), faults::Stretch::Sleep)));
    let before = walks(&pool).scan;
    let slept = page(&pool, WalkClass::Scan);
    let after = walks(&pool).scan;
    faults::stretch_steps(StepKind::Capture, None);
    faults::stretch_steps(StepKind::Encode, None);
    let steps = u64::from(slept.encode_steps);
    assert_eq!(after.captures.past[0] - before.captures.past[0], 1);
    assert_eq!(after.captures.past_cpu[0] - before.captures.past_cpu[0], 0);
    assert_eq!(after.encodes.past[0] - before.encodes.past[0], steps);
    assert_eq!(after.encodes.past_cpu[0] - before.encodes.past_cpu[0], 0);
    assert_eq!(
        after.encodes.past_by_wall_alone() - before.encodes.past_by_wall_alone(),
        steps,
        "every slept step past 100 µs by the wall alone"
    );
    // each gave its cpu up inside it, which its kernel events say, and none is counted as faulted
    // on its own cpu (item 353)
    assert_eq!(after.captures.past_switched - before.captures.past_switched, 1);
    assert_eq!(after.encodes.past_switched - before.encodes.past_switched, steps);
    assert_eq!(after.encodes.past_cpu_faulted - before.encodes.past_cpu_faulted, 0);
    assert!(after.encodes.past_kernel.voluntary_switches - before.encodes.past_kernel.voluntary_switches >= steps);
    // the longest step since the start is one of them, kept whole, held mostly off its cpu
    let longest = after.encodes.longest;
    assert!(longest.wall_ns >= 1_000_000 && longest.cpu_ns < longest.wall_ns / 2, "{longest:?}");
    assert_eq!((longest.keys, longest.rows), (1, 2), "{longest:?}");
    // and the pages hand out what they always did
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    for page in [&scan, &driver, &spun, &slept] {
        assert_eq!(group_of(page), expected);
    }
    faults::fix_encode_row_cost(None);
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A step that faults fresh memory in past 100 µs on its own cpu is counted among its kind's
/// faulted long steps, with every fault it took; the faults are its own cpu's, so none of them is
/// past the bound by the wall alone, and the pages hand out what they always did
/// ([item 353](../../docs/src/appendix/resolved/capture-allocations.md))
#[tokio::test(flavor = "multi_thread")]
async fn a_long_steps_faults_are_counted_by_kind() -> Result<(), TestError> {
    // a step's fresh memory, in small pages: 2,048 faults a step
    const TOUCHED: usize = 8 << 20;
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    // two rows a step, so a step's own work is far under 100 µs and its faults are what hold it
    faults::fix_encode_row_cost(Some(Duration::from_micros(25)));
    faults::stretch_steps(StepKind::Capture, Some((Duration::ZERO, faults::Stretch::Touch(TOUCHED))));
    faults::stretch_steps(StepKind::Encode, Some((Duration::ZERO, faults::Stretch::Touch(TOUCHED))));
    let before = walks(&pool).scan;
    let touched = page(&pool, WalkClass::Scan);
    let after = walks(&pool).scan;
    faults::stretch_steps(StepKind::Capture, None);
    faults::stretch_steps(StepKind::Encode, None);
    let steps = u64::from(touched.encode_steps);
    let pages = (TOUCHED / 4096) as u64;
    // the capture and every encode step past 100 µs on its own cpu, each counted as faulted
    for (kind, kind_steps, now, then) in [
        ("captures", 1, after.captures, before.captures),
        ("encodes", steps, after.encodes, before.encodes),
    ] {
        assert_eq!(now.past_cpu[0] - then.past_cpu[0], kind_steps, "{kind}: past 100 µs on their own cpu");
        assert_eq!(now.past_cpu_faulted - then.past_cpu_faulted, kind_steps, "{kind}: each of them faulted");
        assert_eq!(now.past_by_wall_alone() - then.past_by_wall_alone(), 0, "{kind}: a fault is its own cpu's");
        let faults = now.past_kernel.minor_faults - then.past_kernel.minor_faults;
        assert!(faults >= kind_steps * pages, "{kind}: {faults} faults in {kind_steps} steps of {pages} pages");
    }
    // the longest carries its own
    assert!(after.encodes.longest.kernel.minor_faults >= pages, "{:?}", after.encodes.longest);
    // and the pages hand out what they always did
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    assert_eq!(group_of(&touched), expected);
    faults::fix_encode_row_cost(None);
    pool.exit()?;
    drop(client);
    Ok(())
}

/// How many long captures each part held since an earlier reading of the counts
///
/// # Arguments
///
/// * `now` - The later reading
/// * `then` - The earlier one
fn held_since(now: [u64; 4], then: [u64; 4]) -> [u64; 4] {
    // each part's count since, which only grows
    [now[0] - then[0], now[1] - then[1], now[2] - then[2], now[3] - then[3]]
}

/// A page's capture is timed by its parts, its keys, its plan and its view, each by the wall, and
/// its rest is what they left of its step's wall: a part held 300 µs on its own cpu holds the
/// capture past 100 µs, counted among the long captures by that part and no other, a stretch of the
/// step itself is its rest's, and the pages hand out what they always did
/// ([item 353](../../docs/src/appendix/resolved/capture-allocations.md))
#[tokio::test(flavor = "multi_thread")]
async fn a_capture_is_timed_by_its_parts() -> Result<(), TestError> {
    // a part's stretch, far past 100 µs on its own cpu
    const HELD: Duration = Duration::from_micros(300);
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let rows: Vec<Entry> = (0..ROWS).map(|seq| entry(seq, 1)).collect();
    write(&client, &rows).await?;
    let expected: BTreeMap<String, Entry> = rows.iter().map(|row| (row.seq.clone(), row.clone())).collect();
    // a capture of each class unstretched: one taken apart, the group's one key planned, and its
    // parts with its rest exactly its step's wall
    for class in [WalkClass::Scan, WalkClass::Driver] {
        let before = walks(&pool);
        let walked = page(&pool, class);
        let after = walks(&pool);
        let (now, then) = match class {
            WalkClass::Scan => (after.scan, before.scan),
            WalkClass::Driver => (after.driver, before.driver),
        };
        let (parts, was) = (now.capture_parts.all, then.capture_parts.all);
        assert_eq!(parts.captures - was.captures, 1, "{class:?}: one capture taken apart");
        assert_eq!(parts.planned - was.planned, 1, "{class:?}: the group's one key planned");
        let timed = parts.timed_ns() + parts.rest_ns - was.timed_ns() - was.rest_ns;
        let wall = now.captures.all.wall_ns - then.captures.all.wall_ns;
        assert_eq!(timed, wall, "{class:?}: the parts and the rest are the step's wall");
        // the view's delta and runs inside its view's time
        let view_parts = parts.view_delta_ns + parts.view_runs_ns - was.view_delta_ns - was.view_runs_ns;
        assert!(view_parts <= parts.view_ns - was.view_ns, "{class:?}: {parts:?}");
        assert_eq!(group_of(&walked), expected, "{class:?}");
    }
    // each timed part held in turn: the long capture is that part's alone
    for part in [CapturePart::Keys, CapturePart::Plan, CapturePart::View] {
        faults::stretch_capture_part(part, Some((HELD, faults::Stretch::Spin)));
        let before = walks(&pool).scan.capture_parts;
        let walked = page(&pool, WalkClass::Scan);
        let after = walks(&pool).scan.capture_parts;
        faults::stretch_capture_part(part, None);
        let mut held = [0; 4];
        held[part.index()] = 1;
        assert_eq!(held_since(after.held_by, before.held_by), held, "{part:?}");
        // a view held after its scan is the rest of its view's, and no other part's is its view's
        let view_held: Vec<u64> = after.view_held_by.iter().zip(before.view_held_by).map(|(now, then)| now - then).collect();
        let expected_view = if part == CapturePart::View { vec![0, 0, 1] } else { vec![0, 0, 0] };
        assert_eq!(view_held, expected_view, "{part:?}");
        assert_eq!(after.long.captures - before.long.captures, 1, "{part:?}");
        let spent = after.long.part_ns(part) - before.long.part_ns(part);
        assert!(spent >= 300_000, "{part:?}: a part held 300 µs counted {spent} ns");
        assert_eq!(group_of(&walked), expected, "{part:?}");
    }
    // and the step itself held after its parts: the rest's
    faults::stretch_steps(StepKind::Capture, Some((HELD, faults::Stretch::Spin)));
    let before = walks(&pool).scan.capture_parts;
    let walked = page(&pool, WalkClass::Scan);
    let after = walks(&pool).scan.capture_parts;
    faults::stretch_steps(StepKind::Capture, None);
    assert_eq!(held_since(after.held_by, before.held_by), [0, 0, 0, 1]);
    assert!(after.long.rest_ns - before.long.rest_ns >= 300_000, "{after:?}");
    assert_eq!(group_of(&walked), expected);
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

/// The process's resident bytes, from its status
fn resident_bytes() -> usize {
    // VmRSS, in kibibytes
    let status = std::fs::read_to_string("/proc/self/status").expect("the process's status");
    let line = status.lines().find(|line| line.starts_with("VmRSS:")).expect("a resident set");
    let kib: usize = line.split_whitespace().nth(1).and_then(|kib| kib.parse().ok()).expect("a size");
    kib << 10
}

/// The eviction passes every shard of a node ran because the process was past the node's memory
///
/// # Arguments
///
/// * `pool` - The node
fn resident_passes(pool: &ShoalPool<EncodeDb>) -> u64 {
    pool.read_verb(None, shoal::server::replication::ReadVerb::Evictions)
        .expect("the shards answered")
        .into_iter()
        .map(|answer| answer.expect("a shard's evictions")["resident_passes"].as_u64().unwrap_or(0))
        .sum()
}

/// A scan of a table larger than memory begins no eviction pass by the process's memory: with
/// the node's memory set just above the process as it stands, a full scan of an archived table
/// reads its partitions through its budget and moves no `Resident` counter
/// ([W7](../../docs/src/future-work/scans/isolation.md#acceptance-tests), M23)
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_never_causes_a_resident_eviction() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_single_shard_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf.clone()).await?;
    // forty-eight groups of a thousand rows, about 48 MiB, archived by a restart
    let rows: Vec<Entry> = (0..48).flat_map(|group| (0..1_000).map(move |seq| row_of(20_000 + group, seq, 1))).collect();
    write(&client, &rows).await?;
    pool.exit()?;
    drop(client);
    // rows of four mebibytes a shard, so nearly all of the table lives in its archives
    conf.resources.memory = 4 << 20;
    let (client, pool) = utils::start_with_conf::<EncodeDb>(conf).await?;
    let mut tablets: Vec<u16> = rows.iter().map(|row| tablet_of(row.get_partition_key()) as u16).collect();
    tablets.sort_unstable();
    tablets.dedup();
    tokio::time::sleep(Duration::from_secs(2)).await;
    // the node's memory a little above the process as it stands, far below the table
    faults::override_node_memory(Some(resident_bytes() + (24 << 20)));
    let before = resident_passes(&pool);
    // every tablet scanned whole
    let mut handed = 0usize;
    for tablet in &tablets {
        let mut after = None;
        loop {
            let mut ask = WalkAsk::tablet(TableId::of("Entry"), *tablet);
            ask.class = WalkClass::Scan;
            ask.after = after;
            let page = match pool.walk(ask) {
                Ok(Ok(page)) => page,
                other => panic!("a scan's page was refused: {other:?}"),
            };
            handed += page.partitions.len();
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
    }
    // a check of the process's memory runs every quarter second, so one more is let pass
    tokio::time::sleep(Duration::from_millis(500)).await;
    let passes = resident_passes(&pool) - before;
    faults::override_node_memory(None);
    assert_eq!(handed, 48, "the scan did not hand out every group");
    assert_eq!(passes, 0, "a scan began {passes} eviction passes by the process's memory");
    pool.exit()?;
    drop(client);
    Ok(())
}
