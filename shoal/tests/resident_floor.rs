//! A node past its memory whose rows could never bring it under
//! ([item 338](../../docs/src/appendix/resolved/resident-floor-reserve.md))
//!
//! A node judges its budget against the process's resident set
//! ([Resolved #149](../../docs/src/appendix/resolved/node-memory-budget.md)), and holds memory no
//! row accounts for: on the lab about 1.4 GiB at rest. A node whose `node_memory` is below that
//! floor once had every shard evict two fifths of its rows at every check, four times a second,
//! and stay over, its foreground reading back from disk what it had just evicted. These rows put a
//! node in process past its floor by moving its memory under it through
//! `faults::override_node_memory`, which is process wide, so they run in a binary of their own,
//! one at a time. A compacted segment's evictable marking, which reads a process wide count of
//! its steps, runs here for the same reason (O159).

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::server::conf::Conf;
use shoal::server::faults;
use shoal::server::replication::ReadVerb;
use shoal::server::shard::EVICTED_PARTITIONS;
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalUnsortedTable};
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tempfile::TempDir;

mod utils;
use utils::TestError;

/// A row of a kibibyte of body, so ten thousand of them hold about eleven mebibytes
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "FloorDb")]
pub struct Blob {
    /// The partition key, one row a partition
    #[shoal(partition)]
    pub key: u64,
    /// A kibibyte of bytes, so a row has a size
    pub body: String,
}

/// A schema of one unsorted table
#[db]
pub struct FloorDb {
    /// The rows a node past its floor holds
    pub blobs: PersistentUnsortedTable<Blob, FileSystem>,
}

/// How many rows a node is given: about eleven mebibytes, past the reserve of eight a shard of
/// a 64 MiB budget keeps past its floor, and well inside that budget
const ROWS: u64 = 10_000;

/// The shard's own budget, whose eighth is the rows it keeps past its node's floor
const SHARD_MEMORY: usize = 64 << 20;

/// Held by each row for its whole length: the node's memory override is process wide
static SERIAL: Mutex<()> = Mutex::new(());

/// Take a row's turn, whichever row before it panicked, with the node's memory its own
fn turn() -> MutexGuard<'static, ()> {
    // one row at a time
    let turn = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    // and nothing a row before it left behind
    faults::override_node_memory(None);
    faults::hold_evictions(false);
    turn
}

/// A row and its kibibyte of body
///
/// # Arguments
///
/// * `key` - Its partition key
fn blob(key: u64) -> Blob {
    // a body made from its key, so every row is its own
    let stem = format!("{key}-");
    Blob {
        key,
        body: stem.repeat(1_000 / stem.len() + 1)[..1_000].to_string(),
    }
}

/// A single shard's config whose rows fit its budget, whose memory is checked often
///
/// The sweeper ticks the loop at a tenth of the query deadline, so a deadline of a second has an
/// idle shard check its node's memory at every quarter second check it is let, with no traffic.
///
/// # Arguments
///
/// * `temp_dir` - Where the node keeps its data
fn conf(temp_dir: &TempDir) -> Conf {
    // one shard, so every row is on it
    let mut conf = utils::build_single_shard_config(temp_dir);
    // a budget the rows never reach
    conf.resources.memory = SHARD_MEMORY;
    // and a loop ticked ten times a second
    conf.networking = conf.networking.query_deadline(Duration::from_secs(1));
    conf
}

/// A node holding every row resident, read back from its archives after a restart
///
/// A partition is evicted only once its writes are archived, so the rows are written, the node
/// started again, and every row read back, which makes each partition resident and evictable.
///
/// # Arguments
///
/// * `temp_dir` - Where the node keeps its data
async fn node_holding_rows(temp_dir: &TempDir) -> Result<(Shoal<FloorDbClient>, ShoalPool<FloorDb>), TestError> {
    // the rows, written and acknowledged
    let (client, pool) = utils::start_with_conf::<FloorDb>(conf(temp_dir)).await?;
    let keys: Vec<u64> = (0..ROWS).collect();
    for chunk in keys.chunks(500) {
        // five hundred inserts a bundle
        let mut bundle = client.query();
        for key in chunk {
            bundle = bundle.add(blob(*key));
        }
        let mut answers = client.send(bundle).await?;
        while let Some(answer) = answers.next().await? {
            assert!(answer.error().is_none(), "a write failed");
        }
    }
    // archived by a restart
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<FloorDb>(conf(temp_dir)).await?;
    // and every row read back, so each is resident
    assert_eq!(read_all(&client).await?, ROWS, "the node did not hold every row");
    Ok((client, pool))
}

/// Read every row back, a thousand keys a get, and count the rows found
///
/// # Arguments
///
/// * `client` - The node's client
async fn read_all(client: &Shoal<FloorDbClient>) -> Result<u64, TestError> {
    // the rows found
    let mut found = 0u64;
    let keys: Vec<u64> = (0..ROWS).collect();
    for chunk in keys.chunks(1_000) {
        // a thousand keys a get
        let get = BlobGet::new(chunk.to_vec());
        let mut answers = client.send(client.query().add(get)).await?;
        while let Some(answer) = answers.next().await? {
            if let Some(rows) = answer.access::<Blob>()? {
                found += rows.len() as u64;
            }
        }
    }
    Ok(found)
}

/// One of a node's eviction figures, summed over its shards; zero where a build names none
///
/// # Arguments
///
/// * `pool` - The node
/// * `figure` - The figure, by its name in `EvictionFigures`
fn figure(pool: &ShoalPool<FloorDb>, figure: &str) -> u64 {
    // every shard's figures, as each reports them for a test
    pool.read_verb(None, ReadVerb::Evictions)
        .expect("the shards answered")
        .into_iter()
        .map(|answer| answer.expect("a shard's evictions")[figure].as_u64().unwrap_or(0))
        .sum()
}

/// The partitions every shard of the process has evicted, under any pressure
fn evicted() -> u64 {
    EVICTED_PARTITIONS.load(Ordering::Relaxed)
}

/// The process's resident bytes, as the kernel counts them
fn resident_bytes() -> usize {
    // VmRSS, in kibibytes
    let status = std::fs::read_to_string("/proc/self/status").expect("the process's status");
    let line = status.lines().find(|line| line.starts_with("VmRSS:")).expect("a resident set");
    let kib: usize = line.split_whitespace().nth(1).and_then(|kib| kib.parse().ok()).expect("a size");
    kib << 10
}

/// A node past its floor holds its rows to a reserve, and evicts nothing more at every check
///
/// The node's memory is moved to a mebibyte, far below what the process holds without a row, so
/// evicting every row could never bring it under. The shard keeps an eighth of its budget past
/// that floor: it evicts what its evictable rows hold above three fifths of it, once, and nothing
/// after while they stay under it, though every check still finds the node past its memory and
/// counts so. Read back, the rows are evicted to the reserve again, and no further.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_past_its_floor_holds_its_rows_to_a_reserve() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let (client, pool) = node_holding_rows(&temp_dir).await?;
    // settled: no pass yet, the rows inside their budget
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (passes, checks, before) = (figure(&pool, "resident_passes"), figure(&pool, "floor_checks"), evicted());
    // the node's memory far below its floor
    faults::override_node_memory(Some(1 << 20));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let first = (figure(&pool, "resident_passes") - passes, evicted() - before);
    let first_checks = figure(&pool, "floor_checks") - checks;
    // and a quiet second more, the rows under their reserve
    let (quiet_from, floor_passes) = (evicted(), figure(&pool, "floor_passes"));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let quiet = (evicted() - quiet_from, figure(&pool, "floor_passes") - floor_passes);
    // every row read back past the floor, then a settle
    let again_from = evicted();
    assert_eq!(read_all(&client).await?, ROWS, "a row past the floor was not read back");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let again = evicted() - again_from;
    faults::override_node_memory(None);
    // no pass begun to bring the node under, which its rows never could
    assert_eq!(
        first.0, 0,
        "a node past its floor ran {} passes by its resident memory, evicting {} of {ROWS} partitions",
        first.0, first.1
    );
    // the rows above the reserve alone evicted, a pass leaving three fifths of it
    assert!(
        first.1 > 0 && first.1 <= ROWS * 7 / 10,
        "past its floor the node evicted {} of {ROWS} partitions, where three fifths of its reserve keep about 4,000",
        first.1
    );
    assert!(first_checks >= 2, "only {first_checks} checks found the node past its floor");
    // nothing more while the rows stay under it
    assert_eq!(quiet, (0, 0), "a node under its reserve evicted (partitions, passes) {quiet:?}");
    // and rows read back are evicted to it again, no further
    assert!(
        again > 0 && again <= ROWS * 7 / 10,
        "rows read back past the floor lost {again} of {ROWS} partitions"
    );
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A node its rows can bring back under its memory still evicts them by its resident memory
///
/// The node's memory is moved two mebibytes under what the process holds, which its eleven
/// mebibytes of rows cover, so the pass #149 made runs as it did.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_its_rows_can_bring_under_still_evicts_them() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    let (client, pool) = node_holding_rows(&temp_dir).await?;
    // settled, then the node's memory a little under the process
    tokio::time::sleep(Duration::from_millis(500)).await;
    let passes = figure(&pool, "resident_passes");
    faults::override_node_memory(Some(resident_bytes() - (2 << 20)));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let ran = figure(&pool, "resident_passes") - passes;
    faults::override_node_memory(None);
    // the rows covered the excess, so the node evicted them to get under
    assert!(ran >= 1, "a node its rows could bring under ran no pass by its resident memory");
    pool.exit()?;
    drop(client);
    Ok(())
}

/// A node whose memory its floor alone fills refuses at ready, by name
///
/// Two mebibytes of node memory, with no object work and no scans carved out of it, are less
/// than the process holds before any row: the node would be past its floor from its first check,
/// so its start is refused, naming its resident set and the setting to raise.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_floor_leaves_its_rows_nothing_refuses_to_start() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    // a node of two mebibytes, nothing carved out of them
    let mut conf = conf(&temp_dir);
    conf.resources.node_memory = Some(2 << 20);
    conf.scans.enabled = false;
    let mut pool = ShoalPool::<FloorDb>::start(conf)?;
    // refused once its shards answer, by name
    let refused = pool.ready(utils::READY_TIMEOUT);
    let message = match &refused {
        Ok(addr) => format!("started on {addr}"),
        Err(error) => format!("{error}"),
    };
    // whatever the answer, the node is stopped
    let _ = pool.exit();
    assert!(refused.is_err(), "a node whose floor fills its memory {message}");
    assert!(
        message.contains("node_memory") && message.contains("resident"),
        "the refusal did not name the setting and the resident set: {message}"
    );
    Ok(())
}

/// A compacted segment holding more partitions than one step marks is marked whole, a step at a time
///
/// O159: a segment of the cell's held about 8,800 partitions, and marked in one message they held
/// a Zen1 shard's loop about 4 ms. A marking now takes 256 partitions a step, the rest from the
/// loop's second inbox between its messages. Ten thousand blobs of a kibibyte written through an
/// intent log of a mebibyte compact about a thousand partitions to a segment: every partition a
/// marking was handed is marked, and the markings took more steps than there were markings.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_segment_past_a_step_is_marked_whole() -> Result<(), TestError> {
    let _turn = turn();
    let temp_dir = utils::test_dir();
    // segments of about a thousand blobs each
    let mut conf = conf(&temp_dir);
    conf.storage.default.filesystem.latency_sensitive.intent_log_size = 1 << 20;
    let (client, pool) = utils::start_with_conf::<FloorDb>(conf).await?;
    let before = faults::markings();
    // every blob its own partition, written as a node holding rows writes them
    let keys: Vec<u64> = (0..ROWS).collect();
    for chunk in keys.chunks(500) {
        let mut bundle = client.query();
        for key in chunk {
            bundle = bundle.add(blob(*key));
        }
        let mut answers = client.send(bundle).await?;
        while let Some(answer) = answers.next().await? {
            assert!(answer.error().is_none(), "a write failed");
        }
    }
    // until most segments are compacted and every partition their markings named is marked
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let now = faults::markings();
        let (handed, marked, markings, steps) =
            (now.0 - before.0, now.1 - before.1, now.2 - before.2, now.3 - before.3);
        if handed >= ROWS / 2 && marked == handed && steps > markings {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the markings were handed {handed} partitions and marked {marked}, in {markings} markings of {steps} steps"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pool.exit()?;
    Ok(())
}
