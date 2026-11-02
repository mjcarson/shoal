//! Storage faults for a directory a test names: a torn write, a full disk and a lost device
//!
//! The failure model object storage adds to the tablets' ([P7](../../../docs/src/object-storage/contract.md#the-contract))
//! names three faults of a device that its node outlives: a write that tears, a device that
//! fills, and a device that stops answering. A clause no test can violate is not checked, so a
//! test has to be able to cause each of them, for one directory and nothing beside it
//! ([F70](../../../docs/src/features/storage-faults.md)).
//!
//! The faults are decided here and applied in glommio, whose files ask an [`IoHook`] before
//! every open, read, write, sync and metadata operation once one is set. Every byte of a
//! table's intent log, an archive, the shard's WAL, a checkpoint, a snapshot and the control
//! log goes through a glommio file, so they are all reached, handles opened before the fault
//! was armed included. The few small files a start writes through `std::fs` - the storage
//! marker, its lock and the hosting table - ask [`guard`] themselves.
//!
//! Like the crash points, this is off unless armed and behind no feature: until a fault is
//! armed the hook is not even set, and once one has been, an operation outside every armed
//! directory costs one relaxed load and a short scan of the table.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use glommio::io::{IoHook, IoOp, IoVerdict};

/// What happens to the operations under a directory
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The write that takes the bytes written under the directory past this many is torn: its
    /// start reaches the device and is synced, and the process ends as a crash would
    Torn {
        /// How many bytes are written whole before the tear
        after_bytes: u64,
    },
    /// The directory holds this many more bytes than it did when the fault was armed, and a
    /// write that would grow its files past that fails with `ENOSPC`
    Full {
        /// How many bytes the files may grow by
        budget: u64,
    },
    /// Every operation under the directory fails with `EIO`, as a device that stopped answering
    Lost,
}

/// One armed fault and what it has counted
#[derive(Debug)]
struct Armed {
    /// The directory, and everything under it
    dir: PathBuf,
    /// The fault
    fault: Fault,
    /// The bytes written under the directory since it was armed, for a tear
    written: u64,
    /// The bytes the directory's files have grown by since it was armed, for a full disk
    grown: u64,
    /// The size each file has reached, seeded from its size on disk the first time it is written
    sizes: HashMap<PathBuf, u64>,
    /// Whether the tear has happened
    torn: bool,
}

/// Whether any fault has been armed, which is all an operation looks at otherwise
static ARMED: AtomicBool = AtomicBool::new(false);

/// Every armed fault, one a directory
static TABLE: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

/// Whether a tear ends the process, which every caller but this module's own tests wants
static EXIT_ON_TEAR: AtomicBool = AtomicBool::new(true);

/// The hook glommio asks, which decides by the table
struct Faults;

/// The one hook this process sets, on the first fault armed
static HOOK: Faults = Faults;

/// Take the table, whatever a panicking holder left
fn table() -> MutexGuard<'static, Vec<Armed>> {
    TABLE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The armed fault whose directory holds a path, the deepest when two do
///
/// # Arguments
///
/// * `table` - The armed faults
/// * `path` - The path
fn covering<'a>(table: &'a mut [Armed], path: &Path) -> Option<&'a mut Armed> {
    // by component, so `/a/b` never covers `/a/bc`
    table
        .iter_mut()
        .filter(|armed| path.starts_with(&armed.dir))
        .max_by_key(|armed| armed.dir.components().count())
}

/// The error a lost device answers everything with
fn lost() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

/// Grow a file under a full fault by what an operation takes past its end, or refuse it with the
/// disk's full error once the budget would be passed
///
/// # Arguments
///
/// * `armed` - The fault armed over the file
/// * `path` - The file
/// * `pos` - Where the operation starts
/// * `len` - How many bytes it takes from there
/// * `budget` - The bytes the directory may still grow by
fn grow(armed: &mut Armed, path: &Path, pos: u64, len: u64, budget: u64) -> IoVerdict {
    let end = pos.saturating_add(len);
    let size = *armed
        .sizes
        .entry(path.to_path_buf())
        .or_insert_with(|| std::fs::metadata(path).map_or(0, |meta| meta.len()));
    // only what lies past the file's end takes space
    let growth = end.saturating_sub(size);
    if armed.grown.saturating_add(growth) > budget {
        return IoVerdict::Fail(io::Error::from_raw_os_error(libc::ENOSPC));
    }
    armed.grown += growth;
    armed.sizes.insert(path.to_path_buf(), size.max(end));
    IoVerdict::Proceed
}

impl IoHook for Faults {
    /// Decide an operation by the fault armed over its path, if any is
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file or directory
    /// * `op` - What the operation is about to do
    fn check(&self, path: &Path, op: IoOp) -> IoVerdict {
        // nothing armed is nothing to decide
        if !ARMED.load(Ordering::Relaxed) {
            return IoVerdict::Proceed;
        }
        let mut table = table();
        let Some(armed) = covering(&mut table, path) else {
            return IoVerdict::Proceed;
        };
        match (armed.fault, op) {
            // a lost device answers nothing
            (Fault::Lost, _) => IoVerdict::Fail(lost()),
            // a full disk refuses a write that grows a file past the budget, and only that
            (Fault::Full { budget }, IoOp::Write { pos, len }) => grow(armed, path, pos, len as u64, budget),
            // and an allocation ahead of the writes, which grows the file as they would have (F96)
            (Fault::Full { budget }, IoOp::Allocate { pos, len }) => grow(armed, path, pos, len, budget),
            // a tear cuts the one write that crosses its mark, and nothing after it
            (Fault::Torn { after_bytes }, IoOp::Write { len, .. }) if !armed.torn => {
                let after = armed.written.saturating_add(len as u64);
                if after > after_bytes {
                    armed.torn = true;
                    let keep = after_bytes.saturating_sub(armed.written);
                    return IoVerdict::Tear(usize::try_from(keep).unwrap_or(usize::MAX));
                }
                armed.written = after;
                IoVerdict::Proceed
            }
            // anything else under a full or torn directory goes ahead
            _ => IoVerdict::Proceed,
        }
    }

    /// End the process once a torn write's start is on the device
    ///
    /// # Arguments
    ///
    /// * `path` - The file the write was torn in
    /// * `written` - How many of its bytes reached the device
    fn torn(&self, path: &Path, written: usize) {
        tracing::error!(
            msg = "dying at an armed torn write",
            path = %path.display(),
            written
        );
        // exits with 137, the way a kill and a crash point do, with nothing flushed
        if EXIT_ON_TEAR.load(Ordering::Relaxed) {
            std::process::exit(137);
        }
    }
}

/// Arm a fault for a directory and everything under it, replacing one armed there before
///
/// # Arguments
///
/// * `dir` - The directory
/// * `fault` - The fault
pub fn arm(dir: &Path, fault: Fault) {
    // the hook is set on the first fault, and never before, so a process that arms nothing
    // pays nothing for it
    glommio::io::set_io_hook(&HOOK);
    let mut table = table();
    table.retain(|armed| armed.dir != dir);
    table.push(Armed {
        dir: dir.to_path_buf(),
        fault,
        written: 0,
        grown: 0,
        sizes: HashMap::new(),
        torn: false,
    });
    ARMED.store(true, Ordering::Relaxed);
    tracing::warn!(msg = "armed a storage fault", dir = %dir.display(), ?fault);
}

/// Lift the fault armed for a directory, if there is one
///
/// # Arguments
///
/// * `dir` - The directory
pub fn clear(dir: &Path) {
    let mut table = table();
    table.retain(|armed| armed.dir != dir);
    ARMED.store(!table.is_empty(), Ordering::Relaxed);
}

/// Read a fault as a test spells it: `torn <bytes>`, `full <bytes>` or `lost`
///
/// # Arguments
///
/// * `spec` - The fault as spelled
///
/// # Errors
///
/// Refuses a fault this module does not know, and a byte count that is not a number.
pub fn parse(spec: &str) -> Result<Fault, String> {
    // the kind, then its byte count where it takes one
    let mut words = spec.split_whitespace();
    let bytes = |word: Option<&str>| -> Result<u64, String> {
        word.ok_or_else(|| format!("{spec:?} names no byte count"))?
            .parse()
            .map_err(|error| format!("{spec:?} has a bad byte count: {error}"))
    };
    match words.next() {
        Some("torn") => Ok(Fault::Torn {
            after_bytes: bytes(words.next())?,
        }),
        Some("full") => Ok(Fault::Full {
            budget: bytes(words.next())?,
        }),
        Some("lost") => Ok(Fault::Lost),
        _ => Err(format!(
            "{spec:?} is not a fault; a fault is torn <bytes>, full <bytes> or lost"
        )),
    }
}

/// Refuse an operation through `std::fs` on a path under a lost directory
///
/// The storage marker, its lock and the hosting table are written without glommio, so they ask
/// here. A full or torn directory lets them through: they are a few hundred bytes, written whole.
///
/// # Arguments
///
/// * `path` - The path about to be opened, read or written
///
/// # Errors
///
/// `EIO` when a lost fault is armed over the path.
pub fn guard(path: &Path) -> io::Result<()> {
    // nothing armed is nothing to refuse
    if !ARMED.load(Ordering::Relaxed) {
        return Ok(());
    }
    match covering(&mut table(), path).map(|armed| armed.fault) {
        Some(Fault::Lost) => Err(lost()),
        _ => Ok(()),
    }
}

/// The free bytes a full fault armed over a path leaves, if one is
///
/// A full fault's directory reports what is left of its budget as its free bytes, so a node
/// sees its disk fill the way a real one would and sheds before it fails, if its reserve says to.
///
/// # Arguments
///
/// * `path` - The path free bytes are asked of
#[must_use]
pub fn free_bytes_under(path: &Path) -> Option<u64> {
    // nothing armed is the filesystem's own answer
    if !ARMED.load(Ordering::Relaxed) {
        return None;
    }
    match covering(&mut table(), path) {
        Some(Armed {
            fault: Fault::Full { budget },
            grown,
            ..
        }) => Some(budget.saturating_sub(*grown)),
        _ => None,
    }
}

/// Whether any path's key has been forced, which is all the object layer looks at otherwise
static PATHS_FORCED: AtomicBool = AtomicBool::new(false);

/// The prefixes whose paths are forced onto one key, and the key each is forced onto
static FORCED_KEYS: Mutex<Vec<(String, u64)>> = Mutex::new(Vec::new());

/// Force every path that starts with a prefix onto one key, as a collision of the path hash would
///
/// Two paths whose hashes collide are two entries of one `ObjectMeta` row, told apart by the whole
/// path ([P14](../../../docs/src/object-storage/contract.md), [S3](../../../docs/src/object-storage/objects.md#path-identity)).
/// A 64 bit hash does not collide on demand, so a test forces one here: every path under the prefix
/// hashes to `key` in this process's object layer, and nothing else does
/// ([F80](../../../docs/src/features/object-wire.md)). Until one is forced the object layer pays
/// one relaxed load a path.
///
/// # Arguments
///
/// * `prefix` - The prefix of the paths to force
/// * `key` - The key they all hash to
pub fn force_path_key(prefix: &str, key: u64) {
    // the prefix and its key, then the flag every path reads first
    let mut forced = FORCED_KEYS.lock().unwrap_or_else(PoisonError::into_inner);
    forced.retain(|(held, _)| held != prefix);
    forced.push((prefix.to_string(), key));
    PATHS_FORCED.store(true, Ordering::Release);
}

/// Stop forcing every prefix [`force_path_key`] forced
pub fn clear_path_keys() {
    // empty the table, then the flag
    FORCED_KEYS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
    PATHS_FORCED.store(false, Ordering::Release);
}

/// The key an object's `ObjectMeta` row lives under: its path's hash, or the key a test forced
///
/// # Arguments
///
/// * `path` - The object's path
#[must_use]
pub fn path_key(path: &str) -> u64 {
    // nothing forced: the hash, after one load
    if !PATHS_FORCED.load(Ordering::Acquire) {
        return crate::shared::objects::path_key(path);
    }
    // the key forced for the longest prefix of this path, if any
    let forced = FORCED_KEYS.lock().unwrap_or_else(PoisonError::into_inner);
    forced
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix.as_str()))
        .max_by_key(|(prefix, _)| prefix.len())
        .map_or_else(|| crate::shared::objects::path_key(path), |(_, key)| *key)
}

/// Whether every table's segment merges are held, as a compactor hundreds of jobs behind holds them
static MERGES_HELD: AtomicBool = AtomicBool::new(false);

/// How many of the next segment merges fail before they read anything
static MERGES_TO_FAIL: AtomicU64 = AtomicU64::new(0);

/// Hold every table's segment merges in its compactor's backlog, or let them go
///
/// A held merge stays queued where a backlog would keep it: a cut, an install and an archive
/// pass still run, and the shard hands the compactor segments as ever. What lets a test find the
/// state a node far behind its merges is in, between a segment's hand-off and its merge
/// ([item 260](../../../docs/src/appendix/resolved/checkpoint-in-segment-order.md)). Until one is
/// held a compactor pays one relaxed load a job.
///
/// # Arguments
///
/// * `hold` - Whether to hold them
pub fn hold_merges(hold: bool) {
    MERGES_HELD.store(hold, Ordering::Release);
}

/// Whether a test holds every segment merge
#[must_use]
pub fn merges_held() -> bool {
    MERGES_HELD.load(Ordering::Acquire)
}

/// Fail the next segment merges before they read anything, so each is tried again
///
/// A merge that fails before it writes is set aside and tried again after a backoff, as an
/// unreadable segment's or a corrupt record's is ([Resolved #91](../../../docs/src/appendix/resolved/compaction-retry.md),
/// [Resolved #166](../../../docs/src/appendix/resolved/segment-compaction-corrupt-loop.md)).
///
/// # Arguments
///
/// * `count` - How many merges to fail
pub fn fail_merges(count: u64) {
    MERGES_TO_FAIL.store(count, Ordering::Release);
}

/// Whether the merge about to run is one a test asked to fail, counting it if so
#[must_use]
pub fn fail_this_merge() -> bool {
    // nothing armed: one load
    if MERGES_TO_FAIL.load(Ordering::Acquire) == 0 {
        return false;
    }
    // one of the armed failures taken, unless another merge took the last
    MERGES_TO_FAIL
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

/// Whether any group's copy is held from standing, so the check costs one load until a test holds
/// one
static ELECTIONS_HELD_ANY: AtomicBool = AtomicBool::new(false);

/// The groups whose copy on this node a test holds from standing for their elections, by id
static ELECTIONS_HELD: Mutex<Vec<u64>> = Mutex::new(Vec::new());

/// Hold a group's copy on this node from standing for its elections, or let it stand again
///
/// The fixture's `ELECT <group> off` sets openraft's switch through the shard, and the shard sets
/// the switch again whenever its own rule for standing changes: a link that comes back, a reserve
/// that lifts, a head start that ends. Each of those reads the hold first, so a copy a test keeps
/// from standing stays kept until the test lets it go (item 142). A transfer of the lead still
/// elects it: openraft's handoff does not ask the switch.
///
/// # Arguments
///
/// * `group` - The group, by its id
/// * `held` - Whether it is held
pub fn hold_elections(group: u64, held: bool) {
    let mut table = ELECTIONS_HELD.lock().unwrap_or_else(PoisonError::into_inner);
    // at most one entry a group, there while it is held
    table.retain(|id| *id != group);
    if held {
        table.push(group);
    }
    ELECTIONS_HELD_ANY.store(!table.is_empty(), Ordering::Release);
}

/// Whether a group's copy on this node is held from standing for its elections
///
/// # Arguments
///
/// * `group` - The group, by its id
#[must_use]
pub fn elections_held(group: u64) -> bool {
    // nothing held: one load
    if !ELECTIONS_HELD_ANY.load(Ordering::Acquire) {
        return false;
    }
    ELECTIONS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&group)
}

/// Whether any table's walk reads are held, so the check costs one load until a test holds one
static WALK_READS_HELD_ANY: AtomicBool = AtomicBool::new(false);

/// The walk pages a test holds before their reads, by table identity and class
static WALK_READS_HELD: Mutex<Vec<(u64, crate::shared::protocol::admin::WalkClass)>> = Mutex::new(Vec::new());

/// How many walk pages are waiting in a hold right now
static WALK_READS_WAITING: AtomicU64 = AtomicU64::new(0);

/// Hold the reads of a table's walk pages of one class, or let them go
///
/// A held page is admitted and counted in flight, and its reads wait before they start, so a test
/// can fill a class's count and see what a page of another class is answered
/// ([F116](../../../docs/src/features/scan-admission.md)). Keyed by table, since every test of a
/// binary shares these statics.
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `class` - The class of the pages held
/// * `held` - Whether they are held
pub fn hold_walk_reads(table: u64, class: crate::shared::protocol::admin::WalkClass, held: bool) {
    let mut entries = WALK_READS_HELD.lock().unwrap_or_else(PoisonError::into_inner);
    // at most one entry a table and class, there while it is held
    entries.retain(|entry| *entry != (table, class));
    if held {
        entries.push((table, class));
    }
    WALK_READS_HELD_ANY.store(!entries.is_empty(), Ordering::Release);
}

/// Whether a table's walk pages of a class are held before their reads
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `class` - The page's class
#[must_use]
pub fn walk_reads_held(table: u64, class: crate::shared::protocol::admin::WalkClass) -> bool {
    // nothing held: one load
    if !WALK_READS_HELD_ANY.load(Ordering::Acquire) {
        return false;
    }
    WALK_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&(table, class))
}

/// Wait while a page's reads are held, counted as waiting the whole time
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `class` - The page's class
pub async fn wait_walk_reads(table: u64, class: crate::shared::protocol::admin::WalkClass) {
    // nothing held: no wait and no count
    if !walk_reads_held(table, class) {
        return;
    }
    WALK_READS_WAITING.fetch_add(1, Ordering::AcqRel);
    while walk_reads_held(table, class) {
        glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    }
    WALK_READS_WAITING.fetch_sub(1, Ordering::AcqRel);
}

/// How many walk pages are waiting in a hold right now, which a test waits on rather than a time
#[must_use]
pub fn walk_reads_waiting() -> u64 {
    WALK_READS_WAITING.load(Ordering::Acquire)
}

/// Whether any table's partition reads are held, so the check costs one load until a test holds one
static PARTITION_READS_HELD_ANY: AtomicBool = AtomicBool::new(false);

/// The tables whose partition reads a test holds in flight, by identity
static PARTITION_READS_HELD: Mutex<Vec<u64>> = Mutex::new(Vec::new());

/// How many partition reads are waiting in a hold right now
static PARTITION_READS_WAITING: AtomicU64 = AtomicU64::new(0);

/// Hold a table's partition reads in flight, or let them go
///
/// A held read is a get's or a write's read of a partition from its archive, counted in flight
/// on its device and waiting before it reads, so a test can keep a device busy for as long as it
/// likes and see what a scan's reads of it do meanwhile
/// ([F119](../../../docs/src/features/scan-pacing.md)). Keyed by table, since every test of a
/// binary shares these statics.
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `held` - Whether its reads are held
pub fn hold_partition_reads(table: u64, held: bool) {
    let mut tables = PARTITION_READS_HELD.lock().unwrap_or_else(PoisonError::into_inner);
    // at most one entry a table, there while it is held
    tables.retain(|entry| *entry != table);
    if held {
        tables.push(table);
    }
    PARTITION_READS_HELD_ANY.store(!tables.is_empty(), Ordering::Release);
}

/// Whether a table's partition reads are held
///
/// # Arguments
///
/// * `table` - The table, by its identity
fn partition_reads_held(table: u64) -> bool {
    PARTITION_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&table)
}

/// Wait while a table's partition reads are held, counted as waiting the whole time
///
/// # Arguments
///
/// * `table` - The table, by name, its identity read only while something is held
pub async fn wait_partition_reads(table: &str) {
    // nothing held: one load, and no identity hashed
    if !PARTITION_READS_HELD_ANY.load(Ordering::Acquire) {
        return;
    }
    let table = crate::shared::identity::TableId::of(table).0;
    if !partition_reads_held(table) {
        return;
    }
    PARTITION_READS_WAITING.fetch_add(1, Ordering::AcqRel);
    while partition_reads_held(table) {
        glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    }
    PARTITION_READS_WAITING.fetch_sub(1, Ordering::AcqRel);
}

/// How many partition reads are waiting in a hold right now, which a test waits on rather than a
/// time
#[must_use]
pub fn partition_reads_waiting() -> u64 {
    PARTITION_READS_WAITING.load(Ordering::Acquire)
}

/// Whether any table's resolved reads are held, so the check costs one load until a test holds one
static RESOLVED_READS_HELD_ANY: AtomicBool = AtomicBool::new(false);

/// The tables whose resolved reads a test holds, by identity
static RESOLVED_READS_HELD: Mutex<Vec<u64>> = Mutex::new(Vec::new());

/// How many resolved reads are waiting in a hold right now
static RESOLVED_READS_WAITING: AtomicU64 = AtomicU64::new(0);

/// A resolved read counted as waiting until it drops, however its wait ends
struct ResolvedReadWaiting;

impl Drop for ResolvedReadWaiting {
    /// The read waits no longer, let go or dropped with its executor
    fn drop(&mut self) {
        RESOLVED_READS_WAITING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Hold a table's partition reads between finding a partition's records and reading them
///
/// A held read has looked its partition's chain up in the map and not yet opened an archive, so a
/// test can move or prune the partition under it, as an archive pass does, and see what the read
/// answers once it is let go ([Resolved #343](../../../docs/src/appendix/resolved/moved-partition-read-again.md)).
/// Keyed by table, since every test of a binary shares these statics.
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `held` - Whether its resolved reads are held
pub fn hold_resolved_reads(table: u64, held: bool) {
    let mut tables = RESOLVED_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // at most one entry a table, there while it is held
    tables.retain(|entry| *entry != table);
    if held {
        tables.push(table);
    }
    RESOLVED_READS_HELD_ANY.store(!tables.is_empty(), Ordering::Release);
}

/// Whether a table's resolved reads are held
///
/// # Arguments
///
/// * `table` - The table, by its identity
fn resolved_reads_held(table: u64) -> bool {
    RESOLVED_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&table)
}

/// Wait while a table's resolved reads are held, counted as waiting the whole time
///
/// # Arguments
///
/// * `table` - The table, by name, its identity read only while something is held
pub async fn wait_resolved_reads(table: &str) {
    // nothing held: one load, and no identity hashed
    if !RESOLVED_READS_HELD_ANY.load(Ordering::Acquire) {
        return;
    }
    let table = crate::shared::identity::TableId::of(table).0;
    if !resolved_reads_held(table) {
        return;
    }
    RESOLVED_READS_WAITING.fetch_add(1, Ordering::AcqRel);
    let _waiting = ResolvedReadWaiting;
    while resolved_reads_held(table) {
        glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// How many resolved reads are waiting in a hold right now, which a test waits on rather than a
/// time
#[must_use]
pub fn resolved_reads_waiting() -> u64 {
    RESOLVED_READS_WAITING.load(Ordering::Acquire)
}

/// Whether any table's page reads are held, so the check costs one load until a test holds one
static PAGE_READS_HELD_ANY: AtomicBool = AtomicBool::new(false);

/// The tables whose scan pages' reads a test holds, by identity
static PAGE_READS_HELD: Mutex<Vec<u64>> = Mutex::new(Vec::new());

/// How many page reads are waiting in a hold right now
static PAGE_READS_WAITING: AtomicU64 = AtomicU64::new(0);

/// A page read counted as waiting until it drops, however its wait ends
struct PageReadWaiting;

impl Drop for PageReadWaiting {
    /// The read waits no longer, let go or dropped with its page
    fn drop(&mut self) {
        PAGE_READS_WAITING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Hold a table's scan pages' archive reads once each is drawn and paced, before it is read
///
/// A held read is one a page's reader has issued, after its wave was drawn from the page's budget
/// and the read itself paced by its device, so a test can count a page's reads in flight before
/// any is answered, or move a page's records under them
/// ([M23](../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)). Keyed
/// by table, since every test of a binary shares these statics.
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `held` - Whether its pages' reads are held
pub fn hold_page_reads(table: u64, held: bool) {
    let mut tables = PAGE_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // at most one entry a table, there while it is held
    tables.retain(|entry| *entry != table);
    if held {
        tables.push(table);
    }
    PAGE_READS_HELD_ANY.store(!tables.is_empty(), Ordering::Release);
}

/// Whether a table's page reads are held
///
/// # Arguments
///
/// * `table` - The table, by its identity
fn page_reads_held(table: u64) -> bool {
    PAGE_READS_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&table)
}

/// Wait while a table's page reads are held, counted as waiting the whole time
///
/// # Arguments
///
/// * `table` - The table, by name, its identity read only while something is held
pub async fn wait_page_reads(table: &str) {
    // nothing held: one load, and no identity hashed
    if !PAGE_READS_HELD_ANY.load(Ordering::Acquire) {
        return;
    }
    let table = crate::shared::identity::TableId::of(table).0;
    if !page_reads_held(table) {
        return;
    }
    PAGE_READS_WAITING.fetch_add(1, Ordering::AcqRel);
    let _waiting = PageReadWaiting;
    while page_reads_held(table) {
        glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// How many page reads are waiting in a hold right now, which a test waits on rather than a time
#[must_use]
pub fn page_reads_waiting() -> u64 {
    PAGE_READS_WAITING.load(Ordering::Acquire)
}

/// Whether a test holds every loop's encode steps, so the check costs one load until it does
static ENCODE_STEPS_HELD: AtomicBool = AtomicBool::new(false);

/// How many encode steps a hold lets run before it holds the rest
static ENCODE_STEPS_LEFT: AtomicU64 = AtomicU64::new(0);

/// How many times a loop found an encode step it was held from running
static ENCODE_STEPS_DECLINED: AtomicU64 = AtomicU64::new(0);

/// Hold every shard's encode steps once a count of them has run, or let them go
///
/// A held step waits at the head of its loop's second inbox, which runs nothing behind it, so a
/// test can evict, compact or rewrite a planned partition between a scan page's plan and its
/// encoding, or between two of its pieces, and see what the page hands out once it is let go
/// ([M23](../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)). Process
/// wide, so a test that holds them runs in a binary of its own; a loop held from a step looks
/// again only at its next message.
///
/// # Arguments
///
/// * `run_first` - How many more steps run before the rest are held; none lets every step go
pub fn hold_encode_steps(run_first: Option<u64>) {
    // the count first, so no step slips past a hold being set
    match run_first {
        Some(count) => {
            ENCODE_STEPS_LEFT.store(count, Ordering::Release);
            ENCODE_STEPS_HELD.store(true, Ordering::Release);
        }
        None => ENCODE_STEPS_HELD.store(false, Ordering::Release),
    }
}

/// Whether the encode step at the head of a loop's second inbox may run now: always unless a test
/// holds them, and then only one of those the hold lets run first, a decline counted otherwise
#[must_use]
pub fn encode_step_runs() -> bool {
    // nothing held: one load
    if !ENCODE_STEPS_HELD.load(Ordering::Acquire) {
        return true;
    }
    // one of those the hold lets run first, if any is left
    let mut left = ENCODE_STEPS_LEFT.load(Ordering::Acquire);
    while left > 0 {
        match ENCODE_STEPS_LEFT.compare_exchange_weak(left, left - 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(now) => left = now,
        }
    }
    ENCODE_STEPS_DECLINED.fetch_add(1, Ordering::AcqRel);
    false
}

/// How many times a loop was held from an encode step, which a test waits on rather than a time
#[must_use]
pub fn encode_steps_declined() -> u64 {
    ENCODE_STEPS_DECLINED.load(Ordering::Acquire)
}

/// What a test says one row of every encode step costs, in nanoseconds, none at zero
static ENCODE_ROW_NANOS: AtomicU64 = AtomicU64::new(0);

/// Say what one row of every shard's encode steps costs, so a test knows how many rows a step
/// takes, or let each shard size its steps by what they have shown a row costs again
/// ([item 347](../../../docs/src/appendix/resolved/encode-steps-by-rows.md)). Process wide, so a
/// test that says it runs in a binary of its own.
///
/// # Arguments
///
/// * `cost` - What a row costs, none to go back to what the steps show
pub fn fix_encode_row_cost(cost: Option<std::time::Duration>) {
    // a cost said is at least a nanosecond, so none stays zero
    let nanos = cost.map_or(0, |cost| u64::try_from(cost.as_nanos()).unwrap_or(u64::MAX).max(1));
    ENCODE_ROW_NANOS.store(nanos, Ordering::Release);
}

/// What a test says one row of an encode step costs, in nanoseconds, if it says
#[must_use]
pub fn encode_row_cost() -> Option<u64> {
    // nothing said: one load
    let nanos = ENCODE_ROW_NANOS.load(Ordering::Acquire);
    (nanos > 0).then_some(nanos)
}

/// How a test lengthens a walk page's steps on its loop
/// ([F130](../../../docs/src/features/loop-steps-by-kind.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stretch {
    /// Spin on the thread's own cpu, as a step's own work would hold its loop
    Spin,
    /// Sleep the thread, off its cpu, as a step a preemption held would
    Sleep,
    /// Map this many bytes of fresh memory and touch every page of it, as a step whose memory the
    /// kernel had to fault in would; the stretch's time is not read
    /// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
    Touch(usize),
}

/// Whether a test lengthens any kind of step, so the check costs one load until it does
static STEPS_STRETCHED: AtomicBool = AtomicBool::new(false);

/// How each kind of step is lengthened, a capture's first and an encode step's second
static STEP_STRETCHES: Mutex<[Option<(std::time::Duration, Stretch)>; 2]> = Mutex::new([None, None]);

/// Where a kind of step's stretch is kept
///
/// # Arguments
///
/// * `kind` - The kind of step
fn stretch_slot(kind: crate::shared::protocol::stats::StepKind) -> usize {
    match kind {
        crate::shared::protocol::stats::StepKind::Capture => 0,
        crate::shared::protocol::stats::StepKind::Encode => 1,
    }
}

/// Lengthen every shard's steps of a kind inside their clock, or stop
///
/// A step spun is held on its thread's own cpu, as one held by its own work is; a step slept is
/// held off its cpu, as one a preemption held is, so a test sees a node's figures tell the two
/// apart ([F130](../../../docs/src/features/loop-steps-by-kind.md)). Process wide, so a test that
/// stretches steps runs in a binary of its own or takes its turn.
///
/// # Arguments
///
/// * `kind` - The kind of step
/// * `stretch` - How long and how, none to stop
pub fn stretch_steps(kind: crate::shared::protocol::stats::StepKind, stretch: Option<(std::time::Duration, Stretch)>) {
    // the stretches first, then whether any is set, so no step reads a flag without its stretch
    let mut stretches = STEP_STRETCHES.lock().unwrap_or_else(PoisonError::into_inner);
    stretches[stretch_slot(kind)] = stretch;
    STEPS_STRETCHED.store(stretches.iter().any(Option::is_some), Ordering::Release);
}

/// Lengthen a step of a kind as a test asked, inside its clock: nothing, at one load, unless one did
///
/// # Arguments
///
/// * `kind` - The kind of step
pub fn stretch_step(kind: crate::shared::protocol::stats::StepKind) {
    // nothing stretched: one load
    if !STEPS_STRETCHED.load(Ordering::Acquire) {
        return;
    }
    let stretch = STEP_STRETCHES.lock().unwrap_or_else(PoisonError::into_inner)[stretch_slot(kind)];
    match stretch {
        // on the thread's own cpu until it has spent the time
        Some((time, Stretch::Spin)) => spin_on_cpu(u64::try_from(time.as_nanos()).unwrap_or(u64::MAX)),
        // off its cpu, as a preemption holds it
        Some((time, Stretch::Sleep)) => std::thread::sleep(time),
        // on its cpu in the kernel, a page fault at a time
        Some((_, Stretch::Touch(bytes))) => touch_fresh_pages(bytes),
        None => (),
    }
}

/// Whether a test lengthens any timed part of a capture, so the check costs one load until it does
static CAPTURE_PARTS_STRETCHED: AtomicBool = AtomicBool::new(false);

/// How each timed part of a capture is lengthened: its keys, its plan and its view, in that order
static CAPTURE_PART_STRETCHES: Mutex<[Option<(std::time::Duration, Stretch)>; 3]> = Mutex::new([None, None, None]);

/// Lengthen one timed part of every shard's captures inside its time, or stop
/// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
///
/// A capture's rest has no stretch of its own: [`stretch_steps`] of a capture lengthens it, since
/// that stretch runs inside the step after every part. Process wide, as every stretch is.
///
/// # Arguments
///
/// * `part` - The part: its keys, its plan or its view
/// * `stretch` - How long and how, none to stop
pub fn stretch_capture_part(
    part: crate::shared::protocol::stats::CapturePart,
    stretch: Option<(std::time::Duration, Stretch)>,
) {
    // the stretches first, then whether any is set, so no part reads a flag without its stretch
    let mut stretches = CAPTURE_PART_STRETCHES.lock().unwrap_or_else(PoisonError::into_inner);
    // the rest is lengthened by the step's own stretch, so it has no slot
    if let Some(slot) = stretches.get_mut(part.index()) {
        *slot = stretch;
    }
    CAPTURE_PARTS_STRETCHED.store(stretches.iter().any(Option::is_some), Ordering::Release);
}

/// Lengthen a timed part of a capture as a test asked, inside its time: nothing, at one load,
/// unless one did
///
/// # Arguments
///
/// * `part` - The part
pub fn stretch_capture(part: crate::shared::protocol::stats::CapturePart) {
    // nothing stretched: one load
    if !CAPTURE_PARTS_STRETCHED.load(Ordering::Acquire) {
        return;
    }
    let stretch = CAPTURE_PART_STRETCHES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(part.index())
        .copied()
        .flatten();
    match stretch {
        // on the thread's own cpu until it has spent the time
        Some((time, Stretch::Spin)) => spin_on_cpu(u64::try_from(time.as_nanos()).unwrap_or(u64::MAX)),
        // off its cpu, as a preemption holds it
        Some((time, Stretch::Sleep)) => std::thread::sleep(time),
        // on its cpu in the kernel, a page fault at a time
        Some((_, Stretch::Touch(bytes))) => touch_fresh_pages(bytes),
        None => (),
    }
}

/// Map fresh anonymous memory in small pages, write a byte of every page, and unmap it, each write
/// a page fault the kernel serves on the thread's own cpu
///
/// # Arguments
///
/// * `bytes` - How much memory to touch
fn touch_fresh_pages(bytes: usize) {
    // the page a fault is taken for, never a huge one: the mapping is advised against them
    const PAGE: usize = 4096;
    if bytes == 0 {
        return;
    }
    // SAFETY: an anonymous private mapping of a length the call is given, nothing else named
    let map = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if map == libc::MAP_FAILED {
        return;
    }
    // SAFETY: the range is the mapping just made; a kernel that takes no advice only faults less
    unsafe { libc::madvise(map, bytes, libc::MADV_NOHUGEPAGE) };
    // a byte of every page written, each a fault
    for offset in (0..bytes).step_by(PAGE) {
        // SAFETY: the offset is inside the mapping, which is writable and lives until unmapped
        unsafe { std::ptr::write_volatile(map.cast::<u8>().add(offset), 1) };
    }
    // SAFETY: the mapping made above, unmapped once, with nothing pointing into it after
    unsafe { libc::munmap(map, bytes) };
}

/// Spin on the thread's own cpu until it has spent a time there, by the wall if its clock is unread
///
/// # Arguments
///
/// * `time_ns` - How long to spend on the cpu, in nanoseconds
fn spin_on_cpu(time_ns: u64) {
    // the wall and the thread's own clock as the spin begins
    let began = std::time::Instant::now();
    let cpu = crate::server::step_clock::thread_cpu_ns();
    loop {
        // what the thread has spent on its cpu so far, or the wall if its clock is unread
        let spent = match (cpu, crate::server::step_clock::thread_cpu_ns()) {
            (Some(from), Some(now)) => now.saturating_sub(from),
            _ => u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX),
        };
        if spent >= time_ns {
            break;
        }
        std::hint::spin_loop();
    }
}

/// What a test says each row a piece copies costs on its thread's own cpu, in nanoseconds, or none
static PIECE_ROW_NANOS: AtomicU64 = AtomicU64::new(0);

/// Make every row an encode step's piece copies cost a time on its thread's own cpu, as rows that
/// cost more than their table's rows have shown would, or stop
/// ([item 350](../../../docs/src/appendix/resolved/encode-copy-deadline.md)). Process wide, so a
/// test that slows them runs in a binary of its own or takes its turn.
///
/// # Arguments
///
/// * `cost` - What each row copied costs, none to stop
pub fn slow_piece_rows(cost: Option<std::time::Duration>) {
    // a cost said is at least a nanosecond, so none stays zero
    let nanos = cost.map_or(0, |cost| u64::try_from(cost.as_nanos()).unwrap_or(u64::MAX).max(1));
    PIECE_ROW_NANOS.store(nanos, Ordering::Release);
}

/// What a test says each row a piece copies costs, read once a piece: one load unless it says
#[must_use]
pub fn piece_row_cost() -> Option<u64> {
    // nothing said: one load
    let nanos = PIECE_ROW_NANOS.load(Ordering::Acquire);
    (nanos > 0).then_some(nanos)
}

/// Spend what a test says one row a piece copies costs, on the thread's own cpu
///
/// # Arguments
///
/// * `nanos` - What [`piece_row_cost`] said
pub fn slow_piece_row(nanos: u64) {
    spin_on_cpu(nanos);
}

/// Whether a test holds every shard's evictions
static EVICTIONS_HELD: AtomicBool = AtomicBool::new(false);

/// Hold every shard's evictions, or let them go
///
/// A shard under memory pressure evicts nothing while held, so a test can load a partition into a
/// node that evicts everything it may and have it stay resident long enough for a scan's page to
/// plan it (M23). Process wide, so a test that holds them runs in a binary of its own.
///
/// # Arguments
///
/// * `held` - Whether evictions are held
pub fn hold_evictions(held: bool) {
    EVICTIONS_HELD.store(held, Ordering::Release);
}

/// Whether a test holds every shard's evictions, one load
#[must_use]
pub fn evictions_held() -> bool {
    EVICTIONS_HELD.load(Ordering::Acquire)
}

/// The node memory a test has every shard check the process against instead of its own, zero
/// for none
static NODE_MEMORY_OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Have every shard check the process's resident memory against a budget of a test's, or against
/// its own again
///
/// A test sets it under a running node, just above the process as it stands, so whatever the
/// process holds past it is what the test did since (M23). Process wide, so a test that sets it
/// runs in a binary of its own.
///
/// # Arguments
///
/// * `bytes` - The budget, or none for the node's own
pub fn override_node_memory(bytes: Option<usize>) {
    NODE_MEMORY_OVERRIDE.store(bytes.map_or(0, |bytes| bytes as u64), Ordering::Release);
}

/// The node memory a test set in place of the node's own, if any: one load
#[must_use]
pub fn node_memory_override() -> Option<usize> {
    match NODE_MEMORY_OVERRIDE.load(Ordering::Acquire) {
        0 => None,
        bytes => Some(usize::try_from(bytes).unwrap_or(usize::MAX)),
    }
}

/// How many tombstones merges have swept from every table's resident partitions
static TOMBSTONES_SWEPT: AtomicU64 = AtomicU64::new(0);

/// Count the tombstones a merge swept from a table's resident partitions, which a test of an
/// overlay swept under a scan's page waits on (M23)
///
/// # Arguments
///
/// * `swept` - How many it swept
pub fn count_swept(swept: usize) {
    // a flush that swept nothing costs one comparison
    if swept > 0 {
        TOMBSTONES_SWEPT.fetch_add(swept as u64, Ordering::Relaxed);
    }
}

/// How many tombstones merges have swept from resident partitions since the process started
#[must_use]
pub fn tombstones_swept() -> u64 {
    TOMBSTONES_SWEPT.load(Ordering::Relaxed)
}

/// Whether a test holds every shard's second inbox, so its steps wait as on a loop never idle
static IDLE_STEPS_HELD: AtomicBool = AtomicBool::new(false);

/// Hold every shard's second inbox, or let it go
///
/// A held inbox runs no step, as a loop whose inbox is never empty and whose forced step is never
/// due would not, so a test can change a copy under a scan's page waiting for its capture and see
/// what the page reports once it is let go ([F117](../../../docs/src/features/scan-class.md)).
///
/// # Arguments
///
/// * `held` - Whether it is held
pub fn hold_idle_steps(held: bool) {
    IDLE_STEPS_HELD.store(held, Ordering::Release);
}

/// Whether every shard's second inbox is held
#[must_use]
pub fn idle_steps_held() -> bool {
    IDLE_STEPS_HELD.load(Ordering::Acquire)
}

/// How many scan pages wait for their capture in every shard's second inbox
static CAPTURES_WAITING: AtomicU64 = AtomicU64::new(0);

/// Count a scan page queued for its capture, or one taken from the queue
///
/// A scan's admission is a few a millisecond at most, so one relaxed add each is nothing; a test
/// waits on the count rather than on a time.
///
/// # Arguments
///
/// * `queued` - Whether one was queued, or taken
pub fn count_capture(queued: bool) {
    if queued {
        CAPTURES_WAITING.fetch_add(1, Ordering::Relaxed);
    } else {
        let _ = CAPTURES_WAITING.try_update(Ordering::Relaxed, Ordering::Relaxed, |count| count.checked_sub(1));
    }
}

/// How many scan pages wait for their capture in every shard's second inbox of this process
#[must_use]
pub fn captures_waiting() -> u64 {
    CAPTURES_WAITING.load(Ordering::Relaxed)
}

/// How many of this node's next proposals of a plan's move a test makes stale
static PLAN_MOVES_TO_STALE: AtomicU64 = AtomicU64::new(0);

/// Make this node's next proposals of a plan's move lose to the topology version, as a busy
/// control log makes them lose
///
/// The leader proposes a plan's move against the version its state has applied, and every entry
/// already in the log ahead of the move that moves the version makes it stale: under load, the
/// progress of the moves in flight made every try of the next one lose (item 331). A proposal made
/// stale here is written against the version before the current one, committed, and refused by the
/// state as the race's is.
///
/// # Arguments
///
/// * `count` - How many proposals to make stale
pub fn stale_plan_moves(count: u64) {
    PLAN_MOVES_TO_STALE.store(count, Ordering::Release);
}

/// Whether the plan's move about to be proposed is one a test makes stale, counting it if so
#[must_use]
pub fn stale_this_plan_move() -> bool {
    // nothing armed: one load
    if PLAN_MOVES_TO_STALE.load(Ordering::Acquire) == 0 {
        return false;
    }
    // one of the armed proposals taken, unless another took the last
    PLAN_MOVES_TO_STALE
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

/// Whether this node's shards hold every map pushed once theirs carries a restore
static MAPS_HELD_PAST_RESTORE: AtomicBool = AtomicBool::new(false);

/// Hold this node's shards' maps once each has one that carries a restore, or let them go
///
/// A held shard keeps the map it has and the newest pushed after it, as a node catching up on
/// the control log after a partition is behind the record it drives from: a shard that led a
/// group whose install it had not seen drove it from loading, and failed it on the install's rows
/// (item 336). A shard let go installs the newest it held at its next tick. Until one is held a
/// map's install pays one load.
///
/// # Arguments
///
/// * `held` - Whether they are held
pub fn hold_maps_past_restore(held: bool) {
    MAPS_HELD_PAST_RESTORE.store(held, Ordering::Release);
}

/// Whether a test holds this node's shards' maps once theirs carries a restore
#[must_use]
pub fn maps_held_past_restore() -> bool {
    MAPS_HELD_PAST_RESTORE.load(Ordering::Acquire)
}

/// Whether any table's writes are shed, so the check costs one load until a test sheds some
static WRITES_SHED_ANY: AtomicBool = AtomicBool::new(false);

/// The tables whose writes a test sheds, by identity, each until when
static WRITES_SHED: Mutex<Vec<(u64, std::time::Instant)>> = Mutex::new(Vec::new());

/// Shed a table's writes on a standalone node for a while, or stop
///
/// A shed write is answered as one past the table's pending bound is, through the same path, so it
/// is counted and warned of by its cause: a bench's preload met a cluster that shed a write for
/// longer than its retries lasted, and nothing on a node said why
/// ([item 357](../../../docs/src/appendix/resolved/preload-sheds-waited-out.md)). Keyed by table,
/// since every test of a binary shares these statics.
///
/// # Arguments
///
/// * `table` - The table, by its identity
/// * `shed_for` - How long from now its writes are shed, or `None` to stop
pub fn shed_writes(table: u64, shed_for: Option<std::time::Duration>) {
    let mut tables = WRITES_SHED.lock().unwrap_or_else(PoisonError::into_inner);
    // at most one entry a table, there while its writes are shed
    tables.retain(|(entry, _)| *entry != table);
    if let Some(shed_for) = shed_for {
        tables.push((table, std::time::Instant::now() + shed_for));
    }
    WRITES_SHED_ANY.store(!tables.is_empty(), Ordering::Release);
}

/// Whether a table's write is to be shed now
///
/// # Arguments
///
/// * `table` - The table's identity, asked for only while some table's writes are shed
#[must_use]
pub fn writes_shed(table: impl FnOnce() -> u64) -> bool {
    // nothing shed: one load, and no identity hashed
    if !WRITES_SHED_ANY.load(Ordering::Acquire) {
        return false;
    }
    let table = table();
    let now = std::time::Instant::now();
    WRITES_SHED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|(entry, until)| *entry == table && now < *until)
}

#[cfg(test)]
mod tests {
    use super::*;
    use glommio::io::{BufferedFile, DmaFile};
    use glommio::LocalExecutor;

    /// A fresh directory
    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("a temp dir")
    }

    /// A group held from standing is held until it is let go, once a group, and the rest never
    /// are (item 142)
    #[test]
    fn a_group_held_from_standing_is_held_until_let_go() {
        // two groups held, one of them twice, and a third never
        hold_elections(0x1001, true);
        hold_elections(0x1002, true);
        hold_elections(0x1001, true);
        assert!(elections_held(0x1001));
        assert!(elections_held(0x1002));
        assert!(!elections_held(0x1003));
        // one let go: once is enough, since a hold is kept once a group
        hold_elections(0x1001, false);
        assert!(!elections_held(0x1001));
        assert!(elections_held(0x1002));
        // and the last: nothing held, which the check reads in one load
        hold_elections(0x1002, false);
        assert!(!elections_held(0x1002));
        assert!(!ELECTIONS_HELD_ANY.load(Ordering::Acquire));
    }

    /// A fault acts under its directory, by component, and nowhere beside it
    #[test]
    fn a_fault_matches_its_directory_and_nothing_beside_it() {
        let root = dir();
        let armed = root.path().join("a");
        arm(&armed, Fault::Lost);
        assert!(guard(&armed.join("deep").join("file")).is_err());
        assert!(guard(&armed).is_err());
        assert!(guard(&root.path().join("ab")).is_ok());
        assert!(guard(root.path()).is_ok());
        // the deepest fault wins where two cover a path
        arm(&armed.join("deep"), Fault::Full { budget: 7 });
        assert!(guard(&armed.join("deep").join("file")).is_ok());
        assert_eq!(free_bytes_under(&armed.join("deep").join("file")), Some(7));
        clear(&armed.join("deep"));
        clear(&armed);
        assert!(guard(&armed.join("deep")).is_ok());
        // and a fault is read as a test spells it
        assert_eq!(parse("torn 4096"), Ok(Fault::Torn { after_bytes: 4096 }));
        assert_eq!(parse("full 1"), Ok(Fault::Full { budget: 1 }));
        assert_eq!(parse("lost"), Ok(Fault::Lost));
        assert!(parse("full").is_err() && parse("melt").is_err());
    }

    /// A full directory counts what its files grow by, not every byte written, and refuses the
    /// write that would pass its budget
    #[test]
    fn full_counts_growth_since_arming() {
        let root = dir();
        let path = root.path().join("file");
        std::fs::write(&path, vec![0u8; 100]).expect("a file");
        arm(root.path(), Fault::Full { budget: 50 });
        LocalExecutor::default().run(async {
            let file = glommio::io::OpenOptions::new()
                .read(true)
                .write(true)
                .buffered_open(&path)
                .await
                .expect("the file opens");
            // rewriting what is there grows nothing
            file.write_at(vec![1u8; 100], 0).await.expect("a rewrite");
            // forty bytes past the end grow the file by forty, which fits
            file.write_at(vec![2u8; 40], 100)
                .await
                .expect("growth within the budget");
            assert_eq!(free_bytes_under(&path), Some(10));
            // and twenty more do not
            let refused = file
                .write_at(vec![3u8; 20], 140)
                .await
                .expect_err("past the budget");
            assert!(format!("{refused}").contains("No space left"), "{refused}");
            file.close().await.expect("the file closes");
        });
        clear(root.path());
    }

    /// A lost directory fails every operation, on a handle opened before it was lost too
    #[test]
    fn lost_fails_every_operation() {
        let root = dir();
        let path = root.path().join("file");
        LocalExecutor::default().run(async {
            let file = BufferedFile::create(&path).await.expect("the file is made");
            file.write_at(vec![1u8; 8], 0)
                .await
                .expect("a write before the loss");
            arm(root.path(), Fault::Lost);
            assert!(file.write_at(vec![2u8; 8], 8).await.is_err(), "a write");
            assert!(file.read_at(0, 8).await.is_err(), "a read");
            assert!(file.fdatasync().await.is_err(), "a sync");
            assert!(BufferedFile::open(&path).await.is_err(), "an open");
            assert!(glommio::io::remove(&path).await.is_err(), "a remove");
            clear(root.path());
            file.close()
                .await
                .expect("the file closes once the device is back");
        });
    }

    /// A torn write keeps its start, synced, fails, and tears nothing after it
    #[test]
    fn torn_cuts_one_write_once() {
        // a tear here fails the write rather than ending the test binary
        EXIT_ON_TEAR.store(false, Ordering::Relaxed);
        let root = dir();
        let path = root.path().join("file");
        LocalExecutor::default().run(async {
            arm(root.path(), Fault::Torn { after_bytes: 30 });
            let file = BufferedFile::create(&path).await.expect("the file is made");
            // twenty bytes whole, then a write of twenty torn after ten
            file.write_at(vec![1u8; 20], 0)
                .await
                .expect("a whole write");
            assert!(
                file.write_at(vec![2u8; 20], 20).await.is_err(),
                "the torn write"
            );
            let on_disk = std::fs::read(&path).expect("the file reads");
            assert_eq!(on_disk.len(), 30);
            assert!(on_disk[20..].iter().all(|byte| *byte == 2));
            // and the next write is whole again
            file.write_at(vec![3u8; 5], 30)
                .await
                .expect("a write after the tear");
            file.close().await.expect("the file closes");
            // a direct write is torn on a block boundary
            let direct = root.path().join("direct");
            let file = DmaFile::create(&direct).await.expect("the file is made");
            let align = file.alignment() as usize;
            arm(
                root.path(),
                Fault::Torn {
                    after_bytes: (align * 2 + 3) as u64,
                },
            );
            let mut buf = file.alloc_dma_buffer(align * 4);
            buf.as_bytes_mut().fill(9);
            assert!(
                file.write_at(buf, 0).await.is_err(),
                "the torn direct write"
            );
            let kept = std::fs::read(&direct).expect("the file reads").len();
            assert_eq!(
                kept,
                align * 2,
                "the tear did not keep whole blocks of {align}"
            );
            file.close().await.expect("the file closes");
        });
        clear(root.path());
    }
}
