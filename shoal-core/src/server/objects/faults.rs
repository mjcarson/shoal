//! What a test can make a stripe write do: crash at a step, hold its stager, mint a forced id
//!
//! Every hook here is idle unless a test arms it, and an idle hook costs one atomic load
//! ([F86](../../../../docs/src/features/replicated-pools.md)). The crash points are S16's
//! ([S16](../../../../docs/src/object-storage/testing.md#the-fixture)): a coordinator, a holder or
//! the leader killed at each step of a stripe write leaves the stripe at its old state or its new
//! one on every chunk read.
//!
//! # Invariants
//!
//! **A crash point exits the process, never a task**: what a crash leaves is what the devices and
//! the logs held at that instant, which is the only state a restart can find.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// A step of a stripe write a test can end the process at
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StripeCrashPoint {
    /// No crash point is armed
    None = 0,
    /// A holder, once a stage is synced and before it answers
    StageSynced = 1,
    /// The coordinator, once its holders answered and before it proposes
    Answered = 2,
    /// The coordinator, once its commit is sent and before its answer comes
    Proposed = 3,
    /// The coordinator, once its commit applied and before it tells any holder
    Committed = 4,
    /// A holder, half way through writing an apply's units in place
    MidApply = 5,
    /// A holder, once an apply is synced and before its staged copy is dropped
    AppliedNotDropped = 6,
}

impl StripeCrashPoint {
    /// Every crash point, in the order a stripe write passes them
    pub const ALL: [StripeCrashPoint; 6] = [
        StripeCrashPoint::StageSynced,
        StripeCrashPoint::Answered,
        StripeCrashPoint::Proposed,
        StripeCrashPoint::Committed,
        StripeCrashPoint::MidApply,
        StripeCrashPoint::AppliedNotDropped,
    ];

    /// The point's name, as a test arms it
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            StripeCrashPoint::None => "none",
            StripeCrashPoint::StageSynced => "stage_synced",
            StripeCrashPoint::Answered => "answered",
            StripeCrashPoint::Proposed => "proposed",
            StripeCrashPoint::Committed => "committed",
            StripeCrashPoint::MidApply => "mid_apply",
            StripeCrashPoint::AppliedNotDropped => "applied_not_dropped",
        }
    }

    /// The point a name names
    ///
    /// # Arguments
    ///
    /// * `name` - The name
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|point| point.name() == name)
    }
}

/// The crash point armed, as its discriminant
static CRASH_AT: AtomicU8 = AtomicU8::new(0);

/// Arm a crash point: the next stripe write to reach it ends the process
///
/// # Arguments
///
/// * `point` - The point
pub fn crash_at(point: StripeCrashPoint) {
    CRASH_AT.store(point as u8, Ordering::Release);
}

/// End the process if a stripe write reached the armed crash point
///
/// # Arguments
///
/// * `point` - The point reached
pub fn hit(point: StripeCrashPoint) {
    // one load when nothing is armed
    if CRASH_AT.load(Ordering::Acquire) == point as u8 && point != StripeCrashPoint::None {
        tracing::event!(tracing::Level::ERROR, msg = "a stripe write reached its armed crash point", point = point.name());
        std::process::exit(137);
    }
}

/// How long one arm of a rotational device is held for each apply and each read, in microseconds;
/// zero for none
static SLOW_ARM_US: AtomicU64 = AtomicU64::new(0);

/// Stand one disk arm in for every rotational slice, or stop with zero: each apply and each read
/// of such a slice takes the slice's one arm for this long before its I/O, in arrival order, as a
/// disk's seek and revolution would hold its one arm (F91)
///
/// What lets a test hold a batch of applies to its bound and a read to one batch on a directory
/// that is not a disk.
///
/// # Arguments
///
/// * `hold` - How long each operation holds the arm
pub fn slow_arm(hold: Duration) {
    SLOW_ARM_US.store(u64::try_from(hold.as_micros()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long each operation holds a rotational slice's arm, if a test stands one in
#[must_use]
pub fn arm_hold() -> Option<Duration> {
    match SLOW_ARM_US.load(Ordering::Acquire) {
        0 => None,
        micros => Some(Duration::from_micros(micros)),
    }
}

/// How long a stager waits after its stages and before it proposes, in milliseconds
static HOLD_STAGER_MS: AtomicU64 = AtomicU64::new(0);

/// Hold every stager between its stages and its commit, as a stager paused across a leader
/// change or a timeout would be
///
/// # Arguments
///
/// * `hold` - How long; zero holds none
pub fn hold_stager(hold: Duration) {
    HOLD_STAGER_MS.store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long a stager is held, if a test holds one
#[must_use]
pub fn stager_hold() -> Option<Duration> {
    match HOLD_STAGER_MS.load(Ordering::Acquire) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    }
}

/// How many stagers a test's hold is holding now
static STAGERS_HELD: AtomicU64 = AtomicU64::new(0);

/// A stager counted as held for as long as this lives, so one cancelled in its hold is let go too
struct StagerHeld;

impl StagerHeld {
    /// Count a stager as held
    fn new() -> Self {
        STAGERS_HELD.fetch_add(1, Ordering::AcqRel);
        StagerHeld
    }
}

impl Drop for StagerHeld {
    /// Count the stager let go
    fn drop(&mut self) {
        STAGERS_HELD.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Hold a stager for a test's hold, counted as held while it waits
///
/// A test that races a held writer waits for the writer to be counted here, never for a time: a
/// writer a loaded host had not brought to its hold yet wrote after what the test raced it with
/// (item 332).
///
/// # Arguments
///
/// * `hold` - How long
pub async fn hold_this_stager(hold: Duration) {
    // counted by a guard its end drops, never by a pair of calls
    let _held = StagerHeld::new();
    glommio::timer::sleep(hold).await;
}

/// How many stagers a test's hold is holding now
#[must_use]
pub fn stagers_held() -> u64 {
    STAGERS_HELD.load(Ordering::Acquire)
}

/// How long a rebuild is held between its copy and its commit, in milliseconds; zero for none
static HOLD_REBUILD_MS: AtomicU64 = AtomicU64::new(0);

/// Hold every rebuild between writing its holder and committing its position current, or stop
/// holding them with zero: what lets a test land a write between the two (F88)
///
/// # Arguments
///
/// * `hold` - How long
pub fn hold_rebuild(hold: Duration) {
    HOLD_REBUILD_MS.store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long a rebuild is held, if a test holds one
#[must_use]
pub fn rebuild_hold() -> Option<Duration> {
    match HOLD_REBUILD_MS.load(Ordering::Acquire) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    }
}

/// Whether every row a write that fell short reads again is taken for one that could not be read
static ROW_REREADS_FAIL: AtomicBool = AtomicBool::new(false);

/// Take every row a write that fell short reads again for one that could not be read, or stop
///
/// A leader change or a deadline under load fails that read, which is what lets a test find what
/// a write that fell short answers when it cannot tell whether its row moved
/// ([Resolved #262](../../../../docs/src/appendix/resolved/behind-asked-of-holders-first.md)).
///
/// # Arguments
///
/// * `fail` - Whether to fail them
pub fn fail_row_rereads(fail: bool) {
    ROW_REREADS_FAIL.store(fail, Ordering::Release);
}

/// Whether a test fails every row a write that fell short reads again
#[must_use]
pub fn row_rereads_fail() -> bool {
    ROW_REREADS_FAIL.load(Ordering::Acquire)
}

/// How many more of this node's archive reads a test refuses, as a group that cannot confirm its
/// leader refuses them (item 322)
static ARCHIVE_REFUSALS: AtomicU32 = AtomicU32::new(0);

/// Refuse this node's next archive reads, one an object a backup archives, as a strong read is
/// refused while its group cannot confirm its leader (item 322)
///
/// # Arguments
///
/// * `count` - How many to refuse
pub fn refuse_archive_reads(count: u32) {
    ARCHIVE_REFUSALS.store(count, Ordering::Release);
}

/// Whether a test refuses this archive read, taking one of the refusals it asked for
#[must_use]
pub fn archive_read_refused() -> bool {
    // nothing refused: no, after one load
    if ARCHIVE_REFUSALS.load(Ordering::Acquire) == 0 {
        return false;
    }
    ARCHIVE_REFUSALS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .is_ok()
}

/// How many more of this node's scrubs' reads of a chunk's owner a test refuses, as a strong read is
/// refused while its group cannot confirm its leader (item 330)
static SCRUB_OWNER_REFUSALS: AtomicU32 = AtomicU32::new(0);

/// Refuse this node's next scrub reads of a row-less chunk's owner for now, as a loaded host's
/// strong read of the owner's entry is refused (item 330)
///
/// # Arguments
///
/// * `count` - How many to refuse
pub fn refuse_scrub_owner_reads(count: u32) {
    SCRUB_OWNER_REFUSALS.store(count, Ordering::Release);
}

/// Whether a test refuses this scrub read of an owner, taking one of the refusals it asked for
#[must_use]
pub fn scrub_owner_read_refused() -> bool {
    // nothing refused: no, after one load
    if SCRUB_OWNER_REFUSALS.load(Ordering::Acquire) == 0 {
        return false;
    }
    SCRUB_OWNER_REFUSALS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .is_ok()
}

/// How many more of this node's cuts of a table group's backup a test refuses (item 322)
static BACKUP_CUT_REFUSALS: AtomicU32 = AtomicU32::new(0);

/// Whether the cuts a test refuses are refused for now, so their backup is tried again, or for
/// good
static BACKUP_CUTS_FOR_NOW: AtomicBool = AtomicBool::new(true);

/// Refuse this node's next cuts of a table group's backup, for now as a cut that did not land in
/// time is, or for good (item 322)
///
/// # Arguments
///
/// * `count` - How many to refuse
/// * `for_now` - Whether a backup refused so may be tried again
pub fn refuse_backup_cuts(count: u32, for_now: bool) {
    BACKUP_CUTS_FOR_NOW.store(for_now, Ordering::Release);
    BACKUP_CUT_REFUSALS.store(count, Ordering::Release);
}

/// Whether a test refuses this cut of a table group's backup, taking one of the refusals it asked
/// for: `Some(true)` for now, `Some(false)` for good
#[must_use]
pub fn backup_cut_refused() -> Option<bool> {
    // nothing refused: no, after one load
    if BACKUP_CUT_REFUSALS.load(Ordering::Acquire) == 0 {
        return None;
    }
    BACKUP_CUT_REFUSALS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .ok()
        .map(|_| BACKUP_CUTS_FOR_NOW.load(Ordering::Acquire))
}

/// How many more of this node's group restores a test hands back after their `Verifying` commit
/// (item 327)
static RESTORE_HAND_BACKS: AtomicU32 = AtomicU32::new(0);

/// Hand this node's next group restores back right after their `Verifying` commit, as a driver
/// that loses its lead leaves its group, so another drive finishes them from `Verifying`
/// (item 327)
///
/// Returns how many hand-backs were still asked for before this call, so a test can tell how many
/// it took.
///
/// # Arguments
///
/// * `count` - How many to hand back
pub fn hand_back_restores(count: u32) -> u32 {
    RESTORE_HAND_BACKS.swap(count, Ordering::AcqRel)
}

/// What a test does to a restore's verifying scrub on this node (item 297)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyingFault {
    /// A member's report is missing, as one that did not answer within the scrub's time leaves it
    MissingReport,
    /// The scrub's proposal fails, as a handle stopped under a restart fails it
    StoppedHandle,
}

/// Which fault a test puts on this node's verifying scrubs: 1 a missing report, 2 a stopped
/// handle (item 297)
static VERIFYING_FAULT: AtomicU8 = AtomicU8::new(0);

/// How many more of this node's verifying scrubs a test puts its fault on (item 297)
static VERIFYING_FAULTS: AtomicU32 = AtomicU32::new(0);

/// Put a fault on this node's next verifying scrubs of a restore (item 297)
///
/// # Arguments
///
/// * `count` - How many scrubs to put it on
/// * `fault` - What to do to each
pub fn fail_verifying_scrubs(count: u32, fault: VerifyingFault) {
    // the kind first, so a scrub that takes a count never reads the old one
    let kind = match fault {
        VerifyingFault::MissingReport => 1,
        VerifyingFault::StoppedHandle => 2,
    };
    VERIFYING_FAULT.store(kind, Ordering::Release);
    VERIFYING_FAULTS.store(count, Ordering::Release);
}

/// The fault a test puts on this verifying scrub, taking one of the scrubs it asked for
#[must_use]
pub fn verifying_fault() -> Option<VerifyingFault> {
    // nothing asked for: none, after one load
    if VERIFYING_FAULTS.load(Ordering::Acquire) == 0 {
        return None;
    }
    VERIFYING_FAULTS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .ok()?;
    match VERIFYING_FAULT.load(Ordering::Acquire) {
        1 => Some(VerifyingFault::MissingReport),
        2 => Some(VerifyingFault::StoppedHandle),
        _ => None,
    }
}

/// How many more of this node's repair and restore installs a test has find their group
/// installing a snapshot already (item 297)
static INSTALLS_FOUND_BUSY: AtomicU32 = AtomicU32::new(0);

/// Have this node's next repair and restore installs find their group installing a snapshot
/// already, as a fresh copy caught up by a snapshot of its own is when a restore's file arrives
/// (item 297)
///
/// # Arguments
///
/// * `count` - How many installs find it so
pub fn find_installs_busy(count: u32) {
    INSTALLS_FOUND_BUSY.store(count, Ordering::Release);
}

/// Whether a test has this install find its group installing a snapshot already, taking one of
/// the installs it asked for
#[must_use]
pub fn install_found_busy() -> bool {
    // nothing asked for: no, after one load
    if INSTALLS_FOUND_BUSY.load(Ordering::Acquire) == 0 {
        return false;
    }
    INSTALLS_FOUND_BUSY
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .is_ok()
}

/// How many more snapshot streams this node sends a test holds before their chunks, and for how
/// many milliseconds (item 293)
static STREAM_HOLDS: AtomicU32 = AtomicU32::new(0);

/// How long each stream a test holds waits before its chunks
static STREAM_HOLD_MS: AtomicU64 = AtomicU64::new(0);

/// Hold this node's next snapshot streams after their begin is taken and before their chunks, so
/// each stays short of its bytes, assembling on its receiver against its stream budget, while it
/// waits (item 293)
///
/// # Arguments
///
/// * `count` - How many streams to hold
/// * `ms` - How long each waits
pub fn hold_streams(count: u32, ms: u64) {
    // the length first, so a stream that takes a count never reads the old one
    STREAM_HOLD_MS.store(ms, Ordering::Release);
    STREAM_HOLDS.store(count, Ordering::Release);
}

/// How long a test holds this stream before its chunks, taking one of the holds it asked for
#[must_use]
pub fn stream_held() -> Option<std::time::Duration> {
    // nothing asked for: none, after one load
    if STREAM_HOLDS.load(Ordering::Acquire) == 0 {
        return None;
    }
    STREAM_HOLDS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .ok()?;
    Some(std::time::Duration::from_millis(STREAM_HOLD_MS.load(Ordering::Acquire)))
}

/// How many more messages this node's shard loops take longer over, and by how many microseconds
/// each (item 284)
static SLOW_MESSAGES: AtomicU32 = AtomicU32::new(0);

/// How long each slowed message takes beyond its work
static SLOW_MESSAGE_US: AtomicU64 = AtomicU64::new(0);

/// Have this node's shard loops take longer over each of their next messages, the thread held
/// without yielding as a loaded host's slower work holds it: what lets a test make a burst of
/// writes keep a shard's loop busy past a hop's silence bound (item 284)
///
/// # Arguments
///
/// * `count` - How many messages to slow
/// * `micros` - How much longer each takes
pub fn slow_messages(count: u32, micros: u64) {
    // the length first, so a message that takes a count never reads the old one
    SLOW_MESSAGE_US.store(micros, Ordering::Release);
    SLOW_MESSAGES.store(count, Ordering::Release);
}

/// How much longer a test has this shard loop take over the message it just handled, taking one
/// of the messages it asked for
#[must_use]
pub fn message_slowed() -> Option<std::time::Duration> {
    // nothing asked for: none, after one load
    if SLOW_MESSAGES.load(Ordering::Acquire) == 0 {
        return None;
    }
    SLOW_MESSAGES
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .ok()?;
    Some(std::time::Duration::from_micros(SLOW_MESSAGE_US.load(Ordering::Acquire)))
}

/// How many more of this node's moves a test has find their arrivals down at their first look
/// before the switch (item 281)
static ARRIVALS_DOWN: AtomicU32 = AtomicU32::new(0);

/// Have this node's next moves find their arrivals down at their first look before the switch, as
/// a node the leader's detector calls down for a moment under load is found (item 281)
///
/// # Arguments
///
/// * `count` - How many moves' first looks find it so
pub fn find_arrivals_down(count: u32) {
    ARRIVALS_DOWN.store(count, Ordering::Release);
}

/// Whether a test has this move's first look before its switch find its arrivals down, taking one
/// of the looks it asked for
#[must_use]
pub fn arrivals_found_down() -> bool {
    // nothing asked for: no, after one load
    if ARRIVALS_DOWN.load(Ordering::Acquire) == 0 {
        return false;
    }
    ARRIVALS_DOWN
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .is_ok()
}

/// Whether a test hands this group restore back after its `Verifying` commit, taking one of the
/// hand-backs it asked for
#[must_use]
pub fn restore_handed_back() -> bool {
    // nothing asked for: no, after one load
    if RESTORE_HAND_BACKS.load(Ordering::Acquire) == 0 {
        return false;
    }
    RESTORE_HAND_BACKS
        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
        .is_ok()
}

/// Whether every recovery and move driver on this node is paused
static RECOVERY_PAUSED: AtomicBool = AtomicBool::new(false);

/// Pause every rebuild and move driver on this node, or let them go on: what lets a test drive a
/// move by hand and then watch a rebuild finish it (F88)
///
/// # Arguments
///
/// * `paused` - Whether they are paused
pub fn pause_recovery(paused: bool) {
    RECOVERY_PAUSED.store(paused, Ordering::Release);
}

/// Whether a test paused this node's rebuild and move drivers
#[must_use]
pub fn recovery_paused() -> bool {
    RECOVERY_PAUSED.load(Ordering::Acquire)
}

/// How long a scrub's driver is held after each placement group it finishes, in milliseconds;
/// zero for none
static HOLD_SCRUB_MS: AtomicU64 = AtomicU64::new(0);

/// Hold every scrub driver on this node after each placement group it finishes, or stop holding
/// them with zero: what lets a test kill a leader in the middle of a pass (F89)
///
/// # Arguments
///
/// * `hold` - How long
pub fn hold_scrub(hold: Duration) {
    HOLD_SCRUB_MS.store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long a scrub is held after each placement group, if a test holds one
#[must_use]
pub fn scrub_hold() -> Option<Duration> {
    match HOLD_SCRUB_MS.load(Ordering::Acquire) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    }
}

/// Whether every scrub driver on this node is paused
static SCRUB_PAUSED: AtomicBool = AtomicBool::new(false);

/// Pause every scrub driver on this node, or let them go on: what lets a test arrange damage
/// before anything scrubs it (F89)
///
/// # Arguments
///
/// * `paused` - Whether they are paused
pub fn pause_scrub(paused: bool) {
    SCRUB_PAUSED.store(paused, Ordering::Release);
}

/// Whether a test paused this node's scrub drivers
#[must_use]
pub fn scrub_paused() -> bool {
    SCRUB_PAUSED.load(Ordering::Acquire)
}

/// Whether every reclamation driver on this node is paused (M20)
static RECLAIM_PAUSED: AtomicBool = AtomicBool::new(false);

/// Pause every reclamation driver on this node, or let them go on: what lets a test leave a retired
/// object where it is, or a moved group's old holders, and look at them first
///
/// # Arguments
///
/// * `paused` - Whether they are paused
pub fn pause_reclaim(paused: bool) {
    RECLAIM_PAUSED.store(paused, Ordering::Release);
}

/// Whether a test paused this node's reclamation drivers
#[must_use]
pub fn reclaim_paused() -> bool {
    RECLAIM_PAUSED.load(Ordering::Acquire)
}

/// How long a reclamation is held between its seal and its strong read, in milliseconds; zero for
/// none
static HOLD_RECLAIM_MS: AtomicU64 = AtomicU64::new(0);

/// Hold every reclamation between sealing its holders and reading the fact it removes on, or stop
/// holding them with zero: what lets a test race a write against a seal (M20)
///
/// # Arguments
///
/// * `hold` - How long
pub fn hold_reclaim(hold: Duration) {
    HOLD_RECLAIM_MS.store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long a reclamation is held after its seal, if a test holds one
#[must_use]
pub fn reclaim_hold() -> Option<Duration> {
    match HOLD_RECLAIM_MS.load(Ordering::Acquire) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    }
}

/// How long a striped read is held between its entry and its stripes, in milliseconds; zero for
/// none
static HOLD_READ_MS: AtomicU64 = AtomicU64::new(0);

/// Hold every striped read between reading its object's entry and reading its stripes, or let
/// them go with zero: what lets a test replace and reclaim an object under a reader (M20)
///
/// # Arguments
///
/// * `hold` - How long at most
pub fn hold_read(hold: Duration) {
    HOLD_READ_MS.store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Release);
}

/// How long a striped read is held after its entry, if a test holds one
#[must_use]
pub fn read_hold() -> Option<Duration> {
    match HOLD_READ_MS.load(Ordering::Acquire) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    }
}

/// How many times a test asked every reclamation driver to pass now, past its grace
static RECLAIM_NOW: AtomicU64 = AtomicU64::new(0);

/// Ask every reclamation driver on this node for a pass now, every retired object taken past its
/// grace: what a test that does not wait five minutes asks (M20)
pub fn reclaim_now() {
    RECLAIM_NOW.fetch_add(1, Ordering::AcqRel);
}

/// How many times a test asked for a pass now, which a driver compares with what it saw
#[must_use]
pub fn reclaim_asked() -> u64 {
    RECLAIM_NOW.load(Ordering::Acquire)
}

/// Whether any put's bytes are held unread (item 309)
static FEEDS_HELD: AtomicBool = AtomicBool::new(false);

/// How long the puts of each path prefix hold their bytes unread before their pipelines start
static HELD_FEEDS: Mutex<Vec<(String, Duration)>> = Mutex::new(Vec::new());

/// Hold the bytes of every put of a path under a prefix unread for a while before its pipeline
/// starts, as a put whose holders take none of them would (item 309)
///
/// # Arguments
///
/// * `prefix` - The paths' prefix
/// * `hold` - How long each put holds its bytes
pub fn hold_feed(prefix: &str, hold: Duration) {
    let mut held = HELD_FEEDS.lock().unwrap_or_else(PoisonError::into_inner);
    held.retain(|(other, _)| other != prefix);
    held.push((prefix.to_string(), hold));
    FEEDS_HELD.store(true, Ordering::Release);
}

/// Stop holding any put's bytes
pub fn clear_feed_holds() {
    HELD_FEEDS.lock().unwrap_or_else(PoisonError::into_inner).clear();
    FEEDS_HELD.store(false, Ordering::Release);
}

/// How long a put of a path holds its bytes unread before its pipeline starts, if a test asked
///
/// # Arguments
///
/// * `path` - The put's path
#[must_use]
pub fn feed_hold(path: &str) -> Option<Duration> {
    // nothing held: none, after one load
    if !FEEDS_HELD.load(Ordering::Acquire) {
        return None;
    }
    let held = HELD_FEEDS.lock().unwrap_or_else(PoisonError::into_inner);
    held.iter()
        .find(|(prefix, _)| path.starts_with(prefix.as_str()))
        .map(|(_, hold)| *hold)
}

/// How many puts a test's feed hold is holding now
static FEEDS_HOLDING: AtomicU64 = AtomicU64::new(0);

/// A put counted as held at its feed for as long as this lives, so one cancelled in its hold is let
/// go too
struct FeedHeld;

impl FeedHeld {
    /// Count a put as held
    fn new() -> Self {
        FEEDS_HOLDING.fetch_add(1, Ordering::AcqRel);
        FeedHeld
    }
}

impl Drop for FeedHeld {
    /// Count the put let go
    fn drop(&mut self) {
        FEEDS_HOLDING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Hold a put's bytes unread for a test's feed hold, counted as held while it waits
///
/// A test that races a held put waits for the put to be counted here, never for a time: the put
/// has placed every stripe and written nothing (item 250).
///
/// # Arguments
///
/// * `hold` - How long
pub async fn hold_this_feed(hold: Duration) {
    // counted by a guard its end drops, never by a pair of calls
    let _held = FeedHeld::new();
    glommio::timer::sleep(hold).await;
}

/// How many puts a test's feed hold is holding now
#[must_use]
pub fn feeds_holding() -> u64 {
    FEEDS_HOLDING.load(Ordering::Acquire)
}

/// Whether this node's move drivers ask the control group for no admission, as a cluster whose
/// every arrival's slot is taken admits none (item 250)
static ADMISSION_HELD: AtomicBool = AtomicBool::new(false);

/// Hold this node's move drivers from asking the control group to admit a move, or let them ask
/// again
///
/// # Arguments
///
/// * `held` - Whether they are held
pub fn hold_admission(held: bool) {
    ADMISSION_HELD.store(held, Ordering::Release);
}

/// Whether a test holds this node's move drivers from asking for admission
#[must_use]
pub fn admission_held() -> bool {
    ADMISSION_HELD.load(Ordering::Acquire)
}

/// Whether any write's stripe commits are refused (item 312)
static COMMITS_REFUSED: AtomicBool = AtomicBool::new(false);

/// How many more stripe commits of the writes of each path prefix are refused before they are
/// proposed
static REFUSED_COMMITS: Mutex<Vec<(String, u64)>> = Mutex::new(Vec::new());

/// Refuse the next stripe commits of the writes of every path under a prefix before they are
/// proposed, as a commit another writer's beat on the sequence read is refused: what lets a test
/// make one write lose its stripe more times than any count of rounds allows (item 312)
///
/// # Arguments
///
/// * `prefix` - The paths' prefix
/// * `count` - How many commits to refuse, zero to stop
pub fn refuse_stripe_commits(prefix: &str, count: u64) {
    let mut refused = REFUSED_COMMITS.lock().unwrap_or_else(PoisonError::into_inner);
    refused.retain(|(other, _)| other != prefix);
    if count > 0 {
        refused.push((prefix.to_string(), count));
    }
    COMMITS_REFUSED.store(!refused.is_empty(), Ordering::Release);
}

/// Whether a stripe commit of a write of this path is to be refused, counting it if so
///
/// # Arguments
///
/// * `path` - The write's path
#[must_use]
pub fn stripe_commit_refused(path: &str) -> bool {
    // nothing refused: no, after one load
    if !COMMITS_REFUSED.load(Ordering::Acquire) {
        return false;
    }
    let mut refused = REFUSED_COMMITS.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(left) = refused
        .iter_mut()
        .find(|(prefix, left)| *left > 0 && path.starts_with(prefix.as_str()))
        .map(|(_, left)| left)
    else {
        return false;
    };
    // one fewer to refuse
    *left -= 1;
    true
}

/// How many stripe commits of the writes under a prefix are still to be refused
///
/// # Arguments
///
/// * `prefix` - The paths' prefix
#[must_use]
pub fn stripe_commits_to_refuse(prefix: &str) -> u64 {
    let refused = REFUSED_COMMITS.lock().unwrap_or_else(PoisonError::into_inner);
    refused
        .iter()
        .find(|(other, _)| other == prefix)
        .map_or(0, |(_, left)| *left)
}

/// Whether any slice's answers are held
static ANSWERS_HELD: AtomicBool = AtomicBool::new(false);

/// The slices whose served lanes hold their answers unwritten
static HELD_ANSWERS: Mutex<Vec<crate::shared::devices::SliceId>> = Mutex::new(Vec::new());

/// Hold the answers every lane serving a slice owes, unwritten, until let go, so a test can have
/// many ready at once (F111)
///
/// # Arguments
///
/// * `slice` - The slice
/// * `held` - Whether its answers are held from now on
pub fn hold_answers(slice: crate::shared::devices::SliceId, held: bool) {
    let mut slices = HELD_ANSWERS.lock().unwrap_or_else(PoisonError::into_inner);
    slices.retain(|other| *other != slice);
    if held {
        slices.push(slice);
    }
    ANSWERS_HELD.store(!slices.is_empty(), Ordering::Release);
}

/// Whether a test holds the answers of lanes serving a slice
///
/// # Arguments
///
/// * `slice` - The slice
#[must_use]
pub fn answers_held(slice: crate::shared::devices::SliceId) -> bool {
    // nothing held: no, after one load
    if !ANSWERS_HELD.load(Ordering::Acquire) {
        return false;
    }
    HELD_ANSWERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&slice)
}

/// Whether any object id is forced
static IDS_FORCED: AtomicBool = AtomicBool::new(false);

/// The object id forced for each path prefix
static FORCED_IDS: Mutex<Vec<(String, u128)>> = Mutex::new(Vec::new());

/// Mint one object id for every put of a path under a prefix, so two buckets' objects share one
///
/// # Arguments
///
/// * `prefix` - The paths' prefix
/// * `id` - The id they are all minted as
pub fn force_object_id(prefix: &str, id: u128) {
    let mut forced = FORCED_IDS.lock().unwrap_or_else(PoisonError::into_inner);
    forced.retain(|(held, _)| held != prefix);
    forced.push((prefix.to_string(), id));
    IDS_FORCED.store(true, Ordering::Release);
}

/// Stop forcing every object id
pub fn clear_object_ids() {
    FORCED_IDS.lock().unwrap_or_else(PoisonError::into_inner).clear();
    IDS_FORCED.store(false, Ordering::Release);
}

/// The id a put of a path mints: from its identity and its path, or the one a test forced
///
/// # Arguments
///
/// * `op` - The put's identity
/// * `path` - The object's path
#[must_use]
pub fn object_id(op: &uuid::Uuid, path: &str) -> u128 {
    // nothing forced: minted, after one load
    if !IDS_FORCED.load(Ordering::Acquire) {
        return crate::shared::objects::object_id(op, path);
    }
    let forced = FORCED_IDS.lock().unwrap_or_else(PoisonError::into_inner);
    forced
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix.as_str()))
        .max_by_key(|(prefix, _)| prefix.len())
        .map_or_else(|| crate::shared::objects::object_id(op, path), |(_, id)| *id)
}

/// Stall every object lane's reader on this node, as a peer that stops reading would
static LANES_STALLED: AtomicBool = AtomicBool::new(false);

/// Stall, or release, every object lane this node serves
///
/// # Arguments
///
/// * `stalled` - Whether they stop reading
pub fn stall_lanes(stalled: bool) {
    LANES_STALLED.store(stalled, Ordering::Release);
}

/// Whether this node's object lanes are stalled
#[must_use]
pub fn lanes_stalled() -> bool {
    LANES_STALLED.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::StripeCrashPoint;

    /// Every crash point's name reads back as the point, and nothing else does
    #[test]
    fn crash_points_are_named() {
        for point in StripeCrashPoint::ALL {
            assert_eq!(StripeCrashPoint::from_name(point.name()), Some(point));
        }
        assert_eq!(StripeCrashPoint::from_name("nowhere"), None);
        assert_eq!(StripeCrashPoint::from_name("none"), None);
    }
}
