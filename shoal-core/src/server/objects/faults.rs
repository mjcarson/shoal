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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
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
