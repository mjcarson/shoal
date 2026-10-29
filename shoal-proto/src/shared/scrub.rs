//! A pool's scrubs: how often its placement groups are read on purpose, where each tablet group's
//! passes are, and what they found ([S11](../../../docs/src/object-storage/scrub.md),
//! [F89](../../../docs/src/features/scrub-and-repair.md))
//!
//! Damage is caught in three places. Every read verifies the units it touches. A **light** scrub
//! reads no data: each holder lists what it holds of a placement group and the listings are
//! compared with the rows and with each other, which finds a chunk that is missing, a label that
//! is stale, and a chunk nothing names. A **deep** scrub reads every unit of every chunk where it
//! lies and verifies it, and compares the copies' checksum tables, which finds damage in bytes
//! nobody asked for.
//!
//! A scrub is driven for each tablet group of a bucket's stripe table by the group's leader, one
//! placement group at a time in key order, and each placement group it finishes is committed as
//! the group's cursor moves ([`ScrubCursor`]): a new leader resumes where the last one stopped.
//! The cursor lives in the group's placement state, so it is persisted as of the checkpoint and
//! carried by a snapshot as the rest of that state is.
//!
//! # Invariants
//!
//! **A pass finishes every placement group of its tablet group, in key order.** A step that does
//! not follow the cursor is refused, so no placement group is skipped and none is counted twice.
//!
//! **Nothing here decides what is current.** A time is a driver's wall clock, read to schedule a
//! pass and to report when one finished. Which bytes are current is a row's label, and what a
//! scrub finds stale or corrupt is made current again by the rebuild path, through the row.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::placement_state::PgKey;

/// A span of time written as a person writes one: `90s`, `30m`, `12h`, `7d`
///
/// Whole seconds, since nothing scheduled here is finer: a scrub's interval, a window's length, a
/// device's count of checksum failures. A bare number is refused, as `DurationSpec` refuses one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Span(pub u64);

impl Span {
    /// A span of whole seconds
    ///
    /// # Arguments
    ///
    /// * `seconds` - How many
    #[must_use]
    pub const fn seconds(seconds: u64) -> Self {
        Span(seconds)
    }

    /// The span in milliseconds
    #[must_use]
    pub const fn millis(self) -> u64 {
        self.0.saturating_mul(1000)
    }

    /// The span as a duration
    #[must_use]
    pub const fn duration(self) -> std::time::Duration {
        std::time::Duration::from_secs(self.0)
    }

    /// Parse a span from its written form
    ///
    /// # Arguments
    ///
    /// * `text` - A whole number followed by `s`, `m`, `h` or `d`
    ///
    /// # Errors
    ///
    /// Text with no unit, an unknown unit, no whole number, or a span past a `u64` of seconds.
    pub fn parse(text: &str) -> Result<Self, String> {
        // the unit is whatever follows the digits
        let text = text.trim();
        let split = text
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| format!("{text:?} has no unit; write it as 90s, 30m, 12h or 7d"))?;
        let (number, unit) = text.split_at(split);
        let number: u64 = number
            .parse()
            .map_err(|_| format!("{text:?} does not start with a whole number"))?;
        // one of four units, and nothing else
        let scale = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            "d" => 86_400,
            other => return Err(format!("{text:?} has unit {other:?}; use s, m, h or d")),
        };
        number
            .checked_mul(scale)
            .map(Span)
            .ok_or_else(|| format!("{text:?} is longer than any span there is"))
    }
}

impl std::fmt::Display for Span {
    /// Write the span in the largest unit that divides it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // the largest unit first, so a week reads 7d and not 604800s
        for (scale, unit) in [(86_400, "d"), (3600, "h"), (60, "m")] {
            if self.0 != 0 && self.0.is_multiple_of(scale) {
                return write!(f, "{}{unit}", self.0 / scale);
            }
        }
        write!(f, "{}s", self.0)
    }
}

impl Serialize for Span {
    /// Write the span as its written form
    ///
    /// # Arguments
    ///
    /// * `serializer` - Where it goes
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Span {
    /// Read the span from its written form
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where it comes from
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Span::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// How often a light scrub passes over a pool's placement groups unless its pool says: daily, as
/// Ceph's `osd_scrub_min_interval` (S11)
pub const DEFAULT_LIGHT_INTERVAL: Span = Span(86_400);

/// The least time between the starts of two deep passes unless a pool says: a week
///
/// A deep pass is paced by its devices' idle time, so how long it takes is what its devices'
/// foreground leaves; a pass slower than this is followed by the next as it ends, and the interval
/// is then how long a pass takes ([X12](../../../docs/src/object-storage/recovery-scrub-rates.md)).
/// On an SSD a pass takes hours, and this keeps one from reading its device end to end all day.
pub const DEFAULT_DEEP_INTERVAL: Span = Span(7 * 86_400);

/// How many deep passes it takes to check every placement group's parity by summaries
///
/// Decided with the user on 2026-10-15 (Q28): a deep pass checks a quarter of a pool's placement
/// groups' parity, rotating, so every group's is checked every fourth pass. X12 found the fold adds
/// 0.49 to 0.71 of the checksum's cost; a quarter of it is 12 to 18%. A replicated pool has no
/// parity, so nothing reads this until M18 builds the check. ✅ Read since M18, by
/// [`ScrubPolicy::checks_parity`] ([F90](../../../docs/src/features/erasure-coded-pools.md)).
pub const PARITY_CHECK_EVERY: u64 = 4;

/// The two scrubs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrubKind {
    /// Listings against the rows and each other: no data read
    Light,
    /// Every unit read and verified where it lies, and the copies' checksum tables compared
    Deep,
}

impl ScrubKind {
    /// Both kinds, light first
    pub const ALL: [ScrubKind; 2] = [ScrubKind::Light, ScrubKind::Deep];

    /// The kind's name, as an operator writes it
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ScrubKind::Light => "light",
            ScrubKind::Deep => "deep",
        }
    }

    /// The byte a placement command carries the kind as
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            ScrubKind::Light => 1,
            ScrubKind::Deep => 2,
        }
    }

    /// The kind a byte names, if it names one
    ///
    /// # Arguments
    ///
    /// * `byte` - The byte
    #[must_use]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(ScrubKind::Light),
            2 => Some(ScrubKind::Deep),
            _ => None,
        }
    }
}

impl std::str::FromStr for ScrubKind {
    type Err = String;

    /// Read a kind as an operator writes it
    ///
    /// # Arguments
    ///
    /// * `text` - `light` or `deep`
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.trim().to_ascii_lowercase().as_str() {
            "light" => Ok(ScrubKind::Light),
            "deep" => Ok(ScrubKind::Deep),
            other => Err(format!("{other:?} is no scrub; it is light or deep")),
        }
    }
}

/// The hours of a day, in UTC, a scheduled scrub may run in, written `01-06` or `01:00-06:00`
///
/// A pass outside its window pauses between placement groups and goes on when the window opens
/// again. A window that ends before it starts runs over midnight, and one that ends where it starts
/// is the whole day. An operator's scrub runs at once, whatever the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScrubWindow {
    /// The hour it opens, 0 to 23
    pub start: u8,
    /// The hour it closes, 0 to 23
    pub end: u8,
}

impl ScrubWindow {
    /// Parse a window from its written form
    ///
    /// # Arguments
    ///
    /// * `text` - Two hours of a day, `HH` or `HH:00`, joined by a dash
    ///
    /// # Errors
    ///
    /// Text that is not two whole hours under 24, or minutes that are not `00`.
    pub fn parse(text: &str) -> Result<Self, String> {
        // each end is an hour, with or without its minutes
        let hour = |part: &str| -> Result<u8, String> {
            let part = part.trim();
            let (hour, minutes) = part.split_once(':').unwrap_or((part, "00"));
            if minutes != "00" {
                return Err(format!("{text:?}: a scrub window opens and closes on the hour"));
            }
            match hour.parse::<u8>() {
                Ok(hour) if hour < 24 => Ok(hour),
                _ => Err(format!("{text:?}: {hour:?} is no hour of a day")),
            }
        };
        let (start, end) = text
            .split_once('-')
            .ok_or_else(|| format!("{text:?} is no window; write it as 01:00-06:00"))?;
        Ok(ScrubWindow {
            start: hour(start)?,
            end: hour(end)?,
        })
    }

    /// Whether a time is inside the window
    ///
    /// # Arguments
    ///
    /// * `now_ms` - The time, in milliseconds since the Unix epoch
    #[must_use]
    pub fn contains(&self, now_ms: u64) -> bool {
        // the hour of the day it is, in UTC
        #[allow(clippy::cast_possible_truncation)]
        let hour = ((now_ms / 3_600_000) % 24) as u8;
        match self.start.cmp(&self.end) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Less => self.start <= hour && hour < self.end,
            std::cmp::Ordering::Greater => hour >= self.start || hour < self.end,
        }
    }
}

impl std::fmt::Display for ScrubWindow {
    /// Write the window as an operator writes it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02}:00-{:02}:00", self.start, self.end)
    }
}

impl Serialize for ScrubWindow {
    /// Write the window as its written form
    ///
    /// # Arguments
    ///
    /// * `serializer` - Where it goes
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ScrubWindow {
    /// Read the window from its written form
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where it comes from
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        ScrubWindow::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// How often a pool's placement groups are scrubbed, committed with the pool
///
/// Policy, so every leader of the pool's tablet groups schedules alike whichever node it is on.
/// An interval of `0s` schedules no pass of that kind; an operator's scrub still runs one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScrubPolicy {
    /// How often a light pass runs; daily unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub light: Option<Span>,
    /// The least time between the starts of two deep passes; a week unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep: Option<Span>,
    /// The hours a scheduled pass may run in; any hour unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<ScrubWindow>,
    /// How many deep passes it takes to check every placement group's parity by summaries;
    /// [`PARITY_CHECK_EVERY`] unless set, and one checks every group every pass (F90)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parity_every: Option<u32>,
}

impl ScrubPolicy {
    /// How often a kind's passes run, in milliseconds; zero for never
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub fn interval_ms(&self, kind: ScrubKind) -> u64 {
        match kind {
            ScrubKind::Light => self.light.unwrap_or(DEFAULT_LIGHT_INTERVAL).millis(),
            ScrubKind::Deep => self.deep.unwrap_or(DEFAULT_DEEP_INTERVAL).millis(),
        }
    }

    /// Whether a scheduled pass may run now
    ///
    /// # Arguments
    ///
    /// * `now_ms` - The time, in milliseconds since the Unix epoch
    #[must_use]
    pub fn in_window(&self, now_ms: u64) -> bool {
        self.window.is_none_or(|window| window.contains(now_ms))
    }

    /// Whether a deep pass checks a placement group's parity by summaries (F90)
    ///
    /// A quarter of a pool's groups a pass unless the policy says otherwise, rotating, so every
    /// group's parity is checked every [`PARITY_CHECK_EVERY`] passes: the group's index among its
    /// tablet's, and its tablet's, offset by the pass.
    ///
    /// # Arguments
    ///
    /// * `pass` - The deep pass
    /// * `tablet` - The placement group's tablet
    /// * `sub` - Its index among its tablet's groups
    #[must_use]
    pub fn checks_parity(&self, pass: u64, tablet: u16, sub: u16) -> bool {
        let every = u64::from(self.parity_every.unwrap_or(PARITY_CHECK_EVERY as u32).max(1));
        (pass + u64::from(tablet) + u64::from(sub)) % every == 0
    }
}

/// Where one kind of scrub is in a tablet group's passes
///
/// Every field moves only by a placement command the group committed, so every copy holds the
/// same cursor and a new leader resumes from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ScrubCursor {
    /// The pass it is in, or last finished; zero before the first
    pub pass: u64,
    /// The last placement group the pass finished; none at its start
    pub after: Option<PgKey>,
    /// How many placement groups the pass has finished
    pub done: u64,
    /// Whether the pass finished
    pub finished: bool,
    /// The operator's request the pass serves: the pool's count of requests when it began
    pub request: u64,
    /// Whether an operator's request began the pass, which then runs whatever the window
    pub asked: bool,
    /// When the pass began, by its driver's clock, in milliseconds since the Unix epoch
    pub began_ms: u64,
    /// The request the last pass that finished served; none before one has
    pub served: Option<u64>,
    /// When the last pass that finished finished, by its driver's clock; zero before one has
    pub last_ms: u64,
}

impl ScrubCursor {
    /// Whether a pass is under way
    #[must_use]
    pub const fn running(&self) -> bool {
        self.pass > 0 && !self.finished
    }

    /// Whether no finished pass serves a count of requests: none finished, or the last served
    /// fewer
    ///
    /// # Arguments
    ///
    /// * `requested` - The pool's count of requests of the cursor's kind
    #[must_use]
    pub fn pending(&self, requested: u64) -> bool {
        self.served.is_none_or(|served| served < requested)
    }
}

/// Both of a tablet group's cursors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ScrubState {
    /// The light scrub's
    pub light: ScrubCursor,
    /// The deep scrub's
    pub deep: ScrubCursor,
}

impl ScrubState {
    /// A kind's cursor
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub const fn cursor(&self, kind: ScrubKind) -> &ScrubCursor {
        match kind {
            ScrubKind::Light => &self.light,
            ScrubKind::Deep => &self.deep,
        }
    }

    /// A kind's cursor, to change
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    pub fn cursor_mut(&mut self, kind: ScrubKind) -> &mut ScrubCursor {
        match kind {
            ScrubKind::Light => &mut self.light,
            ScrubKind::Deep => &mut self.deep,
        }
    }
}

/// How many scrubs of each kind an operator has asked of a pool, committed on its pool map
///
/// A leader whose tablet group's cursor serves fewer begins a pass at once. A count, not a flag,
/// so a request made while a pass runs is not lost to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ScrubRequests {
    /// Light scrubs asked for
    #[serde(default)]
    pub light: u64,
    /// Deep scrubs asked for
    #[serde(default)]
    pub deep: u64,
    /// What the newest light request narrows its pass to, or none for the whole pool
    /// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub light_target: Option<ScrubTarget>,
    /// What the newest deep request narrows its pass to, or none for the whole pool
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep_target: Option<ScrubTarget>,
    /// The first light request of the run that names the newest one's target, which a group that
    /// has not served every request before it scrubs past
    /// ([Resolved #266](../../../docs/src/appendix/resolved/narrowed-scrub-widens.md))
    #[serde(default, skip_serializing_if = "is_zero")]
    pub light_since: u64,
    /// The same for deep requests
    #[serde(default, skip_serializing_if = "is_zero")]
    pub deep_since: u64,
}

/// Whether a count is zero, which a request's run before any is recorded is
///
/// # Arguments
///
/// * `count` - The count
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(count: &u64) -> bool {
    *count == 0
}

/// What an operator's scrub of a pool narrows its pass to
///
/// A request is counted on its pool whatever it names, so every tablet group of the pool serves
/// it; a narrowed one scrubs only the placement groups it names, and the rest of each group's
/// pass ends at once ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrubTarget {
    /// One bucket's stripes in the pool
    Bucket(crate::shared::identity::BucketId),
    /// The placement groups with a position on one device
    Device(crate::shared::devices::DeviceId),
    /// The placement groups with a position on one slice of a device
    Slice(crate::shared::poolmap::SliceRef),
}

impl ScrubRequests {
    /// A kind's count
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub const fn of(&self, kind: ScrubKind) -> u64 {
        match kind {
            ScrubKind::Light => self.light,
            ScrubKind::Deep => self.deep,
        }
    }

    /// Count one more of a kind, and answer the new count
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    pub fn ask(&mut self, kind: ScrubKind) -> u64 {
        self.ask_for(kind, None)
    }

    /// Count one more of a kind, narrowed to a target or the whole pool, and answer the new count
    ///
    /// A target unlike the one before it begins a run of requests of its own, which is what lets a
    /// group that has not served the requests before the run scrub them too: a request's target
    /// once replaced every earlier one's, so a whole pool's request pending beside a later narrowed
    /// one was served as the narrowed pass alone
    /// ([Resolved #266](../../../docs/src/appendix/resolved/narrowed-scrub-widens.md)).
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    /// * `target` - What the request narrows its pass to, or none
    pub fn ask_for(&mut self, kind: ScrubKind, target: Option<ScrubTarget>) -> u64 {
        let (count, narrowed, since) = match kind {
            ScrubKind::Light => (&mut self.light, &mut self.light_target, &mut self.light_since),
            ScrubKind::Deep => (&mut self.deep, &mut self.deep_target, &mut self.deep_since),
        };
        *count += 1;
        // a target unlike the newest one's, or the first recorded, begins a run here
        if *narrowed != target || *since == 0 {
            *since = *count;
        }
        *narrowed = target;
        *count
    }

    /// What the newest request of a kind narrows its pass to
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    #[must_use]
    pub const fn target(&self, kind: ScrubKind) -> Option<ScrubTarget> {
        match kind {
            ScrubKind::Light => self.light_target,
            ScrubKind::Deep => self.deep_target,
        }
    }

    /// What a group's pass of a kind narrows to, serving every request it has not served
    ///
    /// The newest request's target when every request past the last the group served names it,
    /// and the whole pool when one of them named another, since a pass serves them all.
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    /// * `served` - The request the group's last finished pass served, none before one has
    #[must_use]
    pub fn target_for(&self, kind: ScrubKind, served: Option<u64>) -> Option<ScrubTarget> {
        let since = match kind {
            ScrubKind::Light => self.light_since,
            ScrubKind::Deep => self.deep_since,
        };
        // the first request the group has not served, inside the newest target's run or not
        let unserved = served.map_or(1, |served| served + 1);
        if unserved >= since {
            self.target(kind)
        } else {
            None
        }
    }
}

/// What a tablet group's leader does about one kind of scrub now
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// Nothing: no pass is under way and none is due, or the window is shut
    Wait,
    /// Go on with the pass under way
    Continue,
    /// Begin a pass
    Begin {
        /// The request it serves
        request: u64,
        /// Whether an operator's request begins it
        asked: bool,
    },
}

/// What a tablet group's leader does about one kind of scrub now
///
/// An operator's request newer than the one the cursor serves begins a pass at once, whatever the
/// window, and one already under way is begun again for it. Otherwise a pass under way goes on
/// while the window is open, an operator's always. A group never scrubbed begins its first pass
/// once its stagger has passed since its leader first saw it, so a pool's groups do not all begin
/// together; a later pass begins an interval after the last began, or as it finished when it was
/// slower than that.
///
/// # Arguments
///
/// * `policy` - The pool's scrub policy
/// * `kind` - The kind
/// * `cursor` - The group's cursor of that kind
/// * `requested` - The pool's count of requests of that kind
/// * `now_ms` - The time, in milliseconds since the Unix epoch
/// * `first_ms` - When this leader first saw the group, by the same clock
/// * `stagger_ms` - How long after that a group never scrubbed begins its first pass
#[must_use]
pub fn due(
    policy: &ScrubPolicy,
    kind: ScrubKind,
    cursor: &ScrubCursor,
    requested: u64,
    now_ms: u64,
    first_ms: u64,
    stagger_ms: u64,
) -> Due {
    // an operator's newer request first, whatever is under way
    if requested > cursor.request {
        return Due::Begin {
            request: requested,
            asked: true,
        };
    }
    // a pass under way goes on, a scheduled one inside its window
    if cursor.running() {
        return if cursor.asked || policy.in_window(now_ms) {
            Due::Continue
        } else {
            Due::Wait
        };
    }
    // a kind whose interval is zero is never scheduled
    let interval = policy.interval_ms(kind);
    if interval == 0 || !policy.in_window(now_ms) {
        return Due::Wait;
    }
    let next = if cursor.pass == 0 {
        first_ms.saturating_add(stagger_ms)
    } else {
        cursor.began_ms.saturating_add(interval).max(cursor.last_ms)
    };
    if now_ms >= next {
        Due::Begin {
            request: cursor.request,
            asked: false,
        }
    } else {
        Due::Wait
    }
}

/// How long after its leader first sees it a group never scrubbed waits for its first pass
///
/// A share of the interval fixed by the group's identity, F44's spread for a table's scrubs, so a
/// pool's groups begin over the interval and not together. Never none: a group's first pass waits
/// at least a 997th of the interval after its leader first sees it, so a cluster just started, or
/// a leader just elected, is not scrubbed before it is busy with anything else.
///
/// # Arguments
///
/// * `group` - A number fixed for the tablet group, its id's
/// * `interval_ms` - The kind's interval
#[must_use]
pub const fn stagger_ms(group: u64, interval_ms: u64) -> u64 {
    // a share of 997, a prime, so neighbouring groups land apart, and one share at least
    interval_ms / 997 * (1 + group % 996)
}

/// What one pool's scrubs found and did on a node ([F89](../../../docs/src/features/scrub-and-repair.md))
///
/// The counters are what the node's drivers did since it started. The gauges are read from the
/// cursors of the tablet groups the node leads, so each group is counted on one member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ScrubFigures {
    /// Light passes finished
    #[serde(default)]
    pub light_passes: u64,
    /// Deep passes finished
    #[serde(default)]
    pub deep_passes: u64,
    /// Placement groups a pass of either kind finished
    #[serde(default)]
    pub scrubbed: u64,
    /// Chunks a deep scrub verified whole
    #[serde(default)]
    pub chunks: u64,
    /// The bytes a deep scrub read and verified
    #[serde(default)]
    pub bytes: u64,
    /// Positions a scrub found stale: a holder behind the row, or without its chunk
    #[serde(default)]
    pub stale: u64,
    /// Positions a scrub or a read found corrupt: a unit or a header that fails its checksum
    #[serde(default)]
    pub corrupt: u64,
    /// Chunks a light scrub found that no row state explains, reported and left for M20
    #[serde(default)]
    pub unexplained: u64,
    /// Chunks of a stripe its row reclaimed, left for M20
    #[serde(default)]
    pub reclaimable: u64,
    /// Stripes whose copies each verified and disagreed, stopped with their evidence
    #[serde(default)]
    pub disagreed: u64,
    /// Stripes with no verified current copy, which nothing could rebuild
    #[serde(default)]
    pub lost: u64,
    /// Erasure coded stripes a deep scrub checked the parity of by summaries (F90)
    #[serde(default)]
    pub parity_checked: u64,
    /// Of those, the stripes whose every chunk verified and whose parity disagreed with their
    /// data: stopped with their evidence, and repaired by no rule (F90)
    #[serde(default)]
    pub parity_mismatch: u64,
    /// Stripes whose holders a scrub found ahead of their row, which only a `force_recover` that
    /// lost a write leaves: kept, never marked or rebuilt over, for an operator's `ResolveStripe`
    /// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default)]
    pub behind: u64,
    /// Tablet groups led here whose last finished light pass is older than the pool's latest
    /// light request, or that never finished one
    #[serde(default)]
    pub light_pending: u64,
    /// The same for deep passes: zero on every member once an operator's deep scrub is done
    #[serde(default)]
    pub deep_pending: u64,
    /// When the light pass of the group led here that finished longest ago finished, in
    /// milliseconds since the Unix epoch; zero if one never did, none if no group is led here
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub light_last_ms: Option<u64>,
    /// The same for deep passes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep_last_ms: Option<u64>,
    /// The pool's count of light requests the pending groups were judged against, the oldest any
    /// member judged by; none if no group is led here
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub light_against: Option<u64>,
    /// The same for deep requests: an operator's deep scrub is done once this reaches its request
    /// and no group is pending
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep_against: Option<u64>,
}

impl ScrubFigures {
    /// Add another's figures to these; a time is the older of the two
    ///
    /// # Arguments
    ///
    /// * `other` - The figures added
    pub fn absorb(&mut self, other: &ScrubFigures) {
        self.light_passes += other.light_passes;
        self.deep_passes += other.deep_passes;
        self.scrubbed += other.scrubbed;
        self.chunks += other.chunks;
        self.bytes += other.bytes;
        self.stale += other.stale;
        self.corrupt += other.corrupt;
        self.unexplained += other.unexplained;
        self.behind += other.behind;
        self.reclaimable += other.reclaimable;
        self.disagreed += other.disagreed;
        self.lost += other.lost;
        self.parity_checked += other.parity_checked;
        self.parity_mismatch += other.parity_mismatch;
        self.light_pending += other.light_pending;
        self.deep_pending += other.deep_pending;
        self.light_last_ms = oldest(self.light_last_ms, other.light_last_ms);
        self.deep_last_ms = oldest(self.deep_last_ms, other.deep_last_ms);
        self.light_against = oldest(self.light_against, other.light_against);
        self.deep_against = oldest(self.deep_against, other.deep_against);
    }

    /// Whether every group the figures cover has finished a pass of a kind that serves a count of
    /// requests, judged against that count or a later one
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind
    /// * `requested` - The count of requests
    #[must_use]
    pub fn served(&self, kind: ScrubKind, requested: u64) -> bool {
        match kind {
            ScrubKind::Light => self.light_against.is_some_and(|against| against >= requested) && self.light_pending == 0,
            ScrubKind::Deep => self.deep_against.is_some_and(|against| against >= requested) && self.deep_pending == 0,
        }
    }

    /// Note a tablet group's cursors: its pending passes and when each kind last finished
    ///
    /// # Arguments
    ///
    /// * `state` - The group's cursors
    /// * `requests` - The pool's counts of requests
    pub fn note_group(&mut self, state: &ScrubState, requests: &ScrubRequests) {
        for kind in ScrubKind::ALL {
            let cursor = state.cursor(kind);
            // pending until a finished pass serves the latest request
            let pending = u64::from(cursor.pending(requests.of(kind)));
            match kind {
                ScrubKind::Light => {
                    self.light_pending += pending;
                    self.light_last_ms = oldest(self.light_last_ms, Some(cursor.last_ms));
                    self.light_against = oldest(self.light_against, Some(requests.light));
                }
                ScrubKind::Deep => {
                    self.deep_pending += pending;
                    self.deep_last_ms = oldest(self.deep_last_ms, Some(cursor.last_ms));
                    self.deep_against = oldest(self.deep_against, Some(requests.deep));
                }
            }
        }
    }

    /// Whether nothing was counted
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == ScrubFigures::default()
    }
}

/// The older of two times, either of which may be absent
///
/// # Arguments
///
/// * `a` - One time
/// * `b` - The other
const fn oldest(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// Every pool's counts of requests, by pool
pub type PoolScrubRequests = BTreeMap<String, ScrubRequests>;

#[cfg(test)]
mod tests;
