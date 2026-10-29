//! What one object executor has in flight, kept where any thread reads it without the executor
//! ([F108](../../../../docs/src/features/object-work-in-flight.md))
//!
//! An executor's lanes keep their figures in cells only its own thread reads, and the fixture's
//! `OBJECTS` verb asked for them through the executor's mailbox, so an executor that stopped
//! answering said nothing of what it held. Item 305's put stalled for minutes that way, and a node
//! had nothing to read. These are kept beside those figures, in atomics each executor shares with
//! the store's handle, which every shard's report reads.
//!
//! # Invariants
//!
//! **A gauge counted up is counted down on every way its work ends**: a request answered, refused
//! or dropped with its link, an answer written or dropped with its lane, a link down or dropped.
//! Each is held through a [`Held`] whose drop counts it down, never by a pair of calls a failure or a
//! cancelled task could split.
//!
//! **Nothing here waits.** A report reads these whatever the executor is doing, so they are plain
//! atomics with relaxed ordering: a reader sees each figure as it was a moment ago, and no two of
//! them at one instant.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::shared::protocol::stats::ObjectWork;

/// How often one kind of warning about object work is logged, the rest counted into the next
pub const WARN_EVERY: Duration = Duration::from_secs(10);

/// What one object executor has in flight, and what its lanes refused
#[derive(Debug, Default)]
pub struct InFlight {
    /// Lanes it dialled that are up now
    links: AtomicU64,
    /// Requests it sent on them that are not answered yet
    waiting: AtomicU64,
    /// Requests it sent on them
    sent: AtomicU64,
    /// Requests a lane refused
    refused: AtomicU64,
    /// Lanes it serves now
    served_lanes: AtomicU64,
    /// Answers its served lanes owe now
    owed: AtomicU64,
    /// Requests its served lanes took
    served: AtomicU64,
}

/// The gauges a [`Held`] holds one of
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gauge {
    /// A dialled lane that is up
    Link,
    /// A request sent and not answered yet
    Waiting,
    /// A lane being served
    ServedLane,
    /// An answer a served lane owes
    Owed,
}

/// One of a gauge, held until it is dropped
#[derive(Debug)]
pub struct Held {
    /// The executor's figures
    figures: Arc<InFlight>,
    /// The gauge
    gauge: Gauge,
}

impl Drop for Held {
    /// Count the gauge down
    fn drop(&mut self) {
        self.figures
            .gauge(self.gauge)
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl InFlight {
    /// A gauge's counter
    ///
    /// # Arguments
    ///
    /// * `gauge` - The gauge
    fn gauge(&self, gauge: Gauge) -> &AtomicU64 {
        match gauge {
            Gauge::Link => &self.links,
            Gauge::Waiting => &self.waiting,
            Gauge::ServedLane => &self.served_lanes,
            Gauge::Owed => &self.owed,
        }
    }

    /// Hold one of a gauge until what is returned is dropped
    ///
    /// # Arguments
    ///
    /// * `gauge` - The gauge
    #[must_use]
    pub fn hold(self: &Arc<Self>, gauge: Gauge) -> Held {
        self.gauge(gauge).fetch_add(1, Ordering::Relaxed);
        Held {
            figures: self.clone(),
            gauge,
        }
    }

    /// Count a request sent on a lane
    pub fn count_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a request a lane refused
    pub fn count_refused(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a request a served lane took
    pub fn count_served(&self) {
        self.served.fetch_add(1, Ordering::Relaxed);
    }

    /// Add these figures to a node's
    ///
    /// # Arguments
    ///
    /// * `work` - The node's figures
    pub fn add_to(&self, work: &mut ObjectWork) {
        work.links += self.links.load(Ordering::Relaxed);
        work.waiting += self.waiting.load(Ordering::Relaxed);
        work.sent += self.sent.load(Ordering::Relaxed);
        work.refused += self.refused.load(Ordering::Relaxed);
        work.served_lanes += self.served_lanes.load(Ordering::Relaxed);
        work.owed += self.owed.load(Ordering::Relaxed);
        work.served += self.served.load(Ordering::Relaxed);
    }
}

/// A warning logged at most once every interval, saying how many it held back since
///
/// One a kind of warning, kept in a static and shared by every thread that warns of it: a lane that
/// refuses a request a millisecond logs a line every [`WARN_EVERY`] with the count of the rest.
#[derive(Debug)]
pub struct Throttle {
    /// When it last let a warning through, in milliseconds from the first warning of any kind,
    /// or zero if it never has
    last_ms: AtomicU64,
    /// The warnings held back since
    held: AtomicU64,
}

/// The instant every throttle counts from
static EPOCH: OnceLock<Instant> = OnceLock::new();

impl Throttle {
    /// A throttle that has let nothing through yet
    #[must_use]
    pub const fn new() -> Self {
        Throttle {
            last_ms: AtomicU64::new(0),
            held: AtomicU64::new(0),
        }
    }

    /// Whether a warning is logged now: the count held back since the last, when it is
    ///
    /// # Arguments
    ///
    /// * `every` - How long after the last warning let through the next one is
    #[must_use]
    pub fn pass(&self, every: Duration) -> Option<u64> {
        // milliseconds from the epoch, never zero, which means never
        let now = EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1;
        let last = self.last_ms.load(Ordering::Relaxed);
        // too soon after the last is counted into the next
        if last != 0 && now.saturating_sub(last) < every.as_millis() as u64 {
            self.held.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        // and of two threads warning at once, one logs and the other is counted
        if self
            .last_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.held.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.held.swap(0, Ordering::Relaxed))
    }
}

impl Default for Throttle {
    /// A throttle that has let nothing through yet
    fn default() -> Self {
        Throttle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gauge held is counted until its guard is dropped, however many are held, and the counts
    /// reach a node's figures
    #[test]
    fn a_gauge_is_held_until_its_guard_drops() {
        let figures = Arc::new(InFlight::default());
        // two requests waiting and an answer owed
        let first = figures.hold(Gauge::Waiting);
        let second = figures.hold(Gauge::Waiting);
        let owed = figures.hold(Gauge::Owed);
        figures.count_sent();
        figures.count_sent();
        let mut work = ObjectWork::default();
        figures.add_to(&mut work);
        assert_eq!(
            (work.waiting, work.owed, work.sent, work.links),
            (2, 1, 2, 0)
        );
        // one answered, the answer written: each counted down by its drop alone
        drop(first);
        drop(owed);
        let mut work = ObjectWork::default();
        figures.add_to(&mut work);
        assert_eq!((work.waiting, work.owed, work.sent), (1, 0, 2));
        drop(second);
        let mut work = ObjectWork::default();
        figures.add_to(&mut work);
        assert_eq!(work.waiting, 0);
    }

    /// A throttle lets the first warning through, holds back the ones too soon after it, and says
    /// how many it held back when it lets the next through
    #[test]
    fn a_throttle_counts_what_it_holds_back() {
        let throttle = Throttle::new();
        assert_eq!(throttle.pass(Duration::from_millis(50)), Some(0));
        assert_eq!(throttle.pass(Duration::from_millis(50)), None);
        assert_eq!(throttle.pass(Duration::from_millis(50)), None);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(throttle.pass(Duration::from_millis(50)), Some(2));
    }
}
