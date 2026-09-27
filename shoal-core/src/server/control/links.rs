//! Whether this node's own links are slow: every peer's round trip far above what it was
//!
//! On the lab a 100 ms delay on one node's peer links took the whole cluster to a third of its
//! throughput, because the slow node went on leading its third of the groups and every pipelined
//! client filled its window with their writes
//! ([cluster testing, section 12](../../../../docs/src/cluster-testing/correctness.md#12-scenarios-nobody-had-run)).
//! Leads are balanced by count, and nothing moved them off a member whose links were slow.
//!
//! The control thread pings every member once a second. Each peer's round trips are smoothed,
//! and each peer keeps the lowest smoothed figure seen as its baseline, which creeps up slowly so
//! a network that is simply slower is learnt rather than judged. The node is impaired when the
//! round trip to every peer is past both a floor and a multiple of that peer's baseline. Judged
//! from every peer at once, one slow node sees all its peers slow while each of them sees only
//! it, so only the slow node judges itself impaired. With fewer than two peers the two ends of
//! a slow link cannot be told apart, and nothing is judged.
//!
//! An impaired node hands its leads on and answers `MayLead` with no, so the handback does not
//! bring them back. It goes on standing for election: with every member impaired, somebody has
//! to lead.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Whether this process's links are judged impaired, which every shard reads
static IMPAIRED: AtomicBool = AtomicBool::new(false);

/// How much of each new round trip the smoothed figure takes
const SMOOTHING: f64 = 0.3;

/// How much of the gap to a higher smoothed figure a baseline closes each sample
///
/// A time constant of nearly three hours at one sample a second: a fault of minutes stays a
/// fault, and a network that became slower for good is learnt within the day.
const BASELINE_CREEP: f64 = 0.0001;

/// The least smoothed round trip, in microseconds, that can count as impaired
const IMPAIRED_FLOOR_US: f64 = 10_000.0;

/// How many times a peer's baseline its round trip has to be to count as impaired
const IMPAIRED_FACTOR: f64 = 20.0;

/// The floor an impaired node's round trips have to fall under before it is well again
const RECOVERED_FLOOR_US: f64 = 5_000.0;

/// The multiple of the baseline an impaired node's round trips have to fall under
const RECOVERED_FACTOR: f64 = 10.0;

/// How long a peer's figure counts after its last answered ping
const FRESH_FOR: Duration = Duration::from_secs(10);

/// One peer's round trips, as this node's pings have measured them
#[derive(Debug, Clone)]
pub struct LinkRtt {
    /// The smoothed round trip, in microseconds
    ewma_us: f64,
    /// The lowest smoothed round trip seen, creeping up towards the current one
    baseline_us: f64,
    /// When the last answer arrived
    last: Instant,
}

impl LinkRtt {
    /// A peer's first answered ping
    ///
    /// # Arguments
    ///
    /// * `rtt` - Its round trip
    #[must_use]
    pub fn new(rtt: Duration) -> Self {
        // truncation is irrelevant for a round trip anybody waits for
        #[allow(clippy::cast_precision_loss)]
        let us = rtt.as_micros() as f64;
        LinkRtt {
            ewma_us: us,
            baseline_us: us,
            last: Instant::now(),
        }
    }

    /// Fold in another answered ping
    ///
    /// # Arguments
    ///
    /// * `rtt` - Its round trip
    pub fn observe(&mut self, rtt: Duration) {
        #[allow(clippy::cast_precision_loss)]
        let us = rtt.as_micros() as f64;
        // the smoothed figure, then the baseline under it
        self.ewma_us += (us - self.ewma_us) * SMOOTHING;
        if self.ewma_us < self.baseline_us {
            self.baseline_us = self.ewma_us;
        } else {
            self.baseline_us += (self.ewma_us - self.baseline_us) * BASELINE_CREEP;
        }
        self.last = Instant::now();
    }

    /// Whether this peer's round trip is past the line for impaired, or for recovered
    ///
    /// # Arguments
    ///
    /// * `impaired` - Whether the node is impaired now, which asks the lower recovery line
    fn slow(&self, impaired: bool) -> bool {
        let (floor, factor) = if impaired {
            (RECOVERED_FLOOR_US, RECOVERED_FACTOR)
        } else {
            (IMPAIRED_FLOOR_US, IMPAIRED_FACTOR)
        };
        self.ewma_us > floor.max(self.baseline_us * factor)
    }

    /// The smoothed round trip, in microseconds
    #[must_use]
    pub fn ewma_us(&self) -> f64 {
        self.ewma_us
    }
}

/// Judge whether this node's links are impaired from every peer's figures
///
/// # Arguments
///
/// * `peers` - Every peer's round trips
/// * `impaired` - Whether the node is impaired now
#[must_use]
pub fn judge<'a>(peers: impl Iterator<Item = &'a LinkRtt>, impaired: bool) -> bool {
    // only peers heard from lately count, and at least two of them
    let now = Instant::now();
    let fresh: Vec<&LinkRtt> = peers
        .filter(|peer| now.duration_since(peer.last) < FRESH_FOR)
        .collect();
    fresh.len() >= 2 && fresh.iter().all(|peer| peer.slow(impaired))
}

/// Say whether this node's links are impaired, for every shard to read
///
/// # Arguments
///
/// * `impaired` - The judgement
pub fn set_impaired(impaired: bool) {
    IMPAIRED.store(impaired, Ordering::Relaxed);
}

/// Whether this node's links are judged impaired
#[must_use]
pub fn impaired() -> bool {
    IMPAIRED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A peer's figure after a run of equal round trips
    ///
    /// # Arguments
    ///
    /// * `base` - The round trips it was learnt at
    /// * `now` - The round trips it sees now
    fn peer(base: Duration, now: Duration) -> LinkRtt {
        let mut link = LinkRtt::new(base);
        for _ in 0..30 {
            link.observe(base);
        }
        for _ in 0..30 {
            link.observe(now);
        }
        link
    }

    /// Every peer slow is impaired, one slow peer is not, and a single peer is never judged
    #[test]
    fn impaired_only_when_every_peer_is_slow() {
        let lan = Duration::from_micros(120);
        let slow = Duration::from_millis(100);
        // both peers slow: this node is the slow one
        let both = [peer(lan, slow), peer(lan, slow)];
        assert!(judge(both.iter(), false));
        // one slow, one fast: the slow one is the other end's problem
        let one = [peer(lan, slow), peer(lan, lan)];
        assert!(!judge(one.iter(), false));
        // a single peer cannot say which end is slow
        let single = [peer(lan, slow)];
        assert!(!judge(single.iter(), false));
    }

    /// A few milliseconds is never impaired however fast the baseline, and recovery needs the
    /// round trips well down again
    #[test]
    fn the_floor_and_the_recovery_line_hold() {
        let lan = Duration::from_micros(120);
        // 5 ms is forty times a LAN, and under the 10 ms floor
        let busy = [
            peer(lan, Duration::from_millis(5)),
            peer(lan, Duration::from_millis(5)),
        ];
        assert!(!judge(busy.iter(), false));
        // once impaired, 8 ms is still past the 5 ms recovery floor
        let easing = [
            peer(lan, Duration::from_millis(8)),
            peer(lan, Duration::from_millis(8)),
        ];
        assert!(judge(easing.iter(), true));
        assert!(!judge(easing.iter(), false));
    }
}
