//! The leader's phi-accrual failure detector over the members' status reports
//!
//! Every member reports to the leader at `failure_detector.interval_ms`. The leader keeps, per
//! member, the intervals between the arrivals of its fresh reports, fits a normal distribution
//! to the last `window` of them, and asks at every tick how improbable the silence since the
//! last one has become: `phi = -log10(P(a report arrives later than now))`. A member whose phi
//! crosses `phi_threshold` is proposed `Down` through the group, and its next fresh report
//! proposes it `Up` again; a minority can therefore never call anybody down, since it cannot
//! commit ([F39](../../../../docs/src/features/membership.md)).
//!
//! A report is fresh when its sequence is above the last one seen from that incarnation of the
//! node; a replayed or reordered report is counted and ignored, and never resets the clock. A
//! new leader starts with no evidence: every member is seeded with the expected interval and a
//! grace period, so a member that never reports to it is suspected after the grace and nobody is
//! called down for the election itself.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::server::conf::cluster::FailureDetector;
use crate::shared::identity::NodeId;

/// The largest phi reported, since a probability below what a double holds is still a number
const PHI_CAP: f64 = 300.0;

/// How many intervals of grace a member gets before a new leader may suspect it
const GRACE_INTERVALS: u32 = 5;

/// What the detector holds about one member
#[derive(Debug, Clone)]
struct Samples {
    /// The sequence of the last fresh report
    last_seq: u64,
    /// The incarnation that report came from
    last_incarnation: u64,
    /// When it arrived, or when a seeded member's grace ends
    last_arrival: Instant,
    /// The intervals between arrivals, in milliseconds, newest last
    intervals: VecDeque<f64>,
    /// Whether the intervals are seeded rather than observed
    seeded: bool,
}

/// One row of the detector's view, for the admin `Detector` read
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetectorEntry {
    /// The member
    pub node: NodeId,
    /// How many intervals are held
    pub samples: usize,
    /// Whether those are seeded rather than observed
    pub seeded: bool,
    /// The mean interval, in milliseconds
    pub mean_ms: f64,
    /// The standard deviation used, floor applied, in milliseconds
    pub stddev_ms: f64,
    /// How long since the last fresh report, in milliseconds; negative during a grace period
    pub since_last_ms: f64,
    /// The suspicion level now
    pub phi: f64,
    /// The sequence of the last fresh report
    pub last_seq: u64,
}

/// The detector
#[derive(Debug)]
pub struct Detector {
    /// How many intervals are kept per member
    window: usize,
    /// How many are needed before a verdict
    min_samples: usize,
    /// The phi a member is called down at
    threshold: f64,
    /// The interval members report at
    interval: Duration,
    /// What is held per member
    members: BTreeMap<NodeId, Samples>,
    /// How many reports were ignored for being replayed or reordered
    pub stale_ignored: u64,
}

impl Detector {
    /// Build a detector for a policy
    ///
    /// # Arguments
    ///
    /// * `policy` - The failure detector settings
    #[must_use]
    pub fn new(policy: &FailureDetector) -> Self {
        Detector {
            window: policy.window.max(1),
            min_samples: policy.min_samples.max(1),
            threshold: policy.phi_threshold,
            interval: Duration::from_millis(policy.interval_ms.max(1)),
            members: BTreeMap::new(),
            stale_ignored: 0,
        }
    }

    /// Forget everything, as a new leader must
    pub fn reset(&mut self) {
        self.members.clear();
    }

    /// Seed a member a new leader has no evidence about
    ///
    /// The member gets `min_samples` intervals at the expected pace and a grace period, so it is
    /// suspected only once it has been silent for the grace and then as long as any member would
    /// be, and never merely for the time an election took.
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `incarnation` - Its committed incarnation
    /// * `now` - The moment of seeding
    pub fn seed(&mut self, node: NodeId, incarnation: u64, now: Instant) {
        if self.members.contains_key(&node) {
            return;
        }
        let interval_ms = self.interval.as_secs_f64() * 1000.0;
        self.members.insert(
            node,
            Samples {
                last_seq: 0,
                last_incarnation: incarnation,
                last_arrival: now + self.interval * GRACE_INTERVALS,
                intervals: std::iter::repeat_n(interval_ms, self.min_samples).collect(),
                seeded: true,
            },
        );
    }

    /// Forget a member, once it is no longer one
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    pub fn forget(&mut self, node: NodeId) {
        self.members.remove(&node);
    }

    /// Take a report, and say whether it was fresh
    ///
    /// A fresh report is the first from a newer incarnation, or one whose sequence is above the
    /// last from the same incarnation; anything else is counted and ignored.
    ///
    /// # Arguments
    ///
    /// * `node` - Who reported
    /// * `incarnation` - Which run of it
    /// * `seq` - The report's sequence
    /// * `now` - When it arrived
    pub fn observe(&mut self, node: NodeId, incarnation: u64, seq: u64, now: Instant) -> bool {
        match self.members.get_mut(&node) {
            // a newer run starts its own clock; a seeded member's first report does too
            Some(samples) if incarnation > samples.last_incarnation || samples.seeded => {
                *samples = Samples::first(incarnation, seq, now);
                true
            }
            // an older run, or a report at or behind the last: stale
            Some(samples) if incarnation < samples.last_incarnation || seq <= samples.last_seq => {
                self.stale_ignored += 1;
                false
            }
            // the next report from the run being watched
            Some(samples) => {
                let elapsed = now.saturating_duration_since(samples.last_arrival);
                samples.intervals.push_back(elapsed.as_secs_f64() * 1000.0);
                while samples.intervals.len() > self.window {
                    samples.intervals.pop_front();
                }
                samples.last_seq = seq;
                samples.last_arrival = now;
                true
            }
            // the first report ever from this member
            None => {
                self.members.insert(node, Samples::first(incarnation, seq, now));
                true
            }
        }
    }

    /// The suspicion level of a member now, if there is enough evidence for one
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `now` - The moment asked about
    #[must_use]
    pub fn phi(&self, node: NodeId, now: Instant) -> Option<f64> {
        let samples = self.members.get(&node)?;
        if samples.intervals.len() < self.min_samples {
            return None;
        }
        let (mean, stddev) = self.fit(samples);
        Some(phi(since_ms(samples.last_arrival, now), mean, stddev))
    }

    /// The members whose phi is over the threshold now
    ///
    /// # Arguments
    ///
    /// * `now` - The moment asked about
    #[must_use]
    pub fn suspects(&self, now: Instant) -> Vec<NodeId> {
        self.members
            .keys()
            .filter(|node| self.phi(**node, now).is_some_and(|phi| phi > self.threshold))
            .copied()
            .collect()
    }

    /// Every member's row, for the admin read
    ///
    /// # Arguments
    ///
    /// * `now` - The moment asked about
    #[must_use]
    pub fn view(&self, now: Instant) -> Vec<DetectorEntry> {
        self.members
            .iter()
            .map(|(node, samples)| {
                let (mean, stddev) = self.fit(samples);
                let since = since_ms(samples.last_arrival, now);
                DetectorEntry {
                    node: *node,
                    samples: samples.intervals.len(),
                    seeded: samples.seeded,
                    mean_ms: mean,
                    stddev_ms: stddev,
                    since_last_ms: since,
                    phi: if samples.intervals.len() < self.min_samples {
                        0.0
                    } else {
                        phi(since, mean, stddev)
                    },
                    last_seq: samples.last_seq,
                }
            })
            .collect()
    }

    /// The normal fit of a member's intervals: the mean, and the deviation floored at a quarter
    /// of the reporting interval so that a perfectly regular reporter is not suspected by the
    /// first jitter
    ///
    /// # Arguments
    ///
    /// * `samples` - The member's samples
    fn fit(&self, samples: &Samples) -> (f64, f64) {
        let count = samples.intervals.len().max(1) as f64;
        let mean = samples.intervals.iter().sum::<f64>() / count;
        let variance = samples
            .intervals
            .iter()
            .map(|interval| (interval - mean).powi(2))
            .sum::<f64>()
            / count;
        let floor = self.interval.as_secs_f64() * 1000.0 / 4.0;
        (mean, variance.sqrt().max(floor))
    }
}

impl Samples {
    /// What a member's first fresh report from a run establishes
    ///
    /// # Arguments
    ///
    /// * `incarnation` - The run
    /// * `seq` - The report's sequence
    /// * `now` - When it arrived
    fn first(incarnation: u64, seq: u64, now: Instant) -> Self {
        Samples {
            last_seq: seq,
            last_incarnation: incarnation,
            last_arrival: now,
            intervals: VecDeque::new(),
            seeded: false,
        }
    }
}

/// Milliseconds from an arrival to now, negative while a grace period has not ended
///
/// # Arguments
///
/// * `arrival` - When the last report arrived, or when the grace ends
/// * `now` - The moment asked about
fn since_ms(arrival: Instant, now: Instant) -> f64 {
    if now >= arrival {
        now.duration_since(arrival).as_secs_f64() * 1000.0
    } else {
        -(arrival.duration_since(now).as_secs_f64() * 1000.0)
    }
}

/// The phi of a silence, under a normal fit of the intervals
///
/// `-log10` of the probability that a report arrives later than `elapsed`, which is the upper
/// tail of the normal distribution at that point. A silence shorter than the mean is barely
/// suspicious; each standard deviation past it is an order of magnitude more so.
///
/// # Arguments
///
/// * `elapsed` - How long the silence has lasted, in milliseconds
/// * `mean` - The mean interval
/// * `stddev` - The deviation, floored
#[must_use]
pub fn phi(elapsed: f64, mean: f64, stddev: f64) -> f64 {
    if elapsed <= 0.0 {
        return 0.0;
    }
    let z = (elapsed - mean) / stddev;
    // the upper tail, from the complementary error function
    let later = 0.5 * erfc(z / std::f64::consts::SQRT_2);
    if later <= 0.0 {
        return PHI_CAP;
    }
    (-later.log10()).min(PHI_CAP)
}

/// The complementary error function, to about seven digits
///
/// Abramowitz and Stegun 7.1.26, which is what the tail of a normal fit needs and no more.
///
/// # Arguments
///
/// * `x` - The argument
fn erfc(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736 + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    let erf = 1.0 - poly * (-x * x).exp();
    1.0 - sign * erf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A detector at a 100 ms interval with five samples needed
    fn detector() -> Detector {
        Detector::new(&FailureDetector {
            interval_ms: 100,
            phi_threshold: 8.0,
            window: 100,
            min_samples: 5,
        })
    }

    /// Phi grows with the silence and is zero before the mean is even reached (F39)
    #[test]
    fn phi_grows_with_silence() {
        assert_eq!(phi(0.0, 100.0, 25.0), 0.0);
        assert!(phi(100.0, 100.0, 25.0) < 1.0);
        let at_three_sigma = phi(175.0, 100.0, 25.0);
        assert!(at_three_sigma > 2.0 && at_three_sigma < 4.0, "{at_three_sigma}");
        assert!(phi(250.0, 100.0, 25.0) > 8.0);
        assert!(phi(100_000.0, 100.0, 25.0) <= PHI_CAP);
        assert!((erfc(0.0) - 1.0).abs() < 1e-6);
        assert!((erfc(1.0) - 0.157_299_2).abs() < 1e-6);
    }

    /// A regular reporter is not suspected until it falls silent; a stale report is counted and
    /// never resets the clock; a newer incarnation starts over (F39)
    #[test]
    fn regular_reports_are_fresh_and_stale_ones_are_ignored() {
        let mut detector = detector();
        let node = NodeId::mint();
        let start = Instant::now();
        let step = Duration::from_millis(100);
        for seq in 1..=6 {
            assert!(detector.observe(node, 1, seq, start + step * seq as u32));
        }
        // five intervals, so a verdict exists and is calm
        let now = start + step * 6;
        assert!(detector.phi(node, now).is_some_and(|phi| phi < 1.0));
        assert!(detector.suspects(now + step).is_empty());
        // a replayed report and one from an older run are ignored
        assert!(!detector.observe(node, 1, 3, now + step));
        assert!(!detector.observe(node, 0, 99, now + step));
        assert_eq!(detector.stale_ignored, 2);
        assert!(detector.suspects(now + step).is_empty());
        // silence is suspected
        assert_eq!(detector.suspects(now + step * 10), vec![node]);
        let view = detector.view(now + step * 10);
        assert_eq!(view[0].samples, 5);
        assert!(view[0].phi > 8.0);
        // a newer run starts over, with no verdict until it has reported enough
        assert!(detector.observe(node, 2, 1, now + step * 10));
        assert!(detector.phi(node, now + step * 20).is_none());
        // and a forgotten member is gone
        detector.forget(node);
        assert!(detector.view(now).is_empty());
    }

    /// A new leader seeds the members it knows: none is suspected during the grace, a silent
    /// one is after it, and the first real report replaces the seed (F39)
    #[test]
    fn a_seeded_member_gets_a_grace_period() {
        let mut detector = detector();
        let node = NodeId::mint();
        let start = Instant::now();
        detector.seed(node, 1, start);
        // inside the grace: no suspicion, whatever the silence
        assert!(detector.suspects(start + Duration::from_millis(400)).is_empty());
        // well past it: suspected
        assert_eq!(detector.suspects(start + Duration::from_millis(900)), vec![node]);
        // a real report replaces the seed, and the member needs real samples again
        assert!(detector.observe(node, 1, 7, start + Duration::from_millis(450)));
        assert!(!detector.view(start)[0].seeded);
        assert!(detector.phi(node, start + Duration::from_secs(5)).is_none());
        // a reset forgets everything
        detector.reset();
        assert!(detector.view(start).is_empty());
    }
}
