//! Latency samples, and the distribution they summarize into
//!
//! The percentile arithmetic here came from `shoal::bencher`, which the `tmdb` example used and
//! nothing else did. What did not come with it is that module's other half - loading a prior
//! result, refusing it on a version mismatch, and printing a coloured delta against it. This crate
//! has owned comparison since F7, so a second comparison engine in the measured binary is one
//! nobody reads and one that can disagree with the one that counts.
//!
//! # One struct, not a mirror of one
//!
//! [`crate::model::macro_layer::Stats`] used to be a hand kept copy of `shoal::bencher::Stats`,
//! with a `serde(flatten)` catch-all and a test over every committed artifact to notice when the
//! two drifted. They cannot drift now: the writer builds the reader's struct. That deletes a whole
//! category of bug rather than detecting it, which is why the workloads live in this crate at all.

use std::time::Duration;

use crate::model::macro_layer::{DurationParts, Stats};

/// The latency samples for one operation
///
/// Samples are kept per operation rather than pooled, because an insert waits on the intent log
/// and a get may not touch disk at all. A percentile over the two together does not describe
/// either of them - it reports where the boundary between the two distributions happens to land.
#[derive(Debug, Default)]
pub struct Samples {
    /// Every sample taken, in the order they were taken
    times: Vec<Duration>,
}

impl Samples {
    /// Creates a sample set with room for a known number of samples
    ///
    /// # Arguments
    ///
    /// * `expected` - Roughly how many samples this will hold, to size the allocation once
    pub fn with_capacity(expected: usize) -> Self {
        Samples {
            times: Vec::with_capacity(expected),
        }
    }

    /// Records one sample
    ///
    /// # Arguments
    ///
    /// * `elapsed` - How long the operation took
    pub fn record(&mut self, elapsed: Duration) {
        self.times.push(elapsed);
    }

    /// Takes another sample set's samples into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The set to drain into this one
    pub fn absorb(&mut self, other: &mut Samples) {
        self.times.append(&mut other.times);
    }

    /// How many samples have been recorded
    pub fn len(&self) -> usize {
        self.times.len()
    }

    /// Whether nothing has been recorded
    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    /// Summarizes these samples into the distribution the artifact stores
    ///
    /// Sorts in place. An empty set yields an all zero distribution with a count of zero rather
    /// than panicking, since a workload that only writes leaves its read set empty.
    pub fn summarize(&mut self) -> Stats {
        // an empty set has no percentiles, and saying so is better than refusing to finish a run
        if self.times.is_empty() {
            return Stats {
                count: 0,
                max: DurationParts { secs: 0, nanos: 0 },
                p99: DurationParts { secs: 0, nanos: 0 },
                p95: DurationParts { secs: 0, nanos: 0 },
                p90: DurationParts { secs: 0, nanos: 0 },
                p50: DurationParts { secs: 0, nanos: 0 },
                avg: DurationParts { secs: 0, nanos: 0 },
                min: DurationParts { secs: 0, nanos: 0 },
            };
        }
        // sort so percentiles are an index rather than a search
        self.times.sort_unstable();
        // sum in nanoseconds, in a width that a long run of slow samples cannot overflow
        let sum: u128 = self.times.iter().map(|time| time.as_nanos()).sum();
        let avg = Duration::from_nanos((sum / self.times.len() as u128) as u64);
        Stats {
            count: self.times.len() as u64,
            // the set is sorted, so the extremes are its ends
            max: parts(self.times[self.times.len() - 1]),
            p99: parts(percentile(&self.times, 0.99)),
            p95: parts(percentile(&self.times, 0.95)),
            p90: parts(percentile(&self.times, 0.90)),
            p50: parts(percentile(&self.times, 0.50)),
            avg: parts(avg),
            min: parts(self.times[0]),
        }
    }
}

/// Splits a duration into the two field shape the artifact stores it in
///
/// # Arguments
///
/// * `duration` - The duration to split
fn parts(duration: Duration) -> DurationParts {
    // this is exactly how serde writes a `Duration`, which is the shape every committed artifact
    // already uses
    DurationParts {
        secs: duration.as_secs(),
        nanos: duration.subsec_nanos(),
    }
}

/// The sample at a percentile, by nearest rank
///
/// For `n` samples the rank of a percentile `p` is `ceil(n * p)`, which is one based. The samples
/// are zero indexed, so the index is one below the rank.
///
/// # Arguments
///
/// * `sorted` - The samples to index, already sorted ascending and never empty
/// * `percentile` - The percentile to read, between 0 and 1
fn percentile(sorted: &[Duration], percentile: f64) -> Duration {
    // the one based rank of this percentile
    let rank = (sorted.len() as f64 * percentile).ceil() as usize;
    // and the zero based index it names, clamped into the set so p0 and p100 both land inside it
    let index = std::cmp::min(rank.saturating_sub(1), sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::{percentile, Samples};
    use std::time::Duration;

    /// Builds 100 samples of 1ms through 100ms
    fn samples() -> Vec<Duration> {
        (1..=100).map(Duration::from_millis).collect()
    }

    /// Builds a sample set holding the given durations
    ///
    /// # Arguments
    ///
    /// * `times` - The samples to load
    fn loaded(times: Vec<Duration>) -> Samples {
        let mut set = Samples::with_capacity(times.len());
        for time in times {
            set.record(time);
        }
        set
    }

    /// Percentiles use nearest rank against a zero indexed sample set
    ///
    /// The rank of a percentile is `ceil(n * p)` and is one based, so indexing a zero based slice
    /// with it directly reports the sample one rank too high.
    #[test]
    fn percentile_uses_nearest_rank() {
        let sorted = samples();
        assert_eq!(percentile(&sorted, 0.50), Duration::from_millis(50));
        assert_eq!(percentile(&sorted, 0.90), Duration::from_millis(90));
        assert_eq!(percentile(&sorted, 0.95), Duration::from_millis(95));
        assert_eq!(percentile(&sorted, 0.99), Duration::from_millis(99));
    }

    /// The extremes of a percentile range stay inside the sample set
    #[test]
    fn percentile_clamps_at_both_ends() {
        let sorted = samples();
        assert_eq!(percentile(&sorted, 0.0), Duration::from_millis(1));
        assert_eq!(percentile(&sorted, 1.0), Duration::from_millis(100));
    }

    /// A single sample is its own percentile
    #[test]
    fn percentile_of_one_sample() {
        let sorted = vec![Duration::from_millis(7)];
        assert_eq!(percentile(&sorted, 0.50), Duration::from_millis(7));
        assert_eq!(percentile(&sorted, 0.99), Duration::from_millis(7));
    }

    /// A distribution summarizes its samples without needing them recorded in order
    #[test]
    fn stats_summarize_unsorted_samples() {
        let mut times = samples();
        times.reverse();
        let stats = loaded(times).summarize();
        assert_eq!(stats.count, 100);
        assert_eq!(stats.min.as_nanos(), 1_000_000);
        assert_eq!(stats.max.as_nanos(), 100_000_000);
        assert_eq!(stats.p50.as_nanos(), 50_000_000);
        assert_eq!(stats.p99.as_nanos(), 99_000_000);
        // 1..=100 averages to 50.5ms
        assert_eq!(stats.avg.as_nanos(), 50_500_000);
    }

    /// An empty sample set yields a zeroed distribution rather than panicking
    ///
    /// Keeping the operations apart makes this reachable: a workload that only inserts leaves its
    /// get set empty.
    #[test]
    fn stats_handle_no_samples() {
        let stats = Samples::default().summarize();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.max.as_nanos(), 0);
        assert_eq!(stats.p99.as_nanos(), 0);
    }

    /// Absorbing another set pools every sample from both
    #[test]
    fn absorbing_pools_both_sets() {
        let mut left = loaded((1..=50).map(Duration::from_millis).collect());
        let mut right = loaded((51..=100).map(Duration::from_millis).collect());
        left.absorb(&mut right);
        assert_eq!(left.len(), 100);
        assert!(right.is_empty(), "an absorbed set keeps nothing back");
        let stats = left.summarize();
        assert_eq!(stats.p50.as_nanos(), 50_000_000);
    }
}
