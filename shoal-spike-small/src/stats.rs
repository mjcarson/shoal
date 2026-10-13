//! Latency summaries for one side, and intervals across rounds
//!
//! Copied from X10's harness (`shoal-spike-rows/src/stats.rs`), which took X6's with the latencies
//! kept in an `hdrhistogram` rather than a vector: a cell here can answer a few hundred thousand
//! writes. A figure is compared across rounds by its interval, the
//! lowest and the highest round with the median between, and the lab's rule is that a difference
//! counts only when two intervals do not overlap.

use hdrhistogram::Histogram;
use std::time::Duration;

/// A side's latencies, in nanoseconds
#[derive(Debug, Clone)]
pub struct Samples(pub Histogram<u64>);

impl Default for Samples {
    /// An empty set of latencies, from a microsecond to a minute at three significant digits
    fn default() -> Self {
        // a minute is far past any answer the driver waits for
        Samples(Histogram::new_with_bounds(1_000, 60_000_000_000, 3).expect("the bounds are valid"))
    }
}

impl Samples {
    /// Add one latency
    ///
    /// # Arguments
    ///
    /// * `took` - The latency
    pub fn push(&mut self, took: Duration) {
        // clamped into the histogram's range, so a sub-microsecond or a minute long answer is
        // counted at the edge rather than lost
        let nanos = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX).clamp(1_000, 60_000_000_000);
        self.0.record(nanos).expect("the value was clamped into range");
    }

    /// Add every latency of another set
    ///
    /// # Arguments
    ///
    /// * `other` - The other set
    pub fn extend(&mut self, other: &Samples) {
        self.0.add(&other.0).expect("both sets share their bounds");
    }

    /// How many latencies were added
    #[must_use]
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// Whether nothing was added
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The percentiles of these latencies, in microseconds
    #[must_use]
    pub fn summary(&self) -> Summary {
        // an empty side reads as zeros rather than as the histogram's edge
        if self.0.is_empty() {
            return Summary::default();
        }
        // each read at its quantile and turned from nanoseconds to microseconds
        let at = |q: f64| self.0.value_at_quantile(q) as f64 / 1e3;
        Summary {
            n: self.0.len(),
            p50: at(0.5),
            p99: at(0.99),
            p999: at(0.999),
            max: self.0.max() as f64 / 1e3,
            mean: self.0.mean() / 1e3,
        }
    }
}

/// The percentiles of one side's latencies, in microseconds
#[derive(Debug, Clone, Copy, Default)]
pub struct Summary {
    /// How many samples
    pub n: u64,
    /// The median
    pub p50: f64,
    /// The 99th percentile
    pub p99: f64,
    /// The 99.9th percentile
    pub p999: f64,
    /// The slowest
    pub max: f64,
    /// The mean
    pub mean: f64,
}

/// A figure across rounds: its lowest, its median and its highest
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    /// The lowest round
    pub min: f64,
    /// The median round
    pub median: f64,
    /// The highest round
    pub max: f64,
    /// How many rounds
    pub rounds: usize,
}

impl Interval {
    /// The interval of a figure's rounds
    ///
    /// # Arguments
    ///
    /// * `values` - One value a round
    #[must_use]
    pub fn of(values: &[f64]) -> Option<Interval> {
        // no rounds, no interval
        if values.is_empty() {
            return None;
        }
        // sorted, so the ends and the middle can be read
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 0 {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        };
        Some(Interval {
            min: sorted[0],
            median,
            max: sorted[sorted.len() - 1],
            rounds: sorted.len(),
        })
    }

    /// Whether this interval lies wholly above a value
    ///
    /// # Arguments
    ///
    /// * `line` - The value
    #[must_use]
    pub fn wholly_above(&self, line: f64) -> bool {
        self.min > line
    }

    /// Whether this interval lies wholly below a value
    ///
    /// # Arguments
    ///
    /// * `line` - The value
    #[must_use]
    pub fn wholly_below(&self, line: f64) -> bool {
        self.max < line
    }

    /// The interval written as `median [min–max]`
    #[must_use]
    pub fn show(&self) -> String {
        // one round has no spread to show
        if self.rounds <= 1 {
            fmt(self.median)
        } else {
            format!("{} [{}–{}]", fmt(self.median), fmt(self.min), fmt(self.max))
        }
    }
}

/// A figure with as many significant digits as a table needs
///
/// # Arguments
///
/// * `value` - The figure
#[must_use]
pub fn fmt(value: f64) -> String {
    // fewer decimals the larger the figure
    let size = value.abs();
    if size == 0.0 {
        "0".to_string()
    } else if size >= 1000.0 {
        format!("{value:.0}")
    } else if size >= 100.0 {
        format!("{value:.1}")
    } else if size >= 1.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.3}")
    }
}

/// A seeded generator: SplitMix64
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// A generator from a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// The next value
    pub fn next(&mut self) -> u64 {
        // SplitMix64's step and output function
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    /// A value below a bound
    ///
    /// # Arguments
    ///
    /// * `bound` - The bound, which must not be zero
    pub fn below(&mut self, bound: u64) -> u64 {
        // the bias of a modulus is far below anything a measurement here can see
        self.next() % bound
    }
}

/// SplitMix64's output function, which turns a counter into a well spread value
///
/// # Arguments
///
/// * `value` - The value to mix
#[must_use]
pub fn mix(value: u64) -> u64 {
    // the three xor-shift-multiply steps of SplitMix64
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An interval's ends and median are the rounds' lowest, middle and highest
    #[test]
    fn an_interval_reads_its_rounds() {
        // four rounds, so the median is the mean of the middle two
        let interval = Interval::of(&[4.0, 1.0, 3.0, 2.0]).expect("rounds were given");
        assert_eq!(interval.min, 1.0);
        assert_eq!(interval.max, 4.0);
        assert_eq!(interval.median, 2.5);
        assert!(interval.wholly_above(0.9));
        assert!(!interval.wholly_above(1.0));
        assert!(interval.wholly_below(4.1));
    }

    /// A summary reads its percentiles in microseconds
    #[test]
    fn a_summary_is_in_microseconds() {
        // a hundred latencies of one to a hundred milliseconds
        let mut samples = Samples::default();
        for ms in 1..=100 {
            samples.push(Duration::from_millis(ms));
        }
        let summary = samples.summary();
        assert_eq!(summary.n, 100);
        // three significant digits, so within a part in a thousand
        assert!((summary.p50 - 50_000.0).abs() < 100.0, "{summary:?}");
        assert!((summary.max - 100_000.0).abs() < 100.0, "{summary:?}");
    }
}
