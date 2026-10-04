//! Percentiles of one side, and intervals across rounds
//!
//! A side's latencies are summarised by percentiles. A figure is then compared across rounds by
//! its interval: the lowest and the highest round, with the median between. The lab's rule is
//! that a difference counts only when two sides' intervals do not overlap.

use std::time::Duration;

/// A side's latencies, in nanoseconds
#[derive(Debug, Clone, Default)]
pub struct Samples(pub Vec<u64>);

impl Samples {
    /// Add one latency
    ///
    /// # Arguments
    ///
    /// * `took` - The latency
    pub fn push(&mut self, took: Duration) {
        self.0.push(took.as_nanos() as u64);
    }

    /// Add every latency of another set
    ///
    /// # Arguments
    ///
    /// * `other` - The other set
    pub fn extend(&mut self, other: &Samples) {
        self.0.extend_from_slice(&other.0);
    }

    /// The percentiles of these latencies
    #[must_use]
    pub fn summary(&self) -> Summary {
        // sorted once, read at each rank
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        Summary {
            n: sorted.len(),
            p50: at(&sorted, 50.0),
            p99: at(&sorted, 99.0),
            p999: at(&sorted, 99.9),
            max: sorted.last().copied().unwrap_or(0) as f64 / 1e3,
        }
    }
}

/// The value at a percentile of sorted samples, in microseconds
///
/// # Arguments
///
/// * `sorted` - The samples, ascending
/// * `p` - The percentile, 0 to 100
fn at(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    // the nearest rank, as the rest of the spike computes it
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)] as f64 / 1e3
}

/// The percentiles of one side's latencies, in microseconds
#[derive(Debug, Clone, Copy, Default)]
pub struct Summary {
    /// How many samples
    pub n: usize,
    /// The median
    pub p50: f64,
    /// The 99th percentile
    pub p99: f64,
    /// The 99.9th percentile
    pub p999: f64,
    /// The slowest
    pub max: f64,
}

impl Summary {
    /// The 99th percentile when there are enough samples to have one, else the slowest
    #[must_use]
    pub fn tail(&self) -> f64 {
        if self.n >= 100 {
            self.p99
        } else {
            self.max
        }
    }
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

    /// Whether this interval lies wholly above another
    ///
    /// # Arguments
    ///
    /// * `other` - The other interval
    #[must_use]
    pub fn above(&self, other: &Interval) -> bool {
        self.min > other.max
    }

    /// The interval written as `median [min–max]`
    #[must_use]
    pub fn show(&self) -> String {
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

/// A seeded generator for offsets and payloads: SplitMix64
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
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value below a bound
    ///
    /// # Arguments
    ///
    /// * `bound` - The bound, above zero
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// Fill a buffer with bytes nobody would mistake for zeros
    ///
    /// # Arguments
    ///
    /// * `bytes` - The buffer
    pub fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next().to_ne_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}
