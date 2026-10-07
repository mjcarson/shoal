//! A figure across rounds as an interval, and a figure written as a table needs it
//!
//! Copied from X10's harness (`shoal-spike-rows/src/stats.rs`), which copied X6's. A figure is
//! compared across rounds by its interval, the lowest and the highest round with the median
//! between, and the lab's rule is that a difference counts only when two intervals do not
//! overlap. Latencies are not kept here: every arm's are shoal-loadgen's own windows.

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
        let median = if sorted.len().is_multiple_of(2) {
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


#[cfg(test)]
mod tests {
    use super::*;

    /// An interval's ends are the lowest and highest rounds, and its median the middle one's
    #[test]
    fn an_interval_reads_its_rounds() {
        let interval = Interval::of(&[4.0, 1.0, 3.0, 2.0]).expect("rounds");
        assert_eq!((interval.min, interval.median, interval.max), (1.0, 2.5, 4.0));
        assert!(interval.wholly_above(0.5));
        assert!(!interval.wholly_above(1.0));
        assert!(interval.wholly_below(4.5));
        assert!(Interval::of(&[]).is_none());
    }
}
