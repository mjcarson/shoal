//! Whether Shoal is ahead of another store on one figure, by the lab's rule
//! ([before and after on the lab](../../../../docs/src/performance/benchmarking.md#before-and-after-on-the-lab))
//!
//! A difference is a result only when the two run intervals, lowest to highest, do not overlap;
//! its size is the gap between the nearest ends, as a share of the other store's nearer end, so it
//! is a lower bound and never a difference of medians. This is `shoal_loadgen::compare::judge`
//! with the other store as the baseline and Shoal as the candidate, written again here because the
//! runner half of this crate names no loadgen type; `tests/store_comparison.rs` holds the two to one
//! answer.

/// The lowest and highest of a figure across runs, and the median
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    /// The lowest run
    pub low: f64,
    /// The median run
    pub median: f64,
    /// The highest run
    pub high: f64,
}

impl Interval {
    /// The interval of some runs' values, if there are any
    ///
    /// # Arguments
    ///
    /// * `values` - Each run's value
    #[must_use]
    pub fn of(values: &[f64]) -> Option<Self> {
        // sorted, so the median is the middle value, or the mean of the middle two
        let mut sorted: Vec<f64> = values.iter().copied().filter(|value| value.is_finite()).collect();
        if sorted.is_empty() {
            return None;
        }
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 0 {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        };
        Some(Interval {
            low: sorted[0],
            median,
            high: sorted[sorted.len() - 1],
        })
    }
}

/// What two intervals of one figure say, from Shoal's side
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict {
    /// Shoal is better by at least this share
    Ahead(f64),
    /// Shoal is worse by at least this share
    Behind(f64),
    /// The intervals overlap
    Within,
}

impl Verdict {
    /// The verdict as a table cell says it
    ///
    /// # Arguments
    ///
    /// * `higher_is_better` - Whether a higher value of the figure is better, as it was judged
    #[must_use]
    pub fn cell(&self, higher_is_better: bool) -> String {
        // Shoal's nearer end is above the other's when it is ahead on a figure better higher, or
        // behind on one better lower
        match self {
            Verdict::Ahead(gap) => format!("Shoal ahead ≥{}", size(*gap, higher_is_better)),
            Verdict::Behind(gap) => format!("Shoal behind ≥{}", size(*gap, !higher_is_better)),
            Verdict::Within => "within".to_string(),
        }
    }
}

/// A gap as a cell writes it: a share in percent, or once one nearer end is twice the other or
/// more, the factor between them
///
/// The gap is a share of the other store's nearer end, so Shoal's end above it is that share plus
/// one times it, and below it one less the share. A factor is written whichever side is above, so
/// Shoal six times slower and six times faster read alike. Every gap is a lower bound, so each is
/// rounded down, never up.
///
/// # Arguments
///
/// * `gap` - The share, in percent
/// * `shoal_above` - Whether Shoal's nearer end is above the other store's
fn size(gap: f64, shoal_above: bool) -> String {
    // the factor between the nearer ends, larger over smaller; an end of nothing has none
    let factor = if shoal_above {
        Some(1.0 + gap / 100.0)
    } else if gap < 100.0 {
        Some(100.0 / (100.0 - gap))
    } else {
        None
    };
    // a factor of two or more says more than a share
    if let Some(factor) = factor.filter(|factor| *factor >= 2.0) {
        let factor = (factor * 10.0).floor() / 10.0;
        return format!("{factor:.1}×");
    }
    // a share past ten is a whole number; a small one keeps a decimal
    if gap >= 10.0 {
        format!("{:.0}%", gap.floor())
    } else {
        format!("{:.1}%", (gap * 10.0).floor() / 10.0)
    }
}

/// Judge Shoal's interval against another store's
///
/// # Arguments
///
/// * `other` - The other store's interval
/// * `shoal` - Shoal's
/// * `higher_is_better` - Whether a higher value is better: a rate is, a latency is not
#[must_use]
pub fn judge(other: Interval, shoal: Interval, higher_is_better: bool) -> Verdict {
    // the gap between the nearest ends, as a share of the other store's nearer end
    let gap = |from: f64, to: f64| {
        if from.abs() < f64::EPSILON {
            return 0.0;
        }
        (to - from).abs() / from.abs() * 100.0
    };
    if shoal.low > other.high {
        let gap = gap(other.high, shoal.low);
        return if higher_is_better { Verdict::Ahead(gap) } else { Verdict::Behind(gap) };
    }
    if shoal.high < other.low {
        let gap = gap(other.low, shoal.high);
        return if higher_is_better { Verdict::Behind(gap) } else { Verdict::Ahead(gap) };
    }
    Verdict::Within
}

#[cfg(test)]
mod tests {
    use super::{judge, Interval, Verdict};

    /// An interval of runs: its ends and its median
    #[test]
    fn an_interval_has_its_ends_and_median() {
        let interval = Interval::of(&[3.0, 1.0, 2.0, 10.0]).unwrap();
        assert_eq!((interval.low, interval.median, interval.high), (1.0, 2.5, 10.0));
        assert!(Interval::of(&[]).is_none());
        assert!(Interval::of(&[f64::NAN]).is_none());
    }

    /// Only disjoint intervals are a result, judged by the nearest ends, and a latency is better
    /// lower
    #[test]
    fn verdicts_follow_the_interval_rule() {
        let other = Interval { low: 100.0, median: 105.0, high: 110.0 };
        let ahead = Interval { low: 121.0, median: 125.0, high: 130.0 };
        assert_eq!(judge(other, ahead, true), Verdict::Ahead(10.0));
        assert_eq!(judge(other, ahead, false), Verdict::Behind(10.0));
        let behind = Interval { low: 40.0, median: 45.0, high: 50.0 };
        assert_eq!(judge(other, behind, true), Verdict::Behind(50.0));
        let touching = Interval { low: 110.0, median: 115.0, high: 120.0 };
        assert_eq!(judge(other, touching, true), Verdict::Within);
        assert_eq!(Verdict::Ahead(10.0).cell(true), "Shoal ahead ≥10%");
        assert_eq!(Verdict::Behind(4.25).cell(true), "Shoal behind ≥4.2%");
        // a lower bound is rounded down, and twice the other or more is a factor
        assert_eq!(Verdict::Behind(42.8).cell(true), "Shoal behind ≥42%");
        assert_eq!(Verdict::Behind(386.5).cell(false), "Shoal behind ≥4.8×");
        assert_eq!(Verdict::Ahead(100.0).cell(true), "Shoal ahead ≥2.0×");
        // Shoal's rate below the other's by 84% is a sixth of it, written as the factor it is,
        // as Shoal's p99 under the other's by as much is
        assert_eq!(Verdict::Behind(84.0).cell(true), "Shoal behind ≥6.2×");
        assert_eq!(Verdict::Ahead(84.0).cell(false), "Shoal ahead ≥6.2×");
        // and a share under half is a share whichever side is above
        assert_eq!(Verdict::Ahead(49.0).cell(false), "Shoal ahead ≥49%");
    }
}
