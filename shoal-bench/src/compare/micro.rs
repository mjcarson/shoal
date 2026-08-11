//! Judging a micro capture against a baseline
//!
//! # The band inside which a difference is not a result
//!
//! This is **not** criterion's confidence interval. That interval describes how stable the
//! samples were within one process, and it is far too narrow to use here. Whatever differs
//! between two processes - allocator layout, code and data alignment, ASLR, thermal state - is
//! invisible to a measurement taken inside one, so a tight interval is not evidence: across four
//! identical repeats `partition_sorted/get_key/4096` moved 22% and reported its outlying value
//! with a ±0.2% interval, tighter than any of the runs it disagreed with. (The CPU governor was
//! suspected and then measured; it makes no detectable difference.)
//!
//! The band is tiered by how long the benchmark takes, because instability scales inversely with
//! duration. Measured across four identical repeats, by duration bucket:
//!
//! | Duration | Median spread | Worst spread |
//! | --- | --- | --- |
//! | <100 ns | 4.6% | 8.8% |
//! | 100 ns - 1 µs | 3.4% | 7.8% |
//! | 1 - 20 µs | 1.8% | 3.8% |
//! | >20 µs | 1.2% | 4.3% |
//!
//! A fixed-cost perturbation - a cache line landing differently, a branch predictor entry - is a
//! large fraction of a 25 ns benchmark and a rounding error in a 200 µs one. One global threshold
//! is therefore either too loose for the slow benchmarks or too tight for the fast ones.
//!
//! **These are screens, not verdicts.** They do not bound the worst case, and a result is
//! accepted only after a confirming repeat of the whole capture. See
//! `docs/src/operations/performance-baseline.md#what-the-micro-layer-can-actually-resolve`.
//!
//! Those four bucket figures are also what the noise-band chart on the generated page is drawn
//! against, so this table and that chart cannot drift apart without a test noticing.

use serde::Serialize;

use crate::model::micro::MicroCapture;

/// The band applied to a benchmark faster than [`NoiseBand::fast_threshold_ns`]
pub const NOISE_FAST_PCT: f64 = 9.0;

/// The band applied to a benchmark at or slower than [`NoiseBand::fast_threshold_ns`]
pub const NOISE_SLOW_PCT: f64 = 5.0;

/// Where the fast tier gives way to the slow one, in nanoseconds
pub const FAST_THRESHOLD_NS: f64 = 1000.0;

/// How wide a difference has to be before it is called a result
#[derive(Debug, Clone, Copy)]
pub struct NoiseBand {
    /// One flat band to apply to everything, overriding both tiers
    pub override_pct: Option<f64>,
}

impl Default for NoiseBand {
    /// The tiered band, which is what a comparison uses unless told otherwise
    fn default() -> Self {
        NoiseBand {
            override_pct: None,
        }
    }
}

impl NoiseBand {
    /// A band that applies one flat percentage to every benchmark
    ///
    /// # Arguments
    ///
    /// * `pct` - The percentage to apply
    pub fn flat(pct: f64) -> Self {
        NoiseBand {
            override_pct: Some(pct),
        }
    }

    /// The band that applies to a benchmark of a given duration
    ///
    /// The tier is chosen from the **baseline** duration rather than the measured one, so that
    /// the tier cannot move with the result it is being used to judge. A change that crossed the
    /// threshold would otherwise be judged by a band the change itself selected.
    ///
    /// # Arguments
    ///
    /// * `baseline_ns` - How long the benchmark took in the baseline
    pub fn band_for(&self, baseline_ns: f64) -> f64 {
        // an explicit override replaces both tiers
        if let Some(pct) = self.override_pct {
            return pct;
        }
        // otherwise pick the tier from how long the baseline took
        if baseline_ns < FAST_THRESHOLD_NS {
            NOISE_FAST_PCT
        } else {
            NOISE_SLOW_PCT
        }
    }

    /// How this band should be described in a heading
    pub fn describe(&self) -> String {
        // say which band is in force, so a table can never be read against the wrong one
        match self.override_pct {
            Some(pct) => format!("noise band ±{pct}%"),
            None => format!(
                "noise band ±{NOISE_FAST_PCT}% under {FAST_THRESHOLD_NS:.0}ns, ±{NOISE_SLOW_PCT}% above"
            ),
        }
    }
}

/// One benchmark's movement between a baseline and a run
#[derive(Debug, Clone, Serialize)]
pub struct MicroRow {
    /// The criterion id both sides were joined on
    pub name: String,
    /// What the baseline measured, in nanoseconds
    pub baseline_ns: f64,
    /// What the run measured, in nanoseconds
    pub run_ns: f64,
    /// The difference, in nanoseconds
    pub delta_ns: f64,
    /// The difference as a percentage of the baseline
    pub pct: f64,
    /// The band this row was judged against
    pub band_pct: f64,
    /// Whether the difference is wider than the band
    pub significant: bool,
}

impl MicroRow {
    /// Whether this row is a regression: significant, and in the slower direction
    pub fn is_regression(&self) -> bool {
        // only a significant move counts, and only one that made things slower
        self.significant && self.pct > 0.0
    }
}

/// Everything one comparison found
#[derive(Debug, Clone, Serialize)]
pub struct MicroComparison {
    /// Every benchmark present on both sides, sorted by percentage change ascending
    ///
    /// Ascending means the biggest improvement is at the top and the worst regression at the
    /// bottom, which is the order the result of a change is usually read in.
    pub rows: Vec<MicroRow>,
    /// Benchmarks the baseline has that this run does not
    pub only_in_baseline: Vec<String>,
    /// Benchmarks this run has that the baseline does not
    pub only_in_run: Vec<String>,
}

impl MicroComparison {
    /// Whether anything moved outside its band in the slower direction
    pub fn has_regression(&self) -> bool {
        // any single significant slowdown is enough
        self.rows.iter().any(MicroRow::is_regression)
    }
}

/// Compares a micro capture against a baseline
///
/// Only benchmarks present on both sides can be compared, and the two set differences are
/// reported rather than dropped. A benchmark that silently vanished from a comparison is how a
/// regression gets missed - and it is not hypothetical here: the frozen baseline holds 35
/// benchmarks and the current bench file declares 59.
///
/// # Arguments
///
/// * `run` - The capture being judged
/// * `baseline` - What to judge it against
/// * `band` - How wide a difference has to be before it counts
pub fn compare(run: &MicroCapture, baseline: &MicroCapture, band: &NoiseBand) -> MicroComparison {
    // join the two on the benchmark id, which is criterion's full_id on both sides
    let mut rows = Vec::new();
    for (name, measured) in &run.benchmarks {
        // a benchmark the baseline never measured cannot be compared, only reported
        let Some(before) = baseline.benchmarks.get(name) else {
            continue;
        };
        // the movement, and what fraction of the baseline it is
        let delta_ns = measured.mean_ns - before.mean_ns;
        let pct = if before.mean_ns > 0.0 {
            delta_ns / before.mean_ns * 100.0
        } else {
            0.0
        };
        // judged against the tier the baseline's own duration selects
        let band_pct = band.band_for(before.mean_ns);
        rows.push(MicroRow {
            name: name.clone(),
            baseline_ns: before.mean_ns,
            run_ns: measured.mean_ns,
            delta_ns,
            pct,
            band_pct,
            significant: pct.abs() > band_pct,
        });
    }
    // biggest improvement first, breaking ties on the name so the order is stable
    rows.sort_by(|left, right| {
        left.pct
            .partial_cmp(&right.pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
    });
    // then whatever only one side has, which is the part a join would otherwise hide
    let only_in_baseline = baseline
        .benchmarks
        .keys()
        .filter(|name| !run.benchmarks.contains_key(*name))
        .cloned()
        .collect();
    let only_in_run = run
        .benchmarks
        .keys()
        .filter(|name| !baseline.benchmarks.contains_key(*name))
        .cloned()
        .collect();
    MicroComparison {
        rows,
        only_in_baseline,
        only_in_run,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::micro::{MICRO_VERSION, MicroStat};

    /// Builds a capture from `(name, mean_ns)` pairs
    ///
    /// # Arguments
    ///
    /// * `entries` - What the capture measured
    fn capture(entries: &[(&str, f64)]) -> MicroCapture {
        // the interval is carried but never judged against, so it is filled in from the mean
        let benchmarks: BTreeMap<String, MicroStat> = entries
            .iter()
            .map(|(name, mean)| {
                (
                    name.to_string(),
                    MicroStat {
                        mean_ns: *mean,
                        lower_ns: *mean,
                        upper_ns: *mean,
                        median_ns: *mean,
                    },
                )
            })
            .collect();
        MicroCapture {
            version: MICRO_VERSION,
            captured: "2026-08-09T00:00:00Z".to_string(),
            benchmarks,
        }
    }

    /// The tier boundary is exactly at the threshold, and the slow tier owns it
    #[test]
    fn the_tier_boundary_is_where_it_says_it_is() {
        let band = NoiseBand::default();
        assert_eq!(band.band_for(999.9), NOISE_FAST_PCT);
        assert_eq!(band.band_for(1000.0), NOISE_SLOW_PCT);
        assert_eq!(band.band_for(1000.1), NOISE_SLOW_PCT);
    }

    /// The tier comes from the baseline, so a change cannot select the band that judges it
    #[test]
    fn the_tier_is_chosen_from_the_baseline_not_the_run() {
        // a benchmark that was fast and became slow is still judged by the fast tier
        let comparison = compare(
            &capture(&[("x", 1100.0)]),
            &capture(&[("x", 900.0)]),
            &NoiseBand::default(),
        );
        assert_eq!(comparison.rows[0].band_pct, NOISE_FAST_PCT);
        // and one that was slow and became fast is still judged by the slow tier
        let comparison = compare(
            &capture(&[("x", 900.0)]),
            &capture(&[("x", 1100.0)]),
            &NoiseBand::default(),
        );
        assert_eq!(comparison.rows[0].band_pct, NOISE_SLOW_PCT);
    }

    /// An override replaces both tiers
    #[test]
    fn an_override_replaces_both_tiers() {
        let band = NoiseBand::flat(1.0);
        assert_eq!(band.band_for(10.0), 1.0);
        assert_eq!(band.band_for(1_000_000.0), 1.0);
    }

    /// A difference is only a result when it is wider than its band
    #[test]
    fn only_a_difference_wider_than_the_band_is_a_result() {
        // 8% on a fast benchmark is inside the 9% band
        let comparison = compare(
            &capture(&[("fast", 108.0)]),
            &capture(&[("fast", 100.0)]),
            &NoiseBand::default(),
        );
        assert!(!comparison.rows[0].significant);
        // the same 8% on a slow one is outside the 5% band
        let comparison = compare(
            &capture(&[("slow", 10_800.0)]),
            &capture(&[("slow", 10_000.0)]),
            &NoiseBand::default(),
        );
        assert!(comparison.rows[0].significant);
        assert!(comparison.rows[0].is_regression());
    }

    /// An improvement wider than the band is significant but is not a regression
    #[test]
    fn an_improvement_is_significant_without_being_a_regression() {
        let comparison = compare(
            &capture(&[("x", 5_000.0)]),
            &capture(&[("x", 10_000.0)]),
            &NoiseBand::default(),
        );
        assert!(comparison.rows[0].significant);
        assert!(!comparison.rows[0].is_regression());
        assert!(!comparison.has_regression());
    }

    /// Rows come back ordered from the biggest improvement to the worst regression
    #[test]
    fn rows_are_sorted_by_change() {
        let comparison = compare(
            &capture(&[("slower", 200.0), ("faster", 50.0), ("same", 100.0)]),
            &capture(&[("slower", 100.0), ("faster", 100.0), ("same", 100.0)]),
            &NoiseBand::default(),
        );
        let order: Vec<&str> = comparison
            .rows
            .iter()
            .map(|row| row.name.as_str())
            .collect();
        assert_eq!(order, vec!["faster", "same", "slower"]);
    }

    /// A benchmark on only one side is reported rather than quietly dropped
    #[test]
    fn set_differences_are_reported_both_ways() {
        let comparison = compare(
            &capture(&[("kept", 100.0), ("added", 100.0)]),
            &capture(&[("kept", 100.0), ("removed", 100.0)]),
            &NoiseBand::default(),
        );
        assert_eq!(comparison.rows.len(), 1);
        assert_eq!(comparison.only_in_baseline, vec!["removed".to_string()]);
        assert_eq!(comparison.only_in_run, vec!["added".to_string()]);
    }

    /// A zero baseline is not a division, and is not a change either
    #[test]
    fn a_zero_baseline_is_not_an_infinite_change() {
        let comparison = compare(
            &capture(&[("x", 100.0)]),
            &capture(&[("x", 0.0)]),
            &NoiseBand::default(),
        );
        assert_eq!(comparison.rows[0].pct, 0.0);
        assert!(!comparison.rows[0].significant);
    }
}
