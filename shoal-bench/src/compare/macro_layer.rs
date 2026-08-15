//! Judging a macro capture against another one
//!
//! The shell scripts this replaces could not do this at all. `scripts/compare.sh` diffed two maps
//! of criterion estimates and nothing else, so the layer that measures what a client actually
//! experiences was captured, committed, and never compared to anything.
//!
//! # Why the band here is not a percentage
//!
//! The micro layer's 9%/5% tiers were fitted to criterion repeats and mean nothing here. The
//! macro layer's own spread is wider than most changes anyone would try: the frozen
//! `B1-performance` capture ran five times and spread 10.5% between its fastest and slowest run.
//! A fixed percentage band would therefore be either wider than every change worth making, or
//! narrower than the noise it is supposed to screen out.
//!
//! So a macro difference is a result only when the two captures' **observed intervals are
//! disjoint** - when the slowest run of one side is still faster than the fastest run of the
//! other. The effect reported is the gap between the nearest endpoints, which is a conservative
//! lower bound on the change rather than the difference between the medians.
//!
//! This is not a new protocol. It is the test F5's interleaved A/B experiment was judged by when
//! it was run by hand: gated 1,823 ms, ungated 1,820 ms, gated again 1,857 ms - two runs of the
//! same arm further apart than either was from the other arm, therefore no result. Encoding it
//! means the tool now applies the standard the documentation already describes.

//! # What a comparison joins on
//!
//! The workload identifier, and then the operation name within it. Both sides of a comparison are
//! walked, and a workload present on only one of them is reported as
//! [`MacroVerdict::Absent`] rather than silently dropped - a workload that vanished from a capture
//! is exactly how a regression goes unnoticed, and it is also what happens when a version 1
//! capture is compared against a version 2 one.

use std::collections::BTreeSet;

use serde::Serialize;

use crate::model::macro_layer::{MacroCaptureV2, STAT_METRICS, WorkloadCapture};

/// What a comparison of one macro metric concluded
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MacroVerdict {
    /// The intervals are disjoint, so the difference is larger than either side's spread
    Result,
    /// The intervals overlap, so nothing can be concluded
    NotAResult,
    /// At least one side has no interval, so nothing can be concluded and nothing was screened
    ///
    /// Every capture taken before this tool existed is in this state for the percentile metrics:
    /// `scripts/bench.sh` kept only each run's wall clock, so the percentiles of the run it chose
    /// have no error bar and no way to acquire one after the fact.
    NoErrorBar,
    /// The workload is on one side of the comparison and not the other
    ///
    /// Never a result, and never silence either. A workload that stopped being captured looks
    /// exactly like one that never regressed, and the whole point of naming what is missing is
    /// that those two are different.
    Absent,
}

/// One macro metric's movement between two captures
#[derive(Debug, Clone, Serialize)]
pub struct MacroRow {
    /// The metric this row describes, such as `macro/insert/p99`
    pub metric: String,
    /// Whether a larger number is better for this metric
    ///
    /// True for throughput and false for everything else, which is what decides whether a
    /// disjoint pair of intervals is an improvement or a regression.
    pub higher_is_better: bool,
    /// What the baseline's kept run measured
    pub baseline: f64,
    /// What this run's kept run measured
    pub run: f64,
    /// The baseline's observed interval, if it ran more than once
    pub baseline_interval: Option<(f64, f64)>,
    /// This run's observed interval, if it ran more than once
    pub run_interval: Option<(f64, f64)>,
    /// The difference between the two kept runs, as a percentage of the baseline's
    pub pct: f64,
    /// What the comparison concluded
    pub verdict: MacroVerdict,
    /// The gap between the nearest endpoints of two disjoint intervals
    ///
    /// A conservative lower bound on the size of the change. `None` unless the verdict is
    /// [`MacroVerdict::Result`].
    pub gap: Option<f64>,
}

impl MacroRow {
    /// Whether this row is a regression: a result, in the worse direction
    pub fn is_regression(&self) -> bool {
        // only a disjoint pair of intervals is a result at all
        if self.verdict != MacroVerdict::Result {
            return false;
        }
        // then the direction depends on what the metric measures
        if self.higher_is_better {
            self.run < self.baseline
        } else {
            self.run > self.baseline
        }
    }
}

/// Everything one macro comparison found
#[derive(Debug, Clone, Serialize)]
pub struct MacroComparison {
    /// One row per metric, in a fixed order
    pub rows: Vec<MacroRow>,
    /// Workloads the run has that the baseline does not
    pub only_in_run: Vec<String>,
    /// Workloads the baseline has that the run does not
    pub only_in_baseline: Vec<String>,
    /// Whether the two captures have no workload in common at all
    ///
    /// The state a version 2 capture is in when compared against one of the seven taken before
    /// purpose built workloads existed. Reported explicitly, because an empty table of rows is
    /// otherwise indistinguishable from a comparison in which nothing moved.
    pub disjoint: bool,
}

impl MacroComparison {
    /// Whether any metric moved in the worse direction by more than both spreads
    pub fn has_regression(&self) -> bool {
        // any single one is enough
        self.rows.iter().any(MacroRow::is_regression)
    }

    /// Every workload that is on one side and not the other
    pub fn missing(&self) -> Vec<&str> {
        // both directions matter: a workload that appeared is as much a change to what was
        // measured as one that vanished
        self.only_in_run
            .iter()
            .chain(self.only_in_baseline.iter())
            .map(String::as_str)
            .collect()
    }
}

/// Whether two intervals are disjoint, and by how much
///
/// # Arguments
///
/// * `left` - One interval, as an inclusive low and high
/// * `right` - The other interval
fn disjoint_gap(left: (f64, f64), right: (f64, f64)) -> Option<f64> {
    // left entirely below right
    if left.1 < right.0 {
        return Some(right.0 - left.1);
    }
    // or entirely above it
    if right.1 < left.0 {
        return Some(left.0 - right.1);
    }
    // otherwise they overlap, and the difference is inside the noise of one or both
    None
}

/// Builds one row from two measurements and their intervals
///
/// # Arguments
///
/// * `metric` - What the row describes
/// * `higher_is_better` - Whether a larger number is an improvement
/// * `baseline` - What the baseline's kept run measured
/// * `run` - What this run's kept run measured
/// * `baseline_interval` - The baseline's observed interval, if it has one
/// * `run_interval` - This run's observed interval, if it has one
fn row(
    metric: &str,
    higher_is_better: bool,
    baseline: f64,
    run: f64,
    baseline_interval: Option<(f64, f64)>,
    run_interval: Option<(f64, f64)>,
) -> MacroRow {
    // the headline difference, which is reported whether or not it is a result
    let pct = if baseline != 0.0 {
        (run - baseline) / baseline * 100.0
    } else {
        0.0
    };
    // a conclusion needs an interval on both sides
    let (verdict, gap) = match (baseline_interval, run_interval) {
        (Some(before), Some(after)) => match disjoint_gap(before, after) {
            Some(gap) => (MacroVerdict::Result, Some(gap)),
            None => (MacroVerdict::NotAResult, None),
        },
        // without one, the difference is reported and explicitly not screened
        _ => (MacroVerdict::NoErrorBar, None),
    };
    MacroRow {
        metric: metric.to_string(),
        higher_is_better,
        baseline,
        run,
        baseline_interval,
        run_interval,
        pct,
        verdict,
        gap,
    }
}

/// Compares a macro capture against another one
///
/// # Arguments
///
/// * `run` - The capture being judged
/// * `baseline` - What to judge it against
pub fn compare(run: &MacroCaptureV2, baseline: &MacroCaptureV2) -> MacroComparison {
    // work out what the two sides have in common before comparing anything, so a capture that
    // shares nothing with its baseline can say so rather than producing an empty table
    let run_ids: BTreeSet<&str> = run.workload_ids().into_iter().collect();
    let baseline_ids: BTreeSet<&str> = baseline.workload_ids().into_iter().collect();
    let shared: Vec<&str> = run_ids.intersection(&baseline_ids).copied().collect();
    let only_in_run: Vec<String> = run_ids
        .difference(&baseline_ids)
        .map(|id| (*id).to_string())
        .collect();
    let only_in_baseline: Vec<String> = baseline_ids
        .difference(&run_ids)
        .map(|id| (*id).to_string())
        .collect();
    let mut rows = Vec::new();
    // one block of rows per shared workload, in identifier order so the report is deterministic
    for id in &shared {
        let (Some(before), Some(after)) = (baseline.workloads.get(*id), run.workloads.get(*id))
        else {
            continue;
        };
        rows.extend(compare_one(id, after, before));
    }
    // then a row per workload that only one side has, so it is named rather than dropped
    for id in only_in_run.iter().chain(only_in_baseline.iter()) {
        rows.push(MacroRow {
            metric: format!("{id}/wall_clock"),
            higher_is_better: false,
            baseline: 0.0,
            run: 0.0,
            baseline_interval: None,
            run_interval: None,
            pct: 0.0,
            verdict: MacroVerdict::Absent,
            gap: None,
        });
    }
    MacroComparison {
        rows,
        only_in_run,
        only_in_baseline,
        // sharing nothing is different from sharing something and finding no movement
        disjoint: shared.is_empty(),
    }
}

/// Compares one workload against the same workload from another capture
///
/// # Arguments
///
/// * `id` - The workload being compared, which prefixes every metric name
/// * `run` - The workload's result in the capture being judged
/// * `baseline` - The workload's result in what it is being judged against
fn compare_one(id: &str, run: &WorkloadCapture, baseline: &WorkloadCapture) -> Vec<MacroRow> {
    let mut rows = Vec::new();
    // the wall clock, which is the number this layer exists to produce
    rows.push(row(
        &format!("{id}/wall_clock"),
        false,
        baseline.median_wall_clock_ns() as f64,
        run.median_wall_clock_ns() as f64,
        baseline
            .wall_clock_interval_ns()
            .map(|(low, high)| (low as f64, high as f64)),
        run.wall_clock_interval_ns()
            .map(|(low, high)| (low as f64, high as f64)),
    ));
    // and the throughput it implies, which is the same measurement read the other way up. its
    // interval comes from the same array, inverted: the slowest run is the least throughput.
    rows.push(row(
        &format!("{id}/throughput"),
        true,
        baseline.rows_per_sec(),
        run.rows_per_sec(),
        throughput_interval(baseline),
        throughput_interval(run),
    ));
    // then every percentile of every operation both sides recorded. the union rather than one
    // side's list, so an operation that only one capture has is skipped by the check below rather
    // than by whichever side happened to be walked.
    let ops: BTreeSet<&str> = run
        .op_names()
        .into_iter()
        .chain(baseline.op_names())
        .collect();
    for op in ops {
        for metric in STAT_METRICS {
            // an operation missing from either side is skipped rather than reported as zero
            let (Some(before), Some(after)) =
                (baseline.stat_ns(op, metric), run.stat_ns(op, metric))
            else {
                continue;
            };
            rows.push(row(
                &format!("{id}/{op}/{metric}"),
                false,
                before as f64,
                after as f64,
                baseline
                    .stat_interval_ns(op, metric)
                    .map(|(low, high)| (low as f64, high as f64)),
                run.stat_interval_ns(op, metric)
                    .map(|(low, high)| (low as f64, high as f64)),
            ));
        }
    }
    rows
}

/// The interval of rows per second implied by a workload's wall clock interval
///
/// # Arguments
///
/// * `capture` - The workload result to read
fn throughput_interval(capture: &WorkloadCapture) -> Option<(f64, f64)> {
    // the row counts are fixed across a workload's runs, so the throughput interval is the wall
    // clock interval turned upside down
    let (fastest, slowest) = capture.wall_clock_interval_ns()?;
    let rows: u64 = capture.counters.values().sum();
    // guard the division, since a zero wall clock is not a run that happened
    if fastest == 0 || slowest == 0 {
        return None;
    }
    let low = rows as f64 / (slowest as f64 / 1_000_000_000.0);
    let high = rows as f64 / (fastest as f64 / 1_000_000_000.0);
    Some((low, high))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::macro_layer::{DurationParts, Stats};

    /// Builds a duration from nanoseconds
    ///
    /// # Arguments
    ///
    /// * `nanos` - The duration to build
    fn dur(nanos: u64) -> DurationParts {
        DurationParts {
            secs: nanos / 1_000_000_000,
            nanos: (nanos % 1_000_000_000) as u32,
        }
    }

    /// Builds a flat set of statistics where every percentile is the same
    ///
    /// # Arguments
    ///
    /// * `nanos` - The value to use for every percentile
    fn stats(nanos: u64) -> Stats {
        Stats {
            count: 1,
            max: dur(nanos),
            p99: dur(nanos),
            p95: dur(nanos),
            p90: dur(nanos),
            p50: dur(nanos),
            avg: dur(nanos),
            min: dur(nanos),
        }
    }

    /// The workload the fixtures below are built under
    const ID: &str = "macro/insert_unsorted";

    /// Builds a capture of one workload with a given set of wall clocks
    ///
    /// # Arguments
    ///
    /// * `walls` - Every run's wall clock in nanoseconds, which must be sorted
    fn capture(walls: &[u64]) -> MacroCaptureV2 {
        capture_as(ID, walls)
    }

    /// Builds a capture of one named workload with a given set of wall clocks
    ///
    /// # Arguments
    ///
    /// * `id` - The workload to build it under
    /// * `walls` - Every run's wall clock in nanoseconds, which must be sorted
    fn capture_as(id: &str, walls: &[u64]) -> MacroCaptureV2 {
        use crate::model::macro_layer::{ScaleFacts, Timing};
        let mut ops = std::collections::BTreeMap::new();
        ops.insert("insert".to_string(), stats(1_000));
        ops.insert("get".to_string(), stats(1_000));
        let mut counters = std::collections::BTreeMap::new();
        counters.insert("inserted".to_string(), 400_000u64);
        counters.insert("retrieved".to_string(), 100_000u64);
        let workload = WorkloadCapture {
            timing: Timing::PerBatch,
            seed: 42,
            scale: ScaleFacts {
                scale: "full".to_string(),
                rows: 500_000,
                row_bytes: 256,
                keys: 400_000,
                concurrency: 4096,
                clients: None,
            },
            conf: None,
            counters,
            ops,
            runs: Some(walls.len() as u32),
            wall_clock_ns: Some(walls.to_vec()),
            spread_pct: None,
            runs_detail: None,
        };
        let mut capture = MacroCaptureV2::new(Some("test".to_string()));
        capture.workloads.insert(id.to_string(), workload);
        capture
    }

    /// Two overlapping intervals are not a result however far apart their medians are
    #[test]
    fn overlapping_intervals_are_not_a_result() {
        let comparison = compare(&capture(&[1_500, 1_700, 1_900]), &capture(&[1_600, 1_800, 2_000]));
        let wall = &comparison.rows[0];
        assert_eq!(wall.verdict, MacroVerdict::NotAResult);
        assert!(wall.gap.is_none());
        assert!(!comparison.has_regression());
    }

    /// Two disjoint intervals are a result, reported as the gap between their nearest ends
    #[test]
    fn disjoint_intervals_are_a_result() {
        let comparison = compare(&capture(&[1_000, 1_100, 1_200]), &capture(&[1_600, 1_800, 2_000]));
        let wall = &comparison.rows[0];
        assert_eq!(wall.verdict, MacroVerdict::Result);
        // the gap is baseline's fastest minus this run's slowest, not median minus median
        assert_eq!(wall.gap, Some(400.0));
        // and it got faster, so it is not a regression
        assert!(!wall.is_regression());
        assert!(!comparison.has_regression());
    }

    /// A disjoint pair in the slower direction is a regression
    #[test]
    fn a_slower_disjoint_result_is_a_regression() {
        let comparison = compare(&capture(&[2_000, 2_100, 2_200]), &capture(&[1_000, 1_100, 1_200]));
        assert!(comparison.rows[0].is_regression());
        assert!(comparison.has_regression());
    }

    /// A capture that ran once has no interval, so nothing can be screened
    #[test]
    fn a_single_run_has_no_error_bar() {
        let comparison = compare(&capture(&[1_000]), &capture(&[2_000]));
        assert_eq!(comparison.rows[0].verdict, MacroVerdict::NoErrorBar);
        assert!(!comparison.rows[0].is_regression());
    }

    /// Throughput reads the same measurement the other way up, so its direction is inverted
    #[test]
    fn throughput_is_the_wall_clock_inverted() {
        // this run is faster, so its wall clock fell and its throughput rose
        let comparison = compare(&capture(&[1_000, 1_100, 1_200]), &capture(&[1_600, 1_800, 2_000]));
        let throughput = comparison
            .rows
            .iter()
            .find(|row| row.metric == format!("{ID}/throughput"))
            .expect("throughput is always compared");
        assert_eq!(throughput.verdict, MacroVerdict::Result);
        assert!(throughput.higher_is_better);
        assert!(throughput.run > throughput.baseline);
        assert!(!throughput.is_regression());
    }

    /// Two real committed captures whose intervals overlap are not a result
    ///
    /// `B1-performance` ran in 1,699-1,878 ms and `o17-after` in 1,802-1,902 ms. Their medians
    /// differ by 1.7%, which a percentage band of any plausible width would have called a
    /// regression, and their intervals overlap across 76 ms - so it is not one. This is the case
    /// the interval rule exists for, and it is taken from the tree rather than invented.
    #[test]
    fn two_real_overlapping_captures_are_not_a_result() {
        let store = crate::store::Store::new(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("shoal-bench has a parent"),
        );
        let (_, frozen) = store
            .resolve_macro("B1-performance")
            .expect("the frozen macro capture reads")
            .expect("the frozen macro capture exists");
        let (_, after) = store
            .resolve_macro("o17-after")
            .expect("the o17 macro capture reads")
            .expect("the o17 macro capture exists");
        let comparison = compare(&after, &frozen);
        let wall = &comparison.rows[0];
        assert_eq!(wall.metric, "macro/tmdb/wall_clock");
        // the medians moved by more than a percent
        assert!(
            wall.pct.abs() > 1.0,
            "expected the medians to differ, got {}%",
            wall.pct
        );
        // and it is still not a result, because five runs of each overlap
        assert_eq!(wall.verdict, MacroVerdict::NotAResult);
        assert!(!comparison.has_regression());
        // the percentiles of both predate per run detail, so none of them is screened either
        for row in &comparison.rows {
            if row.metric.starts_with("macro/tmdb/insert/")
                || row.metric.starts_with("macro/tmdb/get/")
            {
                assert_eq!(
                    row.verdict,
                    MacroVerdict::NoErrorBar,
                    "{} was screened without an error bar",
                    row.metric
                );
            }
        }
    }

    /// Percentiles are compared, and are reported as having no error bar without per run detail
    #[test]
    fn percentiles_are_compared_without_an_error_bar() {
        let comparison = compare(&capture(&[1_000, 1_100]), &capture(&[1_000, 1_100]));
        let p99 = comparison
            .rows
            .iter()
            .find(|row| row.metric == format!("{ID}/insert/p99"))
            .expect("insert p99 is compared");
        assert_eq!(p99.verdict, MacroVerdict::NoErrorBar);
    }

    /// Every metric carries the workload it belongs to, so two workloads never share a row
    #[test]
    fn every_metric_is_namespaced_by_its_workload() {
        let comparison = compare(&capture(&[1_000]), &capture(&[1_000]));
        assert!(!comparison.rows.is_empty());
        for row in &comparison.rows {
            assert!(row.metric.starts_with(ID), "{}", row.metric);
        }
    }

    /// A workload on only one side is named rather than dropped
    ///
    /// A workload that stopped being captured otherwise looks exactly like one that never
    /// regressed, and those are not the same thing.
    #[test]
    fn a_workload_on_one_side_only_is_reported_absent() {
        let mut run = capture_as("macro/a", &[1_000, 1_100]);
        run.workloads.extend(capture_as("macro/b", &[2_000, 2_100]).workloads);
        let baseline = capture_as("macro/a", &[1_000, 1_100]);
        let comparison = compare(&run, &baseline);
        // the shared workload compared normally
        assert!(comparison.rows.iter().any(|row| row.metric == "macro/a/wall_clock"
            && row.verdict != MacroVerdict::Absent));
        // and the one only the run has is named
        assert_eq!(comparison.only_in_run, vec!["macro/b".to_string()]);
        assert!(comparison.only_in_baseline.is_empty());
        assert_eq!(comparison.missing(), vec!["macro/b"]);
        assert!(
            comparison
                .rows
                .iter()
                .any(|row| row.metric == "macro/b/wall_clock" && row.verdict == MacroVerdict::Absent)
        );
        // an absent workload is never a regression, since nothing was measured twice
        assert!(!comparison.has_regression());
        // and the two captures do share something, so this is not the disjoint case
        assert!(!comparison.disjoint);
    }

    /// Comparing against a capture that shares no workload says so rather than comparing nothing
    ///
    /// This is what a version 2 capture against `B1-performance` is. An empty set of comparable
    /// rows is otherwise indistinguishable from a comparison in which nothing moved.
    #[test]
    fn a_capture_sharing_no_workload_is_reported_disjoint() {
        let comparison = compare(
            &capture_as("macro/insert_unsorted", &[1_000, 1_100]),
            &capture_as("macro/tmdb", &[2_000, 2_100]),
        );
        assert!(comparison.disjoint);
        assert_eq!(comparison.missing().len(), 2);
        // nothing was screened, so nothing can be a regression
        assert!(!comparison.has_regression());
        assert!(
            comparison
                .rows
                .iter()
                .all(|row| row.verdict == MacroVerdict::Absent)
        );
    }

    /// A real committed version 1 capture is comparable against another one, through the lift
    ///
    /// The seven captures taken before purpose built workloads existed keep their history: they
    /// lift to the same workload name, so they still join with each other.
    #[test]
    fn two_lifted_v1_captures_still_share_their_workload() {
        let store = crate::store::Store::new(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("shoal-bench has a parent"),
        );
        let (_, frozen) = store
            .resolve_macro("B1-performance")
            .expect("the frozen macro capture reads")
            .expect("the frozen macro capture exists");
        let (_, after) = store
            .resolve_macro("o17-after")
            .expect("the o17 macro capture reads")
            .expect("the o17 macro capture exists");
        let comparison = compare(&after, &frozen);
        assert!(!comparison.disjoint, "two version 1 captures must still join");
        assert!(comparison.missing().is_empty());
    }
}
