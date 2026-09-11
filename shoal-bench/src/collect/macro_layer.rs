//! Folding several runs of each workload into one artifact
//!
//! # Why the median and not the mean
//!
//! A workload's spread is wide and its outliers are one sided: a run can be arbitrarily slow and
//! cannot be faster than the work. A mean is dragged by that tail and a median is not, so the
//! median run is kept whole - its own percentiles, not a percentile of percentiles.
//!
//! Every run's wall clock is kept alongside it. Without the spread the median is a number with no
//! error bar, and the whole reason this layer exists in the form it does is that its spread is wide
//! enough to swallow most of what it might be asked to measure. Each run's own percentile
//! distribution is kept too, which is what lets a comparison say whether a move in p99 is a result
//! or is the spread.
//!
//! # The median is chosen per workload, independently
//!
//! This is the one thing that changed when the macro layer went from one workload to several, and
//! it is not a detail. A capture runs every workload five times, and a hiccup during the fourth run
//! of one workload says nothing about which run of a different workload deserves to be kept. Folding
//! them together - picking one median run of the whole capture and taking every workload's numbers
//! from it - would let one workload's outlier choose every other workload's reported result.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

use crate::model::macro_layer::{MacroCaptureV2, WorkloadCapture, WorkloadRun};

/// Folds every run of every workload into one artifact
///
/// # Arguments
///
/// * `runs` - Each workload's result files, in the order the runs were taken
/// * `label` - The name this capture is being taken under
pub fn collect(runs: &BTreeMap<String, Vec<PathBuf>>, label: Option<String>) -> Result<MacroCaptureV2> {
    // a capture with no workloads is not a capture
    if runs.is_empty() {
        bail!("the macro layer produced no runs to fold together");
    }
    let mut folded = MacroCaptureV2::new(label);
    // fold each workload on its own, so no workload's spread can choose another's kept run
    for (id, paths) in runs {
        folded.workloads.insert(id.clone(), fold_one(id, paths)?);
    }
    Ok(folded)
}

/// Folds one workload's runs into one result
///
/// # Arguments
///
/// * `id` - The workload being folded, for the error messages
/// * `paths` - Its result files, in the order the runs were taken
fn fold_one(id: &str, paths: &[PathBuf]) -> Result<WorkloadCapture> {
    // a workload with no runs is one that never ran
    if paths.is_empty() {
        bail!("{id} produced no runs to fold together");
    }
    // read each run, taking this workload's block out of the single workload file it wrote
    let mut captures: Vec<WorkloadCapture> = Vec::with_capacity(paths.len());
    for path in paths {
        let capture = crate::model::macro_layer::read(path).map_err(|err| anyhow::anyhow!(err))?;
        // the file a run writes holds exactly the workload that was run, and naming which one is
        // what stops a mismatched scratch file being folded into the wrong workload's numbers
        let Some(block) = capture.workloads.get(id) else {
            bail!(
                "{} holds {:?}, not {id}",
                path.display(),
                capture.workload_ids()
            );
        };
        captures.push(block.clone());
    }
    // each run's distribution, in the order the runs happened, so a later reader can see whether
    // the tail wandered as the machine warmed up
    let runs_detail: Vec<WorkloadRun> = captures
        .iter()
        .map(|capture| WorkloadRun {
            wall_clock_ns: run_wall_clock(capture),
            ops: capture.ops.clone(),
            counters: capture.counters.clone(),
        })
        .collect();
    // every wall clock, sorted, which is both the interval and the way the median is found
    let mut walls: Vec<u64> = runs_detail.iter().map(|run| run.wall_clock_ns).collect();
    walls.sort_unstable();
    // the middle run is kept whole rather than averaged into anything
    let mut ordered: Vec<&WorkloadCapture> = captures.iter().collect();
    ordered.sort_by_key(|capture| run_wall_clock(capture));
    let mut kept = ordered[ordered.len() / 2].clone();
    // the spread, which is the error bar the median otherwise would not have
    let spread_pct = if walls[0] > 0 {
        (walls[walls.len() - 1] - walls[0]) as f64 / walls[0] as f64 * 100.0
    } else {
        0.0
    };
    kept.runs = Some(runs_detail.len() as u32);
    kept.wall_clock_ns = Some(walls);
    kept.spread_pct = Some(spread_pct);
    kept.runs_detail = Some(runs_detail);
    Ok(kept)
}

/// The wall clock of a single run's capture
///
/// # Arguments
///
/// * `capture` - The single run capture to read
fn run_wall_clock(capture: &WorkloadCapture) -> u64 {
    // a run writes one entry, so the first is the only
    capture
        .wall_clock_ns
        .as_ref()
        .and_then(|walls| walls.first().copied())
        .unwrap_or(0)
}

/// Reads a macro artifact back and describes what it holds
///
/// # Arguments
///
/// * `path` - The artifact to describe
pub fn describe(path: &Path) -> Result<String> {
    let capture = crate::model::macro_layer::read(path).map_err(|err| anyhow::anyhow!(err))?;
    // one line per workload, since a capture now holds several and a single summary line would
    // have to pick one of them to be about
    //
    // every line carries its own indent rather than relying on the caller's, because the caller
    // prints this with one `println!` and only the first line would otherwise line up
    let mut lines = Vec::with_capacity(capture.workloads.len());
    for (id, workload) in &capture.workloads {
        let rows: u64 = workload.counters.values().sum();
        lines.push(format!(
            "  {id}: wall clock {} (spread {}), {} rows",
            crate::fmt::duration_ns(workload.median_wall_clock_ns() as f64),
            crate::fmt::signed_pct(workload.spread_pct.unwrap_or(0.0)),
            crate::fmt::thousands(u128::from(rows))
        ));
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::macro_layer::{DurationParts, ScaleFacts, Stats, Timing};

    /// The workload the fixtures below are written under
    const ID: &str = "macro/insert_unsorted";

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

    /// Writes one run's result the way a workload does
    ///
    /// # Arguments
    ///
    /// * `dir` - Where to write it
    /// * `name` - What to call it
    /// * `wall` - The run's wall clock in nanoseconds
    /// * `p99` - The run's insert p99 in nanoseconds
    fn write_run(dir: &Path, name: &str, wall: u64, p99: u64) -> PathBuf {
        write_run_as(dir, name, ID, wall, p99)
    }

    /// Writes one run's result under a named workload
    ///
    /// # Arguments
    ///
    /// * `dir` - Where to write it
    /// * `name` - What to call it
    /// * `id` - The workload to write it under
    /// * `wall` - The run's wall clock in nanoseconds
    /// * `p99` - The run's insert p99 in nanoseconds
    fn write_run_as(dir: &Path, name: &str, id: &str, wall: u64, p99: u64) -> PathBuf {
        let stats = Stats {
            count: 10,
            max: dur(p99 * 2),
            p99: dur(p99),
            p95: dur(p99),
            p90: dur(p99),
            p50: dur(p99 / 2),
            avg: dur(p99 / 2),
            min: dur(1),
        };
        let mut ops = BTreeMap::new();
        ops.insert("insert".to_string(), stats);
        let mut counters = BTreeMap::new();
        counters.insert("inserted".to_string(), 400_000u64);
        let capture = WorkloadCapture {
            timing: Timing::PerBatch,
            seed: 42,
            scale: ScaleFacts {
                scale: "full".to_string(),
                rows: 400_000,
                row_bytes: 256,
                keys: 400_000,
                concurrency: 4096,
                clients: None,
                // not a mixture, a width distribution or a skewed access pattern
                ..ScaleFacts::default()
            },
            conf: None,
            cluster: None,
            counters,
            ops,
            runs: None,
            wall_clock_ns: Some(vec![wall]),
            spread_pct: None,
            runs_detail: None,
        };
        let mut file = MacroCaptureV2::new(Some("L".to_string()));
        file.workloads.insert(id.to_string(), capture);
        let path = dir.join(name);
        crate::store::write_json(&path, &file).expect("writing a run");
        path
    }

    /// Folds a single workload's runs the way a capture does
    ///
    /// # Arguments
    ///
    /// * `paths` - The run files to fold
    fn fold(paths: Vec<PathBuf>) -> WorkloadCapture {
        let mut runs = BTreeMap::new();
        runs.insert(ID.to_string(), paths);
        let folded = collect(&runs, Some("L".to_string())).expect("it folds");
        folded.workloads[ID].clone()
    }

    /// The middle run is kept, whole, rather than averaged with the others
    #[test]
    fn the_median_run_is_kept_whole() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        // written out of order, to prove the choice is by wall clock and not by file name
        let kept = fold(vec![
            write_run(dir.path(), "run-1.json", 3_000, 300),
            write_run(dir.path(), "run-2.json", 1_000, 100),
            write_run(dir.path(), "run-3.json", 2_000, 200),
        ]);
        // the middle wall clock, and that same run's own percentiles
        assert_eq!(kept.median_wall_clock_ns(), 2_000);
        assert_eq!(kept.stat_ns("insert", "p99"), Some(200));
    }

    /// Every run's wall clock is kept, sorted, so the median has an error bar
    #[test]
    fn every_wall_clock_is_kept() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let kept = fold(vec![
            write_run(dir.path(), "run-1.json", 3_000, 300),
            write_run(dir.path(), "run-2.json", 1_000, 100),
            write_run(dir.path(), "run-3.json", 2_000, 200),
        ]);
        assert_eq!(kept.wall_clock_ns, Some(vec![1_000, 2_000, 3_000]));
        assert_eq!(kept.runs, Some(3));
        // the spread is measured from the fastest run, which is the floor the work cannot beat
        assert_eq!(kept.spread_pct, Some(200.0));
        assert_eq!(kept.wall_clock_interval_ns(), Some((1_000, 3_000)));
    }

    /// Each run's own distribution is kept, in the order the runs were taken
    #[test]
    fn each_runs_distribution_is_kept_in_order() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let kept = fold(vec![
            write_run(dir.path(), "run-1.json", 3_000, 300),
            write_run(dir.path(), "run-2.json", 1_000, 100),
        ]);
        let detail = kept.runs_detail.as_ref().expect("detail is kept");
        // the order runs happened in, not the sorted order
        assert_eq!(detail[0].wall_clock_ns, 3_000);
        assert_eq!(detail[1].wall_clock_ns, 1_000);
        // which is what gives the percentiles an interval for the first time
        assert_eq!(kept.stat_interval_ns("insert", "p99"), Some((100, 300)));
    }

    /// A single run folds into itself, with no interval to speak of
    #[test]
    fn a_single_run_folds_into_itself() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let kept = fold(vec![write_run(dir.path(), "run-1.json", 1_500, 150)]);
        assert_eq!(kept.median_wall_clock_ns(), 1_500);
        assert_eq!(kept.runs, Some(1));
        assert_eq!(kept.spread_pct, Some(0.0));
        // one observation is not an interval, and must not be reported as one
        assert_eq!(kept.wall_clock_interval_ns(), None);
        assert_eq!(kept.stat_interval_ns("insert", "p99"), None);
    }

    /// Each workload's median is chosen from its own runs and nothing else's
    ///
    /// The reason this is folded per workload. Here one workload's runs get slower as the capture
    /// goes on and the other's get faster; folding them together would make one of them report a
    /// run it did not deserve.
    #[test]
    fn each_workload_picks_its_own_median() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut runs = BTreeMap::new();
        runs.insert(
            "macro/a".to_string(),
            vec![
                write_run_as(dir.path(), "a-1.json", "macro/a", 1_000, 100),
                write_run_as(dir.path(), "a-2.json", "macro/a", 2_000, 200),
                write_run_as(dir.path(), "a-3.json", "macro/a", 3_000, 300),
            ],
        );
        runs.insert(
            "macro/b".to_string(),
            vec![
                write_run_as(dir.path(), "b-1.json", "macro/b", 90_000, 900),
                write_run_as(dir.path(), "b-2.json", "macro/b", 50_000, 500),
                write_run_as(dir.path(), "b-3.json", "macro/b", 10_000, 100),
            ],
        );
        let folded = collect(&runs, None).expect("it folds");
        // each kept the middle of its own runs, which are different runs of the capture
        assert_eq!(folded.workloads["macro/a"].median_wall_clock_ns(), 2_000);
        assert_eq!(folded.workloads["macro/b"].median_wall_clock_ns(), 50_000);
        assert_eq!(folded.workloads["macro/a"].stat_ns("insert", "p99"), Some(200));
        assert_eq!(folded.workloads["macro/b"].stat_ns("insert", "p99"), Some(500));
    }

    /// A run file holding a different workload is refused rather than folded in
    ///
    /// A scratch file left behind by an earlier capture would otherwise be folded into whichever
    /// workload's list it happened to land in, and would look exactly like a real run of it.
    #[test]
    fn a_mismatched_run_file_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut runs = BTreeMap::new();
        runs.insert(
            "macro/a".to_string(),
            vec![write_run_as(dir.path(), "wrong.json", "macro/b", 1_000, 100)],
        );
        let error = collect(&runs, None).expect_err("a mismatched run is an error");
        assert!(format!("{error}").contains("macro/b"), "{error}");
    }

    /// Folding nothing is an error rather than an empty artifact
    #[test]
    fn folding_nothing_is_an_error() {
        assert!(collect(&BTreeMap::new(), None).is_err());
    }
}
