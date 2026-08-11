//! Folding criterion's output into one durable artifact
//!
//! Criterion writes its results under `target/criterion`, and it has its own baseline mechanism.
//! Neither is the durable record: `target/` does not survive a `cargo clean`, is not committed,
//! and cannot be read next to the macro benchmark's numbers. This produces one file, in the same
//! shape every time, that can be.
//!
//! The layout is `<group>/<function>/<value>/new/{estimates,benchmark}.json`. The estimates carry
//! the point estimate and the confidence interval; the benchmark carries the full name. Both are
//! needed, because the directory names alone lose the group separators.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::model::micro::{MicroCapture, MicroStat};

/// One of criterion's estimates and the interval around it
#[derive(Debug, Deserialize)]
struct Estimate {
    /// The point estimate, in nanoseconds
    point_estimate: f64,
    /// The interval criterion put around it
    confidence_interval: ConfidenceInterval,
}

/// The interval criterion puts around an estimate
#[derive(Debug, Deserialize)]
struct ConfidenceInterval {
    /// The bottom of the interval, in nanoseconds
    lower_bound: f64,
    /// The top of the interval, in nanoseconds
    upper_bound: f64,
}

/// The estimates criterion writes for one benchmark
#[derive(Debug, Deserialize)]
struct Estimates {
    /// The mean and its interval
    mean: Estimate,
    /// The median and its interval
    median: Estimate,
}

/// What criterion records about a benchmark alongside its estimates
#[derive(Debug, Deserialize)]
struct BenchmarkInfo {
    /// The benchmark's full name, group separators and all
    full_id: String,
}

/// Reads every benchmark criterion measured into one capture
///
/// # Arguments
///
/// * `criterion_dir` - Criterion's output directory, usually `target/criterion`
/// * `captured` - The timestamp to stamp the capture with
/// * `since` - Ignore results written before this, or `None` to take everything
pub fn collect(
    criterion_dir: &Path,
    captured: &str,
    since: Option<SystemTime>,
) -> Result<MicroCapture> {
    // criterion having written nothing means the benchmarks did not run
    if !criterion_dir.is_dir() {
        bail!(
            "no criterion output at {} - the benchmarks did not run",
            criterion_dir.display()
        );
    }
    let mut capture = MicroCapture::new(captured);
    let mut skipped = 0usize;
    // walk to every `new/estimates.json`, which is one per benchmark
    for entry in walkdir::WalkDir::new(criterion_dir).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking {}", criterion_dir.display()))?;
        if !entry.file_type().is_file() || entry.file_name() != "estimates.json" {
            continue;
        }
        let path = entry.path();
        // only the `new` directory holds this run's results; the others are saved baselines
        if path.parent().and_then(|dir| dir.file_name()) != Some(std::ffi::OsStr::new("new")) {
            continue;
        }
        // a result left over from an earlier capture is not a result of this one. criterion keeps
        // a directory per benchmark id forever, so without this a benchmark that was renamed or
        // removed reports its last value into every capture taken afterwards.
        if let Some(since) = since
            && !written_since(path, since)
        {
            skipped += 1;
            continue;
        }
        let estimates: Estimates = crate::store::read_json(path)?;
        // the full name comes from the sibling file, falling back to the directory path with the
        // `new` stripped when criterion did not write one
        let name = full_id(path);
        capture.benchmarks.insert(
            name,
            MicroStat {
                mean_ns: estimates.mean.point_estimate,
                lower_ns: estimates.mean.confidence_interval.lower_bound,
                upper_ns: estimates.mean.confidence_interval.upper_bound,
                median_ns: estimates.median.point_estimate,
            },
        );
    }
    // a capture with nothing in it is a failure, not an empty result
    if capture.benchmarks.is_empty() {
        bail!(
            "found no fresh criterion estimates under {} ({skipped} were left over from an \
             earlier capture)",
            criterion_dir.display()
        );
    }
    Ok(capture)
}

/// The benchmark name that belongs with an estimates file
///
/// # Arguments
///
/// * `estimates` - The path to the estimates file
fn full_id(estimates: &Path) -> String {
    let dir = estimates.parent().unwrap_or(Path::new(""));
    // criterion writes the full name beside the estimates
    let info: Option<BenchmarkInfo> = crate::store::read_json(&dir.join("benchmark.json")).ok();
    if let Some(info) = info {
        return info.full_id;
    }
    // failing that, the directory path is the name with the group separators already in it
    let mut parts: Vec<String> = Vec::new();
    for component in dir.components() {
        parts.push(component.as_os_str().to_string_lossy().to_string());
    }
    // drop the trailing `new`, which is criterion's own bookkeeping and not part of the name
    if parts.last().map(String::as_str) == Some("new") {
        parts.pop();
    }
    parts.join("/")
}

/// Whether a file was written at or after an instant
///
/// # Arguments
///
/// * `path` - The file to check
/// * `since` - The instant to compare against
fn written_since(path: &Path, since: SystemTime) -> bool {
    // a file whose time cannot be read is kept rather than dropped: losing a real result is worse
    // than keeping a stale one, and the stale one is at least visible in the artifact
    let Ok(meta) = std::fs::metadata(path) else {
        return true;
    };
    let Ok(modified) = meta.modified() else {
        return true;
    };
    modified >= since
}

/// Where criterion writes its output
///
/// # Arguments
///
/// * `root` - The repository root
pub fn criterion_dir(root: &Path) -> PathBuf {
    root.join("target/criterion")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Writes a fake criterion result
    ///
    /// # Arguments
    ///
    /// * `root` - The criterion directory to write into
    /// * `dir` - The benchmark's directory beneath it
    /// * `full` - The full name to record, or `None` to leave the name file out
    /// * `mean` - The mean to record
    fn write_result(root: &Path, dir: &str, full: Option<&str>, mean: f64) -> PathBuf {
        let target = root.join(dir).join("new");
        std::fs::create_dir_all(&target).expect("creating a benchmark directory");
        let estimates = format!(
            r#"{{"mean":{{"confidence_interval":{{"confidence_level":0.95,"lower_bound":{low},
               "upper_bound":{high}}},"point_estimate":{mean},"standard_error":1.0}},
               "median":{{"confidence_interval":{{"confidence_level":0.95,"lower_bound":{low},
               "upper_bound":{high}}},"point_estimate":{mean},"standard_error":1.0}}}}"#,
            low = mean - 1.0,
            high = mean + 1.0,
        );
        std::fs::write(target.join("estimates.json"), estimates).expect("writing estimates");
        if let Some(full) = full {
            std::fs::write(
                target.join("benchmark.json"),
                format!(r#"{{"group_id":"g","function_id":null,"value_str":null,"full_id":"{full}","directory_name":"{dir}"}}"#),
            )
            .expect("writing the benchmark info");
        }
        target.join("estimates.json")
    }

    /// The full name comes from criterion's own record of it
    #[test]
    fn the_full_id_comes_from_the_benchmark_file() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(
            dir.path(),
            "partition_sorted_insert/16",
            Some("partition_sorted/insert/16"),
            148.0,
        );
        let capture = collect(dir.path(), "2026-08-09T00:00:00Z", None).expect("it collects");
        assert!(capture.benchmarks.contains_key("partition_sorted/insert/16"));
    }

    /// Without that record the directory path stands in for the name
    #[test]
    fn the_path_stands_in_for_a_missing_name() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(dir.path(), "group/bench/16", None, 148.0);
        let capture = collect(dir.path(), "2026-08-09T00:00:00Z", None).expect("it collects");
        // the path with the trailing `new` removed, which is the name with its separators intact
        let name = capture
            .benchmarks
            .keys()
            .next()
            .expect("one benchmark was collected");
        assert!(name.ends_with("group/bench/16"), "{name}");
    }

    /// Every estimate criterion recorded comes through, with its interval
    #[test]
    fn estimates_and_intervals_are_carried_through() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(dir.path(), "a", Some("a"), 100.0);
        write_result(dir.path(), "b", Some("b"), 200.0);
        let capture = collect(dir.path(), "2026-08-09T00:00:00Z", None).expect("it collects");
        assert_eq!(capture.benchmarks.len(), 2);
        let a = capture.benchmarks.get("a").expect("a was collected");
        assert_eq!(a.mean_ns, 100.0);
        assert_eq!(a.lower_ns, 99.0);
        assert_eq!(a.upper_ns, 101.0);
    }

    /// A saved baseline sitting beside the new results is not mistaken for one
    #[test]
    fn a_saved_baseline_is_not_collected() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(dir.path(), "a", Some("a"), 100.0);
        // `--save-baseline` writes a sibling directory of `new`, holding the same file names
        let saved = dir.path().join("a/some-label");
        std::fs::create_dir_all(&saved).expect("creating a saved baseline");
        std::fs::copy(
            dir.path().join("a/new/estimates.json"),
            saved.join("estimates.json"),
        )
        .expect("copying the estimates");
        let capture = collect(dir.path(), "2026-08-09T00:00:00Z", None).expect("it collects");
        assert_eq!(capture.benchmarks.len(), 1);
    }

    /// A result left over from an earlier capture is dropped rather than reported as fresh
    #[test]
    fn a_stale_result_is_not_collected() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(dir.path(), "stale", Some("stale"), 100.0);
        // everything written before now counts as left over
        let cutoff = SystemTime::now() + Duration::from_secs(60);
        let err = collect(dir.path(), "2026-08-09T00:00:00Z", Some(cutoff))
            .expect_err("everything was stale, so there is nothing to collect");
        assert!(format!("{err}").contains("left over"), "{err}");
    }

    /// A fresh result alongside a stale one is the only one collected
    #[test]
    fn a_fresh_result_survives_the_freshness_check() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        write_result(dir.path(), "old", Some("old"), 100.0);
        // anything written before this instant is left over, and the next write is not
        std::thread::sleep(Duration::from_millis(20));
        let cutoff = SystemTime::now();
        std::thread::sleep(Duration::from_millis(20));
        write_result(dir.path(), "new-one", Some("new-one"), 200.0);
        let capture =
            collect(dir.path(), "2026-08-09T00:00:00Z", Some(cutoff)).expect("it collects");
        assert_eq!(capture.benchmarks.len(), 1);
        assert!(capture.benchmarks.contains_key("new-one"));
    }

    /// Criterion having written nothing is an error, not an empty capture
    #[test]
    fn no_output_at_all_is_an_error() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        assert!(collect(&dir.path().join("missing"), "t", None).is_err());
        assert!(collect(dir.path(), "t", None).is_err());
    }
}
