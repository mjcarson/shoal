//! Checking over the stage report a run wrote
//!
//! Like the hotpath profile, the report is written by the run itself - only the instrumented
//! build can join the client and server halves of a record, so it does the join and writes the
//! result. What is left to do here is check the join.
//!
//! A join that matched far fewer records than the run issued is not a report about that run. The
//! counts are surfaced rather than left in the file for someone to notice later, because a report
//! built from a tenth of the queries looks exactly like a report built from all of them once it
//! is a chart.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::model::stages::{StageReport, StageReports};

/// The fraction of records that must have joined for a report to be about the run it names
///
/// Not a tight bound. It is here to catch a join that failed, not to police a handful of records
/// that arrived after the shards handed over their tail.
const MIN_JOIN_RATIO: f64 = 0.5;

/// Gathers the per-workload reports a capture's instrumented runs wrote into one artifact
///
/// Each run writes its own file, because they used to share one and the last one won
/// ([item 73](../../../docs/src/appendix/resolved-issues.md)). This folds them back together,
/// keyed by the workload each report names itself with rather than by the file it was found in -
/// a file name is a thing the runner chose and the workload is a thing the run knows.
///
/// **The caller names the files rather than this walking the directory.** Scratch is created and
/// never cleared, so it holds every run of every capture ever taken in this tree; a glob over it
/// would fold a previous capture's stage reports into this one's artifact, under the same workload
/// keys, and the result would look exactly like a correct capture. That is the same failure item 73
/// is about, one directory up.
///
/// # Arguments
///
/// * `wrote` - The reports this capture's runs were told to write, in the order they ran
/// * `into` - Where the layer's artifact goes
pub fn collect(wrote: &[PathBuf], into: &Path) -> Result<StageReports> {
    let mut reports: std::collections::BTreeMap<String, StageReport> =
        std::collections::BTreeMap::new();
    for path in wrote {
        let report: StageReport = crate::store::read_json(path)
            .with_context(|| format!("failed to read the stage report at {}", path.display()))?;
        report
            .check_version(path)
            .map_err(|err| anyhow::anyhow!(err))?;
        // a report with no workload name cannot be keyed, and silently filing it under a guess
        // would put one workload's breakdown under another's name
        let workload = report.workload.clone().ok_or_else(|| {
            anyhow::anyhow!("{} does not say which workload it describes", path.display())
        })?;
        // and two reports claiming one workload is a run that was planned twice, which would leave
        // the artifact describing whichever ran last - the defect this whole change is about
        if let Some(existing) = reports.insert(workload.clone(), report) {
            let _ = existing;
            bail!("two stage reports both describe {workload}");
        }
    }
    if reports.is_empty() {
        bail!("no stage report was written, so there is nothing to collect");
    }
    let artifact = StageReports::new(reports);
    crate::store::write_json(into, &artifact)?;
    Ok(artifact)
}

/// Checks a stage artifact and describes what it holds
///
/// # Arguments
///
/// * `path` - The artifact to check
pub fn check(path: &Path) -> Result<String> {
    let reports = crate::store::read_stage_reports(path)?;
    let join = reports.join();
    // a report that joined nothing is not a report
    if join.joined == 0 {
        bail!(
            "{} joined no queries at all, so it is not a report about this run \
             ({} server only, {} client only)",
            path.display(),
            join.server_only,
            join.client_only
        );
    }
    // and one that joined a small fraction of what it saw is describing a subset nobody chose
    let seen = join.joined + join.server_only + join.client_only;
    let ratio = join.joined as f64 / seen as f64;
    if ratio < MIN_JOIN_RATIO {
        bail!(
            "{} joined only {} of {seen} records ({}), which is too few for the report to \
             describe this run",
            path.display(),
            crate::fmt::thousands(join.joined as u128),
            crate::fmt::share_pct(ratio)
        );
    }
    Ok(format!(
        "joined {}, server only {}, client only {}, duplicates {}",
        crate::fmt::thousands(join.joined as u128),
        join.server_only,
        join.client_only,
        join.duplicates
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes a stage report with a given set of join counts
    ///
    /// # Arguments
    ///
    /// * `dir` - Where to write it
    /// * `joined` - How many records joined
    /// * `server_only` - How many the server saw alone
    /// * `client_only` - How many the client saw alone
    fn write_report(
        dir: &Path,
        joined: usize,
        server_only: usize,
        client_only: usize,
    ) -> std::path::PathBuf {
        let path = dir.join("stages.json");
        std::fs::write(
            &path,
            format!(
                r#"{{"version":1,"label":"L","clock":"CLOCK_MONOTONIC","clock_overhead_ns":20,
                   "join":{{"joined":{joined},"server_only":{server_only},
                   "client_only":{client_only},"duplicates":0,"saturated":0,"window_missing":0}},
                   "ops":{{}}}}"#
            ),
        )
        .expect("writing a report");
        path
    }

    /// Writes a stage report naming a workload
    ///
    /// # Arguments
    ///
    /// * `dir` - Where to write it
    /// * `workload` - What the report says it describes, if anything
    /// * `joined` - How many records joined
    fn write_named(dir: &Path, workload: Option<&str>, joined: usize) -> std::path::PathBuf {
        let named = match workload {
            Some(name) => format!(r#""workload":"{name}","#),
            None => String::new(),
        };
        let path = dir.join(format!("stages-{}.json", workload.unwrap_or("anon").replace('/', "-")));
        std::fs::write(
            &path,
            format!(
                r#"{{"version":1,"label":"L",{named}"clock":"CLOCK_MONOTONIC",
                   "clock_overhead_ns":20,
                   "join":{{"joined":{joined},"server_only":0,"client_only":0,
                   "duplicates":0,"saturated":0,"window_missing":0}},"ops":{{}}}}"#
            ),
        )
        .expect("writing a report");
        path
    }

    /// Several reports fold into one artifact, keyed by the workload each one names
    #[test]
    fn the_reports_fold_into_one_artifact() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let wrote = vec![
            write_named(dir.path(), Some("macro/grid/unsorted/r50/1024"), 10),
            write_named(dir.path(), Some("macro/grid/unsorted/r50/8192"), 20),
        ];
        let into = dir.path().join("L.stages.json");
        let artifact = collect(&wrote, &into).expect("it collects");
        assert_eq!(artifact.reports.len(), 2);
        assert!(artifact.get("macro/grid/unsorted/r50/8192").is_some());
        // and the joins are summed, which is what the provenance records
        assert_eq!(artifact.join().joined, 30);
        // written where it was asked to write it, and it reads back
        assert!(crate::store::read_stage_reports(&into).is_ok());
    }

    /// A report that does not say which workload it describes is refused
    ///
    /// Filing it under a guess would put one workload's breakdown under another's name, which is
    /// the shape of the defect this collector exists to fix rather than an inconvenience.
    #[test]
    fn an_unnamed_report_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let wrote = vec![write_named(dir.path(), None, 10)];
        let into = dir.path().join("L.stages.json");
        assert!(collect(&wrote, &into).is_err());
    }

    /// Two reports claiming one workload is refused rather than silently deduplicated
    #[test]
    fn two_reports_for_one_workload_are_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let one = write_named(dir.path(), Some("macro/insert_unsorted"), 10);
        let wrote = vec![one.clone(), one];
        let into = dir.path().join("L.stages.json");
        assert!(collect(&wrote, &into).is_err());
    }

    /// A file the plan named and no run wrote is an error, not a shorter artifact
    ///
    /// Scratch is never cleared, so a missing file cannot be told apart from a stale one by looking
    /// at the directory. Naming what was expected is what makes the absence visible.
    #[test]
    fn a_report_the_run_never_wrote_is_an_error() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let wrote = vec![dir.path().join("stages-never-ran.json")];
        let into = dir.path().join("L.stages.json");
        assert!(collect(&wrote, &into).is_err());
    }

    /// A report whose halves lined up passes, and says how well
    #[test]
    fn a_joined_report_passes() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = write_report(dir.path(), 617_175, 0, 0);
        let described = check(&path).expect("it checks out");
        assert!(described.contains("617,175"), "{described}");
    }

    /// A report that joined nothing is refused
    #[test]
    fn a_report_that_joined_nothing_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = write_report(dir.path(), 0, 1_000, 1_000);
        let err = check(&path).expect_err("a report with no join is not a report");
        assert!(format!("{err}").contains("joined no queries"), "{err}");
    }

    /// A report that joined a small fraction of what it saw is refused
    #[test]
    fn a_mostly_unjoined_report_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = write_report(dir.path(), 100, 900, 0);
        let err = check(&path).expect_err("a mostly unjoined report is not about this run");
        assert!(format!("{err}").contains("too few"), "{err}");
    }

    /// A handful of unmatched records is not a failure
    #[test]
    fn a_few_unmatched_records_are_tolerated() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = write_report(dir.path(), 617_000, 175, 12);
        assert!(check(&path).is_ok());
    }

    /// A report of a version this tool does not read is refused rather than rendered
    #[test]
    fn a_future_report_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("stages.json");
        std::fs::write(
            &path,
            r#"{"version":99,"label":null,"clock":"c","clock_overhead_ns":1,
               "join":{"joined":1,"server_only":0,"client_only":0,"duplicates":0,
               "saturated":0,"window_missing":0},"ops":{}}"#,
        )
        .expect("writing a report");
        assert!(check(&path).is_err());
    }
}
