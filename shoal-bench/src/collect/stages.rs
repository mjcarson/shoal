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

use std::path::Path;

use anyhow::{Result, bail};

use crate::model::stages::StageReport;

/// The fraction of records that must have joined for a report to be about the run it names
///
/// Not a tight bound. It is here to catch a join that failed, not to police a handful of records
/// that arrived after the shards handed over their tail.
const MIN_JOIN_RATIO: f64 = 0.5;

/// Checks a stage artifact and describes what it holds
///
/// # Arguments
///
/// * `path` - The artifact to check
pub fn check(path: &Path) -> Result<String> {
    let report: StageReport = crate::store::read_json(path)?;
    report
        .check_version(path)
        .map_err(|err| anyhow::anyhow!(err))?;
    let join = report.join;
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
