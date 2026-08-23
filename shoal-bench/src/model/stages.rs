//! The stage layer's artifact: where one query's latency went
//!
//! Mirrors `shoal::stages::StageReport`, which is written by a build of the `tmdb` example made
//! with the `stage-profile` feature. Like the hotpath layer this is attribution only - the
//! instrumented build stamps nineteen points on the query path, so its wall clock is not a
//! latency anyone should quote.
//!
//! The report is built by ranking queries by their *total* latency and averaging each stage over
//! a window around each rank, because per stage percentiles do not add up to the total
//! percentile. A bucket is therefore "what the queries at this rank spent their time on", not
//! "the p99 of each stage".

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The schema version this tool is willing to read
///
/// Must track `shoal::stages::REPORT_VERSION`. A report of a different version is refused rather
/// than rendered, since a stage list that shifted underneath a chart would relabel every segment.
pub const STAGE_REPORT_VERSION: u32 = 1;

/// The schema version of the *artifact* a capture writes
///
/// Distinct from [`STAGE_REPORT_VERSION`] because the two changed for different reasons and at
/// different times. A report describes one workload and its shape has not moved; the artifact used
/// to *be* one such report and now holds several, keyed by the workload each came from, because the
/// stage layer runs the same workload at three row widths and one file cannot answer for all three.
///
/// Version 1 of the artifact is a bare [`StageReport`]. [`StageReports::read`] still accepts one, so
/// every capture taken before the layer profiled more than one workload keeps rendering.
pub const STAGE_ARTIFACT_VERSION: u32 = 2;

/// What one stage cost the queries in one bucket
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StageCost {
    /// The mean nanoseconds this stage took across the bucket's window
    pub mean_ns: u64,
    /// This stage's share of the bucket's total latency
    pub share: f64,
    /// Whether this stage is charged once per batch rather than once per query
    #[serde(default)]
    pub per_batch: bool,
    /// Whether this stage's cost is close enough to the clock's own cost to be mostly instrument
    ///
    /// A stage at the floor is not a measurement. Charts fold these into an `other` segment
    /// rather than drawing them, because a segment says "this is where the time went" and a
    /// number below the instrument's resolution does not say that.
    #[serde(default)]
    pub at_floor: bool,
    /// How many queries this stage's mean was taken over
    pub samples: usize,
}

/// The stage breakdown of the queries at one latency rank
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    /// Which rank this bucket describes, one of `all`, `p50`, `p90`, `p99`, `p999` or `max`
    pub rank: String,
    /// The mean total latency of the queries in this bucket, in nanoseconds
    pub total_ns: u64,
    /// How many queries fell in this bucket's window
    pub samples: usize,
    /// What each stage cost, keyed by stage name
    pub stages: BTreeMap<String, StageCost>,
    /// The nanoseconds of the total that no stage accounted for
    ///
    /// Signed, and never rounded away: a breakdown whose parts do not sum to the whole is saying
    /// something, and hiding it would make every chart built from it look complete when it is not.
    pub unaccounted_ns: i64,
}

/// The stage breakdown of one kind of query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpReport {
    /// How many queries of this kind the run issued
    pub count: usize,
    /// How many of them were answered on a log rotation rather than on a watermark
    pub rotated: usize,
    /// One bucket per latency rank
    pub buckets: Vec<Bucket>,
}

/// How the client and server halves of a run lined up
///
/// A join that matched far fewer records than the run issued is not a report about that run, so
/// these counts are surfaced rather than left in the file for someone to find later.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinStats {
    /// How many queries were matched on both sides
    pub joined: usize,
    /// How many the server recorded and the client did not
    pub server_only: usize,
    /// How many the client recorded and the server did not
    pub client_only: usize,
    /// How many ids turned up more than once
    pub duplicates: usize,
    /// How many records had an offset too large to fit and were saturated
    pub saturated: usize,
    /// How many records were missing the window they should have fallen in
    pub window_missing: usize,
}

/// A whole stage report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageReport {
    /// The schema version this report was written with
    pub version: u32,
    /// The name this run was captured under, if it was given one
    #[serde(default)]
    pub label: Option<String>,
    /// Which workload this report describes
    ///
    /// Defaulted rather than required, so a version 1 artifact - written when only one workload
    /// ever profiled and nothing needed to say which - still parses. [`StageReports::read`] is what
    /// supplies the name in that case.
    #[serde(default)]
    pub workload: Option<String>,
    /// What clock the stamps in this run were taken with
    pub clock: String,
    /// What one reading of that clock costs, in nanoseconds
    pub clock_overhead_ns: u64,
    /// How the client and server halves of this run lined up
    pub join: JoinStats,
    /// The breakdown for each kind of query, keyed by `insert` or `get`
    pub ops: BTreeMap<String, OpReport>,
}

impl StageReport {
    /// Checks that this report's schema version is one this tool understands
    ///
    /// # Arguments
    ///
    /// * `path` - The path this report was read from, for the error message
    pub fn check_version(&self, path: &std::path::Path) -> Result<(), String> {
        // refuse anything this tool was not written against, since the stage list itself could
        // have changed and every chart segment is labelled from it
        if self.version != STAGE_REPORT_VERSION {
            return Err(format!(
                "{}: stage report is version {}, this tool reads version {STAGE_REPORT_VERSION}",
                path.display(),
                self.version
            ));
        }
        Ok(())
    }

    /// Looks up one operation's bucket at a given rank
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read, `insert` or `get`
    /// * `rank` - Which rank to read, one of `all`, `p50`, `p90`, `p99`, `p999` or `max`
    pub fn bucket(&self, op: &str, rank: &str) -> Option<&Bucket> {
        // find the operation, then the bucket within it that carries this rank
        self.ops
            .get(op)?
            .buckets
            .iter()
            .find(|bucket| bucket.rank == rank)
    }
}

/// The only workload the stage layer ever ran before it ran more than one
///
/// A version 1 artifact does not say which workload it describes, because there was only one it
/// could have been. Reading one keys it under this name rather than under something invented, so an
/// old capture and a new one join on the same string where they overlap.
pub const LEGACY_STAGE_WORKLOAD: &str = "macro/insert_unsorted";

/// Every stage report a capture produced, keyed by the workload that produced it
///
/// The artifact the stage layer writes. One file per capture, as every other layer has, rather than
/// one file per workload - a snapshot addresses a layer by name and a reader that had to enumerate
/// files to find the pieces of one layer would be the only reader in this tool that did.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageReports {
    /// The schema version this artifact was written with
    pub version: u32,
    /// One report per workload, keyed by the workload's identifier
    pub reports: BTreeMap<String, StageReport>,
}

impl StageReports {
    /// Builds an artifact from the reports a capture's runs wrote
    ///
    /// # Arguments
    ///
    /// * `reports` - One report per workload, keyed by the workload's identifier
    pub fn new(reports: BTreeMap<String, StageReport>) -> Self {
        StageReports {
            version: STAGE_ARTIFACT_VERSION,
            reports,
        }
    }

    /// Reads an artifact, accepting either shape
    ///
    /// Version 2 is this struct. Version 1 was a bare [`StageReport`] and is wrapped under
    /// [`LEGACY_STAGE_WORKLOAD`], which is the only workload that could have written one. The
    /// fallback is what keeps every stage artifact committed before the layer grew a second
    /// workload readable, rather than trading nine captures for a schema change.
    ///
    /// # Arguments
    ///
    /// * `raw` - The bytes the artifact holds
    /// * `path` - Where they came from, for the error message
    pub fn read(raw: &str, path: &std::path::Path) -> Result<Self, String> {
        // the current shape first, so the common case never pays for the fallback
        if let Ok(reports) = serde_json::from_str::<StageReports>(raw) {
            if reports.version != STAGE_ARTIFACT_VERSION {
                return Err(format!(
                    "{}: stage artifact is version {}, this tool reads version \
                     {STAGE_ARTIFACT_VERSION}",
                    path.display(),
                    reports.version
                ));
            }
            for report in reports.reports.values() {
                report.check_version(path)?;
            }
            return Ok(reports);
        }
        // and then the one report a capture used to write
        let single: StageReport = serde_json::from_str(raw)
            .map_err(|err| format!("{}: {err}", path.display()))?;
        single.check_version(path)?;
        let name = single
            .workload
            .clone()
            .unwrap_or_else(|| LEGACY_STAGE_WORKLOAD.to_string());
        Ok(StageReports::new(BTreeMap::from([(name, single)])))
    }

    /// The report for one workload, if the capture holds one
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload's report to read
    pub fn get(&self, workload: &str) -> Option<&StageReport> {
        self.reports.get(workload)
    }

    /// The report a page should draw when it wants one and does not care which
    ///
    /// The legacy workload when it is present, since that is the one every attribution page has
    /// always drawn, and otherwise the first by identifier - deterministic, so two renders of one
    /// artifact never disagree.
    pub fn primary(&self) -> Option<&StageReport> {
        self.reports
            .get(LEGACY_STAGE_WORKLOAD)
            .or_else(|| self.reports.values().next())
    }

    /// How the client and server halves lined up across every report
    ///
    /// Summed, because the provenance records one figure per layer and a capture that profiled
    /// three workloads has three joins behind that figure.
    pub fn join(&self) -> JoinStats {
        let mut total = JoinStats::default();
        for report in self.reports.values() {
            total.joined += report.join.joined;
            total.server_only += report.join.server_only;
            total.client_only += report.join.client_only;
            total.duplicates += report.join.duplicates;
            total.saturated += report.join.saturated;
            total.window_missing += report.join.window_missing;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a report with a given join count and workload name
    ///
    /// # Arguments
    ///
    /// * `workload` - What the report says it describes, if anything
    /// * `joined` - How many records joined
    fn report(workload: Option<&str>, joined: usize) -> StageReport {
        StageReport {
            version: STAGE_REPORT_VERSION,
            label: Some("L".to_string()),
            workload: workload.map(str::to_string),
            clock: "std::time::Instant".to_string(),
            clock_overhead_ns: 20,
            join: JoinStats {
                joined,
                ..JoinStats::default()
            },
            ops: BTreeMap::new(),
        }
    }

    /// A version 1 artifact is a bare report, and still reads
    ///
    /// Nine captures were taken before the layer profiled more than one workload. Refusing them
    /// would trade a corpus for a schema change that touches no field any of them holds.
    #[test]
    fn a_single_report_reads_as_a_one_workload_artifact() {
        let raw = serde_json::to_string(&report(None, 100)).expect("serializes");
        let reports =
            StageReports::read(&raw, std::path::Path::new("old.json")).expect("version 1 reads");
        assert_eq!(reports.version, STAGE_ARTIFACT_VERSION);
        assert_eq!(reports.reports.len(), 1);
        // filed under the only workload that could have written one
        assert!(reports.get(LEGACY_STAGE_WORKLOAD).is_some());
        assert_eq!(reports.join().joined, 100);
    }

    /// A version 1 report that does name its workload is filed under that name
    #[test]
    fn a_named_single_report_keeps_its_own_name() {
        let raw = serde_json::to_string(&report(Some("macro/grid/unsorted/r50/8192"), 5))
            .expect("serializes");
        let reports = StageReports::read(&raw, std::path::Path::new("one.json")).expect("reads");
        assert!(reports.get("macro/grid/unsorted/r50/8192").is_some());
    }

    /// An artifact holding several reports reads as several, and sums their joins
    #[test]
    fn several_reports_read_as_several() {
        let artifact = StageReports::new(BTreeMap::from([
            ("macro/grid/unsorted/r50/1024".to_string(), report(Some("a"), 3)),
            ("macro/grid/unsorted/r50/8192".to_string(), report(Some("b"), 4)),
        ]));
        let raw = serde_json::to_string(&artifact).expect("serializes");
        let read = StageReports::read(&raw, std::path::Path::new("new.json")).expect("reads");
        assert_eq!(read.reports.len(), 2);
        // summed, because the provenance records one join figure per layer
        assert_eq!(read.join().joined, 7);
    }

    /// The report a page draws when it wants one is the write path workload, when it is there
    ///
    /// Deterministic either way: two renders of one artifact must not disagree about which report
    /// they drew, or `render --check` fails for no reason.
    #[test]
    fn the_primary_report_is_the_write_path_one() {
        let with_legacy = StageReports::new(BTreeMap::from([
            ("macro/grid/unsorted/r50/1024".to_string(), report(Some("a"), 1)),
            (LEGACY_STAGE_WORKLOAD.to_string(), report(Some("b"), 2)),
        ]));
        assert_eq!(with_legacy.primary().expect("one").join.joined, 2);
        // and without it, the first by identifier rather than whichever the map yields first
        let without = StageReports::new(BTreeMap::from([
            ("macro/grid/unsorted/r50/8192".to_string(), report(Some("a"), 9)),
            ("macro/grid/unsorted/r50/1024".to_string(), report(Some("b"), 8)),
        ]));
        assert_eq!(without.primary().expect("one").join.joined, 8);
    }

    /// An artifact of a version this tool does not read is refused rather than rendered
    #[test]
    fn an_unknown_artifact_version_is_refused() {
        let mut artifact = StageReports::new(BTreeMap::from([(
            "macro/insert_unsorted".to_string(),
            report(Some("a"), 1),
        )]));
        artifact.version = STAGE_ARTIFACT_VERSION + 1;
        let raw = serde_json::to_string(&artifact).expect("serializes");
        assert!(StageReports::read(&raw, std::path::Path::new("future.json")).is_err());
    }
}
