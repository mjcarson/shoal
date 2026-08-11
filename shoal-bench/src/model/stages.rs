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
