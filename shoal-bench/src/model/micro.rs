//! The micro layer's artifact: criterion's per benchmark estimates, folded into one file
//!
//! This is the shape `scripts/collect-micro.sh` wrote and the shape every file in
//! `docs/perf/baselines/` and every `docs/perf/runs/*.micro.json` already has. It is deliberately
//! unchanged: `docs/perf/baselines/B1-performance.json` is frozen, and a schema change here would
//! either break it or force a migration of the one file that must never be rewritten.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The schema version this tool writes and is willing to read
pub const MICRO_VERSION: u32 = 1;

/// One criterion benchmark's estimate, in nanoseconds
///
/// The confidence interval is carried because criterion reports it, not because it is used to
/// judge anything - see the module docs on `crate::compare::micro` for why it is not.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MicroStat {
    /// The mean point estimate
    pub mean_ns: f64,
    /// The lower bound of criterion's confidence interval on the mean
    pub lower_ns: f64,
    /// The upper bound of criterion's confidence interval on the mean
    pub upper_ns: f64,
    /// The median point estimate
    pub median_ns: f64,
}

/// Every criterion benchmark from one capture
///
/// The keys of `benchmarks` are criterion's `full_id` strings verbatim. That is load bearing:
/// they are what a comparison joins on, so a run captured today can be compared against a
/// baseline captured before this tool existed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroCapture {
    /// The schema version this file was written with
    pub version: u32,
    /// When the capture was taken, as an RFC 3339 timestamp in UTC
    pub captured: String,
    /// Each benchmark's estimate, keyed by criterion's `full_id`
    ///
    /// A `BTreeMap` rather than a `HashMap` so that writing a capture back out, and every table
    /// and chart built from one, comes out in the same order every time.
    pub benchmarks: BTreeMap<String, MicroStat>,
}

impl MicroCapture {
    /// Creates an empty capture stamped with the current time
    ///
    /// # Arguments
    ///
    /// * `captured` - The RFC 3339 timestamp to stamp this capture with
    pub fn new<S: Into<String>>(captured: S) -> Self {
        MicroCapture {
            version: MICRO_VERSION,
            captured: captured.into(),
            benchmarks: BTreeMap::default(),
        }
    }

    /// Checks that this capture's schema version is one this tool understands
    ///
    /// A file of a different version is refused rather than compared, for the same reason
    /// `shoal::bencher` refuses one: a baseline that loads cleanly while describing a different
    /// measurement is worse than a baseline that fails to load.
    ///
    /// # Arguments
    ///
    /// * `path` - The path this capture was read from, for the error message
    pub fn check_version(&self, path: &std::path::Path) -> Result<(), String> {
        // refuse anything this tool was not written against
        if self.version != MICRO_VERSION {
            return Err(format!(
                "{}: micro capture is version {}, this tool reads version {MICRO_VERSION}",
                path.display(),
                self.version
            ));
        }
        Ok(())
    }
}
