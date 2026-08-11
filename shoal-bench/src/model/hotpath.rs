//! The hotpath layer's artifact: which scopes cost the most across a whole run
//!
//! This is `hotpath` 0.9's own JSON, emitted as the last line of stdout by a build of the `tmdb`
//! example made with the `hotpath` feature. It is an attribution layer only: the instrumented
//! build takes extra timestamps on the query path, so its wall clock is not comparable to the
//! uninstrumented build's and no latency or throughput number is ever taken from it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One instrumented scope's cost across a whole run
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HotpathScope {
    /// How many times this scope was entered
    pub calls: u64,
    /// The mean nanoseconds spent inside it
    pub avg: u64,
    /// The 50th percentile, in nanoseconds
    pub p50: u64,
    /// The 90th percentile, in nanoseconds
    pub p90: u64,
    /// The 95th percentile, in nanoseconds
    pub p95: u64,
    /// The 99th percentile, in nanoseconds
    pub p99: u64,
    /// The total nanoseconds spent inside it, summed over every shard that entered it
    pub total: u64,
    /// What hotpath believes this scope's share of the run was
    ///
    /// Never used, and never plotted. It is not normalised across concurrent scopes: twelve
    /// shards each spending most of a run inside a scope sum to well over 100%, and the committed
    /// `B1-performance.hotpath.json` reports `stream::write_helper` at 12,530%. See known issue
    /// 53. [`HotpathScope::total`] is the field to rank by.
    pub percent_total: f64,
}

/// A whole hotpath profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotpathProfile {
    /// What hotpath was measuring, `timing` for every profile this harness takes
    pub hotpath_profiling_mode: String,
    /// The wall clock of the instrumented run, in nanoseconds
    pub total_elapsed: u64,
    /// hotpath's own description of what the numbers mean
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The scope the profile was rooted at
    pub caller_name: String,
    /// Every scope that was entered, keyed by its fully qualified path
    ///
    /// A `BTreeMap` so that a table or chart built from a profile comes out in the same order
    /// every time.
    pub output: BTreeMap<String, HotpathScope>,
}

impl HotpathProfile {
    /// The scopes of this profile ranked by total time spent inside them, most expensive first
    ///
    /// Ranking is by [`HotpathScope::total`] rather than by `percent_total`, which is not a
    /// meaningful number for a concurrent scope.
    ///
    /// # Arguments
    ///
    /// * `limit` - How many scopes to return
    pub fn top_by_total(&self, limit: usize) -> Vec<(&str, &HotpathScope)> {
        // start from every scope in the profile
        let mut ranked: Vec<(&str, &HotpathScope)> = self
            .output
            .iter()
            .map(|(name, scope)| (name.as_str(), scope))
            .collect();
        // rank by total time descending, breaking ties on the name so the order is stable
        ranked.sort_by(|left, right| {
            right
                .1
                .total
                .cmp(&left.1.total)
                .then_with(|| left.0.cmp(right.0))
        });
        // and keep only as many as were asked for
        ranked.truncate(limit);
        ranked
    }
}
