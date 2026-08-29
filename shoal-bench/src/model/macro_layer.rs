//! The macro layer's artifact: what each purpose built workload measured
//!
//! # Two versions, one of which is only ever read
//!
//! [`MacroCaptureV1`] is the shape from when there was one workload and it was the `tmdb` example:
//! one result at the top level, with `insert` and `get` as fields. Seven committed captures are in
//! that shape and they are the historical record, so they are **never rewritten** - rewriting them
//! would mean the bytes on disk are no longer the bytes that were captured. They are lifted into
//! [`MacroCaptureV2`] on read instead, as a single workload named [`TMDB_WORKLOAD`], by
//! [`MacroCaptureV1::upgrade`].
//!
//! [`MacroCaptureV2`] is a map of workloads, each with its own operations, counters, scale and
//! configuration. Everything downstream of [`read`] sees only this shape.
//!
//! # Why version 1 needed a drift alarm and version 2 does not
//!
//! Version 1's structs were a hand kept mirror of `shoal::bencher::BenchResult` in another crate,
//! so [`MacroCaptureV1::extra`] catches every key this file does not name and a test asserts it
//! comes back empty over every committed artifact. Version 2 is written by
//! `crate::workloads::harness`, which builds *these* structs - the writer and the reader are the
//! same types, so there is nothing to drift and no catch-all to keep.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The schema version this tool writes
///
/// Version 1 held one workload's result at the top level, because there was one workload and it
/// was the `tmdb` example. Version 2 holds a map of them. Version 1 files are still read - they are
/// the historical record and are never rewritten - and are lifted into the version 2 shape as a
/// single workload named [`TMDB_WORKLOAD`]. See [`read`].
pub const MACRO_VERSION: u32 = 2;

/// The version of this artifact that predates purpose built workloads
pub const MACRO_VERSION_V1: u32 = 1;

/// What a version 1 capture's single workload is called once it is lifted
///
/// No version 2 capture will ever contain this id. The workload it names ran against a 65 MB CSV
/// that was never in the repository, so it cannot be reproduced and deliberately has no
/// replacement - a workload merely shaped like it would produce numbers that look comparable and
/// are not. A comparison that shares no workload with its baseline says so rather than comparing
/// nothing. See `docs/src/operations/performance-baseline.md`.
pub const TMDB_WORKLOAD: &str = "macro/tmdb";

/// How a workload's latency samples were taken
///
/// This is load bearing and must never be dropped from the artifact. The two are not comparable in
/// either direction: a per batch p99 is a batch completion time that charges every query for the
/// ones ahead of it, and a per query p99 is a service time. Recording which one a number is, is
/// what stops the two being joined by a comparison that has no way to tell them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// One timestamp covers a whole batch, and every query in it is charged for the batch
    ///
    /// What every version 1 capture is. A throughput workload runs this way on purpose: it
    /// saturates the pipeline, and the number worth reading off it is the wall clock.
    PerBatch,
    /// Every query is stamped on its own, at a bounded concurrency
    ///
    /// A service time. Costs throughput to measure, which is why only the workloads that exist to
    /// report a latency run this way.
    PerQuery,
}

impl Timing {
    /// The lowercase name of this timing mode
    pub fn as_str(&self) -> &'static str {
        // the stored name is the displayed name, so there is only one spelling to remember
        match self {
            Timing::PerBatch => "per_batch",
            Timing::PerQuery => "per_query",
        }
    }
}

/// How much data a workload built and how hard it drove it
///
/// Recorded so a capture is self describing. Two captures taken at different scales are not
/// comparable, and without this the artifact gives no way to notice that.
///
/// [`Default`] exists so that a workload can fill in the facts it has and leave the rest, which is
/// how every workload that is not a mixture says so: `..ScaleFacts::default()` reads as "no read
/// share, no width distribution, no skew, one client". It is not a usable value on its own - a
/// defaulted `scale` is the empty string, which no run ever produces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleFacts {
    /// Which named scale this run used
    pub scale: String,
    /// How many rows the workload built
    pub rows: u64,
    /// How wide each row's payload was, in bytes
    pub row_bytes: u64,
    /// How many distinct partition keys those rows covered
    pub keys: u64,
    /// How many queries the workload kept outstanding at once
    pub concurrency: u32,
    /// How many independent clients produced that load
    ///
    /// `None` means one, which is what every workload but the encryption client sweep uses and
    /// what every capture taken before that sweep existed did. It is separate from
    /// [`ScaleFacts::concurrency`] because the two are different axes: eight queries outstanding
    /// on one client share a connection pool, a response map and a set of TLS handshakes, where
    /// one query outstanding on each of eight clients has eight of each.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<u32>,
    /// What share of this workload's queries were reads, as a percentage
    ///
    /// `None` means the workload is not a mixture at all, which is what every workload before
    /// [F17](../../../docs/src/features/workload-grid.md) was: reads and writes were separate
    /// workloads so that neither could hide the other. A mixture is a different kind of
    /// measurement rather than a better one, and this field is what says which kind is being read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_pct: Option<u32>,
    /// Which named row width distribution the payloads were drawn from
    ///
    /// `None` means every row was exactly [`ScaleFacts::row_bytes`] wide, which is what a fixed
    /// width workload does. When this is set, `row_bytes` carries the **mean** of the distribution
    /// and this names the distribution it is the mean of - without it a mixture and a fixed width
    /// run of the same average are indistinguishable in the artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_profile: Option<String>,
    /// Which named key access distribution the queries were drawn from
    ///
    /// `None` means uniform, which is what every workload before the skew sweep used. Uniform is
    /// the honest default because it defeats every cache in the system; a skewed distribution
    /// measures a warmer table and the two must never be compared to each other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    /// Which kind of table the workload drove
    ///
    /// `None` on a workload whose identifier already says, which is every one that predates the
    /// grid. Recorded rather than parsed back out of the identifier, so a reader that groups by
    /// table type is reading a fact instead of a naming convention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_kind: Option<String>,
}

/// The server settings a workload ran against
///
/// Workloads now run under different configurations on purpose - a read workload that has to reach
/// disk needs a memory limit that forces eviction, and a durability workload changes nothing but
/// the barrier. Without these recorded, a comparison across a configuration change is silently
/// invalid rather than visibly so.
///
/// [`Default`] exists for the same reason [`ScaleFacts`]'s does: a caller fills in the facts it has
/// and leaves the rest. It is not a usable value on its own - a defaulted `memory` is the empty
/// string, which no run ever produces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfFacts {
    /// How many shards the server ran with
    pub shards: u64,
    /// The memory limit the shards were held to
    pub memory: String,
    /// Which durability barrier writes waited on
    pub durability: String,
    /// Whether the connection between the client and the shards was encrypted
    ///
    /// An encrypted capture and a plaintext one are not the same measurement, so this has to be on
    /// the artifact rather than inferred from the workload's name. It defaults to false when an
    /// older artifact is read, which is correct: every capture taken before
    /// [F14](../../../docs/src/features/encryption-in-transit.md) was plaintext.
    #[serde(default)]
    pub tls: bool,
    /// How many bytes the latency sensitive writer buffered before flushing
    ///
    /// The intent log's write size, and a minimum rather than an exact one: the writer rounds it up
    /// to the device's O_DIRECT alignment. `None` on every capture taken before
    /// [F20](../../../docs/src/features/configuration-sweeps.md), which is why this and the five
    /// fields below it are optional - un-skipping them would rewrite the whole committed corpus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_buffer_size: Option<u64>,
    /// How many writes the latency sensitive writer kept in flight at once
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_write_behind: Option<u64>,
    /// How large the intent log was allowed to grow before compaction was due
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_log_size: Option<String>,
    /// How many bytes the throughput sensitive writer buffered before flushing
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_buffer_size: Option<u64>,
    /// How many writes the throughput sensitive writer kept in flight at once
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_write_behind: Option<u64>,
    /// The largest frame the server would accept, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_bytes: Option<u64>,
    /// The level the subscriber this run installed was filtered at
    ///
    /// **This is the field that decides whether a capture is comparable**, more than
    /// [`ConfFacts::trace_remote`] below it. `#[instrument]` defaults to `INFO`, so anything at
    /// that level or finer switches on a span per query in `tracing`'s registry — the same class
    /// of cost [F5](../../../docs/src/features/flushed-sweep-gate.md) measured at 711,638 slab
    /// inserts per run for a single span it then removed. A run at `Warn` and a run at `Info` are
    /// two different measurements of two different programs.
    ///
    /// `None` on every capture taken before
    /// [F34](../../../docs/src/features/benchmark-tracing.md), which is correct: nothing installed
    /// a subscriber at all then, so there was no level for one to run at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_level: Option<String>,
    /// Whether spans were being exported to a collector while this was measured
    ///
    /// Recorded separately from the level because it costs separately: the level decides what is
    /// built, this decides what is serialized and POSTed off the box while the run is in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_remote: Option<bool>,
    /// A digest over the whole resolved configuration
    ///
    /// The named fields above are the ones worth reading. This covers everything else, so a
    /// configuration change that moved a setting nobody thought to name here still shows up.
    pub digest: String,
}

/// One run's contribution to a multi run workload capture
///
/// The version 2 shape of [`RunDetail`], which named `insert` and `get` as fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadRun {
    /// This run's wall clock in nanoseconds
    pub wall_clock_ns: u64,
    /// This run's latencies, per operation
    pub ops: BTreeMap<String, Stats>,
    /// How many rows this run moved, per counter
    pub counters: BTreeMap<String, u64>,
}

/// One workload's result within a capture
///
/// The fields from `runs` down are `None` in the per run file a workload writes and `Some` in the
/// folded capture, which is the same split version 1 had between what the example wrote and what
/// `scripts/bench.sh` grafted on afterwards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadCapture {
    /// How this workload's samples were taken
    pub timing: Timing,
    /// The seed every row of this workload was derived from
    pub seed: u64,
    /// How much data it built and how hard it drove it
    pub scale: ScaleFacts,
    /// The server settings it ran against, if it needed a server
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conf: Option<ConfFacts>,
    /// How many rows it moved, per counter
    pub counters: BTreeMap<String, u64>,
    /// The latencies of the run that was kept, per operation
    pub ops: BTreeMap<String, Stats>,
    /// How many runs were taken before the median was picked
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<u32>,
    /// Every run's wall clock in nanoseconds, sorted ascending
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_ns: Option<Vec<u64>>,
    /// The spread between the fastest and slowest run, as a percentage of the fastest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_pct: Option<f64>,
    /// Every run's full summary, in the order the runs were taken
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs_detail: Option<Vec<WorkloadRun>>,
}

impl WorkloadCapture {
    /// The median wall clock of this workload in nanoseconds
    pub fn median_wall_clock_ns(&self) -> u128 {
        // the recorded array is the honest source of the median, and is written sorted
        match &self.wall_clock_ns {
            Some(walls) if !walls.is_empty() => u128::from(walls[walls.len() / 2]),
            // a single run capture has no array, and its own wall clock is its median
            _ => 0,
        }
    }

    /// The observed interval of this workload's wall clock, if it was run more than once
    ///
    /// A single run has no interval, which is not the same as having a zero width one: a
    /// comparison against it cannot say whether a difference is a result.
    pub fn wall_clock_interval_ns(&self) -> Option<(u64, u64)> {
        // an interval needs at least two observations to mean anything
        match &self.wall_clock_ns {
            Some(walls) if walls.len() > 1 => Some((walls[0], walls[walls.len() - 1])),
            _ => None,
        }
    }

    /// Rows handled per second, counting every counter this workload kept
    pub fn rows_per_sec(&self) -> f64 {
        // a zero wall clock would be a divide by zero, and is not a run that happened
        let wall = self.median_wall_clock_ns();
        if wall == 0 {
            return 0.0;
        }
        let rows: u64 = self.counters.values().sum();
        rows as f64 / (wall as f64 / 1_000_000_000.0)
    }

    /// Queries answered per second, counting only what this workload actually asked for
    ///
    /// Separate from [`WorkloadCapture::rows_per_sec`], which sums every counter and so charges a
    /// mixture's reads and its writes to one figure while a fan-out workload's single query counts
    /// as the two hundred and fifty six rows it returned. This counts operations, which is the
    /// figure a throughput comparison across a read/write mixture needs.
    ///
    /// **Read it beside [`WorkloadCapture::timing`].** A [`Timing::PerQuery`] workload holds a
    /// bounded number of queries outstanding, so this is the throughput *at that depth* and not
    /// the throughput the server is capable of. Only a saturating [`Timing::PerBatch`] run
    /// produces the second thing.
    ///
    /// `None` when the workload counted no queries, which is every workload taken before
    /// [`OP_COUNTERS`] existed. That is deliberately not zero: a workload that did not count is
    /// not a workload that answered nothing, and a chart that plotted it at the origin would be
    /// inventing a measurement.
    pub fn ops_per_sec(&self) -> Option<f64> {
        // a zero wall clock would be a divide by zero, and is not a run that happened
        let wall = self.median_wall_clock_ns();
        if wall == 0 {
            return None;
        }
        // the operation counters, which are the ones a query is counted under exactly once. a row
        // counter cannot stand in for them: one fan-out query answers with two hundred and fifty
        // six rows, and one get of a wide row answers with one
        let mut ops = 0u64;
        let mut counted = false;
        for name in OP_COUNTERS {
            if let Some(seen) = self.counters.get(name) {
                ops += *seen;
                counted = true;
            }
        }
        // nothing counted queries, so there is no query rate to report
        if !counted {
            return None;
        }
        Some(ops as f64 / (wall as f64 / 1_000_000_000.0))
    }

    /// Payload bytes moved per second, as the workload's own row width reports them
    ///
    /// The width is the *mean* when [`ScaleFacts::row_profile`] is set, so a mixture's figure is a
    /// mean rate rather than a measured byte count. It covers payloads only: framing, keys and the
    /// archive's own overhead are not in it, so this is a floor on what crossed the wire and never
    /// a ceiling.
    pub fn bytes_per_sec(&self) -> Option<f64> {
        // one row's worth of payload per operation, at whatever width this workload ran
        self.ops_per_sec()
            .map(|ops| ops * self.scale.row_bytes as f64)
    }

    /// Reads one percentile metric out of the run that was kept
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read
    /// * `metric` - Which percentile to read, one of [`STAT_METRICS`]
    pub fn stat_ns(&self, op: &str, metric: &str) -> Option<u128> {
        // pick the operation, then the percentile within it
        self.ops
            .get(op)
            .and_then(|stats| stats.get(metric))
            .map(|dur| dur.as_nanos())
    }

    /// The observed interval of one percentile metric across every run of this workload
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read
    /// * `metric` - Which percentile to read, one of [`STAT_METRICS`]
    pub fn stat_interval_ns(&self, op: &str, metric: &str) -> Option<(u128, u128)> {
        // an interval needs the per run detail, and more than one run to be an interval at all
        let detail = self.runs_detail.as_ref()?;
        if detail.len() < 2 {
            return None;
        }
        // pull this metric out of every run
        let mut seen = Vec::with_capacity(detail.len());
        for run in detail {
            seen.push(run.ops.get(op)?.get(metric)?.as_nanos());
        }
        // the extremes of what was observed are the interval
        seen.sort_unstable();
        Some((seen[0], seen[seen.len() - 1]))
    }

    /// Every operation this workload recorded samples for, in a stable order
    pub fn op_names(&self) -> Vec<&str> {
        // a `BTreeMap` iterates sorted, which is what keeps the rendered page deterministic
        self.ops.keys().map(String::as_str).collect()
    }
}

/// A capture of every workload that ran under one label
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MacroCaptureV2 {
    /// The schema version this file was written with
    pub version: u32,
    /// The name this capture was taken under
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Each workload's result, keyed by its identifier
    ///
    /// A `BTreeMap` rather than a `Vec` because the identifier is the join key of every comparison
    /// and the sort order is what keeps the rendered page deterministic.
    pub workloads: BTreeMap<String, WorkloadCapture>,
}

impl MacroCaptureV2 {
    /// Creates an empty capture under a label
    ///
    /// # Arguments
    ///
    /// * `label` - The name to record this capture under
    pub fn new(label: Option<String>) -> Self {
        MacroCaptureV2 {
            version: MACRO_VERSION,
            label,
            workloads: BTreeMap::new(),
        }
    }

    /// Every workload this capture holds, in a stable order
    pub fn workload_ids(&self) -> Vec<&str> {
        self.workloads.keys().map(String::as_str).collect()
    }

    /// Whether this capture is one of the seven that predate purpose built workloads
    ///
    /// Used to explain a comparison that shares nothing with its baseline, which is otherwise an
    /// empty table with no reason attached.
    pub fn is_lifted_v1(&self) -> bool {
        // a version 1 capture lifts to exactly this one workload, and no version 2 capture will
        // ever contain it
        self.workloads.len() == 1 && self.workloads.contains_key(TMDB_WORKLOAD)
    }
}

/// Reads a macro capture of either version, lifting version 1 into the version 2 shape
///
/// The version field decides which struct the file is read into, rather than serde being asked to
/// guess with `untagged`. An untagged enum picks the first arm that deserializes, and these two
/// shapes are similar enough that a malformed version 2 file would quietly parse as some version 1
/// it never was.
///
/// # Arguments
///
/// * `path` - The file to read
pub fn read(path: &Path) -> Result<MacroCaptureV2, String> {
    // read once into a generic value so the version can be inspected before committing to a shape
    let body = std::fs::read_to_string(path)
        .map_err(|error| format!("reading {}: {error}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .map_err(|error| format!("parsing {}: {error}", path.display()))?;
    // an artifact with no version at all is not one of ours
    let version = value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("{}: no version field", path.display()))?;
    match version as u32 {
        MACRO_VERSION_V1 => {
            // the historical shape, lifted rather than rewritten
            let v1: MacroCaptureV1 = serde_json::from_value(value)
                .map_err(|error| format!("parsing {} as version 1: {error}", path.display()))?;
            Ok(v1.upgrade())
        }
        MACRO_VERSION => {
            // the shape this tool writes
            serde_json::from_value(value)
                .map_err(|error| format!("parsing {} as version 2: {error}", path.display()))
        }
        other => Err(format!(
            "{}: macro capture is version {other}, this tool reads versions {MACRO_VERSION_V1} \
             and {MACRO_VERSION}",
            path.display()
        )),
    }
}

/// A `std::time::Duration` in the shape serde gives it
///
/// `Duration` serializes as a two field struct, so every latency in this artifact is a pair
/// rather than a scalar. Comparisons and charts want nanoseconds, which is what
/// [`DurationParts::as_nanos`] is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationParts {
    /// Whole seconds
    pub secs: u64,
    /// Nanoseconds past the whole second, always under one billion
    pub nanos: u32,
}

impl DurationParts {
    /// Flattens this duration into nanoseconds
    pub fn as_nanos(&self) -> u128 {
        // one billion nanoseconds to the second, plus the remainder
        u128::from(self.secs) * 1_000_000_000 + u128::from(self.nanos)
    }

    /// Flattens this duration into nanoseconds as a float, for arithmetic on ratios
    pub fn as_nanos_f64(&self) -> f64 {
        // the largest duration this harness records is seconds, so the cast cannot lose anything
        // that matters at the precision anything downstream prints
        self.as_nanos() as f64
    }
}

/// The summary of one kind of operation's latencies across a whole run
///
/// Mirrors `shoal::bencher::Stats`. Insert and get are summarized apart because the client pools
/// its requests, which makes a percentile over the two together meaningless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    /// How many samples went into this summary
    pub count: u64,
    /// The slowest sample
    pub max: DurationParts,
    /// The 99th percentile, by nearest rank
    pub p99: DurationParts,
    /// The 95th percentile, by nearest rank
    pub p95: DurationParts,
    /// The 90th percentile, by nearest rank
    pub p90: DurationParts,
    /// The 50th percentile, by nearest rank
    pub p50: DurationParts,
    /// The mean of every sample
    pub avg: DurationParts,
    /// The fastest sample
    pub min: DurationParts,
}

impl Stats {
    /// Reads one named percentile out of this summary
    ///
    /// Used by the comparison engine, which iterates metric names rather than fields so that the
    /// set of macro metrics is declared in one place.
    ///
    /// # Arguments
    ///
    /// * `name` - The metric to read, one of `min`, `p50`, `p90`, `p95`, `p99`, `avg` or `max`
    pub fn get(&self, name: &str) -> Option<DurationParts> {
        // map the metric name onto the field that holds it
        match name {
            "min" => Some(self.min),
            "p50" => Some(self.p50),
            "p90" => Some(self.p90),
            "p95" => Some(self.p95),
            "p99" => Some(self.p99),
            "avg" => Some(self.avg),
            "max" => Some(self.max),
            _ => None,
        }
    }
}

/// The names of the percentile metrics a macro comparison covers, in the order they are reported
pub const STAT_METRICS: [&str; 7] = ["min", "p50", "p90", "p95", "p99", "avg", "max"];

/// The counters that hold a count of queries rather than a count of rows
///
/// [`WorkloadCapture::ops_per_sec`] sums these and nothing else. They are separate from `inserted`
/// and `retrieved` because those two count **rows**: one fan-out query is answered with two
/// hundred and fifty six of them and one keyed get with one, so a rate built from them is a row
/// rate and cannot be compared across workloads that return different numbers of rows per query.
pub const OP_COUNTERS: [&str; 2] = ["reads", "writes"];

/// One run's contribution to a multi run capture
///
/// `scripts/bench.sh` kept only every run's wall clock, so the median run's percentiles had no
/// error bar at all. This keeps each run's percentiles too, which is what lets a macro
/// comparison say whether a move in p99 is a result or is the spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDetail {
    /// This run's wall clock in nanoseconds
    pub wall_clock_ns: u64,
    /// This run's insert latencies
    pub insert: Stats,
    /// This run's get latencies
    pub get: Stats,
}

/// The macro layer's artifact for one label
///
/// The fields down to `retrieved` are `shoal::bencher::BenchResult` exactly. The four after it
/// are added when the runs are folded together, and are `Option` because the seven captures
/// taken before this tool existed do not have them - `runs_detail` is new here, and the other
/// three were added by `scripts/bench.sh`'s `jq` reduction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroCaptureV1 {
    /// The schema version this file was written with
    pub version: u32,
    /// The name this run was captured under
    pub label: Option<String>,
    /// The insert latencies of the run that was kept
    pub insert: Stats,
    /// The get latencies of the run that was kept
    pub get: Stats,
    /// The wall clock of the run that was kept
    pub total: DurationParts,
    /// How many rows were inserted
    pub inserted: u64,
    /// How many rows were read back
    pub retrieved: u64,
    /// How many runs were taken before the median was picked
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<u32>,
    /// Every run's wall clock in nanoseconds, sorted ascending
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_ns: Option<Vec<u64>>,
    /// The spread between the fastest and slowest run, as a percentage of the fastest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_pct: Option<f64>,
    /// Every run's full summary, in the order the runs were taken
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs_detail: Option<Vec<RunDetail>>,
    /// Anything in the file this struct does not name
    ///
    /// This exists to catch drift from `shoal::bencher::BenchResult`. It is asserted empty over
    /// every committed macro artifact by `tests/committed_artifacts.rs`, so a field added there
    /// fails a test that names it rather than disappearing.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl MacroCaptureV1 {
    /// Lifts this capture into the version 2 shape
    ///
    /// The whole capture becomes one workload named [`TMDB_WORKLOAD`], because that is what it
    /// was: one workload, run end to end, reported at the top level.
    ///
    /// Three mappings, each chosen so the lifted numbers are the recorded numbers and not a
    /// re-derivation of them:
    ///
    /// - `insert` and `get` become entries in `ops` under those names.
    /// - `inserted` and `retrieved` become entries in `counters`, so
    ///   [`WorkloadCapture::rows_per_sec`] sums to exactly what
    ///   [`MacroCaptureV1::rows_per_sec`] computed.
    /// - `total` is dropped, and `wall_clock_ns` is filled from it when the capture predates that
    ///   array. Version 1 stored both and they had to agree; keeping only the array means they
    ///   cannot disagree.
    ///
    /// The scale is recorded as the row counts the capture actually moved, and the timing as
    /// [`Timing::PerBatch`], which is what the `tmdb` workload was. There is no configuration to
    /// record: version 1 captures did not carry one.
    pub fn upgrade(self) -> MacroCaptureV2 {
        // the two operations version 1 named as fields
        let mut ops = BTreeMap::new();
        ops.insert("insert".to_string(), self.insert);
        ops.insert("get".to_string(), self.get);
        // the two counters it named as fields
        let mut counters = BTreeMap::new();
        counters.insert("inserted".to_string(), self.inserted);
        counters.insert("retrieved".to_string(), self.retrieved);
        // the wall clocks, falling back to `total` for the captures taken before the array existed
        let wall_clock_ns = self
            .wall_clock_ns
            .clone()
            .unwrap_or_else(|| vec![self.total.as_nanos() as u64]);
        // each run's own distribution, in the same shape, where the capture kept them
        let runs_detail = self.runs_detail.as_ref().map(|runs| {
            runs.iter()
                .map(|run| {
                    let mut ops = BTreeMap::new();
                    ops.insert("insert".to_string(), run.insert);
                    ops.insert("get".to_string(), run.get);
                    WorkloadRun {
                        wall_clock_ns: run.wall_clock_ns,
                        ops,
                        // version 1 kept row counts per capture, not per run
                        counters: BTreeMap::new(),
                    }
                })
                .collect()
        });
        let capture = WorkloadCapture {
            // the `tmdb` workload saturated and took one timestamp per batch
            timing: Timing::PerBatch,
            // it read a CSV rather than generating rows, so there was no seed
            seed: 0,
            scale: ScaleFacts {
                scale: "full".to_string(),
                rows: self.inserted + self.retrieved,
                // the row was TMDB's twenty four field shape, which had no single width
                row_bytes: 0,
                keys: self.inserted,
                concurrency: 0,
                clients: None,
                // it blended reads and writes without recording the share, which is one of the
                // reasons it was replaced. an invented share here would be worse than none
                ..ScaleFacts::default()
            },
            // version 1 captures did not record what they ran against
            conf: None,
            counters,
            ops,
            runs: self.runs,
            wall_clock_ns: Some(wall_clock_ns),
            spread_pct: self.spread_pct,
            runs_detail,
        };
        let mut lifted = MacroCaptureV2::new(self.label);
        lifted.workloads.insert(TMDB_WORKLOAD.to_string(), capture);
        lifted
    }

    /// The median wall clock of this capture in nanoseconds
    ///
    /// Falls back to `total` for the seven captures that predate `wall_clock_ns`, which is the
    /// same number - `bench.sh` kept the median run whole.
    pub fn median_wall_clock_ns(&self) -> u128 {
        // prefer the recorded array, which is the honest source of the median
        match &self.wall_clock_ns {
            Some(walls) if !walls.is_empty() => {
                // the array is written sorted, so the middle element is the median that was kept
                u128::from(walls[walls.len() / 2])
            }
            // no array means a single run capture, whose total is its own median
            _ => self.total.as_nanos(),
        }
    }

    /// The observed interval of this capture's wall clock, if it was run more than once
    ///
    /// A capture with one run has no interval, which is not the same as having a zero width one:
    /// a comparison against it cannot say whether a difference is a result.
    pub fn wall_clock_interval_ns(&self) -> Option<(u64, u64)> {
        // an interval needs at least two observations to mean anything
        match &self.wall_clock_ns {
            Some(walls) if walls.len() > 1 => {
                // the array is sorted, so the ends are the extremes
                Some((walls[0], walls[walls.len() - 1]))
            }
            _ => None,
        }
    }

    /// The observed interval of one percentile metric across every run of this capture
    ///
    /// Returns `None` for the seven captures taken before `runs_detail` existed. Those metrics
    /// are reported without an error bar and are never called a result.
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read, `insert` or `get`
    /// * `metric` - Which percentile to read, one of [`STAT_METRICS`]
    pub fn stat_interval_ns(&self, op: &str, metric: &str) -> Option<(u128, u128)> {
        // an interval needs the per run detail, which older captures do not carry
        let detail = self.runs_detail.as_ref()?;
        // and it needs more than one run to be an interval at all
        if detail.len() < 2 {
            return None;
        }
        // pull this metric out of every run
        let mut seen = Vec::with_capacity(detail.len());
        for run in detail {
            // pick the operation this metric belongs to
            let stats = match op {
                "insert" => run.insert,
                "get" => run.get,
                _ => return None,
            };
            // and the named percentile within it
            seen.push(stats.get(metric)?.as_nanos());
        }
        // the extremes of what was observed are the interval
        seen.sort_unstable();
        Some((seen[0], seen[seen.len() - 1]))
    }

    /// Reads one percentile metric out of the run that was kept
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read, `insert` or `get`
    /// * `metric` - Which percentile to read, one of [`STAT_METRICS`]
    pub fn stat_ns(&self, op: &str, metric: &str) -> Option<u128> {
        // pick the operation, then the percentile within it
        let stats = match op {
            "insert" => self.insert,
            "get" => self.get,
            _ => return None,
        };
        stats.get(metric).map(|dur| dur.as_nanos())
    }

    /// Rows handled per second, counting both the inserts and the reads
    pub fn rows_per_sec(&self) -> f64 {
        // a zero wall clock would be a divide by zero, and is not a run that happened
        let wall = self.median_wall_clock_ns();
        if wall == 0 {
            return 0.0;
        }
        // both phases moved rows, so both count toward the throughput of the run
        (self.inserted + self.retrieved) as f64 / (wall as f64 / 1_000_000_000.0)
    }

    /// Checks that this capture is the version this struct describes
    ///
    /// This struct is the version 1 shape specifically, so it only ever accepts version 1. What
    /// decides which struct a file is read into is the `version` field itself, which is why this
    /// is a check on a known shape rather than a check against whatever the tool writes today.
    ///
    /// # Arguments
    ///
    /// * `path` - The path this capture was read from, for the error message
    pub fn check_version(&self, path: &std::path::Path) -> Result<(), String> {
        // refuse anything that is not the shape this struct describes
        if self.version != MACRO_VERSION_V1 {
            return Err(format!(
                "{}: macro capture is version {}, this struct reads version {MACRO_VERSION_V1}",
                path.display(),
                self.version
            ));
        }
        Ok(())
    }
}
