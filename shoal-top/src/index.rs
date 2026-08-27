//! The portable description of a benchmark corpus, and the only thing the explorer plots
//!
//! # Why this is not the capture types
//!
//! `docs/perf/runs/` is thirteen megabytes across ninety-odd files, and almost all of it is detail
//! the explorer never draws: per run histograms, per operation counters, and a configuration block
//! repeated once per workload. What a chart needs is one number per (capture, workload, metric),
//! and enough provenance to say when two captures must not share an axis. That is what this is.
//!
//! It is also the only shape that can cross the crate boundary. `shoal-bench` links `walkdir`,
//! whose `same-file` selects its implementation on `cfg(unix)` and `cfg(windows)` and has no third
//! arm, so it cannot be compiled for `wasm32-unknown-unknown` at all. The corpus is therefore
//! *projected* into these types by `shoal_bench::explore::index`, and the explorer reads nothing
//! else.
//!
//! # Nothing here reads a clock, a file or a process
//!
//! `SystemTime::now` panics on `wasm32-unknown-unknown` and `std::process` is a stub there. Every
//! timestamp below is the RFC 3339 string the capture itself recorded, carried verbatim, and every
//! verdict was reached on the native side before the index was built. The crate's one dependency is
//! `serde`, and it must stay that way.
//!
//! # Absent is not zero
//!
//! Every measurement is an `Option` and stays one all the way to the plot. A capture that did not
//! count queries did not answer none of them, and a chart that drew it at the origin would have
//! invented a measurement. This is the same rule `WorkloadCapture::ops_per_sec` keeps by returning
//! `None`, and the whole of [`Source::series`] exists to carry it through unflattened.
//!
//! # How this grows
//!
//! [`INDEX_VERSION`] is a **floor**, not an equality check, which is deliberately unlike
//! `MicroCapture::check_version`. Those guard committed historical artifacts; this index is a
//! derived cache rebuilt on every invocation, so the only compatibility that matters is that a
//! stale browser bundle must refuse a newer index rather than silently mis-read it. Sections added
//! later are `#[serde(default)]`, and an empty one means *this index does not carry that layer*,
//! never *that layer measured nothing*.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The schema version this crate writes, and the newest one it is willing to read
pub const INDEX_VERSION: u32 = 1;

/// One of the four measurement layers
///
/// A copy of `shoal_bench::registry::Layer` rather than a re-export, because that one is a
/// `clap::ValueEnum` on a crate that cannot be built for wasm. The four names are the file name
/// infix of every artifact, so they are stable, and a test in `shoal-bench` asserts the two agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Layer {
    /// Criterion benchmarks over the partition internals
    Micro,
    /// The purpose built workloads, against a live server
    Macro,
    /// An instrumented build attributing time to scopes
    Hotpath,
    /// An instrumented build attributing a query's latency to stages
    Stages,
}

/// A staleness or comparability verdict, already reduced to what a badge shows
///
/// `shoal_bench::stale::CodeVerdict` and `EnvVerdict` are both collapsed into this. The UI draws a
/// word and a tooltip, and rebuilding two discriminated unions across a crate boundary that cannot
/// be compiled together would be two more things to keep in step. The **words are carried whole** -
/// never a colour, never an icon - which is the rule those two `label` methods already keep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    /// The word, exactly as the verdict's own `label` produced it
    pub label: String,
    /// Whether this verdict means the measurement still describes the tree the index was built at
    ///
    /// `None` rather than `false` for a capture that recorded no provenance. Several captures in
    /// the corpus predate the tool and said nothing about their tree; answering either way about
    /// them would be inventing the answer, so the UI draws them as unknown and says so.
    pub current: Option<bool>,
    /// What differs, for an incomparable environment, so the tooltip has something to name
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

/// One capture, and everything about how it was taken that decides what it may be drawn against
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capture {
    /// The name it was stored under, which is its `docs/perf/runs/` file stem
    pub label: String,
    /// When it was taken, as the RFC 3339 string the capture itself recorded
    ///
    /// A string, not an instant. It is a sort key and a tooltip and never arithmetic, and
    /// `shoal-bench` owns no date dependency on purpose.
    pub captured: String,
    /// The abbreviated commit it was taken at, empty for the captures that predate provenance
    pub head_short: String,
    /// Whether the working tree carried uncommitted changes when it was taken
    pub dirty: bool,
    /// Which layers produced an artifact at all
    pub layers: Vec<Layer>,
    /// Which of those covered the whole registry rather than the part a filter selected
    pub complete: Vec<Layer>,
    /// Whether a filter narrowed this capture to part of the registry
    ///
    /// A partial capture is not a lesser capture, it is a different one: every arm the filter
    /// excluded is *absent* rather than unchanged, so a line drawn through it is drawn through a
    /// hole. The picker marks it and the chart gaps it.
    pub partial: bool,
    /// What was concluded about each layer's code, in `layers` order
    pub code: Vec<(Layer, Verdict)>,
    /// Whether it was taken somewhere comparable to where the index was built
    pub env: Verdict,
    /// The hash of the fields two captures must agree on to be comparable
    ///
    /// Captures whose digests differ are still drawn together - refusing would be worse, since the
    /// numbers exist and somebody will go looking for them - but the chart carries a strip naming
    /// what differs. Joining them silently is how a governor change gets read as a regression.
    pub env_digest: String,
    /// The machine it ran on
    pub host: String,
    /// The scaling governor that machine was set to
    pub governor: String,
    /// The full `rustc -vV` version line
    pub rustc: String,
    /// The CPU model string
    pub cpu_model: String,
}

/// One family's four mandatory blocks, projected out of `shoal_bench::render::family::FAMILIES`
///
/// Projected, never retyped, and a test asserts the projection is total. These four are the only
/// thing on a generated results page that a chart cannot carry, so an explorer that dropped them
/// would be a downgrade wearing an upgrade's clothes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FamilyText {
    /// The short slug, which is the picker's group key
    pub name: String,
    /// What this group is called in prose
    pub title: String,
    /// The title of the generated page this family's charts are drawn on
    pub surface_title: String,
    /// That page's file name, so the explorer can point at the prose rather than replace it
    pub surface_link: String,
    /// What the numbers are of
    pub what_it_measures: String,
    /// How to turn a number here into a conclusion
    pub how_to_read_it: String,
    /// What would make a number here mean something other than it appears to
    pub what_would_make_it_wrong: String,
    /// The question a reader will want to ask that this family cannot answer
    pub what_it_cannot_say: String,
}

/// One workload identifier, and the family whose prose explains it
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    /// The identifier, verbatim, which is the key every comparison joins on
    pub id: String,
    /// Which family explains it, as an index into [`Index::families`]
    ///
    /// `None` for a workload no family claims. Those are still drawn, and drawn with a warning:
    /// a workload nothing explains is a gap in the documentation, not a reason to hide the number.
    pub family: Option<u32>,
}

/// How much data a workload built and how hard it drove it
///
/// A trimmed `ScaleFacts`: every field the pages select arms by, and nothing else. Interned,
/// because a couple of hundred distinct combinations cover every measurement in the corpus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleFactsLite {
    /// Which named scale this ran at, `full` or `smoke`
    ///
    /// The explorer refuses to put the two on one axis, for the reason a comparison joins on it:
    /// a p99 over two thousand rows is not a smaller version of a p99 over two hundred thousand.
    pub scale: String,
    /// How many rows the workload built
    pub rows: u64,
    /// How wide each row's payload was, as a mean when `row_profile` is set
    pub row_bytes: u64,
    /// How many distinct partition keys those rows covered
    pub keys: u64,
    /// How many queries it kept outstanding at once
    pub concurrency: u32,
    /// How many independent clients produced that load, absent meaning one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<u32>,
    /// What share of the queries were reads, absent meaning the workload was not a mixture
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_pct: Option<u32>,
    /// Which named row width distribution the payloads came from, absent meaning a fixed width
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_profile: Option<String>,
    /// Which key access distribution the queries came from, absent meaning uniform
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    /// Which kind of table it drove
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_kind: Option<String>,
}

/// The server settings a workload ran against
///
/// The configuration digest is deliberately **not** a field here. Interning on it would give a
/// distinct entry per workload rather than per configuration, which is thirty times more rows for
/// no more information; the digest rides on the measurement instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfFactsLite {
    /// How many shards the server ran
    pub shards: u64,
    /// The memory limit the shards were held to
    pub memory: String,
    /// Which durability barrier a write waited on
    pub durability: String,
    /// Whether the connection to the shards was encrypted
    pub tls: bool,
    /// The latency sensitive writer's buffer size, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_buffer_size: Option<u64>,
    /// How many latency sensitive writes were kept in flight
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_write_behind: Option<u64>,
    /// How large the intent log could grow before a compaction was due
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_log_size: Option<String>,
    /// The throughput sensitive writer's buffer size, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_buffer_size: Option<u64>,
    /// How many throughput sensitive writes were kept in flight
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_write_behind: Option<u64>,
    /// The largest frame the server would accept, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame_bytes: Option<u64>,
}

/// How a workload's latency samples were taken
///
/// Carried because the two are not comparable in either direction. A per batch p99 charges every
/// query for the ones queued ahead of it and a per query p99 is a service time; putting them in
/// one series is the easiest confident wrong answer this tool could produce, so [`Source::series`]
/// refuses to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// One timestamp covers a whole batch
    PerBatch,
    /// Every query is stamped on its own, at a bounded concurrency
    PerQuery,
}

impl Timing {
    /// How this stamping is described where a sentence needs to name it
    pub fn phrase(&self) -> &'static str {
        // written to read in the slot `AxisUnits::differs_from` puts it in
        match self {
            Timing::PerBatch => "per batch",
            Timing::PerQuery => "per query",
        }
    }
}

/// One operation's percentiles within one measurement, in nanoseconds
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OpStats {
    /// How many samples went into this summary
    pub count: u64,
    /// The fastest sample
    pub min_ns: u64,
    /// The median sample
    pub p50_ns: u64,
    /// The 90th percentile
    pub p90_ns: u64,
    /// The 95th percentile
    pub p95_ns: u64,
    /// The 99th percentile
    pub p99_ns: u64,
    /// The mean of every sample
    pub avg_ns: u64,
    /// The slowest sample
    pub max_ns: u64,
}

impl OpStats {
    /// Reads one percentile out of this summary
    ///
    /// # Arguments
    ///
    /// * `percentile` - Which one to read
    pub fn at(&self, percentile: Percentile) -> u64 {
        // one arm per recorded rank, which is the whole of what `Stats` carries
        match percentile {
            Percentile::Min => self.min_ns,
            Percentile::P50 => self.p50_ns,
            Percentile::P90 => self.p90_ns,
            Percentile::P95 => self.p95_ns,
            Percentile::P99 => self.p99_ns,
            Percentile::Avg => self.avg_ns,
            Percentile::Max => self.max_ns,
        }
    }
}

/// One workload's result within one capture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroPoint {
    /// Which capture, as an index into [`Index::captures`]
    pub capture: u32,
    /// Which workload, as an index into [`Index::workloads`]
    pub workload: u32,
    /// Which scale facts it ran under, as an index into [`Index::scales`]
    pub scale: u32,
    /// Which server configuration it ran against, as an index into [`Index::confs`]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conf: Option<u32>,
    /// The digest over the whole resolved configuration, for an exact comparability check
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub conf_digest: String,
    /// How this workload's samples were taken
    pub timing: Timing,
    /// Queries answered per second, absent when nothing counted queries
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ops_per_sec: Option<f64>,
    /// Rows handled per second, summing every counter
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_per_sec: Option<f64>,
    /// Payload bytes moved per second, a floor on what crossed the wire
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_sec: Option<f64>,
    /// The median wall clock across the runs taken, in nanoseconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_ns: Option<u64>,
    /// The fastest and slowest wall clock observed, absent for a single run capture
    ///
    /// A single run has no interval, which is not a zero width one. The explorer draws no band at
    /// all rather than a flat one, because a flat band reads as a tight measurement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_interval_ns: Option<(u64, u64)>,
    /// The spread between the fastest and slowest run, as a percentage of the fastest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_pct: Option<f64>,
    /// How many runs were taken before the median was picked
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<u32>,
    /// Every percentile of every operation this workload recorded, keyed by operation name
    ///
    /// A `BTreeMap` so that two builds of one corpus walk it in the same order and produce the
    /// same bytes, which is the determinism rule the generated pages already keep.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ops: BTreeMap<String, OpStats>,
}

/// Everything the explorer draws from
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    /// The schema version this index was written with
    pub version: u32,
    /// The commit it was built against, abbreviated
    ///
    /// Every verdict below is relative to this. An index whose verdicts have no referent is making
    /// claims about nothing.
    pub head_short: String,
    /// Whether anything outside the generated pages was uncommitted when it was built
    pub dirty: bool,
    /// Every capture, **oldest first**, ties broken on the label
    ///
    /// Sorted here rather than in the UI, so that the timeline axis is the same for two readers of
    /// the same index.
    pub captures: Vec<Capture>,
    /// Every family a workload can belong to, in declaration order
    pub families: Vec<FamilyText>,
    /// Every workload identifier seen anywhere in the corpus, sorted
    pub workloads: Vec<Workload>,
    /// Every distinct set of scale facts, referenced by index from a measurement
    pub scales: Vec<ScaleFactsLite>,
    /// Every distinct server configuration, referenced by index from a measurement
    pub confs: Vec<ConfFactsLite>,
    /// Every macro measurement, sorted by capture then workload
    pub macro_points: Vec<MacroPoint>,
}

/// Which rank of a latency summary to plot
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Percentile {
    /// The fastest sample
    Min,
    /// The median sample
    P50,
    /// The 90th percentile
    P90,
    /// The 95th percentile
    P95,
    /// The 99th percentile
    P99,
    /// The mean of every sample
    Avg,
    /// The slowest sample
    Max,
}

impl Percentile {
    /// Every rank, in the order a picker should offer them
    pub const ALL: [Percentile; 7] = [
        Percentile::Min,
        Percentile::P50,
        Percentile::P90,
        Percentile::P95,
        Percentile::P99,
        Percentile::Avg,
        Percentile::Max,
    ];

    /// The name this rank is recorded and displayed under
    pub fn as_str(&self) -> &'static str {
        // the stored name is the displayed name, so there is only one spelling to remember
        match self {
            Percentile::Min => "min",
            Percentile::P50 => "p50",
            Percentile::P90 => "p90",
            Percentile::P95 => "p95",
            Percentile::P99 => "p99",
            Percentile::Avg => "avg",
            Percentile::Max => "max",
        }
    }
}

/// What a value is in, which decides how an axis label is formatted
///
/// The two rates are separate variants rather than one `Rate`, because they are separate
/// quantities: a chart of queries a second and one of rows a second answer different questions,
/// and [`crate::plot::unit_name`] has to be able to say which it is looking at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// Queries answered per second
    QueryRate,
    /// Rows handled per second
    RowRate,
    /// Bytes per second
    ByteRate,
    /// A duration in nanoseconds
    Duration,
    /// A percentage, already scaled to 0..100
    Percent,
}

/// What the chart puts on its value axis
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Metric {
    /// Queries answered per second
    OpsPerSec,
    /// Rows handled per second
    RowsPerSec,
    /// Payload bytes moved per second
    BytesPerSec,
    /// The median wall clock of the whole workload
    WallClock,
    /// The spread between the fastest and slowest run
    Spread,
    /// One rank of one operation's latency
    Latency {
        /// Which operation, by the name it was recorded under
        op: String,
        /// Which rank of it
        percentile: Percentile,
    },
}

impl Metric {
    /// What this metric's values are in
    pub fn unit(&self) -> Unit {
        // the unit is a property of the metric, so a formatter never has to guess from magnitude
        match self {
            Metric::OpsPerSec => Unit::QueryRate,
            Metric::RowsPerSec => Unit::RowRate,
            Metric::BytesPerSec => Unit::ByteRate,
            Metric::WallClock | Metric::Latency { .. } => Unit::Duration,
            Metric::Spread => Unit::Percent,
        }
    }

    /// What to label the value axis with
    pub fn axis_label(&self) -> String {
        // spelled the way the generated pages spell it, so the two read alike
        match self {
            Metric::OpsPerSec => "queries answered per second".to_string(),
            Metric::RowsPerSec => "rows handled per second".to_string(),
            Metric::BytesPerSec => "payload bytes per second".to_string(),
            Metric::WallClock => "wall clock".to_string(),
            Metric::Spread => "spread between the fastest and slowest run".to_string(),
            Metric::Latency { op, percentile } => format!("{op} {}", percentile.as_str()),
        }
    }

    /// Whether a larger value is a better one
    ///
    /// Used only to colour a delta, never to reorder anything: which direction is better is a fact
    /// about the metric, and guessing it from the numbers is how a latency chart ends up claiming a
    /// regression is an improvement.
    pub fn larger_is_better(&self) -> bool {
        // the three rates go up when things improve; everything else is a cost
        matches!(
            self,
            Metric::OpsPerSec | Metric::RowsPerSec | Metric::BytesPerSec
        )
    }

    /// Which kind of measurement this is, before an operation and a rank refine a latency
    pub fn kind(&self) -> MetricKind {
        // the five whole-workload numbers stand for themselves; every latency collapses to one
        // kind, because the operation and the rank are chosen separately
        match self {
            Metric::OpsPerSec => MetricKind::OpsPerSec,
            Metric::RowsPerSec => MetricKind::RowsPerSec,
            Metric::BytesPerSec => MetricKind::BytesPerSec,
            Metric::WallClock => MetricKind::WallClock,
            Metric::Spread => MetricKind::Spread,
            Metric::Latency { .. } => MetricKind::Latency,
        }
    }
}

/// The kind of measurement on the value axis, before an operation and a rank refine a latency
///
/// The metric control is three lists rather than one, because a flat list is the product of the
/// operations the corpus recorded and the seven ranks of each - thirty three entries, of which any
/// one workload can answer at most fourteen. Splitting it means no list is ever longer than seven,
/// and that choosing an operation is a separate act from choosing which rank of it to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Queries answered per second
    OpsPerSec,
    /// Rows handled per second
    RowsPerSec,
    /// Payload bytes moved per second
    BytesPerSec,
    /// The median wall clock of the whole workload
    WallClock,
    /// The spread between the fastest and slowest run
    Spread,
    /// One rank of one operation's latency, which an operation and a rank still have to refine
    Latency,
}

impl MetricKind {
    /// Every kind, in the order the metric list offers them
    ///
    /// The rates first, which is what most questions are about, and the latency last, because it
    /// is the only one that opens two more lists.
    pub const ALL: [MetricKind; 6] = [
        MetricKind::OpsPerSec,
        MetricKind::RowsPerSec,
        MetricKind::BytesPerSec,
        MetricKind::WallClock,
        MetricKind::Spread,
        MetricKind::Latency,
    ];

    /// What to call this kind in the list that offers it
    pub fn label(&self) -> &'static str {
        // spelled the way `Metric::axis_label` spells the same thing, so the list and the axis
        // under the chart read alike
        match self {
            MetricKind::OpsPerSec => "queries answered per second",
            MetricKind::RowsPerSec => "rows handled per second",
            MetricKind::BytesPerSec => "payload bytes per second",
            MetricKind::WallClock => "wall clock",
            MetricKind::Spread => "spread between the fastest and slowest run",
            MetricKind::Latency => "latency",
        }
    }
}

/// Which recorded fact a sweep puts on its key axis
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepAxis {
    /// The share of queries that were reads
    ReadShare,
    /// How wide each row's payload was
    RowBytes,
    /// How many queries were outstanding at once
    Concurrency,
    /// How many rows the workload built
    Rows,
    /// How many distinct partition keys those rows covered
    Keys,
}

impl SweepAxis {
    /// Every axis, in the order a picker should offer them
    pub const ALL: [SweepAxis; 5] = [
        SweepAxis::ReadShare,
        SweepAxis::RowBytes,
        SweepAxis::Concurrency,
        SweepAxis::Rows,
        SweepAxis::Keys,
    ];

    /// What to label this axis with
    pub fn label(&self) -> &'static str {
        // the same words the generated pages use for the same axis
        match self {
            SweepAxis::ReadShare => "reads, as a share of all queries",
            SweepAxis::RowBytes => "row width",
            SweepAxis::Concurrency => "queries outstanding at once",
            SweepAxis::Rows => "rows built",
            SweepAxis::Keys => "distinct partition keys",
        }
    }

    /// Renders one value on this axis the way its quantity is normally written
    ///
    /// # Arguments
    ///
    /// * `value` - The position on this axis
    pub fn format(&self, value: f64) -> String {
        // each axis is a different quantity, so each gets the formatter for that quantity rather
        // than a shared numeric one that would print a row width as `1024` and a share as `50`
        match self {
            SweepAxis::ReadShare => format!("{}%", crate::fmt::fixed(value, 0)),
            SweepAxis::RowBytes => crate::fmt::bytes_axis(value),
            SweepAxis::Concurrency | SweepAxis::Rows | SweepAxis::Keys => {
                crate::fmt::thousands(value.max(0.0).round() as u128)
            }
        }
    }

    /// Reads this axis's value out of one workload's scale facts
    ///
    /// # Arguments
    ///
    /// * `scale` - The facts the measurement recorded
    pub fn value(&self, scale: &ScaleFactsLite) -> Option<f64> {
        // read the recorded fact, never a number parsed back out of the identifier
        match self {
            SweepAxis::ReadShare => scale.read_pct.map(f64::from),
            SweepAxis::RowBytes => Some(scale.row_bytes as f64),
            SweepAxis::Concurrency => Some(f64::from(scale.concurrency)),
            SweepAxis::Rows => Some(scale.rows as f64),
            SweepAxis::Keys => Some(scale.keys as f64),
        }
    }
}

/// What the chart puts on its key axis
///
/// The two answer different questions and both are needed. A sweep says what a curve looks like and
/// how a whole curve moved between captures; a timeline says whether one number is going up or
/// down. Neither subsumes the other: a timeline of a single capture is one point, and a sweep says
/// nothing about when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
    /// A recorded fact, one point per workload arm
    Sweep(SweepAxis),
    /// The capture, in the order the captures were taken
    Timeline,
}

/// What one measurement's position on the key axis is a quantity of
///
/// The key axis is chosen once for the whole chart, so every measurement on it is nominally in the
/// same quantity - but two of them are not, and this is what separates them. A workload that is not
/// a mixture has no share to sit at, and a workload whose payloads come from a declared
/// distribution has a *mean* width where the one beside it has a measured one. `row_size.rs` says
/// why that matters in the page it draws: plotting a mean beside a measurement invites the two to
/// be read alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyUnit {
    /// The capture it was taken in
    Capture,
    /// A share of all queries, as a percentage
    Share,
    /// A fixed row width, in bytes
    Width,
    /// A mean row width over a declared distribution, in bytes
    MeanWidth,
    /// A count of queries outstanding at once
    Depth,
    /// A count of rows
    Rows,
    /// A count of distinct partition keys
    Keys,
}

impl KeyUnit {
    /// What one measurement's position on an axis is in, or nothing when it has no position on it
    ///
    /// # Arguments
    ///
    /// * `axis` - The key axis the chart is drawn against
    /// * `scale` - The facts the measurement recorded
    pub fn of(axis: Axis, scale: &ScaleFactsLite) -> Option<KeyUnit> {
        match axis {
            // every capture is a capture, so a timeline puts everything in one quantity
            Axis::Timeline => Some(KeyUnit::Capture),
            // a workload that is not a mixture has no share, which is not a share of zero
            Axis::Sweep(SweepAxis::ReadShare) => scale.read_pct.map(|_| KeyUnit::Share),
            // a declared distribution of widths reports its mean, which is a different quantity
            // from a width somebody set
            Axis::Sweep(SweepAxis::RowBytes) => match scale.row_profile {
                Some(_) => Some(KeyUnit::MeanWidth),
                None => Some(KeyUnit::Width),
            },
            Axis::Sweep(SweepAxis::Concurrency) => Some(KeyUnit::Depth),
            Axis::Sweep(SweepAxis::Rows) => Some(KeyUnit::Rows),
            Axis::Sweep(SweepAxis::Keys) => Some(KeyUnit::Keys),
        }
    }

    /// How this quantity is described where a sentence needs to name it
    pub fn phrase(&self) -> &'static str {
        // written to read in the slot `differs_from` puts it in, which is mid sentence
        match self {
            KeyUnit::Capture => "the capture it was taken in",
            KeyUnit::Share => "a share of all queries",
            KeyUnit::Width => "a fixed row width",
            KeyUnit::MeanWidth => "a mean width over a declared distribution",
            KeyUnit::Depth => "how many queries were outstanding",
            KeyUnit::Rows => "how many rows were built",
            KeyUnit::Keys => "how many partition keys they covered",
        }
    }
}

/// The units one measurement's two axes are in
///
/// Two measurements may share a chart when these agree, and for no other reason. This is
/// deliberately weaker than *they measure the same thing*: an insert loop and a read/write mixture
/// are both queries a second and are allowed onto one axis, because the reader asking how the two
/// compare is asking a real question. What is refused is a comparison that cannot be read at all -
/// a rate over a hundredth of the rows, a service time beside a queueing delay, a mean width beside
/// a measured one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AxisUnits {
    /// What the value axis is in
    pub value: Unit,
    /// What the key axis is in, absent when the measurement has no position on it at all
    pub key: Option<KeyUnit>,
    /// Which named scale the rows were built at
    ///
    /// Part of the units because a rate over two thousand rows is not a smaller version of one over
    /// two hundred thousand; it is a different measurement wearing the same label.
    pub scale: String,
    /// How the samples were stamped, carried only for a metric whose meaning that changes
    ///
    /// Absent for every metric except a latency. A rate is a count over a wall clock and is the
    /// same quotient either way; a percentile is not.
    pub timing: Option<Timing>,
}

impl AxisUnits {
    /// Says what stops this measurement from sharing an axis with another, in a reader's words
    ///
    /// Returns nothing when the two agree. Only the first difference is named: a tooltip has one
    /// line, and a reader who fixes the first will be told about the second.
    ///
    /// # Arguments
    ///
    /// * `anchor` - The units already on the chart, which this one has to match
    pub fn differs_from(&self, anchor: &AxisUnits) -> Option<String> {
        // no position on the axis at all, which is not a difference of degree
        let Some(key) = self.key else {
            return Some("it has no position on this axis".to_string());
        };
        // a mean where the chart holds measurements, or the reverse
        if let Some(theirs) = anchor.key {
            if key != theirs {
                return Some(format!(
                    "its position on this axis is {}, not {}",
                    key.phrase(),
                    theirs.phrase()
                ));
            }
        }
        // a hundredth of the data is a different measurement, not a smaller one
        if self.scale != anchor.scale {
            return Some(format!(
                "it was measured at the {} scale, not the {} one",
                self.scale, anchor.scale
            ));
        }
        // the value axis, which only differs if a caller built these against two metrics
        if self.value != anchor.value {
            return Some("it is not measured in the same quantity".to_string());
        }
        // a per batch percentile charges a query for the ones queued ahead of it, and a per query
        // one is a service time
        match (self.timing, anchor.timing) {
            (Some(mine), Some(theirs)) if mine != theirs => Some(format!(
                "its samples are stamped {}, not {}",
                mine.phrase(),
                theirs.phrase()
            )),
            _ => None,
        }
    }
}

/// The facts that say which curve an arm sits on, rather than where on that curve it sits
///
/// An ordered list of phrases rather than a struct of options, and every entry reads on its own, so
/// that naming what separates two curves is one walk down a pair of lists and the answer is already
/// in words. Every key built in one call has the same length, because the fact the sweep axis is
/// reading is left out of all of them or none.
type CurveKey = Vec<String>;

/// The tables a measurement can name, in the order the book's charts read them in
///
/// A mirror of `shoal_bench::render::arms::table_kinds`, which cannot be linked from here:
/// `shoal-bench` pulls `walkdir`, which does not build for `wasm32-unknown-unknown`. `preset.rs`
/// re-declares its constants for the same reason. The order is the persistent pair before the
/// ephemeral one and the unsorted table before the sorted one - the control after the thing it
/// controls for - which is what every chart and table on the site puts them in.
///
/// **These are slots, not a filter.** All four are reserved whether or not a chart draws them, so
/// that a table's colour is a property of the table rather than of what happens to be ticked.
pub const TABLE_ORDER: [&str; 4] = [
    "persistent_unsorted",
    "persistent_sorted",
    "ephemeral_unsorted",
    "ephemeral_sorted",
];

/// One curve as it is going to be drawn, after a colliding one has been split
///
/// The step between [`Index::curve_key`] grouping arms and [`Source::series`] drawing them. It
/// exists because a curve key can be wrong in one direction that the key itself cannot fix: two
/// workloads that set none of the recorded facts differently are one key and two workloads.
struct DrawnCurve {
    /// What separates it from the other curves being drawn, or nothing when it stands alone
    name: Option<String>,
    /// Which table it ran against, which is what decides its colour
    table: Option<String>,
    /// The workloads whose arms it joins, in the order they were ticked
    members: Vec<u32>,
}

/// The colour bucket each curve sits in
///
/// # Arguments
///
/// * `tables` - The table each curve ran against, in the order they will be drawn
fn hues(tables: &[Option<String>]) -> Vec<u32> {
    let base = TABLE_ORDER.len() as u32;
    // the slots past the four reserved ones: one per table kind this build does not know, and one
    // per curve that names no table at all
    let mut extra: Vec<Option<String>> = Vec::new();
    tables
        .iter()
        .map(|table| {
            if let Some(kind) = table {
                // a table the book already reads in a fixed order keeps that order's slot
                if let Some(at) = TABLE_ORDER.iter().position(|known| known == kind) {
                    return at as u32;
                }
                // an unknown kind is still a kind, so every curve on it shares one slot rather than
                // taking one each
                if let Some(at) = extra.iter().position(|seen| seen.as_deref() == Some(kind)) {
                    return base + at as u32;
                }
            }
            // a kind seen for the first time, or a curve that names none and so shares with nothing
            extra.push(table.clone());
            base + extra.len() as u32 - 1
        })
        .collect()
}

/// Which of its colour's curves each one is
///
/// # Arguments
///
/// * `hues` - The colour bucket each curve sits in, in the order they will be drawn
fn marks(hues: &[u32]) -> Vec<u32> {
    // counted in draw order rather than assigned from a table, so a chart of one line per table is
    // every mark zero and carries no markers at all
    hues.iter()
        .enumerate()
        .map(|(at, hue)| hues[..at].iter().filter(|seen| *seen == hue).count() as u32)
        .collect()
}

/// Whether two of a curve's arms would be drawn at one position on the key axis
///
/// # Arguments
///
/// * `positions` - Where each of the curve's arms sits, absent when it has no position at all
fn collides(positions: &[Option<f64>]) -> bool {
    for (at, left) in positions.iter().enumerate() {
        // an arm with no position on this axis draws nothing, so it collides with nothing
        let Some(left) = left else {
            continue;
        };
        if positions[at + 1..]
            .iter()
            .flatten()
            .any(|right| (left - right).abs() < f64::EPSILON)
        {
            return true;
        }
    }
    false
}

/// How the selected series are drawn
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chart {
    /// Joined by a line, gapped wherever a measurement is absent
    Line,
    /// One bar per point, grouped by position on the key axis
    Bars,
}

/// What the reader has asked to see
#[derive(Debug, Clone)]
pub struct Selection {
    /// Which captures to draw, as indices into [`Index::captures`]
    pub captures: Vec<u32>,
    /// Which workloads to draw, as indices into [`Index::workloads`]
    pub workloads: Vec<u32>,
    /// What to put on the value axis
    pub metric: Metric,
    /// What to put on the key axis
    pub axis: Axis,
}

/// One line or one group of bars, with its points already in axis order
#[derive(Debug, Clone)]
pub struct Series {
    /// What the legend calls it
    pub name: String,
    /// Which capture it came from, as an index into [`Index::captures`]
    ///
    /// Carried so the chart can give every series from one capture the same line style, leaving
    /// hue free to mean the workload or the table. With several captures and several tables
    /// selected there are more series than there are distinguishable colours.
    pub capture: u32,
    /// Which colour this line is drawn in, as a position in the palette
    ///
    /// **The table it ran against**, at that table's fixed slot in [`TABLE_ORDER`], so a table keeps
    /// its colour however many other tables are on the chart and whichever order they were ticked
    /// in. A curve whose measurement records a table kind this build does not know shares one slot
    /// with every other curve on that kind; a curve that records none at all takes a slot of its
    /// own, because there is nothing it is known to have in common with another.
    pub hue: u32,
    /// Which of its colour's curves this is, counted in the order they are drawn
    ///
    /// The other half of the split, and what a marker stands for. Two captures of one curve share a
    /// `hue` and a `mark` and differ in `capture`, which is what draws them as the same colour in
    /// two dash patterns - the comparison the explorer exists to make.
    pub mark: u32,
    /// The points, as key against value
    ///
    /// The value is an `Option` and stays one. A `None` is a measurement that was never taken, and
    /// the chart draws a gap; flattening it to zero here would be inventing a data point at the one
    /// place a reader is most likely to misread it.
    pub points: Vec<(f64, Option<f64>)>,
}

/// Where the explorer's series come from
///
/// The corpus is the first implementor and a live server is meant to be the second, which is why
/// the chart is written against this rather than against [`Index`]. Nothing here is allowed to
/// touch a file, a socket or a clock - an implementor does that on its own side and hands over
/// points that are already ordered.
pub trait Source {
    /// The captures, or whatever this source's equivalent of a capture is
    fn captures(&self) -> &[Capture];

    /// The workloads this source can draw
    fn workloads(&self) -> &[Workload];

    /// The families whose prose explains what this source can draw
    fn families(&self) -> &[FamilyText];

    /// Builds the series a selection asks for
    ///
    /// # Arguments
    ///
    /// * `selection` - What the reader has asked to see
    fn series(&self, selection: &Selection) -> Vec<Series>;

    /// The tick labels for the key axis, when it is one a formatter cannot label on its own
    ///
    /// Returns the label for each integer position, or an empty vector when the axis is a real
    /// quantity that should be formatted numerically.
    fn key_labels(&self, selection: &Selection) -> Vec<String>;
}

impl Index {
    /// Refuses an index this build cannot read
    ///
    /// A floor rather than an equality check: an older index is readable because every section
    /// added since is `#[serde(default)]`, and a newer one is not, because a stale browser bundle
    /// silently dropping a section it does not know about is worse than a message telling somebody
    /// to rebuild.
    pub fn check_version(&self) -> Result<(), String> {
        // a newer index carries meanings this build has never been told about
        if self.version > INDEX_VERSION {
            return Err(format!(
                "this index is version {}, and this build of the explorer understands {}. Rebuild \
                 the explorer with `shoal-bench explore --serve --build`.",
                self.version, INDEX_VERSION
            ));
        }
        Ok(())
    }

    /// Finds one workload's measurement within one capture
    ///
    /// # Arguments
    ///
    /// * `capture` - Which capture, as an index into [`Index::captures`]
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    pub fn macro_point(&self, capture: u32, workload: u32) -> Option<&MacroPoint> {
        // the vector is written sorted on exactly this pair, so the lookup is a binary search
        // rather than the scan a map of tuples would have cost to deserialize
        self.macro_points
            .binary_search_by(|probe| {
                probe
                    .capture
                    .cmp(&capture)
                    .then_with(|| probe.workload.cmp(&workload))
            })
            .ok()
            .map(|at| &self.macro_points[at])
    }

    /// Reads one metric out of one measurement
    ///
    /// # Arguments
    ///
    /// * `point` - The measurement to read
    /// * `metric` - Which number to take from it
    pub fn value(&self, point: &MacroPoint, metric: &Metric) -> Option<f64> {
        // every arm returns `None` rather than a default, so an absent measurement stays absent
        match metric {
            Metric::OpsPerSec => point.ops_per_sec,
            Metric::RowsPerSec => point.rows_per_sec,
            Metric::BytesPerSec => point.bytes_per_sec,
            Metric::WallClock => point.wall_clock_ns.map(|ns| ns as f64),
            Metric::Spread => point.spread_pct,
            Metric::Latency { op, percentile } => point
                .ops
                .get(op)
                .map(|stats| stats.at(*percentile) as f64),
        }
    }

    /// The most recent capture that measured anything on a layer
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer it has to carry
    pub fn newest_with(&self, layer: Layer) -> Option<u32> {
        // the captures are oldest first, so the last match is the most recent one
        self.captures
            .iter()
            .enumerate()
            .filter(|(_, capture)| capture.layers.contains(&layer))
            .next_back()
            .map(|(at, _)| at as u32)
    }

    /// Names what stops a set of captures from being read against each other
    ///
    /// Returns an empty vector when they agree. This never refuses to draw - the numbers exist and
    /// somebody will go looking for them - it only gives the chart something honest to say above
    /// itself.
    ///
    /// # Arguments
    ///
    /// * `captures` - The captures about to share an axis
    pub fn incomparable(&self, captures: &[u32]) -> Vec<String> {
        let mut differs = Vec::new();
        // one capture is always comparable with itself
        let Some((first, rest)) = captures.split_first() else {
            return differs;
        };
        let Some(head) = self.captures.get(*first as usize) else {
            return differs;
        };
        // the digest is one hash over the whole environment, so it decides; the named fields below
        // are only walked to explain a mismatch, which is the same shape `env_verdict` uses
        for other in rest {
            let Some(capture) = self.captures.get(*other as usize) else {
                continue;
            };
            if capture.env_digest == head.env_digest {
                continue;
            }
            if capture.host != head.host && !differs.iter().any(|seen| seen == "host") {
                differs.push("host".to_string());
            }
            if capture.cpu_model != head.cpu_model && !differs.iter().any(|seen| seen == "cpu") {
                differs.push("cpu".to_string());
            }
            if capture.governor != head.governor
                && !differs.iter().any(|seen| seen == "governor")
            {
                differs.push("governor".to_string());
            }
            if capture.rustc != head.rustc && !differs.iter().any(|seen| seen == "rustc") {
                differs.push("rustc".to_string());
            }
        }
        // a digest that differs with no field to point at is still a real difference
        if differs.is_empty()
            && rest.iter().any(|other| {
                self.captures
                    .get(*other as usize)
                    .is_some_and(|capture| capture.env_digest != head.env_digest)
            })
        {
            differs.push("environment".to_string());
        }
        differs
    }

    /// The units one measurement's axes are in
    ///
    /// # Arguments
    ///
    /// * `point` - The measurement to describe
    /// * `metric` - What is on the value axis
    /// * `axis` - What is on the key axis
    pub fn units(&self, point: &MacroPoint, metric: &Metric, axis: Axis) -> Option<AxisUnits> {
        // a measurement whose scale row is missing cannot be placed on any axis, which is a
        // corrupt index rather than a state to draw
        let scale = self.scales.get(point.scale as usize)?;
        // a measurement carrying no value for this metric has no units on this chart. an
        // encryption arm counted no queries, so it has nowhere to sit on a queries a second axis -
        // which is not the same as sitting there at zero
        self.value(point, metric)?;
        Some(AxisUnits {
            value: metric.unit(),
            key: KeyUnit::of(axis, scale),
            scale: scale.scale.clone(),
            // only a percentile's meaning turns on how its samples were stamped; a rate is a count
            // over a wall clock and is the same quotient either way
            timing: matches!(metric, Metric::Latency { .. }).then_some(point.timing),
        })
    }

    /// The units a workload would be drawn in, or nothing when nothing has ever measured it
    ///
    /// Taken from that workload's **most recent** measurement, so the answer does not change as
    /// captures are ticked and untucked. A workload whose recorded facts moved between captures is
    /// therefore judged by the newer ones, which is the pair a reader is most likely looking at.
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    /// * `metric` - What is on the value axis
    /// * `axis` - What is on the key axis
    pub fn workload_units(
        &self,
        workload: u32,
        metric: &Metric,
        axis: Axis,
    ) -> Option<AxisUnits> {
        self.newest_point(workload)
            .and_then(|point| self.units(point, metric, axis))
    }

    /// Whether any capture in this corpus has measured one workload at all
    ///
    /// Distinct from [`Index::workload_answers`], and the picker needs both: a workload nothing has
    /// ever measured and one whose measurements carry no value for the current metric are hidden
    /// for different reasons, and only one of the two is fixed by changing the metric.
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    pub fn measured(&self, workload: u32) -> bool {
        self.newest_point(workload).is_some()
    }

    /// Whether one workload's most recent measurement carries a value for a metric
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    /// * `metric` - The number it would have to carry
    pub fn workload_answers(&self, workload: u32, metric: &Metric) -> bool {
        self.newest_point(workload)
            .and_then(|point| self.value(point, metric))
            .is_some()
    }

    /// Which captures carry a value for a metric, in [`Index::captures`] order
    ///
    /// The capture list's counterpart to [`Index::workload_answers`], asking a different question
    /// of the same primitive: not *can this workload ever answer this*, but *would ticking this
    /// capture put anything on the chart*. A capture that answers nothing is greyed rather than
    /// hidden - a capture is an identity a reader knows by name out of a list of twenty-seven, and
    /// that is not the tree [`Index::workload_answers`]' hiding was argued about.
    ///
    /// With nothing ticked the answer is taken over everything the capture measured, which is the
    /// rule [`Index::metrics_for`] already follows: nothing has been committed to yet, so nothing
    /// narrows it.
    ///
    /// # Arguments
    ///
    /// * `metric` - The number a capture would have to carry
    /// * `workloads` - What is currently ticked, as indices into [`Index::workloads`]
    pub fn captures_answering(&self, metric: &Metric, workloads: &[u32]) -> Vec<bool> {
        // one flag per capture, false until a measurement of it says otherwise
        let mut answering = vec![false; self.captures.len()];
        // one pass over the measurements rather than a binary search per (capture, workload) pair.
        // the picker asks this on every frame, and there are fifteen hundred of them
        for point in &self.macro_points {
            let Some(flag) = answering.get_mut(point.capture as usize) else {
                continue;
            };
            // an earlier measurement of this capture has already answered for it
            if *flag {
                continue;
            }
            // a selection narrows the question to the arms it holds; with nothing ticked every
            // measurement in the capture counts, because nothing has been committed to yet
            if !workloads.is_empty() && !workloads.contains(&point.workload) {
                continue;
            }
            *flag = self.value(point, metric).is_some();
        }
        answering
    }

    /// Every metric one workload's most recent measurement can answer
    ///
    /// Read from the **newest** measurement, which is the same rule [`Index::workload_units`]
    /// follows and for the same reason: an answer taken per capture would flicker as captures were
    /// ticked, and the newest is the one a reader is most likely looking at.
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    pub fn workload_metrics(&self, workload: u32) -> Vec<Metric> {
        // a workload nothing has measured answers nothing, which is not the same as answering
        // everything with a gap
        let Some(point) = self.newest_point(workload) else {
            return Vec::new();
        };
        // the five whole-workload numbers, each offered only where the measurement carries it.
        // three of them are universal in the committed corpus and two are not, so this is a real
        // filter rather than a formality
        let mut metrics: Vec<Metric> = [
            Metric::OpsPerSec,
            Metric::RowsPerSec,
            Metric::BytesPerSec,
            Metric::WallClock,
            Metric::Spread,
        ]
        .into_iter()
        .filter(|metric| self.value(point, metric).is_some())
        .collect();
        // then every rank of every operation this workload recorded, in the map's own order, which
        // is sorted because it is a `BTreeMap`
        for op in point.ops.keys() {
            for percentile in Percentile::ALL {
                metrics.push(Metric::Latency {
                    op: op.clone(),
                    percentile,
                });
            }
        }
        metrics
    }

    /// Every metric anything in this corpus can answer, in the order a list should offer them
    ///
    /// The operation names are read out of the measurements rather than from a fixed list, because
    /// which operations exist is a property of the workloads and has changed once already - a
    /// capture lifted from the first artifact version names them `insert` and `get` where a current
    /// one names them `write` and `read`.
    pub fn corpus_metrics(&self) -> Vec<Metric> {
        // the rates first, which is what most questions are about
        let mut metrics = vec![
            Metric::OpsPerSec,
            Metric::RowsPerSec,
            Metric::BytesPerSec,
            Metric::WallClock,
            Metric::Spread,
        ];
        // every operation the corpus recorded, in a stable order
        let mut ops: Vec<&str> = self
            .macro_points
            .iter()
            .flat_map(|point| point.ops.keys().map(String::as_str))
            .collect();
        ops.sort_unstable();
        ops.dedup();
        for op in ops {
            for percentile in Percentile::ALL {
                metrics.push(Metric::Latency {
                    op: op.to_string(),
                    percentile,
                });
            }
        }
        metrics
    }

    /// Every metric **every** one of these workloads can answer
    ///
    /// An intersection rather than a union, which is the whole point: a metric only some of the
    /// selection carries would draw the rest as curves that are not there, at the one place a
    /// reader has no way to tell an absent measurement from a slow one. With nothing selected the
    /// answer is the whole corpus, because nothing has been committed to yet.
    ///
    /// Kept in [`Index::corpus_metrics`] order, so the list does not reshuffle as boxes are ticked.
    ///
    /// # Arguments
    ///
    /// * `workloads` - What is currently ticked, as indices into [`Index::workloads`]
    pub fn metrics_for(&self, workloads: &[u32]) -> Vec<Metric> {
        // nothing ticked means nothing has set what the chart is about, so everything is still open
        if workloads.is_empty() {
            return self.corpus_metrics();
        }
        // what each ticked workload can answer, worked out once rather than once per candidate
        let answered: Vec<Vec<Metric>> = workloads
            .iter()
            .map(|at| self.workload_metrics(*at))
            .collect();
        self.corpus_metrics()
            .into_iter()
            .filter(|metric| answered.iter().all(|held| held.contains(metric)))
            .collect()
    }

    /// The most recent measurement of one workload
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    fn newest_point(&self, workload: u32) -> Option<&MacroPoint> {
        // the captures newest first, binary searching each, rather than scanning the measurement
        // vector backwards. the picker asks this for every workload on every frame, and a workload
        // that only the oldest capture measured would otherwise walk all fifteen thousand of them
        (0..self.captures.len() as u32)
            .rev()
            .find_map(|capture| self.macro_point(capture, workload))
    }

    /// The selected workloads that share the units the first ticked one set
    ///
    /// The picker does not offer the ones this drops, so in the application this never removes
    /// anything. It is here because [`Source::series`] is the contract: a source that built points
    /// out of mismatched units would be inventing a comparison at the one place a reader has no
    /// way to check it.
    ///
    /// # Arguments
    ///
    /// * `selection` - What the reader has asked to see
    fn in_one_unit(&self, selection: &Selection) -> Vec<u32> {
        let mut anchor: Option<AxisUnits> = None;
        let mut kept = Vec::new();
        for workload in &selection.workloads {
            let Some(units) = self.workload_units(*workload, &selection.metric, selection.axis)
            else {
                continue;
            };
            // a measurement with no position on this axis is not on this chart at all
            if units.key.is_none() {
                continue;
            }
            match &anchor {
                // the first ticked workload sets the units, so which group survives is the
                // reader's choice rather than whichever happened to be the largest
                None => {
                    anchor = Some(units);
                    kept.push(*workload);
                }
                Some(head) => {
                    if units.differs_from(head).is_none() {
                        kept.push(*workload);
                    }
                }
            }
        }
        kept
    }

    /// Which curve one measurement sits on, for a chart swept along one fact
    ///
    /// Every fact a caller sets goes in, and the one the axis is reading comes back out. **`rows`
    /// and `keys` are deliberately absent**: a workload sizes them from the row width against a
    /// byte budget, clamped at both ends, so they follow the axis rather than naming a curve.
    /// Putting them in would give a width sweep one curve per point and no curve at all.
    ///
    /// # Arguments
    ///
    /// * `point` - The measurement to place
    /// * `sweep` - Which fact the key axis is reading
    fn curve_key(&self, point: &MacroPoint, sweep: SweepAxis) -> CurveKey {
        let mut key = Vec::new();
        let Some(scale) = self.scales.get(point.scale as usize) else {
            return key;
        };
        // the three facts that can be the axis are each written unless they are
        if sweep != SweepAxis::RowBytes {
            key.push(format!("{} rows", crate::fmt::bytes_axis(scale.row_bytes as f64)));
        }
        if sweep != SweepAxis::ReadShare {
            key.push(match scale.read_pct {
                Some(share) => format!("{share}% reads"),
                None => "not a mixture".to_string(),
            });
        }
        if sweep != SweepAxis::Concurrency {
            key.push(format!("{} outstanding", scale.concurrency));
        }
        // and the facts that never are, each rendered so that it reads on its own in a legend
        key.push(match scale.clients {
            Some(clients) => format!("{clients} clients"),
            None => "one client".to_string(),
        });
        key.push(match &scale.row_profile {
            Some(profile) => format!("{profile} widths"),
            None => "fixed widths".to_string(),
        });
        key.push(match &scale.distribution {
            Some(distribution) => format!("{distribution} keys"),
            None => "uniform keys".to_string(),
        });
        key.push(match &scale.table_kind {
            Some(kind) => kind.clone(),
            None => "an unnamed table".to_string(),
        });
        // the server it ran against. two captures that resolved the same configuration intern to
        // one entry and stay one curve, and two that did not are two curves rather than one line
        // joining measurements of two different servers
        self.conf_key(point, &mut key);
        key
    }

    /// Writes the configuration half of a curve key
    ///
    /// Always the same ten entries, whether or not the measurement recorded a configuration, so
    /// that every key built in one call has the same length and the diff between two of them is a
    /// walk down a pair of equal lists.
    ///
    /// # Arguments
    ///
    /// * `point` - The measurement to place
    /// * `key` - The key being built
    fn conf_key(&self, point: &MacroPoint, key: &mut CurveKey) {
        let conf = point.conf.and_then(|at| self.confs.get(at as usize));
        let Some(conf) = conf else {
            // a capture that recorded no configuration says so ten times rather than once, so that
            // it lines up against one that did
            key.extend(std::iter::repeat_n(
                "an unrecorded configuration".to_string(),
                10,
            ));
            return;
        };
        key.push(format!("{} shards", conf.shards));
        key.push(format!("{} memory", conf.memory));
        key.push(format!("{} durability", conf.durability));
        key.push(match conf.tls {
            true => "TLS".to_string(),
            false => "no TLS".to_string(),
        });
        // the six settings a sweep moves one at a time, absent meaning the server's own default
        let optional: [(&str, Option<String>); 6] = [
            ("latency buffer", conf.latency_buffer_size.map(|at| at.to_string())),
            ("latency write behind", conf.latency_write_behind.map(|at| at.to_string())),
            ("intent log", conf.intent_log_size.clone()),
            ("throughput buffer", conf.throughput_buffer_size.map(|at| at.to_string())),
            ("throughput write behind", conf.throughput_write_behind.map(|at| at.to_string())),
            ("max frame", conf.max_frame_bytes.map(|at| at.to_string())),
        ];
        for (name, value) in optional {
            key.push(match value {
                Some(found) => format!("{found} {name}"),
                None => format!("the default {name}"),
            });
        }
    }

    /// What one workload is called, or a placeholder when the index does not carry it
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    fn workload_id(&self, workload: u32) -> String {
        // an out of range index is a bug rather than a state to render, but a legend that panicked
        // on one would take the whole page with it
        self.workloads
            .get(workload as usize)
            .map_or_else(|| "unknown".to_string(), |found| found.id.clone())
    }

    /// Which table one workload's newest measurement was taken against
    ///
    /// Read from the newest measurement rather than per capture, for the same reason the curve is:
    /// a workload that changed table between two captures would otherwise change colour along its
    /// own line.
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    fn table_of(&self, workload: u32) -> Option<String> {
        self.newest_point(workload)
            .and_then(|point| self.scales.get(point.scale as usize))
            .and_then(|scale| scale.table_kind.clone())
    }

    /// Where one workload's newest measurement sits on a sweep axis
    ///
    /// `None` when it has no position on that axis at all, which is not a position of zero.
    ///
    /// # Arguments
    ///
    /// * `workload` - Which workload, as an index into [`Index::workloads`]
    /// * `sweep` - Which fact the key axis is reading
    fn sweep_position(&self, workload: u32, sweep: SweepAxis) -> Option<f64> {
        self.newest_point(workload)
            .and_then(|point| self.scales.get(point.scale as usize))
            .and_then(|scale| sweep.value(scale))
    }

    /// Turns the curves a selection covers into the lines that will actually be drawn
    ///
    /// A curve is a set of arms that differ only in the fact the axis is reading. Two workloads that
    /// set none of the recorded facts differently answer to one key and are still two workloads, so
    /// a curve that would draw two of its arms at one position on the axis is **split into one line
    /// per workload**, each named by the identifier that is the only thing separating them. Every
    /// other curve is carried through whole, which is what keeps a genuine sweep one line.
    ///
    /// The split happens here rather than in [`Index::curve_key`] deliberately: a key holding the
    /// workload identifier would give a width sweep one curve per point, for the same reason `rows`
    /// and `keys` are kept out of it.
    ///
    /// # Arguments
    ///
    /// * `names` - What separates each curve from the others, in curve order
    /// * `tables` - The table each curve ran against, in curve order
    /// * `placed` - Which curve each workload was placed on
    /// * `sweep` - Which fact the key axis is reading
    fn split_curves(
        &self,
        names: &[Option<String>],
        tables: &[Option<String>],
        placed: &[(u32, usize)],
        sweep: SweepAxis,
    ) -> Vec<DrawnCurve> {
        let mut drawn = Vec::new();
        for (curve, name) in names.iter().enumerate() {
            // this curve's arms in tick order, and where each of them sits on the axis
            let members: Vec<u32> = placed
                .iter()
                .filter(|(_, at)| *at == curve)
                .map(|(workload, _)| *workload)
                .collect();
            let positions: Vec<Option<f64>> = members
                .iter()
                .map(|workload| self.sweep_position(*workload, sweep))
                .collect();
            let table = tables.get(curve).cloned().flatten();
            // nothing lands on top of anything, so this is a curve and is drawn as one line
            if !collides(&positions) {
                drawn.push(DrawnCurve {
                    name: name.clone(),
                    table,
                    members,
                });
                continue;
            }
            // two arms at one position, so the line through them would join two unrelated
            // measurements and the shape a reader took off it would be the shape of the sort
            for workload in members {
                let id = self.workload_id(workload);
                drawn.push(DrawnCurve {
                    // the qualifier the curve already had, plus the only thing that separates this
                    // arm from its neighbour
                    name: Some(match name {
                        Some(what) => format!("{what} · {id}"),
                        None => id,
                    }),
                    table: table.clone(),
                    members: vec![workload],
                });
            }
        }
        drawn
    }

    /// The captures a selection covers, in the order they were taken
    ///
    /// # Arguments
    ///
    /// * `selection` - What the reader has asked to see
    fn ordered_captures(&self, selection: &Selection) -> Vec<u32> {
        // the stored order is already oldest first, so sorting the indices restores chronology
        // whatever order the reader happened to tick the boxes in
        let mut ordered = selection.captures.clone();
        ordered.sort_unstable();
        ordered.dedup();
        ordered
    }
}

impl Source for Index {
    fn captures(&self) -> &[Capture] {
        &self.captures
    }

    fn workloads(&self) -> &[Workload] {
        &self.workloads
    }

    fn families(&self) -> &[FamilyText] {
        &self.families
    }

    fn series(&self, selection: &Selection) -> Vec<Series> {
        let captures = self.ordered_captures(selection);
        // everything the first ticked workload's units admit, and nothing else
        let workloads = self.in_one_unit(selection);
        let mut series = Vec::new();
        match selection.axis {
            // one line per curve per capture, walking each curve along a recorded fact
            Axis::Sweep(sweep) => {
                // the curves the selection covers, in the order their first arm was ticked, so the
                // colours a reader has learned do not shuffle when an unrelated arm is added. a
                // workload is placed by its newest measurement rather than per capture, or one
                // curve could become two on the way between two captures of it
                let mut curves: Vec<CurveKey> = Vec::new();
                let mut tables: Vec<Option<String>> = Vec::new();
                let mut placed: Vec<(u32, usize)> = Vec::new();
                for workload in &workloads {
                    let Some(point) = self.newest_point(*workload) else {
                        continue;
                    };
                    let key = self.curve_key(point, sweep);
                    let at = match curves.iter().position(|seen| *seen == key) {
                        Some(found) => found,
                        None => {
                            curves.push(key);
                            // the table half of the same key, kept beside it rather than read back
                            // out of it, so the hue never depends on parsing a legend entry apart
                            tables.push(
                                self.scales
                                    .get(point.scale as usize)
                                    .and_then(|scale| scale.table_kind.clone()),
                            );
                            curves.len() - 1
                        }
                    };
                    placed.push((*workload, at));
                }
                // what separates the curves being drawn, so a legend says that and nothing else
                let names = curve_names(&curves);
                // and what is actually drawn, after any curve that would put two of its arms at one
                // position on the axis has been split into one line per workload
                let lines = self.split_curves(&names, &tables, &placed, sweep);
                // the hue is the table and the mark is which of that table's curves this is, worked
                // out over the split set rather than over the grouped one
                let drawn: Vec<Option<String>> =
                    lines.iter().map(|line| line.table.clone()).collect();
                let hues = hues(&drawn);
                let marks = marks(&hues);
                for (curve, line) in lines.iter().enumerate() {
                    for capture in &captures {
                        // gather this line's arms within this capture, keyed by where they sit on
                        // the sweep axis, so a set ticked in any order still draws left to right
                        let mut points: Vec<(f64, Option<f64>)> = Vec::new();
                        for workload in &line.members {
                            let Some(point) = self.macro_point(*capture, *workload) else {
                                continue;
                            };
                            let Some(scale) = self.scales.get(point.scale as usize) else {
                                continue;
                            };
                            // an arm with no value on this axis is not on this sweep at all, which
                            // is different from one whose measurement is missing
                            let Some(key) = sweep.value(scale) else {
                                continue;
                            };
                            points.push((key, self.value(point, &selection.metric)));
                        }
                        if points.is_empty() {
                            continue;
                        }
                        // sorted on the key, with ties left alone, so the line is drawn along the
                        // axis
                        points.sort_by(|left, right| {
                            left.0.partial_cmp(&right.0).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        series.push(Series {
                            name: match &line.name {
                                Some(what) => format!("{} · {what}", self.label_of(*capture)),
                                None => self.label_of(*capture),
                            },
                            capture: *capture,
                            hue: hues[curve],
                            mark: marks[curve],
                            points,
                        });
                    }
                }
            }
            // one line per workload, walking the selected captures in the order they were taken
            Axis::Timeline => {
                // the table each drawn workload ran against, gathered as the series are built so it
                // covers the ones that survived rather than the ones that were ticked
                let mut tables: Vec<Option<String>> = Vec::new();
                for workload in &workloads {
                    let mut points: Vec<(f64, Option<f64>)> = Vec::new();
                    for (at, capture) in captures.iter().enumerate() {
                        // the x is the position, not a parsed timestamp: the captures cluster in
                        // bursts, so a real time axis would smear most of them into one column
                        let value = self
                            .macro_point(*capture, *workload)
                            .and_then(|point| self.value(point, &selection.metric));
                        points.push((at as f64, value));
                    }
                    // a series with nothing in it at all is not drawn, but one with a single
                    // measurement is - that is a real result, and the chart marks it as a point
                    if points.iter().all(|(_, value)| value.is_none()) {
                        continue;
                    }
                    tables.push(self.table_of(*workload));
                    // every workload is its own curve here, so the two channels are filled in below,
                    // once the set that is actually drawn is known
                    series.push(Series {
                        name: self.workload_id(*workload),
                        capture: captures.first().copied().unwrap_or_default(),
                        hue: 0,
                        mark: 0,
                        points,
                    });
                }
                // the hue is still the table and the mark is still which of that table's curves this
                // is - there are simply as many curves here as there are workloads
                let hues = hues(&tables);
                let marks = marks(&hues);
                for (line, (hue, mark)) in series.iter_mut().zip(hues.into_iter().zip(marks)) {
                    line.hue = hue;
                    line.mark = mark;
                }
            }
        }
        series
    }

    fn key_labels(&self, selection: &Selection) -> Vec<String> {
        match selection.axis {
            // a sweep axis is a real quantity, and is formatted rather than labelled
            Axis::Sweep(_) => Vec::new(),
            // a timeline's positions mean nothing without the capture they stand for
            Axis::Timeline => self
                .ordered_captures(selection)
                .into_iter()
                .map(|capture| self.label_of(capture))
                .collect(),
        }
    }
}

/// What separates each curve from the others being drawn, or nothing when it stands alone
///
/// Only the facts that actually differ across the set are named. A chart of one curve needs no name
/// beyond its capture's, which is what a single curve chart looked like before curves existed, and
/// a chart of four tables at one width says `sorted` rather than repeating the width four times.
///
/// # Arguments
///
/// * `curves` - The curve keys being drawn, in the order they will be
fn curve_names(curves: &[CurveKey]) -> Vec<Option<String>> {
    // one curve is told apart from nothing, so it carries no qualifier at all
    if curves.len() < 2 {
        return vec![None; curves.len()];
    }
    // every key in one call is built the same way and is therefore the same length, but a corrupt
    // index could still produce a short one, and reading past it would panic the whole page
    let width = curves.iter().map(Vec::len).min().unwrap_or_default();
    let varies: Vec<usize> = (0..width)
        .filter(|at| curves.iter().any(|curve| curve[*at] != curves[0][*at]))
        .collect();
    curves
        .iter()
        .map(|curve| {
            let what: Vec<&str> = varies.iter().map(|at| curve[*at].as_str()).collect();
            match what.is_empty() {
                // two curves whose keys agree at every position they share are the same curve, and
                // there is nothing honest to call the second one
                true => None,
                false => Some(what.join(", ")),
            }
        })
        .collect()
}

impl Index {
    /// The label of one capture, or a placeholder when the index does not have it
    ///
    /// # Arguments
    ///
    /// * `capture` - Which capture, as an index into [`Index::captures`]
    fn label_of(&self, capture: u32) -> String {
        // an out of range index is a bug rather than a state to render, but a chart that panicked
        // on one would take the whole page with it
        self.captures
            .get(capture as usize)
            .map_or_else(|| "unknown".to_string(), |found| found.label.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an index covering every way two arms can fail to belong on one chart
    ///
    /// Two captures, so a timeline has something to walk, and ten workloads chosen so that each
    /// pair below differs in exactly one thing: the table, the share, the scale, the row profile,
    /// or how the samples were stamped. The table kinds are the corpus's own spellings rather than
    /// short ones, because `index::TABLE_ORDER` reserves a colour for each of those four by name and
    /// a fixture using anything else would be testing the unrecorded-kind path throughout. The measurement `(capture 1, workload 1)` is deliberately
    /// missing, and that gap is what several of these tests are really about.
    fn fixture() -> Index {
        // two captures, taken in the same place, so nothing below is about comparability
        let capture = |label: &str| Capture {
            label: label.to_string(),
            captured: format!("2026-01-0{}T00:00:00Z", label.len()),
            head_short: "abc1234".to_string(),
            dirty: false,
            layers: vec![Layer::Macro],
            complete: vec![Layer::Macro],
            partial: false,
            code: vec![(
                Layer::Macro,
                Verdict {
                    label: "fresh".to_string(),
                    current: Some(true),
                    fields: Vec::new(),
                },
            )],
            env: Verdict {
                label: "comparable".to_string(),
                current: Some(true),
                fields: Vec::new(),
            },
            env_digest: "same".to_string(),
            host: "jove".to_string(),
            governor: "performance".to_string(),
            rustc: "rustc 1.0".to_string(),
            cpu_model: "a chip".to_string(),
        };
        // the reference arm every row below is a single edit away from
        let base = ScaleFactsLite {
            scale: "full".to_string(),
            rows: 1_000,
            row_bytes: 1024,
            keys: 10,
            concurrency: 32,
            clients: None,
            read_pct: Some(50),
            row_profile: None,
            distribution: None,
            table_kind: Some("persistent_unsorted".to_string()),
        };
        // one arm at a share against a table, which is what the grid chart's axis is made of
        let cell = |read_pct: u32, table: &str| ScaleFactsLite {
            read_pct: Some(read_pct),
            table_kind: Some(table.to_string()),
            ..base.clone()
        };
        // and one at a width, where the row count follows from the width rather than being set
        let width = |row_bytes: u64, rows: u64| ScaleFactsLite {
            row_bytes,
            rows,
            ..base.clone()
        };
        let scales = vec![
            cell(25, "persistent_unsorted"),                                  // 0
            cell(75, "persistent_unsorted"),                                  // 1
            cell(25, "persistent_sorted"),                                    // 2
            cell(75, "persistent_sorted"),                                    // 3
            ScaleFactsLite { scale: "smoke".to_string(), ..base.clone() },     // 4
            ScaleFactsLite { row_profile: Some("mixed".to_string()), ..base.clone() }, // 5
            ScaleFactsLite { read_pct: None, ..base.clone() },                 // 6
            width(64, 20_000),                                                 // 7
            width(4096, 64),                                                   // 8
            base.clone(),                                                      // 9
            base.clone(),                                                      // 10
            base.clone(),                                                      // 11
        ];
        let workloads = vec![
            "macro/grid/unsorted/r25/1024",
            "macro/grid/unsorted/r75/1024",
            "macro/grid/sorted/r25/1024",
            "macro/grid/sorted/r75/1024",
            "macro/grid/unsorted/r50/1024/smoke",
            "macro/grid/unsorted/r50/mixed",
            "macro/write/insert_row",
            "macro/grid/unsorted/r50/64",
            "macro/grid/unsorted/r50/4096",
            "macro/write/insert_row/batched",
            // the two that make the metric axis a real filter: one that counted no queries, the
            // way every `macro/encryption` arm in the real corpus does, and one that recorded an
            // operation under a different name
            "macro/encryption/clients/plain/1024/1",
            "macro/grid/unsorted/r50/1024/write-only",
        ]
        .into_iter()
        .map(|id| Workload { id: id.to_string(), family: None })
        .collect::<Vec<_>>();
        // one latency summary, so the percentile metrics have something to read
        let stats = |p99: u64| OpStats {
            count: 100,
            min_ns: 1,
            p50_ns: p99 / 2,
            p90_ns: p99,
            p95_ns: p99,
            p99_ns: p99,
            avg_ns: p99 / 2,
            max_ns: p99,
        };
        let point = |capture: u32, workload: u32, rate: f64, timing: Timing| MacroPoint {
            capture,
            workload,
            // the scale row and the workload are one to one in this fixture
            scale: workload,
            conf: None,
            conf_digest: String::new(),
            timing,
            // workload 10 counted no queries, so it has no rate and no byte rate. that is not a
            // rate of zero, and it is what half the real corpus looks like
            ops_per_sec: (workload != 10).then_some(rate),
            rows_per_sec: None,
            bytes_per_sec: None,
            wall_clock_ns: Some(1_000),
            wall_clock_interval_ns: None,
            spread_pct: None,
            runs: Some(5),
            // which operation was recorded is a property of the workload, so the fixture holds
            // three of the four shapes the corpus does: a read, a write, and neither
            ops: match workload {
                10 => BTreeMap::new(),
                11 => BTreeMap::from([("write".to_string(), stats(rate as u64))]),
                _ => BTreeMap::from([("read".to_string(), stats(rate as u64))]),
            },
        };
        // how each workload's samples were stamped: the last one per batch, everything else per
        // query, so a pair differing in nothing else exists
        let timing = |workload: u32| match workload {
            6 | 9 => Timing::PerBatch,
            _ => Timing::PerQuery,
        };
        let mut macro_points = Vec::new();
        for capture in 0..2u32 {
            for workload in 0..scales.len() as u32 {
                // the second capture never measured workload 1, which is the gap
                if capture == 1 && workload == 1 {
                    continue;
                }
                // workload 0 is the only one that moved between the two captures
                let rate = match (capture, workload) {
                    (1, 0) => 150.0,
                    (_, at) => 100.0 + f64::from(at) * 100.0,
                };
                macro_points.push(point(capture, workload, rate, timing(workload)));
            }
        }
        Index {
            version: INDEX_VERSION,
            head_short: "abc1234".to_string(),
            dirty: false,
            captures: vec![capture("one"), capture("two2")],
            families: Vec::new(),
            workloads,
            scales,
            confs: Vec::new(),
            macro_points,
        }
    }

    /// A selection of one capture's workloads on one axis, for the tests that only vary those
    ///
    /// # Arguments
    ///
    /// * `workloads` - Which workloads to draw
    /// * `metric` - What to put on the value axis
    /// * `axis` - What to put on the key axis
    fn pick(workloads: &[u32], metric: Metric, axis: Axis) -> Selection {
        Selection {
            captures: vec![0],
            workloads: workloads.to_vec(),
            metric,
            axis,
        }
    }

    /// How many points a set of series actually drew
    ///
    /// # Arguments
    ///
    /// * `series` - The series that were built
    fn drawn(series: &[Series]) -> usize {
        series.iter().map(|line| line.points.len()).sum()
    }

    /// With nothing ticked, a capture is judged over everything it measured
    #[test]
    fn a_capture_answers_a_metric_something_in_it_carries() {
        let index = fixture();
        // both captures measured arms that counted queries, so both answer a rate
        assert_eq!(
            index.captures_answering(&Metric::OpsPerSec, &[]),
            vec![true, true]
        );
        // and only one workload in the fixture recorded a `write`, but it is in both captures
        let write = Metric::Latency {
            op: "write".to_string(),
            percentile: Percentile::P99,
        };
        assert_eq!(index.captures_answering(&write, &[]), vec![true, true]);
        // a metric nothing in the corpus carries is answered by no capture, rather than by the
        // ones that happen to hold the most measurements
        assert_eq!(
            index.captures_answering(&Metric::Spread, &[]),
            vec![false, false]
        );
    }

    /// A selection narrows the question to the arms it holds
    #[test]
    fn a_capture_is_judged_against_what_is_ticked() {
        let index = fixture();
        // workload 1 is the gap: the second capture never measured it, so ticking it alone leaves
        // that capture with nothing to draw. this is the whole reason the list greys
        assert_eq!(
            index.captures_answering(&Metric::OpsPerSec, &[1]),
            vec![true, false]
        );
        // workload 10 counted no queries in either capture, which is what half the real corpus
        // looks like on this metric
        assert_eq!(
            index.captures_answering(&Metric::OpsPerSec, &[10]),
            vec![false, false]
        );
        // and one arm that answers is enough, even ticked beside one that does not
        assert_eq!(
            index.captures_answering(&Metric::OpsPerSec, &[10, 0]),
            vec![true, true]
        );
    }

    /// The metric decides it, not the selection alone
    #[test]
    fn a_capture_that_measured_the_workload_can_still_answer_nothing() {
        let index = fixture();
        // every capture measured workload 0, and it recorded a `read` and no `write`. a capture
        // holding the measurement is not a capture holding the number
        let read = Metric::Latency {
            op: "read".to_string(),
            percentile: Percentile::P99,
        };
        let write = Metric::Latency {
            op: "write".to_string(),
            percentile: Percentile::P99,
        };
        assert_eq!(index.captures_answering(&read, &[0]), vec![true, true]);
        assert_eq!(index.captures_answering(&write, &[0]), vec![false, false]);
    }

    #[test]
    fn a_missing_measurement_is_none_and_never_zero() {
        let index = fixture();
        // the pair that exists
        assert!(index.macro_point(1, 0).is_some());
        // and the one that does not, which must not be answered with a default
        assert!(index.macro_point(1, 1).is_none());
    }

    #[test]
    fn a_timeline_carries_the_gap_through_to_the_points() {
        let index = fixture();
        let selection = Selection {
            captures: vec![0, 1],
            workloads: vec![0, 1],
            metric: Metric::OpsPerSec,
            axis: Axis::Timeline,
        };
        let series = index.series(&selection);
        assert_eq!(series.len(), 2);
        // the workload both captures measured has two values
        let first = series.iter().find(|line| line.name.ends_with("r25/1024")).unwrap();
        assert_eq!(first.points, vec![(0.0, Some(100.0)), (1.0, Some(150.0))]);
        // the one only the first capture measured keeps its position and reports no value there,
        // which is what draws a gap rather than a drop to the origin
        let second = series.iter().find(|line| line.name.ends_with("r75/1024")).unwrap();
        assert_eq!(second.points, vec![(0.0, Some(200.0)), (1.0, None)]);
    }

    #[test]
    fn a_sweep_is_ordered_by_its_key_not_by_selection_order() {
        let index = fixture();
        // the workloads are ticked in the reverse of their read share, which is what a reader
        // clicking around produces
        let selection = pick(&[1, 0], Metric::OpsPerSec, Axis::Sweep(SweepAxis::ReadShare));
        let series = index.series(&selection);
        assert_eq!(series.len(), 1);
        // and the line is still drawn left to right along the axis
        assert_eq!(
            series[0].points,
            vec![(25.0, Some(100.0)), (75.0, Some(200.0))]
        );
    }

    #[test]
    fn a_newer_index_is_refused_and_an_older_one_is_read() {
        let mut index = fixture();
        // an older index is readable, because every section added since is defaulted
        index.version = INDEX_VERSION.saturating_sub(1).max(1);
        assert!(index.check_version().is_ok());
        // a newer one is not, because a stale bundle silently dropping a section it has never been
        // told about is worse than a message telling somebody to rebuild
        index.version = INDEX_VERSION + 1;
        assert!(index.check_version().is_err());
    }

    #[test]
    fn captures_taken_in_one_place_are_comparable() {
        let mut index = fixture();
        // the fixture's two captures share a digest
        assert!(index.incomparable(&[0, 1]).is_empty());
        // and one that does not is reported, naming what differs rather than only that it does
        index.captures[1].env_digest = "different".to_string();
        index.captures[1].governor = "powersave".to_string();
        assert_eq!(index.incomparable(&[0, 1]), vec!["governor".to_string()]);
    }

    #[test]
    fn the_newest_capture_with_a_layer_is_the_last_one_that_has_it() {
        let mut index = fixture();
        assert_eq!(index.newest_with(Layer::Macro), Some(1));
        // the most recent capture carrying only a micro layer must not be chosen for a macro view,
        // which is the state the real corpus is in
        index.captures[1].layers = vec![Layer::Micro];
        assert_eq!(index.newest_with(Layer::Macro), Some(0));
        assert_eq!(index.newest_with(Layer::Stages), None);
    }

    #[test]
    fn a_sweep_draws_one_curve_per_table_not_one_line_per_capture() {
        let index = fixture();
        // the shape of the book's grid chart: two shares against two tables, in one capture
        let selection = pick(
            &[0, 1, 2, 3],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        );
        let series = index.series(&selection);
        // one curve per table. joining them is a line that walks 25% unsorted, 25% sorted, 75%
        // unsorted, 75% sorted and means nothing at any of the four
        assert_eq!(
            series.len(),
            2,
            "expected one curve per table, got {:?}",
            series.iter().map(|line| line.name.as_str()).collect::<Vec<_>>()
        );
        for line in &series {
            assert_eq!(line.points.len(), 2, "{} has the wrong arms", line.name);
        }
    }

    #[test]
    fn a_smoke_arm_may_not_share_an_axis_with_a_full_one() {
        let index = fixture();
        // two full scale arms and one smoke arm, which is a hundredth of the data
        let selection = pick(
            &[0, 1, 4],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        );
        let series = index.series(&selection);
        // the smoke arm is not drawn at all: a rate over a hundredth of the rows is not a smaller
        // version of the same measurement
        assert_eq!(drawn(&series), 2, "the smoke arm reached the axis");
    }

    #[test]
    fn a_per_batch_latency_may_not_share_an_axis_with_a_per_query_one() {
        let index = fixture();
        // one arm stamped per query and one stamped per batch, at the same metric
        let selection = pick(
            &[0, 9],
            Metric::Latency { op: "read".to_string(), percentile: Percentile::P99 },
            Axis::Sweep(SweepAxis::ReadShare),
        );
        let series = index.series(&selection);
        // a per batch p99 charges every query for the ones queued ahead of it and a per query p99
        // is a service time, so only the arm that was ticked first is drawn
        assert_eq!(drawn(&series), 1, "the per batch arm reached the axis");
    }

    #[test]
    fn timing_does_not_split_a_throughput_chart() {
        let index = fixture();
        // the same two arms as above, at a rate rather than a percentile
        let selection = pick(
            &[0, 9],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        );
        let series = index.series(&selection);
        // a rate is a count over a wall clock and is the same quotient however the samples were
        // stamped, so refusing this pair would be refusing a comparison that reads perfectly well
        assert_eq!(drawn(&series), 2, "a stamping mode split a rate");
    }

    #[test]
    fn a_mean_width_is_not_on_the_axis_a_fixed_width_is() {
        let index = fixture();
        // two measured widths and one declared distribution of them, on the width axis
        let selection = pick(
            &[7, 8, 5],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::RowBytes),
        );
        let series = index.series(&selection);
        // the distribution reports a mean, and plotting a mean beside a measurement invites the
        // two to be read alike - which is the reason the book's row width page leaves it off
        assert_eq!(drawn(&series), 2, "a mean width reached the axis");
    }

    #[test]
    fn an_arm_with_no_read_share_is_not_offered_on_a_read_share_sweep() {
        let index = fixture();
        // the arm that is not a mixture is ticked first, which is the case that decides whether it
        // can capture the anchor and refuse everything else
        let selection = pick(
            &[6, 0, 1],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        );
        let series = index.series(&selection);
        // it has no share to sit at, which is not a share of zero, so it is not on this chart and
        // does not get to decide what is
        assert_eq!(drawn(&series), 2, "an arm with no share reached the axis");
        assert!(index.workload_units(6, &Metric::OpsPerSec, Axis::Sweep(SweepAxis::ReadShare))
            .is_some_and(|units| units.key.is_none()));
    }

    #[test]
    fn a_metric_a_measurement_does_not_carry_has_no_units() {
        let index = fixture();
        // workload 10 counted no queries, so it has nowhere to sit on a queries a second axis.
        // before this rule it sat there, ticked cleanly, and drew a curve that was not there
        assert!(
            index
                .workload_units(10, &Metric::OpsPerSec, Axis::Sweep(SweepAxis::ReadShare))
                .is_none(),
            "an arm that counted no queries was offered a queries a second axis"
        );
        // and the metrics it does carry are unaffected, because this refuses a missing value
        // rather than refusing the workload
        assert!(
            index
                .workload_units(10, &Metric::WallClock, Axis::Sweep(SweepAxis::ReadShare))
                .is_some(),
            "an arm was refused a metric it carries"
        );
    }

    #[test]
    fn an_arm_that_never_recorded_an_operation_is_not_on_that_operations_axis() {
        let index = fixture();
        let p99 = |op: &str| Metric::Latency {
            op: op.to_string(),
            percentile: Percentile::P99,
        };
        // workload 11 recorded a write and no read, so it is on one of the two axes and not the
        // other. the corpus names its operations differently in different eras, which is exactly
        // how a reader ends up on an axis nothing they ticked was measured on
        assert!(
            index
                .workload_units(11, &p99("read"), Axis::Sweep(SweepAxis::ReadShare))
                .is_none(),
            "an arm that recorded no read was offered a read latency axis"
        );
        assert!(
            index
                .workload_units(11, &p99("write"), Axis::Sweep(SweepAxis::ReadShare))
                .is_some(),
            "an arm was refused the operation it did record"
        );
    }

    #[test]
    fn the_offered_metrics_are_the_ones_every_ticked_workload_answers() {
        let index = fixture();
        // a read arm and a write arm together can answer neither operation's percentiles, because
        // an intersection is what stops half a selection being drawn as curves that are not there
        let both = index.metrics_for(&[0, 11]);
        for op in ["read", "write"] {
            assert!(
                !both.contains(&Metric::Latency {
                    op: op.to_string(),
                    percentile: Percentile::P99,
                }),
                "{op} survived an intersection with an arm that never recorded it"
            );
        }
        // what they share is still there, so the intersection narrows rather than empties
        assert!(both.contains(&Metric::OpsPerSec), "a metric both carry was dropped");
        // and one of them alone keeps its own operation
        assert!(
            index.metrics_for(&[11]).contains(&Metric::Latency {
                op: "write".to_string(),
                percentile: Percentile::P99,
            }),
            "an arm ticked alone lost the operation it recorded"
        );
        // the arm that counted no queries takes the two rates out with it
        let with_uncounted = index.metrics_for(&[0, 10]);
        assert!(
            !with_uncounted.contains(&Metric::OpsPerSec),
            "a rate only one of the two carries survived the intersection"
        );
        assert!(
            with_uncounted.contains(&Metric::WallClock),
            "the intersection dropped the metric everything carries"
        );
    }

    #[test]
    fn nothing_ticked_offers_the_whole_corpus() {
        let index = fixture();
        // before anything is ticked nothing has been committed to, so every metric is still open.
        // an intersection over an empty selection is the whole corpus, not the empty set
        let offered = index.metrics_for(&[]);
        assert_eq!(offered, index.corpus_metrics());
        for op in ["read", "write"] {
            assert!(
                offered.contains(&Metric::Latency {
                    op: op.to_string(),
                    percentile: Percentile::P99,
                }),
                "{op} is in the corpus and was not offered with nothing ticked"
            );
        }
    }

    #[test]
    fn the_offered_metrics_keep_the_corpus_order() {
        let index = fixture();
        // the list a reader sees has to be a subsequence of the full one, or the entries move
        // under the cursor as boxes are ticked
        let whole = index.corpus_metrics();
        let narrowed = index.metrics_for(&[0]);
        assert!(narrowed.len() < whole.len(), "the fixture no longer narrows anything");
        let mut walk = whole.iter();
        for metric in &narrowed {
            assert!(
                walk.any(|found| found == metric),
                "{} is offered out of the order the corpus lists it in",
                metric.axis_label()
            );
        }
    }

    #[test]
    fn row_count_does_not_split_a_width_sweep() {
        let index = fixture();
        // three widths of one mixture against one table, whose row counts differ by three hundred
        // times because each seeds the same budget of bytes
        let selection = pick(
            &[7, 9, 8],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::RowBytes),
        );
        let series = index.series(&selection);
        // one curve of three points. row count follows the width rather than naming a curve, and a
        // key that held it would give this chart three curves of one point and no line at all
        assert_eq!(series.len(), 1, "the row count split the sweep");
        assert_eq!(
            series[0].points,
            vec![(64.0, Some(800.0)), (1024.0, Some(1000.0)), (4096.0, Some(900.0))]
        );
    }

    #[test]
    fn the_first_ticked_workload_decides_the_units() {
        let index = fixture();
        // the smoke arm ticked first takes the chart, and the two full scale arms are refused
        let smoke = index.series(&pick(
            &[4, 0, 1],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        assert_eq!(drawn(&smoke), 1);
        // and ticked last it is the one refused, from the same three workloads
        let full = index.series(&pick(
            &[0, 1, 4],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        assert_eq!(drawn(&full), 2);
    }

    #[test]
    fn a_curve_is_named_by_what_separates_it_and_nothing_else() {
        let index = fixture();
        // two shares against two tables, which differ in the table and in nothing else
        let series = index.series(&pick(
            &[0, 1, 2, 3],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        let mut names: Vec<&str> = series.iter().map(|line| line.name.as_str()).collect();
        names.sort_unstable();
        // the width, the load depth and the key distribution are held by all four and so are not
        // repeated in any of them
        assert_eq!(
            names,
            vec!["one · persistent_sorted", "one · persistent_unsorted"]
        );
        // and one curve on its own is still called after the capture that took it
        let alone = index.series(&pick(
            &[0, 1],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        assert_eq!(alone[0].name, "one");
    }

    #[test]
    fn two_captures_of_one_curve_share_a_hue_and_a_mark_and_differ_in_the_capture() {
        let index = fixture();
        // one curve, drawn from both captures
        let selection = Selection {
            captures: vec![0, 1],
            workloads: vec![0, 1],
            metric: Metric::OpsPerSec,
            axis: Axis::Sweep(SweepAxis::ReadShare),
        };
        let series = index.series(&selection);
        assert_eq!(series.len(), 2);
        // the hue is the table and the marker is the curve within it, so the two are the same colour
        // and the same shape in two dash patterns - the comparison the explorer exists to make
        assert_eq!(series[0].hue, series[1].hue);
        assert_eq!(series[0].mark, series[1].mark);
        assert_ne!(series[0].capture, series[1].capture);
    }

    #[test]
    fn one_table_is_one_hue_and_its_curves_differ_in_the_mark() {
        let index = fixture();
        // two tables at two shares each, which the fixture writes as `unsorted` and `sorted`
        let series = index.series(&pick(
            &[0, 1, 2, 3],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        assert_eq!(series.len(), 2);
        // one curve per table, so two hues and no marker on either of them
        assert_ne!(series[0].hue, series[1].hue);
        assert_eq!((series[0].mark, series[1].mark), (0, 0));
    }

    #[test]
    fn a_table_keeps_its_hue_when_another_table_is_added() {
        let index = fixture();
        // the sorted table on its own
        let alone = index.series(&pick(
            &[2, 3],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        // and the same table with the unsorted one ticked ahead of it, which is what sets the units
        let beside = index.series(&pick(
            &[0, 1, 2, 3],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        let sorted = beside
            .iter()
            .find(|line| line.name.contains("sorted") && !line.name.contains("unsorted"))
            .expect("the sorted curve was not drawn");
        // a hue is a property of the table, not of what happens to be ticked beside it, or the
        // colours a reader has learned shuffle every time an unrelated arm is added
        assert_eq!(alone[0].hue, sorted.hue);
    }

    #[test]
    fn a_named_table_holds_its_slot_whether_or_not_it_is_drawn() {
        // the four the book reads in a fixed order keep that order's slot, so a chart of the
        // ephemeral pair alone does not renumber them onto the persistent pair's colours
        let named: Vec<Option<String>> = TABLE_ORDER
            .iter()
            .map(|kind| Some((*kind).to_string()))
            .collect();
        assert_eq!(hues(&named), vec![0, 1, 2, 3]);
        assert_eq!(hues(&named[2..]), vec![2, 3]);
        // a kind this build has never been told about is still a kind: every curve on it shares one
        // slot, and it sits after the four reserved ones
        let unknown = vec![
            Some("something_new".to_string()),
            Some("persistent_sorted".to_string()),
            Some("something_new".to_string()),
        ];
        assert_eq!(hues(&unknown), vec![4, 1, 4]);
        // and a curve that names no table shares with nothing, because nothing is known to be in
        // common between it and another
        assert_eq!(hues(&[None, None]), vec![4, 5]);
    }

    #[test]
    fn a_marker_counts_the_curves_of_one_colour() {
        // two colours, the first holding three curves and the second one. the count is in draw
        // order, so a chart of one line per table is every mark zero and carries no markers at all
        assert_eq!(marks(&[0, 1, 0, 0]), vec![0, 0, 1, 2]);
    }

    #[test]
    fn two_workloads_at_one_key_position_are_two_lines() {
        let index = fixture();
        // two workloads whose recorded facts agree in every entry the curve key holds. they are
        // still two workloads, and they sit at the same read share, so one line through both of
        // them would pass through two unrelated measurements at one position on the axis
        let series = index.series(&pick(
            &[9, 11],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        assert_eq!(series.len(), 2, "the two workloads were folded into one line");
        // and each of them draws its own measurement at that position rather than sharing one
        for line in &series {
            assert_eq!(line.points.len(), 1);
            assert_eq!(line.points[0].0, 50.0);
        }
    }

    #[test]
    fn a_split_line_is_named_by_the_workload_that_separates_it() {
        let index = fixture();
        // nothing the curve key holds tells these two apart, so the only honest name for either is
        // the identifier the corpus joins on
        let series = index.series(&pick(
            &[9, 11],
            Metric::OpsPerSec,
            Axis::Sweep(SweepAxis::ReadShare),
        ));
        let mut names: Vec<&str> = series.iter().map(|line| line.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "one · macro/grid/unsorted/r50/1024/write-only",
                "one · macro/write/insert_row/batched",
            ]
        );
    }
}
