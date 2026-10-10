//! What a run records, written as JSON so the runs on every host can be read back by `report`

use serde::{Deserialize, Serialize};

/// Everything one run of the harness measured, on one host from one build
#[derive(Serialize, Deserialize, Default)]
pub struct RunRecord {
    /// Where and how it ran
    pub labels: Labels,
    /// What each candidate did beyond its speed
    pub facts: Vec<SumFacts>,
    /// The correctness and stability checks
    pub check: Option<CheckRecord>,
    /// Every speed cell
    pub speed: Vec<SpeedCell>,
    /// What one combine costs, for the candidates that combine
    pub combine: Vec<CombineCell>,
}

/// Where and how a run ran
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Labels {
    /// The host's name
    pub host: String,
    /// The cpu's model name
    pub cpu: String,
    /// The governor of the core the run was pinned to
    pub governor: String,
    /// The `-C target-cpu` the binary was built for
    pub target_cpu: String,
    /// Any `-C target-feature` list the binary was built with
    pub target_features: String,
    /// The harness's own cargo features: the gxhash variants that change what is compiled
    pub cargo_features: Vec<String>,
    /// The instruction set extensions the compiler was allowed to assume
    pub compiled: Vec<String>,
    /// The instruction set extensions detected at run time
    pub detected: Vec<String>,
    /// The compiler
    pub rustc: String,
    /// When the run started, in UTC
    pub date: String,
    /// The core the run was pinned to
    pub core: usize,
    /// How many times each speed cell was measured
    pub runs: usize,
    /// The least time each measurement ran for, in milliseconds
    pub budget_ms: u64,
    /// The cold arena's size in MiB
    pub arena_mib: usize,
    /// Whether this was the quick pass that only proves every adapter runs
    pub quick: bool,
}

impl Labels {
    /// The build: the target cpu, any target features and any cargo feature of the harness
    pub fn build(&self) -> String {
        // the target cpu always
        let mut build = self.target_cpu.clone();
        // then the target features, when any were named
        if !self.target_features.is_empty() {
            build.push(' ');
            build.push_str(&self.target_features);
        }
        // then the harness's features, which change what gxhash compiles to
        for feature in &self.cargo_features {
            build.push_str(" +");
            build.push_str(feature);
        }
        build
    }

    /// A short name for the run: host and build
    pub fn short(&self) -> String {
        format!("{} {}", self.host, self.build())
    }
}

/// What one candidate did beyond its speed
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SumFacts {
    /// The candidate
    pub sum: String,
    /// The digest's width in bits
    pub bits: u32,
    /// The kernel it chose, where the crate says
    pub kernel: Option<String>,
    /// The process's threads after every candidate before it had run
    pub threads_before: u64,
    /// The process's threads after it ran
    pub threads_after: u64,
    /// Allocations made by one call of each operation, at 64 KiB
    pub allocations: Vec<AllocFact>,
}

/// The allocations one call made
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct AllocFact {
    /// The operation
    pub op: String,
    /// How many allocations
    pub allocations: u64,
    /// How many bytes
    pub bytes: u64,
}

/// The correctness and stability checks
#[derive(Serialize, Deserialize, Default)]
pub struct CheckRecord {
    /// Published check values, each from the definition's own source
    pub vectors: Vec<VectorResult>,
    /// Digests of seeded inputs, compared across runs by `report`
    pub digests: Vec<DigestResult>,
    /// The same bytes checksummed from every start offset in a cache line
    pub alignment: Vec<AlignResult>,
    /// One call against the crate's incremental interface fed in pieces
    pub streams: Vec<StreamResult>,
    /// A checksum of a whole made from the checksums of its parts
    pub combines: Vec<CombineResult>,
    /// A finished checksum continued over more bytes without the bytes it covers
    pub extends: Vec<ExtendResult>,
}

/// One published check value
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct VectorResult {
    /// The candidate
    pub sum: String,
    /// The input, described
    pub input: String,
    /// What the definition says the output is
    pub expected: String,
    /// What the candidate gave
    pub got: String,
    /// Where the expected value comes from
    pub source: String,
}

impl VectorResult {
    /// Whether the candidate gave the published value
    pub fn ok(&self) -> bool {
        self.expected == self.got
    }
}

/// A digest of one set of seeded inputs, for one candidate
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct DigestResult {
    /// The candidate
    pub sum: String,
    /// The set of inputs
    pub set: String,
    /// The candidate's output, or an FNV-1a over its outputs for a set of many inputs
    pub digest: String,
}

/// The same bytes checksummed from every start offset in a cache line
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct AlignResult {
    /// The candidate
    pub sum: String,
    /// Offsets tried
    pub offsets: u64,
    /// Offsets whose output was the output at offset zero
    pub equal: u64,
}

/// One call against the crate's incremental interface, for one way of cutting the bytes
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct StreamResult {
    /// The candidate
    pub sum: String,
    /// The incremental interface, or why there is none
    pub api: String,
    /// How the bytes were cut
    pub splits: String,
    /// Inputs tried
    pub tried: u64,
    /// Inputs whose streamed output was the one-shot output
    pub equal_one_shot: u64,
    /// Inputs whose streamed output was the same interface's output fed the whole in one piece
    pub equal_whole: u64,
}

/// A checksum of a whole made from the checksums of its parts
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CombineResult {
    /// The candidate
    pub sum: String,
    /// How the whole was cut
    pub how: String,
    /// Wholes tried
    pub tried: u64,
    /// Wholes whose combined checksum was the one-shot checksum of the whole
    pub equal: u64,
}

/// A finished checksum continued over more bytes
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ExtendResult {
    /// The candidate
    pub sum: String,
    /// The interface it was continued through
    pub how: String,
    /// Inputs tried
    pub tried: u64,
    /// Inputs whose continued checksum was the one-shot checksum of the bytes and their suffix
    pub equal: u64,
}

/// One speed cell: a candidate, a unit, an operation, cold or hot, measured `runs` times
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SpeedCell {
    /// The candidate
    pub sum: String,
    /// The unit, in bytes
    pub unit: usize,
    /// The operation: `one-shot`, or `stream-4k` for the unit fed in pieces of 4 KiB
    pub op: String,
    /// Whether one unit was used for every call, so the data stayed in cache
    pub hot: bool,
    /// GiB a second, once a measurement
    pub gib_per_sec: Vec<f64>,
    /// Microseconds a call, once a measurement
    pub us_per_call: Vec<f64>,
    /// Calls made, once a measurement
    pub calls: Vec<u64>,
}

/// What one combine costs
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CombineCell {
    /// The candidate
    pub sum: String,
    /// The length of the second part, which is what a combine's cost depends on
    pub len_b: u64,
    /// Nanoseconds a combine, once a measurement
    pub ns_per_combine: Vec<f64>,
}

/// The median of some measurements
///
/// # Arguments
///
/// * `values` - The measurements
pub fn median(values: &[f64]) -> Option<f64> {
    // nothing measured has no median
    if values.is_empty() {
        return None;
    }
    // sort a copy and take the middle, or the mean of the middle two
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    }
}
