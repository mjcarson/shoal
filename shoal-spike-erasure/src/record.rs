//! What a run records, written as JSON so the runs on every host can be read back by `report`

use serde::{Deserialize, Serialize};

/// Everything one run of the harness measured, on one host from one build
#[derive(Serialize, Deserialize, Default)]
pub struct RunRecord {
    /// Where and how it ran
    pub labels: Labels,
    /// What each candidate did beyond its speed
    pub facts: Vec<CodeFacts>,
    /// The correctness checks
    pub check: Option<CheckRecord>,
    /// Every speed cell
    pub speed: Vec<SpeedCell>,
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
    /// The `-march` reed-solomon-erasure's C kernels were compiled with
    pub rse_arch: String,
    /// The compiler
    pub rustc: String,
    /// The instruction set extensions detected at run time that a candidate dispatches on
    pub features: Vec<String>,
    /// When the run started, in UTC
    pub date: String,
    /// The core the run was pinned to
    pub core: usize,
    /// How many times each speed cell was measured
    pub runs: usize,
    /// The least time each measurement ran for, in milliseconds
    pub budget_ms: u64,
    /// The data arena's size in MiB
    pub arena_mib: usize,
    /// Whether this was the quick pass that only proves every adapter runs
    pub quick: bool,
    /// The subcommand that ran: `all`, or `kernels` for the forced kernel sets
    #[serde(default)]
    pub pass: String,
}

impl Labels {
    /// A short name for the run: host and build, and which pass when it is not the main one
    pub fn short(&self) -> String {
        if self.is_kernels() {
            format!("{} {}, kernel sets", self.host, self.target_cpu)
        } else {
            format!("{} {}", self.host, self.target_cpu)
        }
    }

    /// Whether this run is the kernels pass
    pub fn is_kernels(&self) -> bool {
        self.pass == "kernels"
    }
}

/// What one candidate did beyond its speed
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CodeFacts {
    /// The candidate
    pub code: String,
    /// The kernel set it chose, where it says
    pub kernels: Option<String>,
    /// The process's threads after every candidate before it had run
    pub threads_before: u64,
    /// The process's threads after it ran
    pub threads_after: u64,
    /// Allocations made by one call of each operation, at 4+2 and 64 KiB (4+1 for xor)
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
    /// Why it was not counted, when it was not
    pub note: Option<String>,
}

/// The correctness checks
#[derive(Serialize, Deserialize, Default)]
pub struct CheckRecord {
    /// Every loss pattern at every small layout
    pub patterns: Vec<PatternResult>,
    /// The chance a random set of k chunks fails to decode, for codes with random coefficients
    pub random: Vec<RandomResult>,
    /// Runs of partial updates checked against a fresh encode
    pub updates: Vec<UpdateResult>,
    /// Digests of fixed inputs, compared across runs by `report`
    pub digests: Vec<DigestResult>,
    /// Whether two candidates' parity is the same bytes
    pub compat: Vec<CompatResult>,
}

/// Every loss pattern of one layout, for one candidate
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct PatternResult {
    /// The candidate
    pub code: String,
    /// The layout
    pub layout: String,
    /// Loss patterns tried: every set of 1 to m chunks
    pub patterns: u64,
    /// Patterns decoded to the right bytes
    pub decoded: u64,
    /// Patterns the code said it could not decode
    pub undecodable: u64,
    /// Patterns decoded to the wrong bytes
    pub wrong: u64,
    /// Patterns with exactly m chunks lost that it could not decode
    pub undecodable_at_m: u64,
    /// Patterns with exactly m chunks lost
    pub patterns_at_m: u64,
    /// Single chunks rebuilt
    pub rebuilds: u64,
    /// Rebuilds that gave the wrong bytes, or a recoded chunk the stripe could not decode with
    pub rebuilt_wrong: u64,
    /// Recoded chunks with which the stripe was undecodable
    pub rebuilt_undecodable: u64,
    /// The first errors returned, if any
    pub errors: Vec<String>,
}

/// The chance a random set of k chunks fails to decode
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct RandomResult {
    /// The candidate
    pub code: String,
    /// The layout
    pub layout: String,
    /// Trials, each fresh coefficients and a random m chunks lost
    pub trials: u64,
    /// Trials that could not decode
    pub failures: u64,
    /// Trials that decoded to the wrong bytes
    pub wrong: u64,
}

/// A run of partial updates checked against a fresh encode
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct UpdateResult {
    /// The candidate
    pub code: String,
    /// The layout
    pub layout: String,
    /// Updates applied
    pub updates: u64,
    /// Whether the parity then equalled a fresh encode; none when there is no update
    pub equal: Option<bool>,
    /// Why there is no update
    pub note: Option<String>,
}

/// A digest of one candidate's stored chunks for fixed inputs
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct DigestResult {
    /// The candidate
    pub code: String,
    /// The layout
    pub layout: String,
    /// FNV-1a of every stored chunk, in hex
    pub digest: String,
    /// Whether a second encode in the same run gave the same digest
    pub repeatable: bool,
}

/// Whether two candidates' parity is the same bytes
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CompatResult {
    /// One side
    pub left: String,
    /// The other
    pub right: String,
    /// The layout
    pub layout: String,
    /// Whether every parity byte agreed
    pub equal: bool,
}

/// One speed cell: a candidate, a layout, a unit and an operation, measured `runs` times
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SpeedCell {
    /// The candidate
    pub code: String,
    /// The layout
    pub layout: String,
    /// The unit in bytes
    pub unit: usize,
    /// The operation: `encode`, `decode-j` with j chunks lost, `rebuild` or `update`
    pub op: String,
    /// Whether the same row was used for every call, so the data stays in cache; otherwise rows
    /// are taken in turn from arenas too large for any cache
    #[serde(default)]
    pub hot: bool,
    /// GiB a second of each measurement, of the bytes the operation is counted by
    pub gib_per_sec: Vec<f64>,
    /// Microseconds a call of each measurement
    pub us_per_call: Vec<f64>,
    /// Calls made in each measurement
    pub calls: Vec<u64>,
    /// Calls the code said it could not decode
    pub failures: u64,
    /// Why it was not measured, when it was not
    pub note: Option<String>,
}

impl SpeedCell {
    /// The median of the measurements, if there were any
    pub fn median(&self) -> Option<f64> {
        median(&self.gib_per_sec)
    }

    /// The median microseconds a call, if there were any
    pub fn median_us(&self) -> Option<f64> {
        median(&self.us_per_call)
    }

    /// The spread of the measurements as a fraction of the median
    pub fn spread(&self) -> Option<f64> {
        let mid = self.median()?;
        let lo = self
            .gib_per_sec
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let hi = self.gib_per_sec.iter().copied().fold(0.0, f64::max);
        Some((hi - lo) / mid)
    }
}

/// The median of some values
///
/// # Arguments
///
/// * `values` - The values
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // sort a copy and take the middle, or the mean of the middle two
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    })
}
