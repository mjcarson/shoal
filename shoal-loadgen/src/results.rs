//! What a benchmark run leaves behind: a capture, with everything needed to judge it later
//!
//! A capture is one `bench.json` in a directory of its own. It holds what was run (the spec and
//! its digest), what it was run on (provenance: the code, the build, every host, the dataset)
//! and what happened (every run of every arm, a window a second). Two captures are only
//! compared when everything that would change their numbers is the same, which is why so much
//! of this is facts rather than results ([`crate::compare`]).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

use crate::events::{Catchup, Mark};
use crate::feed::{FeedFacts, TableScan};
use crate::spec::{ArmId, BenchSpec, EventKind, Mode};
use crate::window::WindowSummary;

/// The version of the capture format; a reader refuses any other
pub const FORMAT: u32 = 1;

/// The name of a capture's file inside its directory
pub const CAPTURE_FILE: &str = "bench.json";

/// Where a piece of code came from
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeFacts {
    /// The commit it was built from, if it is in a git checkout
    pub commit: Option<String>,
    /// Whether the checkout had uncommitted changes
    pub dirty: bool,
    /// Its version, for code from a registry rather than a checkout
    pub version: Option<String>,
}

/// One machine's facts that change what it measures
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostFacts {
    /// Its hostname
    pub hostname: String,
    /// Its cpu model
    pub cpu: String,
    /// How many cpus it has online
    pub cores: u64,
    /// How much memory it has, in bytes
    pub memory_bytes: u64,
    /// Its cpu frequency governor
    pub governor: String,
    /// Its kernel release
    pub kernel: String,
}

/// One node of the benchmarked cluster
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFacts {
    /// The machine
    pub host: HostFacts,
    /// The governor it ran under, if the bench set one
    pub governor_ran: Option<String>,
    /// The cpu its program was built for
    pub target_cpu: Option<String>,
    /// The sha256 of the program it ran
    pub program_sha256: Option<String>,
}

/// The schema benchmarked
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFacts {
    /// The database struct's name
    pub db: String,
    /// Its fingerprint, which the nodes compared at every connection
    pub fingerprint: u64,
}

/// Everything about how and where a capture was taken
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The version of the tool that took it
    pub tool: String,
    /// When it started, as RFC 3339
    pub started_at: String,
    /// When it finished, as RFC 3339
    pub finished_at: Option<String>,
    /// Whether it drove its own cluster or an attached one
    pub mode: Option<Mode>,
    /// What the nodes ran: `release`, `profile`, or `attached` for a program the bench did not build
    pub flavor: String,
    /// The project the schema came from
    pub project: CodeFacts,
    /// The shoal it was built against
    pub shoal: CodeFacts,
    /// `rustc -V`
    pub rustc: Option<String>,
    /// The RUSTFLAGS the programs were built with
    pub rustflags: Option<String>,
    /// The schema
    pub schema: SchemaFacts,
    /// The machine the driver ran on
    pub driver: HostFacts,
    /// Each node, by its name in the inventory
    pub nodes: BTreeMap<String, NodeFacts>,
    /// Whether the driver ran on one of the nodes
    pub driver_shares_host: bool,
    /// The sha256 of the inventory as given
    pub inventory_digest: Option<String>,
    /// The sha256 of the inventory the bench's cluster was deployed from
    pub bench_inventory_digest: Option<String>,
    /// A sha256 of the inventory's shape: what changes the numbers, without names, paths or ports
    pub inventory_shape: Option<String>,
    /// Units the bench stopped for the run and started again after
    pub stopped_units: Vec<String>,
    /// Whether the run was allowed to share hosts with other running clusters
    pub neighbours_allowed: bool,
}

/// The dataset as it was found
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetFacts {
    /// A sha256 over every table's name, format and file digest
    pub digest: String,
    /// What scanning each table's file found
    pub tables: Vec<TableScan>,
}

impl DatasetFacts {
    /// The facts of a set of scanned tables, with their digest
    ///
    /// # Arguments
    ///
    /// * `tables` - What each scan found
    #[must_use]
    pub fn new(tables: Vec<TableScan>) -> Self {
        use sha2::{Digest, Sha256};
        // sorted by table so the folder's listing order does not move it
        let mut parts: Vec<String> = tables
            .iter()
            .map(|table| format!("{}\t{}\t{}\n", table.table, table.format.as_str(), table.sha256))
            .collect();
        parts.sort();
        let digest = crate::read::hex(&Sha256::digest(parts.concat().as_bytes()));
        DatasetFacts { digest, tables }
    }
}

/// Which part of an arm a second fell in
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecondPhase {
    /// Before it was measured
    Warmup,
    /// While it was measured
    Measure,
    /// After its time, while the last answers came in
    Drain,
}

/// One second of an arm
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecondSample {
    /// Which second of the arm, from 0
    pub at: u64,
    /// Which part of the arm it fell in
    pub phase: SecondPhase,
    /// What it did
    pub summary: WindowSummary,
    /// How busy the driver's process was, in percent of one cpu
    pub driver_cpu_pct: f64,
}

/// One second of a node's own figures, as `shoaladm stats` reads them (F65)
///
/// Timed from when a bundle's frame arrived, not from its send, so a node's p99 is never
/// compared with the driver's.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServerSample {
    /// Milliseconds since the arm started
    pub at_ms: u64,
    /// Answers a second, by kind, summed over the members
    pub answers_per_sec: BTreeMap<String, f64>,
    /// The slowest member's p99, in milliseconds
    pub p99_ms: Option<f64>,
}

/// Why an arm ended before its time
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndedEarly {
    /// Why: `inserts exhausted`, `aborted`
    pub reason: String,
    /// When, in seconds since the arm started
    pub at_secs: f64,
}

/// Reading back what an arm's inserts were acknowledged for
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VerifyFacts {
    /// How many distinct acknowledged rows were read back
    pub checked: u64,
    /// How many of them were not found: acknowledged writes the cluster lost
    pub lost: u64,
    /// How many reads failed, by code, and so proved nothing either way
    pub errors: BTreeMap<String, u64>,
}

/// What an event did and what the client saw of it
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventFacts {
    /// The event
    pub kind: EventKind,
    /// The node it acted on, if one
    pub target: Option<String>,
    /// What was done when
    pub marks: Vec<Mark>,
    /// The client's numbers in each window: `before`, `during`, `after`
    pub windows: BTreeMap<String, WindowSummary>,
    /// The first second the client saw a failure
    pub failure_at: Option<usize>,
    /// The first of the clean seconds that ended the outage
    pub recovery_at: Option<usize>,
    /// The during window's p99 over the before's, in thousandths
    pub p99_ratio_permille: Option<u64>,
    /// `finished`, `unfinished` (the arm's time ran out first), or `failed`
    pub outcome: String,
    /// What a returning node's catch up looked like
    pub catchup: Option<Catchup>,
}

/// One run of one arm
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// Which run
    pub run: u32,
    /// Where in the whole capture it ran, from 0
    pub order: usize,
    /// When it started, as RFC 3339
    pub started_at: String,
    /// What it did while measured
    pub measured: WindowSummary,
    /// What it did while warming up
    pub warmup: WindowSummary,
    /// Every second of it
    pub series: Vec<SecondSample>,
    /// Why it ended early, if it did
    pub ended_early: Option<EndedEarly>,
    /// What each table's insert feed did
    pub feeds: BTreeMap<String, FeedFacts>,
    /// Whether any feed started its pool over, so some inserts were overwrites
    pub wrapped: bool,
    /// The read back of its acknowledged inserts
    pub verify: Option<VerifyFacts>,
    /// Its event, if it had one
    pub event: Option<EventFacts>,
    /// The nodes' own figures while it ran
    pub server_series: Vec<ServerSample>,
    /// The busiest second of the driver's process, in percent of one cpu
    pub driver_cpu_peak_pct: f64,
    /// How many progress events were dropped because the screen fell behind
    pub progress_dropped: u64,
}

/// Every run of one arm
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmResult {
    /// What it is compared by
    pub id: ArmId,
    /// Its mix, by name
    pub mix: String,
    /// Its bundle size
    pub bundle: usize,
    /// How many queries a worker kept outstanding
    pub in_flight: usize,
    /// The override it ran under
    pub overrides: Option<String>,
    /// Its event
    pub event: EventKind,
    /// Each run, in run order
    pub runs: Vec<RunResult>,
}

/// One benchmark run, whole
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capture {
    /// The format version
    pub format: u32,
    /// Its label, which is also its directory's name
    pub label: String,
    /// How and where it was taken
    pub provenance: Provenance,
    /// What was asked for
    pub spec: BenchSpec,
    /// The digest of what was asked for
    pub spec_digest: String,
    /// The dataset
    pub dataset: DatasetFacts,
    /// How long the preload took, and what it did, if one ran
    pub preload: Option<WindowSummary>,
    /// Every arm, in the order it first ran
    pub arms: Vec<ArmResult>,
    /// Whether every planned arm ran
    pub complete: bool,
    /// Why the run stopped, if it did not finish
    pub error: Option<String>,
}

impl Capture {
    /// Find an arm, adding it in the order it first ran
    ///
    /// # Arguments
    ///
    /// * `arm` - The arm's plan
    pub fn arm_mut(&mut self, arm: &crate::spec::ArmPlan) -> &mut ArmResult {
        // an arm is added the first time one of its runs finishes
        if let Some(index) = self.arms.iter().position(|result| result.id == arm.id) {
            return &mut self.arms[index];
        }
        self.arms.push(ArmResult {
            id: arm.id.clone(),
            mix: arm.mix.name.clone(),
            bundle: arm.bundle,
            in_flight: arm.in_flight,
            overrides: arm.overrides.as_ref().map(|over| over.name.clone()),
            event: arm.event,
            runs: Vec::new(),
        });
        self.arms.last_mut().expect("an arm was just added")
    }

    /// Write this capture into its directory, atomically
    ///
    /// # Arguments
    ///
    /// * `dir` - The capture's directory
    ///
    /// # Errors
    ///
    /// When the directory or the file cannot be written.
    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        // write beside the file and rename over it, so a reader never sees half of one
        std::fs::create_dir_all(dir)?;
        let partial = dir.join(format!("{CAPTURE_FILE}.partial"));
        let json = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(&partial, json)?;
        std::fs::rename(partial, dir.join(CAPTURE_FILE))
    }

    /// Read a capture from its directory or its file
    ///
    /// # Arguments
    ///
    /// * `path` - The capture's directory, or its `bench.json`
    ///
    /// # Errors
    ///
    /// When it cannot be read, is not a capture, or is of another format.
    pub fn read(path: &Path) -> Result<Self, String> {
        // a directory holds its file
        let file = if path.is_dir() {
            path.join(CAPTURE_FILE)
        } else {
            path.to_path_buf()
        };
        let bytes = std::fs::read(&file)
            .map_err(|error| format!("{} cannot be read: {error}", file.display()))?;
        // the format is checked before anything else is trusted
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{} is not json: {error}", file.display()))?;
        let format = value.get("format").and_then(serde_json::Value::as_u64);
        if format != Some(u64::from(FORMAT)) {
            return Err(format!(
                "{} is capture format {format:?}; this build reads format {FORMAT}",
                file.display()
            ));
        }
        serde_json::from_value(value)
            .map_err(|error| format!("{} is not a capture: {error}", file.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Capture, DatasetFacts, Provenance, FORMAT};
    use crate::dataset::Format;
    use crate::feed::TableScan;
    use crate::spec::BenchSpec;

    /// A scan of a table with a digest
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `sha` - Its file's digest
    fn scan(table: &str, sha: &str) -> TableScan {
        TableScan {
            table: table.to_string(),
            path: format!("/somewhere/{table}.csv").into(),
            format: Format::Csv,
            sorted: false,
            rows: 1,
            parse_errors: 0,
            first_errors: Vec::new(),
            distinct_keys: 1,
            duplicate_rows: 0,
            bytes: 1,
            sha256: sha.to_string(),
            preload_rows: 1,
            read_keys: 1,
            insert_rows: 0,
        }
    }

    /// The dataset digest follows the files' contents, not their order or where they are
    #[test]
    fn the_dataset_digest_follows_contents() {
        let one = DatasetFacts::new(vec![scan("A", "aa"), scan("B", "bb")]);
        let swapped = DatasetFacts::new(vec![scan("B", "bb"), scan("A", "aa")]);
        let changed = DatasetFacts::new(vec![scan("A", "aa"), scan("B", "bc")]);
        assert_eq!(one.digest, swapped.digest);
        assert_ne!(one.digest, changed.digest);
    }

    /// A capture round trips, and another format is refused by its number
    #[test]
    fn a_capture_round_trips_and_checks_its_format() {
        let capture = Capture {
            format: FORMAT,
            label: "test".to_string(),
            provenance: Provenance::default(),
            spec: BenchSpec::default(),
            spec_digest: BenchSpec::default().digest(),
            dataset: DatasetFacts::new(vec![scan("A", "aa")]),
            preload: None,
            arms: Vec::new(),
            complete: true,
            error: None,
        };
        let dir = tempfile::tempdir().unwrap();
        capture.write(dir.path()).unwrap();
        assert_eq!(Capture::read(dir.path()).unwrap(), capture);
        let other = Capture {
            format: FORMAT + 1,
            ..capture
        };
        other.write(dir.path()).unwrap();
        assert!(Capture::read(dir.path()).unwrap_err().contains("format"));
    }
}
