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

/// One member's memory, as its own figures say ([F71](../../docs/src/features/bench-device-memory.md))
///
/// Every figure is what the member last reported to the control leader, so it is as old as
/// that report, a few seconds at most.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberMemory {
    /// The process's resident memory, everything included
    pub resident_bytes: u64,
    /// Bytes of rows the shards hold in memory, which their eviction budgets bound
    pub memory_bytes: u64,
    /// Bytes the shards' archive maps' indexes hold
    pub archive_map_bytes: u64,
    /// Bytes the shards' tables' partition indexes hold
    pub table_index_bytes: u64,
    /// Bytes the shards' WAL indexes of their retained entries hold
    pub wal_index_bytes: u64,
    /// Bytes the shards' eviction lists hold
    pub lru_bytes: u64,
}

impl MemberMemory {
    /// The bytes every index holds together: the archive maps', the tables' and the WAL's
    #[must_use]
    pub fn index_bytes(&self) -> u64 {
        self.archive_map_bytes + self.table_index_bytes + self.wal_index_bytes
    }
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
    /// Each live member's memory, by the name the stats view gives it; empty in a capture from
    /// before [F71](../../docs/src/features/bench-device-memory.md)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub memory: BTreeMap<String, MemberMemory>,
    /// The hops the members took for the driver's queries a second, by kind - `forwarded`,
    /// `proposals_hopped` and `barriers_hopped` - summed over the members; empty in a capture
    /// from before [F74](../../docs/src/features/client-routing.md)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hops_per_sec: BTreeMap<String, f64>,
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

/// What one block device did while a run ran, from the kernel's own counters
/// ([F71](../../docs/src/features/bench-device-memory.md))
///
/// The difference between two reads of `/proc/diskstats`, one just before the arm's clock
/// started and one once its last answer was in, so it counts the warmup and the drain as the
/// driver's series does. It is the device's, not the cluster's: whatever else wrote to the
/// device in that time is in it too.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeviceCounters {
    /// The device's kernel name, as `/proc/diskstats` spells it: `nvme0n1p2`, `dm-0`
    pub device: String,
    /// The storage roots on it, each as `<node> <role> <path>`; a role is `latency`, where the
    /// WAL is, or `throughput`, where the archives are
    pub roots: Vec<String>,
    /// The seconds between the two reads, by the host's own clock
    pub secs: f64,
    /// Reads completed
    pub reads: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Writes completed
    pub writes: u64,
    /// Bytes written
    pub written_bytes: u64,
    /// Discards completed; zero on a kernel before 4.18
    pub discards: u64,
    /// Bytes discarded
    pub discarded_bytes: u64,
    /// Flush requests completed; zero on a kernel before 5.5
    pub flushes: u64,
    /// Milliseconds the device had I/O in flight
    pub busy_ms: u64,
}

/// One host's devices while a run ran ([F71](../../docs/src/features/bench-device-memory.md))
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HostDevices {
    /// The host, as the inventory reaches it
    pub host: String,
    /// The nodes it ran, by their names in the inventory
    pub nodes: Vec<String>,
    /// Every device a root of theirs is on, in name order
    pub devices: Vec<DeviceCounters>,
    /// The roots on no device the kernel counts - a tmpfs, an overlay - each as
    /// `<node> <role> <path>`
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<String>,
}

/// What the paced stream did during one run ([F72](../../docs/src/features/bench-paced-stream.md))
///
/// A second driver against one table, at an offered rate rather than at a depth, with its own
/// windows: what a table driven lightly beside the main load saw of it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PacedResult {
    /// The table it drove
    pub table: String,
    /// Its workload, by name
    pub workload: String,
    /// The rate it offered, operations a second
    pub per_sec: f64,
    /// What it did while measured, latency counted from each operation's scheduled send
    pub measured: WindowSummary,
    /// What it did while the arm warmed up
    pub warmup: WindowSummary,
    /// Every second of it
    pub series: Vec<SecondSample>,
    /// What its insert feed did, if it inserted
    pub feed: Option<FeedFacts>,
    /// The read back of its acknowledged inserts
    pub verify: Option<VerifyFacts>,
}

impl PacedResult {
    /// The worst p99 of any measured second it answered in, in milliseconds
    #[must_use]
    pub fn worst_second_p99_ms(&self) -> Option<f64> {
        // every measured second's slowest kind, read or insert
        self.series
            .iter()
            .filter(|second| second.phase == SecondPhase::Measure)
            .flat_map(|second| [&second.summary.read, &second.summary.insert])
            .filter(|kind| kind.ok > 0)
            .map(|kind| kind.latency.p99_ms)
            .fold(None, |worst: Option<f64>, p99| Some(worst.map_or(p99, |worst| worst.max(p99))))
    }
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
    /// The members that answered with no query figures while it ran: a build from before F65,
    /// whose answers are missing from `server_series` (item 199)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unfigured: Vec<String>,
    /// Why the nodes' own figures could not be read while it ran, if they could not: a node
    /// that is not a cluster member keeps none, so `server_series` is empty (item 200)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub figures_unread: Option<String>,
    /// The busiest second of the driver's process, in percent of one cpu
    pub driver_cpu_peak_pct: f64,
    /// How many progress events were dropped because the screen fell behind
    pub progress_dropped: u64,
    /// What each host's devices did while it ran
    /// ([F71](../../docs/src/features/bench-device-memory.md))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<HostDevices>,
    /// Why the hosts' devices could not be read, if they could not
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub devices_unread: Option<String>,
    /// What the paced stream did beside it, if the run had one
    /// ([F72](../../docs/src/features/bench-paced-stream.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paced: Option<PacedResult>,
}

impl RunResult {
    /// The bytes the main stream's bundles took on the wire over the whole run: warmup, measured
    /// time and drain, the same time its device counters cover
    #[must_use]
    pub fn bytes_sent(&self) -> u64 {
        // every second, whatever phase it fell in
        self.series.iter().map(|second| second.summary.bytes_sent).sum()
    }

    /// The bytes every host's devices wrote while it ran
    #[must_use]
    pub fn device_written_bytes(&self) -> u64 {
        // every device of every host
        self.devices
            .iter()
            .flat_map(|host| &host.devices)
            .map(|device| device.written_bytes)
            .sum()
    }

    /// The bytes the devices wrote for each byte the driver sent, when both were counted
    ///
    /// Each replica writes what it is sent and each node writes its WAL and then its archives,
    /// so a cluster at a factor of three that rewrote nothing would read near six.
    #[must_use]
    pub fn device_bytes_per_sent_byte(&self) -> Option<f64> {
        // a run that sent nothing, or read no device, has no ratio
        let sent = self.bytes_sent();
        if sent == 0 || self.devices.is_empty() {
            return None;
        }
        Some(self.device_written_bytes() as f64 / sent as f64)
    }

    /// The largest resident set any member reported while it ran, by member
    #[must_use]
    pub fn peak_resident(&self) -> BTreeMap<String, u64> {
        // the largest of every sample, member by member
        let mut peaks = BTreeMap::new();
        for sample in &self.server_series {
            for (member, memory) in &sample.memory {
                let peak = peaks.entry(member.clone()).or_insert(0);
                *peak = (*peak).max(memory.resident_bytes);
            }
        }
        peaks
    }

    /// The hops the members took a second for this run's queries, every kind together, averaged
    /// over the samples that name them; none in a capture from before
    /// [F74](../../docs/src/features/client-routing.md)
    #[must_use]
    pub fn mean_hops_per_sec(&self) -> Option<f64> {
        // every sample that names its hops, each summed over the kinds
        let sums: Vec<f64> = self
            .server_series
            .iter()
            .filter(|sample| !sample.hops_per_sec.is_empty())
            .map(|sample| sample.hops_per_sec.values().sum())
            .collect();
        // the mean of them, if there were any
        #[allow(clippy::cast_precision_loss)]
        (!sums.is_empty()).then(|| sums.iter().sum::<f64>() / sums.len() as f64)
    }

    /// Each member's memory in the last sample that had any
    #[must_use]
    pub fn last_memory(&self) -> Option<&BTreeMap<String, MemberMemory>> {
        // the newest sample with figures in it
        self.server_series
            .iter()
            .rev()
            .map(|sample| &sample.memory)
            .find(|memory| !memory.is_empty())
    }
}

/// Every run of one arm
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmResult {
    /// What it is compared by
    pub id: ArmId,
    /// Its workload, by name; `mix` before F67
    #[serde(alias = "mix")]
    pub workload: String,
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
            workload: arm.workload.name.clone(),
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

    /// An arm written before F67 names its workload `mix`, and still reads as the same arm
    #[test]
    fn an_arm_that_says_mix_still_reads() {
        let old = r#"{"id": "rw50/b1/none", "mix": "rw50", "bundle": 1, "in_flight": 4,
                      "overrides": null, "event": "none", "runs": []}"#;
        let arm: super::ArmResult = serde_json::from_str(old).unwrap();
        assert_eq!(arm.workload, "rw50");
        // and is written back under the new name
        let written = serde_json::to_string(&arm).unwrap();
        assert!(written.contains("\"workload\":\"rw50\"") && !written.contains("\"mix\""), "{written}");
    }

    /// A run's devices, members' memory and paced stream round trip, and a run from before
    /// them still reads, with none of each (F71, F72)
    #[test]
    fn devices_memory_and_the_paced_stream_round_trip() {
        use super::{
            DeviceCounters, HostDevices, MemberMemory, PacedResult, RunResult, SecondPhase, SecondSample,
        };
        use crate::window::WindowSummary;
        use std::collections::BTreeMap;
        // a run from before F71, as a capture of it holds one
        let old = r#"{"run": 0, "order": 0, "started_at": "", "measured": {"secs": 1.0, "read": {"ok": 0,
            "per_sec": 0.0, "latency": {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0,
            "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0}, "misses": 0, "errors": {}, "samples": {}},
            "insert": {"ok": 0, "per_sec": 0.0, "latency": {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0,
            "p90_ms": 0.0, "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0}, "misses": 0, "errors": {},
            "samples": {}}, "bundle": {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0,
            "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0}, "feed_wait_ms": 0.0},
            "warmup": {"secs": 1.0, "read": {"ok": 0, "per_sec": 0.0, "latency": {"count": 0,
            "mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0},
            "misses": 0, "errors": {}, "samples": {}}, "insert": {"ok": 0, "per_sec": 0.0, "latency":
            {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "p999_ms": 0.0,
            "max_ms": 0.0}, "misses": 0, "errors": {}, "samples": {}}, "bundle": {"count": 0,
            "mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0},
            "feed_wait_ms": 0.0}, "series": [], "ended_early": null, "feeds": {}, "wrapped": false,
            "verify": null, "event": null, "server_series": [{"at_ms": 0, "answers_per_sec": {},
            "p99_ms": null}], "driver_cpu_peak_pct": 0.0, "progress_dropped": 0}"#;
        let mut run: RunResult = serde_json::from_str(old).unwrap();
        assert!(run.devices.is_empty() && run.devices_unread.is_none() && run.paced.is_none());
        assert!(run.server_series[0].memory.is_empty());
        assert_eq!(run.device_bytes_per_sent_byte(), None);
        assert!(run.peak_resident().is_empty() && run.last_memory().is_none());
        // and one with all three written back and read again
        run.devices = vec![HostDevices {
            host: "titan".to_string(),
            nodes: vec!["titan".to_string()],
            devices: vec![DeviceCounters {
                device: "nvme0n1p2".to_string(),
                roots: vec!["titan latency /optane/shoal".to_string()],
                written_bytes: 4096,
                ..DeviceCounters::default()
            }],
            unresolved: vec!["titan throughput /dev/shm/x".to_string()],
        }];
        run.server_series[0].memory = BTreeMap::from([(
            "titan".to_string(),
            MemberMemory {
                resident_bytes: 1 << 30,
                ..MemberMemory::default()
            },
        )]);
        // a paced stream whose slowest measured second answered in 9 ms, past a 30 ms warmup
        let second = |phase, p99_ms| {
            let mut summary = WindowSummary::default();
            summary.read.ok = 1;
            summary.read.latency.p99_ms = p99_ms;
            SecondSample {
                at: 0,
                phase,
                summary,
                driver_cpu_pct: 0.0,
            }
        };
        run.paced = Some(PacedResult {
            table: "Review".to_string(),
            series: vec![
                second(SecondPhase::Warmup, 30.0),
                second(SecondPhase::Measure, 2.0),
                second(SecondPhase::Measure, 9.0),
            ],
            ..PacedResult::default()
        });
        let again: RunResult = serde_json::from_str(&serde_json::to_string(&run).unwrap()).unwrap();
        assert_eq!(again, run);
        assert_eq!(again.paced.as_ref().unwrap().worst_second_p99_ms(), Some(9.0));
        assert_eq!(again.peak_resident()["titan"], 1 << 30);
    }
}
