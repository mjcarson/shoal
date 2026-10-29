//! A `shoaladm bench` capture as the store comparison reads it ([F94](../../../../docs/src/features/bench-object-endpoints.md))
//!
//! Mirror structs of `shoal_loadgen::results::Capture`, holding only what the comparison draws.
//! The runner half of this crate builds with no shoal crate at all, so it never names the loadgen
//! types; every field here is defaulted and nothing is denied, so a capture that has more than
//! this reads, and one from before a field existed reads it as empty.
//! `tests/store_captures.rs` holds this mirror to what loadgen writes.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// The bytes in a mebibyte
const MIB: f64 = 1024.0 * 1024.0;

/// The bytes in a gibibyte
const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// One latency distribution's percentiles, in milliseconds
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Latency {
    /// The median
    pub p50_ms: f64,
    /// The ninetieth percentile
    pub p90_ms: f64,
    /// The ninety-ninth
    pub p99_ms: f64,
    /// The ninety-nine point ninth
    pub p999_ms: f64,
}

/// One kind of operation over a window
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Kind {
    /// How many succeeded
    pub ok: u64,
    /// How many a second
    pub per_sec: f64,
    /// How long they took
    pub latency: Latency,
    /// How many found nothing
    pub misses: u64,
    /// How many failed, by code
    pub errors: BTreeMap<String, u64>,
}

impl Kind {
    /// How many missed or failed
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.misses + self.errors.values().sum::<u64>()
    }
}

/// A window of a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Window {
    /// How long it covered
    pub secs: f64,
    /// Each kind, by name: `put`, `get`, `stat`
    pub kinds: BTreeMap<String, Kind>,
    /// The bytes sent a second
    pub sent_per_sec: f64,
    /// The bytes received a second
    pub received_per_sec: f64,
    /// The bytes sent
    pub bytes_sent: u64,
    /// The bytes received
    pub bytes_received: u64,
}

/// One second of a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Second {
    /// Which second, from the arm's start
    pub at: u64,
    /// `warmup`, `measure` or `drain`
    pub phase: String,
    /// What it did
    pub summary: Window,
}

/// One device's counters over a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Device {
    /// The kernel's name for it
    pub device: String,
    /// Bytes written
    pub written_bytes: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// The seconds between its two reads, which the host counters all cover
    pub secs: f64,
}

/// One interface's counters over a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Interface {
    /// Its name
    pub name: String,
    /// Bytes sent
    pub sent_bytes: u64,
    /// Bytes received
    pub received_bytes: u64,
}

/// A store's processes of one name over a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Process {
    /// The name they run under
    pub name: String,
    /// Their cpu-seconds
    pub cpu_secs: f64,
    /// Their high water marks, summed
    pub peak_resident_bytes: u64,
}

/// A host's cpus over a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Cpu {
    /// Cpu-seconds busy
    pub busy_secs: f64,
    /// Cpu-seconds counted
    pub total_secs: f64,
}

/// One host over a run
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Host {
    /// How it was reached
    pub host: String,
    /// Its devices
    pub devices: Vec<Device>,
    /// Its cpus
    pub cpu: Option<Cpu>,
    /// Its interfaces with a device under them, and the loopback
    pub interfaces: Vec<Interface>,
    /// The store's processes on it
    pub processes: Vec<Process>,
}

/// A run's byte totals, which a slim capture keeps where it drops the series
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(default)]
pub struct Totals {
    /// Bytes sent
    pub sent: u64,
    /// Bytes received
    pub received: u64,
}

/// The read back of what was acknowledged
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Verify {
    /// How many were read
    pub checked: u64,
    /// How many were gone
    pub lost: u64,
    /// How many held other bytes
    pub misread: u64,
}

/// One run of an arm
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Run {
    /// Which run
    pub run: u32,
    /// The measured window
    pub measured: Window,
    /// Every second, where the capture kept them
    pub series: Vec<Second>,
    /// Every host's counters
    pub devices: Vec<Host>,
    /// The byte totals
    pub totals: Option<Totals>,
    /// Whether a put ran past its description and put an object twice
    pub wrapped: bool,
    /// The read back of a sample of what the run put
    pub verify: Option<Verify>,
}

/// Every run of one arm
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Arm {
    /// Its id: `{mix}/s{size}/c{n}/none`
    pub id: String,
    /// Each run
    pub runs: Vec<Run>,
}

/// A machine's facts
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct HostFacts {
    /// Its hostname
    pub hostname: String,
    /// Its cpu model
    pub cpu: String,
    /// Its cpus online
    pub cores: u64,
    /// Its memory
    pub memory_bytes: u64,
    /// Its governor
    pub governor: String,
    /// Its kernel
    pub kernel: String,
}

/// A node's facts
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct NodeFacts {
    /// The machine
    pub host: HostFacts,
    /// The governor it ran under
    pub governor_ran: Option<String>,
    /// The sha256 of its program, for a Shoal node
    pub program_sha256: Option<String>,
}

/// Where a piece of code came from
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Code {
    /// Its commit
    pub commit: Option<String>,
    /// Whether it had uncommitted changes
    pub dirty: bool,
}

/// Another store, as `bench s3` recorded it
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Store {
    /// What it is
    pub system: String,
    /// Its version
    pub version: String,
    /// How it kept the bucket
    pub redundancy: String,
    /// The endpoints, in order
    pub endpoints: Vec<String>,
    /// Anything else recorded
    pub notes: BTreeMap<String, String>,
}

/// How and where a capture was taken
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Provenance {
    /// When it started
    pub started_at: String,
    /// The bench's code
    pub shoal: Code,
    /// The driver's machine
    pub driver: HostFacts,
    /// Each node, or each host of another store
    pub nodes: BTreeMap<String, NodeFacts>,
    /// The store, for a capture of another store
    pub store: Option<Store>,
}

/// The parts of a spec the comparison checks two captures agree on
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Spec {
    /// How long an arm was measured
    pub duration: u64,
    /// How long it warmed up
    pub warmup: u64,
    /// How many runs an arm had
    pub runs: u32,
    /// The seed
    pub seed: u64,
    /// The object mixes
    pub objects: Vec<String>,
    /// The object sizes
    pub object_sizes: Vec<serde_json::Value>,
    /// The concurrencies
    pub concurrency: Vec<usize>,
    /// The arms kept
    pub object_only: Vec<String>,
    /// What was done between arms
    pub between_arms: Option<String>,
    /// Whether the caches were dropped before each run
    pub drop_caches: bool,
    /// The preload
    pub preload: serde_json::Value,
}

/// The dataset
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Dataset {
    /// Its digest
    pub digest: String,
}

/// A whole capture, as the comparison reads it
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Capture {
    /// Its format, which must be 1
    pub format: u32,
    /// Its label
    pub label: String,
    /// How and where it was taken
    pub provenance: Provenance,
    /// What was asked for
    pub spec: Spec,
    /// The dataset
    pub dataset: Dataset,
    /// Every arm
    pub arms: Vec<Arm>,
    /// Whether every arm ran
    pub complete: bool,
    /// The read back of the preload at the end
    pub verify_end: Option<Verify>,
}

impl Capture {
    /// Read a capture's file
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    ///
    /// # Errors
    ///
    /// When it cannot be read, is not json, or is of another format.
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let capture: Capture =
            serde_json::from_slice(&bytes).with_context(|| format!("{} is not a capture", path.display()))?;
        if capture.format != 1 {
            bail!("{} is capture format {}, and this reads format 1", path.display(), capture.format);
        }
        Ok(capture)
    }

    /// The system it measured: the store's name, or `shoal`
    #[must_use]
    pub fn system(&self) -> &str {
        self.provenance.store.as_ref().map_or("shoal", |store| store.system.as_str())
    }

    /// An arm, by its id
    ///
    /// # Arguments
    ///
    /// * `id` - The arm's id
    #[must_use]
    pub fn arm(&self, id: &str) -> Option<&Arm> {
        self.arms.iter().find(|arm| arm.id == id)
    }
}

impl Run {
    /// The bytes sent over the whole run, from its series or its totals
    #[must_use]
    pub fn bytes_sent(&self) -> u64 {
        match (&self.totals, self.series.is_empty()) {
            (Some(totals), true) => totals.sent,
            _ => self.series.iter().map(|second| second.summary.bytes_sent).sum(),
        }
    }

    /// The bytes received over the whole run
    #[must_use]
    pub fn bytes_received(&self) -> u64 {
        match (&self.totals, self.series.is_empty()) {
            (Some(totals), true) => totals.received,
            _ => self.series.iter().map(|second| second.summary.bytes_received).sum(),
        }
    }

    /// Every operation a second, every kind together
    #[must_use]
    pub fn ops_per_sec(&self) -> f64 {
        self.measured.kinds.values().map(|kind| kind.per_sec).sum()
    }

    /// The payload a second, both ways, in MiB
    #[must_use]
    pub fn mib_per_sec(&self) -> f64 {
        (self.measured.sent_per_sec + self.measured.received_per_sec) / MIB
    }

    /// A kind's figures in the measured window, if it ran
    ///
    /// # Arguments
    ///
    /// * `kind` - `put`, `get` or `stat`
    #[must_use]
    pub fn kind(&self, kind: &str) -> Option<&Kind> {
        self.measured.kinds.get(kind).filter(|kind| kind.ok > 0)
    }

    /// Whether anything failed or missed while it was measured
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.measured.kinds.values().map(Kind::failed).sum()
    }

    /// The bytes every host's devices wrote for each byte the run sent, if it sent any
    #[must_use]
    pub fn device_bytes_per_sent_byte(&self) -> Option<f64> {
        let sent = self.bytes_sent();
        let written: u64 = self.devices.iter().flat_map(|host| &host.devices).map(|device| device.written_bytes).sum();
        (sent > 0 && !self.devices.is_empty()).then(|| written as f64 / sent as f64)
    }

    /// The bytes every host sent on the wire, loopback left out, for each payload byte moved
    #[must_use]
    pub fn wire_bytes_per_moved_byte(&self) -> Option<f64> {
        let moved = self.bytes_sent() + self.bytes_received();
        let hosts: Vec<&Host> = self.devices.iter().filter(|host| !host.interfaces.is_empty()).collect();
        if moved == 0 || hosts.is_empty() {
            return None;
        }
        let wire: u64 = hosts
            .iter()
            .flat_map(|host| &host.interfaces)
            .filter(|interface| interface.name != "lo")
            .map(|interface| interface.sent_bytes)
            .sum();
        Some(wire as f64 / moved as f64)
    }

    /// The store's cpu-seconds on every host for each GiB moved
    #[must_use]
    pub fn cpu_secs_per_gib(&self) -> Option<f64> {
        let moved = (self.bytes_sent() + self.bytes_received()) as f64 / GIB;
        let processes: Vec<&Process> = self.devices.iter().flat_map(|host| &host.processes).collect();
        (moved > 0.0 && !processes.is_empty()).then(|| processes.iter().map(|process| process.cpu_secs).sum::<f64>() / moved)
    }

    /// The store's cpu-seconds on every host for each thousand operations
    ///
    /// The counters cover the whole run, warmup and drain included, and the operations are the
    /// measured window's, so the measured rate is taken over the time the counters cover.
    #[must_use]
    pub fn cpu_secs_per_kop(&self) -> Option<f64> {
        // the time the counters cover, and the operations at the measured rate over it
        let secs = self.devices.iter().flat_map(|host| &host.devices).map(|device| device.secs).fold(0.0, f64::max);
        let ops = self.ops_per_sec() * secs;
        let processes: Vec<&Process> = self.devices.iter().flat_map(|host| &host.processes).collect();
        (ops > 0.0 && !processes.is_empty()).then(|| processes.iter().map(|process| process.cpu_secs).sum::<f64>() * 1000.0 / ops)
    }

    /// The most any one host's store processes had resident, their high water marks summed
    #[must_use]
    pub fn peak_resident(&self) -> Option<u64> {
        self.devices
            .iter()
            .filter(|host| !host.processes.is_empty())
            .map(|host| host.processes.iter().map(|process| process.peak_resident_bytes).sum::<u64>())
            .max()
    }
}

/// An arm's id, split: its mix, its size in bytes, and its concurrency
///
/// # Arguments
///
/// * `id` - The id, `{mix}/s{size}/c{n}/none`
#[must_use]
pub fn split_id(id: &str) -> Option<(String, u64, usize)> {
    let mut parts = id.split('/');
    let mix = parts.next()?.to_string();
    let size = parse_size(parts.next()?.strip_prefix('s')?)?;
    let concurrency = parts.next()?.strip_prefix('c')?.parse().ok()?;
    Some((mix, size, concurrency))
}

/// A size label's bytes: `4k`, `64k`, `1m`, `16m`
///
/// # Arguments
///
/// * `label` - The label, without its `s`
fn parse_size(label: &str) -> Option<u64> {
    // a unit of a power of two, or plain bytes
    let (number, shift) = match label.chars().last()? {
        'k' => (&label[..label.len() - 1], 10),
        'm' => (&label[..label.len() - 1], 20),
        'g' => (&label[..label.len() - 1], 30),
        _ => (label, 0),
    };
    number.parse::<u64>().ok().map(|number| number << shift)
}

#[cfg(test)]
mod tests {
    use super::split_id;

    /// An arm's id splits into its mix, size and concurrency, in the labels loadgen writes
    #[test]
    fn ids_split() {
        assert_eq!(split_id("put100/s4k/c16/none"), Some(("put100".to_string(), 4096, 16)));
        assert_eq!(split_id("rw50/s16m/c1/none"), Some(("rw50".to_string(), 16 << 20, 1)));
        assert_eq!(split_id("nonsense"), None);
    }
}
