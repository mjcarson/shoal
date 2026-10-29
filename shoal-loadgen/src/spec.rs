//! What a benchmark runs: the spec, its workloads, and the arms it expands to
//!
//! A spec is written in YAML (`bench.yml`) and every field has a default, so a spec can be as
//! short as the dataset it names; the command line overrides any field. It expands into
//! **arms**: one workload of reads and inserts at one bundle size, under one set of inventory overrides and
//! one cluster event, run `runs` times. An arm's [`ArmId`] is the key two captures are compared
//! on, so its spelling is never changed - only added to.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::feed::Preload;
use crate::keys::KeyDistribution;
use crate::objects::{size_label, ObjectArm, ObjectMix};
use shoal::shared::pools::ByteSize;

/// A workload: what share of an arm's operations are reads, inserts and each supplied kind, by weight
///
/// Written as a name - `insert100`, `read100`, `rw50`, `read90` - or as weights by kind,
/// `read:N,insert:M`, with any kind the driver is handed beside them since
/// [F69](../../docs/src/features/driver-operation-kinds.md): `read:50,lookup:50`. A workload of
/// read and insert alone is named and written exactly as it was before F69, so its arms keep
/// their ids and a spec naming it keeps its digest. Whether a supplied kind exists is judged
/// when an arm is planned, where the schema's kinds are known, never here.
/// Called a mix until F67; a spec or capture that says `mixes` or `mix` still reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Workload {
    /// What the workload is called in an arm's id
    pub name: String,
    /// The weight of reads
    pub read: u32,
    /// The weight of inserts
    pub insert: u32,
    /// The weight of each supplied kind, by name
    pub kinds: BTreeMap<String, u32>,
}

impl Workload {
    /// The four workloads a spec runs when it names none
    #[must_use]
    pub fn defaults() -> Vec<Workload> {
        // every one of these parses
        ["read100", "insert100", "rw50", "read90"]
            .into_iter()
            .map(|name| name.parse().expect("the default workloads parse"))
            .collect()
    }

    /// Whether an arm of this workload writes
    ///
    /// A supplied kind is taken to write: only the schema that supplies it knows, so a caller
    /// that does uses [`Workload::writes_with`].
    #[must_use]
    pub fn writes(&self) -> bool {
        self.insert > 0 || self.kinds.values().any(|weight| *weight > 0)
    }

    /// Whether an arm of this workload writes, given which supplied kinds do
    ///
    /// # Arguments
    ///
    /// * `kind_writes` - Whether the supplied kind of this name writes
    #[must_use]
    pub fn writes_with(&self, kind_writes: impl Fn(&str) -> bool) -> bool {
        self.insert > 0
            || self
                .kinds
                .iter()
                .any(|(name, weight)| *weight > 0 && kind_writes(name))
    }

    /// Whether an arm of this workload inserts rows from the insert pool
    #[must_use]
    pub fn inserts(&self) -> bool {
        self.insert > 0
    }

    /// Whether an arm of this workload reads
    #[must_use]
    pub fn reads(&self) -> bool {
        self.read > 0
    }
}

/// Whether a name can be a supplied kind's: lowercase letters and underscores, never one of the
/// driver's own
///
/// # Arguments
///
/// * `name` - The name
#[must_use]
pub fn is_kind_name(name: &str) -> bool {
    // a letter first, then letters and underscores, and not a kind the driver already has
    name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_lowercase() || c == '_')
        && name != "read"
        && name != "insert"
}

impl std::str::FromStr for Workload {
    type Err = String;

    /// Parse a named workload, or weights by kind
    ///
    /// # Arguments
    ///
    /// * `raw` - The workload as written
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // the four named workloads
        let named = |read, insert| Workload {
            name: raw.to_string(),
            read,
            insert,
            kinds: BTreeMap::new(),
        };
        match raw {
            "insert100" => return Ok(named(0, 100)),
            "read100" => return Ok(named(100, 0)),
            "rw50" => return Ok(named(50, 50)),
            "read90" => return Ok(named(90, 10)),
            _ => (),
        }
        // otherwise weights by kind
        let (mut read, mut insert) = (0u32, 0u32);
        let mut kinds: BTreeMap<String, u32> = BTreeMap::new();
        for entry in raw.split(',').filter(|entry| !entry.trim().is_empty()) {
            let (kind, weight) = entry
                .split_once(':')
                .ok_or_else(|| format!("{entry:?} is not kind:weight; a workload is insert100, read100, rw50, read90 or read:N,insert:M with any supplied kind:N beside them"))?;
            let weight: u32 = weight
                .trim()
                .parse()
                .map_err(|error| format!("{entry:?} has a bad weight: {error}"))?;
            match kind.trim() {
                "read" => read += weight,
                "insert" => insert += weight,
                other if is_kind_name(other) => *kinds.entry(other.to_string()).or_default() += weight,
                other => return Err(format!("{other:?} is not read, insert or a kind's name (lowercase letters and underscores)")),
            }
        }
        if read + insert + kinds.values().sum::<u32>() == 0 {
            return Err(format!("the workload {raw:?} has no weight"));
        }
        // a custom workload is named by its weights, so two spellings of one workload are one
        // arm; the supplied kinds follow read and insert in name order
        let mut name = format!("read{read}-insert{insert}");
        for (kind, weight) in &kinds {
            name.push_str(&format!("-{kind}{weight}"));
        }
        Ok(Workload {
            name,
            read,
            insert,
            kinds,
        })
    }
}

impl TryFrom<String> for Workload {
    type Error = String;

    /// Parse a workload read from a spec
    ///
    /// # Arguments
    ///
    /// * `raw` - The workload as written
    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse()
    }
}

impl From<Workload> for String {
    /// Write a workload as the name it is known by
    ///
    /// # Arguments
    ///
    /// * `workload` - The workload
    fn from(workload: Workload) -> Self {
        // a custom workload's name is its weights, which parse back to it
        if workload.name.starts_with("read") && workload.name.contains("-insert") {
            let mut written = format!("read:{},insert:{}", workload.read, workload.insert);
            for (kind, weight) in &workload.kinds {
                written.push_str(&format!(",{kind}:{weight}"));
            }
            return written;
        }
        workload.name
    }
}

/// Something done to the cluster while an arm runs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    /// Nothing: the steady state
    #[default]
    None,
    /// A node is killed with SIGKILL and started again
    Kill,
    /// A node is stopped cleanly and started again
    Stop,
    /// A spare node is added and the cluster rebalanced onto it
    Rebalance,
    /// A node is drained onto a spare and leaves
    Decommission,
    /// A node is killed for good and removed
    Remove,
    /// A table's replicas are verified against each other in the background
    Repair,
    /// A table is backed up in the background
    Backup,
    /// An operator fails one of a node's pool devices, and its placement groups move off it
    /// ([F88](../../docs/src/features/recovery-and-moves.md)); an object arm's event
    #[serde(rename = "device_fail")]
    DeviceFail,
    /// A node is restarted with a pool device added, and placement groups move onto it (F88): the
    /// device is failed and moved off before the arm's preload, and at the event its directory is
    /// emptied, as a new disk's is, and its node started again; an object arm's event
    #[serde(rename = "device_add")]
    DeviceAdd,
    /// An operator asks every pool for a deep scrub, which reads every chunk where it lies in its
    /// slices' idle time ([F89](../../docs/src/features/scrub-and-repair.md)); an object arm's event
    #[serde(rename = "deep_scrub")]
    DeepScrub,
    /// An operator drains one of a node's pool devices, and its placement groups move off it,
    /// copied from it while it serves, and what they leave on it is released
    /// ([F92](../../docs/src/features/reclamation-and-device-lifecycle.md)); an object arm's event
    #[serde(rename = "device_drain")]
    DeviceDrain,
}

impl EventKind {
    /// What the event is called in an arm's id
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        // the serialized spelling
        match self {
            EventKind::None => "none",
            EventKind::Kill => "kill",
            EventKind::Stop => "stop",
            EventKind::Rebalance => "rebalance",
            EventKind::Decommission => "decommission",
            EventKind::Remove => "remove",
            EventKind::Repair => "repair",
            EventKind::Backup => "backup",
            EventKind::DeviceFail => "device_fail",
            EventKind::DeviceAdd => "device_add",
            EventKind::DeepScrub => "deep_scrub",
            EventKind::DeviceDrain => "device_drain",
        }
    }

    /// Whether the event acts on a pool's devices, which only an object arm carries
    #[must_use]
    pub fn on_devices(&self) -> bool {
        matches!(
            self,
            EventKind::DeviceFail | EventKind::DeviceAdd | EventKind::DeepScrub | EventKind::DeviceDrain
        )
    }

    /// Whether an object arm carries the event: nothing, a node down and back, or a device's
    ///
    /// The others act on a table or on the cluster's membership, which no object arm measures.
    #[must_use]
    pub fn fits_objects(&self) -> bool {
        matches!(
            self,
            EventKind::None
                | EventKind::Kill
                | EventKind::Stop
                | EventKind::DeviceFail
                | EventKind::DeviceAdd
                | EventKind::DeepScrub
                | EventKind::DeviceDrain
        )
    }

    /// Whether the event needs a node beyond the bootstrap set to move onto
    #[must_use]
    pub fn needs_spare(&self) -> bool {
        matches!(self, EventKind::Rebalance | EventKind::Decommission)
    }

    /// Whether the event takes a node down, which an attached cluster never allows
    #[must_use]
    pub fn disrupts(&self) -> bool {
        !matches!(self, EventKind::None | EventKind::Repair | EventKind::Backup | EventKind::DeepScrub)
    }
}

impl std::str::FromStr for EventKind {
    type Err = String;

    /// Parse an event by name
    ///
    /// # Arguments
    ///
    /// * `raw` - The event as written
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // the same spellings the spec uses
        serde_yaml::from_str(raw.trim()).map_err(|_| {
            format!("{raw:?} is not none, kill, stop, rebalance, decommission, remove, repair, backup, device_fail, device_add, deep_scrub or device_drain")
        })
    }
}

/// Whether the bench drives a cluster of its own or one already deployed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// A cluster the bench bootstraps beside the inventory's, wipes between arms and destroys
    #[default]
    Owned,
    /// The inventory's own cluster, as it is
    Attach,
}

/// Whether reads find their rows in memory or have to go to storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Reads {
    /// As the preload left them
    #[default]
    Warm,
    /// After every node is restarted, so a read starts from storage
    Cold,
}

/// The level reads are served at
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReadLevel {
    /// Whatever the table or cluster defaults to
    #[default]
    Default,
    /// One replica's applied state
    One,
    /// A quorum barrier first
    Quorum,
}

impl ReadLevel {
    /// The level a read asks the server for, or none to take the server's default
    #[must_use]
    pub fn wire(self) -> Option<shoal::shared::protocol::read::ReadLevel> {
        // the default names nothing, so the table or the cluster decides
        match self {
            ReadLevel::Default => None,
            ReadLevel::One => Some(shoal::shared::protocol::read::ReadLevel::One),
            ReadLevel::Quorum => Some(shoal::shared::protocol::read::ReadLevel::Quorum),
        }
    }
}

/// Where the driver's clients send their queries
/// ([F74](../../docs/src/features/client-routing.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Routing {
    /// Each query to the member that serves it, by the topology the cluster pushes
    Topology,
    /// Every bundle through the member its worker's client was made for, as every run before
    /// F74 sent it
    Endpoints,
}

impl Routing {
    /// What a spec that names no routing measured: written before F74, it sent every bundle
    /// through the endpoints
    #[must_use]
    pub fn recorded_default() -> Self {
        Routing::Endpoints
    }

    /// Whether this is the routing a spec from before F74 measured, which is left out of a
    /// written spec so its digest is what it was
    #[must_use]
    pub fn is_endpoints(&self) -> bool {
        *self == Routing::Endpoints
    }
}

/// What an arm does when its inserts run out before its time does
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnExhaust {
    /// End the arm there, recording when, so the workload is never quietly changed
    #[default]
    End,
    /// Start the insert pool over, recording where, so later inserts are overwrites
    Wrap,
}

/// A named set of inventory settings an arm runs under
///
/// The settings are inventory fields, by their path in the inventory (`resources.memory`,
/// `replication.fragment_min_bytes`), applied to the bench's own cluster before the arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Override {
    /// What the override is called in an arm's id
    pub name: String,
    /// The inventory fields it sets, by path
    pub set: BTreeMap<String, serde_yaml::Value>,
}

/// A second stream beside the main load: one table, driven at an offered rate
/// ([F72](../../docs/src/features/bench-paced-stream.md))
///
/// The main load is a closed loop over every other table and sends as fast as answers come
/// back. This stream sends on a schedule instead, so a stall shows as latency rather than as
/// fewer operations, and its windows are its own: what a small table driven lightly beside a
/// large one saw of it. It is called paced, not a neighbour, because `--allow-neighbours`
/// already means other units on the hosts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Paced {
    /// The table it drives, which the main load then leaves alone
    pub table: String,
    /// What it sends: reads, inserts, or both; never a supplied kind
    #[serde(default = "Paced::default_workload")]
    pub workload: Workload,
    /// The operations a second it offers, over all its streams
    #[serde(default = "Paced::default_per_sec")]
    pub per_sec: f64,
    /// How many streams it sends on, spread over the members as the main load's are
    #[serde(default = "Paced::default_workers")]
    pub workers: usize,
}

impl Paced {
    /// A paced stream of reads of a table at the default rate
    ///
    /// # Arguments
    ///
    /// * `table` - The table it drives
    #[must_use]
    pub fn new(table: impl Into<String>) -> Self {
        Paced {
            table: table.into(),
            workload: Paced::default_workload(),
            per_sec: Paced::default_per_sec(),
            workers: Paced::default_workers(),
        }
    }

    /// Reads alone, unless told otherwise: a light neighbour that changes nothing
    fn default_workload() -> Workload {
        "read100".parse().expect("a named workload parses")
    }

    /// Twenty operations a second, unless told otherwise
    fn default_per_sec() -> f64 {
        20.0
    }

    /// One stream, unless told otherwise
    fn default_workers() -> usize {
        1
    }

    /// How many operations one of its streams keeps outstanding at most: a second's worth
    ///
    /// An operation due while its stream is at the cap waits, and its latency still counts from
    /// when it was due, so a stall is never hidden by the cap.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        // a second of each stream's share, and never less than one
        (self.per_sec / self.workers.max(1) as f64).ceil().max(1.0) as usize
    }

    /// Every problem this paced stream has on its own, and with the main load's table weights
    ///
    /// Whether its table is in the dataset, and not the only one, is judged where the dataset
    /// is known.
    ///
    /// # Arguments
    ///
    /// * `tables` - The main load's weight of each table
    #[must_use]
    pub fn problems(&self, tables: &BTreeMap<String, u32>) -> Vec<String> {
        // each check says what to change
        let mut problems = Vec::new();
        if self.table.is_empty() {
            problems.push("--paced needs a table to drive".to_string());
        }
        if !(self.per_sec.is_finite() && self.per_sec > 0.0) {
            problems.push(format!("--paced-rate {} must be a rate above zero", self.per_sec));
        }
        if self.workers == 0 {
            problems.push("--paced-workers must be at least 1".to_string());
        }
        // a supplied kind is the schema's, not a table's, so it has no table to be paced on
        if !self.workload.kinds.is_empty() {
            problems.push(format!(
                "the paced workload {} names a supplied kind; a paced stream reads and inserts one table",
                self.workload.name
            ));
        }
        if self.workload.read == 0 && self.workload.insert == 0 {
            problems.push(format!("the paced workload {} has nothing to send", self.workload.name));
        }
        // the main load never drives the paced table, so it cannot be weighted for it
        if tables.contains_key(&self.table) {
            problems.push(format!(
                "{} is the paced stream's table, which the main load leaves alone; take it out of --tables",
                self.table
            ));
        }
        problems
    }
}

/// Everything a benchmark run is asked to do
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BenchSpec {
    /// The dataset folder
    pub dataset: PathBuf,
    /// A cluster of the bench's own, or the inventory's
    pub mode: Mode,
    /// How much of each file is loaded before anything is measured
    pub preload: Preload,
    /// A file's own preload, by the table or bucket it holds, over `preload` for the rest
    /// ([F104](../../docs/src/features/memory-in-a-capture.md)): a whole table beside a few of a bucket's objects, which one
    /// value for every file cannot ask. Left out of a spec that names none, so no earlier spec's
    /// digest moves
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub preload_files: BTreeMap<String, Preload>,
    /// Whether the data is already in an attached cluster, so nothing is preloaded
    pub preloaded: bool,
    /// The workloads to run; `mixes` before F67
    #[serde(alias = "mixes")]
    pub workloads: Vec<Workload>,
    /// The bundle sizes to run each workload at
    pub bundles: Vec<usize>,
    /// The sets of inventory settings to run each workload under; none runs the inventory as it is
    pub overrides: Vec<Override>,
    /// The events to run each workload under
    pub events: Vec<EventKind>,
    /// How many streams drive the cluster, spread over its members
    pub workers: usize,
    /// How many queries a worker keeps outstanding; by default four bundles
    pub in_flight: Option<usize>,
    /// How long an arm is measured, in seconds
    pub duration: u64,
    /// How long an arm runs before it is measured, in seconds
    pub warmup: u64,
    /// How many times each arm is run
    pub runs: u32,
    /// The seed every choice is drawn from
    pub seed: u64,
    /// Which read keys are asked for most
    pub distribution: KeyDistribution,
    /// How many keys one read asks for, as one get
    pub read_keys: usize,
    /// The level reads are served at
    pub read_level: ReadLevel,
    /// Whether reads start warm or from storage
    pub reads: Reads,
    /// Whether an insert skips a row whose key an earlier row had
    pub dedupe: bool,
    /// What an arm does when its inserts run out
    pub on_exhaust: OnExhaust,
    /// How many times an arm's query that failed in a retriable way is sent again; none by
    /// default, so a failure is counted where it happened
    pub retries: u32,
    /// The share of a file's rows that may fail to parse, in percent
    pub max_parse_errors: f64,
    /// The weight of each table, by name; by default the size of its pool
    pub tables: BTreeMap<String, u32>,
    /// How far into an event arm's measured time its event starts, in percent
    pub event_at: f64,
    /// How far into a kill or stop arm's measured time the node is started again, in percent
    pub restart_at: f64,
    /// The longest an event arm waits past its time for its event to finish, in seconds
    pub event_timeout: u64,
    /// The node an event acts on: a name, `leader`, or `last`
    pub victim: Option<String>,
    /// The node a rebalance or decommission moves onto
    pub spare: Option<String>,
    /// The table a repair or backup acts on; by default the first in the dataset
    pub event_table: Option<String>,
    /// Whether every acknowledged insert is read back after its arm
    pub verify_acks: bool,
    /// A table driven at an offered rate beside the main load, if any; left out of a spec
    /// without one, so its digest is what it was before
    /// [F72](../../docs/src/features/bench-paced-stream.md)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paced: Option<Paced>,
    /// Where the driver's clients send their queries; topology for a new spec, and endpoints
    /// for one that names none, which is what every spec before
    /// [F74](../../docs/src/features/client-routing.md) measured. Left out when it is endpoints,
    /// so such a spec's digest is what it was
    #[serde(
        default = "Routing::recorded_default",
        skip_serializing_if = "Routing::is_endpoints"
    )]
    pub routing: Routing,
    /// The object mixes to run over every bucket the dataset describes; none for a dataset that
    /// describes none, and left out of such a spec, so its digest is what it was
    /// ([F80](../../docs/src/features/object-wire.md))
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<ObjectMix>,
    /// The object sizes each mix runs at; none runs each description's own sizes
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub object_sizes: Vec<ByteSize>,
    /// How many operations an object arm keeps in flight, each a value an arm runs at; eight
    /// when none is named
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub concurrency: Vec<usize>,
    /// What is done between an arm that wrote and the next: the bench's own cluster reset, or the
    /// arm's objects deleted ([F94](../../docs/src/features/bench-object-endpoints.md)); left out
    /// when it is a reset, so every spec from before F94 digests as it did
    #[serde(skip_serializing_if = "BetweenArms::is_reset")]
    pub between_arms: BetweenArms,
    /// The object arms kept, by the start of their ids, such as `stat100/s4k/`; every arm when
    /// none is named, and left out then, so a spec's digest is what it was (F94)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub object_only: Vec<String>,
    /// Whether every host's page cache is dropped before each run, so a store's reads start from
    /// its devices whatever the run before it read (F94); left out when off
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub drop_caches: bool,
}

/// What is done between an arm that wrote and the next ([F94](../../docs/src/features/bench-object-endpoints.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BetweenArms {
    /// The bench's own cluster is wiped, bootstrapped and preloaded again
    #[default]
    Reset,
    /// What the arm put is deleted and the store left running, which is all a store the bench
    /// did not deploy allows
    Delete,
}

impl BetweenArms {
    /// Whether it is a reset, which a spec leaves out
    #[must_use]
    pub fn is_reset(&self) -> bool {
        *self == BetweenArms::Reset
    }
}

impl std::str::FromStr for BetweenArms {
    type Err = String;

    /// Read it by name: `reset` or `delete`
    ///
    /// # Arguments
    ///
    /// * `text` - The name
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "reset" => Ok(BetweenArms::Reset),
            "delete" => Ok(BetweenArms::Delete),
            other => Err(format!("{other:?} is not reset or delete")),
        }
    }
}

/// How many operations an object arm keeps in flight when the spec names none
pub const DEFAULT_CONCURRENCY: usize = 8;

impl Default for BenchSpec {
    /// The defaults every field falls back to
    fn default() -> Self {
        BenchSpec {
            dataset: PathBuf::new(),
            mode: Mode::Owned,
            preload: Preload::default(),
            preload_files: BTreeMap::new(),
            preloaded: false,
            workloads: Workload::defaults(),
            bundles: vec![1, 16, 64],
            overrides: Vec::new(),
            events: vec![EventKind::None],
            workers: 8,
            in_flight: None,
            duration: 60,
            warmup: 10,
            runs: 3,
            seed: 7,
            distribution: KeyDistribution::Uniform,
            read_keys: 1,
            read_level: ReadLevel::Default,
            reads: Reads::Warm,
            dedupe: false,
            on_exhaust: OnExhaust::End,
            retries: 0,
            max_parse_errors: 1.0,
            tables: BTreeMap::new(),
            event_at: 33.0,
            restart_at: 67.0,
            event_timeout: 300,
            victim: None,
            spare: None,
            event_table: None,
            verify_acks: true,
            paced: None,
            routing: Routing::Topology,
            objects: Vec::new(),
            object_sizes: Vec::new(),
            concurrency: Vec::new(),
            between_arms: BetweenArms::Reset,
            object_only: Vec::new(),
            drop_caches: false,
        }
    }
}

/// The name an arm is compared by, never renamed: `{workload}/b{bundle}[/{override}]/{event}`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArmId(pub String);

impl std::fmt::Display for ArmId {
    /// Write the id as it is spelled
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One arm of a run, in the order it runs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmPlan {
    /// What it is compared by
    pub id: ArmId,
    /// Its workload
    pub workload: Workload,
    /// How many queries a bundle holds
    pub bundle: usize,
    /// How many queries a worker keeps outstanding
    pub in_flight: usize,
    /// The inventory settings it runs under, if any
    pub overrides: Option<Override>,
    /// What is done to the cluster while it runs
    pub event: EventKind,
    /// Which run of the arm this is, from 0
    pub run: u32,
    /// What an object arm does, for an arm of a bucket's objects rather than of tables
    /// ([F80](../../docs/src/features/object-wire.md)); left out of a table arm
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<ObjectArm>,
}

impl ArmPlan {
    /// Whether this arm leaves the cluster other than it found it
    #[must_use]
    pub fn disturbs(&self) -> bool {
        self.workload.writes() || self.event != EventKind::None
    }
}

impl BenchSpec {
    /// How much of a file is loaded before anything is measured: its own preload, or the spec's
    /// ([F104](../../docs/src/features/memory-in-a-capture.md))
    ///
    /// # Arguments
    ///
    /// * `file` - The table or bucket the file holds
    #[must_use]
    pub fn preload_of(&self, file: &str) -> Preload {
        self.preload_files.get(file).copied().unwrap_or(self.preload)
    }

    /// Every problem the spec has on its own, before any dataset or cluster is looked at
    #[must_use]
    pub fn problems(&self) -> Vec<String> {
        // each check says what to change
        let mut problems = Vec::new();
        if self.dataset.as_os_str().is_empty() {
            problems.push("no dataset folder was given (--dataset)".to_string());
        }
        if self.workloads.is_empty() && self.objects.is_empty() {
            problems.push("no workload was given (--workloads, or --objects for a bucket)".to_string());
        }
        if self.concurrency.contains(&0) {
            problems.push("every object concurrency must be at least 1".to_string());
        }
        if self.bundles.is_empty() || self.bundles.contains(&0) {
            problems.push("every bundle size must be at least 1".to_string());
        }
        if self.workers == 0 {
            problems.push("--workers must be at least 1".to_string());
        }
        if let Some(in_flight) = self.in_flight {
            if let Some(bundle) = self.bundles.iter().find(|bundle| **bundle > in_flight) {
                problems.push(format!(
                    "--in-flight {in_flight} is smaller than the bundle size {bundle}, so no bundle would fill"
                ));
            }
        }
        if self.duration == 0 {
            problems.push("--duration must be at least one second".to_string());
        }
        if self.runs == 0 {
            problems.push("--runs must be at least 1".to_string());
        }
        if self.read_keys == 0 {
            problems.push("--read-keys must be at least 1".to_string());
        }
        if self.events.is_empty() {
            problems.push("no event was given; `none` is the steady state".to_string());
        }
        if !(0.0..100.0).contains(&self.event_at) || self.event_at <= 0.0 {
            problems.push("--event-at must be between 0 and 100 percent".to_string());
        }
        if self.restart_at <= self.event_at || self.restart_at >= 100.0 {
            problems.push("--restart-at must come after --event-at and before 100 percent".to_string());
        }
        if !(0.0..=100.0).contains(&self.max_parse_errors) {
            problems.push("--max-parse-errors must be between 0 and 100 percent".to_string());
        }
        // two overrides of one name would be one arm
        let mut names: Vec<&str> = self.overrides.iter().map(|o| o.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            problems.push("two overrides share a name".to_string());
        }
        if self.overrides.iter().any(|o| o.name.is_empty() || o.name.contains('/')) {
            problems.push("an override's name must be non-empty and have no '/'".to_string());
        }
        // an attached cluster is somebody's: nothing that takes a node down, and no cold reads
        if self.mode == Mode::Attach {
            if let Some(event) = self.events.iter().find(|event| event.disrupts()) {
                problems.push(format!(
                    "the event {} takes a node down, which an attached cluster never allows",
                    event.as_str()
                ));
            }
            if self.reads == Reads::Cold {
                problems.push("cold reads restart every node, which an attached cluster never allows".to_string());
            }
            if !self.overrides.is_empty() {
                problems.push("overrides reconfigure the cluster, which an attached cluster never allows".to_string());
            }
        }
        // a device's event is measured by an object arm alone
        if let Some(event) = self.events.iter().find(|event| event.on_devices()) {
            if self.objects.is_empty() {
                problems.push(format!(
                    "the event {} acts on a pool's devices, which only an object arm (--objects) measures",
                    event.as_str()
                ));
            }
        }
        // deleting between arms puts back what an object arm put, and nothing a table arm inserted
        if self.between_arms == BetweenArms::Delete && !self.workloads.is_empty() {
            problems.push(
                "--between-arms delete deletes what object arms put, and leaves a table arm's inserts, so it runs \
                 object arms alone"
                    .to_string(),
            );
        }
        // an event that moves data needs somewhere to move it
        if self.events.iter().any(EventKind::needs_spare) && self.spare.is_none() {
            problems.push("rebalance and decommission need --spare, a node outside the inventory's bootstrap set".to_string());
        }
        // a paced stream offers some rate of reads and inserts on a table the main load leaves
        if let Some(paced) = &self.paced {
            problems.extend(paced.problems(&self.tables));
        }
        problems
    }

    /// Whether an arm of this run leaves the cluster other than it found it, counting what the
    /// paced stream beside it inserted
    ///
    /// # Arguments
    ///
    /// * `arm` - The arm
    #[must_use]
    pub fn disturbs(&self, arm: &ArmPlan) -> bool {
        // the arm's own writes and event, or a paced stream that inserts beside every arm
        arm.disturbs() || self.paced.as_ref().is_some_and(|paced| paced.workload.writes())
    }

    /// Whether any arm of this run writes, the paced stream's operations included
    #[must_use]
    pub fn writes(&self) -> bool {
        // a workload that writes, an object mix that puts, or a paced stream that inserts
        self.workloads.iter().any(Workload::writes)
            || self.objects.iter().any(ObjectMix::writes)
            || self.paced.as_ref().is_some_and(|paced| paced.workload.writes())
    }

    /// How many queries a worker keeps outstanding at a bundle size
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle size
    #[must_use]
    pub fn in_flight_for(&self, bundle: usize) -> usize {
        // four bundles unless told otherwise, and never less than one
        self.in_flight.unwrap_or(bundle * 4).max(bundle)
    }

    /// Every arm of every run, in the order they run
    ///
    /// Steady state arms run before event arms, which disturb the cluster. Within each, a run
    /// starts one arm later than the run before it, so drift over a long run is spread across
    /// the arms rather than landing on the last one every time.
    #[must_use]
    pub fn arms(&self) -> Vec<ArmPlan> {
        // one arm a combination, steady ones first
        let overrides: Vec<Option<&Override>> = if self.overrides.is_empty() {
            vec![None]
        } else {
            self.overrides.iter().map(Some).collect()
        };
        let mut steady = Vec::new();
        let mut eventful = Vec::new();
        for event in self.events.iter().filter(|event| !event.on_devices()) {
            for over in &overrides {
                for workload in &self.workloads {
                    for bundle in &self.bundles {
                        let id = match over {
                            Some(over) => format!("{}/b{bundle}/{}/{}", workload.name, over.name, event.as_str()),
                            None => format!("{}/b{bundle}/{}", workload.name, event.as_str()),
                        };
                        let arm = ArmPlan {
                            id: ArmId(id),
                            workload: workload.clone(),
                            bundle: *bundle,
                            in_flight: self.in_flight_for(*bundle),
                            overrides: over.cloned(),
                            event: *event,
                            run: 0,
                            object: None,
                        };
                        if *event == EventKind::None {
                            steady.push(arm);
                        } else {
                            eventful.push(arm);
                        }
                    }
                }
            }
        }
        // the object arms after the tables': `{mix}/s{size}/c{n}[/{override}]/{event}` (F80), with
        // each event an object arm carries (F88)
        let sizes: Vec<Option<u64>> = if self.object_sizes.is_empty() {
            vec![None]
        } else {
            self.object_sizes.iter().map(|size| Some(size.get())).collect()
        };
        let concurrency = if self.concurrency.is_empty() {
            vec![DEFAULT_CONCURRENCY]
        } else {
            self.concurrency.clone()
        };
        for event in self.events.iter().filter(|event| event.fits_objects()) {
            for over in &overrides {
                for mix in &self.objects {
                    for size in &sizes {
                        for conc in &concurrency {
                            let label = size_label(*size);
                            let id = match over {
                                Some(over) => format!("{}/{label}/c{conc}/{}/{}", mix.name, over.name, event.as_str()),
                                None => format!("{}/{label}/c{conc}/{}", mix.name, event.as_str()),
                            };
                            // only the arms named by the start of their ids, when any are (F94)
                            if !self.object_only.is_empty()
                                && !self.object_only.iter().any(|prefix| id.starts_with(prefix.as_str()))
                            {
                                continue;
                            }
                            let arm = ArmPlan {
                                id: ArmId(id),
                                // a put is a write and a get or a stat a read, as an arm's reset reads it
                                workload: Workload {
                                    name: mix.name.clone(),
                                    read: mix.get + mix.stat,
                                    insert: mix.put,
                                    kinds: BTreeMap::new(),
                                },
                                bundle: 1,
                                in_flight: *conc,
                                overrides: over.cloned(),
                                event: *event,
                                run: 0,
                                object: Some(ObjectArm {
                                    mix: mix.clone(),
                                    size: *size,
                                    concurrency: *conc,
                                }),
                            };
                            if *event == EventKind::None {
                                steady.push(arm);
                            } else {
                                eventful.push(arm);
                            }
                        }
                    }
                }
            }
        }
        // each run, rotated by its index within each group
        let mut arms = Vec::new();
        for run in 0..self.runs {
            for group in [&steady, &eventful] {
                if group.is_empty() {
                    continue;
                }
                let shift = run as usize % group.len();
                for arm in group.iter().cycle().skip(shift).take(group.len()) {
                    arms.push(ArmPlan {
                        run,
                        ..arm.clone()
                    });
                }
            }
        }
        arms
    }

    /// A digest of everything in the spec that changes what is measured
    ///
    /// The dataset's path is left out, since the dataset's own digest says what was loaded, and
    /// so is the mode, which a capture records beside it.
    #[must_use]
    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        // canonical json: struct fields in declaration order, maps sorted
        let mut value = serde_json::to_value(self).expect("a spec is json");
        if let Some(object) = value.as_object_mut() {
            object.remove("dataset");
            object.remove("mode");
        }
        crate::read::hex(&Sha256::digest(value.to_string().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::{BenchSpec, EventKind, Mode, Override, Paced, Routing, Workload};
    use std::collections::BTreeMap;

    /// The named workloads and custom weights parse, and nonsense does not
    #[test]
    fn workloads_parse_by_name_or_weight() {
        let rw50: Workload = "rw50".parse().unwrap();
        assert_eq!((rw50.read, rw50.insert), (50, 50));
        let custom: Workload = "insert:3,read:7".parse().unwrap();
        assert_eq!((custom.name.as_str(), custom.read, custom.insert), ("read7-insert3", 7, 3));
        // a custom workload writes back as weights and reads back as itself
        let written: String = custom.clone().into();
        assert_eq!(written.parse::<Workload>().unwrap(), custom);
        for bad in ["read:x", "read:0", "both", "Update:1", "lookup-x:1"] {
            assert!(bad.parse::<Workload>().is_err(), "{bad}");
        }
        // a supplied kind parses beside them, is named after read and insert, and writes back;
        // whether the schema supplies it is the plan's question, not the parse's (F69)
        let supplied: Workload = "read:50,lookup:50".parse().unwrap();
        assert_eq!(supplied.name, "read50-insert0-lookup50");
        assert_eq!(supplied.kinds["lookup"], 50);
        let written: String = supplied.clone().into();
        assert_eq!(written, "read:50,insert:0,lookup:50");
        assert_eq!(written.parse::<Workload>().unwrap(), supplied);
        assert!(supplied.writes() && !supplied.inserts());
        assert!(!supplied.writes_with(|_| false));
    }

    /// A table workload's arm ids and a spec's digest are what they were before supplied kinds
    ///
    /// An arm id is the key every comparison joins on, and the spec's digest is a fact
    /// `compare` refuses to join across, so a capture taken before [F69](../../docs/src/features/driver-operation-kinds.md)
    /// and one taken after are one benchmark only if both are unchanged for every workload
    /// that names read and insert alone. Frozen on the tree before F69.
    #[test]
    fn table_arm_ids_are_unchanged() {
        let spec = BenchSpec {
            dataset: "data".into(),
            workloads: ["read100", "insert100", "rw50", "read90", "read:7,insert:3"]
                .into_iter()
                .map(|name| name.parse().unwrap())
                .collect(),
            bundles: vec![1, 16],
            overrides: vec![Override {
                name: "fast".to_string(),
                set: BTreeMap::new(),
            }],
            events: vec![EventKind::None, EventKind::Stop],
            runs: 1,
            // what every spec measured before F74, so its digest is the one pinned below
            routing: Routing::Endpoints,
            ..BenchSpec::default()
        };
        let ids: Vec<String> = spec.arms().into_iter().map(|arm| arm.id.0).collect();
        assert_eq!(ids.len(), 20);
        for expected in [
            "read100/b1/fast/none",
            "insert100/b16/fast/none",
            "rw50/b1/fast/stop",
            "read90/b16/fast/stop",
            "read7-insert3/b1/fast/none",
            "read7-insert3/b16/fast/stop",
        ] {
            assert!(ids.iter().any(|id| id == expected), "{expected} is gone: {ids:?}");
        }
        // the workloads write back as they were spelled, and the digest has not moved
        let written: Vec<String> = spec.workloads.iter().cloned().map(String::from).collect();
        assert_eq!(
            written,
            vec!["read100", "insert100", "rw50", "read90", "read:7,insert:3"]
        );
        assert_eq!(spec.digest(), "95b060505630a0d20ebe958212e654470e9209fe42a349267e67fccee777d74f");
        // a spec routed by topology measures something else, and says so in its digest (F74)
        let routed = BenchSpec {
            routing: Routing::Topology,
            ..spec.clone()
        };
        assert_ne!(routed.digest(), spec.digest());
        assert_eq!(routed.arms().len(), spec.arms().len());
    }

    /// An object arm's id is the key every comparison of it joins on, so the ids and the digest
    /// of a spec of object arms are pinned like a table arm's (F80)
    ///
    /// The ids are `{mix}/s{size}/c{concurrency}[/{override}]/none`, `sd` for each description's
    /// own sizes, after every table arm; and a spec of tables alone writes none of the three new
    /// keys, which `table_arm_ids_are_unchanged` holds by its digest.
    #[test]
    fn object_arm_ids_are_unchanged() {
        use crate::objects::ObjectMix;
        use shoal::shared::pools::ByteSize;
        let spec = BenchSpec {
            dataset: "data".into(),
            workloads: vec!["read100".parse().unwrap()],
            bundles: vec![1],
            objects: ObjectMix::defaults(),
            object_sizes: vec![ByteSize::parse("4KiB").unwrap(), ByteSize::parse("1MiB").unwrap()],
            concurrency: vec![8],
            overrides: vec![Override {
                name: "fast".to_string(),
                set: BTreeMap::new(),
            }],
            events: vec![EventKind::None],
            runs: 1,
            routing: Routing::Topology,
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        let ids: Vec<String> = arms.iter().map(|arm| arm.id.0.clone()).collect();
        // one table arm, then three mixes at two sizes, all under the one override
        assert_eq!(ids.len(), 7, "{ids:?}");
        assert_eq!(ids[0], "read100/b1/fast/none");
        for expected in [
            "put100/s4k/c8/fast/none",
            "get100/s1m/c8/fast/none",
            "rw50/s4k/c8/fast/none",
            "rw50/s1m/c8/fast/none",
        ] {
            assert!(ids.iter().any(|id| id == expected), "{expected} is gone: {ids:?}");
        }
        // every object arm carries what it does, and keeps its concurrency in flight
        for arm in &arms[1..] {
            let object = arm.object.as_ref().expect("an object arm");
            assert_eq!((object.concurrency, arm.in_flight, arm.bundle), (8, 8, 1));
        }
        // a spec that names no size runs each description's own, and no override drops the segment
        let described = BenchSpec {
            workloads: Vec::new(),
            object_sizes: Vec::new(),
            overrides: Vec::new(),
            ..spec.clone()
        };
        let ids: Vec<String> = described.arms().into_iter().map(|arm| arm.id.0).collect();
        assert_eq!(ids, vec!["put100/sd/c8/none", "get100/sd/c8/none", "rw50/sd/c8/none"]);
        // and the digest has not moved
        assert_eq!(spec.digest(), "075bb89f08e24d56f48babb39ee2ff4a58d18ce7263341771c6e68d6cf17fbe3");
    }

    /// An object arm carries a node's events and a device's, after every steady arm; a table arm
    /// never carries a device's, and a spec that names one with no object arm is refused (F88)
    #[test]
    fn object_arms_carry_device_and_node_events() {
        use crate::objects::ObjectMix;
        use shoal::shared::pools::ByteSize;
        let spec = BenchSpec {
            dataset: "data".into(),
            workloads: vec!["read100".parse().unwrap()],
            bundles: vec![1],
            objects: vec![ObjectMix::defaults().remove(0)],
            object_sizes: vec![ByteSize::parse("1MiB").unwrap()],
            concurrency: vec![8],
            events: vec![
                EventKind::None,
                EventKind::Kill,
                EventKind::DeviceFail,
                EventKind::DeviceAdd,
                EventKind::Repair,
                EventKind::DeepScrub,
            ],
            runs: 1,
            routing: Routing::Topology,
            ..BenchSpec::default()
        };
        assert!(spec.problems().is_empty(), "{:?}", spec.problems());
        let ids: Vec<String> = spec.arms().into_iter().map(|arm| arm.id.0).collect();
        assert_eq!(
            ids,
            vec![
                "read100/b1/none",
                "put100/s1m/c8/none",
                "read100/b1/kill",
                "read100/b1/repair",
                "put100/s1m/c8/kill",
                "put100/s1m/c8/device_fail",
                "put100/s1m/c8/device_add",
                "put100/s1m/c8/deep_scrub",
            ]
        );
        // the spellings read back
        assert_eq!("device_fail".parse::<EventKind>().unwrap(), EventKind::DeviceFail);
        assert_eq!("device_add".parse::<EventKind>().unwrap(), EventKind::DeviceAdd);
        assert_eq!("deep_scrub".parse::<EventKind>().unwrap(), EventKind::DeepScrub);
        assert_eq!("device_drain".parse::<EventKind>().unwrap(), EventKind::DeviceDrain);
        assert!(EventKind::DeviceDrain.on_devices() && EventKind::DeviceDrain.fits_objects());
        // a deep scrub takes no node down, so an attached cluster may run it (F89)
        assert!(!EventKind::DeepScrub.disrupts());
        // and a device's event with nothing to measure it is refused by name
        let tables = BenchSpec {
            objects: Vec::new(),
            ..spec
        };
        assert!(
            tables.problems().iter().any(|problem| problem.contains("device_fail")),
            "{:?}",
            tables.problems()
        );
    }

    /// A spec that names no routing measured what every spec before F74 did, and reads back so;
    /// a new spec routes by topology and writes it
    #[test]
    fn routing_reads_back_as_what_it_measured() {
        let old: BenchSpec = serde_yaml::from_str("dataset: data\n").unwrap();
        assert_eq!(old.routing, Routing::Endpoints);
        assert_eq!(BenchSpec::default().routing, Routing::Topology);
        let written = serde_yaml::to_string(&BenchSpec::default()).unwrap();
        assert!(written.contains("routing: topology"), "{written}");
        let endpoints = serde_yaml::to_string(&old).unwrap();
        assert!(!endpoints.contains("routing"), "{endpoints}");
        let back: BenchSpec = serde_yaml::from_str(&written).unwrap();
        assert_eq!(back.routing, Routing::Topology);
    }

    /// A spec of only a dataset takes every default, and an unknown field is refused
    #[test]
    fn a_short_spec_takes_the_defaults() {
        let spec: BenchSpec = serde_yaml::from_str("dataset: data\nworkloads: [read100, rw50]\npreload: 1000\n").unwrap();
        assert_eq!(spec.workloads.len(), 2);
        assert_eq!(spec.bundles, vec![1, 16, 64]);
        assert_eq!(spec.preload, crate::feed::Preload::Rows(1000));
        assert!(spec.problems().is_empty(), "{:?}", spec.problems());
        assert!(serde_yaml::from_str::<BenchSpec>("dataset: data\nbundle: [1]\n").is_err());
        // a share round trips
        let spec: BenchSpec = serde_yaml::from_str("dataset: data\npreload: 25%\n").unwrap();
        let back: BenchSpec = serde_yaml::from_str(&serde_yaml::to_string(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);
    }

    /// A spec written before F67 names its workloads `mixes`, and still reads as the same spec
    #[test]
    fn a_spec_that_says_mixes_still_reads() {
        let old: BenchSpec = serde_yaml::from_str("dataset: data\nmixes: [read100, rw50]\n").unwrap();
        let new: BenchSpec = serde_yaml::from_str("dataset: data\nworkloads: [read100, rw50]\n").unwrap();
        assert_eq!(old, new);
        // and is written back under the new name
        let written = serde_yaml::to_string(&old).unwrap();
        assert!(written.contains("workloads:") && !written.contains("mixes:"), "{written}");
    }

    /// The matrix is every combination, steady arms first, rotated a step a run
    #[test]
    fn arms_expand_and_rotate() {
        let spec = BenchSpec {
            dataset: "data".into(),
            workloads: vec!["read100".parse().unwrap(), "insert100".parse().unwrap()],
            bundles: vec![1, 16],
            events: vec![EventKind::Kill, EventKind::None],
            runs: 2,
            spare: None,
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        assert_eq!(arms.len(), 16);
        let first: Vec<&str> = arms[..8].iter().map(|arm| arm.id.0.as_str()).collect();
        assert_eq!(
            first,
            vec![
                "read100/b1/none",
                "read100/b16/none",
                "insert100/b1/none",
                "insert100/b16/none",
                "read100/b1/kill",
                "read100/b16/kill",
                "insert100/b1/kill",
                "insert100/b16/kill",
            ]
        );
        // the second run starts each group one arm later
        assert_eq!(arms[8].id.0, "read100/b16/none");
        assert_eq!(arms[8].run, 1);
        assert_eq!(arms[12].id.0, "read100/b16/kill");
        // only an arm that writes or runs an event disturbs the cluster
        assert!(!arms[0].disturbs());
        assert!(arms[2].disturbs());
        assert!(arms[4].disturbs());
        // in flight defaults to four bundles
        assert_eq!(arms[1].in_flight, 64);
    }

    /// An override names its arms
    #[test]
    fn an_override_names_its_arms() {
        let spec = BenchSpec {
            dataset: "data".into(),
            workloads: vec!["read100".parse().unwrap()],
            bundles: vec![8],
            runs: 1,
            overrides: vec![Override {
                name: "small-memory".to_string(),
                set: BTreeMap::from([("resources.memory".to_string(), "1Gi".into())]),
            }],
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        assert_eq!(arms[0].id.0, "read100/b8/small-memory/none");
    }

    /// An attached cluster refuses anything that takes it down, and a move needs a spare
    #[test]
    fn an_attached_spec_refuses_disruption() {
        let spec = BenchSpec {
            dataset: "data".into(),
            mode: Mode::Attach,
            events: vec![EventKind::Kill, EventKind::Repair, EventKind::Rebalance],
            ..BenchSpec::default()
        };
        let problems = spec.problems().join("\n");
        assert!(problems.contains("kill takes a node down"), "{problems}");
        assert!(problems.contains("--spare"), "{problems}");
        assert!(!problems.contains("repair"), "{problems}");
    }

    /// The digest moves with what is measured and not with where the data lives
    #[test]
    fn the_digest_ignores_the_dataset_path() {
        let spec = BenchSpec {
            dataset: "one".into(),
            ..BenchSpec::default()
        };
        let moved = BenchSpec {
            dataset: "two".into(),
            ..spec.clone()
        };
        let longer = BenchSpec {
            duration: 61,
            ..spec.clone()
        };
        assert_eq!(spec.digest(), moved.digest());
        assert_ne!(spec.digest(), longer.digest());
    }

    /// A paced stream is read from a spec with its defaults, moves the digest only when given,
    /// and is refused when it could not run beside the main load
    #[test]
    fn a_paced_stream_reads_and_is_judged() {
        let spec: BenchSpec = serde_yaml::from_str("dataset: data\npaced:\n  table: Review\n").unwrap();
        let paced = spec.paced.clone().unwrap();
        assert_eq!(paced, Paced::new("Review"));
        assert_eq!(paced.workload.name, "read100");
        assert_eq!((paced.per_sec, paced.workers, paced.in_flight()), (20.0, 1, 20));
        assert!(spec.problems().is_empty(), "{:?}", spec.problems());
        // without one the spec writes no paced key at all, so its digest is what it was
        let plain = BenchSpec {
            paced: None,
            ..spec.clone()
        };
        assert!(!serde_json::to_string(&plain).unwrap().contains("paced"));
        assert_ne!(plain.digest(), spec.digest());
        // an unknown field of the stream is refused like one of the spec's
        assert!(serde_yaml::from_str::<BenchSpec>("dataset: data\npaced:\n  table: A\n  rate: 5\n").is_err());
        // a stream that offers nothing, sends a supplied kind, or is weighted for the main load
        let bad = BenchSpec {
            dataset: "data".into(),
            tables: BTreeMap::from([("Review".to_string(), 1)]),
            paced: Some(Paced {
                per_sec: 0.0,
                workers: 0,
                workload: "read:1,lookup:1".parse().unwrap(),
                ..Paced::new("Review")
            }),
            ..BenchSpec::default()
        };
        let problems = bad.problems().join("\n");
        for expected in ["--paced-rate 0", "--paced-workers", "supplied kind", "take it out of --tables"] {
            assert!(problems.contains(expected), "{expected} not in {problems}");
        }
        // a stream that inserts makes the run one that writes, though its workloads only read
        let reads = BenchSpec {
            workloads: vec!["read100".parse().unwrap()],
            ..BenchSpec::default()
        };
        assert!(!reads.writes());
        let inserting = BenchSpec {
            paced: Some(Paced {
                workload: "insert100".parse().unwrap(),
                ..Paced::new("Review")
            }),
            ..reads
        };
        assert!(inserting.writes());
        // two streams share the rate, and each keeps a second of its share outstanding
        let shared = Paced {
            per_sec: 5.0,
            workers: 2,
            ..Paced::new("Review")
        };
        assert_eq!(shared.in_flight(), 3);
    }

    /// A file names its own preload over the spec's, and a spec that names none writes nothing of
    /// it, so its digest is the digest it had before F104 (F104)
    #[test]
    fn a_file_names_its_own_preload() {
        use crate::feed::Preload;
        // a spec of one preload for every file
        let spec = BenchSpec {
            preload: Preload::Rows(1024),
            ..BenchSpec::default()
        };
        assert_eq!(spec.preload_of("Movie"), Preload::Rows(1024));
        let written = serde_json::to_value(&spec).expect("json");
        assert!(written.get("preload_files").is_none(), "{written}");
        // one file named its whole, the rest at the spec's, and the digest moves with it
        let named: BenchSpec = serde_yaml::from_str(
            "dataset: data\npreload: 1024\npreload_files: {Movie: 100%}\n",
        )
        .expect("a spec");
        assert_eq!(named.preload_of("Movie"), Preload::Percent(100.0));
        assert_eq!(named.preload_of("Posters"), Preload::Rows(1024));
        let unnamed = BenchSpec {
            preload_files: std::collections::BTreeMap::new(),
            ..named.clone()
        };
        assert_ne!(named.digest(), unnamed.digest());
    }
}
