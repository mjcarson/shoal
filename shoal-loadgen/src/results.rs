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
    /// The storage pools and the devices under them, without paths: what a pool's numbers depend
    /// on ([F85](../../docs/src/features/devices-and-pools.md))
    ///
    /// Absent from a capture before F85, which `compare` reads as no pools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_shape: Option<PoolShape>,
    /// The store measured, when it is not a Shoal cluster the bench deployed or attached to:
    /// another store's S3 endpoints ([F94](../../docs/src/features/bench-object-endpoints.md)).
    /// Absent from every Shoal capture, which `compare` reads as the system `shoal`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<StoreFacts>,
}

impl Provenance {
    /// The system a capture measured: the store's name, or `shoal` for a capture of a Shoal
    /// cluster ([F94](../../docs/src/features/bench-object-endpoints.md))
    #[must_use]
    pub fn system(&self) -> &str {
        self.store.as_ref().map_or("shoal", |store| store.system.as_str())
    }
}

/// Another store, as a capture of it records it ([F94](../../docs/src/features/bench-object-endpoints.md))
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreFacts {
    /// What it is, by a name of one word: `ceph`, `minio`, `rustfs`, `seaweedfs`
    pub system: String,
    /// The version deployed, as its operator named it
    pub version: String,
    /// How it keeps a bucket's objects, as its operator named it: `rep3`, `ec21`
    pub redundancy: String,
    /// The endpoints the driver's streams were spread over, in the order they were given
    pub endpoints: Vec<String>,
    /// How every request was signed
    pub signing: String,
    /// The region requests were signed for
    pub region: String,
    /// The bucket each dataset's objects were put in, by the dataset's bucket
    pub buckets: BTreeMap<String, String>,
    /// The placement a bucket was made with, if one was asked for
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location_constraint: Option<String>,
    /// The names of the store's processes the hosts' counters follow
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub processes: Vec<String>,
    /// Anything else the operator put on record, by name: settings, a program's sha256
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub notes: BTreeMap<String, String>,
}

/// One device as a capture records it: what it is, never where
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeviceShape {
    /// The class its operator wrote
    pub class: String,
    /// Its slices
    pub slices: u16,
    /// The filesystem it is on, as the host named it
    pub filesystem: String,
    /// Whether the kernel calls the disk under it rotational
    pub rotational: bool,
}

/// A deployment's pools and the devices under them, as a capture records them
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolShape {
    /// Each pool's redundancy, failure domain and class, by name
    pub pools: BTreeMap<String, String>,
    /// Each node's devices, by its name in the inventory, in a stable order
    pub devices: BTreeMap<String, Vec<DeviceShape>>,
}

impl PoolShape {
    /// The shape as one line, which `compare` judges two captures by
    #[must_use]
    pub fn line(&self) -> String {
        let pools: Vec<String> = self.pools.iter().map(|(name, pool)| format!("{name}={pool}")).collect();
        let devices: Vec<String> = self
            .devices
            .iter()
            .map(|(node, devices)| {
                let each: Vec<String> = devices
                    .iter()
                    .map(|device| {
                        format!(
                            "{}x{} on {}{}",
                            device.class,
                            device.slices,
                            device.filesystem,
                            if device.rotational { " rotational" } else { "" }
                        )
                    })
                    .collect();
                format!("{node}=[{}]", each.join(", "))
            })
            .collect();
        format!("pools {} devices {}", pools.join("; "), devices.join("; "))
    }
}

/// The dataset as it was found
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetFacts {
    /// A sha256 over every table's name, format and file digest
    pub digest: String,
    /// What scanning each table's file found
    pub tables: Vec<TableScan>,
    /// Each bucket's description, whole, since it is a few lines, by bucket; absent from a
    /// dataset that describes none, so its digest is what it was
    /// ([F80](../../docs/src/features/object-wire.md))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<crate::objects::ObjectDataset>,
}

impl DatasetFacts {
    /// The same facts with the buckets the folder describes, folded into the digest
    ///
    /// A dataset that describes no bucket keeps the digest its tables alone give it.
    ///
    /// # Arguments
    ///
    /// * `objects` - Every bucket's description
    #[must_use]
    pub fn with_objects(mut self, objects: Vec<crate::objects::ObjectDataset>) -> Self {
        use sha2::{Digest, Sha256};
        if objects.is_empty() {
            return self;
        }
        // the tables' digest, then each bucket's name and description's digest in name order
        let mut parts: Vec<String> = objects
            .iter()
            .map(|dataset| format!("{}\tobjects\t{}\n", dataset.bucket, dataset.digest))
            .collect();
        parts.sort();
        let text = format!("{}\n{}", self.digest, parts.concat());
        self.digest = crate::read::hex(&Sha256::digest(text.as_bytes()));
        self.objects = objects;
        self
    }

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
        DatasetFacts {
            digest,
            tables,
            objects: Vec::new(),
        }
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
    /// The shards' eviction budgets together ([F104](../../docs/src/features/memory-in-a-capture.md)); zero in a capture from
    /// before F104
    #[serde(default, skip_serializing_if = "is_zero")]
    pub memory_budget: u64,
    /// Partitions the shards evicted since they started, their rows past their own budgets
    /// ([F104](../../docs/src/features/memory-in-a-capture.md))
    #[serde(default, skip_serializing_if = "is_zero")]
    pub evicted_by_budget: u64,
    /// Partitions the shards evicted since they started, the process past the node's memory
    /// ([F104](../../docs/src/features/memory-in-a-capture.md))
    #[serde(default, skip_serializing_if = "is_zero")]
    pub evicted_by_resident: u64,
    /// Bytes of rows the shards evicted since they started, by either
    #[serde(default, skip_serializing_if = "is_zero")]
    pub evicted_bytes: u64,
    /// Bytes the node's caches of read units held ([F102](../../docs/src/features/read-unit-cache.md),
    /// F104)
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_held_bytes: u64,
    /// Reads its caches answered since the node started
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_hits: u64,
    /// Reads that looked in its caches and were not answered since the node started
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_misses: u64,
    /// Checks that found the node past its memory and had its caches give units back instead of
    /// evicting rows, since the node started ([F107](../../docs/src/features/cache-default-share.md))
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_yields: u64,
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
    /// Each live member's object work in flight and what it refused or gave up, by the name the
    /// stats view gives it; empty in a capture from before
    /// [F108](../../docs/src/features/object-work-in-flight.md) or of members with no object work
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub objects: BTreeMap<String, shoal::shared::protocol::stats::ObjectWork>,
    /// Each live member's walk pages and scans since it started, by class, with its scan budget
    /// and pace, by the name the stats view gives it; empty in a capture from before
    /// [M23](../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load) or of
    /// members that walked nothing
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scans: BTreeMap<String, shoal::shared::protocol::stats::ScanFigures>,
    /// Each live member's scan steps by shard, each with the cpu its executor is pinned to, by the
    /// name the stats view gives it; empty in a capture from before
    /// [item 353](../../docs/src/appendix/resolved/capture-allocations.md)
    /// or of members whose shards took no scan step
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scan_shards: BTreeMap<String, Vec<shoal::shared::protocol::stats::ShardScanSteps>>,
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
    /// How many were found holding other bytes than were acknowledged: an object read back that
    /// differs from its bytes made again ([F80](../../docs/src/features/object-wire.md)); absent
    /// from every capture before F80, which read back no object
    #[serde(default, skip_serializing_if = "is_zero")]
    pub misread: u64,
}

/// Deleting what an arm put, outside every window, so the next arm finds the store as the preload
/// left it ([F94](../../docs/src/features/bench-object-endpoints.md))
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CleanupFacts {
    /// How many objects were deleted
    pub deleted: u64,
    /// How many deletes failed, by code; an object a delete failed for is left on the store
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, u64>,
    /// How long the deletes took, in seconds
    pub secs: f64,
    /// How long the store's settle command took, in seconds, if one was given
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle_secs: Option<f64>,
    /// What the settle command exited with, if one was given
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle_status: Option<i32>,
}

/// Whether a count is zero, so a field that was not there before is left out
///
/// # Arguments
///
/// * `count` - The count
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(count: &u64) -> bool {
    *count == 0
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
    /// What the host's cpus did, from `/proc/stat`; none in a capture from before
    /// [F94](../../docs/src/features/bench-object-endpoints.md)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<HostCpu>,
    /// What each network interface carried, from `/proc/net/dev`, in name order (F94)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interfaces: Vec<InterfaceCounters>,
    /// The host's memory before and after (F94)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<HostMemory>,
    /// What the store's processes on the host did, by the name they run under (F94)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub processes: Vec<ProcessCounters>,
    /// How full each root's filesystem was before the run, by the root's label (F94)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub usage: Vec<RootUsage>,
}

/// What a host's cpus did while a run ran, from the `cpu` line of `/proc/stat`
/// ([F94](../../docs/src/features/bench-object-endpoints.md))
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HostCpu {
    /// Cpu-seconds every cpu spent doing anything but idling or waiting for I/O
    pub busy_secs: f64,
    /// Cpu-seconds every cpu counted, busy or not
    pub total_secs: f64,
}

/// What one network interface carried while a run ran (F94)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceCounters {
    /// Its name, as `/proc/net/dev` spells it
    pub name: String,
    /// Bytes received
    pub received_bytes: u64,
    /// Bytes sent
    pub sent_bytes: u64,
}

/// A host's memory around a run, from `/proc/meminfo` (F94)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostMemory {
    /// What the kernel said was available before it
    pub available_before: u64,
    /// And after
    pub available_after: u64,
    /// The page cache before it
    pub cached_before: u64,
    /// And after
    pub cached_after: u64,
}

/// What a store's processes of one name did while a run ran (F94)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcessCounters {
    /// The name they run under, their program's file name
    pub name: String,
    /// How many processes of that name were running when the run ended
    pub processes: u64,
    /// Cpu-seconds they spent, user and system, counting only processes alive at both reads
    pub cpu_secs: f64,
    /// Their resident memory when the run ended, summed
    pub resident_bytes: u64,
    /// The most each had resident since it started, summed: a process's own high water mark
    pub peak_resident_bytes: u64,
}

/// How full a root's filesystem was (F94)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootUsage {
    /// The root, as `<node> <role> <path>`
    pub root: String,
    /// Bytes used on its filesystem
    pub used_bytes: u64,
    /// The filesystem's size
    pub size_bytes: u64,
}

impl RootUsage {
    /// How full it is, in percent
    #[must_use]
    pub fn percent(&self) -> f64 {
        // an empty or unknown size is never full
        if self.size_bytes == 0 {
            return 0.0;
        }
        self.used_bytes as f64 * 100.0 / self.size_bytes as f64
    }
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
    /// The bytes every member's slices still held staged once an object arm's last answer was
    /// in and its records had a minute to be decided; zero is what a run leaves behind, and none
    /// for an arm that wrote no object or a cluster that could not say
    /// ([F86](../../docs/src/features/replicated-pools.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_left: Option<u64>,
    /// What deleting the arm's objects after it did, when objects are deleted between arms
    /// rather than the cluster reset ([F94](../../docs/src/features/bench-object-endpoints.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<CleanupFacts>,
    /// The bytes sent and received over the whole run, kept when its series is dropped from a
    /// slim capture, so what was counted per byte sent is still counted (F94)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub totals: Option<RunTotals>,
    /// What each member's strong reads waited for, and how long their rounds' appends took, over
    /// the whole run as its device counters cover it, by member
    /// ([F99](../../docs/src/features/strong-read-rounds.md))
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rounds: BTreeMap<String, MemberRounds>,
}

/// What one member's strong reads waited for over a run, and how long its rounds' appends took,
/// read from its replication report before and after the run
/// ([F99](../../docs/src/features/strong-read-rounds.md))
///
/// Every list counts waits by bucket, in the node's buckets for a client query's wait: one a
/// microsecond under four, then four to every power of two of microseconds. A list stops at its
/// slowest wait.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MemberRounds {
    /// Read barriers the member's shards obtained, one a group a strong read touched
    pub barriers: u64,
    /// Nanoseconds the member's strong reads waited for their barriers, in all
    pub barrier_wait_ns: u64,
    /// Nanoseconds the member's copies waited to apply through a barrier or a token, in all
    pub apply_wait_ns: u64,
    /// The member's strong reads by how long each waited for its barriers
    pub barrier_buckets: Vec<u64>,
    /// The empty appends the member sent and had answered, by how long each answer took: a
    /// strong read's round and its groups' heartbeats alike
    pub sent_buckets: Vec<u64>,
    /// The empty appends the member answered, by how long it took from an append's arrival
    pub answered_buckets: Vec<u64>,
}

/// The bytes one run sent and received, warmup, measured time and drain together (F94)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTotals {
    /// Bytes sent
    pub sent: u64,
    /// Bytes received
    pub received: u64,
}

impl RunResult {
    /// The bytes the main stream's bundles took on the wire over the whole run: warmup, measured
    /// time and drain, the same time its device counters cover
    #[must_use]
    pub fn bytes_sent(&self) -> u64 {
        // every second, whatever phase it fell in, or the totals a slim capture kept of them
        match (&self.totals, self.series.is_empty()) {
            (Some(totals), true) => totals.sent,
            _ => self.series.iter().map(|second| second.summary.bytes_sent).sum(),
        }
    }

    /// The bytes the run was answered with over the whole run, as [`RunResult::bytes_sent`]
    /// counts what it sent
    #[must_use]
    pub fn bytes_received(&self) -> u64 {
        // every second, or the totals a slim capture kept of them
        match (&self.totals, self.series.is_empty()) {
            (Some(totals), true) => totals.received,
            _ => self.series.iter().map(|second| second.summary.bytes_received).sum(),
        }
    }

    /// The cpu-seconds the store's processes spent on every host while it ran, if any host
    /// counted them ([F94](../../docs/src/features/bench-object-endpoints.md))
    #[must_use]
    pub fn store_cpu_secs(&self) -> Option<f64> {
        // every process of every host that named any
        let counted: Vec<f64> = self
            .devices
            .iter()
            .flat_map(|host| &host.processes)
            .map(|process| process.cpu_secs)
            .collect();
        (!counted.is_empty()).then(|| counted.iter().sum())
    }

    /// The bytes every host sent on its network interfaces while it ran, loopback left out, if
    /// any host counted them (F94); a byte between two hosts is counted once, by its sender
    #[must_use]
    pub fn wire_bytes(&self) -> Option<u64> {
        // every interface but the loopback of every host
        let hosts: Vec<&HostDevices> = self.devices.iter().filter(|host| !host.interfaces.is_empty()).collect();
        (!hosts.is_empty()).then(|| {
            hosts
                .iter()
                .flat_map(|host| &host.interfaces)
                .filter(|interface| interface.name != "lo")
                .map(|interface| interface.sent_bytes)
                .sum()
        })
    }

    /// The most any one host's store processes had resident, their high water marks summed, if
    /// any host counted them (F94)
    #[must_use]
    pub fn store_peak_resident(&self) -> Option<u64> {
        // each host's processes together, then the largest host
        self.devices
            .iter()
            .filter(|host| !host.processes.is_empty())
            .map(|host| host.processes.iter().map(|process| process.peak_resident_bytes).sum::<u64>())
            .max()
    }

    /// The payload bytes the run moved both ways, which a cost per byte is taken over (F94)
    #[must_use]
    pub fn bytes_moved(&self) -> u64 {
        self.bytes_sent() + self.bytes_received()
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
    /// The read back of every object still acknowledged once the last arm was done - the whole
    /// preload - when objects were deleted between arms
    /// ([F94](../../docs/src/features/bench-object-endpoints.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_end: Option<VerifyFacts>,
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
            verify_end: None,
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

    /// A server sample keeps each member's object work and reads it back, one from before F108
    /// reads as none, and one with none writes none (F108)
    #[test]
    fn a_samples_object_work_round_trips() {
        use super::ServerSample;
        // one from before F108, as a capture of it holds one
        let old: ServerSample =
            serde_json::from_str(r#"{"at_ms": 1000, "answers_per_sec": {}, "p99_ms": null}"#)
                .expect("an old sample");
        assert!(old.objects.is_empty());
        // one with a member's work in flight reads back as written
        let mut sample = ServerSample {
            at_ms: 2000,
            ..ServerSample::default()
        };
        sample.objects.insert(
            "titan".to_string(),
            shoal::shared::protocol::stats::ObjectWork {
                waiting: 4,
                owed: 2,
                tasks: 9,
                ..Default::default()
            },
        );
        let back: ServerSample =
            serde_json::from_str(&serde_json::to_string(&sample).expect("written"))
                .expect("read back");
        assert_eq!(back, sample);
        // and one with none writes none
        let none = serde_json::to_string(&ServerSample::default()).expect("written");
        assert!(!none.contains("objects"), "{none}");
    }

    /// A sample keeps each member's walk figures and reads back as written, its steps by kind with
    /// their own cpu among them (F130), one from before them reading as none, and a sample whose
    /// members walked nothing writes none (M23)
    #[test]
    fn a_samples_walk_figures_round_trip() {
        use super::ServerSample;
        // one from before M23, as a capture of it holds one
        let old: ServerSample =
            serde_json::from_str(r#"{"at_ms": 1000, "answers_per_sec": {}, "p99_ms": null}"#)
                .expect("an old sample");
        assert!(old.scans.is_empty());
        // one with a member's scan pages and its steps reads back as written
        let mut figures = shoal::shared::protocol::stats::ScanFigures::default();
        figures.scan.pages = 12;
        let held = shoal::shared::protocol::stats::StepSample {
            wall_ns: 140_000,
            cpu_ns: 60_000,
            keys: 1,
            rows: 30,
            bytes: 40_000,
            ..Default::default()
        };
        figures.scan.step(shoal::shared::protocol::stats::StepKind::Encode, &held);
        figures.driver.pages = 3;
        figures.budget_peak = 1 << 20;
        let mut sample = ServerSample {
            at_ms: 2000,
            ..ServerSample::default()
        };
        sample.scans.insert("titan".to_string(), figures);
        let back: ServerSample =
            serde_json::from_str(&serde_json::to_string(&sample).expect("written")).expect("read back");
        assert_eq!(back, sample);
        assert_eq!(back.scans["titan"].scan.encodes.longest, held);
        // and one with none writes none
        let none = serde_json::to_string(&ServerSample::default()).expect("written");
        assert!(!none.contains("scans"), "{none}");
    }

    /// A sample keeps each member's scan steps by shard with their cpus and reads back as written,
    /// one from before them reads as none, and a sample whose members' shards took no scan step
    /// writes none
    /// ([item 353](../../docs/src/appendix/resolved/capture-allocations.md))
    #[test]
    fn a_samples_shard_steps_round_trip() {
        use super::ServerSample;
        use shoal::shared::protocol::stats::{ShardScanSteps, StepCounts};
        // one from before them, as every capture of M23's exit holds
        let old: ServerSample = serde_json::from_str(r#"{"at_ms": 1000, "answers_per_sec": {}, "p99_ms": null, "scans": {}}"#)
            .expect("an old sample");
        assert!(old.scan_shards.is_empty());
        // one with two shards of a member, one on the cpu its NIC interrupts, reads back as written
        let shard = |shard: u32, cpu: u32, past_cpu: u64| ShardScanSteps {
            shard,
            cpu: Some(cpu),
            captures: StepCounts { steps: 40, past: 1, past_cpu, ..StepCounts::default() },
            encodes: StepCounts { steps: 900, past_cpu_faulted: 1, past_faults: 30, ..StepCounts::default() },
        };
        let mut sample = ServerSample {
            at_ms: 4000,
            ..ServerSample::default()
        };
        sample.scan_shards.insert("titan".to_string(), vec![shard(0, 2, 0), shard(3, 5, 1)]);
        let written = serde_json::to_string(&sample).expect("written");
        let back: ServerSample = serde_json::from_str(&written).expect("read back");
        assert_eq!(back, sample);
        assert_eq!(back.scan_shards["titan"][1].cpu, Some(5));
        // and one with none writes none
        let none = serde_json::to_string(&ServerSample::default()).expect("written");
        assert!(!none.contains("scan_shards"), "{none}");
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
            ..HostDevices::default()
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
