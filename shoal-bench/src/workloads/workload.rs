//! What a workload is, and what the harness promises it
//!
//! A workload exists to isolate one path through the engine. That is the whole difference between
//! this and what it replaces: the `tmdb` example drove inserts and gets together against a live
//! server and reported one blended number, so a change to the read path and a change to the write
//! path moved the same figure and neither could be attributed. A workload names the path it is
//! about in its identifier, records how it was measured, and is compared only against the same
//! workload from another capture.
//!
//! # The trait is object safe on purpose
//!
//! Every workload owns its own client internally and hands back a [`Measurement`], rather than the
//! trait being generic over the database it drives. Two workloads want different schemas -
//! `insert_unsorted` wants an unsorted table and a fanout workload wants a sorted one - and a
//! generic trait would drag the twenty line `where` clause from `shoal/tests/utils.rs` onto every
//! signature that mentioned one. Erasing the database at the trait boundary costs a `Box` per run,
//! which is a rounding error against starting a server.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context as _, Result};
use shoal::server::tables::storage::fs::conf::Durability;
use shoal::shared::tls::TlsClientOptions;

use crate::model::macro_layer::{ConfFacts, ScaleFacts, Timing};
use crate::workloads::harness::seed::Scale;
use crate::workloads::harness::timer::Samples;
use crate::workloads::schema::BenchClient;

/// A future a workload returns, boxed so the trait stays object safe
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a workload needs before it can run
#[derive(Debug, Clone)]
pub struct WorkloadPlan {
    /// Whether this workload needs a server, and how it wants one configured
    pub server: ServerNeed,
    /// How much data to build and how hard to drive it
    pub scale: ScaleFacts,
    /// How many rows to move before sampling starts
    ///
    /// Without a warmup the first batches of a run carry connection establishment and cold
    /// partition faults, and they land in the distribution beside the steady state.
    pub warmup: u64,
}

/// Whether a workload needs a server, and what it needs of it
///
/// [`ServerNeed::None`] has no workload using it yet and is not speculative scaffolding: the
/// storage write path benchmark that `docs/src/appendix/todos.md` asks for is blocked on driving a
/// glommio executor from inside criterion, and that page already says a standalone binary emitting
/// the same JSON is an acceptable substitute. This binary is that substitute, and this arm is where
/// such a workload attaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerNeed {
    /// No server at all - the workload drives engine internals in process
    None,
    /// A server started fresh for this workload, with these overrides applied
    Fresh(ConfOverrides),
    /// A server started fresh, then stopped and started again once the workload has seeded it
    ///
    /// The only reliable way to reach the archived read path from a client. A shutdown flushes and
    /// compacts every partition, and the server that comes back up holds nothing in memory, so
    /// every read must find its partition on disk and go through
    /// [`MaybeLoaded::Accessible`](shoal::tables::partitions::MaybeLoaded) and the blocked read
    /// replay.
    ///
    /// The alternative was to squeeze `resources.memory` until the LRU evicted, which is what
    /// `shoal/tests/utils.rs::build_pressured_config` does. That is not usable here: a partition
    /// cannot be evicted until its generation has been compacted, so how much ends up on disk
    /// depends on how the run happened to interleave with compaction, and the workload would
    /// measure a different mixture of resident and archived reads every time it ran.
    RestartAfterSeed(ConfOverrides),
}

impl ServerNeed {
    /// The configuration overrides this need carries, if it needs a server at all
    pub fn overrides(&self) -> Option<&ConfOverrides> {
        match self {
            ServerNeed::None => None,
            ServerNeed::Fresh(overrides) | ServerNeed::RestartAfterSeed(overrides) => {
                Some(overrides)
            }
        }
    }

    /// Whether the server is cycled between the seed and the measurement
    pub fn restarts(&self) -> bool {
        matches!(self, ServerNeed::RestartAfterSeed(_))
    }
}

/// What a workload changes about the base configuration
///
/// Every field is optional because a workload should only state what it actually depends on. A
/// workload that pins a value it does not care about stops tracking the base configuration when
/// that changes, which is a silent way to stop measuring the thing everything else measures.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfOverrides {
    /// The number of shards to run, when the workload needs a fixed count
    ///
    /// A workload that names this gets deterministic partition placement, which is what a fanout
    /// curve needs to hold the routing constant while the key count varies.
    pub shards: Option<usize>,
    /// The number of shards the server restarts with, when the workload cycles it to another count
    ///
    /// The rehome arm's axis ([F47](../../../docs/src/features/local-rehome.md)): the seed runs
    /// at [`ConfOverrides::shards`], the server is stopped, and the one that comes back runs
    /// this many executors, so its start moves the vanished executors' files first. Read only
    /// by a [`ServerNeed::RestartAfterSeed`] arm.
    pub restart_shards: Option<usize>,
    /// The memory limit to hold the shards to
    ///
    /// Set low to force eviction, which is the only way to reach the archived read path from a
    /// client. `shoal/tests/utils.rs::build_pressured_config` does the same thing.
    pub memory: Option<String>,
    /// Whether this workload's server encrypts its connections
    ///
    /// The axis the TLS half of the transport control pair varies, and nothing else. A pair
    /// differs in this and in nothing else, which is what makes the difference between them
    /// attributable to encryption — the same shape [F9](../../../docs/src/features/ephemeral-tables.md)
    /// established for storage, applied to the wire.
    pub tls: bool,
    /// Which durability barrier a write waits on before its response is released
    ///
    /// The one axis [F8](../../../docs/src/features/purpose-built-workloads.md) said was nearly free
    /// to build and did not build. `Async` acknowledges a write once the kernel has taken it;
    /// `Fsync` waits for the fdatasync, and the difference between the two arms is what the barrier
    /// costs.
    pub durability: Option<Durability>,
    /// How many bytes the intent log buffers before it flushes
    ///
    /// A minimum rather than an exact size: the writer rounds it up to the device's O_DIRECT
    /// alignment, so a value below the device block size is a no-op. See
    /// `shoal-core/src/server/tables/storage/fs/stream.rs`.
    pub latency_buffer_size: Option<usize>,
    /// How many intent log writes may be in flight at once
    ///
    /// The io_uring queue depth for the write path. The writer stalls until a completion drains
    /// once this many are outstanding.
    pub latency_write_behind: Option<usize>,
    /// How large the intent log may grow before compaction is due
    pub intent_log_size: Option<u64>,
    /// How many bytes the throughput sensitive writer buffers before it flushes
    ///
    /// Note that this reaches less of the engine than its name suggests - see item 71 in
    /// `docs/src/appendix/known-issues.md`. The sweep over it is deliberate anyway: a flat line is
    /// the evidence for that item.
    pub throughput_buffer_size: Option<usize>,
    /// How many throughput sensitive writes may be in flight at once
    ///
    /// Carries the same caveat as [`ConfOverrides::throughput_buffer_size`].
    pub throughput_write_behind: Option<usize>,
    /// The largest frame the server will accept, in bytes
    ///
    /// A frame length is used as an allocation size before the body arrives, so this is a bound on
    /// what one client can make the server allocate as much as it is a bound on a batch.
    pub max_frame_bytes: Option<u32>,
    /// Whether the server runs as a cluster node, and with what replication factor
    ///
    /// The one axis the cluster overhead arm moves
    /// ([F37](../../../docs/src/features/node-identity-control-plane.md)): a `cluster:` block
    /// that bootstraps a cluster of one on the default control core, against a standalone twin
    /// that has none. Everything else about the two is the reference cell, which is what makes
    /// the difference between them the control plane's cost.
    pub cluster: Option<ClusterOverride>,
}

/// What a cluster arm asks of its server
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOverride {
    /// The replication factor the bootstrap records
    ///
    /// ~~Recorded, not enforced, at M1: a one node cluster serves everything locally whatever
    /// this says.~~ Since [F40](../../../docs/src/features/replication.md) it is the factor
    /// the tablet groups replicate at, and an arm asking for more copies than it places nodes
    /// is refused by [`ClusterOverride::feasibility`] rather than measured at the smaller factor
    /// the map would settle on ([C10](../../../docs/src/distributed/performance.md)).
    pub replication_factor: u32,
    /// The shard count of every node placed beside this one, in placement order
    ///
    /// Empty is the cluster of one the overhead arm runs. Anything else is a static placement
    /// ([F38](../../../docs/src/features/inter-node-transport.md)): the harness mints the
    /// identities, stages a marker per node, and starts each peer as a `shoal-workload serve`
    /// child of the measured process, with node zero - this one - in process. Node zero's own
    /// shard count is [`ConfOverrides::shards`], which has to be set when this is not empty.
    pub peers: Vec<u16>,
    /// The hop this arm was built to take, recorded on the artifact
    ///
    /// A fact about the arm's construction and nothing the server reads, carried here because
    /// the override is the one thing a workload states about its server and the harness records.
    pub hop: Option<crate::model::macro_layer::HopFacts>,
    /// How this arm's reads are served, if it is a read arm, recorded the same way
    /// ([F41](../../../docs/src/features/read-consistency.md))
    pub read: Option<ReadArm>,
    /// The groups' checkpoint and retention counts, when the arm moves them off the defaults
    ///
    /// What the catch-up arms differ in: the snapshot arm shortens both so the returning node
    /// is past the purge point ([F43](../../../docs/src/features/node-recovery.md)). Applied to
    /// every node of the placement, since every node resolves the arm's own overrides.
    pub retention: Option<RetentionOverride>,
    /// The grace a retired copy's files are kept for, when the arm moves it off the default
    ///
    /// The migration arm shortens it so a move finishes inside the run: a move is done only
    /// once the source has retired its copy, and the default grace is minutes
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    pub retire_after: Option<std::time::Duration>,
    /// The shard count of every member staged beside the placement and placed on by nothing
    ///
    /// A spare joins the cluster and holds no tablet until a move brings it into a set; the
    /// migration arm stages one as its destination
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    pub spares: Vec<u16>,
    /// The grace a down member is removed after, when the arm moves it off the default
    ///
    /// The remove arm shortens it so the expiry lands inside the run
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md)).
    pub auto_remove_after: Option<std::time::Duration>,
    /// How often the control leader looks at its plans, when the arm shortens it
    pub plan_interval: Option<std::time::Duration>,
    /// How many moves one member is the source and destination of at a time, when the arm moves it
    pub moves_per_node: Option<u32>,
}

/// The groups' checkpoint and retention counts an arm moves off the defaults
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionOverride {
    /// How many entries a group commits between snapshots
    pub checkpoint_entries: u64,
    /// How many entries a group keeps behind its snapshot
    pub retained_entries: u64,
}

/// How a read arm was built: the level it reads at, whether it carries tokens, and its fanout
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadArm {
    /// The level every read is sent at, `one` or `quorum`
    pub level: String,
    /// Whether every read carries the token of the last write to its tablet
    pub session: bool,
    /// How the reads fan out, if this is a fanout arm
    pub fanout: Option<crate::model::macro_layer::FanoutFacts>,
}

impl ClusterOverride {
    /// The cluster of one the overhead arm runs
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The factor the bootstrap records
    #[must_use]
    pub fn alone(replication_factor: u32) -> Self {
        ClusterOverride {
            replication_factor,
            peers: Vec::new(),
            hop: None,
            read: None,
            retention: None,
            retire_after: None,
            spares: Vec::new(),
            auto_remove_after: None,
            plan_interval: None,
            moves_per_node: None,
        }
    }

    /// A placement of this many peers, each of the same shard count, at a factor
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The factor every tablet replicates at
    /// * `peers` - How many nodes to place beside node zero
    /// * `shards` - The shard count of each of them
    #[must_use]
    pub fn placed(replication_factor: u32, peers: usize, shards: u16) -> Self {
        ClusterOverride {
            replication_factor,
            peers: vec![shards; peers],
            hop: None,
            read: None,
            retention: None,
            retire_after: None,
            spares: Vec::new(),
            auto_remove_after: None,
            plan_interval: None,
            moves_per_node: None,
        }
    }

    /// How many members this arm stages, counting node zero and every spare
    #[must_use]
    pub fn members(&self) -> usize {
        self.nodes() + self.spares.len()
    }

    /// How many nodes this arm places, counting node zero
    #[must_use]
    pub fn nodes(&self) -> usize {
        self.peers.len() + 1
    }

    /// Whether the factor this arm asks for is one its placement can serve
    ///
    /// A cluster holds at most one copy of a tablet per node, so a factor past the node count
    /// is served at the node count: the map's `active_rf` is the smaller of the two and the
    /// write quorum stays the one the desired factor names, which on one node refuses every
    /// default write. That is an availability test, and a throughput arm built on it would
    /// measure a quorum nobody configured. The runner refuses the arm instead
    /// ([C10](../../../docs/src/distributed/performance.md),
    /// [F40](../../../docs/src/features/replication.md)).
    ///
    /// # Errors
    ///
    /// Says what the arm asked for and what it placed when the factor cannot be met.
    pub fn feasibility(&self) -> Result<(), String> {
        let nodes = self.nodes();
        if usize::try_from(self.replication_factor).map_or(true, |factor| factor > nodes) {
            return Err(format!(
                "a replication factor of {} on {nodes} node(s) is served at {nodes} copies with a \
                 quorum of {}: an availability test, not a throughput arm",
                self.replication_factor,
                self.replication_factor / 2 + 1
            ));
        }
        Ok(())
    }
}

/// A fault an arm asks the harness to inject while it runs
///
/// The harness does it, not the workload: the peers are the harness's children and the
/// workload holds a client and nothing else. The schedule is measured from the start of the
/// measured phase, on the harness's clock ([F42](../../../docs/src/features/primary-failover.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultSpec {
    /// The placement position of the node to kill, which is never zero: node zero is this process
    pub node: u32,
    /// How long after the measured phase starts to kill it
    pub at: std::time::Duration,
    /// How long after the kill to start it again, from the same staged identity
    pub restart_after: std::time::Duration,
    /// Whether to start it again at all: the remove arm kills a node for good and lets the
    /// grace remove it ([F46](../../../docs/src/features/capacity-rebalancing.md))
    pub restart: bool,
    /// How long the whole run is scheduled for, which bounds anything the harness watches
    /// after the restart ([F43](../../../docs/src/features/node-recovery.md))
    pub run_for: std::time::Duration,
}

/// What a background arm asks the harness to run inside its measured phase
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackgroundKind {
    /// A `Repair` of a table in verify mode ([F44](../../../docs/src/features/repair.md))
    Repair,
    /// A `Move` of the set holding a tablet from one node of the placement to another member
    /// ([F45](../../../docs/src/features/replica-migration.md))
    Move {
        /// The tablet whose set moves
        tablet: u16,
        /// The node leaving the set, by its position among the staged nodes
        from: u32,
        /// The member replacing it, by its position among the staged nodes
        to: u32,
    },
    /// A `Rebalance`: the sets spread over the members by weight and bytes, which is what
    /// brings a spare in ([F46](../../../docs/src/features/capacity-rebalancing.md))
    Rebalance,
    /// A `Decommission` of a node of the placement, by its position among the staged nodes
    Decommission {
        /// The member to drain
        node: u32,
        /// Whether the arm was built with nowhere for the member's sets to go, so the record
        /// is named for the blocked case it shows rather than for a drain
        blocked: bool,
    },
    /// A `Backup` of a table under the workload's own storage root, after an `Activate` of
    /// the wire version the file header needs
    /// ([F49](../../../docs/src/features/backup-and-recovery.md))
    Backup,
    /// The expiry of a killed node's grace: nothing is asked for; the harness's fault kills
    /// the node and the plan the leader records for it is polled once it appears
    Expire {
        /// The member the fault kills, by its position among the staged nodes
        node: u32,
    },
}

impl BackgroundKind {
    /// Whether this is a plan - a rebalance, a decommission or an expiry - rather than one operation
    #[must_use]
    pub const fn is_plan(&self) -> bool {
        matches!(
            self,
            BackgroundKind::Rebalance
                | BackgroundKind::Decommission { .. }
                | BackgroundKind::Expire { .. }
        )
    }
}

/// An operation an arm asks the harness to run in the background of its measured phase
///
/// Only a placed arm can ask for one, since the operation is asked of the cluster's control
/// plane; the harness refuses one on any other arm ([F44](../../../docs/src/features/repair.md),
/// [F45](../../../docs/src/features/replica-migration.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundSpec {
    /// How long after the measured phase starts to ask for it
    pub at: std::time::Duration,
    /// How long the whole run is scheduled for, which bounds the polling
    pub run_for: std::time::Duration,
    /// The table to verify, by the name the schema spells it; a move names a tablet instead
    pub table: &'static str,
    /// What is asked for
    pub kind: BackgroundKind,
}

/// One operation of a timed run, as the client saw it
///
/// What a fault arm keeps beside its distribution: every operation stamped by when it started
/// and whether it was answered, so the outage can be cut out of the run afterwards rather than
/// averaged into it ([C10](../../../docs/src/distributed/performance.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineSample {
    /// When the operation was sent, from the start of the run
    pub at: std::time::Duration,
    /// How long it took to be answered, or to fail
    pub elapsed: std::time::Duration,
    /// Whether it was answered
    pub ok: bool,
}

/// Everything a workload produced
#[derive(Debug, Default)]
pub struct Measurement {
    /// The latency samples, keyed by the operation they came from
    pub ops: BTreeMap<String, Samples>,
    /// How many rows moved, keyed by what they were
    pub counters: BTreeMap<String, u64>,
    /// When the run started, on the driver's clock, if the driver keeps a timeline
    ///
    /// The instant every [`TimelineSample::at`] and every fault mark is measured from. Absent
    /// from every driver but the timed one, whose samples are the only ones that need an axis.
    pub started: Option<std::time::Instant>,
    /// Every operation of a timed run in the order it was sent; empty for every other driver
    pub timeline: Vec<TimelineSample>,
    /// The client half of each query's stage record, when this build records them
    ///
    /// Handed back with the measurement rather than pushed to a global, so a workload that fails
    /// partway cannot leave half its records behind for the next one to pick up. The server half
    /// is drained from the shards after they shut down, and the two are joined by
    /// `crate::workloads::stages::build_report`.
    ///
    /// Unconditional, and a zero sized type unless this is a profiling build. It was a
    /// `#[cfg]`-gated `Vec` until [Resolved #76](../../../../docs/src/appendix/resolved/stage-join.md),
    /// and a field only some builds have is a field only some drivers remember to fill.
    pub stages: crate::workloads::stage_log::StageLog,
}

impl Measurement {
    /// Records a sample against an operation, creating its sample set on first use
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation this sample came from
    /// * `elapsed` - How long it took
    pub fn record(&mut self, op: &str, elapsed: std::time::Duration) {
        // an operation's set is created the first time it is sampled, so a workload never has to
        // declare up front which operations it will end up recording
        self.ops.entry(op.to_string()).or_default().record(elapsed);
    }

    /// Adds to a counter, creating it on first use
    ///
    /// # Arguments
    ///
    /// * `name` - What is being counted
    /// * `by` - How much to add
    pub fn count(&mut self, name: &str, by: u64) {
        *self.counters.entry(name.to_string()).or_insert(0) += by;
    }

    /// Takes another measurement's samples and counters into this one
    ///
    /// Used to pool what several client workers each gathered.
    ///
    /// # Arguments
    ///
    /// * `other` - The measurement to drain into this one
    pub fn absorb(&mut self, mut other: Measurement) {
        // pool each operation's samples under the same name
        for (op, mut samples) in std::mem::take(&mut other.ops) {
            self.ops.entry(op).or_default().absorb(&mut samples);
        }
        // and sum each counter
        for (name, count) in other.counters {
            self.count(&name, count);
        }
        // and keep every stage record both sides gathered
        self.stages.absorb(other.stages);
        // the timeline is one axis, so the earlier start is the start and the samples pool
        self.started = match (self.started, other.started) {
            (Some(mine), Some(theirs)) => Some(mine.min(theirs)),
            (mine, theirs) => mine.or(theirs),
        };
        self.timeline.append(&mut other.timeline);
    }
}

/// What a workload is handed when it runs
pub struct Context {
    /// The address the server for this workload is listening on
    pub addr: String,
    /// The seed every row of this run derives from
    pub seed: u64,
    /// How much data to build and how hard to drive it
    pub scale: ScaleFacts,
    /// How many rows to move before sampling starts
    pub warmup: u64,
    /// The configuration the server was actually started with
    pub conf: Option<ConfFacts>,
    /// What a client has to do to reach this workload's server, if it is encrypted
    ///
    /// Populated from the resolved config rather than from the workload, so that a workload asks
    /// for TLS in one place — its [`ConfOverrides`] — and gets a client that can reach it without
    /// naming a certificate anywhere.
    pub tls: Option<TlsClientOptions>,
}

impl Context {
    /// Open a client that can reach this workload's server
    ///
    /// Every workload builds its client through here rather than calling a constructor, so that an
    /// axis added to the server's configuration reaches every workload at once. Before this
    /// existed, adding TLS would have meant editing eleven call sites that all said
    /// `Shoal::new(&ctx.addr)`.
    pub async fn client(&self) -> Result<shoal::Shoal<BenchClient>> {
        // an unencrypted workload gets exactly the client it always got
        let options = match &self.tls {
            Some(tls) => shoal::client::ClientOptions::new().tls(tls.clone()),
            None => shoal::client::ClientOptions::new(),
        };
        shoal::Shoal::<BenchClient>::with_options(&self.addr, options)
            .await
            .context("failed to open a client")
    }
}

/// One purpose built benchmark
pub trait Workload: Send + Sync {
    /// What this workload is called
    ///
    /// This is the key every comparison joins on and the key the artifact is written under, so it
    /// must stay byte identical across captures. Renaming one orphans every capture taken before
    /// the rename. See `crate::workload_ids`.
    fn id(&self) -> &'static str;

    /// One line saying what path this workload isolates
    fn summary(&self) -> &'static str;

    /// How this workload's samples are taken
    fn timing(&self) -> Timing;

    /// Whether the hotpath layer may run this workload
    ///
    /// `hotpath` emits one profile per process, so attributing a profile to a workload means one
    /// instrumented run per workload that opts in. Every workload opting in would make the
    /// attribution layers cost as much as the whole rest of a capture, for profiles that mostly
    /// repeat each other.
    fn profiles(&self) -> bool;

    /// Whether the stage layer may run this workload
    ///
    /// Asked separately from [`Workload::profiles`] because the two instrumented layers answer
    /// different questions at the same price. A hotpath profile attributes process time to scopes,
    /// and a second workload's profile mostly repeats the first's - which is why one workload opts
    /// into it. A stage breakdown attributes *one query's* latency to nineteen points along its
    /// path, and the thing worth learning from it is which of those points grows with the row
    /// width. That is a question about the same workload at several widths, so the two lists are
    /// not the same list and pretending they were meant the stage layer could only ever see one.
    ///
    /// Defaults to whatever a workload told the hotpath layer, so every workload that opted into
    /// attribution before this existed still gets both.
    fn stage_profiles(&self) -> bool {
        self.profiles()
    }

    /// Whether this workload's reads are expected to find rows
    ///
    /// The harness refuses a run that timed reads and retrieved nothing, since that is almost
    /// always a read arm over an empty table reporting how fast the server finds nothing. An
    /// arm that reads keys it never wrote, on purpose, says so here and is let through
    /// ([F41](../../../docs/src/features/read-consistency.md)).
    fn expects_rows(&self) -> bool {
        true
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan;

    /// Puts the data this workload reads into the server, before anything is timed
    ///
    /// **Nothing here is measured.** The wall clock and every sample belong to [`Workload::run`],
    /// so a read workload's numbers describe reading and not the writing that had to happen first.
    /// That separation is the reason this is a phase of its own rather than the first half of
    /// `run`: `insert_unsorted` reports the cost of inserting because inserting is its subject,
    /// and `get_resident` must not, even though it inserts exactly the same rows.
    ///
    /// The default seeds nothing, which is right for any workload whose subject is the write path.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        // a workload that writes what it measures has nothing to set up
        let _ = ctx;
        Box::pin(async { Ok(()) })
    }

    /// Runs this workload and hands back what it measured
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>>;

    /// The fault this workload asks the harness to inject while it runs, if any
    ///
    /// Only an arm that places peers can ask for one, since the fault is done to a peer the
    /// harness started; the harness refuses one on any other arm. The default asks for none,
    /// which is every arm but the failover one
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for, which decides the schedule
    fn fault(&self, scale: Scale) -> Option<FaultSpec> {
        // an ordinary arm runs against a cluster nothing happens to
        let _ = scale;
        None
    }

    /// Whether the harness samples the returning node's catch-up after the fault's restart
    ///
    /// Only meaningful on an arm with a fault whose node comes back; the catch-up arms ask for
    /// it and the record gains `cluster.catchup`
    /// ([F43](../../../docs/src/features/node-recovery.md)).
    fn catchup(&self) -> bool {
        false
    }

    /// The repair this workload asks the harness to run in the background, if any
    ///
    /// Only the background arm asks for one, and the record gains `cluster.background`
    /// ([F44](../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for, which decides the schedule
    fn background(&self, scale: Scale) -> Option<BackgroundSpec> {
        // an ordinary arm runs against a cluster nothing scrubs
        let _ = scale;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::Measurement;
    use std::time::Duration;

    /// A measurement pools samples and counters from several workers under the same names
    #[test]
    fn absorbing_pools_both_halves() {
        let mut left = Measurement::default();
        left.record("get", Duration::from_millis(1));
        left.count("retrieved", 10);
        let mut right = Measurement::default();
        right.record("get", Duration::from_millis(3));
        right.record("insert", Duration::from_millis(5));
        right.count("retrieved", 5);
        right.count("inserted", 7);
        left.absorb(right);
        // the shared operation pooled, and the one only the other worker saw came across
        assert_eq!(left.ops["get"].len(), 2);
        assert_eq!(left.ops["insert"].len(), 1);
        // counters summed rather than being overwritten
        assert_eq!(left.counters["retrieved"], 15);
        assert_eq!(left.counters["inserted"], 7);
    }
}
