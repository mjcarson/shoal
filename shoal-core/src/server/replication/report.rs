//! What a shard says about its tablet groups, for readiness, the artifact and the fixture
//!
//! A shard posts a [`ShardReplication`] to the control thread on every detector tick and
//! answers one on request; the control thread folds every shard's into the readiness view's
//! `replication` block and the `Replication` admin read, which is the "lag" record
//! [C9](../../../../docs/src/distributed/operations.md) asks for and what a capture writes
//! down ([F40](../../../../docs/src/features/replication.md)). The verbs are the fixture's:
//! a digest at a common boundary, a forced rotation, a forced compaction, a stalled group.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::shared::identity::{GroupId, ShardAddr, TableId};
use crate::shared::protocol::stats::{QUERY_OPS, WriteCounters};

/// One group as its hosting shard sees it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupReport {
    /// The group
    pub group: GroupId,
    /// The table it serves
    pub table: TableId,
    /// The name the schema spells the table as
    pub table_name: String,
    /// How many tablets it serves
    pub tablets: u32,
    /// Which tablets it serves, ascending
    pub tablet_ids: Vec<u16>,
    /// Its members, primary first
    pub members: Vec<ShardAddr>,
    /// The leader this shard knows, if any
    pub leader: Option<ShardAddr>,
    /// Whether this shard leads it
    pub is_leader: bool,
    /// The last index this shard applied
    pub applied: u64,
    /// The last index this shard has committed
    pub committed: u64,
    /// The last index in this shard's log
    pub last_log: u64,
    /// The index the table's archives are complete to
    pub checkpoint: u64,
    /// Whether the checkpoint file on disk names `checkpoint` yet
    ///
    /// The index moves when a compaction lands and the file follows on a task of its own; a
    /// crash in between opens from the checkpoint before
    /// ([Resolved #115](../../../../docs/src/appendix/resolved/retry-sidecar-crash-window.md)).
    #[serde(default)]
    pub checkpoint_durable: bool,
    /// The index the log is purged to
    pub purged: u64,
    /// Bytes proposed through this shard and not yet answered
    pub pending_bytes: usize,
    /// Whether the group's log lives in memory alone
    pub volatile: bool,
    /// Whether the group's handle is up
    pub up: bool,
    /// Whether a snapshot is being installed for it, during which its tablets serve no `One` read
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(default)]
    pub installing: bool,
    /// The voters of the group's committed membership, as this shard's handle has it
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub voters: Vec<ShardAddr>,
    /// Whether this shard hosts the group as a move's learner
    #[serde(default)]
    pub learner: bool,
    /// Why this shard's copy is quarantined, if it is ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub quarantined: Option<crate::server::control::repair::QuarantineReason>,
    /// The bytes the table's archives hold for the group's tablets on this shard
    ///
    /// What a plan weighs a set by; a volatile group holds no archives and reports zero,
    /// which the planner counts as a set of no measured size
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    #[serde(default)]
    pub bytes: u64,
    /// Why this copy's `RaftCore` is gone, if it is
    ///
    /// A core that panicked answers nothing and moves nothing; the shard's sweep finds it and
    /// says so here, and `up` is false for as long as it is
    /// ([Resolved #109](../../../../docs/src/appendix/resolved/volatile-majority-loss.md)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_dead: Option<String>,
    /// Whether this copy stopped applying on a partition it could not read
    ///
    /// Such a copy is down and quarantined as unreadable until its leader repairs it, and is
    /// not counted as starting ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md)).
    #[serde(default)]
    pub stalled: bool,
    /// The floor this copy's vote is held to, if its log was lost and it has not been fed past it
    ///
    /// Such a copy grants no vote to a candidate behind what it held
    /// ([Resolved #176](../../../../docs/src/appendix/resolved/unreadable-voter-log.md)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor: Option<u64>,
    /// The term this shard's copy of the group is at
    ///
    /// Climbs by one per election; a copy standing for an election nobody grants climbs it
    /// alone, which is what a test of a returning leader or an isolated member reads
    /// ([Resolved #103](../../../../docs/src/appendix/resolved/returning-leader.md)).
    #[serde(default)]
    pub term: u64,
    /// The partitions the table's archives hold for the group's tablets on this shard
    ///
    /// Counted in the same pass over the archive map as `bytes`, so it lags an insert until
    /// the compactor archives it ([F52](../../../../docs/src/features/cluster-stats.md)).
    #[serde(default)]
    pub partitions: u64,
    /// How many of those partitions are chains, a base with fragments over it
    /// ([F61](../../../../docs/src/features/fragmented-partitions.md))
    #[serde(default)]
    pub chained: u64,
    /// The rows and bytes this shard's copy has applied for the group since the shard started
    /// ([F52](../../../../docs/src/features/cluster-stats.md))
    #[serde(default)]
    pub writes: WriteCounters,
}

/// What a shard's snapshots have done since it started
///
/// Counted on both ends of a transfer and folded over the node into
/// [`NodeReplication::snapshots`], which the `Replication` admin read reports and the
/// catch-up arms' captures carry ([F43](../../../../docs/src/features/node-recovery.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SnapshotStats {
    /// Snapshot files cut on this shard
    pub built: u64,
    /// Transfers this shard completed as the sender
    pub sent: u64,
    /// Snapshots installed on this shard, durably
    pub installed: u64,
    /// Bytes sent on the bulk lane
    pub bytes_sent: u64,
    /// Bytes accepted into a partial on this shard
    pub bytes_received: u64,
    /// Chunks accepted into a partial
    pub chunks: u64,
    /// Chunks that arrived for bytes already held
    pub duplicate_chunks: u64,
    /// Chunks that arrived past the prefix held, or for a stream not being assembled
    pub dropped_chunks: u64,
    /// Streams resumed from a held prefix rather than started from zero
    pub resumed: u64,
    /// Transfers this shard gave up on as the sender
    pub aborted: u64,
    /// Installs redone at open from a pending marker
    pub redone: u64,
    /// Purges forced by the retention budget, one per group per sweep that was over it
    #[serde(default)]
    pub forced: u64,
    /// Forced purges not asked of a group because a member was taking a snapshot of it, one per
    /// group per sweep, within `hold_bytes`
    #[serde(default)]
    pub forced_held: u64,
    /// Log entries the installed snapshots covered: the boundary less what was applied before
    ///
    /// What lets a catch-up be split by path: the applied position grows by this through
    /// snapshots and by the rest through the log.
    #[serde(default)]
    pub entries_installed: u64,
    /// Begins refused because the shard was installing as many streams as it may at once
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default)]
    pub refused_budget: u64,
    /// Streams this shard began to assemble, every begin it took: each one's sender spends its
    /// budget's first bucket on it (item 293)
    #[serde(default)]
    pub begun: u64,
    /// Begins refused because the storage was short of the disk reserve
    #[serde(default)]
    pub refused_reserve: u64,
    /// Partial snapshots dropped because their senders gave up on them
    /// ([#194](../../../../docs/src/appendix/resolved/abandoned-partial-snapshots.md))
    #[serde(default)]
    pub abandoned: u64,
    /// The most streams the shard was assembling at once since it started
    ///
    /// Folded over a node as the largest of its shards', not their sum
    #[serde(default)]
    pub peak_streams: u64,
    /// Nanoseconds the sender spent waiting on its byte budget, in all
    #[serde(default)]
    pub budget_wait_ns: u64,
}

impl SnapshotStats {
    /// Fold another shard's counters into these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &SnapshotStats) {
        self.built += other.built;
        self.sent += other.sent;
        self.installed += other.installed;
        self.bytes_sent += other.bytes_sent;
        self.bytes_received += other.bytes_received;
        self.chunks += other.chunks;
        self.duplicate_chunks += other.duplicate_chunks;
        self.dropped_chunks += other.dropped_chunks;
        self.resumed += other.resumed;
        self.aborted += other.aborted;
        self.redone += other.redone;
        self.forced += other.forced;
        self.forced_held += other.forced_held;
        self.entries_installed += other.entries_installed;
        self.refused_budget += other.refused_budget;
        self.begun += other.begun;
        self.refused_reserve += other.refused_reserve;
        self.abandoned += other.abandoned;
        self.peak_streams = self.peak_streams.max(other.peak_streams);
        self.budget_wait_ns += other.budget_wait_ns;
    }
}

/// What a shard's storage has seen of its own integrity
///
/// Counted on the shard - the archive maps count the reads, the loop counts what it found at
/// open - and folded over the node into [`NodeReplication::integrity`], which the `Replication`
/// admin read reports and the fixture's `GROUPS` view carries
/// ([F44](../../../../docs/src/features/repair.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IntegrityStats {
    /// Archive records read whose payload did not hash to its checksum
    pub checksum_failures: u64,
    /// Archive records read from an archive with no checksums, which nothing could verify
    pub unverified_reads: u64,
    /// Copies quarantined on this shard since it started, by a checksum failure or a verdict
    #[serde(default)]
    pub quarantined: u64,
    /// Groups built at open whose checkpoint or archives had no log behind them
    ///
    /// A durable member that lost its WAL: what it acknowledged is gone, and the leader feeds
    /// it again from its log or a snapshot rather than stopping
    /// ([Resolved #99](../../../../docs/src/appendix/resolved/durable-log-reversion.md)).
    pub log_lost: u64,
    /// Scrubs applied on this shard: canonical cuts taken at a committed boundary
    #[serde(default)]
    pub scrubs: u64,
    /// Bytes the scrubs' tasks read from the archives
    #[serde(default)]
    pub scrub_bytes: u64,
    /// Partitions the scrubs hashed, resident and archived
    #[serde(default)]
    pub scrub_partitions: u64,
}

impl IntegrityStats {
    /// Fold another shard's counters into these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &IntegrityStats) {
        self.checksum_failures += other.checksum_failures;
        self.unverified_reads += other.unverified_reads;
        self.log_lost += other.log_lost;
        self.quarantined += other.quarantined;
        self.scrubs += other.scrubs;
        self.scrub_bytes += other.scrub_bytes;
        self.scrub_partitions += other.scrub_partitions;
    }
}

/// What one shard reports about every group it hosts
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ShardReplication {
    /// The shard
    pub shard: usize,
    /// Every group, in group order
    pub groups: Vec<GroupReport>,
    /// Bytes proposed through this shard and not yet answered, across every group
    pub pending_bytes: usize,
    /// Bytes every volatile group's log holds together
    pub volatile_bytes: usize,
    /// How many segments the shard's WAL holds
    pub segments: usize,
    /// The sealed segments handed to a compactor and not yet merged by every table in them
    ///
    /// What the fixture reads to catch a segment between two merges
    /// ([Resolved #104](../../../../docs/src/appendix/resolved/segments-recompacted-after-restart.md)).
    #[serde(default)]
    pub compacting: Vec<u64>,
    /// Proposals answered unknown since the shard started
    pub unknown_outcomes: u64,
    /// Proposals refused since the shard started
    pub rejected: u64,
    /// What the shard's reads have waited on and dropped
    /// ([F41](../../../../docs/src/features/read-consistency.md))
    #[serde(default)]
    pub reads: ReadStats,
    /// How long the shard's strong reads waited for their barriers, and its rounds' appends took
    /// ([F99](../../../../docs/src/features/strong-read-rounds.md))
    #[serde(default)]
    pub rounds: RoundStats,
    /// How long the shard's committed writes waited for its own copies to apply them before they
    /// were answered, and how each wait ended (item 291)
    #[serde(default)]
    pub apply_waits: ApplyWaits,
    /// What the shard's snapshots have done ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(default)]
    pub snapshots: SnapshotStats,
    /// What the shard's storage has seen of its own integrity ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub integrity: IntegrityStats,
    /// Bytes of rows the shard holds in memory, as its eviction counts them
    #[serde(default)]
    pub memory_bytes: u64,
    /// The shard's eviction budget
    #[serde(default)]
    pub memory_budget: u64,
    /// Bytes the shard's archive maps' indexes hold, estimated from their capacities
    #[serde(default)]
    pub archive_map_bytes: u64,
    /// Bytes the shard's tables' partition indexes hold, estimated from their capacities
    #[serde(default)]
    pub table_index_bytes: u64,
    /// Bytes the shard's WAL index of its retained entries holds, estimated
    #[serde(default)]
    pub wal_index_bytes: u64,
    /// Bytes the shard's eviction list holds, an entry per evictable partition, estimated
    #[serde(default)]
    pub lru_bytes: u64,
    /// What this shard's recovery found in the records of the groups it leads, and what its
    /// drivers did, by pool ([F88](../../../../docs/src/features/recovery-and-moves.md))
    #[serde(default)]
    pub recovery: std::collections::BTreeMap<String, crate::shared::protocol::stats::RecoveryFigures>,
    /// What the node's object executors did with the erasure code, as this shard read it: the
    /// node's figures, which every shard reports alike
    /// ([F90](../../../../docs/src/features/erasure-coded-pools.md))
    #[serde(default)]
    pub codec: crate::shared::protocol::stats::CodecFigures,
    /// The rows this shard evicted under memory pressure since it started, by what began each
    /// pass ([F104](../../../../docs/src/features/memory-in-a-capture.md))
    #[serde(default)]
    pub evictions: crate::shared::protocol::stats::EvictionFigures,
    /// What this shard's walk pages did since it started, by class, with the node's scan budget
    /// and pace as this shard read them
    /// ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load))
    #[serde(default)]
    pub scans: crate::shared::protocol::stats::ScanFigures,
    /// What the node's caches of read units did, as this shard read them: the node's figures,
    /// which every shard reports alike ([F104](../../../../docs/src/features/memory-in-a-capture.md))
    #[serde(default)]
    pub object_cache: crate::shared::protocol::stats::CacheReport,
    /// The node's object work in flight as this shard read it, beside this shard's own object
    /// tasks: the store's figures every shard reports alike, and `tasks`, `tasks_refused` and
    /// `puts_given_up` this shard's alone ([F108](../../../../docs/src/features/object-work-in-flight.md))
    #[serde(default)]
    pub objects: crate::shared::protocol::stats::ObjectWork,
    /// What each bucket holds in the groups this shard leads, and what its coordinators answered
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default)]
    pub buckets: std::collections::BTreeMap<String, crate::shared::protocol::stats::BucketFigures>,
    /// Move drivers running on this shard, which its concurrency counts
    /// ([Resolved #197](../../../../docs/src/appendix/resolved/retired-driver-holds-slot.md))
    #[serde(default)]
    pub driving_moves: u32,
    /// Whether this shard's groups stand for election by its own rule now: never while every
    /// replication link is down ([Resolved #106](../../../../docs/src/appendix/resolved/isolated-member-term-inflation.md))
    /// or its disk is under the append reserve ([Resolved #156](../../../../docs/src/appendix/resolved/wal-failure-stops-the-node.md))
    #[serde(default)]
    pub stands: bool,
    /// WAL batches written and synced since the shard started, one `fdatasync` each
    #[serde(default)]
    pub wal_syncs: u64,
    /// Bytes those batches held
    #[serde(default)]
    pub wal_bytes: u64,
    /// Appends those batches carried, one per flush a group asked for
    #[serde(default)]
    pub wal_appends: u64,
    /// Microseconds the WAL spent writing and syncing those batches
    #[serde(default)]
    pub wal_sync_micros: u64,
    /// How many of those batches fell in each size bucket (`wal::SYNC_SIZE_BOUNDS`)
    #[serde(default)]
    pub wal_sync_sizes: Vec<u64>,
    /// What the shard's clients were answered and how long they waited
    /// ([F65](../../../../docs/src/features/query-figures-home-tab.md))
    #[serde(default)]
    pub queries: QueryCounters,
    /// The hops the shard took for its clients' queries
    /// ([F74](../../../../docs/src/features/client-routing.md))
    #[serde(default)]
    pub hops: crate::shared::protocol::stats::HopCounters,
    /// Where the replication lanes the shard accepted, took in and served were read
    /// ([F84](../../../../docs/src/features/replication-lane-handoff.md))
    #[serde(default)]
    pub lanes: crate::shared::protocol::stats::ReplicationLanes,
    /// What the shard's clients cancelled, and the work and bytes that saved
    /// ([F75](../../../../docs/src/features/client-cancel.md))
    #[serde(default)]
    pub cancels: crate::shared::protocol::stats::CancelCounters,
}

/// What a shard's clients were answered since it started, by kind, and how long they waited
///
/// Counted by the shard a client connected to, whichever shards served its queries, so a node's
/// shards summed count every client query once ([`crate::server::shard::meter`]). Every array
/// is in the order of [`QUERY_OPS`](crate::shared::protocol::stats::QUERY_OPS).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryCounters {
    /// Answers written, by kind
    pub answers: [u64; QUERY_OPS.len()],
    /// Bytes of those answers, by kind
    pub bytes_out: [u64; QUERY_OPS.len()],
    /// Bytes of the bundles the clients sent
    pub bytes_in: u64,
    /// How many timed answers fell in each latency bucket
    /// ([`crate::server::shard::meter::bucket_of`]), by kind, the empty buckets past a kind's
    /// slowest wait left off
    pub latency: [Vec<u64>; QUERY_OPS.len()],
}

impl QueryCounters {
    /// What these counters gained since an earlier reading of them, a counter below its
    /// earlier reading read whole
    ///
    /// A counter going backwards means the shard started again, which counts from zero.
    ///
    /// # Arguments
    ///
    /// * `prev` - The earlier reading
    #[must_use]
    pub fn since(&self, prev: &QueryCounters) -> QueryCounters {
        // a counter that went backwards belongs to a shard that started again
        let gained = |now: u64, then: u64| now.checked_sub(then).unwrap_or(now);
        QueryCounters {
            answers: std::array::from_fn(|op| gained(self.answers[op], prev.answers[op])),
            bytes_out: std::array::from_fn(|op| gained(self.bytes_out[op], prev.bytes_out[op])),
            bytes_in: gained(self.bytes_in, prev.bytes_in),
            latency: std::array::from_fn(|op| {
                self.latency[op]
                    .iter()
                    .enumerate()
                    .map(|(bucket, now)| {
                        gained(*now, prev.latency[op].get(bucket).copied().unwrap_or(0))
                    })
                    .collect()
            }),
        }
    }

    /// Add another shard's counters to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &QueryCounters) {
        // every counter is summed on its own
        for op in 0..self.answers.len() {
            self.answers[op] = self.answers[op].saturating_add(other.answers[op]);
            self.bytes_out[op] = self.bytes_out[op].saturating_add(other.bytes_out[op]);
            // the buckets grow to the longest either holds
            let buckets = &mut self.latency[op];
            if buckets.len() < other.latency[op].len() {
                buckets.resize(other.latency[op].len(), 0);
            }
            for (total, count) in buckets.iter_mut().zip(&other.latency[op]) {
                *total = total.saturating_add(*count);
            }
        }
        self.bytes_in = self.bytes_in.saturating_add(other.bytes_in);
    }
}

/// What a shard's reads have cost and dropped since it started
///
/// Counted on the shard whether or not it is a cluster node: a standalone node has gathers to
/// expire and shares to drop late, and a cluster node has barriers and session waits on top.
/// Folded over the node into [`NodeReplication::reads`], read by the `Replication` admin
/// operation and the fixture's `GATHERS` verb, and written into a capture's facts
/// ([F41](../../../../docs/src/features/read-consistency.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReadStats {
    /// Read barriers this shard obtained, one per group a strong read touched
    pub barriers: u64,
    /// Barriers that had to be asked of a leader on another shard
    pub barrier_hops: u64,
    /// Nanoseconds spent waiting for barriers, in all
    pub barrier_wait_ns_total: u64,
    /// The longest one barrier took, in nanoseconds
    pub barrier_wait_ns_max: u64,
    /// Nanoseconds spent waiting for this replica to apply through a barrier or a token
    pub apply_wait_ns_total: u64,
    /// The longest one application wait took, in nanoseconds
    pub apply_wait_ns_max: u64,
    /// Reads served past a session token's lower bound
    pub session_waits: u64,
    /// Tokens refused because their lineage is not the one this replica holds
    pub lineage_refusals: u64,
    /// Reads answered `Timeout`: gathers that expired and waits that ran out
    pub timeouts: u64,
    /// Shares that arrived after their gather was answered or for an attempt it moved past
    pub late_shares: u64,
    /// Shares that arrived for a slot already covered
    pub duplicate_shares: u64,
    /// Forwards a link never wrote that were sent again to another holder
    /// ([F42](../../../../docs/src/features/primary-failover.md))
    #[serde(default)]
    pub reroutes: u64,
    /// Forwards a node refused because it no longer serves the tablet
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub stale_refusals: u64,
    /// Queries this shard refused because no group here serves their tablet
    #[serde(default)]
    pub stale_served: u64,
}

impl ReadStats {
    /// Fold another shard's counters into these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &ReadStats) {
        self.barriers += other.barriers;
        self.barrier_hops += other.barrier_hops;
        self.barrier_wait_ns_total += other.barrier_wait_ns_total;
        self.barrier_wait_ns_max = self.barrier_wait_ns_max.max(other.barrier_wait_ns_max);
        self.apply_wait_ns_total += other.apply_wait_ns_total;
        self.apply_wait_ns_max = self.apply_wait_ns_max.max(other.apply_wait_ns_max);
        self.session_waits += other.session_waits;
        self.lineage_refusals += other.lineage_refusals;
        self.timeouts += other.timeouts;
        self.late_shares += other.late_shares;
        self.duplicate_shares += other.duplicate_shares;
        self.reroutes += other.reroutes;
        self.stale_refusals += other.stale_refusals;
        self.stale_served += other.stale_served;
    }

    /// Record one barrier, and whether it hopped
    ///
    /// # Arguments
    ///
    /// * `hopped` - Whether the barrier was asked of another shard's leader
    /// * `wait_ns` - How long it took
    pub fn record_barrier(&mut self, hopped: bool, wait_ns: u64) {
        self.barriers += 1;
        if hopped {
            self.barrier_hops += 1;
        }
        self.barrier_wait_ns_total += wait_ns;
        self.barrier_wait_ns_max = self.barrier_wait_ns_max.max(wait_ns);
    }

    /// Record one wait for this replica to apply
    ///
    /// # Arguments
    ///
    /// * `wait_ns` - How long it took
    pub fn record_apply_wait(&mut self, wait_ns: u64) {
        self.apply_wait_ns_total += wait_ns;
        self.apply_wait_ns_max = self.apply_wait_ns_max.max(wait_ns);
    }
}

/// How long a shard's strong reads waited for their barriers, and how long the rounds' appends
/// took, each as a count of waits by bucket
///
/// A strong read on a group's leader confirms the lead with a round: an empty append to every
/// other voter, answered once the voter's copy has taken it. Every list counts waits in the
/// buckets a client query's wait is counted in ([`crate::server::shard::meter::bucket_of`]),
/// grown only as far as its slowest wait, so a capture can say where a slow strong read's time
/// went ([F99](../../../../docs/src/features/strong-read-rounds.md)). Since item 302 it also counts
/// the barriers this shard obtained as a leader by how it obtained them, inside its lease or with a
/// round, which tells a lease read's rounds from its groups' heartbeats where `sent` cannot
/// ([F100](../../../../docs/src/features/lease-reads.md)).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RoundStats {
    /// Each strong read's barriers, by how long the read waited for them together
    pub barriers: Vec<u64>,
    /// Each empty append this shard sent and had answered, by how long the answer took: a
    /// strong read's round and its groups' heartbeats alike
    pub sent: Vec<u64>,
    /// Each empty append this shard answered, by how long its copy took from the append's
    /// arrival to the answer
    pub answered: Vec<u64>,
    /// Each strong read's barrier this shard obtained as its group's leader inside its lease,
    /// with no round sent: its own reads' and the hops' it answered
    pub by_lease: u64,
    /// Each strong read's barrier this shard obtained as its group's leader with a round of its
    /// own: every barrier where lease reads are off, and a lease read refused. A group whose
    /// leader is its only voter sends no round and is counted here all the same, and a placement
    /// read's round is not counted here
    pub by_round: u64,
}

impl RoundStats {
    /// Count one wait in a list of buckets
    ///
    /// # Arguments
    ///
    /// * `buckets` - The list to count it in
    /// * `wait` - How long it took
    pub fn record(buckets: &mut Vec<u64>, wait: Duration) {
        // the bucket a client query's wait of the same length falls in
        let micros = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        let bucket = crate::server::shard::meter::bucket_of(micros);
        // the list grows only as far as its slowest wait
        if buckets.len() <= bucket {
            buckets.resize(bucket + 1, 0);
        }
        buckets[bucket] = buckets[bucket].saturating_add(1);
    }

    /// Fold another shard's counts into these
    ///
    /// # Arguments
    ///
    /// * `other` - The counts to add
    pub fn absorb(&mut self, other: &RoundStats) {
        // each list summed bucket by bucket, grown to the longer of the two
        for (mine, theirs) in [
            (&mut self.barriers, &other.barriers),
            (&mut self.sent, &other.sent),
            (&mut self.answered, &other.answered),
        ] {
            if mine.len() < theirs.len() {
                mine.resize(theirs.len(), 0);
            }
            for (total, count) in mine.iter_mut().zip(theirs) {
                *total = total.saturating_add(*count);
            }
        }
        // and the barriers by how they were obtained
        self.by_lease = self.by_lease.saturating_add(other.by_lease);
        self.by_round = self.by_round.saturating_add(other.by_round);
    }
}

/// How a committed write's wait for its coordinator's own copy to apply it ended
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyWaitEnd {
    /// The copy applied the write
    Applied,
    /// The copy was installing a snapshot, which applies nothing until it ends
    Installing,
    /// The wait reached its bound, and the write was answered before the copy applied it
    RanOut,
}

/// How long a shard's committed writes waited for its own copies to apply them before they were
/// answered, by bucket, and how each wait ended
///
/// A coordinator answers a committed write once its own copy of the group applied it, at once
/// while the copy installs a snapshot, or at a bound of two heartbeats
/// ([Resolved #146](../../../../docs/src/appendix/resolved/apply-wait-on-a-lagging-copy.md)). The
/// waits are counted in the buckets a client query's wait is counted in
/// ([`crate::server::shard::meter::bucket_of`]), so the part of a write's answer its coordinator's
/// copy cost can be told from its commit and its trips (item 291).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApplyWaits {
    /// Each wait, by how long it took
    pub waits: Vec<u64>,
    /// The waits that reached their bound, each write answered before its copy here applied it
    pub ran_out: u64,
    /// The waits that ended at once since the copy here was installing a snapshot
    pub installing: u64,
}

impl ApplyWaits {
    /// Count one wait
    ///
    /// # Arguments
    ///
    /// * `wait` - How long it took
    /// * `end` - How it ended
    pub fn record(&mut self, wait: Duration, end: ApplyWaitEnd) {
        // the wait in its bucket, as a strong read's barrier is counted
        RoundStats::record(&mut self.waits, wait);
        // and how it ended, where it did not end applied
        match end {
            ApplyWaitEnd::Applied => (),
            ApplyWaitEnd::Installing => self.installing = self.installing.saturating_add(1),
            ApplyWaitEnd::RanOut => self.ran_out = self.ran_out.saturating_add(1),
        }
    }

    /// Fold another shard's counts into these
    ///
    /// # Arguments
    ///
    /// * `other` - The counts to add
    pub fn absorb(&mut self, other: &ApplyWaits) {
        // the waits summed bucket by bucket, grown to the longer of the two
        if self.waits.len() < other.waits.len() {
            self.waits.resize(other.waits.len(), 0);
        }
        for (total, count) in self.waits.iter_mut().zip(&other.waits) {
            *total = total.saturating_add(*count);
        }
        // and how they ended
        self.ran_out = self.ran_out.saturating_add(other.ran_out);
        self.installing = self.installing.saturating_add(other.installing);
    }
}

/// A verb the fixture drives a shard's reads with, on a standalone node or a cluster one
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadVerb {
    /// Hold every share this shard would send for a while, then release them, twice if asked
    HoldShares {
        /// How many milliseconds to hold them
        ms: u64,
        /// Whether to send each held share twice when released
        dup: bool,
    },
    /// Release the held shares now
    ReleaseShares,
    /// How many gathers are resident, and the read counters
    Gathers,
    /// Block this shard's executor for a while, so every group on it falls silent
    ///
    /// The control thread keeps reporting, so the node stays `Up` while its tablets miss their
    /// heartbeats: the shard stall of [C7](../../../../docs/src/distributed/failover.md)
    /// ([F42](../../../../docs/src/features/primary-failover.md)).
    StallShard {
        /// How many milliseconds to block for
        ms: u64,
    },
}

impl ShardReplication {
    /// How many groups this shard leads
    #[must_use]
    pub fn leading(&self) -> usize {
        self.groups.iter().filter(|group| group.is_leader).count()
    }

    /// The largest gap between committed and applied across the groups
    #[must_use]
    pub fn lag_max(&self) -> u64 {
        self.groups
            .iter()
            .map(|group| group.committed.saturating_sub(group.applied))
            .max()
            .unwrap_or(0)
    }
}

/// What every shard of a node reports together, as readiness carries it
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeReplication {
    /// How many groups the node hosts
    pub groups: usize,
    /// How many of them it leads
    pub leading: usize,
    /// The largest committed-to-applied gap on any group
    pub lag_max: u64,
    /// Bytes proposed and not yet answered, across every shard
    pub pending_bytes: usize,
    /// Bytes every volatile group's log holds together
    pub volatile_bytes: usize,
    /// Proposals answered unknown since the shards started
    pub unknown_outcomes: u64,
    /// Proposals refused since the shards started
    pub rejected: u64,
    /// What the node's reads have cost and dropped, folded over its shards
    #[serde(default)]
    pub reads: ReadStats,
    /// How long the node's strong reads waited for their barriers, and its rounds' appends took,
    /// folded over its shards ([F99](../../../../docs/src/features/strong-read-rounds.md))
    #[serde(default)]
    pub rounds: RoundStats,
    /// How long the node's committed writes waited for its own copies to apply them, folded over
    /// its shards (item 291)
    #[serde(default)]
    pub apply_waits: ApplyWaits,
    /// What the node's snapshots have done, folded over its shards
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(default)]
    pub snapshots: SnapshotStats,
    /// How many groups are installing a snapshot right now
    #[serde(default)]
    pub installing: usize,
    /// How many groups have no handle up yet, because they are still starting
    ///
    /// A node whose groups are starting has nothing to lag behind, so `lag_max` alone reads zero
    /// on a node that serves nothing yet; this is what says it is not back
    /// ([Resolved #137](../../../../docs/src/appendix/resolved/upgrade-waits-for-groups.md)).
    #[serde(default)]
    pub starting: usize,
    /// How many of the node's copies stopped applying on a partition they could not read
    /// ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md))
    #[serde(default)]
    pub stalled: usize,
    /// How many of the node's copies are quarantined ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub quarantined: usize,
    /// What the node's storage has seen of its own integrity, folded over its shards
    /// ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub integrity: IntegrityStats,
    /// The rows and bytes the node's copies have applied, folded over its shards
    /// ([F52](../../../../docs/src/features/cluster-stats.md))
    #[serde(default)]
    pub writes: WriteCounters,
    /// The hops the node took for its clients' queries, folded over its shards
    /// ([F74](../../../../docs/src/features/client-routing.md))
    #[serde(default)]
    pub hops: crate::shared::protocol::stats::HopCounters,
    /// Where the node's replication lanes were read, folded over its shards
    /// ([F84](../../../../docs/src/features/replication-lane-handoff.md))
    #[serde(default)]
    pub lanes: crate::shared::protocol::stats::ReplicationLanes,
    /// What the node's clients cancelled, folded over its shards
    /// ([F75](../../../../docs/src/features/client-cancel.md))
    #[serde(default)]
    pub cancels: crate::shared::protocol::stats::CancelCounters,
    /// Every shard's report, in shard order
    pub shards: Vec<ShardReplication>,
}

impl NodeReplication {
    /// Fold every shard's report into the node's
    ///
    /// # Arguments
    ///
    /// * `shards` - The reports, in shard order
    #[must_use]
    pub fn fold(shards: Vec<ShardReplication>) -> Self {
        NodeReplication {
            groups: shards.iter().map(|shard| shard.groups.len()).sum(),
            leading: shards.iter().map(ShardReplication::leading).sum(),
            lag_max: shards
                .iter()
                .map(ShardReplication::lag_max)
                .max()
                .unwrap_or(0),
            pending_bytes: shards.iter().map(|shard| shard.pending_bytes).sum(),
            volatile_bytes: shards.iter().map(|shard| shard.volatile_bytes).sum(),
            unknown_outcomes: shards.iter().map(|shard| shard.unknown_outcomes).sum(),
            rejected: shards.iter().map(|shard| shard.rejected).sum(),
            reads: shards
                .iter()
                .fold(ReadStats::default(), |mut folded, shard| {
                    folded.absorb(&shard.reads);
                    folded
                }),
            rounds: shards
                .iter()
                .fold(RoundStats::default(), |mut folded, shard| {
                    folded.absorb(&shard.rounds);
                    folded
                }),
            apply_waits: shards
                .iter()
                .fold(ApplyWaits::default(), |mut folded, shard| {
                    folded.absorb(&shard.apply_waits);
                    folded
                }),
            snapshots: shards
                .iter()
                .fold(SnapshotStats::default(), |mut folded, shard| {
                    folded.absorb(&shard.snapshots);
                    folded
                }),
            installing: shards
                .iter()
                .map(|shard| shard.groups.iter().filter(|group| group.installing).count())
                .sum(),
            starting: shards
                .iter()
                .map(|shard| {
                    shard
                        .groups
                        .iter()
                        .filter(|group| !group.up && !group.stalled)
                        .count()
                })
                .sum(),
            stalled: shards
                .iter()
                .map(|shard| shard.groups.iter().filter(|group| group.stalled).count())
                .sum(),
            quarantined: shards
                .iter()
                .map(|shard| {
                    shard
                        .groups
                        .iter()
                        .filter(|group| group.quarantined.is_some())
                        .count()
                })
                .sum(),
            integrity: shards
                .iter()
                .fold(IntegrityStats::default(), |mut folded, shard| {
                    folded.absorb(&shard.integrity);
                    folded
                }),
            writes: shards.iter().flat_map(|shard| shard.groups.iter()).fold(
                WriteCounters::default(),
                |mut folded, group| {
                    folded.absorb(&group.writes);
                    folded
                },
            ),
            hops: shards.iter().fold(
                crate::shared::protocol::stats::HopCounters::default(),
                |mut folded, shard| {
                    folded.absorb(&shard.hops);
                    folded
                },
            ),
            lanes: shards.iter().fold(
                crate::shared::protocol::stats::ReplicationLanes::default(),
                |mut folded, shard| {
                    folded.absorb(&shard.lanes);
                    folded
                },
            ),
            cancels: shards.iter().fold(
                crate::shared::protocol::stats::CancelCounters::default(),
                |mut folded, shard| {
                    folded.absorb(&shard.cancels);
                    folded
                },
            ),
            shards,
        }
    }
}

/// A verb the fixture drives a shard's groups with
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationVerb {
    /// The applied state of a table, hashed, with every group's applied index
    Digest {
        /// The table
        table: TableId,
    },
    /// Force the WAL into a new segment
    Rotate,
    /// Hand every resolved segment to the compactors now, and say how many were
    Compact,
    /// Hold back a group's flush completions
    Stall {
        /// The group
        group: GroupId,
    },
    /// Release a group's held completions
    Release {
        /// The group
        group: GroupId,
    },
    /// Release a group's flush completions one at a time, each this many milliseconds after
    /// the last, or lift that with zero: a follower that commits at a rate the test chooses
    /// ([Resolved #129](../../../../docs/src/appendix/resolved/overload-sheds.md))
    Slow {
        /// The group
        group: GroupId,
        /// The delay before each completion, in milliseconds
        delay_ms: u64,
    },
    /// Let this shard's copy of a group stand for election when its leader's lease times out, or
    /// stop it standing: what lets a test arrange which member an election is won by, rather than
    /// time it (item 142)
    Elect {
        /// The group
        group: GroupId,
        /// Whether the copy may stand
        enabled: bool,
    },
    /// Cut a snapshot of a group now, and say what was built
    ///
    /// What the boundary test reads: the manifest of a cut taken between two compactions
    /// ([F43](../../../../docs/src/features/node-recovery.md)).
    Snapshot {
        /// The group
        group: GroupId,
    },
    /// Drop the next committed write replies this shard would send, so a client's answer is lost
    ///
    /// The proposal commits and applies as ever; only the reply to the client is dropped, which
    /// is the lost response a retry under the same identity has to recover from
    /// ([F42](../../../../docs/src/features/primary-failover.md)).
    DropReplies {
        /// How many replies to drop
        n: u64,
    },
    /// Propose a scrub of a group through this shard, which has to lead it, and poll every
    /// member's digest ([F44](../../../../docs/src/features/repair.md))
    Scrub {
        /// The group
        group: GroupId,
    },
    /// Inject a fault into one partition's archived copy, and evict its resident one
    /// ([F44](../../../../docs/src/features/repair.md))
    Fault {
        /// The table
        table: TableId,
        /// The fault
        fault: crate::storage::ArchiveFault,
        /// The partition
        key: u64,
    },
    /// Hand no sealed segment to the compactors until released, or release them
    ///
    /// What leaves entries past a copy's checkpoint in sealed segments, the way a compactor
    /// hundreds of jobs behind does on the lab
    /// ([Resolved #176](../../../../docs/src/appendix/resolved/unreadable-voter-log.md)).
    HoldCompaction {
        /// Whether to hold the handoffs, or release them
        hold: bool,
    },
    /// Every group's placement state on this shard: as of its applied position, as of its
    /// checkpoint, and whether it is unknown ([F88](../../../../docs/src/features/recovery-and-moves.md))
    PlacementState,
    /// Propose a placement command through this shard's copy of the group that holds its
    /// placement group, which has to lead it, and say what applying it produced
    Placement {
        /// The stripe table whose group holds the placement group
        table: crate::shared::identity::TableId,
        /// The command
        command: crate::shared::placement_state::PlacementCommand,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wait is counted in the bucket a client query's wait of the same length is, each list
    /// grows only as far as its slowest wait, and a node folds its shards' lists bucket by bucket
    /// and its barriers by how they were obtained
    #[test]
    fn a_round_is_counted_by_bucket_and_folded() {
        let millisecond = crate::server::shard::meter::bucket_of(1_000);
        // one shard: two barriers and an append sent
        let mut shard = RoundStats::default();
        RoundStats::record(&mut shard.barriers, Duration::from_micros(3));
        RoundStats::record(&mut shard.barriers, Duration::from_millis(1));
        RoundStats::record(&mut shard.sent, Duration::from_micros(3));
        assert_eq!(shard.barriers.len(), millisecond + 1);
        assert_eq!(shard.barriers[3], 1);
        assert_eq!(shard.barriers[millisecond], 1);
        assert_eq!(shard.sent, vec![0, 0, 0, 1]);
        assert!(shard.answered.is_empty());
        // its two barriers: one inside its lease, one with a round
        shard.by_lease = 1;
        shard.by_round = 1;
        // another: a slower append sent, a barrier and an append answered
        let mut other = RoundStats::default();
        RoundStats::record(&mut other.sent, Duration::from_millis(5));
        RoundStats::record(&mut other.barriers, Duration::from_micros(3));
        RoundStats::record(&mut other.answered, Duration::from_micros(200));
        other.by_lease = 1;
        // the node's report folds both
        let node = NodeReplication::fold(vec![
            ShardReplication {
                rounds: shard,
                ..ShardReplication::default()
            },
            ShardReplication {
                shard: 1,
                rounds: other,
                ..ShardReplication::default()
            },
        ]);
        assert_eq!(node.rounds.barriers[3], 2);
        assert_eq!(node.rounds.barriers[millisecond], 1);
        assert_eq!(
            node.rounds.sent.len(),
            crate::server::shard::meter::bucket_of(5_000) + 1
        );
        assert_eq!(node.rounds.sent.iter().sum::<u64>(), 2);
        assert_eq!(node.rounds.answered.iter().sum::<u64>(), 1);
        assert_eq!((node.rounds.by_lease, node.rounds.by_round), (2, 1));
    }

    /// A write's wait for its coordinator's copy is counted in its bucket and by how it ended,
    /// and a node folds its shards' waits bucket by bucket (item 291)
    #[test]
    fn a_writes_apply_wait_is_counted_by_bucket_and_how_it_ended() {
        let two_heartbeats = crate::server::shard::meter::bucket_of(200_000);
        // one shard: a write applied at once, and one answered at the bound
        let mut shard = ApplyWaits::default();
        shard.record(Duration::from_micros(3), ApplyWaitEnd::Applied);
        shard.record(Duration::from_millis(200), ApplyWaitEnd::RanOut);
        assert_eq!(shard.waits.len(), two_heartbeats + 1);
        assert_eq!((shard.waits[3], shard.waits[two_heartbeats]), (1, 1));
        assert_eq!((shard.ran_out, shard.installing), (1, 0));
        // another: a write answered at once over an install
        let mut other = ApplyWaits::default();
        other.record(Duration::from_micros(3), ApplyWaitEnd::Installing);
        // the node's report folds both
        let node = NodeReplication::fold(vec![
            ShardReplication {
                apply_waits: shard,
                ..ShardReplication::default()
            },
            ShardReplication {
                shard: 1,
                apply_waits: other,
                ..ShardReplication::default()
            },
        ]);
        assert_eq!(node.apply_waits.waits[3], 2);
        assert_eq!(node.apply_waits.waits[two_heartbeats], 1);
        assert_eq!((node.apply_waits.ran_out, node.apply_waits.installing), (1, 1));
    }
}
