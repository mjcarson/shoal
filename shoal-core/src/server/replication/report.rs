//! What a shard says about its tablet groups, for readiness, the artifact and the fixture
//!
//! A shard posts a [`ShardReplication`] to the control thread on every detector tick and
//! answers one on request; the control thread folds every shard's into the readiness view's
//! `replication` block and the `Replication` admin read, which is the "lag" record
//! [C9](../../../../docs/src/distributed/operations.md) asks for and what a capture writes
//! down ([F40](../../../../docs/src/features/replication.md)). The verbs are the fixture's:
//! a digest at a common boundary, a forced rotation, a forced compaction, a stalled group.

use serde::{Deserialize, Serialize};

use crate::shared::identity::{GroupId, ShardAddr, TableId};

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
    /// Log entries the installed snapshots covered: the boundary less what was applied before
    ///
    /// What lets a catch-up be split by path: the applied position grows by this through
    /// snapshots and by the rest through the log.
    #[serde(default)]
    pub entries_installed: u64,
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
        self.entries_installed += other.entries_installed;
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
    /// Groups built at open whose checkpoint or archives had no log behind them
    ///
    /// A durable member that lost its WAL: what it acknowledged is gone, and the leader feeds
    /// it again from its log or a snapshot rather than stopping
    /// ([Resolved #99](../../../../docs/src/appendix/resolved/durable-log-reversion.md)).
    pub log_lost: u64,
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
    /// What the shard's snapshots have done ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(default)]
    pub snapshots: SnapshotStats,
    /// What the shard's storage has seen of its own integrity ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub integrity: IntegrityStats,
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
    /// What the node's snapshots have done, folded over its shards
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(default)]
    pub snapshots: SnapshotStats,
    /// How many groups are installing a snapshot right now
    #[serde(default)]
    pub installing: usize,
    /// What the node's storage has seen of its own integrity, folded over its shards
    /// ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub integrity: IntegrityStats,
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
            lag_max: shards.iter().map(ShardReplication::lag_max).max().unwrap_or(0),
            pending_bytes: shards.iter().map(|shard| shard.pending_bytes).sum(),
            volatile_bytes: shards.iter().map(|shard| shard.volatile_bytes).sum(),
            unknown_outcomes: shards.iter().map(|shard| shard.unknown_outcomes).sum(),
            rejected: shards.iter().map(|shard| shard.rejected).sum(),
            reads: shards.iter().fold(ReadStats::default(), |mut folded, shard| {
                folded.absorb(&shard.reads);
                folded
            }),
            snapshots: shards.iter().fold(SnapshotStats::default(), |mut folded, shard| {
                folded.absorb(&shard.snapshots);
                folded
            }),
            installing: shards
                .iter()
                .map(|shard| shard.groups.iter().filter(|group| group.installing).count())
                .sum(),
            integrity: shards.iter().fold(IntegrityStats::default(), |mut folded, shard| {
                folded.absorb(&shard.integrity);
                folded
            }),
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
}
