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
    /// Proposals answered unknown since the shard started
    pub unknown_outcomes: u64,
    /// Proposals refused since the shard started
    pub rejected: u64,
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
}
