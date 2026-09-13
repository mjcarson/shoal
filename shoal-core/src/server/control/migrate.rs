//! What a replica migration records: the committed data configuration and the move's progress
//!
//! A move is an administrative operation committed to the control log
//! ([F45](../../../../docs/src/features/replica-migration.md)): the record names the replica
//! set holding a tablet, the member leaving it and the member replacing it, and every group
//! the set serves - one per table - with the phase its driver last committed. The record rides
//! the pushed map until it is done, so the destination's shard builds each group as a learner,
//! each group's leader drives its transition, and the source's shard retires its copies once
//! the new configuration is published.
//!
//! A [`DataConfiguration`] is what a finished transition leaves behind: the replica set that
//! no longer follows the placement rule, keyed by its tablets. The rule is the default and a
//! configuration overrides it for exactly its tablets, so a map with no configurations is the
//! map every milestone before this one built.
//!
//! # Invariants
//!
//! **A group's identity is minted once and pinned.** The identity is the hash of the table
//! and the replica set the rule derived at initialization, and a move keeps it: the log, the
//! WAL frames, the checkpoints and the retry tables all name it, and a new identity per
//! configuration would orphan every one of them.
//!
//! **A configuration is never rolled backward.** The group's committed membership is the
//! truth; the record and the configuration follow it, and a late driver's word about an
//! earlier configuration changes nothing.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::identity::{GroupId, NodeId, ShardAddr};

/// How many move records the control state keeps, newest last
pub const KEPT_MOVES: usize = 64;

/// A replica set that no longer follows the placement rule: what a finished move committed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataConfiguration {
    /// The tablets the set serves, ascending
    pub tablets: Vec<u16>,
    /// Its members, the primary first
    pub members: Vec<ShardAddr>,
    /// The log index of the uniform membership each group of the set committed, by group
    pub configs: BTreeMap<GroupId, u64>,
    /// The topology version it was published at
    pub published_at: u64,
}

impl DataConfiguration {
    /// Whether the configuration covers a tablet
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn covers(&self, tablet: u16) -> bool {
        self.tablets.binary_search(&tablet).is_ok()
    }
}

/// Where a move, or one group under it, stands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MovePhase {
    /// Waiting for another transition on the same replica set to finish
    Queued {
        /// The operation it waits behind
        behind: Uuid,
    },
    /// Recorded, and nobody has driven it yet
    Planned,
    /// The destination is a learner of the group and is being fed
    Learner,
    /// The destination's log is within the catch-up lag of the leader's
    CatchingUp,
    /// The joint membership is being written
    Reconfiguring,
    /// The uniform membership naming the target is committed
    Configured,
    /// The destination has applied the uniform membership: it counts, and can serve
    Activated,
    /// The configuration is in the control state and the map carries it
    Published,
    /// The source's copy is retiring
    Retiring,
    /// Nothing more will happen under this operation
    Done,
}

impl MovePhase {
    /// Where this phase stands in the order a move goes through
    #[must_use]
    pub fn rank(&self) -> u8 {
        match self {
            MovePhase::Queued { .. } => 0,
            MovePhase::Planned => 1,
            MovePhase::Learner => 2,
            MovePhase::CatchingUp => 3,
            MovePhase::Reconfiguring => 4,
            MovePhase::Configured => 5,
            MovePhase::Activated => 6,
            MovePhase::Published => 7,
            MovePhase::Retiring => 8,
            MovePhase::Done => 9,
        }
    }

    /// The phase's name, as a record's timings and a log line spell it
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            MovePhase::Queued { .. } => "queued",
            MovePhase::Planned => "planned",
            MovePhase::Learner => "learner",
            MovePhase::CatchingUp => "catching_up",
            MovePhase::Reconfiguring => "reconfiguring",
            MovePhase::Configured => "configured",
            MovePhase::Activated => "activated",
            MovePhase::Published => "published",
            MovePhase::Retiring => "retiring",
            MovePhase::Done => "done",
        }
    }
}

/// What a move came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveOutcome {
    /// The set is served by the target and the source's copy is gone
    Moved,
    /// The move could not be completed; the group is where its committed membership says
    Failed {
        /// Why
        reason: String,
    },
}

/// What a group's transition cost
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveStats {
    /// Snapshot bytes sent to the destination
    pub bytes: u64,
    /// The destination's log position once it had caught up: what it was fed, by snapshot and
    /// by log together, since a learner starts from nothing
    pub entries: u64,
    /// How long each phase took, in milliseconds, by the phase's name
    pub phase_ms: BTreeMap<String, u64>,
    /// When the current phase was entered, in milliseconds since the epoch; zero before any
    #[serde(default)]
    pub since: u64,
}

/// One group's progress under a move
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMove {
    /// Where it stands
    pub phase: MovePhase,
    /// The node driving it, if one has said so
    pub driver: Option<NodeId>,
    /// The log index of the uniform membership naming the target, once committed
    pub config: Option<u64>,
    /// What the transition cost so far
    #[serde(default)]
    pub stats: MoveStats,
    /// What it came to, once done
    pub outcome: Option<MoveOutcome>,
}

impl Default for GroupMove {
    /// A group nobody has driven
    fn default() -> Self {
        GroupMove {
            phase: MovePhase::Planned,
            driver: None,
            config: None,
            stats: MoveStats::default(),
            outcome: None,
        }
    }
}

impl GroupMove {
    /// Whether nothing more will happen to this group under its operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == MovePhase::Done
    }

    /// Whether the group's uniform membership naming the target is committed
    #[must_use]
    pub fn is_activated(&self) -> bool {
        self.phase.rank() >= MovePhase::Activated.rank()
    }
}

/// A move operation, as the control state records it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveRecord {
    /// The operation
    pub op: Uuid,
    /// The tablets the replica set serves, ascending
    pub tablets: Vec<u16>,
    /// The member leaving the set
    pub from: ShardAddr,
    /// The member replacing it, on the shard chosen for it
    pub to: ShardAddr,
    /// The set as it was when the move was recorded, the primary first
    pub expected: Vec<ShardAddr>,
    /// The set as it will be: `expected` with `from` replaced by `to` in place
    pub target: Vec<ShardAddr>,
    /// Where the record as a whole stands: queued, planned, published or done
    pub phase: MovePhase,
    /// Every group of the set, one per table, and where each stands
    pub groups: BTreeMap<GroupId, GroupMove>,
    /// Who asked
    pub principal: String,
    /// The topology version the request was applied at
    pub requested_at: u64,
    /// What it came to, once done
    pub outcome: Option<MoveOutcome>,
}

impl MoveRecord {
    /// Whether nothing more will happen under this operation
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == MovePhase::Done
    }

    /// Whether the record waits behind another transition
    #[must_use]
    pub fn is_queued(&self) -> bool {
        matches!(self.phase, MovePhase::Queued { .. })
    }

    /// Whether the configuration naming the target is in the control state
    #[must_use]
    pub fn is_published(&self) -> bool {
        self.phase.rank() >= MovePhase::Published.rank()
    }

    /// Whether the move covers a tablet
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn covers(&self, tablet: u16) -> bool {
        self.tablets.binary_search(&tablet).is_ok()
    }

    /// The configuration this move publishes, once every group is activated
    ///
    /// # Arguments
    ///
    /// * `published_at` - The topology version it is published at
    #[must_use]
    pub fn configuration(&self, published_at: u64) -> DataConfiguration {
        DataConfiguration {
            tablets: self.tablets.clone(),
            members: self.target.clone(),
            configs: self
                .groups
                .iter()
                .filter_map(|(group, progress)| progress.config.map(|config| (*group, config)))
                .collect(),
            published_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The phases rank in the order a move goes through them, and a record's flags follow its phase
    #[test]
    fn move_phases_rank_in_order() {
        let phases = [
            MovePhase::Queued { behind: Uuid::nil() },
            MovePhase::Planned,
            MovePhase::Learner,
            MovePhase::CatchingUp,
            MovePhase::Reconfiguring,
            MovePhase::Configured,
            MovePhase::Activated,
            MovePhase::Published,
            MovePhase::Retiring,
            MovePhase::Done,
        ];
        // strictly ascending, and every name distinct
        assert!(phases.windows(2).all(|pair| pair[0].rank() < pair[1].rank()));
        let mut names: Vec<&str> = phases.iter().map(MovePhase::name).collect();
        names.dedup();
        assert_eq!(names.len(), phases.len());
        let group = GroupMove {
            phase: MovePhase::Configured,
            driver: None,
            config: Some(9),
            stats: MoveStats::default(),
            outcome: None,
        };
        assert!(!group.is_done());
        assert!(!group.is_activated());
        // a configuration carries every group's uniform index, and covers its tablets alone
        let record = MoveRecord {
            op: Uuid::nil(),
            tablets: vec![1, 4, 7],
            from: ShardAddr::from(1),
            to: ShardAddr::from(4),
            expected: vec![ShardAddr::from(1), ShardAddr::from(2), ShardAddr::from(3)],
            target: vec![ShardAddr::from(4), ShardAddr::from(2), ShardAddr::from(3)],
            phase: MovePhase::Planned,
            groups: [(GroupId(1), group)].into_iter().collect(),
            principal: String::new(),
            requested_at: 3,
            outcome: None,
        };
        assert!(record.covers(4));
        assert!(!record.covers(5));
        let configuration = record.configuration(11);
        assert_eq!(configuration.configs.get(&GroupId(1)), Some(&9));
        assert_eq!(configuration.members, record.target);
        assert!(configuration.covers(7));
        assert!(!configuration.covers(2));
    }
}
