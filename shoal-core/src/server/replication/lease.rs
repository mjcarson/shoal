//! Where a group's authority stands, as one shard's handle sees it
//!
//! openraft's leader never steps down on its own: isolated, it keeps believing it leads for as
//! long as nobody with a higher term reaches it, and what its lease decides is only whether it
//! will take a *new* write - `client_write` on a leader whose lease lapsed is refused with an
//! empty forward hint ([F42](../../../../docs/src/features/primary-failover.md)). Waiting for a
//! leader on such a handle is satisfied at once, since the handle names itself, which made the
//! proposal and barrier loops spin until their deadline and answer `OutcomeUnknown` for a write
//! nothing had accepted. This module classifies the handle's state once so a caller can answer
//! `NotLeader` at a lapsed lease before anything is appended, poll while a fresh lease starts,
//! hop to a leader elsewhere, or wait for an election - and nothing else.
//!
//! The lease is openraft's own: `election_timeout_max` since the last quorum acknowledgement,
//! and a voter alone is its own quorum, so a single-voter group never lapses.

use std::time::Duration;

use openraft::Raft;

use super::machine::GroupMachine;
use super::types::DataConfig;
use crate::server::database::ShoalDatabase;
use crate::shared::identity::ShardAddr;

/// Where a group's authority stands for one shard's handle on it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lease {
    /// This shard leads, and a quorum acknowledged it within the lease
    Leads,
    /// This shard leads, and no quorum has acknowledged it yet: the lease has not started
    NotStarted,
    /// This shard believes it leads, and no quorum has acknowledged it within the lease
    ///
    /// A write proposed here would be refused by openraft with an empty hint; a barrier could
    /// not complete its heartbeat round. Answered by name rather than waited out.
    Lapsed,
    /// Another member leads, as far as this shard knows
    Elsewhere(ShardAddr),
    /// No leader is known: an election is due or under way
    Electing,
}

impl Lease {
    /// Judge one handle's authority now
    ///
    /// # Arguments
    ///
    /// * `raft` - This shard's handle on the group
    /// * `me` - This shard's address
    #[must_use]
    pub fn of<D: ShoalDatabase>(raft: &Raft<DataConfig, GroupMachine<D>>, me: ShardAddr) -> Self {
        match raft.as_leader() {
            Ok(leader) => {
                // a voter alone is its own quorum, as openraft judges it
                if raft.voter_ids().count() <= 1 {
                    return Lease::Leads;
                }
                let lease = Duration::from_millis(raft.config().election_timeout_max);
                match leader.last_quorum_acked() {
                    None => Lease::NotStarted,
                    Some(acked) if acked.elapsed() > lease => Lease::Lapsed,
                    Some(_) => Lease::Leads,
                }
            }
            Err(forward) => match forward.leader_node.or(forward.leader_id) {
                // a hint naming this shard is a vote not yet committed here
                Some(leader) if leader == me => Lease::NotStarted,
                Some(leader) => Lease::Elsewhere(leader),
                None => Lease::Electing,
            },
        }
    }

    /// The lease openraft applies to the group, for a message
    ///
    /// # Arguments
    ///
    /// * `raft` - This shard's handle on the group
    #[must_use]
    pub fn length<D: ShoalDatabase>(raft: &Raft<DataConfig, GroupMachine<D>>) -> Duration {
        Duration::from_millis(raft.config().election_timeout_max)
    }
}
