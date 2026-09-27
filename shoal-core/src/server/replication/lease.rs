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

use openraft::{Raft, RaftMetrics, ServerState};
use openraft_rt::WatchReceiver as _;

use super::machine::GroupMachine;
use super::network::ShardNetwork;
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
        // judged under the watch's lock, which nothing in here holds across an await
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        Self::judge(&metrics, me, Self::length(raft))
    }

    /// Judge one handle's authority from a view of its metrics
    ///
    /// # Arguments
    ///
    /// * `metrics` - The handle's metrics
    /// * `me` - This shard's address
    /// * `lease` - The lease openraft applies to the group
    #[must_use]
    pub fn judge(metrics: &RaftMetrics<DataConfig>, me: ShardAddr, lease: Duration) -> Self {
        // judged from the server state rather than from the committed vote alone: a restarted
        // node holds a vote for itself from its last term and leads nothing until it is elected
        // again, and a lease on a vote that is not being led would be polled for nothing
        if metrics.state != ServerState::Leader {
            return match metrics.current_leader {
                Some(leader) if leader != me => Lease::Elsewhere(leader),
                _ => Lease::Electing,
            };
        }
        // a voter alone is its own quorum, as openraft judges it
        if metrics.membership_config.membership().voter_ids().count() <= 1 {
            return Lease::Leads;
        }
        match metrics.last_quorum_acked {
            None => Lease::NotStarted,
            Some(acked) if acked.into_inner().elapsed() > lease => Lease::Lapsed,
            Some(_) => Lease::Leads,
        }
    }

    /// How long a quorum of this leader's group has been silent on the network, once enough of
    /// its members' nodes have been silent for the hop silence that no quorum is left
    ///
    /// openraft's lease is `election_timeout_max`, twice the failover base: ten seconds at the
    /// default base, for which a leader cut off by dropped packets still takes writes it cannot
    /// commit, and each waits out the write timeout. What the rest of the node already takes a
    /// peer's silence to mean is judged here per member node: a voter whose node has answered
    /// nothing on the replication lane for the hop silence is out of reach, and a leader left
    /// with fewer than a quorum in reach refuses a write before it appends it, and gives up on
    /// one it took ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md)).
    ///
    /// It is judged on the nodes, never on openraft's `last_quorum_acked`: under a saturated
    /// load a follower's acknowledgements queue behind its appends for seconds while its node
    /// goes on talking, and a leader judged quiet by its acknowledgements refused and abandoned
    /// writes a healthy group would have committed. A voter alone, a follower, and a member never
    /// heard from are never quiet.
    ///
    /// # Arguments
    ///
    /// * `raft` - This shard's handle on the group
    /// * `network` - This shard's network, which knows when each peer node was last heard from
    #[must_use]
    pub fn quorum_quiet<D: ShoalDatabase>(
        raft: &Raft<DataConfig, GroupMachine<D>>,
        network: &ShardNetwork,
    ) -> Option<Duration> {
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        Self::quiet_of(&metrics, network)
    }

    /// How long a quorum of this leader's group has been silent, from a view of its metrics
    ///
    /// # Arguments
    ///
    /// * `metrics` - The handle's metrics
    /// * `network` - This shard's network
    #[must_use]
    pub fn quiet_of(metrics: &RaftMetrics<DataConfig>, network: &ShardNetwork) -> Option<Duration> {
        // only a leader answers to a quorum
        if metrics.state != ServerState::Leader {
            return None;
        }
        let me = metrics.id;
        let silence = network.hop_silence();
        let membership = metrics.membership_config.membership();
        let mut voters = 0usize;
        let mut reached = 0usize;
        let mut quiet = Duration::ZERO;
        // every voter this node's network has heard from lately is in reach, this one included
        for voter in membership.voter_ids() {
            voters += 1;
            if voter == me || voter.node == me.node {
                reached += 1;
                continue;
            }
            match network.node_silent_for(voter.node, silence) {
                Some(silent) => quiet = quiet.max(silent),
                None => reached += 1,
            }
        }
        // a quorum is a majority of the voters
        (reached < voters / 2 + 1).then_some(quiet)
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
