//! What the control group carries: its type configuration, its commands and its state
//!
//! openraft is generic over one type that names everything else, and [`ControlConfig`] is that
//! type for the control group. The application data is a [`ControlCommand`], the response is a
//! [`ControlResponse`], a node is named by its [`NodeId`] and described by a [`MemberRecord`],
//! and the runtime is the glommio one. The rest - term, leader id, vote, entry, responder,
//! batching, error source - are openraft's defaults.
//!
//! [`ControlState`] is what applying the committed log produces. At M1 it holds the cluster's
//! identity, one member, the bootstrap policy and a topology version that moves once per
//! applied command. It is the seed of the tablet map
//! ([C4](../../../../docs/src/distributed/tablet-map.md)); it is not the tablet map, which has
//! nothing to place until there is a second node.

use std::collections::BTreeMap;
use std::fmt;

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};

use super::runtime::GlommioRuntime;
use crate::server::conf::cluster::BootstrapPolicy;
use crate::shared::identity::{ClusterId, NodeId};

declare_raft_types!(
    /// The control group's type configuration
    pub ControlConfig:
        D = ControlCommand,
        R = ControlResponse,
        NodeId = NodeId,
        Node = MemberRecord,
        AsyncRuntime = GlommioRuntime,
);

/// What the group knows about one member
///
/// The endpoints are what the member advertises, and at M1 none of them is bound: the client
/// endpoint is the one the shards serve on, and the data and control endpoints are what M2 and
/// M3 will listen on. They are recorded now so the record has its shape before anything fills
/// it, and so a restart that changed an address is visibly a re-observation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MemberRecord {
    /// The node
    pub node: NodeId,
    /// Where clients reach it
    pub client: String,
    /// Where data peers would reach it
    pub data: String,
    /// Where control peers would reach it
    pub control: String,
    /// The cpu its control thread runs on
    pub control_core: usize,
    /// Whether that cpu's physical core is shared with a shard
    pub control_shared: bool,
    /// How many shards it runs
    pub shards: usize,
}

impl fmt::Display for MemberRecord {
    /// The node and where it is reached
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.node, self.client)
    }
}

/// A command the group commits
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ControlCommand {
    /// Create the cluster: its identity, its policy and its first member
    ///
    /// Applied exactly once, at the bootstrap. A second one against a state that already has a
    /// cluster is refused by the state machine rather than applied.
    Bootstrap {
        /// The cluster being created
        cluster: ClusterId,
        /// The policy it is created with
        policy: BootstrapPolicy,
        /// The node creating it
        member: MemberRecord,
    },
    /// Record what a member currently advertises
    ///
    /// Written by a member at every start, so an address that changed is a committed fact and
    /// not something a peer discovers by failing to connect.
    ObserveMember(MemberRecord),
}

impl fmt::Display for ControlCommand {
    /// Name the command, which is what openraft's traces print
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlCommand::Bootstrap { cluster, member, .. } => {
                write!(f, "Bootstrap({cluster} by {member})")
            }
            ControlCommand::ObserveMember(member) => write!(f, "ObserveMember({member})"),
        }
    }
}

/// What applying a command produced
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    /// Applied, and the topology is now at this version
    Applied {
        /// The topology version after the command
        topology_version: u64,
    },
    /// Refused, with the reason
    ///
    /// A refusal is still a committed entry - the log does not skip it - but it changes nothing,
    /// and the topology version does not move.
    Refused {
        /// Why
        reason: String,
    },
}

/// The applied state of the control group
///
/// Every field is what the committed log says, and nothing else writes to it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ControlState {
    /// The cluster, once bootstrapped
    pub cluster: Option<ClusterId>,
    /// How many commands have changed the topology; zero before the bootstrap
    pub topology_version: u64,
    /// Every member the group knows, by node
    pub members: BTreeMap<NodeId, MemberRecord>,
    /// The policy the cluster was bootstrapped with
    pub policy: Option<BootstrapPolicy>,
}

impl ControlState {
    /// Apply one command, returning what it produced
    ///
    /// Pure: the same state and command always produce the same result, which is what lets a
    /// replay of the log rebuild the state exactly.
    ///
    /// # Arguments
    ///
    /// * `command` - The command to apply
    pub fn apply(&mut self, command: &ControlCommand) -> ControlResponse {
        match command {
            // the one command that creates the cluster
            ControlCommand::Bootstrap {
                cluster,
                policy,
                member,
            } => {
                // a cluster that exists is never created again, whatever asks
                if let Some(existing) = self.cluster {
                    return ControlResponse::Refused {
                        reason: format!(
                            "the cluster is already {existing}; a second bootstrap ({cluster}) \
                             would fork it"
                        ),
                    };
                }
                self.cluster = Some(*cluster);
                self.policy = Some(policy.clone());
                self.members.insert(member.node, member.clone());
                self.topology_version += 1;
                ControlResponse::Applied {
                    topology_version: self.topology_version,
                }
            }
            // a member saying where it is
            ControlCommand::ObserveMember(member) => {
                // an observation before the bootstrap describes a member of nothing
                if self.cluster.is_none() {
                    return ControlResponse::Refused {
                        reason: "no cluster has been bootstrapped to observe a member of".to_string(),
                    };
                }
                // the same record again is not a topology change
                if self.members.get(&member.node) == Some(member) {
                    return ControlResponse::Applied {
                        topology_version: self.topology_version,
                    };
                }
                self.members.insert(member.node, member.clone());
                self.topology_version += 1;
                ControlResponse::Applied {
                    topology_version: self.topology_version,
                }
            }
        }
    }

    /// The replication factor the policy asks for, or zero before the bootstrap
    pub fn desired_rf(&self) -> u32 {
        self.policy
            .as_ref()
            .map_or(0, |policy| policy.replication_factor)
    }

    /// The replication factor the members can actually give, which is how many there are
    ///
    /// Capped at the desired factor: three members and a factor of three is three, one member
    /// and a factor of three is one. Nothing places tablets yet, so this is the count of nodes
    /// that could hold a replica rather than the count that does.
    pub fn active_rf(&self) -> u32 {
        let members = u32::try_from(self.members.len()).unwrap_or(u32::MAX);
        members.min(self.desired_rf())
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlCommand, ControlResponse, ControlState, MemberRecord};
    use crate::server::conf::Cluster;
    use crate::shared::identity::{ClusterId, NodeId};

    /// A member record for tests
    fn member(node: NodeId, client: &str) -> MemberRecord {
        MemberRecord {
            node,
            client: client.to_string(),
            data: String::new(),
            control: String::new(),
            control_core: 0,
            control_shared: false,
            shards: 2,
        }
    }

    /// A bootstrap creates the cluster once, and the topology version moves with each change
    #[test]
    fn a_bootstrap_is_applied_once() {
        let mut state = ControlState::default();
        let cluster = ClusterId::mint();
        let node = NodeId::mint();
        let policy = Cluster::default().policy();
        // before anything, an observation is of nothing
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(member(node, "a"))),
            ControlResponse::Refused { .. }
        ));
        // the bootstrap is the first change
        let bootstrap = ControlCommand::Bootstrap {
            cluster,
            policy: policy.clone(),
            member: member(node, "a"),
        };
        assert_eq!(
            state.apply(&bootstrap),
            ControlResponse::Applied {
                topology_version: 1
            }
        );
        assert_eq!(state.cluster, Some(cluster));
        assert_eq!(state.desired_rf(), 3);
        assert_eq!(state.active_rf(), 1);
        // a second one is refused and changes nothing
        let again = ControlCommand::Bootstrap {
            cluster: ClusterId::mint(),
            policy,
            member: member(NodeId::mint(), "b"),
        };
        assert!(matches!(state.apply(&again), ControlResponse::Refused { .. }));
        assert_eq!(state.cluster, Some(cluster));
        assert_eq!(state.topology_version, 1);
        // the same member observed unchanged is not a change
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(member(node, "a"))),
            ControlResponse::Applied {
                topology_version: 1
            }
        );
        // a member at a new address is
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(member(node, "c"))),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.members[&node].client, "c");
    }
}
