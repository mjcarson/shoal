//! The tablet map: what every shard and every client holds of the cluster
//!
//! Built by the control thread from the applied control state on every version that moved,
//! pushed whole to every shard as an `Arc`, and handed to every subscribed client as a
//! [`TopologyFrame`] ([C4](../../../docs/src/distributed/tablet-map.md),
//! [F39](../../../docs/src/features/membership.md)). At M3 it is an ordered node list and a
//! rule rather than a table of tablets: tablet `t` belongs to `placement[t % N]` and, on that
//! node, to shard `(t / N) % shards`, which is the rule every ring is built from and the reason
//! a push is a few hundred bytes. Per tablet records arrive when tablets move (M9a).
//!
//! # Invariants
//!
//! **A map is installed whole or not at all.** A shard holds one `Arc<TabletMap>` and swaps it
//! for a newer one between two messages; nothing ever reads a map with half its members in it,
//! and a version at or below the installed one is ignored rather than merged.
//!
//! **Admission and readiness read one function on one map.** Whether a default write is
//! accepted is [`TabletMap::write_admission`], called by the coordinator that admits it and by
//! the readiness view that reports it, on the same `Arc`; the two cannot disagree.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::conf::cluster::{BootstrapPolicy, Consistency};
use super::control::types::{ControlState, MemberHealth, MemberRole};
use super::peer::handshake::{Admission, PeerAddr, Verdict};
use super::ring::Ring;
use super::ServerError;
use crate::shared::identity::{ClusterId, NodeId, TableId};
use crate::shared::protocol::admin::{TopologyFrame, TopologyMember};

/// One member of the cluster, as the map carries it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapMember {
    /// The node
    pub node: NodeId,
    /// Where clients reach it
    pub client: String,
    /// Where data peers reach it
    pub data: String,
    /// Where control peers reach it
    pub control: String,
    /// How many shards it runs
    pub shards: u16,
    /// Whether it votes
    pub role: MemberRole,
    /// Whether it is joining, up or down
    pub health: MemberHealth,
    /// Which start of it the cluster admitted
    pub incarnation: u64,
    /// The shards that have failed on it, by index
    pub shards_failed: Vec<u16>,
}

/// Why a default write cannot be admitted right now
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumShortfall {
    /// How many members are up
    pub have: u32,
    /// How many the write's consistency needs
    pub need: u32,
}

/// The cluster as every shard holds it
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TabletMap {
    /// How many committed changes the topology has seen
    pub version: u64,
    /// The cluster, or none on a joiner that has not adopted one
    pub cluster: Option<ClusterId>,
    /// The control leader, as the node that built this map knew it
    pub leader: Option<NodeId>,
    /// Every member, by node
    pub members: BTreeMap<NodeId, MapMember>,
    /// The nodes tablets are placed over, in placement order; empty before initialization
    pub placement: Vec<NodeId>,
    /// The tables the schema serves, with their stable identities
    pub tables: Vec<(String, TableId)>,
    /// How many replicas each tablet is meant to have
    pub desired_rf: u32,
    /// What a write waits for
    pub write_consistency: Consistency,
    /// What a read is served at
    pub read_consistency: Consistency,
    /// The principals allowed to change the cluster
    pub admins: Vec<String>,
}

impl Default for TabletMap {
    /// The map a node holds before its control plane has told it anything
    fn default() -> Self {
        TabletMap {
            version: 0,
            cluster: None,
            leader: None,
            members: BTreeMap::new(),
            placement: Vec::new(),
            tables: Vec::new(),
            desired_rf: 0,
            write_consistency: Consistency::Quorum,
            read_consistency: Consistency::One,
            admins: Vec::new(),
        }
    }
}

impl TabletMap {
    /// Build the map from the applied control state
    ///
    /// Before an operator initializes a placement, the bootstrapper's map places every tablet
    /// on itself - a one node placement is exactly the standalone ring - and a joiner's places
    /// nothing, which is what "joins with no tablet authority" means.
    ///
    /// # Arguments
    ///
    /// * `state` - The applied state
    /// * `leader` - The control leader the building node knows, if any
    /// * `tables` - The tables the building node serves, used until the state records them
    #[must_use]
    pub fn from_state(
        state: &ControlState,
        leader: Option<NodeId>,
        tables: &[(String, TableId)],
    ) -> Self {
        let members = state
            .members
            .iter()
            .map(|(node, member)| {
                // a node runs fewer shards than a u16 holds; the ring refuses more
                #[allow(clippy::cast_possible_truncation)]
                let shards = member.record.shards as u16;
                (
                    *node,
                    MapMember {
                        node: *node,
                        client: member.record.client.clone(),
                        data: member.record.data.clone(),
                        control: member.record.control.clone(),
                        shards,
                        role: member.role,
                        health: member.health,
                        incarnation: member.record.incarnation,
                        shards_failed: member.shards_failed.clone(),
                    },
                )
            })
            .collect();
        // the placement the operator committed, or the bootstrapper alone until one is
        let placement = match (&state.initialized, state.bootstrapper) {
            (Some(nodes), _) => nodes.clone(),
            (None, Some(bootstrapper)) => vec![bootstrapper],
            (None, None) => Vec::new(),
        };
        let policy = state.policy.as_ref();
        TabletMap {
            version: state.topology_version,
            cluster: state.cluster,
            leader,
            members,
            placement,
            tables: if state.tables.is_empty() {
                tables.to_vec()
            } else {
                state.tables.clone()
            },
            desired_rf: policy.map_or(0, |policy| policy.replication_factor),
            write_consistency: policy.map_or(Consistency::Quorum, |policy| policy.write_consistency),
            read_consistency: policy.map_or(Consistency::One, |policy| policy.read_consistency),
            admins: policy.map_or_else(Vec::new, |policy| policy.admins.clone()),
        }
    }

    /// One member, if the map knows it
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    #[must_use]
    pub fn member(&self, node: NodeId) -> Option<&MapMember> {
        self.members.get(&node)
    }

    /// Where to dial a member and who to expect there
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    #[must_use]
    pub fn peer_addr(&self, node: NodeId) -> Option<PeerAddr> {
        self.members.get(&node).map(|member| PeerAddr {
            node: Some(node),
            data: member.data.clone(),
            control: member.control.clone(),
            shards: member.shards,
        })
    }

    /// How many members are up
    #[must_use]
    pub fn up(&self) -> u32 {
        let up = self
            .members
            .values()
            .filter(|member| member.health == MemberHealth::Up)
            .count();
        u32::try_from(up).unwrap_or(u32::MAX)
    }

    /// How many members a write under the policy needs up
    ///
    /// # Arguments
    ///
    /// * `consistency` - The write's consistency
    #[must_use]
    pub fn quorum_for(&self, consistency: Consistency) -> u32 {
        match consistency {
            Consistency::One => 1,
            Consistency::Quorum => self.desired_rf / 2 + 1,
            Consistency::All => self.desired_rf.max(1),
        }
    }

    /// Whether a default write can be admitted right now, and if not, why
    ///
    /// The one function the coordinator and the readiness view both call. A write under the
    /// cluster's write consistency needs that many members up; a map with no placement admits
    /// nothing, since nothing owns the tablet.
    ///
    /// # Errors
    ///
    /// Says how many are up and how many are needed.
    pub fn write_admission(&self) -> Result<(), QuorumShortfall> {
        let need = self.quorum_for(self.write_consistency);
        let have = self.up();
        if self.placement.is_empty() {
            return Err(QuorumShortfall { have, need });
        }
        if have < need {
            return Err(QuorumShortfall { have, need });
        }
        Ok(())
    }

    /// How many replicas each tablet has: one under a placement, none before one
    #[must_use]
    pub fn active_rf(&self) -> u32 {
        u32::from(!self.placement.is_empty())
    }

    /// Whether this node holds tablets under the placement
    ///
    /// # Arguments
    ///
    /// * `me` - This node
    #[must_use]
    pub fn places(&self, me: NodeId) -> bool {
        self.placement.contains(&me)
    }

    /// The placement as the ring builder takes it: each placed node with its shard count
    #[must_use]
    pub fn placement_counts(&self) -> Vec<(NodeId, u16)> {
        self.placement
            .iter()
            .filter_map(|node| self.members.get(node).map(|member| (*node, member.shards)))
            .collect()
    }

    /// The ring this node routes with under the placement, or none if it is not placed
    ///
    /// # Arguments
    ///
    /// * `me` - This node
    /// * `shards` - How many shards it runs
    ///
    /// # Errors
    ///
    /// Fails if the placement names this node with another shard count.
    pub fn ring_for(&self, me: NodeId, shards: usize) -> Result<Option<Ring>, ServerError> {
        if !self.places(me) {
            return Ok(None);
        }
        Ring::with_placement(shards, &self.placement_counts(), me).map(Some)
    }

    /// The frame a client is handed
    #[must_use]
    pub fn frame(&self) -> TopologyFrame {
        TopologyFrame {
            cluster: self.cluster.unwrap_or_default(),
            version: self.version,
            leader: self.leader,
            members: self
                .members
                .values()
                .map(|member| TopologyMember {
                    node: member.node,
                    client: member.client.clone(),
                    data: member.data.clone(),
                    control: member.control.clone(),
                    shards: member.shards,
                    role: member.role.name().to_string(),
                    health: member.health.name().to_string(),
                    incarnation: member.incarnation,
                    shards_failed: member.shards_failed.clone(),
                })
                .collect(),
            placement: self.placement.clone(),
            desired_rf: self.desired_rf,
            active_rf: self.active_rf(),
            write_consistency: self.write_consistency.as_str().to_string(),
            read_consistency: self.read_consistency.as_str().to_string(),
            tables: self.tables.clone(),
        }
    }

    /// The policy the map carries, in the shape the bootstrap wrote it
    ///
    /// # Arguments
    ///
    /// * `base` - The rest of the policy, which the map does not carry
    #[must_use]
    pub fn policy_with(&self, base: &BootstrapPolicy) -> BootstrapPolicy {
        BootstrapPolicy {
            replication_factor: self.desired_rf,
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
            admins: self.admins.clone(),
            ..base.clone()
        }
    }
}

/// The map one executor holds, shared between everything on it that reads the cluster
///
/// A shard's links, its listener and its router all read the same cell, and the shard's
/// message loop is the only writer; the control thread has one of its own.
#[derive(Clone, Default)]
pub struct MapCell {
    /// The installed map
    inner: Rc<RefCell<Arc<TabletMap>>>,
}

impl MapCell {
    /// Hold a map
    ///
    /// # Arguments
    ///
    /// * `map` - The map to start with
    #[must_use]
    pub fn new(map: Arc<TabletMap>) -> Self {
        MapCell {
            inner: Rc::new(RefCell::new(map)),
        }
    }

    /// The installed map
    #[must_use]
    pub fn get(&self) -> Arc<TabletMap> {
        self.inner.borrow().clone()
    }

    /// Install a newer map, ignoring one at or below the installed version
    ///
    /// # Arguments
    ///
    /// * `map` - The map to install
    ///
    /// Returns whether it was installed.
    pub fn install(&self, map: Arc<TabletMap>) -> bool {
        let mut inner = self.inner.borrow_mut();
        if map.version <= inner.version && inner.version != 0 {
            return false;
        }
        *inner = map;
        true
    }
}

impl Admission for MapCell {
    /// Judge a peer against the installed map
    fn judge(&self, node: NodeId, incarnation: u64, shards: u16) -> Verdict {
        let map = self.inner.borrow();
        // a map with nobody in it is a node that has been told nothing yet, which trusts the
        // cluster the hello proved rather than refusing every peer it will ever have
        if map.members.is_empty() {
            return Verdict::Member;
        }
        match map.members.get(&node) {
            None => Verdict::Unknown,
            Some(member) if incarnation < member.incarnation => Verdict::Fenced {
                committed: member.incarnation,
            },
            Some(member) if member.shards != shards => Verdict::ShardCount {
                expected: member.shards,
            },
            Some(_) => Verdict::Member,
        }
    }

    /// The cluster the installed map names
    fn cluster(&self) -> Option<ClusterId> {
        self.inner.borrow().cluster
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::conf::Cluster;
    use crate::server::control::types::{ControlCommand, MemberRecord};

    /// A member record for tests
    fn record(node: NodeId, shards: usize, incarnation: u64) -> MemberRecord {
        MemberRecord {
            node,
            client: "c".to_string(),
            data: "127.0.0.1:1".to_string(),
            control: "127.0.0.1:2".to_string(),
            control_core: 0,
            control_shared: false,
            shards,
            incarnation,
        }
    }

    /// A bootstrapped state with the given policy and one member
    fn state_with(policy: crate::server::conf::cluster::BootstrapPolicy) -> (ControlState, NodeId) {
        let mut state = ControlState::default();
        let node = NodeId::mint();
        state.apply(&ControlCommand::Bootstrap {
            cluster: ClusterId::mint(),
            policy,
            member: record(node, 2, 1),
        });
        (state, node)
    }

    /// The quorum rule: One needs one, Quorum a majority of the desired factor, All every one
    #[test]
    fn write_admission_follows_the_policy_and_the_up_count() {
        // one node at the default policy: quorum of three is two, and one is up
        let (state, node) = state_with(Cluster::default().policy());
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.quorum_for(Consistency::One), 1);
        assert_eq!(map.quorum_for(Consistency::Quorum), 2);
        assert_eq!(map.quorum_for(Consistency::All), 3);
        assert_eq!(map.write_admission(), Err(QuorumShortfall { have: 1, need: 2 }));
        // the bootstrapper places on itself before any initialization
        assert_eq!(map.placement, vec![node]);
        assert_eq!(map.active_rf(), 1);
        // at rf one, one is enough
        let (state, node) = state_with(Cluster::default().replication_factor(1).policy());
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.write_admission(), Ok(()));
        // three up under rf three admits, and a down one is not counted
        let (mut state, node) = state_with(Cluster::default().policy());
        let (b, c) = (NodeId::mint(), NodeId::mint());
        for other in [b, c] {
            state.apply(&ControlCommand::Admit(record(other, 1, 1)));
            state.apply(&ControlCommand::ObserveMember(record(other, 1, 1)));
        }
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.up(), 3);
        assert_eq!(map.write_admission(), Ok(()));
        state.apply(&ControlCommand::SetHealth {
            node: c,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: None,
        });
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.up(), 2);
        assert_eq!(map.write_admission(), Ok(()));
        state.apply(&ControlCommand::SetHealth {
            node: b,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: None,
        });
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.write_admission(), Err(QuorumShortfall { have: 1, need: 2 }));
    }

    /// A joiner holds no tablets before an initialization, and the bootstrapper holds them all
    #[test]
    fn a_joiner_is_unplaced_until_initialized() {
        let (mut state, node) = state_with(Cluster::default().replication_factor(1).policy());
        let joiner = NodeId::mint();
        state.apply(&ControlCommand::Admit(record(joiner, 3, 1)));
        state.apply(&ControlCommand::ObserveMember(record(joiner, 3, 1)));
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert!(map.ring_for(joiner, 3).expect("a ring").is_none());
        let ring = map.ring_for(node, 2).expect("a ring").expect("the bootstrapper is placed");
        // a one node placement is the standalone ring
        assert_eq!(ring.shards.len(), 2);
        // after initialization both are placed, in the order given
        state.apply(&ControlCommand::Initialize {
            op: uuid::Uuid::new_v4(),
            principal: String::new(),
            expected_version: state.topology_version,
            nodes: vec![joiner, node],
            tables: vec![("Row".to_string(), TableId::of("Row"))],
        });
        let map = TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.placement, vec![joiner, node]);
        assert_eq!(map.placement_counts(), vec![(joiner, 3), (node, 2)]);
        assert!(map.ring_for(joiner, 3).expect("a ring").is_some());
        // and a node claiming another shard count than the placement's is refused
        assert!(map.ring_for(node, 4).is_err());
        // the frame carries the client endpoints and the tables
        let frame = map.frame();
        assert_eq!(frame.members.len(), 2);
        assert_eq!(frame.tables[0].0, "Row");
        assert_eq!(frame.placement, vec![joiner, node]);
        assert_eq!(frame.write_consistency, "quorum");
    }

    /// A map cell installs only newer versions, and judges peers by what it holds
    #[test]
    fn a_map_cell_installs_newer_maps_and_judges_by_them() {
        let (mut state, node) = state_with(Cluster::default().policy());
        let cell = MapCell::default();
        // an empty cell trusts anybody, which is the joiner before it has been told anything
        assert_eq!(cell.judge(NodeId::mint(), 1, 1), Verdict::Member);
        assert!(cell.install(Arc::new(TabletMap::from_state(&state, None, &[]))));
        assert_eq!(cell.get().version, 1);
        // the same version again is not installed
        assert!(!cell.install(Arc::new(TabletMap::from_state(&state, None, &[]))));
        // a member is judged by its record
        assert_eq!(cell.judge(node, 1, 2), Verdict::Member);
        assert_eq!(cell.judge(node, 0, 2), Verdict::Fenced { committed: 1 });
        assert_eq!(cell.judge(node, 1, 3), Verdict::ShardCount { expected: 2 });
        assert_eq!(cell.judge(NodeId::mint(), 1, 1), Verdict::Unknown);
        // a newer version is installed, an older is not
        let joiner = NodeId::mint();
        state.apply(&ControlCommand::Admit(record(joiner, 1, 1)));
        let newer = Arc::new(TabletMap::from_state(&state, None, &[]));
        assert!(cell.install(newer.clone()));
        assert!(!cell.install(Arc::new(TabletMap {
            version: 1,
            ..(*newer).clone()
        })));
        assert_eq!(cell.get().version, 2);
    }
}
