//! The tablet map: what every shard and every client holds of the cluster
//!
//! Built by the control thread from the applied control state on every version that moved,
//! pushed whole to every shard as an `Arc`, and handed to every subscribed client as a
//! [`TopologyFrame`] ([C4](../../../docs/src/distributed/tablet-map.md),
//! [F39](../../../docs/src/features/membership.md)). At M3 it is an ordered node list and a
//! rule rather than a table of tablets: tablet `t` belongs to `placement[t % N]` and, on that
//! node, to shard `(t / N) % shards`, which is the rule every ring is built from and the reason
//! a push is a few hundred bytes. Since [F45](../../../docs/src/features/replica-migration.md)
//! a replica set that moved is a [`DataConfiguration`] carried beside the rule: the rule is the
//! default and a configuration overrides it for exactly its tablets, so a map with no moves
//! behind it is still the list and the rule.
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
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::conf::cluster::{BootstrapPolicy, Consistency, DurationSpec};
use super::control::migrate::{DataConfiguration, MoveRecord};
use super::control::repair::{QuarantinedCopy, RepairRecord};
use super::control::types::{ControlState, MemberHealth, MemberPhase, MemberRole};
use super::peer::handshake::{Admission, PeerAddr, Verdict};
use super::hosting::Hosting;
use super::ring::{Ring, TABLET_COUNT};
use super::shard::ShardContact;
use super::ServerError;
use crate::shared::identity::{ClusterId, GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::admin::{ConfiguredSet, MoveSummary, QuarantinedMember, TopologyFrame, TopologyMember};
use uuid::Uuid;

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
    /// The copies it holds that are quarantined, which reads are routed around
    /// ([F44](../../../docs/src/features/repair.md))
    #[serde(default)]
    pub quarantined: Vec<QuarantinedCopy>,
    /// Whether it is a plain member, leaving, removing or removed
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default)]
    pub phase: MemberPhase,
}

impl MapMember {
    /// The one name C3's six-state machine gives this member
    #[must_use]
    pub const fn state_name(&self) -> &'static str {
        match self.phase {
            MemberPhase::Member => self.health.name(),
            other => other.name(),
        }
    }
}

/// Why a default write cannot be admitted right now
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumShortfall {
    /// How many members are up
    pub have: u32,
    /// How many the write's consistency needs
    pub need: u32,
}

/// One tablet group a shard hosts: a table over an ordered replica set, and the tablets in it
///
/// Tablets whose replicas land on the same ordered list of shard addresses share one group
/// ([F40](../../../docs/src/features/replication.md)), so at equal shard counts a node hosts
/// `nodes × shards` groups per table rather than a group per tablet - the shape the Q13 spike
/// pointed at, since per-group heartbeats do not coalesce. The leader is preferred on the
/// placement primary, which is the first member.
///
/// The identity is the hash of the table and the members the rule derived at initialization,
/// and a move keeps it: a set that moved has the same identity over different members
/// ([F45](../../../docs/src/features/replica-migration.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpec {
    /// The group's identity, the hash of the table and the members the rule first derived
    pub id: GroupId,
    /// The table it serves
    pub table: TableId,
    /// Its members, the primary first
    pub members: Vec<ShardAddr>,
    /// The tablets it serves, ascending
    pub tablets: Vec<u16>,
    /// Which of this node's shards hosts it
    pub mine: u16,
    /// Whether this node hosts it as a learner: the destination of a move not yet published
    ///
    /// A learner's shard builds the group so the leader's replication reaches a `Raft`, and
    /// never initializes it or stands for its election
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    pub learner: bool,
    /// The move the group is under, if one is not done
    pub transition: Option<Uuid>,
}

impl GroupSpec {
    /// The member on this node
    #[must_use]
    pub fn me(&self, node: NodeId) -> ShardAddr {
        ShardAddr::new(node, self.mine)
    }

    /// Whether this node's member is the placement primary
    #[must_use]
    pub fn is_primary(&self, node: NodeId) -> bool {
        self.members.first().is_some_and(|primary| primary.node == node && primary.shard == self.mine)
    }
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
    /// The tables whose reads are served at a level of their own
    /// ([F41](../../../../docs/src/features/read-consistency.md))
    pub table_read_policy: BTreeMap<TableId, Consistency>,
    /// The principals allowed to change the cluster
    pub admins: Vec<String>,
    /// The repair operations not yet done on every group, which a group's leader drives
    /// ([F44](../../../docs/src/features/repair.md))
    #[serde(default)]
    pub repairs: Vec<RepairRecord>,
    /// The failover base every tablet group derives its timers from, in milliseconds
    ///
    /// The policy's `primary_failover_after`, carried so every node's groups miss a leader at
    /// the pace the cluster agreed rather than the pace its own file says; zero from a map
    /// built before the field existed, which leaves the node's own setting in force
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    #[serde(default)]
    pub primary_failover_ms: u64,
    /// The replica sets that no longer follow the placement rule, by first tablet ascending
    /// ([F45](../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub configurations: Vec<DataConfiguration>,
    /// The move operations not yet done, which the destination learns from and the source
    /// retires under ([F45](../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub moves: Vec<MoveRecord>,
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
            table_read_policy: BTreeMap::new(),
            admins: Vec::new(),
            repairs: Vec::new(),
            primary_failover_ms: 0,
            configurations: Vec::new(),
            moves: Vec::new(),
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
                        quarantined: member.quarantined.clone(),
                        phase: member.phase,
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
            table_read_policy: state.table_read_policy.clone(),
            admins: policy.map_or_else(Vec::new, |policy| policy.admins.clone()),
            repairs: state.repairs.values().filter(|record| !record.is_done()).cloned().collect(),
            // truncation cannot happen: a failover base is seconds, not weeks
            #[allow(clippy::cast_possible_truncation)]
            primary_failover_ms: policy.map_or(0, |policy| policy.primary_failover_after.duration().as_millis() as u64),
            configurations: state.configurations.values().cloned().collect(),
            moves: state.moves.values().filter(|record| !record.is_done()).cloned().collect(),
        }
    }

    /// Whether a member is up, as this map has it
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    #[must_use]
    pub fn is_up(&self, node: NodeId) -> bool {
        self.members.get(&node).is_some_and(|member| member.health == MemberHealth::Up)
    }

    /// Whether a member's copy of a tablet is quarantined for any table
    ///
    /// Routing is per tablet and not per table, so a holder with any table's copy of the tablet
    /// quarantined is routed around for every table; the refusal at the holder is exact
    /// ([F44](../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `tablet` - The tablet
    #[must_use]
    pub fn is_quarantined(&self, node: NodeId, tablet: usize) -> bool {
        // truncation cannot happen: a tablet id is twelve bits
        #[allow(clippy::cast_possible_truncation)]
        let tablet = tablet as u16;
        self.members
            .get(&node)
            .is_some_and(|member| member.quarantined.iter().any(|copy| copy.tablets.contains(&tablet)))
    }

    /// The replica a node holding no copy of a tablet sends to: the first holder that is up
    ///
    /// The primary when it is up, else the next replica that is, else the primary anyway -
    /// health is routing advice, never authority, and a holder that is down gets the query
    /// refused where the link is rather than here. With `avoid` set the answer never names
    /// that node and is none rather than the primary when nobody else is up
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    /// * `avoid` - A holder not to name: the one whose link just went down
    #[must_use]
    pub fn preferred_holder(&self, tablet: usize, avoid: Option<NodeId>) -> Option<ShardAddr> {
        let replicas = self.replicas_of(tablet);
        // a holder whose copy is quarantined is passed over like one that is down
        let up = replicas
            .iter()
            .find(|replica| Some(replica.node) != avoid && self.is_up(replica.node) && !self.is_quarantined(replica.node, tablet))
            .copied();
        match (up, avoid) {
            (Some(holder), _) => Some(holder),
            (None, None) => replicas.first().copied(),
            (None, Some(_)) => None,
        }
    }

    /// Another holder for every partition of a share, when one node holds them all and is up
    ///
    /// For a forward the link never wrote: the share can go to another replica of its
    /// tablets under the same attempt and slot, since nothing was accepted, but only to one
    /// node and one shard that every partition of it lives on. A share whose partitions have
    /// no such holder in common is refused as before
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    ///
    /// # Arguments
    ///
    /// * `partitions` - The partition keys the share covers, as their hashes
    /// * `me` - This node, which holds none of them or the share would not have been forwarded
    /// * `avoid` - The node the share was forwarded to
    #[must_use]
    pub fn alternate_holder(&self, partitions: &[u64], me: NodeId, avoid: NodeId) -> Option<ShardAddr> {
        let mut common: Option<Vec<ShardAddr>> = None;
        for partition in partitions {
            let tablet = Ring::tablet_of(*partition);
            // the holders of this tablet that are up and are neither the one that failed nor us
            let holders: Vec<ShardAddr> = self
                .replicas_of(tablet)
                .into_iter()
                .filter(|replica| {
                    replica.node != avoid && replica.node != me && self.is_up(replica.node) && !self.is_quarantined(replica.node, tablet)
                })
                .collect();
            common = Some(match common {
                None => holders,
                Some(so_far) => so_far.into_iter().filter(|addr| holders.contains(addr)).collect(),
            });
        }
        common.and_then(|holders| holders.first().copied())
    }

    /// The level a table's reads are served at when a bundle does not say
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    #[must_use]
    pub fn read_level_of(&self, table: TableId) -> Consistency {
        self.table_read_policy
            .get(&table)
            .copied()
            .unwrap_or(self.read_consistency)
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

    /// How many replicas each tablet has: the desired factor or the placement's size,
    /// whichever is smaller; none before a placement
    ///
    /// ~~One under a placement~~ Since [F40](../../../docs/src/features/replication.md) a
    /// placement gives every tablet as many copies as it can up to the policy: three nodes at
    /// a factor of three hold every tablet everywhere, one node at the same factor holds one
    /// copy and reports the gap.
    #[must_use]
    pub fn active_rf(&self) -> u32 {
        if self.placement.is_empty() {
            return 0;
        }
        let nodes = u32::try_from(self.placement.len()).unwrap_or(u32::MAX);
        self.desired_rf.max(1).min(nodes)
    }

    /// The replicas of a tablet under the placement rule alone, the placement primary first
    ///
    /// Tablet `t` lives on `placement[(t + k) % N]` for `k` below the active factor, and on
    /// each of those nodes on shard `(t / N) % shards` - the same shard the primary rule picks,
    /// so a node's replica of a tablet is on the shard that would own it were the node primary
    /// ([F40](../../../docs/src/features/replication.md)). The nodes are distinct by
    /// construction, since the factor never passes the placement's size
    /// ([C4](../../../docs/src/distributed/tablet-map.md)). This is what a group's identity
    /// is minted from; what serves the tablet now is [`TabletMap::replicas_of`].
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn rule_replicas_of(&self, tablet: usize) -> Vec<ShardAddr> {
        let counts = self.placement_counts();
        let nodes = counts.len();
        if nodes == 0 {
            return Vec::new();
        }
        let copies = self.active_rf() as usize;
        (0..copies)
            .map(|k| {
                let (node, shards) = counts[(tablet + k) % nodes];
                // truncation cannot happen: the modulus is a u16
                #[allow(clippy::cast_possible_truncation)]
                let shard = ((tablet / nodes) % usize::from(shards.max(1))) as u16;
                ShardAddr::new(node, shard)
            })
            .collect()
    }

    /// The configuration overriding the rule for a tablet, if a move left one
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn configuration_of(&self, tablet: usize) -> Option<&DataConfiguration> {
        // truncation cannot happen: a tablet id is twelve bits
        #[allow(clippy::cast_possible_truncation)]
        let tablet = tablet as u16;
        self.configurations.iter().find(|configuration| configuration.covers(tablet))
    }

    /// The move a tablet is under, if one is not done
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn move_of(&self, tablet: usize) -> Option<&MoveRecord> {
        // truncation cannot happen: a tablet id is twelve bits
        #[allow(clippy::cast_possible_truncation)]
        let tablet = tablet as u16;
        self.moves.iter().find(|record| record.covers(tablet))
    }

    /// The member learning a tablet's groups: the destination of a move not yet published
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn learner_of(&self, tablet: usize) -> Option<ShardAddr> {
        self.move_of(tablet)
            .filter(|record| !record.is_queued() && !record.is_published())
            .map(|record| record.to)
    }

    /// The replicas of a tablet, the primary first: the configuration a move left, or the rule
    ///
    /// ~~Tablet `t` lives on `placement[(t + k) % N]`~~ The rule is the default; a replica set
    /// that moved is served by the members its [`DataConfiguration`] names, on the shards it
    /// names, under the identity the rule minted
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn replicas_of(&self, tablet: usize) -> Vec<ShardAddr> {
        match self.configuration_of(tablet) {
            Some(configuration) => configuration.members.clone(),
            None => self.rule_replicas_of(tablet),
        }
    }

    /// The replica set the rule places a tablet in: the rule's members and every tablet under them
    ///
    /// What a move names: the set's tablets share a configuration and an identity per table,
    /// and are moved together ([F45](../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn rule_set_of(&self, tablet: usize) -> (Vec<ShardAddr>, Vec<u16>) {
        let rule = self.rule_replicas_of(tablet);
        if rule.is_empty() {
            return (rule, Vec::new());
        }
        let tablets = (0..TABLET_COUNT)
            .filter(|other| self.rule_replicas_of(*other) == rule)
            // truncation cannot happen: a tablet id is twelve bits
            .map(|other| u16::try_from(other).unwrap_or(u16::MAX))
            .collect();
        (rule, tablets)
    }

    /// Every replica set the rule derives, keyed by the rule's members, with the tablets under it
    ///
    /// The sets a group's identity is minted from; a set that moved is still one set here,
    /// under the members the rule named, and its configuration is read beside it.
    fn rule_sets(&self) -> BTreeMap<Vec<ShardAddr>, Vec<u16>> {
        let mut sets: BTreeMap<Vec<ShardAddr>, Vec<u16>> = BTreeMap::new();
        for tablet in 0..TABLET_COUNT {
            let members = self.rule_replicas_of(tablet);
            if !members.is_empty() {
                // truncation cannot happen: a tablet id is twelve bits
                #[allow(clippy::cast_possible_truncation)]
                sets.entry(members).or_default().push(tablet as u16);
            }
        }
        sets
    }

    /// Every replica set as it is served now, with the tablets under it
    ///
    /// The rule's sets overlaid by the configurations the moves left: what a plan reads to
    /// know which members hold which set, and what a tombstone is judged against
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md)). In first-tablet order.
    #[must_use]
    pub fn rule_sets_served(&self) -> Vec<(Vec<ShardAddr>, Vec<u16>)> {
        let mut sets: Vec<(Vec<ShardAddr>, Vec<u16>)> = self
            .rule_sets()
            .into_values()
            .map(|tablets| (self.replicas_of(usize::from(tablets[0])), tablets))
            .collect();
        sets.sort_by_key(|(_, tablets)| tablets[0]);
        sets
    }

    /// The groups a node hosts, one per table and replica set it holds a member of
    ///
    /// Every node computes the same groups from the same map, and a shard builds only the ones
    /// whose members name its own address. Sorted by identity, so two nodes' lists line up.
    /// A set that moved is hosted by its configuration's members under the rule's identity,
    /// and the destination of a move not yet published hosts the set's groups as a learner
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `me` - This node
    #[must_use]
    pub fn replica_groups(&self, me: NodeId) -> Vec<GroupSpec> {
        let mut groups = Vec::new();
        for (rule, tablets) in self.rule_sets() {
            // the set's tablets share one configuration and one move, since both cover whole sets
            let first = usize::from(tablets[0]);
            let members = self.replicas_of(first);
            let learner = self.learner_of(first).filter(|learner| learner.node == me);
            let transition = self.move_of(first).map(|record| record.op);
            // this node's member, or the learner's shard when it is the destination
            let mine = match members.iter().find(|member| member.node == me) {
                Some(member) => (member.shard, false),
                None => match learner {
                    Some(learner) => (learner.shard, true),
                    None => continue,
                },
            };
            for (_, table) in &self.tables {
                groups.push(GroupSpec {
                    id: GroupId::of(*table, &rule),
                    table: *table,
                    members: members.clone(),
                    tablets: tablets.clone(),
                    mine: mine.0,
                    learner: mine.1,
                    transition,
                });
            }
        }
        groups.sort_by_key(|group| group.id);
        groups
    }

    /// Every group of one table, with its members and its tablets
    ///
    /// What a repair or a move record is filled with at apply: the same sets every node builds
    /// its own groups from, over every node rather than one, each under the identity the rule
    /// minted and the members that serve it now
    /// ([F44](../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    #[must_use]
    pub fn groups_of(&self, table: TableId) -> Vec<(GroupId, Vec<ShardAddr>, Vec<u16>)> {
        let mut groups: Vec<(GroupId, Vec<ShardAddr>, Vec<u16>)> = self
            .rule_sets()
            .into_iter()
            .map(|(rule, tablets)| {
                let members = self.replicas_of(usize::from(tablets[0]));
                (GroupId::of(table, &rule), members, tablets)
            })
            .collect();
        groups.sort_by_key(|(id, _, _)| *id);
        groups
    }

    /// Whether a node holds a replica of a tablet
    ///
    /// # Arguments
    ///
    /// * `me` - The node
    /// * `tablet` - The tablet
    #[must_use]
    pub fn holds(&self, me: NodeId, tablet: usize) -> bool {
        self.replicas_of(tablet).iter().any(|member| member.node == me)
    }

    /// Whether this node holds tablets: under the placement, under a configuration, or as
    /// the learner of a move
    ///
    /// # Arguments
    ///
    /// * `me` - This node
    #[must_use]
    pub fn places(&self, me: NodeId) -> bool {
        self.routing_counts().iter().any(|(node, _)| *node == me)
    }

    /// Every node a ring names: the placement, then every node a configuration or a move
    /// not yet published brings in that the placement does not, each with its shard count
    ///
    /// A member admitted after the placement was initialized is brought in by a move, and
    /// routes and is routed to from then on
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    #[must_use]
    pub fn routing_counts(&self) -> Vec<(NodeId, u16)> {
        let mut counts = self.placement_counts();
        // the nodes the configurations and the moves name, in node order, once each
        let mut extra: Vec<NodeId> = self
            .configurations
            .iter()
            .flat_map(|configuration| configuration.members.iter().map(|member| member.node))
            .chain(self.moves.iter().filter(|record| !record.is_queued()).map(|record| record.to.node))
            .filter(|node| !self.placement.contains(node))
            .collect();
        extra.sort_unstable();
        extra.dedup();
        for node in extra {
            if let Some(member) = self.members.get(&node) {
                counts.push((node, member.shards));
            }
        }
        counts
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
    /// * `hosting` - Its slots, executors, and which executor hosts each slot
    ///
    /// # Errors
    ///
    /// Fails if the placement names this node with another slot count.
    pub fn ring_for(&self, me: NodeId, hosting: &Hosting) -> Result<Option<Ring>, ServerError> {
        if !self.places(me) {
            return Ok(None);
        }
        Ring::with_placement(hosting, &self.routing_counts(), me).map(Some)
    }

    /// The ring this node serves queries with: a tablet it holds a replica of is served locally
    ///
    /// The placement ring routes every tablet to its primary; this one routes a tablet this
    /// node holds a copy of to the shard holding it, and everything else to the primary. A
    /// `One` read is answered from the local copy ([C6](../../../docs/src/distributed/reads.md))
    /// and a write is proposed by it through the group's leader, which the replica knows
    /// whatever the placement says - so an unreachable primary takes no tablet down with it
    /// ([F40](../../../docs/src/features/replication.md)). At a placement of the desired size
    /// every query is served by a local replica.
    ///
    /// A local copy is served by the executor hosting its slot
    /// ([F47](../../../docs/src/features/local-rehome.md)).
    ///
    /// # Arguments
    ///
    /// * `me` - This node
    /// * `hosting` - Its slots, executors, and which executor hosts each slot
    ///
    /// # Errors
    ///
    /// Fails as [`TabletMap::ring_for`] does.
    pub fn read_ring_for(&self, me: NodeId, hosting: &Hosting) -> Result<Option<Ring>, ServerError> {
        if !self.places(me) {
            return Ok(None);
        }
        let mut ring = Ring::with_placement(hosting, &self.routing_counts(), me)?;
        // the executor hosting one of this node's slots, as the ring indexes it
        //
        // truncation cannot happen: an executor count is bounded by a u16 at the ring
        #[allow(clippy::cast_possible_truncation)]
        let host = |slot: u16| hosting.host_of_slot(slot) as u16;
        for tablet in 0..TABLET_COUNT {
            let replicas = self.replicas_of(tablet);
            // the slot this node's copy is on, if it holds one: the rule's, or the
            // configuration's ([F45](../../../docs/src/features/replica-migration.md))
            let local = replicas.iter().find(|replica| replica.node == me).map(|replica| replica.shard);
            // a copy this node holds and may serve is read here; a quarantined one is read
            // elsewhere while another holder is up, and here as the backstop, where the
            // refusal names the quarantine ([F44](../../../docs/src/features/repair.md))
            if let Some(local) = local.filter(|_| !self.is_quarantined(me, tablet)) {
                ring.set_owner(tablet, host(local));
            } else if let Some(holder) = self.preferred_holder(tablet, None) {
                // a tablet this node holds no copy of goes to a holder that is up, which is
                // the primary until the primary is called down
                if holder.node == me {
                    ring.set_owner(tablet, host(holder.shard));
                    continue;
                }
                let contact = ShardContact::Remote {
                    node: holder.node,
                    shard: holder.shard,
                };
                if let Some(index) = ring.index_of(&contact) {
                    ring.set_owner(tablet, index);
                }
            } else if let Some(local) = local {
                ring.set_owner(tablet, host(local));
            }
        }
        Ok(Some(ring))
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
                    phase: member.phase.name().to_string(),
                    state: member.state_name().to_string(),
                    incarnation: member.incarnation,
                    shards_failed: member.shards_failed.clone(),
                    quarantined: member
                        .quarantined
                        .iter()
                        .map(|copy| QuarantinedMember {
                            table: copy.table,
                            group: copy.group.0,
                            tablets: copy.tablets.clone(),
                            reason: copy.reason.as_str().to_string(),
                        })
                        .collect(),
                })
                .collect(),
            placement: self.placement.clone(),
            desired_rf: self.desired_rf,
            active_rf: self.active_rf(),
            write_consistency: self.write_consistency.as_str().to_string(),
            read_consistency: self.read_consistency.as_str().to_string(),
            tables: self.tables.clone(),
            table_read_policy: self
                .table_read_policy
                .iter()
                .filter_map(|(id, level)| {
                    self.tables
                        .iter()
                        .find(|(_, table)| table == id)
                        .map(|(name, _)| (name.clone(), level.as_str().to_string()))
                })
                .collect(),
            configurations: self
                .configurations
                .iter()
                .map(|configuration| ConfiguredSet {
                    tablets: configuration.tablets.clone(),
                    members: configuration.members.clone(),
                    published_at: configuration.published_at,
                })
                .collect(),
            moves: self
                .moves
                .iter()
                .map(|record| MoveSummary {
                    op: record.op,
                    tablets: record.tablets.clone(),
                    from: record.from,
                    to: record.to,
                    phase: record.phase.name().to_string(),
                })
                .collect(),
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
            primary_failover_after: if self.primary_failover_ms > 0 {
                DurationSpec::from(Duration::from_millis(self.primary_failover_ms))
            } else {
                base.primary_failover_after.clone()
            },
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
            physical: 0,
            incarnation,
            weight: 0,
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
        assert!(map.ring_for(joiner, &Hosting::identity(3)).expect("a ring").is_none());
        let ring = map.ring_for(node, &Hosting::identity(2)).expect("a ring").expect("the bootstrapper is placed");
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
        assert!(map.ring_for(joiner, &Hosting::identity(3)).expect("a ring").is_some());
        // and a node claiming another shard count than the placement's is refused
        assert!(map.ring_for(node, &Hosting::identity(4)).is_err());
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

    /// A placed cluster with nodes of the given shard counts, in placement order
    ///
    /// # Arguments
    ///
    /// * `shards` - Each node's shard count
    /// * `rf` - The desired replication factor
    fn placed(shards: &[usize], rf: u32) -> (TabletMap, Vec<NodeId>) {
        let (mut state, node) = state_with(Cluster::default().replication_factor(rf).policy());
        let mut nodes = vec![node];
        for count in &shards[1..] {
            let other = NodeId::mint();
            state.apply(&ControlCommand::Admit(record(other, *count, 1)));
            state.apply(&ControlCommand::ObserveMember(record(other, *count, 1)));
            nodes.push(other);
        }
        state.apply(&ControlCommand::Initialize {
            op: uuid::Uuid::new_v4(),
            principal: String::new(),
            expected_version: state.topology_version,
            nodes: nodes.clone(),
            tables: vec![("Row".to_string(), TableId::of("Row"))],
        });
        // the bootstrapper's record says two shards; make it what the caller asked
        if let Some(member) = state.members.get_mut(&node) {
            member.record.shards = shards[0];
        }
        (TabletMap::from_state(&state, Some(node), &[]), nodes)
    }

    /// A node hosting four slots on two executors serves every local copy from the executor
    /// hosting its slot, builds only the groups those executors host, and routes every remote
    /// copy exactly as it did
    ///
    /// Three nodes at a factor of three: the first has four slots on two executors. Its read
    /// ring names two local contacts and every remote slot; each tablet it holds is served by
    /// `host_of_slot(slot)`; its groups split between the two executors by the same table, and
    /// their `mine` is still the slot, so the address a peer sees never moves
    /// ([F47](../../../docs/src/features/local-rehome.md)).
    #[test]
    fn a_placement_hosts_slots_on_executors() {
        let (map, nodes) = placed(&[4, 2, 2], 3);
        let me = nodes[0];
        let hosting = Hosting::identity(4).plan(2, true).expect("a plan");
        let ring = map.read_ring_for(me, &hosting).expect("a ring").expect("placed");
        // two local executors and every remote slot
        assert_eq!(ring.shards.iter().filter(|info| info.contact.local_index().is_some()).count(), 2);
        assert_eq!(ring.shards.len(), 2 + 2 + 2);
        for tablet in 0..TABLET_COUNT {
            let key = (tablet as u64) << (u64::BITS - super::super::ring::TABLET_BITS);
            let local = map.replicas_of(tablet).into_iter().find(|replica| replica.node == me).expect("a factor of three holds everything");
            assert_eq!(
                ring.find_shard(key).contact,
                ShardContact::Local(hosting.host_of_slot(local.shard)),
                "tablet {tablet} on slot {}",
                local.shard
            );
        }
        // the placement ring routes a remote primary to its slot, untouched by the hosting
        let placement = map.ring_for(me, &hosting).expect("a ring").expect("placed");
        let identity = map.ring_for(me, &Hosting::identity(4)).expect("a ring").expect("placed");
        for tablet in 0..TABLET_COUNT {
            let key = (tablet as u64) << (u64::BITS - super::super::ring::TABLET_BITS);
            let primary = map.replicas_of(tablet)[0];
            if primary.node != me {
                assert_eq!(placement.find_shard(key).contact, identity.find_shard(key).contact, "tablet {tablet}");
            } else {
                assert_eq!(placement.find_shard(key).contact, ShardContact::Local(hosting.host_of_slot(primary.shard)));
            }
        }
        // the groups keep their slot as `mine`, and split over the executors by the hosting
        let groups = map.replica_groups(me);
        assert!(!groups.is_empty());
        let by_executor: Vec<usize> = (0..2)
            .map(|executor| groups.iter().filter(|spec| hosting.host_of_slot(spec.mine) == executor).count())
            .collect();
        assert!(by_executor.iter().all(|count| *count > 0), "{by_executor:?}");
        assert_eq!(by_executor.iter().sum::<usize>(), groups.len());
        assert!(groups.iter().all(|spec| usize::from(spec.mine) < 4));
        // a hosting for another slot count is refused
        assert!(map.ring_for(me, &Hosting::identity(2)).is_err());
    }

    /// A node holding no copy sends to a holder that is up, a never-sent share finds another
    /// holder, and the map carries the failover base
    ///
    /// Four nodes at a factor of three: the fourth holds three quarters of the tablets, and a
    /// tablet it does not hold is routed to that tablet's primary while the primary is up, to
    /// the next replica once the primary is `Down`, and to the primary again when nobody is
    /// up, since health is advice and not authority. A share the link to the primary never
    /// wrote finds the next replica that is up and neither the failed node nor this one, and
    /// none when nobody qualifies. The policy overlay carries the map's failover base and
    /// leaves the file's in force when the map has none
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    #[test]
    fn routing_prefers_holders_that_are_up_and_reroutes_a_never_sent_share() {
        let (mut map, nodes) = placed(&[1, 1, 1, 1], 3);
        let me = nodes[3];
        // a tablet the fourth node holds no copy of, and the key that hashes to it
        let tablet = (0..TABLET_COUNT).find(|tablet| !map.holds(me, *tablet)).expect("a tablet not held");
        let key = (tablet as u64) << (u64::BITS - super::super::ring::TABLET_BITS);
        assert_eq!(Ring::tablet_of(key), tablet);
        let replicas = map.replicas_of(tablet);
        let primary = replicas[0];
        // everybody up: the primary, on the ring too
        assert_eq!(map.preferred_holder(tablet, None), Some(primary));
        let ring = map.read_ring_for(me, &Hosting::identity(1)).expect("a ring").expect("placed");
        assert_eq!(
            ring.find_shard(key).contact,
            ShardContact::Remote {
                node: primary.node,
                shard: primary.shard
            }
        );
        // the primary down: the next replica, on the ring too, and as the alternate for a share
        map.members.get_mut(&primary.node).expect("a member").health = MemberHealth::Down;
        assert_eq!(map.preferred_holder(tablet, None), Some(replicas[1]));
        let ring = map.read_ring_for(me, &Hosting::identity(1)).expect("a ring").expect("placed");
        assert_eq!(
            ring.find_shard(key).contact,
            ShardContact::Remote {
                node: replicas[1].node,
                shard: replicas[1].shard
            }
        );
        assert_eq!(map.alternate_holder(&[key], me, primary.node), Some(replicas[1]));
        // the failed node is never named even while the map still calls it up
        map.members.get_mut(&primary.node).expect("a member").health = MemberHealth::Up;
        assert_eq!(map.alternate_holder(&[key], me, primary.node), Some(replicas[1]));
        assert_eq!(map.preferred_holder(tablet, Some(primary.node)), Some(replicas[1]));
        // nobody up: the primary anyway for routing, nobody for a reroute
        for replica in &replicas {
            map.members.get_mut(&replica.node).expect("a member").health = MemberHealth::Down;
        }
        assert_eq!(map.preferred_holder(tablet, None), Some(primary));
        assert_eq!(map.preferred_holder(tablet, Some(primary.node)), None);
        assert_eq!(map.alternate_holder(&[key], me, primary.node), None);
        // a tablet this node holds is never rerouted elsewhere by this rule
        assert!(map.alternate_holder(&[], me, primary.node).is_none());
        // the failover base rides the map, and an absent one leaves the file's
        let base = Cluster::default().policy();
        map.primary_failover_ms = 750;
        assert_eq!(map.policy_with(&base).primary_failover_after.duration(), Duration::from_millis(750));
        map.primary_failover_ms = 0;
        assert_eq!(map.policy_with(&base).primary_failover_after, base.primary_failover_after);
    }

    /// Replicas land on distinct nodes, capacity follows shard counts, and the factor is feasible
    ///
    /// Three nodes at a factor of three hold every tablet each; four nodes at three hold three
    /// quarters each, give or take one, on distinct nodes with the primary first; a node with
    /// twice the shards holds every tablet across twice as many shards, each replica on the
    /// shard the primary rule picks; and a factor past the placement's size is the placement's
    /// size ([F40](../../../docs/src/features/replication.md)).
    #[test]
    fn placement_respects_distinct_nodes_and_feasible_capacity() {
        // every node holds every tablet at n equals rf
        let (map, nodes) = placed(&[1, 1, 1], 3);
        assert_eq!(map.active_rf(), 3);
        for tablet in 0..TABLET_COUNT {
            let replicas = map.replicas_of(tablet);
            let mut seen: Vec<NodeId> = replicas.iter().map(|addr| addr.node).collect();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), 3, "tablet {tablet} repeats a node: {replicas:?}");
            assert_eq!(replicas[0].node, nodes[tablet % 3], "tablet {tablet} has the wrong primary");
        }
        for node in &nodes {
            assert_eq!((0..TABLET_COUNT).filter(|tablet| map.holds(*node, *tablet)).count(), TABLET_COUNT);
        }
        // four nodes at three: distinct nodes, and about three quarters of the tablets each
        let (map, nodes) = placed(&[1, 1, 1, 1], 3);
        for tablet in 0..TABLET_COUNT {
            let replicas = map.replicas_of(tablet);
            assert_eq!(replicas.len(), 3);
            let mut seen: Vec<NodeId> = replicas.iter().map(|addr| addr.node).collect();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), 3, "tablet {tablet} repeats a node: {replicas:?}");
        }
        for node in &nodes {
            let held = (0..TABLET_COUNT).filter(|tablet| map.holds(*node, *tablet)).count();
            let share = TABLET_COUNT * 3 / 4;
            assert!(held.abs_diff(share) <= 1, "a node holds {held} tablets, not about {share}");
        }
        // a node with twice the shards spreads its replicas over twice as many shards, and a
        // replica sits on the shard the primary rule would pick were the node primary
        let (map, nodes) = placed(&[2, 1, 1], 3);
        let mut on_shard = [0usize; 2];
        for tablet in 0..TABLET_COUNT {
            for replica in map.replicas_of(tablet) {
                if replica.node == nodes[0] {
                    on_shard[usize::from(replica.shard)] += 1;
                    assert_eq!(usize::from(replica.shard), (tablet / 3) % 2);
                } else {
                    assert_eq!(replica.shard, 0);
                }
            }
        }
        assert_eq!(on_shard[0] + on_shard[1], TABLET_COUNT);
        // three tablets in a row share a shard, so the split is even to within one such run
        assert!(on_shard[0].abs_diff(on_shard[1]) <= 3, "shards hold {on_shard:?}");
        // the groups a node hosts name its own shard as a member, tablets ascending
        for group in map.replica_groups(nodes[0]) {
            assert!(group.members.contains(&group.me(nodes[0])));
            assert!(group.tablets.windows(2).all(|pair| pair[0] < pair[1]));
        }
        // a factor past the placement is the placement, not a node holding two copies
        let (map, _) = placed(&[1, 1], 3);
        assert_eq!(map.active_rf(), 2);
        assert!((0..TABLET_COUNT).all(|tablet| map.replicas_of(tablet).len() == 2));
    }

    /// A configuration overrides the rule for exactly its tablets and keeps the group's
    /// identity, a member outside the placement is placed by one, and a move's destination
    /// hosts the set as a learner until the move is published
    ///
    /// Three placed nodes at a factor of three and a fourth member the placement never named:
    /// one replica set is moved from node two to node three. Under the move the fourth node
    /// derives the set's groups as learner specs on the shard the record names, is placed,
    /// and holds nothing; under the configuration it is a member, node two is not, every other
    /// set is where the rule puts it, and every group's identity is the one the rule minted
    /// ([F45](../../../docs/src/features/replica-migration.md)).
    #[test]
    fn a_configuration_overrides_the_rule_and_keeps_the_id() {
        use crate::server::control::migrate::{DataConfiguration, GroupMove, MovePhase, MoveRecord};
        let (mut map, nodes) = placed(&[1, 1, 1], 3);
        // a fourth member, admitted after the placement, with two shards
        let fourth = NodeId::mint();
        map.members.insert(
            fourth,
            MapMember {
                node: fourth,
                client: "c".to_string(),
                data: "d".to_string(),
                control: "e".to_string(),
                shards: 2,
                role: MemberRole::Learner,
                health: MemberHealth::Up,
                incarnation: 1,
                shards_failed: Vec::new(),
                quarantined: Vec::new(),
                phase: super::MemberPhase::Member,
            },
        );
        assert!(!map.places(fourth));
        assert!(map.ring_for(fourth, &Hosting::identity(2)).expect("a ring").is_none());
        // the set node two leads, and its tablets, from the groups the rule derives
        let before = map.groups_of(TableId::of("Row"));
        let (id, expected, tablets) = before
            .iter()
            .find(|(_, members, _)| members[0].node == nodes[2])
            .cloned()
            .expect("node two leads a set");
        assert_eq!(expected.len(), 3);
        let from = expected[0];
        let to = ShardAddr::new(fourth, 1);
        let mut target = expected.clone();
        target[0] = to;
        // the move, planned: the fourth node learns the set's groups on the shard named
        let record = MoveRecord {
            op: uuid::Uuid::new_v4(),
            tablets: tablets.clone(),
            from,
            to,
            expected: expected.clone(),
            target: target.clone(),
            phase: MovePhase::Planned,
            groups: [(id, GroupMove::default())].into_iter().collect(),
            principal: String::new(),
            requested_at: map.version,
            outcome: None,
        };
        map.moves.push(record.clone());
        assert!(map.places(fourth));
        assert!(!map.holds(fourth, usize::from(tablets[0])));
        assert_eq!(map.replicas_of(usize::from(tablets[0])), expected);
        assert_eq!(map.learner_of(usize::from(tablets[0])), Some(to));
        let learners = map.replica_groups(fourth);
        assert_eq!(learners.len(), 1, "one table, one set: {learners:?}");
        assert!(learners[0].learner);
        assert_eq!(learners[0].id, id);
        assert_eq!(learners[0].mine, 1);
        assert_eq!(learners[0].members, expected);
        assert_eq!(learners[0].transition, Some(record.op));
        assert!(!learners[0].is_primary(fourth));
        // the rings route: the fourth node is in them, and its learner tablets go to the source
        let ring = map.read_ring_for(fourth, &Hosting::identity(2)).expect("a ring").expect("placed by the move");
        let key = (u64::from(tablets[0])) << (u64::BITS - super::super::ring::TABLET_BITS);
        assert_eq!(
            ring.find_shard(key).contact,
            ShardContact::Remote {
                node: from.node,
                shard: from.shard
            }
        );
        // the source still hosts the set as a member, under the move
        let sources = map.replica_groups(nodes[2]);
        let source = sources.iter().find(|spec| spec.id == id).expect("the source hosts it");
        assert!(!source.learner);
        assert_eq!(source.transition, Some(record.op));
        // published: the configuration overrides the rule for the set's tablets alone
        map.moves.clear();
        map.configurations.push(DataConfiguration {
            tablets: tablets.clone(),
            members: target.clone(),
            configs: [(id, 12)].into_iter().collect(),
            published_at: map.version + 1,
        });
        for tablet in 0..TABLET_COUNT {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let moved = tablets.contains(&(tablet as u16));
            assert_eq!(map.replicas_of(tablet) == target, moved, "tablet {tablet}");
            assert_eq!(map.rule_replicas_of(tablet) == expected, moved, "tablet {tablet}");
            assert_eq!(map.holds(fourth, tablet), moved);
            assert_eq!(map.holds(nodes[2], tablet), !moved);
        }
        // the identity is the rule's, on every node, and the members are the target's
        let after = map.groups_of(TableId::of("Row"));
        assert_eq!(after.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(), before.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let moved = after.iter().find(|(found, _, _)| *found == id).expect("the set is still there");
        assert_eq!(moved.1, target);
        assert_eq!(moved.2, tablets);
        let hosted = map.replica_groups(fourth);
        assert_eq!(hosted.len(), 1);
        assert!(!hosted[0].learner);
        assert_eq!(hosted[0].members, target);
        assert!(hosted[0].is_primary(fourth));
        assert_eq!(hosted[0].transition, None);
        assert!(map.replica_groups(nodes[2]).iter().all(|spec| spec.id != id));
        // the fourth node reads its copy on its own shard; node two sends there
        let ring = map.read_ring_for(fourth, &Hosting::identity(2)).expect("a ring").expect("placed");
        assert_eq!(ring.find_shard(key).contact, ShardContact::Local(1));
        let ring = map.read_ring_for(nodes[2], &Hosting::identity(1)).expect("a ring").expect("placed");
        assert_eq!(ring.find_shard(key).contact, ShardContact::Remote { node: fourth, shard: 1 });
        assert_eq!(map.preferred_holder(usize::from(tablets[0]), None), Some(to));
        // and the frame carries the configuration
        let frame = map.frame();
        assert_eq!(frame.configurations.len(), 1);
        assert_eq!(frame.configurations[0].members, target);
        assert!(frame.moves.is_empty());
    }
}
