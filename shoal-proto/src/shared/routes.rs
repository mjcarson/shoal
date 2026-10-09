//! Where a client sends a query: the route table built from a pushed topology frame
//!
//! A client that routes by topology ([F74](../../../docs/src/features/client-routing.md)) asks
//! two things of every query: which members hold the tablets it names, and which of them leads
//! each tablet's group. Both are what the server's own map answers, computed here from the frame
//! the cluster pushes and nothing else, through the placement rule in
//! [`crate::shared::placement`] that the server calls too. The table is built once per frame
//! version, so the send path only looks things up.
//!
//! It lives in the crate both peers link so that the engine's tests can hold it to the server's
//! map for every tablet: a route table that disagreed would cost a hop, never an error, since a
//! node forwards what it does not serve, and a hop is exactly what it is built to remove.

use std::collections::HashMap;

use super::identity::{ClusterId, GroupId, NodeId, ShardAddr, TableId};
use super::placement::{preferred_leader, rule_replicas, tablet_of, TABLET_COUNT};
use super::protocol::admin::TopologyFrame;

/// The words of a bitset with one bit a tablet
const TABLET_WORDS: usize = TABLET_COUNT / 64;

/// One member as the route table holds it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteMember {
    /// The node
    pub node: NodeId,
    /// Where clients reach it, as the cluster advertises it
    pub client: String,
    /// Whether the control group has it up
    pub up: bool,
}

/// One replica set as it is served now, with the leader of each table's group over it
#[derive(Debug, Clone)]
struct RouteSet {
    /// The members serving it, the primary first: a move's configuration, or the rule's
    replicas: Vec<ShardAddr>,
    /// The member index of each replica, or none for a node the frame does not list
    replica_members: Vec<Option<u16>>,
    /// The preferred leader of each table's group over the set, in the frame's table order
    leaders: Vec<Option<ShardAddr>>,
    /// The identity of each table's group over the set, in the frame's table order
    groups: Vec<GroupId>,
}

/// The routes a client sends queries by, built from one topology frame
#[derive(Debug, Clone)]
pub struct RouteTable {
    /// The topology version the frame carried
    pub version: u64,
    /// The cluster it describes
    pub cluster: ClusterId,
    /// Every member, in the frame's order
    pub members: Vec<RouteMember>,
    /// Each member's index, by node
    index: HashMap<NodeId, u16>,
    /// The tables, in the frame's order, each with whether its reads go to a leader
    tables: Vec<(TableId, bool)>,
    /// Whether a table the frame does not list has its reads go to a leader
    strong_default: bool,
    /// The set serving each tablet, as an index into `sets`
    set_of: Box<[u16]>,
    /// Every replica set
    sets: Vec<RouteSet>,
    /// Each member's quarantined tablets, a bit a tablet, in member order
    quarantined: Vec<Box<[u64; TABLET_WORDS]>>,
}

/// Whether a read level as the policy spells it is served through a group's leader
///
/// `quorum` and `all` both wait on a read barrier the leader grants
/// ([F41](../../../docs/src/features/read-consistency.md)); `one` is served by any copy.
///
/// # Arguments
///
/// * `level` - The level's name
fn strong(level: &str) -> bool {
    !level.eq_ignore_ascii_case("one")
}

impl RouteTable {
    /// Build the routes a frame describes, or none if it places nothing
    ///
    /// A frame with no placement - a standalone node's, or a joiner's before the cluster is
    /// initialized - routes nothing, and a client sends as it always did.
    ///
    /// # Arguments
    ///
    /// * `frame` - The topology frame the cluster pushed
    #[must_use]
    pub fn from_frame(frame: &TopologyFrame) -> Option<Self> {
        // index every member by node
        let members: Vec<RouteMember> = frame
            .members
            .iter()
            .map(|member| RouteMember {
                node: member.node,
                client: member.client.clone(),
                up: member.health == "up",
            })
            .collect();
        let index: HashMap<NodeId, u16> = members
            .iter()
            .enumerate()
            .filter_map(|(at, member)| u16::try_from(at).ok().map(|at| (member.node, at)))
            .collect();
        // the placement as the rule takes it: each placed node the frame lists, with its shards
        let counts: Vec<(NodeId, u16)> = frame
            .placement
            .iter()
            .filter_map(|node| {
                frame
                    .members
                    .iter()
                    .find(|member| member.node == *node)
                    .map(|member| (*node, member.shards))
            })
            .collect();
        // nothing is placed, so nothing is routed
        if counts.is_empty() {
            return None;
        }
        let copies = frame.active_rf as usize;
        // a member's lead weight and whether it is up, as the server's map reads them
        let weight = |node: NodeId| {
            frame
                .members
                .iter()
                .find(|member| member.node == node)
                .map_or(1, |member| member.lead_weight.max(1))
        };
        let is_up = |node: NodeId| index.get(&node).is_some_and(|at| members[usize::from(*at)].up);
        // every tablet's set, one per rule replica list and the configuration a move left over it
        let mut sets: Vec<RouteSet> = Vec::new();
        let mut by_rule: HashMap<(Vec<ShardAddr>, Option<usize>), u16> = HashMap::new();
        let mut set_of = vec![0u16; TABLET_COUNT].into_boxed_slice();
        for tablet in 0..TABLET_COUNT {
            let rule = rule_replicas(tablet, &counts, copies);
            // the first configuration covering it, as the server's map finds it
            //
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let short = tablet as u16;
            let configured = frame
                .configurations
                .iter()
                .position(|configured| configured.tablets.binary_search(&short).is_ok());
            // a set already built serves this tablet: a move covers whole rule sets, so this is
            // almost always one set per rule list
            let key = (rule, configured);
            if let Some(set) = by_rule.get(&key) {
                set_of[tablet] = *set;
                continue;
            }
            let rule = &key.0;
            // the members serving it now: the configuration's, else the rule's
            let replicas = configured.map_or_else(
                || rule.clone(),
                |at| frame.configurations[at].members.clone(),
            );
            // a group's voters are its members less every tombstoned node
            let voters: Vec<ShardAddr> = replicas
                .iter()
                .filter(|member| !frame.tombstones.contains(&member.node))
                .copied()
                .collect();
            // each table's group over the set is named by the rule's members, and led by the
            // voter its weights prefer
            let groups: Vec<GroupId> = frame
                .tables
                .iter()
                .map(|(_, table)| GroupId::of(*table, rule))
                .collect();
            let leaders = groups
                .iter()
                .map(|group| preferred_leader(*group, &voters, weight, is_up))
                .collect();
            let replica_members = replicas
                .iter()
                .map(|replica| index.get(&replica.node).copied())
                .collect();
            let at = u16::try_from(sets.len()).unwrap_or(u16::MAX);
            sets.push(RouteSet {
                replicas,
                replica_members,
                leaders,
                groups,
            });
            by_rule.insert(key, at);
            set_of[tablet] = at;
        }
        // every member's quarantined tablets as a bitset
        let quarantined = frame
            .members
            .iter()
            .map(|member| {
                let mut bits = Box::new([0u64; TABLET_WORDS]);
                for copy in &member.quarantined {
                    for tablet in &copy.tablets {
                        let tablet = usize::from(*tablet);
                        if tablet < TABLET_COUNT {
                            bits[tablet / 64] |= 1 << (tablet % 64);
                        }
                    }
                }
                bits
            })
            .collect();
        // the level each table's reads are served at, by the table's name
        let strong_default = strong(&frame.read_consistency);
        let tables = frame
            .tables
            .iter()
            .map(|(name, table)| {
                let level = frame
                    .table_read_policy
                    .iter()
                    .find(|(policy, _)| policy == name)
                    .map_or(strong_default, |(_, level)| strong(level));
                (*table, level)
            })
            .collect();
        Some(RouteTable {
            version: frame.version,
            cluster: frame.cluster,
            members,
            index,
            tables,
            strong_default,
            set_of,
            sets,
            quarantined,
        })
    }

    /// The tablet a partition key is in
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition key, as its hash
    #[must_use]
    pub fn tablet_of(partition: u64) -> usize {
        tablet_of(partition)
    }

    /// A member's index in [`RouteTable::members`], if the frame lists it
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    #[must_use]
    pub fn member_of(&self, node: NodeId) -> Option<u16> {
        self.index.get(&node).copied()
    }

    /// The set serving a tablet
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    fn set(&self, tablet: usize) -> &RouteSet {
        &self.sets[usize::from(self.set_of[tablet % TABLET_COUNT])]
    }

    /// Whether a member's copy of a tablet is quarantined for any table
    ///
    /// Routing is per tablet and not per table, as the server's is: a holder with any table's
    /// copy quarantined is passed over for every table.
    ///
    /// # Arguments
    ///
    /// * `member` - The member's index
    /// * `tablet` - The tablet
    #[must_use]
    pub fn is_quarantined(&self, member: u16, tablet: usize) -> bool {
        self.quarantined
            .get(usize::from(member))
            .is_some_and(|bits| bits[(tablet % TABLET_COUNT) / 64] & (1 << (tablet % 64)) != 0)
    }

    /// The replicas serving a tablet, the primary first, as the server's map names them
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn replicas(&self, tablet: usize) -> &[ShardAddr] {
        &self.set(tablet).replicas
    }

    /// The members holding a tablet that can serve it: up and not quarantined, in replica order
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    pub fn holders(&self, tablet: usize) -> impl Iterator<Item = u16> + '_ {
        self.set(tablet)
            .replica_members
            .iter()
            .flatten()
            .copied()
            .filter(move |member| {
                self.members[usize::from(*member)].up && !self.is_quarantined(*member, tablet)
            })
    }

    /// The holder a node holding no copy of a tablet forwards to: the first that is up and not
    /// quarantined, else the primary
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn fallback_holder(&self, tablet: usize) -> Option<ShardAddr> {
        let set = self.set(tablet);
        // the first replica that can serve it, else the primary anyway
        set.replicas
            .iter()
            .zip(&set.replica_members)
            .find(|(_, member)| {
                member.is_some_and(|member| {
                    self.members[usize::from(member)].up && !self.is_quarantined(member, tablet)
                })
            })
            .map(|(replica, _)| *replica)
            .or_else(|| set.replicas.first().copied())
    }

    /// The preferred leader of the group serving a table's tablet, if the frame names the table
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `tablet` - The tablet
    #[must_use]
    pub fn leader(&self, table: TableId, tablet: usize) -> Option<ShardAddr> {
        let at = self.tables.iter().position(|(id, _)| *id == table)?;
        self.set(tablet).leaders.get(at).copied().flatten()
    }

    /// The identity of the group serving a table's tablet, if the frame names the table
    ///
    /// What a leader hint is keyed by: a write's answer names its group in its session token
    /// ([F74](../../../docs/src/features/client-routing.md)).
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `tablet` - The tablet
    #[must_use]
    pub fn group(&self, table: TableId, tablet: usize) -> Option<GroupId> {
        let at = self.tables.iter().position(|(id, _)| *id == table)?;
        self.set(tablet).groups.get(at).copied()
    }

    /// Whether a table's reads are served through its groups' leaders when a bundle names no level
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    #[must_use]
    pub fn strong_reads(&self, table: TableId) -> bool {
        self.tables
            .iter()
            .find(|(id, _)| *id == table)
            .map_or(self.strong_default, |(_, strong)| *strong)
    }
}

#[cfg(test)]
mod tests {
    use super::RouteTable;
    use crate::shared::identity::{ClusterId, NodeId, ShardAddr, TableId};
    use crate::shared::placement::{rule_replicas, TABLET_COUNT};
    use crate::shared::protocol::admin::{
        ConfiguredSet, QuarantinedMember, TopologyFrame, TopologyMember,
    };

    /// A frame of three members at a factor of three, every one up and at weight one
    fn frame() -> TopologyFrame {
        let members: Vec<TopologyMember> = (1..=3u64)
            .map(|n| TopologyMember {
                node: NodeId::from(n),
                client: format!("10.0.0.{n}:12000"),
                data: format!("10.0.0.{n}:12001"),
                control: format!("10.0.0.{n}:12002"),
                shards: 2,
                role: "voter".to_string(),
                health: "up".to_string(),
                phase: "member".to_string(),
                state: "up".to_string(),
                incarnation: 1,
                shards_failed: Vec::new(),
                quarantined: Vec::new(),
                lead_weight: 1,
            })
            .collect();
        TopologyFrame {
            cluster: ClusterId::mint(),
            version: 9,
            leader: None,
            placement: members.iter().map(|member| member.node).collect(),
            members,
            desired_rf: 3,
            active_rf: 3,
            write_consistency: "quorum".to_string(),
            read_consistency: "one".to_string(),
            tables: vec![("Notes".to_string(), TableId::of("Notes"))],
            table_read_policy: Vec::new(),
            configurations: Vec::new(),
            moves: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    /// Equal weights lead at the primary, every holder is up, and reads follow the policy
    #[test]
    fn a_frame_routes_by_the_placement_rule() {
        let frame = frame();
        let table = TableId::of("Notes");
        let routes = RouteTable::from_frame(&frame).expect("a placed frame routes");
        let counts: Vec<(NodeId, u16)> = frame.placement.iter().map(|node| (*node, 2)).collect();
        for tablet in 0..TABLET_COUNT {
            let rule = rule_replicas(tablet, &counts, 3);
            assert_eq!(routes.replicas(tablet), rule.as_slice());
            assert_eq!(routes.leader(table, tablet), Some(rule[0]));
            assert_eq!(routes.holders(tablet).count(), 3);
        }
        assert!(!routes.strong_reads(table));
        // a table read at quorum by its own policy goes to its leaders
        let mut quorum = frame.clone();
        quorum.table_read_policy = vec![("Notes".to_string(), "quorum".to_string())];
        let routes = RouteTable::from_frame(&quorum).expect("a placed frame routes");
        assert!(routes.strong_reads(table));
        // and a frame that places nothing routes nothing
        let mut empty = frame;
        empty.placement.clear();
        assert!(RouteTable::from_frame(&empty).is_none());
    }

    /// A configuration, a quarantine and a member that is down change the holders
    #[test]
    fn holders_skip_what_cannot_serve() {
        let mut frame = frame();
        // tablet zero's set moved onto shard one of every member
        let moved: Vec<ShardAddr> = frame
            .placement
            .iter()
            .map(|node| ShardAddr::new(*node, 1))
            .collect();
        frame.configurations = vec![ConfiguredSet {
            tablets: vec![0, 3],
            members: moved.clone(),
            published_at: 4,
        }];
        // the second member is down, and the third's copy of tablet three is quarantined
        frame.members[1].health = "down".to_string();
        frame.members[2].quarantined = vec![QuarantinedMember {
            table: TableId::of("Notes"),
            group: 1,
            tablets: vec![3],
            reason: "digest".to_string(),
        }];
        let routes = RouteTable::from_frame(&frame).expect("a placed frame routes");
        assert_eq!(routes.replicas(0), moved.as_slice());
        // a tablet of the same rule set the configuration does not cover keeps the rule's members
        assert_eq!(routes.replicas(6)[0].shard, 0);
        assert_eq!(routes.holders(0).collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(routes.holders(3).collect::<Vec<_>>(), vec![0]);
        assert!(routes.is_quarantined(2, 3));
        assert!(!routes.is_quarantined(2, 0));
        // the primary is up, so it is the holder a forward would pick
        assert_eq!(routes.fallback_holder(3), Some(moved[0]));
    }
}
