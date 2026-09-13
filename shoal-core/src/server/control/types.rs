//! What the control group carries: its type configuration, its commands and its state
//!
//! openraft is generic over one type that names everything else, and [`ControlConfig`] is that
//! type for the control group. The application data is a [`ControlCommand`], the response is a
//! [`ControlResponse`], a node is named by its [`NodeId`] and described by a [`MemberRecord`],
//! and the runtime is the glommio one. The rest - term, leader id, vote, entry, responder,
//! batching, error source - are openraft's defaults.
//!
//! [`ControlState`] is what applying the committed log produces. Since
//! [F39](../../../../docs/src/features/membership.md) it holds every member the cluster has
//! admitted with its role, its health and the incarnation it was admitted at, the placement
//! order the tablets are laid over once an operator initialized one, and the administrative
//! operations it has applied so that a repeat of one is answered as the first was. It is the
//! seed of the tablet map ([C4](../../../../docs/src/distributed/tablet-map.md)): every node
//! builds its [`TabletMap`](crate::server::map::TabletMap) from this and nothing else.
//!
//! # Invariants
//!
//! **`apply` is pure.** The same state and command always produce the same result, which is
//! what lets a replay of the log rebuild the state exactly on every member; anything that
//! would differ between members - a clock, a random id, a local observation - comes in the
//! command, minted by the proposer, and is never read here.
//!
//! **A membership entry is reflected here too.** openraft's own record of who votes and who
//! learns is applied through [`ControlState::observe_membership`] beside the commands, so the
//! roles in this state and the configuration the group runs under never disagree.

use std::collections::BTreeMap;
use std::fmt;

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::runtime::GlommioRuntime;
use crate::server::conf::cluster::{BootstrapPolicy, Consistency};
use crate::shared::identity::{ClusterId, NodeId, TableId};

declare_raft_types!(
    /// The control group's type configuration
    pub ControlConfig:
        D = ControlCommand,
        R = ControlResponse,
        NodeId = NodeId,
        Node = MemberRecord,
        AsyncRuntime = GlommioRuntime,
);

/// How many administrative operations the state remembers the outcome of
///
/// A repeat of a remembered operation is answered as it was the first time; older ones are
/// forgotten oldest first. Bounded so the state stays a few kilobytes however long a cluster
/// runs; a client retrying an operation a thousand mutations later is not a retry.
pub const REMEMBERED_OPERATIONS: usize = 1024;

/// What the group knows about one member
///
/// The endpoints are what the member advertises: the client endpoint is the one its shards
/// serve on, the data endpoint the one every shard's peer listener binds, and the control
/// endpoint the one its control thread binds. A restart that changed an address is visibly a
/// re-observation, and a restart at all is visibly a higher incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MemberRecord {
    /// The node
    pub node: NodeId,
    /// Where clients reach it
    pub client: String,
    /// Where data peers reach it
    pub data: String,
    /// Where control peers reach it
    pub control: String,
    /// The cpu its control thread runs on
    pub control_core: usize,
    /// Whether that cpu's physical core is shared with a shard
    pub control_shared: bool,
    /// How many shards it runs
    pub shards: usize,
    /// Which start of the node this record was written by
    ///
    /// From the node's storage marker, bumped on every start. The cluster's fencing rule is
    /// that the highest wins: a record at a lower incarnation than the committed one is
    /// refused, and a member superseded by a higher one is told so and stops
    /// ([C1](../../../../docs/src/distributed/node-identity.md), Q11).
    #[serde(default)]
    pub incarnation: u64,
}

impl fmt::Display for MemberRecord {
    /// The node and where it is reached
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}#{}", self.node, self.client, self.incarnation)
    }
}

/// Whether a member is being admitted, is up, or has been called down
///
/// The durable half of [C3](../../../../docs/src/distributed/membership.md)'s member state
/// machine. `Leaving`, `Removing` and `Removed` are M9b's and do not exist yet; `Unreachable`
/// is a local observation and deliberately not a value here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberHealth {
    /// Admitted and receiving the control state; not yet reported in
    Joining,
    /// Reported in at its current incarnation, and not since called down
    Up,
    /// The control leader committed that its reports stopped arriving
    Down,
}

impl MemberHealth {
    /// The name this health is spelled as on the wire and in a log line
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            MemberHealth::Joining => "joining",
            MemberHealth::Up => "up",
            MemberHealth::Down => "down",
        }
    }
}

/// Whether a member votes in the control group or only learns from it
///
/// Mirrored from openraft's membership on every membership entry, so the state and the
/// configuration the group runs under agree. A learner holds the whole control state and is
/// a placement target like any other member; it does not vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberRole {
    /// Receives the log and votes in nothing
    Learner,
    /// Votes in elections and counts toward the control quorum
    Voter,
}

impl MemberRole {
    /// The name this role is spelled as on the wire and in a log line
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            MemberRole::Learner => "learner",
            MemberRole::Voter => "voter",
        }
    }
}

/// Everything the group has committed about one member
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberState {
    /// What the member advertises, at the incarnation it was last admitted or observed at
    pub record: MemberRecord,
    /// Whether it is joining, up or down
    pub health: MemberHealth,
    /// Whether it votes
    pub role: MemberRole,
    /// The shards that have failed on it, by index, as it last reported
    #[serde(default)]
    pub shards_failed: Vec<u16>,
    /// The topology version its health last changed at
    #[serde(default)]
    pub since: u64,
    /// The identity of its current down episode, if it is down
    ///
    /// Minted by the leader that committed the episode, so that a grace timer or a removal
    /// (M9b) names the episode it belongs to and a later episode is not mistaken for it.
    #[serde(default)]
    pub episode: Option<Uuid>,
}

/// One administrative operation the state has applied, remembered so a repeat is harmless
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    /// The principal that asked for it
    pub principal: String,
    /// What kind of operation it was
    pub kind: String,
    /// The topology version it was written against
    pub expected_version: u64,
    /// What applying it produced the first time
    pub outcome: ControlResponse,
    /// The topology version it was applied at, which orders the memory
    pub at: u64,
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
    /// Record what a member currently advertises, and that it is up
    ///
    /// Written by a member at every start through the leader, so an address that changed is a
    /// committed fact and not something a peer discovers by failing to connect. The
    /// incarnation in the record is judged against the committed one: lower is fenced, equal
    /// from another address is fenced, higher supersedes.
    ObserveMember(MemberRecord),
    /// Admit a joiner the leader has added as a learner
    ///
    /// Written by the leader once the joiner's identity, cluster and schema have been checked
    /// and the group has been told to replicate to it. The member is `Joining` until it
    /// observes itself.
    Admit(MemberRecord),
    /// Record that a member is up or down, as the leader's detector decided
    SetHealth {
        /// The member
        node: NodeId,
        /// What it now is
        health: MemberHealth,
        /// The incarnation the evidence was about; a lower one than committed is refused
        incarnation: u64,
        /// The identity of the down episode this opens, if it opens one
        episode: Option<Uuid>,
    },
    /// Record which of a member's shards have failed, as it reported
    ReportShards {
        /// The member
        node: NodeId,
        /// The incarnation it reported at
        incarnation: u64,
        /// The shards that have failed, by index
        failed: Vec<u16>,
    },
    /// Place every tablet across these members, once, in this order
    Initialize {
        /// The identity of the operation, so a repeat is answered as the first was
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The members to place over, in placement order
        nodes: Vec<NodeId>,
        /// The tables the proposer serves, with their stable identities
        tables: Vec<(String, TableId)>,
    },
    /// Change how many nodes vote in the control group
    SetControlVoters {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The new count
        count: u32,
    },
    /// Set, or clear, the level one table's reads are served at when a bundle does not say
    ///
    /// Versioned control state rather than a per-node setting, so every coordinator resolves a
    /// table the same way ([F41](../../../../docs/src/features/read-consistency.md)).
    SetTableReadPolicy {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The table
        table: TableId,
        /// The level, or none to fall back to the cluster's default
        level: Option<Consistency>,
    },
}

impl ControlCommand {
    /// The administrative operation this command carries, if it is one
    ///
    /// Only these are remembered by operation id; the rest are the cluster's own bookkeeping.
    #[must_use]
    pub fn operation(&self) -> Option<(Uuid, &str, &str, u64)> {
        match self {
            ControlCommand::Initialize {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "initialize", *expected_version)),
            ControlCommand::SetControlVoters {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "set_control_voters", *expected_version)),
            ControlCommand::SetTableReadPolicy {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "set_table_read_policy", *expected_version)),
            _ => None,
        }
    }
}

impl fmt::Display for ControlCommand {
    /// Name the command, which is what openraft's traces print
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlCommand::Bootstrap { cluster, member, .. } => {
                write!(f, "Bootstrap({cluster} by {member})")
            }
            ControlCommand::ObserveMember(member) => write!(f, "ObserveMember({member})"),
            ControlCommand::Admit(member) => write!(f, "Admit({member})"),
            ControlCommand::SetHealth { node, health, .. } => {
                write!(f, "SetHealth({node} {})", health.name())
            }
            ControlCommand::ReportShards { node, failed, .. } => {
                write!(f, "ReportShards({node} {failed:?})")
            }
            ControlCommand::Initialize { nodes, .. } => write!(f, "Initialize({} nodes)", nodes.len()),
            ControlCommand::SetControlVoters { count, .. } => write!(f, "SetControlVoters({count})"),
            ControlCommand::SetTableReadPolicy { table, level, .. } => {
                write!(f, "SetTableReadPolicy({table} {})", level.map_or("clear", |level| level.as_str()))
            }
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
    /// Refused because the node's committed incarnation is at or beyond the one offered
    ///
    /// The fencing rule's answer: the proposer is a run of the node the cluster has already
    /// replaced, or a second run of the same copy of it, and it must stop.
    Fenced {
        /// The node
        node: NodeId,
        /// The incarnation the cluster holds for it
        committed: u64,
        /// The incarnation that was offered
        offered: u64,
    },
    /// An administrative operation seen before, answered as it was the first time
    Repeated {
        /// What the first application produced
        first: Box<ControlResponse>,
    },
}

impl ControlResponse {
    /// The topology version this response reports, if it applied
    #[must_use]
    pub fn applied_version(&self) -> Option<u64> {
        match self {
            ControlResponse::Applied { topology_version } => Some(*topology_version),
            ControlResponse::Repeated { first } => first.applied_version(),
            ControlResponse::Refused { .. } | ControlResponse::Fenced { .. } => None,
        }
    }
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
    pub members: BTreeMap<NodeId, MemberState>,
    /// The policy the cluster was bootstrapped with, as amended by admin operations since
    pub policy: Option<BootstrapPolicy>,
    /// The node that created the cluster, which holds every tablet until a placement is initialized
    #[serde(default)]
    pub bootstrapper: Option<NodeId>,
    /// The members tablets are placed over, in placement order, once initialized
    #[serde(default)]
    pub initialized: Option<Vec<NodeId>>,
    /// The tables the schema serves, with their stable identities, recorded at initialization
    #[serde(default)]
    pub tables: Vec<(String, TableId)>,
    /// The administrative operations applied, by their identity
    #[serde(default)]
    pub operations: BTreeMap<Uuid, Operation>,
    /// The incarnation each node was last fenced at, if any run of it has been
    #[serde(default)]
    pub fenced: BTreeMap<NodeId, u64>,
    /// Whether the group's configuration is joint: a membership change half way through
    ///
    /// A joint configuration needs a majority of the old voters and of the new, so a cluster
    /// in one cannot afford to lose an old voter yet; the roles above are the union, and this
    /// is what says the union is not yet the whole story.
    #[serde(default)]
    pub joint: bool,
    /// The level each table's reads are served at when a bundle does not say, where set
    ///
    /// A table not named here is served at the policy's `read_consistency`
    /// ([F41](../../../../docs/src/features/read-consistency.md)).
    #[serde(default)]
    pub table_read_policy: BTreeMap<TableId, Consistency>,
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
        // an administrative operation seen before is answered as it was the first time and
        // changes nothing, whatever it asks now
        if let Some((op, _, _, _)) = command.operation() {
            if let Some(seen) = self.operations.get(&op) {
                return ControlResponse::Repeated {
                    first: Box::new(seen.outcome.clone()),
                };
            }
        }
        // apply it, and remember the outcome if it was an operation that applied: a refusal
        // changed nothing, so a retry of it under the same id is a fresh attempt, which is what
        // lets a request refused for a stale version be sent again against the current one
        let response = self.apply_once(command);
        if let (Some((op, principal, kind, expected_version)), ControlResponse::Applied { .. }) =
            (command.operation(), &response)
        {
            self.operations.insert(
                op,
                Operation {
                    principal: principal.to_string(),
                    kind: kind.to_string(),
                    expected_version,
                    outcome: response.clone(),
                    at: self.topology_version,
                },
            );
            // forget the oldest once too many are remembered
            while self.operations.len() > REMEMBERED_OPERATIONS {
                let oldest = self
                    .operations
                    .iter()
                    .min_by_key(|(_, operation)| operation.at)
                    .map(|(op, _)| *op);
                match oldest {
                    Some(op) => {
                        self.operations.remove(&op);
                    }
                    None => break,
                }
            }
        }
        response
    }

    /// Apply one command without the operation memory around it
    ///
    /// # Arguments
    ///
    /// * `command` - The command to apply
    fn apply_once(&mut self, command: &ControlCommand) -> ControlResponse {
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
                self.bootstrapper = Some(member.node);
                // the group's own membership entry may already have named this node a voter
                let role = self
                    .members
                    .get(&member.node)
                    .map_or(MemberRole::Voter, |state| state.role);
                self.topology_version += 1;
                self.members.insert(
                    member.node,
                    MemberState {
                        record: member.clone(),
                        health: MemberHealth::Up,
                        role,
                        shards_failed: Vec::new(),
                        since: self.topology_version,
                        episode: None,
                    },
                );
                self.applied()
            }
            // a member saying where it is and that it is up, or a joiner being let in
            ControlCommand::ObserveMember(record) | ControlCommand::Admit(record) => {
                let admitting = matches!(command, ControlCommand::Admit(_));
                self.observe(record, admitting)
            }
            // the leader's detector calling a member up or down
            ControlCommand::SetHealth {
                node,
                health,
                incarnation,
                episode,
            } => {
                let Some(state) = self.members.get(node) else {
                    return ControlResponse::Refused {
                        reason: format!("{node} is not a member, so has no health to set"),
                    };
                };
                // evidence about an older run of the node says nothing about this one
                if *incarnation < state.record.incarnation {
                    return ControlResponse::Fenced {
                        node: *node,
                        committed: state.record.incarnation,
                        offered: *incarnation,
                    };
                }
                if state.health == *health {
                    return self.applied();
                }
                self.topology_version += 1;
                let version = self.topology_version;
                let state = self.members.get_mut(node).expect("checked above");
                state.health = *health;
                state.since = version;
                state.episode = match health {
                    MemberHealth::Down => *episode,
                    MemberHealth::Up | MemberHealth::Joining => None,
                };
                self.applied()
            }
            // a member's shard health, as it reported
            ControlCommand::ReportShards {
                node,
                incarnation,
                failed,
            } => {
                let Some(state) = self.members.get(node) else {
                    return ControlResponse::Refused {
                        reason: format!("{node} is not a member, so has no shards to report"),
                    };
                };
                if *incarnation < state.record.incarnation {
                    return ControlResponse::Fenced {
                        node: *node,
                        committed: state.record.incarnation,
                        offered: *incarnation,
                    };
                }
                if state.shards_failed == *failed {
                    return self.applied();
                }
                self.topology_version += 1;
                self.members.get_mut(node).expect("checked above").shards_failed = failed.clone();
                self.applied()
            }
            // the one explicit placement
            ControlCommand::Initialize {
                expected_version,
                nodes,
                tables,
                ..
            } => {
                if self.cluster.is_none() {
                    return ControlResponse::Refused {
                        reason: "no cluster has been bootstrapped to place tablets in".to_string(),
                    };
                }
                if let Some(refusal) = self.check_version(*expected_version) {
                    return refusal;
                }
                if self.initialized.is_some() {
                    return ControlResponse::Refused {
                        reason: "the placement is already initialized; moving tablets between \
                                 nodes is a migration (M9a), not a second initialization"
                            .to_string(),
                    };
                }
                if nodes.is_empty() {
                    return ControlResponse::Refused {
                        reason: "a placement needs at least one node".to_string(),
                    };
                }
                // every node named has to be a member that is up, and named once
                let mut seen = std::collections::BTreeSet::new();
                for node in nodes {
                    match self.members.get(node) {
                        Some(state) if state.health == MemberHealth::Up => {}
                        Some(state) => {
                            return ControlResponse::Refused {
                                reason: format!(
                                    "{node} is {}, and only an up member can be placed on",
                                    state.health.name()
                                ),
                            };
                        }
                        None => {
                            return ControlResponse::Refused {
                                reason: format!("{node} is not a member of this cluster"),
                            };
                        }
                    }
                    if !seen.insert(*node) {
                        return ControlResponse::Refused {
                            reason: format!("{node} is named twice in the placement"),
                        };
                    }
                }
                self.initialized = Some(nodes.clone());
                self.tables = tables.clone();
                self.topology_version += 1;
                self.applied()
            }
            // the explicit voter policy
            ControlCommand::SetControlVoters {
                expected_version,
                count,
                ..
            } => {
                let Some(policy) = self.policy.as_mut() else {
                    return ControlResponse::Refused {
                        reason: "no cluster has been bootstrapped to set a policy on".to_string(),
                    };
                };
                if !matches!(count, 1 | 3 | 5) {
                    return ControlResponse::Refused {
                        reason: format!("control_voters is {count}; it has to be 1, 3 or 5"),
                    };
                }
                let current = policy.control_voters;
                if let Some(refusal) = self.check_version(*expected_version) {
                    return refusal;
                }
                if current == *count {
                    return self.applied();
                }
                self.policy.as_mut().expect("checked above").control_voters = *count;
                self.topology_version += 1;
                self.applied()
            }
            // one table's read policy
            ControlCommand::SetTableReadPolicy {
                expected_version,
                table,
                level,
                ..
            } => {
                if self.policy.is_none() {
                    return ControlResponse::Refused {
                        reason: "no cluster has been bootstrapped to set a policy on".to_string(),
                    };
                }
                // `All` is not a read level anything serves; the only strong read is `Quorum`
                if *level == Some(Consistency::All) {
                    return ControlResponse::Refused {
                        reason: "read_consistency All is not served; the strong read level is Quorum (C6)".to_string(),
                    };
                }
                // the table has to be one the schema serves, which the initialization recorded
                if !self.tables.iter().any(|(_, id)| id == table) {
                    return ControlResponse::Refused {
                        reason: format!("table {table} is not one the placement was initialized with"),
                    };
                }
                if let Some(refusal) = self.check_version(*expected_version) {
                    return refusal;
                }
                // unchanged is applied without moving the version
                let current = self.table_read_policy.get(table).copied();
                if current == *level {
                    return self.applied();
                }
                match level {
                    Some(level) => {
                        self.table_read_policy.insert(*table, *level);
                    }
                    None => {
                        self.table_read_policy.remove(table);
                    }
                }
                self.topology_version += 1;
                self.applied()
            }
        }
    }

    /// Admit or re-observe a member under the fencing rule
    ///
    /// # Arguments
    ///
    /// * `record` - What the member advertises, at its incarnation
    /// * `admitting` - Whether this is a leader admitting a joiner, or a member observing itself
    fn observe(&mut self, record: &MemberRecord, admitting: bool) -> ControlResponse {
        // an observation before the bootstrap describes a member of nothing
        if self.cluster.is_none() {
            return ControlResponse::Refused {
                reason: "no cluster has been bootstrapped to observe a member of".to_string(),
            };
        }
        match self.members.get(&record.node) {
            // a node nobody admitted cannot observe itself in; the leader admits it first
            None if !admitting => ControlResponse::Refused {
                reason: format!("{} is not a member; a joiner is admitted by the leader first", record.node),
            },
            // a joiner let in for the first time, joining until it reports in
            None => {
                self.topology_version += 1;
                let version = self.topology_version;
                self.members.insert(
                    record.node,
                    MemberState {
                        record: record.clone(),
                        health: MemberHealth::Joining,
                        role: MemberRole::Learner,
                        shards_failed: Vec::new(),
                        since: version,
                        episode: None,
                    },
                );
                self.applied()
            }
            Some(existing) => {
                let committed = existing.record.incarnation;
                // a lower incarnation is a run the cluster has already replaced
                if record.incarnation < committed {
                    return ControlResponse::Fenced {
                        node: record.node,
                        committed,
                        offered: record.incarnation,
                    };
                }
                // an equal one from another address is a second run of the same copy, which is
                // the cloned directory the lock cannot see; from the same address it is the
                // same run saying where it is again
                if record.incarnation == committed && record.control != existing.record.control {
                    return ControlResponse::Fenced {
                        node: record.node,
                        committed,
                        offered: record.incarnation,
                    };
                }
                // a higher one supersedes, and the run it replaces is fenced
                if record.incarnation > committed {
                    self.fenced.insert(record.node, committed);
                }
                // an admission of a node already admitted at this incarnation keeps its health
                let health = if admitting && record.incarnation == committed {
                    existing.health
                } else {
                    MemberHealth::Up
                };
                let unchanged = existing.record == *record && existing.health == health;
                if unchanged {
                    return self.applied();
                }
                self.topology_version += 1;
                let version = self.topology_version;
                let state = self.members.get_mut(&record.node).expect("checked above");
                let health_moved = state.health != health;
                state.record = record.clone();
                state.health = health;
                if health_moved {
                    state.since = version;
                    state.episode = None;
                }
                self.applied()
            }
        }
    }

    /// Refuse a request written against a version other than the current one
    ///
    /// # Arguments
    ///
    /// * `expected` - The version the request was written against
    fn check_version(&self, expected: u64) -> Option<ControlResponse> {
        if expected == self.topology_version {
            None
        } else {
            Some(ControlResponse::Refused {
                reason: format!(
                    "stale version: the request was written against topology version {expected} \
                     and the cluster is at {}",
                    self.topology_version
                ),
            })
        }
    }

    /// The applied response at the current version
    fn applied(&self) -> ControlResponse {
        ControlResponse::Applied {
            topology_version: self.topology_version,
        }
    }

    /// Reflect the group's own membership into the members' roles
    ///
    /// Called on every membership entry the group commits and on every snapshot installed, so
    /// the role each member is recorded with is the one the configuration gives it. A node in
    /// the configuration that no command has admitted yet - a learner added before its
    /// admission applied - is recorded as joining, with the record the configuration carries.
    /// Returns whether anything changed, which is when the topology version moved.
    ///
    /// # Arguments
    ///
    /// * `membership` - The membership the group committed
    pub fn observe_membership(
        &mut self,
        membership: &openraft::Membership<NodeId, MemberRecord>,
    ) -> bool {
        let mut changed = false;
        // a configuration with two voter sets is a change in progress
        let joint = membership.get_joint_config().len() > 1;
        if self.joint != joint {
            self.joint = joint;
            changed = true;
        }
        let voters: std::collections::BTreeSet<NodeId> = membership.voter_ids().collect();
        for (node, record) in membership.nodes() {
            let role = if voters.contains(node) {
                MemberRole::Voter
            } else {
                MemberRole::Learner
            };
            match self.members.get_mut(node) {
                Some(state) => {
                    if state.role != role {
                        state.role = role;
                        changed = true;
                    }
                }
                None => {
                    self.members.insert(
                        *node,
                        MemberState {
                            record: record.clone(),
                            health: MemberHealth::Joining,
                            role,
                            shards_failed: Vec::new(),
                            since: self.topology_version + 1,
                            episode: None,
                        },
                    );
                    changed = true;
                }
            }
        }
        if changed {
            self.topology_version += 1;
        }
        changed
    }

    /// The replication factor the policy asks for, or zero before the bootstrap
    pub fn desired_rf(&self) -> u32 {
        self.policy
            .as_ref()
            .map_or(0, |policy| policy.replication_factor)
    }

    /// The replication factor the members can actually give
    ///
    /// ~~One where a placement exists, since every tablet has exactly one owner under it~~
    /// The desired factor or the placement's size, whichever is smaller, since
    /// [F40](../../../../docs/src/features/replication.md) places that many copies; zero before
    /// a placement exists. A placement smaller than the factor serves what it can and reports
    /// the gap ([F39](../../../../docs/src/features/membership.md)).
    pub fn active_rf(&self) -> u32 {
        match &self.initialized {
            Some(nodes) => self
                .desired_rf()
                .max(1)
                .min(u32::try_from(nodes.len()).unwrap_or(u32::MAX)),
            None => 0,
        }
    }

    /// How many members are up
    pub fn up_members(&self) -> u32 {
        let up = self
            .members
            .values()
            .filter(|state| state.health == MemberHealth::Up)
            .count();
        u32::try_from(up).unwrap_or(u32::MAX)
    }

    /// The members that vote, in node order
    pub fn voters(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .filter(|(_, state)| state.role == MemberRole::Voter)
            .map(|(node, _)| *node)
            .collect()
    }

    /// The members that only learn, in node order
    pub fn learners(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .filter(|(_, state)| state.role == MemberRole::Learner)
            .map(|(node, _)| *node)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Consistency, ControlCommand, ControlResponse, ControlState, MemberHealth, MemberRecord,
        MemberRole,
    };
    use crate::server::conf::Cluster;
    use crate::shared::identity::{ClusterId, NodeId, TableId};
    use uuid::Uuid;

    /// A member record for tests
    fn member(node: NodeId, client: &str) -> MemberRecord {
        MemberRecord {
            node,
            client: client.to_string(),
            data: String::new(),
            control: format!("{client}:2"),
            control_core: 0,
            control_shared: false,
            shards: 2,
            incarnation: 1,
        }
    }

    /// A bootstrapped state with one member, and its ids
    fn bootstrapped() -> (ControlState, ClusterId, NodeId) {
        let mut state = ControlState::default();
        let cluster = ClusterId::mint();
        let node = NodeId::mint();
        let bootstrap = ControlCommand::Bootstrap {
            cluster,
            policy: Cluster::default().policy(),
            member: member(node, "a"),
        };
        assert_eq!(
            state.apply(&bootstrap),
            ControlResponse::Applied {
                topology_version: 1
            }
        );
        (state, cluster, node)
    }

    /// A bootstrap creates the cluster once, and the topology version moves with each change
    #[test]
    fn a_bootstrap_is_applied_once() {
        let mut state = ControlState::default();
        let node = NodeId::mint();
        let policy = Cluster::default().policy();
        // before anything, an observation is of nothing
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(member(node, "a"))),
            ControlResponse::Refused { .. }
        ));
        let (mut state, cluster, node) = bootstrapped();
        assert_eq!(state.cluster, Some(cluster));
        assert_eq!(state.desired_rf(), 3);
        assert_eq!(state.active_rf(), 0);
        assert_eq!(state.members[&node].role, MemberRole::Voter);
        assert_eq!(state.members[&node].health, MemberHealth::Up);
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
        // a member at a new address and a higher incarnation is
        let mut moved = member(node, "c");
        moved.incarnation = 2;
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(moved)),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.members[&node].record.client, "c");
        assert_eq!(state.fenced[&node], 1);
    }

    /// A joiner is admitted as a learner, joining until it observes itself
    #[test]
    fn a_joiner_is_admitted_then_observes_itself_up() {
        let (mut state, _, _) = bootstrapped();
        let joiner = NodeId::mint();
        // a node nobody admitted cannot observe itself in
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(member(joiner, "b"))),
            ControlResponse::Refused { .. }
        ));
        // the leader admits it
        assert_eq!(
            state.apply(&ControlCommand::Admit(member(joiner, "b"))),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.members[&joiner].health, MemberHealth::Joining);
        assert_eq!(state.members[&joiner].role, MemberRole::Learner);
        assert_eq!(state.up_members(), 1);
        // and it reports in
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(member(joiner, "b"))),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert_eq!(state.members[&joiner].health, MemberHealth::Up);
        assert_eq!(state.up_members(), 2);
        // a second admission at the same incarnation keeps it where it is
        assert_eq!(
            state.apply(&ControlCommand::Admit(member(joiner, "b"))),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert_eq!(state.members[&joiner].health, MemberHealth::Up);
    }

    /// The fencing rule: lower loses, equal from elsewhere loses, higher supersedes
    #[test]
    fn incarnations_are_fenced_highest_wins() {
        let (mut state, _, node) = bootstrapped();
        // a lower incarnation is a run the cluster already replaced
        let mut old = member(node, "a");
        old.incarnation = 0;
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(old.clone())),
            ControlResponse::Fenced {
                committed: 1,
                offered: 0,
                ..
            }
        ));
        assert!(matches!(
            state.apply(&ControlCommand::Admit(old)),
            ControlResponse::Fenced { .. }
        ));
        // an equal one from another control address is a second run of the same copy
        let mut clone = member(node, "a");
        clone.control = "elsewhere:2".to_string();
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(clone)),
            ControlResponse::Fenced {
                committed: 1,
                offered: 1,
                ..
            }
        ));
        // an equal one from the same address is the same run
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(member(node, "a"))),
            ControlResponse::Applied {
                topology_version: 1
            }
        );
        // a higher one from anywhere supersedes
        let mut newer = member(node, "a");
        newer.incarnation = 5;
        newer.control = "elsewhere:2".to_string();
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(newer)),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.members[&node].record.incarnation, 5);
        assert_eq!(state.fenced[&node], 1);
        // and health evidence about the old run is fenced too
        assert!(matches!(
            state.apply(&ControlCommand::SetHealth {
                node,
                health: MemberHealth::Down,
                incarnation: 1,
                episode: Some(Uuid::new_v4()),
            }),
            ControlResponse::Fenced { .. }
        ));
    }

    /// Health moves once per change, a down episode is kept, and a return clears it
    #[test]
    fn health_and_shard_reports_move_the_version_once_per_change() {
        let (mut state, _, node) = bootstrapped();
        let episode = Uuid::new_v4();
        let down = ControlCommand::SetHealth {
            node,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: Some(episode),
        };
        assert_eq!(
            state.apply(&down),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.members[&node].episode, Some(episode));
        assert_eq!(state.members[&node].since, 2);
        assert_eq!(state.up_members(), 0);
        // the same again moves nothing
        assert_eq!(
            state.apply(&down),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        // a member observing itself again is up again
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(member(node, "a"))),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert_eq!(state.members[&node].health, MemberHealth::Up);
        assert_eq!(state.members[&node].episode, None);
        // shard health the same way
        let report = ControlCommand::ReportShards {
            node,
            incarnation: 1,
            failed: vec![1],
        };
        assert_eq!(
            state.apply(&report),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        assert_eq!(
            state.apply(&report),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        assert_eq!(state.members[&node].shards_failed, vec![1]);
        // and an unknown node has nothing to set
        assert!(matches!(
            state.apply(&ControlCommand::SetHealth {
                node: NodeId::mint(),
                health: MemberHealth::Down,
                incarnation: 1,
                episode: None,
            }),
            ControlResponse::Refused { .. }
        ));
    }

    /// An initialization is versioned, checked, applied once, and remembered by operation id
    #[test]
    fn an_initialization_is_versioned_once_and_remembered() {
        let (mut state, _, node) = bootstrapped();
        let joiner = NodeId::mint();
        state.apply(&ControlCommand::Admit(member(joiner, "b")));
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let op = Uuid::new_v4();
        let initialize = |op, expected_version, nodes: Vec<NodeId>| ControlCommand::Initialize {
            op,
            principal: "alice".to_string(),
            expected_version,
            nodes,
            tables: tables.clone(),
        };
        // a stale version is refused, and the refusal is not remembered: the same op sent again
        // against the current version is a fresh attempt, refused here for another reason
        let stale = state.apply(&initialize(op, 0, vec![node]));
        assert!(matches!(&stale, ControlResponse::Refused { reason } if reason.contains("stale")));
        assert!(matches!(
            state.apply(&initialize(op, 2, vec![node, joiner])),
            ControlResponse::Refused { reason } if reason.contains("joining")
        ));
        assert!(state.initialized.is_none());
        // a joining member cannot be placed on
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 2, vec![node, joiner])),
            ControlResponse::Refused { reason } if reason.contains("joining")
        ));
        state.apply(&ControlCommand::ObserveMember(member(joiner, "b")));
        // a duplicate and a stranger are refused too
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 3, vec![node, node])),
            ControlResponse::Refused { reason } if reason.contains("twice")
        ));
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 3, vec![NodeId::mint()])),
            ControlResponse::Refused { reason } if reason.contains("not a member")
        ));
        // the real one applies once
        let op = Uuid::new_v4();
        assert_eq!(
            state.apply(&initialize(op, 3, vec![node, joiner])),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        assert_eq!(state.initialized, Some(vec![node, joiner]));
        assert_eq!(state.tables, tables);
        // two nodes under a factor of three give two copies (F40)
        assert_eq!(state.active_rf(), 2);
        // the same op again is answered as it was, and moves nothing
        assert_eq!(
            state.apply(&initialize(op, 4, vec![joiner])),
            ControlResponse::Repeated {
                first: Box::new(ControlResponse::Applied {
                    topology_version: 4
                })
            }
        );
        assert_eq!(state.initialized, Some(vec![node, joiner]));
        // and a fresh op is refused, naming the migration
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 4, vec![joiner])),
            ControlResponse::Refused { reason } if reason.contains("M9a")
        ));
        assert_eq!(state.operations.len(), 1);
    }

    /// The voter policy is an operation like any other, and only takes 1, 3 or 5
    #[test]
    fn the_voter_policy_is_a_versioned_operation() {
        let (mut state, _, _) = bootstrapped();
        let set = |op, expected_version, count| ControlCommand::SetControlVoters {
            op,
            principal: "alice".to_string(),
            expected_version,
            count,
        };
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 1, 4)),
            ControlResponse::Refused { .. }
        ));
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 1, 5)),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        assert_eq!(state.policy.as_ref().expect("a policy").control_voters, 5);
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 1, 3)),
            ControlResponse::Refused { .. }
        ));
    }

    /// A table's read policy records, clears, moves the version once per change, refuses `All`
    /// and an unknown table, and is idempotent when unchanged (F41)
    #[test]
    fn mixed_table_bundle_resolves_each_table_policy() {
        let (mut state, _, node) = bootstrapped();
        let tables = vec![
            ("Row".to_string(), TableId::of("Row")),
            ("Note".to_string(), TableId::of("Note")),
        ];
        // a policy needs a table the placement was initialized with
        let set = |op, expected_version, table, level| ControlCommand::SetTableReadPolicy {
            op,
            principal: "alice".to_string(),
            expected_version,
            table,
            level,
        };
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 1, TableId::of("Note"), Some(Consistency::Quorum))),
            ControlResponse::Refused { reason } if reason.contains("not one the placement")
        ));
        assert_eq!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "alice".to_string(),
                expected_version: 1,
                nodes: vec![node],
                tables: tables.clone(),
            }),
            ControlResponse::Applied { topology_version: 2 }
        );
        // `All` is not a read level
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 2, TableId::of("Note"), Some(Consistency::All))),
            ControlResponse::Refused { reason } if reason.contains("C6")
        ));
        // a stale version is refused before anything is recorded
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 1, TableId::of("Note"), Some(Consistency::Quorum))),
            ControlResponse::Refused { reason } if reason.contains("stale")
        ));
        // setting records, and moves the version once
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 2, TableId::of("Note"), Some(Consistency::Quorum))),
            ControlResponse::Applied { topology_version: 3 }
        );
        assert_eq!(state.table_read_policy.get(&TableId::of("Note")), Some(&Consistency::Quorum));
        assert_eq!(state.table_read_policy.get(&TableId::of("Row")), None);
        // the same level again applies without moving the version
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 3, TableId::of("Note"), Some(Consistency::Quorum))),
            ControlResponse::Applied { topology_version: 3 }
        );
        // the map resolves each table on its own: Note at its policy, Row at the cluster's
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.read_level_of(TableId::of("Note")), Consistency::Quorum);
        assert_eq!(map.read_level_of(TableId::of("Row")), Consistency::One);
        assert_eq!(map.frame().table_read_policy, vec![("Note".to_string(), "quorum".to_string())]);
        // clearing removes it and moves the version once; clearing again moves nothing
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 3, TableId::of("Note"), None)),
            ControlResponse::Applied { topology_version: 4 }
        );
        assert!(state.table_read_policy.is_empty());
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 4, TableId::of("Note"), None)),
            ControlResponse::Applied { topology_version: 4 }
        );
        // and a repeated op is answered as it was the first time
        let op = Uuid::new_v4();
        assert_eq!(
            state.apply(&set(op, 4, TableId::of("Row"), Some(Consistency::One))),
            ControlResponse::Applied { topology_version: 5 }
        );
        assert!(matches!(
            state.apply(&set(op, 5, TableId::of("Row"), Some(Consistency::Quorum))),
            ControlResponse::Repeated { .. }
        ));
        assert_eq!(state.table_read_policy.get(&TableId::of("Row")), Some(&Consistency::One));
    }

    /// A membership entry sets roles, admits configured strangers as joining, and moves once
    #[test]
    fn a_membership_entry_is_reflected_in_the_roles() {
        let (mut state, _, node) = bootstrapped();
        let learner = NodeId::mint();
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(node, member(node, "a"));
        nodes.insert(learner, member(learner, "b"));
        let membership = openraft::Membership::new(vec![[node].into_iter().collect()], nodes.clone())
            .expect("a membership");
        assert!(state.observe_membership(&membership));
        assert_eq!(state.topology_version, 2);
        assert_eq!(state.members[&node].role, MemberRole::Voter);
        assert_eq!(state.members[&learner].role, MemberRole::Learner);
        assert_eq!(state.members[&learner].health, MemberHealth::Joining);
        // the same again changes nothing
        assert!(!state.observe_membership(&membership));
        assert_eq!(state.topology_version, 2);
        // a promotion moves the role
        let promoted = openraft::Membership::new(vec![[node, learner].into_iter().collect()], nodes)
            .expect("a membership");
        assert!(state.observe_membership(&promoted));
        assert_eq!(state.members[&learner].role, MemberRole::Voter);
        let mut both = vec![node, learner];
        both.sort();
        assert_eq!(state.voters(), both);
        assert!(state.learners().is_empty());
    }
}
