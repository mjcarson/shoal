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
//!
//! **A member's phase is the operator's and the policy's; its health is the detector's.**
//! Since [F46](../../../../docs/src/features/capacity-rebalancing.md) a member carries both:
//! `Leaving`, `Removing` and `Removed` are phases a `Decommission`, a `Remove` or an elapsed
//! grace commit, and a member in any of them can still be up or down. The six-state machine
//! [C3](../../../../docs/src/distributed/membership.md) draws is the two read together
//! ([`MemberState::state_name`]), and every `Up` check in the tree keeps its meaning.

use std::collections::BTreeMap;
use std::fmt;

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::server::errors::ShoalError;

use super::schema::SchemaRecord;
use super::backup::{
    judge_coverage, ArchiveFile, ArchivePhase, ArchiveRestore, BackupFile, BackupOutcome,
    BackupPhase, BackupRecord, GroupBackup, GroupRestore, PlacedGroup, RecoveryRecord,
    RestorePhase, RestoreRecord, KEPT_BACKUPS, KEPT_RESTORES,
};
use super::migrate::{
    DataConfiguration, GroupMove, MoveOutcome, MovePhase, MoveRecord, KEPT_MOVES,
};
use super::plan::{Blocked, PlanKind, PlanOutcome, PlanPhase, PlanRecord, PlanUpdate, KEPT_PLANS};
use super::repair::{
    GroupRepair, QuarantinedCopy, RepairMode, RepairPhase, RepairRecord, KEPT_REPAIRS,
};
use super::runtime::GlommioRuntime;
use crate::server::conf::cluster::{BootstrapPolicy, Consistency};
use crate::shared::poolmap::PoolMap;
use crate::shared::pools::{ObjectPolicy, PoolsConf};
use crate::shared::domains::{domain_at, HOST, LEVELS};
use crate::shared::grants::{check_grant, GrantError, GrantOn, Privileges, Served};
use crate::shared::identity::{BucketId, ClusterId, GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::{
    MIN_PEER_VERSION, OBJECT_WIRE_VERSION, POOL_MAP_WIRE_VERSION, RECLAIM_WIRE_VERSION,
    RECOVERY_WIRE_VERSION, RESTORE_FENCE_WIRE_VERSION, SCHEMA_WIRE_VERSION, SCRUB_WIRE_VERSION,
};

declare_raft_types!(
    /// The control group's type configuration
    pub ControlConfig:
        D = ControlCommand,
        R = ControlResponse,
        NodeId = NodeId,
        Node = MemberRecord,
        AsyncRuntime = GlommioRuntime,
);

/// The node id type the control group is declared over, for a store that names it
pub type NodeIdOf = NodeId;

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
    /// How many slots it has: the shard in every address the rule mints for it, and the modulus
    /// of the rule, claimed once ([F47](../../../../docs/src/features/local-rehome.md))
    pub shards: usize,
    /// How many executors it runs, if that differs from its slots; zero means the slots
    ///
    /// The node's own business as far as placement goes - a slot is what an address names -
    /// but what its weight defaults to, since the cores are what do the work
    /// ([F47](../../../../docs/src/features/local-rehome.md)).
    #[serde(default)]
    pub physical: usize,
    /// Which start of the node this record was written by
    ///
    /// From the node's storage marker, bumped on every start. The cluster's fencing rule is
    /// that the highest wins: a record at a lower incarnation than the committed one is
    /// refused, and a member superseded by a higher one is told so and stops
    /// ([C1](../../../../docs/src/distributed/node-identity.md), Q11).
    #[serde(default)]
    pub incarnation: u64,
    /// The share of the cluster's bytes this node is meant to hold, against the others' weights
    ///
    /// Zero means the node's shard count, so a cluster of like machines needs no weights at
    /// all. Set by the node's own `cluster.weight` and recorded when it observes itself, so a
    /// change is a restart ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    #[serde(default)]
    pub weight: u32,
    /// This node's share of the groups' leads, against the other voters'; zero means one
    ///
    /// Set by the node's own `cluster.lead_weight` and recorded when it observes itself
    /// ([F58](../../../../docs/src/features/weighted-leadership.md)).
    #[serde(default)]
    pub lead_weight: u32,
    /// Where it stands, by level: `host` and the machine it runs on, and `rack` and `zone` when
    /// it names them
    ///
    /// Set by the node's own `cluster.failure_domains`, the host defaulting to the name the
    /// operating system gives its machine, and recorded when it observes itself, so a change is
    /// a restart. A map of levels so a level is another key and not a format change; a record
    /// with none, from before F81 or a test's, stands in a domain of its own
    /// ([F81](../../../../docs/src/features/failure-domains.md)), and a level it leaves out is
    /// read as the nearest one below it names ([`domain_at`](crate::shared::domains::domain_at),
    /// [F83](../../../../docs/src/features/rack-and-zone-domains.md)).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub failure_domains: BTreeMap<String, String>,
    /// The oldest wire version this member reads; zero, from a record before F48, is the floor
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md))
    #[serde(default)]
    pub wire_min: u8,
    /// The newest wire version this member speaks; zero, from a record before F48, is the floor
    ///
    /// Reported at every start, so what a rolling upgrade has reached is a committed fact and an
    /// activation is judged against records rather than against whoever happens to be dialled.
    #[serde(default)]
    pub wire_max: u8,
    /// What this member can act on, as the hello's capability bits
    #[serde(default)]
    pub capabilities: u64,
    /// The structural fingerprint of the schema this member serves
    #[serde(default)]
    pub schema_id: u64,
    /// The lineage of that schema: its id after each of its fields
    ///
    /// Reported at every start, so the schemas a cluster's builds hold are committed facts and
    /// an `ExtendSchema` is judged against records, the same on every replica
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_lineage: Vec<u64>,
    /// The build this member runs, as its package version, for an operator reading `Members`
    #[serde(default)]
    pub build: String,
}

impl MemberRecord {
    /// The weight the planner uses: the configured one, or the executor count
    ///
    /// The executors when the record says how many, else the slots, which is what every record
    /// before F47 meant by `shards`.
    #[must_use]
    pub fn effective_weight(&self) -> u32 {
        if self.weight > 0 {
            self.weight
        } else {
            u32::try_from(self.executors()).unwrap_or(u32::MAX).max(1)
        }
    }

    /// The host this member stands on, if its record names one
    ///
    /// What placement spreads copies over: two members naming one host fail together
    /// ([F81](../../../../docs/src/features/failure-domains.md)).
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.failure_domains.get(HOST).map(String::as_str)
    }

    /// How many executors this member runs: `physical` when recorded, else its slots
    #[must_use]
    pub fn executors(&self) -> usize {
        if self.physical > 0 {
            self.physical
        } else {
            self.shards
        }
    }

    /// The newest wire version this member speaks, reading a record from before F48 as the floor
    #[must_use]
    pub fn wire_max(&self) -> u8 {
        self.wire_max.max(MIN_PEER_VERSION)
    }

    /// The oldest wire version this member reads, reading a record from before F48 as the floor
    #[must_use]
    pub fn wire_min(&self) -> u8 {
        self.wire_min.max(MIN_PEER_VERSION)
    }
}

impl fmt::Display for MemberRecord {
    /// The node and where it is reached
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}#{}", self.node, self.client, self.incarnation)
    }
}

/// Whether a member is being admitted, is up, or has been called down
///
/// The detector's half of [C3](../../../../docs/src/distributed/membership.md)'s member state
/// machine; the operator's half is [`MemberPhase`], and the two are read together.
/// `Unreachable` is a local observation and deliberately not a value here.
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

/// Where a member stands with the cluster, as an operator or the policy decided
///
/// The other half of [C3](../../../../docs/src/distributed/membership.md)'s member state
/// machine ([F46](../../../../docs/src/features/capacity-rebalancing.md)): a plain member is
/// a placement target; a leaving one still serves and counts but takes no new placement; a
/// removing one is having its sets rebuilt elsewhere and cannot be reversed by a late
/// heartbeat; a removed one is tombstoned and never comes back under its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberPhase {
    /// A member like any other
    #[default]
    Member,
    /// An operator asked for it to be drained; no new placement lands on it
    Leaving,
    /// Its grace elapsed or an operator asked for its removal; its sets are being rebuilt
    Removing,
    /// Tombstoned: out of the control group, its identity refused for good
    Removed,
}

impl MemberPhase {
    /// The name this phase is spelled as on the wire and in a log line
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            MemberPhase::Member => "member",
            MemberPhase::Leaving => "leaving",
            MemberPhase::Removing => "removing",
            MemberPhase::Removed => "removed",
        }
    }
}

/// The grace a down member is under before it is removed on its own
///
/// Elapsed time is committed in increments the leader accrues, never a wall-clock deadline,
/// so a leader change loses at most one increment and never restarts or skips a grace
/// ([F46](../../../../docs/src/features/capacity-rebalancing.md), Q7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraceState {
    /// The down episode the grace belongs to
    pub episode: Uuid,
    /// How much of the grace has been committed as elapsed, in milliseconds
    pub elapsed_ms: u64,
    /// Whether an operator has suspended the count for maintenance
    pub suspended: bool,
    /// Whether the grace has elapsed and the member is being removed for it
    pub expired: bool,
    /// The plan the expiry recorded, once it did
    pub plan: Option<Uuid>,
}

/// A removed member's identity, kept so it can never rejoin as an authority
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The incarnation the member was last admitted at
    pub incarnation: u64,
    /// The topology version it was removed at
    pub removed_at: u64,
    /// The plan that removed it
    pub op: Option<Uuid>,
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
    /// The copies it holds that are quarantined, as it last reported
    /// ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub quarantined: Vec<QuarantinedCopy>,
    /// The topology version its health last changed at
    #[serde(default)]
    pub since: u64,
    /// The identity of its current down episode, if it is down
    ///
    /// Minted by the leader that committed the episode, so that a grace timer or a removal
    /// (M9b) names the episode it belongs to and a later episode is not mistaken for it.
    #[serde(default)]
    pub episode: Option<Uuid>,
    /// Where it stands with the cluster: a plain member, leaving, removing or removed
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default)]
    pub phase: MemberPhase,
    /// The grace its current down episode is under, if it is down and the policy removes
    #[serde(default)]
    pub grace: Option<GraceState>,
}

impl MemberState {
    /// A member as it is first recorded: at a health, with nothing else decided about it
    ///
    /// # Arguments
    ///
    /// * `record` - What it advertises
    /// * `health` - Its health
    /// * `role` - Its role
    /// * `since` - The topology version it is recorded at
    #[must_use]
    pub fn fresh(record: MemberRecord, health: MemberHealth, role: MemberRole, since: u64) -> Self {
        MemberState {
            record,
            health,
            role,
            shards_failed: Vec::new(),
            quarantined: Vec::new(),
            since,
            episode: None,
            phase: MemberPhase::Member,
            grace: None,
        }
    }

    /// The one name C3's six-state machine gives this member: the phase when it has one past
    /// plain membership, the health otherwise
    #[must_use]
    pub const fn state_name(&self) -> &'static str {
        match self.phase {
            MemberPhase::Member => self.health.name(),
            other => other.name(),
        }
    }

    /// Whether the member may be placed on: up, and a plain member
    #[must_use]
    pub fn is_placeable(&self) -> bool {
        self.health == MemberHealth::Up && self.phase == MemberPhase::Member
    }
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
        /// The buckets the proposer's schema declares, and the tables each generated
        /// ([F79](../../../../docs/src/features/bucket-schema.md))
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        buckets: Vec<BucketRecord>,
        /// The proposer's schema, which the cluster serves from then on until an `ExtendSchema`
        /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
        #[serde(default)]
        schema: SchemaRecord,
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
    /// Grant a principal privileges on a table or a bucket, on top of what it holds there
    ///
    /// Committed policy rather than a per-node setting, so every coordinator holds a principal
    /// to the same grants ([F77](../../../../docs/src/features/authorization.md),
    /// [F79](../../../../docs/src/features/bucket-schema.md)).
    Grant {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The principal granted
        grantee: String,
        /// Whether the grant is on a table or a bucket
        #[serde(default)]
        on: GrantOn,
        /// The table or bucket, as the schema spells it, or `*`
        name: String,
        /// What is granted
        privileges: Privileges,
    },
    /// Take back privileges a grant on a table or a bucket gave a principal
    Revoke {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The principal revoked
        grantee: String,
        /// Whether the grant was on a table or a bucket
        #[serde(default)]
        on: GrantOn,
        /// The name the grant was made on, as the schema spells it, or `*`
        name: String,
        /// What is taken back
        privileges: Privileges,
    },
    /// Scrub a table's groups, judge the copies, and repair or release
    /// ([F44](../../../../docs/src/features/repair.md))
    Repair {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The table
        table: TableId,
        /// One tablet, or every tablet of the table
        tablet: Option<u16>,
        /// What was asked
        mode: RepairMode,
        /// The copy an operator named as trusted
        source: Option<NodeId>,
        /// Whether to lift the quarantines rather than judge
        release: bool,
    },
    /// A group's driver says where its repair stands
    ///
    /// A node's proposal, not an operator's: it carries no operation id of its own and no
    /// version, and moves the topology so every node's map carries the progress
    /// ([F44](../../../../docs/src/features/repair.md)).
    RepairProgress {
        /// The operation
        op: Uuid,
        /// The group
        group: GroupId,
        /// The node driving it
        node: NodeId,
        /// The incarnation it drives at
        incarnation: u64,
        /// Where the group stands now
        progress: GroupRepair,
    },
    /// A member says which of its copies are quarantined
    /// ([F44](../../../../docs/src/features/repair.md))
    ReportQuarantine {
        /// The member
        node: NodeId,
        /// The incarnation it reported at
        incarnation: u64,
        /// Every quarantined copy it holds
        copies: Vec<QuarantinedCopy>,
    },
    /// Move the replica set holding a tablet from one member to another
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    Move {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// A tablet the set serves
        tablet: u16,
        /// The member leaving the set
        from: NodeId,
        /// The member replacing it
        to: NodeId,
    },
    /// A group's driver says where its move stands
    ///
    /// A node's proposal like `RepairProgress`: no operation id of its own and no version.
    /// The one that carries the last group's `Activated` publishes the configuration, and the
    /// one that carries the last `Done` finishes the record and releases what queued behind
    /// it - both in apply, so every member derives the same map
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    MoveProgress {
        /// The operation
        op: Uuid,
        /// The group
        group: GroupId,
        /// The node driving it
        node: NodeId,
        /// The incarnation it drives at
        incarnation: u64,
        /// Where the group stands now
        progress: GroupMove,
    },
    /// Drain a live member: mark it leaving and plan every set it holds elsewhere
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    Decommission {
        /// The identity of the operation, which is the plan's too
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The member
        node: NodeId,
    },
    /// Remove a down or leaving member: mark it removing and plan its sets elsewhere
    Remove {
        /// The identity of the operation, which is the plan's too
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The member
        node: NodeId,
        /// The member to take its place first, if the operator named one
        replacement: Option<NodeId>,
    },
    /// Suspend or resume a down member's grace
    Maintenance {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The member
        node: NodeId,
        /// Whether to suspend the count, or resume it
        suspend: bool,
    },
    /// Spread the sets over the members by their weights and measured bytes
    Rebalance {
        /// The identity of the operation, which is the plan's too
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
    },
    /// The leader's word on how much of a down member's grace has elapsed
    ///
    /// A leader's bookkeeping, no operation id: applied only for the episode it names and
    /// never backwards. With `expire` the grace is over and the member is removing under the
    /// plan named.
    GraceElapsed {
        /// The member
        node: NodeId,
        /// The down episode
        episode: Uuid,
        /// How much has elapsed, in milliseconds, committed and local together
        elapsed_ms: u64,
        /// The plan to record the removal under, when the grace is over
        expire: Option<Uuid>,
    },
    /// The leader's word on where a plan stands
    PlanProgress {
        /// The plan
        op: Uuid,
        /// The leader
        node: NodeId,
        /// The incarnation it leads at
        incarnation: u64,
        /// What changed
        progress: PlanUpdate,
    },
    /// A removed member's identity, tombstoned for good
    Tombstone {
        /// The member
        node: NodeId,
        /// The plan that removed it
        op: Option<Uuid>,
    },
    /// Back a table, or every table, up: one file per group, cut at a committed boundary each
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md))
    Backup {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The table, or none for every table
        table: Option<TableId>,
        /// The directory the files go under, on every node that writes one
        path: String,
    },
    /// A group's driver says where its backup stands
    ///
    /// A node's proposal like `RepairProgress`: no operation id of its own and no version.
    BackupProgress {
        /// The operation
        op: Uuid,
        /// The group
        group: GroupId,
        /// The node driving it
        node: NodeId,
        /// The incarnation it drives at
        incarnation: u64,
        /// Where the group stands now
        progress: GroupBackup,
    },
    /// Restore a backup into this cluster, which has to be one that has restored nothing
    /// and whose restored tables hold nothing ([F49](../../../../docs/src/features/backup-and-recovery.md))
    Restore {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The directory the files are read from, on every node
        path: String,
        /// The cluster the files were cut in
        source: ClusterId,
        /// Every file, as the leader read the manifests
        files: Vec<BackupFile>,
        /// Every archive of a bucket's objects, as the leader read their manifests (F93)
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        archives: Vec<ArchiveFile>,
    },
    /// Drive every group a finished restore failed again, from the phase each failed in
    /// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md))
    RetryRestore {
        /// The identity of this request
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The restore operation to retry
        restore: Uuid,
    },
    /// A group's driver says where its restore stands
    RestoreProgress {
        /// The operation
        op: Uuid,
        /// The group
        group: GroupId,
        /// The node driving it
        node: NodeId,
        /// The incarnation it drives at
        incarnation: u64,
        /// Where the group stands now
        progress: GroupRestore,
    },
    /// An archive's driver says how far its objects are put again
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    ArchiveProgress {
        /// The operation
        op: Uuid,
        /// The archive, by its name under the backup directory
        archive: String,
        /// The node driving it
        node: NodeId,
        /// The incarnation it drives at
        incarnation: u64,
        /// Where the archive stands now
        progress: ArchiveRestore,
    },
    /// An operator rewrote a stopped survivor's membership after a permanent majority loss
    ///
    /// Written by `force_recover` into the survivor's log, offline, and applied when it starts:
    /// every lost member is removed and tombstoned and the recovery is recorded as evidence
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    ForceRecovered {
        /// The identity of the recovery, which the plans it opens are derived from
        op: Uuid,
        /// The members kept
        survivors: Vec<NodeId>,
        /// The members lost
        lost: Vec<NodeId>,
        /// The node the recovery was run on
        at: NodeId,
        /// The control log index the survivor had committed when it was recovered
        last_committed: u64,
        /// When, in milliseconds since the epoch
        recovered_ms: u64,
    },
    /// Activate a wire version: every member speaks it from here on, and none rolls back past it
    ///
    /// The versions every member's running build reports ride in the command, read by the
    /// leader from the members' status reports, and apply judges those: refused unless every
    /// member in any phase but `Removed` is named at or above the version, and never lowers
    /// what is activated. The committed records are not judged, because a build from before
    /// the field persisted every record without it, so replicas restored on such a build hold
    /// records that differ from the leader's; the command's claim is the same on every replica,
    /// and applying it writes the versions into the records, which heals them
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    Activate {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The version to activate
        wire: u8,
        /// The newest version each member reported, as the leader heard it
        members: BTreeMap<NodeId, u8>,
    },
    /// Move the committed schema to one that appends tables or buckets to it, once every
    /// member's build holds it ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    ///
    /// Judged against the members' committed records, which every start writes again, so every
    /// replica applies it alike; applied, the new tables are the map's and their groups start, and
    /// each new bucket is bound to its pool past every consumer id already given.
    ExtendSchema {
        /// The identity of the operation, so a repeat is answered as the first was
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The schema to move to, which has to extend the committed one
        schema: SchemaRecord,
        /// Every table it serves, with their stable identities, the committed ones first
        tables: Vec<(String, TableId)>,
        /// Every bucket it declares, the committed ones first
        buckets: Vec<BucketRecord>,
        /// Every pool and every bucket's binding, the committed ones unchanged
        pools: PoolsConf,
    },
    /// Commit what a node claimed of its devices into the pool map
    ///
    /// Proposed by the node itself once it has observed itself, at the incarnation it observed
    /// itself at; a device new to the map joins under a seat minted from its id, and one whose
    /// class, slices or weight changed begins an era
    /// ([F85](../../../../docs/src/features/devices-and-pools.md)).
    ObserveDevices {
        /// The node
        node: NodeId,
        /// The incarnation it claimed them at
        incarnation: u64,
        /// What it claimed
        devices: Vec<crate::shared::devices::DeviceFacts>,
    },
    /// Record which of a node's devices it reports failed, in the order it reported them
    ///
    /// Proposed by the control leader when a member's report names a set of failed devices the map
    /// does not hold; refused unless its report is newer than the last one committed, so two
    /// reports committed out of order never leave the older one standing.
    ReportDevices {
        /// The node
        node: NodeId,
        /// The incarnation it reported at
        incarnation: u64,
        /// The report's sequence within that incarnation
        seq: u64,
        /// Each failed device and why
        failed: BTreeMap<crate::shared::devices::DeviceId, String>,
        /// Its devices whose era ended that hold nothing on any slice, which a drained device's
        /// removal is admitted by (F92)
        #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
        empty: std::collections::BTreeSet<crate::shared::devices::DeviceId>,
    },
    /// A pool's placement weights, as the control leader fitted them at a generation
    SetPoolWeights {
        /// The pool
        pool: String,
        /// The generation they were fitted at; refused once the map has moved past it
        generation: u64,
        /// Each device's placement weight
        weights: BTreeMap<crate::shared::devices::DeviceId, u64>,
    },
    /// One consumer's exceptions in a pool, as the control leader computed them at a generation
    SetPoolExceptions {
        /// The pool
        pool: String,
        /// The consumer
        consumer: u64,
        /// The generation they were computed at; refused once the map has moved past it
        generation: u64,
        /// Each placement group's slice the rule names and the slice used instead
        exceptions: Vec<(
            crate::shared::poolmap::PlacementGroup,
            crate::shared::poolmap::SliceRef,
            crate::shared::poolmap::SliceRef,
        )>,
    },
    /// Fix the generation a pool places its stripes at, the first time it is ready
    ///
    /// Proposed by the control leader once a pool's weights and exceptions are fitted at the map's
    /// generation and its readiness says ready there. Refused for a pool already pinned elsewhere,
    /// at a generation the map has not reached, or before the object wire is activated. The pin
    /// moves no generation: it says where stripes are, and nothing moves it. Since wire 10 it is
    /// the base a placement group that never moved sits at, and the pool's groups move to its
    /// target ([F86](../../../../docs/src/features/replicated-pools.md),
    /// [F88](../../../../docs/src/features/recovery-and-moves.md#moves)).
    PinPool {
        /// The pool
        pool: String,
        /// The generation it places at
        generation: u64,
    },
    /// Move a pool's target to a generation it is fitted and ready at, so its placement groups
    /// move there ([F88](../../../../docs/src/features/recovery-and-moves.md))
    ///
    /// Proposed by the control leader, as a pin is. Refused before wire version 10, for a pool not
    /// pinned yet, or at a generation the map has not reached; a target at or past it is kept.
    TargetPool {
        /// The pool
        pool: String,
        /// The generation its groups move to
        generation: u64,
    },
    /// End a failed device's era, so placement stops naming it and its groups move off (F88)
    ///
    /// Proposed by the control leader for a device its node reports failed while the node is up:
    /// a down node's devices are held through its grace instead.
    RetireDevice {
        /// The device
        device: crate::shared::devices::DeviceId,
    },
    /// Admit one placement group's move: at most `MOVES_A_DEVICE` arriving on a device at once (F88)
    ///
    /// Proposed by the leader of the group's tablet group before it begins the move. Refused while
    /// sixteen admitted moves arrive on one of its devices, which the driver tries again later.
    AdmitMove {
        /// The move
        slot: crate::shared::poolmap::MoveSlot,
    },
    /// An operator's order to fail a device, which its node carries out as an I/O error (F88)
    ///
    /// Since [F93](../../../../docs/src/features/operations-and-the-real-cluster.md) an operation
    /// like every other: remembered by its id, so a repeat is answered as the first was, and
    /// judged against the version it was written at.
    FailDevice {
        /// The identity of the operation; the nil id for one the cluster asks of itself
        #[serde(default)]
        op: Uuid,
        /// Who asked
        #[serde(default)]
        principal: String,
        /// The topology version the request was written against
        #[serde(default)]
        expected_version: u64,
        /// The device
        device: crate::shared::devices::DeviceId,
        /// Why, for the device's report
        reason: String,
    },
    /// An operator's request to scrub a pool now, counted on its pool map: the leader of every
    /// tablet group of a bucket it serves begins a pass for it (F89)
    ///
    /// Since F93 an operation with its id and version, as every operator's is, and narrowed to a
    /// bucket, a device or a slice if it names one; a request the cluster asks of itself, a lost
    /// journal's, carries the nil id and checks no version.
    ScrubNow {
        /// The identity of the operation; the nil id for one the cluster asks of itself
        #[serde(default)]
        op: Uuid,
        /// Who asked
        #[serde(default)]
        principal: String,
        /// The topology version the request was written against
        #[serde(default)]
        expected_version: u64,
        /// The pool
        pool: String,
        /// Light or deep
        kind: crate::shared::scrub::ScrubKind,
        /// What the pass is narrowed to, or none for the whole pool
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<crate::shared::scrub::ScrubTarget>,
    },
    /// Drain a device: its era ends so its placement groups move off it, while it serves what
    /// they copy from it ([F92](../../../../docs/src/features/reclamation-and-device-lifecycle.md))
    ///
    /// An operator's, or the control leader's for a node that reports a device past its checksum
    /// drain threshold. Judged on the device alone, so it checks no version: a drain asked again
    /// is the drain begun.
    DrainDevice {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The device
        device: crate::shared::devices::DeviceId,
        /// Why, kept on its record
        reason: String,
    },
    /// Remove a device from the pool map for good, tombstoning its id (F92)
    ///
    /// Admitted for a device that was retired, for one drained whose node reported every slice
    /// empty with no move arriving on it and every pool it served targeted past its era, or on the
    /// operator's word that it is lost.
    RemoveDevice {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The device
        device: crate::shared::devices::DeviceId,
        /// The operator's word that the device is gone, whatever it held
        lost: bool,
    },
    /// Give a device the weight an operator chose, beginning an era at it (F92)
    ReweightDevice {
        /// The identity of the operation
        op: Uuid,
        /// Who asked
        principal: String,
        /// The topology version the request was written against
        expected_version: u64,
        /// The device
        device: crate::shared::devices::DeviceId,
        /// Its share of a pool
        weight: u64,
    },
    /// Free a placement group's move once it switched or aborted (F88)
    MoveDone {
        /// The pool
        pool: String,
        /// The consumer
        consumer: u64,
        /// The placement group
        pg: crate::shared::poolmap::PlacementGroup,
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
            ControlCommand::Grant {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "grant", *expected_version)),
            ControlCommand::Revoke {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "revoke", *expected_version)),
            ControlCommand::Repair {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "repair", *expected_version)),
            ControlCommand::Move {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "move", *expected_version)),
            ControlCommand::Decommission {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "decommission", *expected_version)),
            ControlCommand::Remove {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "remove", *expected_version)),
            ControlCommand::Maintenance {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "maintenance", *expected_version)),
            ControlCommand::Rebalance {
                op,
                principal,
                expected_version,
            } => Some((*op, principal, "rebalance", *expected_version)),
            ControlCommand::Activate {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "activate", *expected_version)),
            ControlCommand::ExtendSchema {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "extend_schema", *expected_version)),
            // an operator's order and request are remembered; the cluster's own are not (F93)
            ControlCommand::FailDevice {
                op,
                principal,
                expected_version,
                ..
            } if !op.is_nil() => Some((*op, principal, "fail_device", *expected_version)),
            ControlCommand::ScrubNow {
                op,
                principal,
                expected_version,
                ..
            } if !op.is_nil() => Some((*op, principal, "scrub", *expected_version)),
            ControlCommand::Backup {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "backup", *expected_version)),
            ControlCommand::Restore {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "restore", *expected_version)),
            ControlCommand::RetryRestore {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "retry_restore", *expected_version)),
            ControlCommand::DrainDevice {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "drain_device", *expected_version)),
            ControlCommand::RemoveDevice {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "remove_device", *expected_version)),
            ControlCommand::ReweightDevice {
                op,
                principal,
                expected_version,
                ..
            } => Some((*op, principal, "reweight_device", *expected_version)),
            _ => None,
        }
    }
}

impl fmt::Display for ControlCommand {
    /// Name the command, which is what openraft's traces print
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlCommand::Bootstrap {
                cluster, member, ..
            } => {
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
            ControlCommand::Initialize { nodes, .. } => {
                write!(f, "Initialize({} nodes)", nodes.len())
            }
            ControlCommand::SetControlVoters { count, .. } => {
                write!(f, "SetControlVoters({count})")
            }
            ControlCommand::SetTableReadPolicy { table, level, .. } => {
                write!(
                    f,
                    "SetTableReadPolicy({table} {})",
                    level.map_or("clear", |level| level.as_str())
                )
            }
            ControlCommand::Grant {
                grantee,
                on,
                name,
                privileges,
                ..
            } => write!(f, "Grant({grantee} {on} {name} {privileges})"),
            ControlCommand::Revoke {
                grantee,
                on,
                name,
                privileges,
                ..
            } => write!(f, "Revoke({grantee} {on} {name} {privileges})"),
            ControlCommand::Repair {
                op, table, mode, ..
            } => write!(f, "Repair({op} {table} {})", mode.as_str()),
            ControlCommand::RepairProgress {
                op,
                group,
                progress,
                ..
            } => {
                write!(f, "RepairProgress({op} {group} {:?})", progress.phase)
            }
            ControlCommand::ReportQuarantine { node, copies, .. } => {
                write!(f, "ReportQuarantine({node} {} copies)", copies.len())
            }
            ControlCommand::Move {
                op,
                tablet,
                from,
                to,
                ..
            } => write!(f, "Move({op} tablet {tablet} {from} -> {to})"),
            ControlCommand::MoveProgress {
                op,
                group,
                progress,
                ..
            } => {
                write!(f, "MoveProgress({op} {group} {})", progress.phase.name())
            }
            ControlCommand::Decommission { op, node, .. } => write!(f, "Decommission({op} {node})"),
            ControlCommand::Remove {
                op,
                node,
                replacement,
                ..
            } => {
                write!(f, "Remove({op} {node} replacement {replacement:?})")
            }
            ControlCommand::Maintenance {
                op, node, suspend, ..
            } => write!(f, "Maintenance({op} {node} suspend {suspend})"),
            ControlCommand::Rebalance { op, .. } => write!(f, "Rebalance({op})"),
            ControlCommand::GraceElapsed {
                node,
                elapsed_ms,
                expire,
                ..
            } => {
                write!(
                    f,
                    "GraceElapsed({node} {elapsed_ms}ms expire {})",
                    expire.is_some()
                )
            }
            ControlCommand::PlanProgress { op, progress, .. } => {
                write!(f, "PlanProgress({op} {progress:?})")
            }
            ControlCommand::Tombstone { node, .. } => write!(f, "Tombstone({node})"),
            ControlCommand::Activate { op, wire, .. } => write!(f, "Activate({op} wire {wire})"),
            ControlCommand::ExtendSchema { op, schema, .. } => {
                write!(f, "ExtendSchema({op} to {:#018x})", schema.id)
            }
            ControlCommand::ObserveDevices { node, devices, .. } => {
                write!(f, "ObserveDevices({node} {} devices)", devices.len())
            }
            ControlCommand::ReportDevices {
                node, seq, failed, ..
            } => write!(f, "ReportDevices({node} seq {seq} {} failed)", failed.len()),
            ControlCommand::SetPoolWeights {
                pool, generation, ..
            } => write!(f, "SetPoolWeights({pool} at {generation})"),
            ControlCommand::SetPoolExceptions {
                pool,
                consumer,
                generation,
                exceptions,
            } => write!(
                f,
                "SetPoolExceptions({pool} consumer {consumer} at {generation}: {})",
                exceptions.len()
            ),
            ControlCommand::TargetPool { pool, generation } => {
                write!(f, "TargetPool({pool} at {generation})")
            }
            ControlCommand::RetireDevice { device } => write!(f, "RetireDevice({device})"),
            ControlCommand::FailDevice { device, .. } => write!(f, "FailDevice({device})"),
            ControlCommand::DrainDevice { op, device, .. } => write!(f, "DrainDevice({op} {device})"),
            ControlCommand::RemoveDevice { op, device, lost, .. } => {
                write!(f, "RemoveDevice({op} {device}{})", if *lost { " lost" } else { "" })
            }
            ControlCommand::ReweightDevice { op, device, weight, .. } => {
                write!(f, "ReweightDevice({op} {device} to {weight})")
            }
            ControlCommand::ScrubNow { pool, kind, .. } => write!(f, "ScrubNow({pool} {})", kind.as_str()),
            ControlCommand::AdmitMove { slot } => write!(
                f,
                "AdmitMove({} {} {:?} to {})",
                slot.pool, slot.consumer, slot.pg, slot.generation
            ),
            ControlCommand::MoveDone { pool, consumer, pg } => {
                write!(f, "MoveDone({pool} {consumer} {pg:?})")
            }
            ControlCommand::PinPool { pool, generation } => {
                write!(f, "PinPool({pool} at {generation})")
            }
            ControlCommand::Backup {
                op, table, path, ..
            } => write!(f, "Backup({op} {table:?} to {path})"),
            ControlCommand::BackupProgress {
                op,
                group,
                progress,
                ..
            } => {
                write!(f, "BackupProgress({op} {group} {:?})", progress.phase)
            }
            ControlCommand::Restore {
                op, source, path, ..
            } => write!(f, "Restore({op} from {source} at {path})"),
            ControlCommand::RetryRestore { op, restore, .. } => {
                write!(f, "RetryRestore({op} of {restore})")
            }
            ControlCommand::RestoreProgress {
                op,
                group,
                progress,
                ..
            } => {
                write!(f, "RestoreProgress({op} {group} {:?})", progress.phase)
            }
            ControlCommand::ArchiveProgress {
                op,
                archive,
                progress,
                ..
            } => {
                write!(f, "ArchiveProgress({op} {archive} {:?} {})", progress.phase, progress.put)
            }
            ControlCommand::ForceRecovered {
                survivors, lost, ..
            } => {
                write!(
                    f,
                    "ForceRecovered({} survivors, {} lost)",
                    survivors.len(),
                    lost.len()
                )
            }
        }
    }
}

/// Why a command was refused, as a kind a caller can act on
///
/// The sentence beside it is for the log; this is for the code. `handle_admin` maps each kind to
/// an [`ErrorCode`](crate::shared::protocol::error::ErrorCode), so a client sees *why* it was
/// refused without parsing the sentence, and a reason whose wording changes changes no code
/// ([Resolved #98](../../../../docs/src/appendix/resolved/admin-refusal-kinds.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RefusalKind {
    /// The node named is not a member of this cluster
    NotMember,
    /// The member named is not up, and the command needs an up one
    NotUp,
    /// The member named is in the wrong phase or under the wrong grace for the command
    WrongPhase,
    /// A node, a set or a plan is named twice, or already holds what it would be given
    Duplicate,
    /// The cluster, the placement or the restore is already done and cannot be done again
    AlreadyInitialized,
    /// No cluster has been bootstrapped or no placement initialized to run the command in
    NotInitialized,
    /// The request was written against a topology version the cluster has moved past
    StaleVersion,
    /// The control voter count is not one the policy allows
    BadVoterCount,
    /// The operation, group, table or tablet named is not one the cluster records
    UnknownOperation,
    /// The operation is queued behind another transition on the same set
    Queued,
    /// The wire version named cannot be activated or is below what the command needs
    WireVersion,
    /// The request is malformed as stated, whatever the cluster's state
    Invalid,
    /// A drive of an operation another has taken over: a restore's group driven under a newer
    /// lead, or a try of it the operation retried since (item 336)
    Superseded,
    /// A reason the kinds above do not name, which a peer on an older wire version decodes to
    #[default]
    Other,
}

/// The version a verify scrub the scheduler asks for is proposed at: judged against no version
///
/// Honoured only by a `Repair` in `Verify` mode, which changes no placement and derives its groups
/// from the state it applies to ([#195](../../../../docs/src/appendix/resolved/scheduled-scrub-starved.md)).
pub const ANY_VERSION: u64 = u64::MAX;

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
        /// Why, as a sentence for the log
        reason: String,
        /// Why, as a kind a caller can act on
        ///
        /// Defaulted on decode so a response from a peer that predates the kind still reads.
        #[serde(default)]
        kind: RefusalKind,
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
    /// Refused because the node's identity is tombstoned
    ///
    /// The proposer is a removed member, whatever its incarnation: a run of it, a clone of
    /// it, or a rejoin under its identity, and it must stop. A replacement joins as a new
    /// identity ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    Removed {
        /// The node
        node: NodeId,
    },
    /// Applied, but a restore's group stands past the phase its drive named, which goes on from
    /// the record rather than moving it back
    ///
    /// The drive's lead is taken, so a drive under the lead before it is refused from then on;
    /// the phase, the attempts and the file installed are the record's. Answered only once
    /// [`RESTORE_FENCE_WIRE_VERSION`] is activated (item 336).
    RestoreAhead {
        /// The topology version after the command
        topology_version: u64,
        /// Where the group stands
        record: GroupRestore,
    },
}

impl ControlResponse {
    /// Refuse a command, with the kind a caller acts on and the sentence the log carries
    ///
    /// # Arguments
    ///
    /// * `kind` - Why, as a kind
    /// * `reason` - Why, as a sentence
    #[must_use]
    pub fn refused<S: Into<String>>(kind: RefusalKind, reason: S) -> Self {
        ControlResponse::Refused {
            reason: reason.into(),
            kind,
        }
    }

    /// The topology version this response reports, if it applied
    #[must_use]
    pub fn applied_version(&self) -> Option<u64> {
        match self {
            ControlResponse::Applied { topology_version }
            | ControlResponse::RestoreAhead {
                topology_version, ..
            } => Some(*topology_version),
            ControlResponse::Repeated { first } => first.applied_version(),
            ControlResponse::Refused { .. }
            | ControlResponse::Fenced { .. }
            | ControlResponse::Removed { .. } => None,
        }
    }
}

/// A bucket a schema declares, as an initialization records it
///
/// The tables a bucket generated are tables like any other to the placement, which derives their
/// groups from the recorded tables; this is what lets a grant tell them apart, since no table
/// grant may name one and a bucket grant names the bucket
/// ([F79](../../../../docs/src/features/bucket-schema.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRecord {
    /// The bucket's name, which is its marker's
    pub name: String,
    /// The bucket's stable identity, the hash of its name
    pub id: BucketId,
    /// The tables it generated, by name, each of which is also in the recorded tables
    pub tables: Vec<String>,
}

impl BucketRecord {
    /// Every bucket a schema declares, with the tables each generated
    ///
    /// # Arguments
    ///
    /// * `bucket_ids` - The schema's buckets and their identities, in declaration order
    /// * `generated` - The schema's generated tables, each paired with its bucket
    #[must_use]
    pub fn of_schema(
        bucket_ids: &[(&str, BucketId)],
        generated: &[(&str, &str)],
    ) -> Vec<BucketRecord> {
        bucket_ids
            .iter()
            .map(|(name, id)| BucketRecord {
                name: (*name).to_string(),
                id: *id,
                // the tables this bucket generated, in the order the schema lists them
                tables: generated
                    .iter()
                    .filter(|(_, bucket)| bucket == name)
                    .map(|(table, _)| (*table).to_string())
                    .collect(),
            })
            .collect()
    }
}

/// What a grant can name, from recorded tables and buckets
///
/// The control state's own record of the schema, or a control plane's copy of its node's, read
/// into the shape a grant is checked against.
///
/// # Arguments
///
/// * `tables` - Every table the schema serves, generated ones included
/// * `buckets` - Every bucket it declares, with the tables each generated
#[must_use]
pub fn served_of<'a>(tables: &'a [(String, TableId)], buckets: &'a [BucketRecord]) -> Served<'a> {
    // the tables each bucket generated, paired with the bucket
    let generated: Vec<(&str, &str)> = buckets
        .iter()
        .flat_map(|bucket| {
            bucket
                .tables
                .iter()
                .map(move |table| (table.as_str(), bucket.name.as_str()))
        })
        .collect();
    // every other table, which is what a client can query and a table grant can name
    let tables = tables
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| !generated.iter().any(|(table, _)| table == name))
        .collect();
    Served {
        tables,
        generated,
        buckets: buckets.iter().map(|bucket| bucket.name.as_str()).collect(),
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
    /// The buckets the schema declares and the tables each generated, recorded at
    /// initialization beside the tables ([F79](../../../../docs/src/features/bucket-schema.md))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<BucketRecord>,
    /// The schema the cluster serves: its id, its lineage and each table's own fingerprint
    ///
    /// Committed at initialization and moved by `ExtendSchema` alone, along its lineage. No
    /// member whose build does not hold it is served
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaRecord>,
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
    /// The repair operations, by identity: every one not done, and the newest `KEPT_REPAIRS` of those
    /// done (item 324)
    /// ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub repairs: BTreeMap<Uuid, RepairRecord>,
    /// The replica sets that no longer follow the placement rule, by their first tablet
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub configurations: BTreeMap<u16, DataConfiguration>,
    /// The move operations, by identity, the newest `KEPT_MOVES` of them
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub moves: BTreeMap<Uuid, MoveRecord>,
    /// The identities of removed members, which never rejoin
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default)]
    pub tombstones: BTreeMap<NodeId, Tombstone>,
    /// The placement plans, by identity, the newest `KEPT_PLANS` of them
    #[serde(default)]
    pub plans: BTreeMap<Uuid, PlanRecord>,
    /// The wire version the cluster has activated; zero, from a state before F48, is the floor
    ///
    /// Read through [`ControlState::activated_wire`]. Every member speaks it, a member that
    /// cannot is refused at every door, and it never goes down
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    #[serde(default)]
    pub activated: u8,
    /// The backup operations, by identity: every one not done, and the newest `KEPT_BACKUPS` of those
    /// done (item 324)
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md))
    #[serde(default)]
    pub backups: BTreeMap<Uuid, BackupRecord>,
    /// The restore operations, by identity, the newest `KEPT_RESTORES` of them
    #[serde(default)]
    pub restores: BTreeMap<Uuid, RestoreRecord>,
    /// The cluster this one was restored from, once; its identities are refused at every door
    #[serde(default)]
    pub restored_from: Option<ClusterId>,
    /// Every recovery an operator ran on a survivor, oldest first
    #[serde(default)]
    pub recoveries: Vec<RecoveryRecord>,
    /// Every device a pool can place on, and the history placement is computed from
    /// ([S5](../../../../docs/src/object-storage/placement.md#the-pool-map),
    /// [F85](../../../../docs/src/features/devices-and-pools.md))
    #[serde(default, skip_serializing_if = "PoolMap::is_empty")]
    pub pool_map: PoolMap,
    /// The last device report committed for each node, as its incarnation and sequence, so a
    /// report committed after a newer one is never left standing
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub device_reports: BTreeMap<NodeId, (u64, u64)>,
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
        let mut response = self.apply_once(command);
        // an archive whose node stopped being a member is dealt again, after whatever the command
        // did to the members, and the answer names the version that dealt it (item 263)
        if self.redeal_archives() {
            if let ControlResponse::Applied { topology_version } = &mut response {
                *topology_version = self.topology_version;
            }
        }
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
                    return ControlResponse::refused(
                        RefusalKind::AlreadyInitialized,
                        format!(
                            "the cluster is already {existing}; a second bootstrap ({cluster}) \
                             would fork it"
                        ),
                    );
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
                    MemberState::fresh(
                        member.clone(),
                        MemberHealth::Up,
                        role,
                        self.topology_version,
                    ),
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
                    return ControlResponse::refused(
                        RefusalKind::NotMember,
                        format!("{node} is not a member, so has no health to set"),
                    );
                };
                // evidence about an older run of the node says nothing about this one
                if *incarnation < state.record.incarnation {
                    return ControlResponse::Fenced {
                        node: *node,
                        committed: state.record.incarnation,
                        offered: *incarnation,
                    };
                }
                // a removed member has no health to set; it is gone, and a report from it is
                // answered as such so it stops
                if state.phase == MemberPhase::Removed {
                    return ControlResponse::Removed { node: *node };
                }
                if state.health == *health {
                    return self.applied();
                }
                // a down episode opens a grace where the policy removes a member for it
                let grace = self
                    .policy
                    .as_ref()
                    .and_then(|policy| policy.auto_remove_after);
                self.topology_version += 1;
                let version = self.topology_version;
                let state = self.members.get_mut(node).expect("checked above");
                state.health = *health;
                state.since = version;
                state.episode = match health {
                    MemberHealth::Down => *episode,
                    MemberHealth::Up | MemberHealth::Joining => None,
                };
                state.grace = match (health, episode, grace) {
                    (MemberHealth::Down, Some(episode), Some(_)) => Some(GraceState {
                        episode: *episode,
                        elapsed_ms: 0,
                        suspended: false,
                        expired: false,
                        plan: None,
                    }),
                    // a member back up under a removal keeps the removal: a late heartbeat
                    // cannot reverse it, and its grace is the record of why
                    (MemberHealth::Up, _, _) if state.phase == MemberPhase::Removing => {
                        state.grace.take()
                    }
                    _ => None,
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
                    return ControlResponse::refused(
                        RefusalKind::NotMember,
                        format!("{node} is not a member, so has no shards to report"),
                    );
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
                self.members
                    .get_mut(node)
                    .expect("checked above")
                    .shards_failed = failed.clone();
                self.applied()
            }
            // the one explicit placement
            ControlCommand::Initialize {
                expected_version,
                nodes,
                tables,
                buckets,
                schema,
                ..
            } => {
                if self.cluster.is_none() {
                    return ControlResponse::refused(
                        RefusalKind::NotInitialized,
                        "no cluster has been bootstrapped to place tablets in".to_string(),
                    );
                }
                if let Some(refusal) = self.check_version(*expected_version) {
                    return refusal;
                }
                if self.initialized.is_some() {
                    return ControlResponse::refused(
                        RefusalKind::AlreadyInitialized,
                        "the placement is already initialized; a replica set moves \
                                 between nodes by a Move operation, not a second initialization"
                            .to_string(),
                    );
                }
                if nodes.is_empty() {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        "a placement needs at least one node".to_string(),
                    );
                }
                // every node named has to be a member that is up, and named once
                let mut seen = std::collections::BTreeSet::new();
                for node in nodes {
                    match self.members.get(node) {
                        Some(state) if state.is_placeable() => {}
                        Some(state) => {
                            return ControlResponse::refused(
                                RefusalKind::NotUp,
                                format!(
                                    "{node} is {}, and only an up member can be placed on",
                                    state.state_name()
                                ),
                            );
                        }
                        None => {
                            return ControlResponse::refused(
                                RefusalKind::NotMember,
                                format!("{node} is not a member of this cluster"),
                            );
                        }
                    }
                    if !seen.insert(*node) {
                        return ControlResponse::refused(
                            RefusalKind::Duplicate,
                            format!("{node} is named twice in the placement"),
                        );
                    }
                }
                self.initialized = Some(nodes.clone());
                self.tables = tables.clone();
                self.buckets = buckets.clone();
                // a proposer that named no schema, a test's, commits none
                self.schema = (!schema.lineage.is_empty()).then(|| schema.clone());
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
                    return ControlResponse::refused(
                        RefusalKind::NotInitialized,
                        "no cluster has been bootstrapped to set a policy on".to_string(),
                    );
                };
                if !matches!(count, 1 | 3 | 5) {
                    return ControlResponse::refused(
                        RefusalKind::BadVoterCount,
                        format!("control_voters is {count}; it has to be 1, 3 or 5"),
                    );
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
                    return ControlResponse::refused(
                        RefusalKind::NotInitialized,
                        "no cluster has been bootstrapped to set a policy on".to_string(),
                    );
                }
                // `All` is not a read level anything serves; the only strong read is `Quorum`
                if *level == Some(Consistency::All) {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        "read_consistency All is not served; the strong read level is Quorum (C6)"
                            .to_string(),
                    );
                }
                // the table has to be one the schema serves, which the initialization recorded
                if !self.tables.iter().any(|(_, id)| id == table) {
                    return ControlResponse::refused(
                        RefusalKind::UnknownOperation,
                        format!("table {table} is not one the placement was initialized with"),
                    );
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
            // a grant, or a revoke, of a principal's privileges on a table (F77) or a bucket (F79)
            ControlCommand::Grant {
                expected_version,
                grantee,
                on,
                name,
                privileges,
                ..
            } => self.change_grants(*expected_version, grantee, *on, name, *privileges, true),
            ControlCommand::Revoke {
                expected_version,
                grantee,
                on,
                name,
                privileges,
                ..
            } => self.change_grants(*expected_version, grantee, *on, name, *privileges, false),
            // a repair: the record, with its groups derived from the placement as it stands
            ControlCommand::Repair {
                op,
                principal,
                expected_version,
                table,
                tablet,
                mode,
                source,
                release,
            } => {
                if self.policy.is_none() || self.initialized.is_none() {
                    return ControlResponse::refused(
                        RefusalKind::NotInitialized,
                        "no placement has been initialized to repair".to_string(),
                    );
                }
                if !self.tables.iter().any(|(_, id)| id == table) {
                    return ControlResponse::refused(
                        RefusalKind::UnknownOperation,
                        format!("table {table} is not one the placement was initialized with"),
                    );
                }
                if let Some(node) = source {
                    if !self.members.contains_key(node) {
                        return ControlResponse::refused(
                            RefusalKind::NotMember,
                            format!("{node} is not a member, so cannot be a source"),
                        );
                    }
                }
                // a scheduled verify scrub is judged against the state it applies to: it derives its
                // groups here and changes no placement, so the version guards nothing for it, and
                // under three leaders' scrubs the version moved faster than a lagging node's map,
                // which was refused every time ([#195](../../../../docs/src/appendix/resolved/scheduled-scrub-starved.md))
                let unconditional =
                    *mode == RepairMode::Verify && *expected_version == ANY_VERSION;
                if !unconditional {
                    if let Some(refusal) = self.check_version(*expected_version) {
                        return refusal;
                    }
                }
                // the groups the placement derives for the table, or the one holding the tablet;
                // a group whose set is under a move not yet done waits behind it
                // ([F45](../../../../docs/src/features/replica-migration.md))
                let map =
                    crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
                let groups: BTreeMap<GroupId, GroupRepair> = map
                    .groups_of(*table)
                    .into_iter()
                    .filter(|(_, _, tablets)| tablet.is_none_or(|wanted| tablets.contains(&wanted)))
                    .map(|(id, _, _)| {
                        let behind = self
                            .moves
                            .values()
                            .filter(|record| !record.is_done() && record.groups.contains_key(&id))
                            .max_by_key(|record| record.requested_at)
                            .map(|record| record.op);
                        let progress = match behind {
                            Some(behind) => GroupRepair {
                                phase: RepairPhase::Queued { behind },
                                ..GroupRepair::default()
                            },
                            None => GroupRepair::default(),
                        };
                        (id, progress)
                    })
                    .collect();
                if groups.is_empty() {
                    return ControlResponse::refused(
                        RefusalKind::UnknownOperation,
                        match tablet {
                            Some(tablet) => format!("no group of {table} serves tablet {tablet}"),
                            None => format!("the placement derives no groups for {table}"),
                        },
                    );
                }
                self.topology_version += 1;
                self.repairs.insert(
                    *op,
                    RepairRecord {
                        op: *op,
                        table: *table,
                        tablet: *tablet,
                        mode: *mode,
                        source: *source,
                        release: *release,
                        principal: principal.clone(),
                        requested_at: self.topology_version,
                        groups,
                    },
                );
                // forget the oldest done once too many are kept: one not done may hold a move or a
                // backup queued behind it, which only its end releases, as a move's records are
                // kept (item 324)
                while self.repairs.len() > KEPT_REPAIRS {
                    let oldest = self
                        .repairs
                        .values()
                        .filter(|record| record.is_done())
                        .min_by_key(|record| record.requested_at)
                        .map(|record| record.op);
                    match oldest {
                        Some(op) => {
                            self.repairs.remove(&op);
                        }
                        None => break,
                    }
                }
                self.applied()
            }
            // a driver's word on where a group stands
            ControlCommand::RepairProgress {
                op,
                group,
                node,
                incarnation,
                progress,
            } => {
                let Some(member) = self.members.get(node) else {
                    return ControlResponse::refused(
                        RefusalKind::NotMember,
                        format!("{node} is not a member, so cannot drive a repair"),
                    );
                };
                if *incarnation < member.record.incarnation {
                    return ControlResponse::Fenced {
                        node: *node,
                        committed: member.record.incarnation,
                        offered: *incarnation,
                    };
                }
                let Some(record) = self.repairs.get_mut(op) else {
                    return ControlResponse::refused(
                        RefusalKind::UnknownOperation,
                        format!("no repair operation {op} is recorded"),
                    );
                };
                let Some(current) = record.groups.get_mut(group) else {
                    return ControlResponse::refused(
                        RefusalKind::UnknownOperation,
                        format!("group {group} is not part of repair {op}"),
                    );
                };
                // a group that is done stays done, whatever a late driver says
                if current.is_done() {
                    return self.applied();
                }
                // a group queued behind a move is nobody's to drive yet
                if current.is_queued() {
                    return ControlResponse::refused(RefusalKind::Queued, format!("group {group} of repair {op} is queued behind a move and cannot be driven yet"));
                }
                if current == progress {
                    return self.applied();
                }
                *current = progress.clone();
                self.topology_version += 1;
                // the last group done releases the moves queued behind this repair
                if record.is_done() {
                    self.release_queued(*op);
                }
                self.applied()
            }
            // a member's quarantined copies
            ControlCommand::ReportQuarantine {
                node,
                incarnation,
                copies,
            } => {
                let Some(state) = self.members.get(node) else {
                    return ControlResponse::refused(
                        RefusalKind::NotMember,
                        format!("{node} is not a member, so has no copies to report"),
                    );
                };
                if *incarnation < state.record.incarnation {
                    return ControlResponse::Fenced {
                        node: *node,
                        committed: state.record.incarnation,
                        offered: *incarnation,
                    };
                }
                if state.quarantined == *copies {
                    return self.applied();
                }
                self.topology_version += 1;
                self.members
                    .get_mut(node)
                    .expect("checked above")
                    .quarantined = copies.clone();
                self.applied()
            }
            // a move: the record, with the set derived from the map as it stands
            ControlCommand::Move {
                op,
                principal,
                expected_version,
                tablet,
                from,
                to,
            } => self.apply_move(*op, principal, *expected_version, *tablet, *from, *to),
            // a driver's word on where a group's move stands
            ControlCommand::MoveProgress {
                op,
                group,
                node,
                incarnation,
                progress,
            } => self.apply_move_progress(*op, *group, *node, *incarnation, progress),
            // a member leaving at an operator's word, with its drain planned
            ControlCommand::Decommission {
                op,
                principal,
                expected_version,
                node,
            } => self.apply_decommission(*op, principal, *expected_version, *node),
            // a member removed at an operator's word, with its rebuild planned
            ControlCommand::Remove {
                op,
                principal,
                expected_version,
                node,
                replacement,
            } => self.apply_remove(*op, principal, *expected_version, *node, *replacement),
            // a grace suspended or resumed
            ControlCommand::Maintenance {
                expected_version,
                node,
                suspend,
                ..
            } => self.apply_maintenance(*expected_version, *node, *suspend),
            // a rebalance planned
            ControlCommand::Rebalance {
                op,
                principal,
                expected_version,
            } => self.apply_rebalance(*op, principal, *expected_version),
            // the leader's count of a grace
            ControlCommand::GraceElapsed {
                node,
                episode,
                elapsed_ms,
                expire,
            } => self.apply_grace_elapsed(*node, *episode, *elapsed_ms, *expire),
            // the leader's word on a plan
            ControlCommand::PlanProgress {
                op,
                node,
                incarnation,
                progress,
            } => self.apply_plan_progress(*op, *node, *incarnation, progress),
            // a removed identity, for good
            ControlCommand::Tombstone { node, op } => self.apply_tombstone(*node, *op),
            // the pool map: a node's devices, their failures, a pool's weights and exceptions (F85)
            ControlCommand::ObserveDevices {
                node,
                incarnation,
                devices,
            } => self.apply_observe_devices(*node, *incarnation, devices),
            ControlCommand::ReportDevices {
                node,
                incarnation,
                seq,
                failed,
                empty,
            } => self.apply_report_devices(*node, *incarnation, *seq, failed, empty),
            ControlCommand::SetPoolWeights {
                pool,
                generation,
                weights,
            } => self.apply_pool_weights(pool, *generation, weights),
            ControlCommand::SetPoolExceptions {
                pool,
                consumer,
                generation,
                exceptions,
            } => self.apply_pool_exceptions(pool, *consumer, *generation, exceptions),
            // a pool's placement fixed where its stripes go, the first time it is ready (F86)
            ControlCommand::PinPool { pool, generation } => self.apply_pin_pool(pool, *generation),
            // where a pool's placement groups move to, a failed device retired, and a move's slot
            // taken and freed (F88)
            ControlCommand::TargetPool { pool, generation } => self.apply_target_pool(pool, *generation),
            ControlCommand::RetireDevice { device } => self.apply_retire_device(*device),
            ControlCommand::FailDevice {
                op,
                expected_version,
                device,
                reason,
                ..
            } => {
                if let Some(refused) = self.recovery_gate() {
                    return refused;
                }
                if !self.pool_map.devices.contains_key(device) {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        format!("no device {device} is on the pool map"),
                    );
                }
                // an operator's order was written against a version, as every operation is
                if !op.is_nil() {
                    if let Some(refusal) = self.check_version(*expected_version) {
                        return refusal;
                    }
                }
                if self.pool_map.order_failed(*device, reason) {
                    self.topology_version += 1;
                }
                self.applied()
            }
            ControlCommand::ScrubNow {
                op,
                expected_version,
                pool,
                kind,
                target,
                ..
            } => self.apply_scrub_now(*op, *expected_version, pool, *kind, *target),
            // a device drained, removed for good, or given a weight (F92)
            ControlCommand::DrainDevice { device, reason, .. } => self.apply_drain_device(*device, reason),
            ControlCommand::RemoveDevice {
                principal,
                expected_version,
                device,
                lost,
                op,
            } => self.apply_remove_device(*op, principal, *expected_version, *device, *lost),
            ControlCommand::ReweightDevice {
                expected_version,
                device,
                weight,
                ..
            } => self.apply_reweight_device(*expected_version, *device, *weight),
            ControlCommand::AdmitMove { slot } => self.apply_admit_move(slot),
            ControlCommand::MoveDone { pool, consumer, pg } => self.apply_move_done(pool, *consumer, *pg),
            // a backup: the record, with its groups derived from the placement as it stands
            ControlCommand::Backup {
                op,
                principal,
                expected_version,
                table,
                path,
            } => self.apply_backup(*op, principal, *expected_version, *table, path),
            // a driver's word on where a group's backup stands
            ControlCommand::BackupProgress {
                op,
                group,
                node,
                incarnation,
                progress,
            } => self.apply_backup_progress(*op, *group, *node, *incarnation, progress),
            // a restore: judged whole against the files and the tables, once per cluster
            ControlCommand::Restore {
                op,
                principal,
                expected_version,
                path,
                source,
                files,
                archives,
            } => self.apply_restore(*op, principal, *expected_version, path, *source, files, archives),
            // a finished restore's failed groups, put back to be driven
            ControlCommand::RetryRestore {
                expected_version,
                restore,
                ..
            } => self.apply_retry_restore(*expected_version, *restore),
            // a driver's word on where a group's restore stands
            ControlCommand::RestoreProgress {
                op,
                group,
                node,
                incarnation,
                progress,
            } => self.apply_restore_progress(*op, *group, *node, *incarnation, progress),
            // a driver's word on how far an archive's objects are put again
            ControlCommand::ArchiveProgress {
                op,
                archive,
                node,
                incarnation,
                progress,
            } => self.apply_archive_progress(*op, archive, *node, *incarnation, progress),
            // an operator's recovery of a survivor after a permanent majority loss
            ControlCommand::ForceRecovered {
                op,
                survivors,
                lost,
                at,
                last_committed,
                recovered_ms,
            } => self.apply_force_recovered(
                *op,
                survivors,
                lost,
                *at,
                *last_committed,
                *recovered_ms,
            ),
            // a wire version activated, judged against what every member reported
            ControlCommand::Activate {
                expected_version,
                wire,
                members,
                ..
            } => self.apply_activate(*expected_version, *wire, members),
            // a schema extended along its lineage, judged against the members' records
            ControlCommand::ExtendSchema {
                expected_version,
                schema,
                tables,
                buckets,
                pools,
                ..
            } => self.apply_extend_schema(*expected_version, schema, tables, buckets, pools),
        }
    }

    /// Try a member's open plan again, forgiving the failures that blocked it
    ///
    /// A decommission or a removal asked again for a member already leaving or removing is the
    /// operator's way to retry one: the plan's failures so far stop counting, and the leader's
    /// next pass plans the sets they blocked. A plan that is not blocked is left as it is.
    ///
    /// # Arguments
    ///
    /// * `node` - The member the plan drains
    fn retry_plan_for(&mut self, node: NodeId) -> ControlResponse {
        let retried = self
            .plans
            .values_mut()
            .filter(|record| !record.is_done() && record.kind.drains() == Some(node))
            .fold(false, |retried, record| record.retry() || retried);
        if retried {
            self.topology_version += 1;
        }
        self.applied()
    }

    /// Mark a member leaving and record the plan that drains it
    ///
    /// # Arguments
    ///
    /// * `op` - The operation, which is the plan's identity
    /// * `principal` - Who asked
    /// * `expected_version` - The topology version the request was written against
    /// * `node` - The member
    fn apply_decommission(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        node: NodeId,
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to decommission a member of".to_string(),
            );
        }
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member of this cluster"),
            );
        };
        match state.phase {
            MemberPhase::Member => {}
            // already leaving is applied and changes nothing: the first plan drains it
            // asked again: a plan its failures blocked is tried again
            // ([Resolved #177](../../../../docs/src/appendix/resolved/blocked-plan-retry.md))
            MemberPhase::Leaving => return self.retry_plan_for(node),
            other => {
                return ControlResponse::refused(
                    RefusalKind::WrongPhase,
                    format!(
                        "{node} is {}, and only a member can be decommissioned",
                        other.name()
                    ),
                );
            }
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        if let Some(refusal) = self.check_one_plan(Some(node)) {
            return refusal;
        }
        self.topology_version += 1;
        let version = self.topology_version;
        let state = self.members.get_mut(&node).expect("checked above");
        state.phase = MemberPhase::Leaving;
        state.since = version;
        self.record_plan(PlanRecord::new(
            op,
            PlanKind::Decommission { node },
            principal,
            version,
        ));
        self.applied()
    }

    /// Mark a member removing and record the plan that rebuilds its sets
    ///
    /// # Arguments
    ///
    /// * `op` - The operation, which is the plan's identity
    /// * `principal` - Who asked
    /// * `expected_version` - The topology version the request was written against
    /// * `node` - The member
    /// * `replacement` - The member to take its place first, if named
    fn apply_remove(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        node: NodeId,
        replacement: Option<NodeId>,
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to remove a member of".to_string(),
            );
        }
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member of this cluster"),
            );
        };
        // a live member is drained by a decommission, not torn out; a leaving one may be
        // hurried out, and a down one replaced
        let removable = state.health == MemberHealth::Down || state.phase == MemberPhase::Leaving;
        match state.phase {
            MemberPhase::Removed => {
                return ControlResponse::refused(
                    RefusalKind::NotMember,
                    format!("{node} is already removed"),
                );
            }
            // asked again: a plan its failures blocked is tried again
            // ([Resolved #177](../../../../docs/src/appendix/resolved/blocked-plan-retry.md))
            MemberPhase::Removing => return self.retry_plan_for(node),
            _ if !removable => {
                return ControlResponse::refused(RefusalKind::WrongPhase, format!(
                        "{node} is {} and a member; only a down or leaving member can be removed - decommission a live one",
                        state.health.name()
                    ));
            }
            _ => {}
        }
        // a replacement has to be a placeable member outside every set the member is in
        if let Some(replacement) = replacement {
            match self.members.get(&replacement) {
                Some(state) if state.is_placeable() => {}
                Some(state) => {
                    return ControlResponse::refused(
                        RefusalKind::NotUp,
                        format!(
                            "{replacement} is {}, and only an up member can replace another",
                            state.state_name()
                        ),
                    );
                }
                None => {
                    return ControlResponse::refused(
                        RefusalKind::NotMember,
                        format!("{replacement} is not a member of this cluster"),
                    );
                }
            }
            if replacement == node {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!("{node} cannot replace itself"),
                );
            }
            let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
            let shared = map.rule_sets_served().into_iter().any(|(members, _)| {
                members.iter().any(|member| member.node == node)
                    && members.iter().any(|member| member.node == replacement)
            });
            if shared {
                return ControlResponse::refused(RefusalKind::Duplicate, format!("{replacement} already holds a set with {node}; a replacement has to be outside every set it would take"));
            }
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        if let Some(refusal) = self.check_one_plan(Some(node)) {
            return refusal;
        }
        self.topology_version += 1;
        let version = self.topology_version;
        let state = self.members.get_mut(&node).expect("checked above");
        state.phase = MemberPhase::Removing;
        state.since = version;
        // the grace, if one runs, is over: the operator decided
        if let Some(grace) = state.grace.as_mut() {
            grace.expired = true;
            grace.plan = Some(op);
        }
        self.record_plan(PlanRecord::new(
            op,
            PlanKind::Remove { node, replacement },
            principal,
            version,
        ));
        self.applied()
    }

    /// Suspend or resume a down member's grace
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The topology version the request was written against
    /// * `node` - The member
    /// * `suspend` - Whether to suspend, or resume
    fn apply_maintenance(
        &mut self,
        expected_version: u64,
        node: NodeId,
        suspend: bool,
    ) -> ControlResponse {
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member of this cluster"),
            );
        };
        let Some(grace) = state.grace.as_ref() else {
            return ControlResponse::refused(
                RefusalKind::WrongPhase,
                format!(
                    "{node} is under no grace to suspend; it is {} and the policy may not remove",
                    state.state_name()
                ),
            );
        };
        if grace.expired {
            return ControlResponse::refused(RefusalKind::WrongPhase, format!("{node}'s grace has elapsed and it is removing; maintenance cannot suspend a removal"));
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        if grace.suspended == suspend {
            return self.applied();
        }
        self.topology_version += 1;
        let state = self.members.get_mut(&node).expect("checked above");
        if let Some(grace) = state.grace.as_mut() {
            grace.suspended = suspend;
        }
        self.applied()
    }

    /// Record a rebalance plan
    ///
    /// # Arguments
    ///
    /// * `op` - The operation, which is the plan's identity
    /// * `principal` - Who asked
    /// * `expected_version` - The topology version the request was written against
    fn apply_rebalance(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to rebalance".to_string(),
            );
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        if let Some(refusal) = self.check_one_plan(None) {
            return refusal;
        }
        self.topology_version += 1;
        let version = self.topology_version;
        self.record_plan(PlanRecord::new(op, PlanKind::Rebalance, principal, version));
        self.applied()
    }

    /// Refuse a second plan on a member, or a second rebalance, while one is not done
    ///
    /// # Arguments
    ///
    /// * `node` - The member the plan drains, or none for a rebalance
    fn check_one_plan(&self, node: Option<NodeId>) -> Option<ControlResponse> {
        let clash = self
            .plans
            .values()
            .filter(|record| !record.is_done())
            .find(|record| record.kind.drains() == node);
        clash.map(|record| {
            ControlResponse::refused(
                RefusalKind::Duplicate,
                match node {
                    Some(node) => format!(
                        "{node} is already under plan {} ({}), which is {}",
                        record.op,
                        record.kind.name(),
                        record.phase.name()
                    ),
                    None => format!(
                        "a rebalance is already under way as plan {}, which is {}",
                        record.op,
                        record.phase.name()
                    ),
                },
            )
        })
    }

    /// Keep a plan record, forgetting the oldest done ones past the bound
    ///
    /// # Arguments
    ///
    /// * `record` - The record
    fn record_plan(&mut self, record: PlanRecord) {
        self.plans.insert(record.op, record);
        while self.plans.len() > KEPT_PLANS {
            let oldest = self
                .plans
                .values()
                .filter(|record| record.is_done())
                .min_by_key(|record| record.requested_at)
                .map(|record| record.op);
            match oldest {
                Some(op) => {
                    self.plans.remove(&op);
                }
                None => break,
            }
        }
    }

    /// Record how much of a grace has elapsed, and the removal when it is over
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `episode` - The down episode the count is of
    /// * `elapsed_ms` - How much has elapsed
    /// * `expire` - The plan to remove under, when the grace is over
    fn apply_grace_elapsed(
        &mut self,
        node: NodeId,
        episode: Uuid,
        elapsed_ms: u64,
        expire: Option<Uuid>,
    ) -> ControlResponse {
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so has no grace to count"),
            );
        };
        let Some(grace) = state.grace.as_ref() else {
            return ControlResponse::refused(
                RefusalKind::WrongPhase,
                format!("{node} is under no grace"),
            );
        };
        // a count of another episode, a suspended grace or one already over changes nothing
        if grace.episode != episode {
            return ControlResponse::refused(
                RefusalKind::WrongPhase,
                format!(
                    "{node}'s grace is of episode {} and the count is of {episode}",
                    grace.episode
                ),
            );
        }
        if grace.suspended || grace.expired {
            return self.applied();
        }
        // elapsed time never goes backwards
        if elapsed_ms < grace.elapsed_ms {
            return self.applied();
        }
        let unchanged = elapsed_ms == grace.elapsed_ms && expire.is_none();
        if unchanged {
            return self.applied();
        }
        self.topology_version += 1;
        let version = self.topology_version;
        let state = self.members.get_mut(&node).expect("checked above");
        let grace = state.grace.as_mut().expect("checked above");
        grace.elapsed_ms = elapsed_ms;
        if let Some(plan) = expire {
            grace.expired = true;
            grace.plan = Some(plan);
            // the member is removing now, from wherever it stood, and its plan is recorded
            state.phase = MemberPhase::Removing;
            state.since = version;
            self.record_plan(PlanRecord::new(
                plan,
                PlanKind::Expiry { node, episode },
                "policy",
                version,
            ));
        }
        self.applied()
    }

    /// Record where a plan stands, as the leader says
    ///
    /// # Arguments
    ///
    /// * `op` - The plan
    /// * `node` - The leader
    /// * `incarnation` - The incarnation it leads at
    /// * `progress` - What changed
    fn apply_plan_progress(
        &mut self,
        op: Uuid,
        node: NodeId,
        incarnation: u64,
        progress: &PlanUpdate,
    ) -> ControlResponse {
        let Some(member) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot drive a plan"),
            );
        };
        if incarnation < member.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: member.record.incarnation,
                offered: incarnation,
            };
        }
        let version = self.topology_version + 1;
        let Some(record) = self.plans.get_mut(&op) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no plan {op} is recorded"),
            );
        };
        // a done plan stays done, whatever a late leader says
        if record.is_done() {
            return self.applied();
        }
        match progress {
            PlanUpdate::Steps { steps, blocked } => {
                record.steps.extend(steps.iter().cloned());
                record.replanned += 1;
                record.blocked = blocked.as_ref().map(|reason| Blocked {
                    reason: reason.clone(),
                    since: version,
                });
                record.phase = if record.blocked.is_some() {
                    PlanPhase::Blocked
                } else {
                    PlanPhase::Running
                };
            }
            PlanUpdate::Step {
                tablet,
                op: moved,
                state,
            } => {
                // the live step for the set, or nothing to update
                let Some(step) = record
                    .steps
                    .iter_mut()
                    .rev()
                    .find(|step| step.tablet == *tablet && step.is_live())
                else {
                    return self.applied();
                };
                if step.state == *state && step.op == *moved {
                    return self.applied();
                }
                if moved.is_some() {
                    step.op = *moved;
                }
                step.state = state.clone();
            }
            PlanUpdate::Blocked(reason) => {
                let same =
                    record.blocked.as_ref().map(|blocked| &blocked.reason) == reason.as_ref();
                if same {
                    return self.applied();
                }
                record.blocked = reason.as_ref().map(|reason| Blocked {
                    reason: reason.clone(),
                    since: version,
                });
                record.phase = if record.blocked.is_some() {
                    PlanPhase::Blocked
                } else {
                    PlanPhase::Running
                };
            }
            PlanUpdate::Finishing => {
                if record.phase == PlanPhase::Finishing {
                    return self.applied();
                }
                record.phase = PlanPhase::Finishing;
                record.blocked = None;
            }
            PlanUpdate::Done(outcome) => {
                record.phase = PlanPhase::Done;
                record.blocked = None;
                record.outcome = Some(outcome.clone());
                // a drain that did not complete leaves its member where it stood: a leaving
                // member is a member again, a removing one stays removing under its grace
                if let (Some(node), false) = (
                    record.kind.drains(),
                    matches!(outcome, PlanOutcome::Completed { .. }),
                ) {
                    if let Some(state) = self.members.get_mut(&node) {
                        if state.phase == MemberPhase::Leaving {
                            state.phase = MemberPhase::Member;
                        }
                    }
                }
            }
        }
        self.topology_version += 1;
        self.applied()
    }

    /// Refuse a pool map command before every member can decode it
    fn pool_map_gate(&self) -> Option<ControlResponse> {
        (self.activated_wire() < POOL_MAP_WIRE_VERSION).then(|| {
            ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "the pool map's commands need wire version {POOL_MAP_WIRE_VERSION} activated, \
                     and {} is",
                    self.activated_wire()
                ),
            )
        })
    }

    /// Commit what a node claimed of its devices, moving the version when the map moved
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `incarnation` - The incarnation it claimed them at
    /// * `devices` - What it claimed
    fn apply_observe_devices(
        &mut self,
        node: NodeId,
        incarnation: u64,
        devices: &[crate::shared::devices::DeviceFacts],
    ) -> ControlResponse {
        if let Some(refused) = self.pool_map_gate() {
            return refused;
        }
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so has no devices to observe"),
            );
        };
        // an older start is fenced, and a newer one observes itself before its devices
        if incarnation < state.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: state.record.incarnation,
                offered: incarnation,
            };
        }
        if incarnation > state.record.incarnation {
            return ControlResponse::refused(
                RefusalKind::StaleVersion,
                format!("{node} has not observed itself at incarnation {incarnation} yet"),
            );
        }
        let domains = state.record.failure_domains.clone();
        match self.pool_map.observe(node, &domains, devices) {
            Ok(true) => {
                self.topology_version += 1;
                self.applied()
            }
            Ok(false) => self.applied(),
            Err(error) => ControlResponse::refused(RefusalKind::Invalid, error.to_string()),
        }
    }

    /// Record which of a node's devices it reports failed, if the report is newer than the last
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `incarnation` - The incarnation it reported at
    /// * `seq` - The report's sequence
    /// * `failed` - Each failed device and why
    /// * `empty` - Its devices whose era ended that hold nothing
    fn apply_report_devices(
        &mut self,
        node: NodeId,
        incarnation: u64,
        seq: u64,
        failed: &BTreeMap<crate::shared::devices::DeviceId, String>,
        empty: &std::collections::BTreeSet<crate::shared::devices::DeviceId>,
    ) -> ControlResponse {
        if let Some(refused) = self.pool_map_gate() {
            return refused;
        }
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so has no devices to report"),
            );
        };
        if incarnation < state.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: state.record.incarnation,
                offered: incarnation,
            };
        }
        // a report older than one committed changes nothing
        if self
            .device_reports
            .get(&node)
            .is_some_and(|last| *last >= (incarnation, seq))
        {
            return self.applied();
        }
        self.device_reports.insert(node, (incarnation, seq));
        if self.pool_map.set_failed(node, failed) {
            self.topology_version += 1;
        }
        // which of its ended devices hold nothing, read by a removal alone, moves no version (F92)
        self.pool_map.set_empty(node, empty);
        self.applied()
    }

    /// Set a pool's fitted weights, if the map is still at the generation they were fitted at
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation they were fitted at
    /// * `weights` - Each device's placement weight
    fn apply_pool_weights(
        &mut self,
        pool: &str,
        generation: u64,
        weights: &BTreeMap<crate::shared::devices::DeviceId, u64>,
    ) -> ControlResponse {
        if let Some(refused) = self.pool_map_gate().or_else(|| self.pinned_gate(pool)) {
            return refused;
        }
        if generation != self.pool_map.generation {
            return ControlResponse::refused(
                RefusalKind::StaleVersion,
                format!(
                    "pool {pool}'s weights were fitted at generation {generation}, and the map is at {}",
                    self.pool_map.generation
                ),
            );
        }
        if self.pool_map.set_fitted(pool, weights.clone()) {
            self.topology_version += 1;
        }
        self.applied()
    }

    /// Set one consumer's exceptions in a pool, if the map is still at their generation
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `generation` - The generation they were computed at
    /// * `exceptions` - Each group's slice the rule names and the one used instead
    fn apply_pool_exceptions(
        &mut self,
        pool: &str,
        consumer: u64,
        generation: u64,
        exceptions: &[(
            crate::shared::poolmap::PlacementGroup,
            crate::shared::poolmap::SliceRef,
            crate::shared::poolmap::SliceRef,
        )],
    ) -> ControlResponse {
        if let Some(refused) = self.pool_map_gate().or_else(|| self.pinned_gate(pool)) {
            return refused;
        }
        if generation != self.pool_map.generation {
            return ControlResponse::refused(
                RefusalKind::StaleVersion,
                format!(
                    "pool {pool}'s exceptions were computed at generation {generation}, and the map is at {}",
                    self.pool_map.generation
                ),
            );
        }
        if self.pool_map.set_exceptions(pool, consumer, exceptions) {
            self.topology_version += 1;
        }
        self.applied()
    }

    /// Refuse a change to a pinned pool's placement before its placement groups can move
    ///
    /// A fit starts a new generation and leaves every earlier one as it was, so once a cluster
    /// moves placement groups (wire 10) a pinned pool's groups follow it by a move; before that a
    /// pinned pool's fit is frozen (F86, [F88](../../../../docs/src/features/recovery-and-moves.md)).
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    fn pinned_gate(&self, pool: &str) -> Option<ControlResponse> {
        if self.activated_wire() >= RECOVERY_WIRE_VERSION {
            return None;
        }
        self.pool_map.pinned_at(pool).map(|generation| {
            ControlResponse::refused(
                RefusalKind::Invalid,
                format!(
                    "pool {pool} is pinned at generation {generation}, and its placement does not \
                     change before the cluster activates wire 10, which moves its placement groups"
                ),
            )
        })
    }

    /// Pin a pool's placement at a generation, the first time it is ready
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation it places at
    fn apply_pin_pool(&mut self, pool: &str, generation: u64) -> ControlResponse {
        // a pin is what a stripe is placed by, which no member places until the object wire
        if self.activated_wire() < OBJECT_WIRE_VERSION {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a pool is pinned once wire version {OBJECT_WIRE_VERSION} is activated, and {} is",
                    self.activated_wire()
                ),
            );
        }
        // only a pool the committed policy defines
        if !self
            .policy
            .as_ref()
            .is_some_and(|policy| policy.objects.pools.contains_key(pool))
        {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("no pool {pool} is committed, so none can be pinned"),
            );
        }
        match self.pool_map.pin(pool, generation) {
            // pinned now, so every member learns where its stripes go
            Ok(true) => {
                self.topology_version += 1;
                self.applied()
            }
            // a pin repeated at its own generation is the same pin
            Ok(false) if self.pool_map.pinned_at(pool) == Some(generation) => self.applied(),
            Ok(false) => ControlResponse::refused(
                RefusalKind::Duplicate,
                format!(
                    "pool {pool} is pinned at generation {}, not {generation}",
                    self.pool_map.pinned_at(pool).unwrap_or_default()
                ),
            ),
            Err(error) => ControlResponse::refused(RefusalKind::StaleVersion, error.to_string()),
        }
    }

    /// Count an operator's request to scrub a pool now (F89)
    ///
    /// Refused before the cluster scrubs, and for a pool the committed policy does not define.
    /// Moves the topology version, so every shard's map carries the new count to the leaders that
    /// begin its passes; never the pool map's generation, which says where chunks are.
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `kind` - Light or deep
    fn apply_scrub_now(
        &mut self,
        op: Uuid,
        expected_version: u64,
        pool: &str,
        kind: crate::shared::scrub::ScrubKind,
        target: Option<crate::shared::scrub::ScrubTarget>,
    ) -> ControlResponse {
        if self.activated_wire() < SCRUB_WIRE_VERSION {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a pool is scrubbed once wire version {SCRUB_WIRE_VERSION} is activated, and {} is",
                    self.activated_wire()
                ),
            );
        }
        // only a pool the committed policy defines
        if !self
            .policy
            .as_ref()
            .is_some_and(|policy| policy.objects.pools.contains_key(pool))
        {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("no pool {pool} is committed, so none can be scrubbed"),
            );
        }
        // a narrowed scrub names a bucket the pool serves, or a device or slice the map holds
        match target {
            Some(crate::shared::scrub::ScrubTarget::Bucket(bucket)) => {
                let served = self.policy.as_ref().is_some_and(|policy| {
                    policy.objects.bindings.iter().any(|(name, binding)| {
                        BucketId::of(name) == bucket && binding.pool == pool
                    })
                });
                if !served {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        format!("pool {pool} serves no bucket {bucket}, so none of it can be scrubbed"),
                    );
                }
            }
            Some(crate::shared::scrub::ScrubTarget::Device(device)) => {
                if !self.pool_map.devices.contains_key(&device) {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        format!("no device {device} is on the pool map"),
                    );
                }
            }
            Some(crate::shared::scrub::ScrubTarget::Slice(slice)) => {
                let held = self
                    .pool_map
                    .devices
                    .get(&slice.device)
                    .is_some_and(|device| device.slice_any(slice.index).is_some());
                if !held {
                    return ControlResponse::refused(
                        RefusalKind::Invalid,
                        format!("no slice {slice} is on the pool map"),
                    );
                }
            }
            None => {}
        }
        // an operator's request was written against a version, as every operation is (F93)
        if !op.is_nil() {
            if let Some(refusal) = self.check_version(expected_version) {
                return refusal;
            }
        }
        self.pool_map.request_scrub(pool, kind, target);
        self.topology_version += 1;
        self.applied()
    }

    /// Refuse a move's command before the cluster moves placement groups
    fn recovery_gate(&self) -> Option<ControlResponse> {
        (self.activated_wire() < RECOVERY_WIRE_VERSION).then(|| {
            ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a placement group moves once wire version {RECOVERY_WIRE_VERSION} is activated, and {} is",
                    self.activated_wire()
                ),
            )
        })
    }

    /// Move a pool's target to a generation its placement groups move to
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation
    fn apply_target_pool(&mut self, pool: &str, generation: u64) -> ControlResponse {
        if let Some(refused) = self.recovery_gate() {
            return refused;
        }
        // a pool has to be placed somewhere before it can move
        if self.pool_map.pinned_at(pool).is_none() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("pool {pool} is not pinned, so it has no placement to move from"),
            );
        }
        match self.pool_map.set_target(pool, generation) {
            Ok(true) => {
                self.topology_version += 1;
                self.applied()
            }
            Ok(false) => self.applied(),
            Err(error) => ControlResponse::refused(RefusalKind::StaleVersion, error.to_string()),
        }
    }

    /// End a failed device's era, so its placement groups move off it
    ///
    /// # Arguments
    ///
    /// * `device` - The device
    fn apply_retire_device(&mut self, device: crate::shared::devices::DeviceId) -> ControlResponse {
        if let Some(refused) = self.recovery_gate() {
            return refused;
        }
        // only a device its node reports failed
        let failed = self
            .pool_map
            .devices
            .get(&device)
            .is_some_and(|record| record.failed.is_some());
        if !failed {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("device {device} is not reported failed, so it is not retired"),
            );
        }
        let why = self
            .pool_map
            .devices
            .get(&device)
            .and_then(|record| record.failed.clone())
            .unwrap_or_default();
        if self.pool_map.retire_device(device, &why) {
            self.topology_version += 1;
        }
        self.applied()
    }

    /// Refuse a device's drain, removal or weight before the cluster reclaims what a move leaves
    fn reclaim_gate(&self) -> Option<ControlResponse> {
        (self.activated_wire() < RECLAIM_WIRE_VERSION).then(|| {
            ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a device is drained, removed or reweighted once wire version {RECLAIM_WIRE_VERSION} is activated, and {} is",
                    self.activated_wire()
                ),
            )
        })
    }

    /// Drain a device, so its placement groups move off it while it serves their copies (F92)
    ///
    /// # Arguments
    ///
    /// * `device` - The device
    /// * `reason` - Who drained it and why
    fn apply_drain_device(&mut self, device: crate::shared::devices::DeviceId, reason: &str) -> ControlResponse {
        if let Some(refused) = self.reclaim_gate() {
            return refused;
        }
        match self.pool_map.drain_device(device, reason) {
            Ok(moved) => {
                if moved {
                    self.topology_version += 1;
                }
                self.applied()
            }
            Err(error) => ControlResponse::refused(RefusalKind::Invalid, error.to_string()),
        }
    }

    /// Remove a device for good, if what it held is elsewhere or its operator says it is lost (F92)
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `principal` - Who asked
    /// * `expected_version` - The topology version the request was written against
    /// * `device` - The device
    /// * `lost` - The operator's word that it is gone
    fn apply_remove_device(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        device: crate::shared::devices::DeviceId,
        lost: bool,
    ) -> ControlResponse {
        if let Some(refused) = self.reclaim_gate() {
            return refused;
        }
        let Some(record) = self.pool_map.devices.get(&device) else {
            return ControlResponse::refused(RefusalKind::Invalid, format!("no device {device} is on the pool map"));
        };
        if record.tombstone.is_some() {
            return self.applied();
        }
        // a device the operator says is lost goes whatever it held; any other only once nothing
        // it holds is anything a placement group still reads
        if !lost {
            if let Some(why) = self.removal_refused(record) {
                return ControlResponse::refused(RefusalKind::WrongPhase, why);
            }
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        let by = format!("{principal} (op {op}){}", if lost { ", lost" } else { "" });
        if let Err(error) = self.pool_map.remove_device(device, &by) {
            return ControlResponse::refused(RefusalKind::Invalid, error.to_string());
        }
        // the moves arriving on it are given up by their drivers, whose arrivals cannot serve
        self.topology_version += 1;
        self.applied()
    }

    /// Why a device that is not lost cannot be removed yet, if it cannot (F92)
    ///
    /// # Arguments
    ///
    /// * `record` - The device
    fn removal_refused(&self, record: &crate::shared::poolmap::DeviceRecord) -> Option<String> {
        let device = record.id;
        // a device still placed has its groups to move first
        let Some(ended) = record.eras.last().and_then(|era| era.until).filter(|_| record.current().is_none()) else {
            return Some(format!(
                "device {device} is still placed: drain it first (drain-device {device}), or say it is lost (--lost)"
            ));
        };
        // a retired device's chunks are never read again: its groups were rebuilt elsewhere
        if record.retired.is_some() {
            return None;
        }
        // a drained one is removed once nothing arrives on it, every pool it served moved past it,
        // and its node said it holds nothing
        let arriving = self
            .pool_map
            .moves
            .iter()
            .filter(|slot| slot.devices.contains(&device))
            .count();
        if arriving > 0 {
            return Some(format!("device {device} has {arriving} moves arriving on it still"));
        }
        let classes: std::collections::BTreeSet<&str> = record.eras.iter().map(|era| era.class.as_str()).collect();
        if let Some(policy) = self.policy.as_ref() {
            for (name, pool) in &policy.objects.pools {
                if !classes.contains(pool.class.as_str()) {
                    continue;
                }
                if self.pool_map.belongs_at(name).is_some_and(|at| at < ended) {
                    return Some(format!(
                        "pool {name}'s placement groups have not been sent past device {device} yet: its target is behind generation {ended}"
                    ));
                }
            }
        }
        if !record.empty {
            return Some(format!(
                "device {device} still holds chunks, as node {} last reported: its placement groups have not all moved off it and released it",
                record.node
            ));
        }
        None
    }

    /// Give a device an operator's weight (F92)
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The topology version the request was written against
    /// * `device` - The device
    /// * `weight` - Its share of a pool
    fn apply_reweight_device(
        &mut self,
        expected_version: u64,
        device: crate::shared::devices::DeviceId,
        weight: u64,
    ) -> ControlResponse {
        if let Some(refused) = self.reclaim_gate() {
            return refused;
        }
        if weight == 0 {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("a weight of zero places nothing on device {device}: drain it instead"),
            );
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        match self.pool_map.reweight_device(device, weight) {
            Ok(moved) => {
                if moved {
                    self.topology_version += 1;
                }
                self.applied()
            }
            Err(error) => ControlResponse::refused(RefusalKind::Invalid, error.to_string()),
        }
    }

    /// Admit one placement group's move, at most sixteen arriving on a device at once
    ///
    /// # Arguments
    ///
    /// * `slot` - The move
    fn apply_admit_move(&mut self, slot: &crate::shared::poolmap::MoveSlot) -> ControlResponse {
        if let Some(refused) = self.recovery_gate() {
            return refused;
        }
        // a slot paces admission and nothing a shard routes by, so it moves no topology version:
        // each did, and a failed device's two thousand moves pushed a map to every shard of every
        // node four times a move, until members fell silent to the detector (Resolved #243)
        match self.pool_map.admit(slot.clone()) {
            Ok(_) => self.applied(),
            Err(busy) => ControlResponse::refused(
                RefusalKind::Queued,
                format!("device {busy} has another move arriving on it"),
            ),
        }
    }

    /// Free a placement group's move
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    fn apply_move_done(
        &mut self,
        pool: &str,
        consumer: u64,
        pg: crate::shared::poolmap::PlacementGroup,
    ) -> ControlResponse {
        if let Some(refused) = self.recovery_gate() {
            return refused;
        }
        // freed as it was admitted, moving no topology version (Resolved #243)
        self.pool_map.move_done(pool, consumer, pg);
        self.applied()
    }

    /// Tombstone a removed member's identity
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `op` - The plan that removed it
    fn apply_tombstone(&mut self, node: NodeId, op: Option<Uuid>) -> ControlResponse {
        let Some(state) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot be tombstoned"),
            );
        };
        if state.phase == MemberPhase::Removed {
            return self.applied();
        }
        if !matches!(state.phase, MemberPhase::Leaving | MemberPhase::Removing) {
            return ControlResponse::refused(
                RefusalKind::WrongPhase,
                format!(
                    "{node} is a plain member; only a leaving or removing member is tombstoned"
                ),
            );
        }
        // a member still holding a set is not out: its plan is what takes it out
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
        if map
            .rule_sets_served()
            .iter()
            .any(|(members, _)| members.iter().any(|member| member.node == node))
        {
            return ControlResponse::refused(RefusalKind::WrongPhase, format!("{node} still holds a replica set; its plan has to move every set before it is tombstoned"));
        }
        self.topology_version += 1;
        let version = self.topology_version;
        // a member that leaves for good takes its devices off the pool map with it (F85), and
        // their ids are never accepted again (F92)
        self.pool_map.retire_node(node);
        self.pool_map.tombstone_node(node, &format!("the removal of node {node}"));
        let state = self.members.get_mut(&node).expect("checked above");
        state.phase = MemberPhase::Removed;
        state.since = version;
        state.grace = None;
        state.episode = None;
        let incarnation = state.record.incarnation;
        self.tombstones.insert(
            node,
            Tombstone {
                incarnation,
                removed_at: version,
                op,
            },
        );
        self.applied()
    }

    /// Record a move of the replica set holding a tablet
    ///
    /// The set is what the map serves the tablet with now - the rule's members, or the
    /// configuration an earlier move left - and every table's group over it; the target is
    /// the set with `from` replaced by `to` in place, on the shard the rule would give the
    /// first tablet on `to`. One transition per set: a move or a repair not yet done on any
    /// of the set's groups queues this one behind it
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `principal` - Who asked
    /// * `expected_version` - The topology version the request was written against
    /// * `tablet` - A tablet the set serves
    /// * `from` - The member leaving
    /// * `to` - The member replacing it
    fn apply_move(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        tablet: u16,
        from: NodeId,
        to: NodeId,
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to move a replica set of".to_string(),
            );
        }
        if from == to {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("{from} cannot replace itself"),
            );
        }
        // the destination has to be an up member that is staying; the source a member at all
        match self.members.get(&to) {
            Some(state) if state.is_placeable() => {}
            Some(state) => {
                return ControlResponse::refused(
                    RefusalKind::NotUp,
                    format!(
                        "{to} is {}, and only an up member can be moved to",
                        state.state_name()
                    ),
                );
            }
            None => {
                return ControlResponse::refused(
                    RefusalKind::NotMember,
                    format!("{to} is not a member of this cluster"),
                );
            }
        }
        if !self.members.contains_key(&from) {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{from} is not a member of this cluster"),
            );
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // the set as it is served now, and the tablets the rule placed together
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
        let expected = map.replicas_of(usize::from(tablet));
        let (rule, tablets) = map.rule_set_of(usize::from(tablet));
        if expected.is_empty() || tablets.is_empty() {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no replica set serves tablet {tablet}"),
            );
        }
        let Some(slot) = expected.iter().position(|member| member.node == from) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{from} is not a member of the set serving tablet {tablet}: {expected:?}"),
            );
        };
        if expected.iter().any(|member| member.node == to) {
            return ControlResponse::refused(
                RefusalKind::Duplicate,
                format!("{to} is already a member of the set serving tablet {tablet}"),
            );
        }
        // the destination's shard: the one the rule would give the set's first tablet on it
        let nodes = self.initialized.as_ref().map_or(1, Vec::len).max(1);
        let to_shards = self
            .members
            .get(&to)
            .map_or(1, |state| state.record.shards)
            .max(1);
        // truncation cannot happen: the modulus is a shard count, which the ring bounds
        #[allow(clippy::cast_possible_truncation)]
        let to_shard = ((usize::from(tablets[0]) / nodes) % to_shards) as u16;
        let to_addr = ShardAddr::new(to, to_shard);
        let mut target = expected.clone();
        target[slot] = to_addr;
        // every table's group over the set, under the identity the rule minted
        let groups: BTreeMap<GroupId, GroupMove> = self
            .tables
            .iter()
            .map(|(_, table)| (GroupId::of(*table, &rule), GroupMove::default()))
            .collect();
        // one transition per set: a move or a repair not done on any of its groups goes first
        let behind_move = self
            .moves
            .values()
            .filter(|record| !record.is_done() && record.tablets == tablets)
            .max_by_key(|record| record.requested_at)
            .map(|record| record.op);
        let behind_repair = self
            .repairs
            .values()
            .filter(|record| {
                record
                    .groups
                    .iter()
                    .any(|(group, progress)| groups.contains_key(group) && !progress.is_done())
            })
            .max_by_key(|record| record.requested_at)
            .map(|record| record.op);
        let phase = match behind_move.or(behind_repair) {
            Some(behind) => MovePhase::Queued { behind },
            None => MovePhase::Planned,
        };
        self.topology_version += 1;
        self.moves.insert(
            op,
            MoveRecord {
                op,
                tablets,
                from: expected[slot],
                to: to_addr,
                expected,
                target,
                phase,
                groups,
                principal: principal.to_string(),
                requested_at: self.topology_version,
                outcome: None,
            },
        );
        // forget the oldest done records once too many are kept
        while self.moves.len() > KEPT_MOVES {
            let oldest = self
                .moves
                .values()
                .filter(|record| record.is_done())
                .min_by_key(|record| record.requested_at)
                .map(|record| record.op);
            match oldest {
                Some(op) => {
                    self.moves.remove(&op);
                }
                None => break,
            }
        }
        self.applied()
    }

    /// Record where a group's move stands, and what the whole record follows from it
    ///
    /// The last group's `Activated` publishes the configuration: the set's target members
    /// with every group's uniform index, replacing an earlier one for the same tablets unless
    /// that would roll a group's configuration backward. The last `Done` finishes the record
    /// and releases every move queued behind it.
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `node` - The node driving it
    /// * `incarnation` - The incarnation it drives at
    /// * `progress` - Where the group stands now
    fn apply_move_progress(
        &mut self,
        op: Uuid,
        group: GroupId,
        node: NodeId,
        incarnation: u64,
        progress: &GroupMove,
    ) -> ControlResponse {
        let Some(member) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot drive a move"),
            );
        };
        if incarnation < member.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: member.record.incarnation,
                offered: incarnation,
            };
        }
        let Some(record) = self.moves.get_mut(&op) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no move operation {op} is recorded"),
            );
        };
        if record.is_queued() {
            return ControlResponse::refused(
                RefusalKind::Queued,
                format!("move {op} is queued behind another transition and cannot be driven yet"),
            );
        }
        let Some(current) = record.groups.get_mut(&group) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("group {group} is not part of move {op}"),
            );
        };
        // a group that is done stays done, whatever a late driver says
        if current.is_done() {
            return self.applied();
        }
        if current == progress {
            return self.applied();
        }
        *current = progress.clone();
        self.topology_version += 1;
        let version = self.topology_version;
        // the last group activated publishes the set's configuration
        if !record.is_published() && record.groups.values().all(GroupMove::is_activated) {
            record.phase = MovePhase::Published;
            let configuration = record.configuration(version);
            let first = configuration.tablets[0];
            let backward = self.configurations.get(&first).is_some_and(|existing| {
                existing.configs.iter().any(|(group, index)| {
                    configuration
                        .configs
                        .get(group)
                        .is_some_and(|new| new < index)
                })
            });
            if !backward {
                self.configurations.insert(first, configuration);
            }
        }
        // the last group done finishes the record and releases what waited behind it; so does
        // the last group settled once one has failed, since a set with a failed group is never
        // published and an activated group would wait for that publish for ever
        // ([Resolved #171](../../../../docs/src/appendix/resolved/failed-group-publishes-its-set.md))
        let record = self.moves.get_mut(&op).expect("checked above");
        let failed_and_settled = record.groups.values().any(GroupMove::is_failed)
            && record.groups.values().all(GroupMove::is_settled);
        if record.groups.values().all(GroupMove::is_done) || failed_and_settled {
            record.phase = MovePhase::Done;
            let failed = record
                .groups
                .values()
                .find_map(|progress| match &progress.outcome {
                    Some(MoveOutcome::Failed { reason }) => Some(reason.clone()),
                    _ => None,
                });
            record.outcome = Some(match failed {
                Some(reason) => MoveOutcome::Failed { reason },
                None => MoveOutcome::Moved,
            });
            self.release_queued(op);
        }
        self.applied()
    }

    /// Release every transition queued behind an operation that is done
    ///
    /// A move queued behind it is planned; the first in request order alone, since the rest
    /// queue behind that one in turn. Deterministic, so every member releases the same one.
    ///
    /// # Arguments
    ///
    /// * `behind` - The operation that is done
    fn release_queued(&mut self, behind: Uuid) {
        // a repair's groups queued behind a move are pending again, all of them: a repair
        // serializes with a move on the set, and its own groups are driven one at a time
        for record in self.repairs.values_mut() {
            for progress in record.groups.values_mut() {
                if progress.phase == (RepairPhase::Queued { behind }) {
                    progress.phase = RepairPhase::Pending;
                }
            }
        }
        let next = self
            .moves
            .values()
            .filter(|record| record.phase == (MovePhase::Queued { behind }))
            .min_by_key(|record| record.requested_at)
            .map(|record| record.op);
        if let Some(next) = next {
            if let Some(record) = self.moves.get_mut(&next) {
                record.phase = MovePhase::Planned;
            }
            // the rest of the queue waits behind the one released
            for record in self.moves.values_mut() {
                if record.phase == (MovePhase::Queued { behind }) {
                    record.phase = MovePhase::Queued { behind: next };
                }
            }
        }
        // a backup's group queued behind it waits behind whatever else still holds its set - a
        // repair or a move released just now among them - or is pending again, since a cut under
        // either would be of a set half moved or half judged (item 323)
        let released: Vec<(Uuid, GroupId, BackupPhase)> = self
            .backups
            .values()
            .flat_map(|record| {
                record
                    .groups
                    .iter()
                    .filter(|(_, progress)| progress.phase == (BackupPhase::Queued { behind }))
                    .map(move |(group, _)| (record.op, *group))
            })
            .map(|(op, group)| match self.holding_set(group) {
                Some(next) => (op, group, BackupPhase::Queued { behind: next }),
                None => (op, group, BackupPhase::Pending),
            })
            .collect();
        for (op, group, phase) in released {
            if let Some(progress) = self
                .backups
                .get_mut(&op)
                .and_then(|record| record.groups.get_mut(&group))
            {
                progress.phase = phase;
            }
        }
    }

    /// The newest move or repair not yet done that holds a group's set, which a backup of the
    /// group waits behind
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    fn holding_set(&self, group: GroupId) -> Option<Uuid> {
        self.moves
            .values()
            .filter(|record| !record.is_done() && record.groups.contains_key(&group))
            .map(|record| (record.requested_at, record.op))
            .chain(
                self.repairs
                    .values()
                    .filter(|record| !record.is_done() && record.groups.contains_key(&group))
                    .map(|record| (record.requested_at, record.op)),
            )
            .max()
            .map(|(_, op)| op)
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
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no cluster has been bootstrapped to observe a member of".to_string(),
            );
        }
        // a removed identity never comes back, at any incarnation and by any door
        if self.tombstones.contains_key(&record.node)
            || self
                .members
                .get(&record.node)
                .is_some_and(|state| state.phase == MemberPhase::Removed)
        {
            return ControlResponse::Removed { node: record.node };
        }
        // a member that cannot speak the activated wire version is refused, whatever else it
        // is: the activation is the boundary no member rolls back past
        // ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let activated = self.activated_wire();
        if record.wire_max() < activated {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "{} speaks wire version {} at most and the cluster has activated {activated}",
                    record.node,
                    record.wire_max()
                ),
            );
        }
        match self.members.get(&record.node) {
            // a node nobody admitted cannot observe itself in; the leader admits it first
            None if !admitting => ControlResponse::refused(
                RefusalKind::NotMember,
                format!(
                    "{} is not a member; a joiner is admitted by the leader first",
                    record.node
                ),
            ),
            // a joiner let in for the first time, joining until it reports in
            None => {
                self.topology_version += 1;
                let version = self.topology_version;
                self.members.insert(
                    record.node,
                    MemberState::fresh(
                        record.clone(),
                        MemberHealth::Joining,
                        MemberRole::Learner,
                        version,
                    ),
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
                // a member observed under another host, rack or zone moves its devices with it,
                // in this command's version (F85)
                self.pool_map
                    .refresh_domains(record.node, &record.failure_domains);
                let state = self.members.get_mut(&record.node).expect("checked above");
                if health_moved {
                    state.since = version;
                    state.episode = None;
                    // back up under a removal keeps the removal; otherwise the grace is over
                    if state.phase != MemberPhase::Removing {
                        state.grace = None;
                    }
                }
                self.applied()
            }
        }
    }

    /// Record a backup, with a group per table group the placement derives
    ///
    /// A group whose set is under a move or a repair not yet done waits behind it, since a
    /// cut under either would be a cut of a set half moved or half judged.
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `principal` - Who asked
    /// * `expected_version` - The version the request was written against
    /// * `table` - The table, or none for every table
    /// * `path` - The directory the files go under
    fn apply_backup(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        table: Option<TableId>,
        path: &str,
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to back up".to_string(),
            );
        }
        if let Some(table) = table {
            if !self.tables.iter().any(|(_, id)| *id == table) {
                return ControlResponse::refused(
                    RefusalKind::UnknownOperation,
                    format!("table {table} is not one the placement was initialized with"),
                );
            }
        }
        if path.is_empty() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "a backup needs a directory to write under".to_string(),
            );
        }
        // the file header that identifies a backup's cluster is the one written past the
        // activation of wire version 5 ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let needed = crate::server::replication::snapshot::SNAPSHOT_V2_FROM_WIRE;
        if self.activated_wire() < needed {
            return ControlResponse::refused(RefusalKind::WireVersion, format!(
                    "a backup needs wire version {needed} activated, and the cluster has activated {}; a \
                     backup file identifies its cluster in a header only that version writes",
                    self.activated_wire()
                ));
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // every group of the table or the tables, queued behind whatever holds its set; a
        // bucket's `ObjectMeta` groups archive its objects, and its `StripeMeta` groups are
        // derived from them, so nothing of those is copied as rows (F93)
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
        let mut groups: BTreeMap<GroupId, GroupBackup> = BTreeMap::new();
        let mut archived: BTreeMap<GroupId, String> = BTreeMap::new();
        // where each group was placed, which the record written beside the files names (item 322)
        let mut placed: BTreeMap<GroupId, PlacedGroup> = BTreeMap::new();
        for (name, id) in self.tables.clone() {
            if table.is_some_and(|wanted| wanted != id) {
                continue;
            }
            let bucket = self.buckets.iter().find(|bucket| bucket.tables.contains(&name));
            let objects = bucket.filter(|bucket| name == format!("{}ObjectMeta", bucket.name));
            let derived = bucket.is_some() && objects.is_none();
            for (group, _, tablets) in map.groups_of(id) {
                placed.insert(group, PlacedGroup { table: id, tablets });
                if derived {
                    groups.insert(
                        group,
                        GroupBackup {
                            phase: BackupPhase::Done,
                            driver: None,
                            boundary: None,
                            outcome: Some(BackupOutcome::Skipped {
                                reason: format!(
                                    "{name} is derived from its bucket's objects, which its \
                                     ObjectMeta groups archive"
                                ),
                            }),
                        },
                    );
                    continue;
                }
                if let Some(bucket) = objects {
                    archived.insert(group, bucket.name.clone());
                }
                let progress = match self.holding_set(group) {
                    Some(behind) => GroupBackup {
                        phase: BackupPhase::Queued { behind },
                        ..GroupBackup::default()
                    },
                    None => GroupBackup::default(),
                };
                groups.insert(group, progress);
            }
        }
        if groups.is_empty() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "the placement derives no groups to back up".to_string(),
            );
        }
        self.topology_version += 1;
        self.backups.insert(
            op,
            BackupRecord {
                op,
                table,
                path: path.to_string(),
                principal: principal.to_string(),
                requested_at: self.topology_version,
                groups,
                archives: archived,
                placed,
            },
        );
        // forget the oldest done once too many are kept: one not done is still driven, and is
        // ended, recorded and released by nothing else (item 324)
        while self.backups.len() > KEPT_BACKUPS {
            let oldest = self
                .backups
                .values()
                .filter(|record| record.is_done())
                .min_by_key(|record| record.requested_at)
                .map(|record| record.op);
            match oldest {
                Some(op) => {
                    self.backups.remove(&op);
                }
                None => break,
            }
        }
        self.applied()
    }

    /// Apply a driver's word on where a group's backup stands
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `node` - The driver
    /// * `incarnation` - The incarnation it drives at
    /// * `progress` - Where the group stands
    fn apply_backup_progress(
        &mut self,
        op: Uuid,
        group: GroupId,
        node: NodeId,
        incarnation: u64,
        progress: &GroupBackup,
    ) -> ControlResponse {
        let Some(member) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot drive a backup"),
            );
        };
        if incarnation < member.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: member.record.incarnation,
                offered: incarnation,
            };
        }
        let Some(record) = self.backups.get_mut(&op) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no backup operation {op} is recorded"),
            );
        };
        let Some(current) = record.groups.get_mut(&group) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("group {group} is not part of backup {op}"),
            );
        };
        // a group that is done stays done, whatever a late driver says
        if current.is_done() {
            return self.applied();
        }
        if current.is_queued() {
            return ControlResponse::refused(RefusalKind::Queued, format!("group {group} of backup {op} is queued behind another operation and cannot be driven yet"));
        }
        if current == progress {
            return self.applied();
        }
        *current = progress.clone();
        self.topology_version += 1;
        self.applied()
    }

    /// Record a restore, judged whole against the files and this cluster's tables
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `principal` - Who asked
    /// * `expected_version` - The version the request was written against
    /// * `path` - The directory the files are read from
    /// * `source` - The cluster the files were cut in
    /// * `files` - Every file, as the leader read the manifests
    /// * `archives` - Every archive of a bucket's objects
    #[allow(clippy::too_many_arguments)]
    fn apply_restore(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        path: &str,
        source: ClusterId,
        files: &[BackupFile],
        archives: &[ArchiveFile],
    ) -> ControlResponse {
        if self.policy.is_none() || self.initialized.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no placement has been initialized to restore into".to_string(),
            );
        }
        // once per cluster: a second restore would put two histories under one identity
        if let Some(restored) = self.restored_from {
            return ControlResponse::refused(RefusalKind::AlreadyInitialized, format!("this cluster was already restored from {restored}; a restore is once, into a new cluster"));
        }
        if Some(source) == self.cluster {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "a backup is restored into a new cluster, never into the one it was cut in"
                    .to_string(),
            );
        }
        // every file judged by its own table, table by table, so a backup cut before an
        // addition restores after it, the tables it lacks left empty; a table whose row changed
        // and a bucket's rows are each refused by name (F93, item 255)
        let schema = self.schema.clone().unwrap_or_default();
        let generated: Vec<&str> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.tables.iter().map(String::as_str))
            .collect();
        for file in files {
            if let Err(reason) = crate::server::control::schema::judge_table(
                &schema,
                &generated,
                &file.table_name,
                file.table,
                file.table_fingerprint,
            ) {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!("{}: {reason}", file.name),
                );
            }
        }
        // every archive of a bucket this cluster declares and binds, written from its rows'
        // layout, dealt round the placement: every host holds every file once a backup is shipped
        if files.is_empty() && archives.is_empty() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "the backup holds no files".to_string(),
            );
        }
        let bound = self
            .policy
            .as_ref()
            .map(|policy| policy.objects.bindings.clone())
            .unwrap_or_default();
        // dealt over the members that can put them now, not the placement the cluster began with,
        // which a member removed since is still in (item 263)
        let placement = self.archive_nodes();
        let mut restoring: BTreeMap<String, ArchiveRestore> = BTreeMap::new();
        for (index, archive) in archives.iter().enumerate() {
            if !self
                .buckets
                .iter()
                .any(|bucket| bucket.name == archive.bucket && bucket.id == archive.bucket_id)
            {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "{} holds bucket {}'s objects, which this cluster's schema does not declare",
                        archive.name, archive.bucket
                    ),
                );
            }
            if !bound.contains_key(&archive.bucket) {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "{} holds bucket {}'s objects, and this cluster binds the bucket to no pool",
                        archive.name, archive.bucket
                    ),
                );
            }
            if archive.rows_layout != crate::shared::objects::OBJECT_ROWS_LAYOUT
                || archive.format != crate::server::objects::archive::ARCHIVE_FORMAT
            {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "{} was written as archive format {} of object rows {:#018x}, and this \
                         build reads format {} of {:#018x}",
                        archive.name,
                        archive.format,
                        archive.rows_layout,
                        crate::server::objects::archive::ARCHIVE_FORMAT,
                        crate::shared::objects::OBJECT_ROWS_LAYOUT
                    ),
                );
            }
            let Some(node) = placement.get(index % placement.len().max(1)).copied() else {
                return ControlResponse::refused(
                    RefusalKind::NotInitialized,
                    "no placement names a member to put the archives again".to_string(),
                );
            };
            restoring.insert(
                archive.name.clone(),
                ArchiveRestore {
                    file: archive.clone(),
                    node,
                    phase: ArchivePhase::Pending,
                    put: 0,
                    bytes: 0,
                    failed: None,
                    generation: 0,
                },
            );
        }
        // every tablet of every restored table in exactly one file
        // a tablet id is twelve bits, so the count fits a u16
        #[allow(clippy::cast_possible_truncation)]
        let coverage = match judge_coverage(
            files,
            &self.tables,
            crate::server::ring::TABLET_COUNT as u16,
        ) {
            Ok(coverage) => coverage,
            Err(reason) => return ControlResponse::refused(RefusalKind::Invalid, reason),
        };
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // every group of every covered table, with the files covering its tablets
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
        let mut groups: BTreeMap<GroupId, GroupRestore> = BTreeMap::new();
        for (table, covered) in &coverage {
            for (group, _, tablets) in map.groups_of(*table) {
                let mut names: Vec<String> = tablets
                    .iter()
                    .filter_map(|tablet| covered.get(tablet).cloned())
                    .collect();
                names.sort();
                names.dedup();
                groups.insert(
                    group,
                    GroupRestore {
                        files: names,
                        ..GroupRestore::default()
                    },
                );
            }
        }
        if groups.is_empty() && restoring.is_empty() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "the placement derives no groups to restore into".to_string(),
            );
        }
        self.topology_version += 1;
        self.restored_from = Some(source);
        self.restores.insert(
            op,
            RestoreRecord {
                op,
                path: path.to_string(),
                source,
                principal: principal.to_string(),
                requested_at: self.topology_version,
                files: files.to_vec(),
                groups,
                archives: restoring,
            },
        );
        while self.restores.len() > KEPT_RESTORES {
            let oldest = self
                .restores
                .values()
                .min_by_key(|record| record.requested_at)
                .map(|record| record.op);
            match oldest {
                Some(op) => {
                    self.restores.remove(&op);
                }
                None => break,
            }
        }
        self.applied()
    }

    /// Apply a driver's word on where a group's restore stands
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `node` - The driver
    /// * `incarnation` - The incarnation it drives at
    /// * `progress` - Where the group stands
    fn apply_restore_progress(
        &mut self,
        op: Uuid,
        group: GroupId,
        node: NodeId,
        incarnation: u64,
        progress: &GroupRestore,
    ) -> ControlResponse {
        let Some(member) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot drive a restore"),
            );
        };
        if incarnation < member.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: member.record.incarnation,
                offered: incarnation,
            };
        }
        // one drive of a group at a time once the fence is activated: below it every replica takes
        // any member's word as a replica built before the fence does (item 336)
        let fenced = self.activated_wire() >= RESTORE_FENCE_WIRE_VERSION;
        let Some(record) = self.restores.get_mut(&op) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no restore operation {op} is recorded"),
            );
        };
        let Some(current) = record.groups.get_mut(&group) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("group {group} is not part of restore {op}"),
            );
        };
        if current.is_done() {
            // a drive that never learned the group is done is told, and goes no further: one that
            // started behind would otherwise install over the restored table
            if fenced && progress.phase != RestorePhase::Done {
                return ControlResponse::RestoreAhead {
                    topology_version: self.topology_version,
                    record: current.clone(),
                };
            }
            return self.applied();
        }
        // a driver of an earlier try, whose group was retried since, is too late to say
        // anything ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md)),
        // and once fenced it is told so, and stops
        if progress.generation != current.generation {
            if fenced && progress.generation < current.generation {
                return ControlResponse::refused(
                    RefusalKind::Superseded,
                    format!(
                        "group {group}'s restore was retried as try {}; a drive of try {} is too late",
                        current.generation, progress.generation
                    ),
                );
            }
            return self.applied();
        }
        let mut progress = progress.clone();
        if fenced {
            // a drive under an older lead than the record's was taken over, and stops: its
            // late word could fail a group a newer drive restores
            if progress.term < current.term {
                return ControlResponse::refused(
                    RefusalKind::Superseded,
                    format!(
                        "group {group}'s restore is driven under term {} now; a drive under term {} was taken over",
                        current.term, progress.term
                    ),
                );
            }
            // a drive that started behind the record never moves it back: it takes the drive
            // over, and goes on from where the group stands
            if progress.phase.rank() < current.phase.rank() {
                let mut taken = current.clone();
                taken.term = progress.term;
                if progress.driver.is_some() {
                    taken.driver = progress.driver;
                }
                if taken != *current {
                    *current = taken;
                    self.topology_version += 1;
                }
                return ControlResponse::RestoreAhead {
                    topology_version: self.topology_version,
                    record: current.clone(),
                };
            }
        } else {
            // a replica built before the fence drops the term, so it is the record's here too,
            // and a record changes, and the version moves, the same way on both
            progress.term = current.term;
        }
        // a progress that names no installed file keeps the one the record holds: a drive resumed
        // past the install never knew it, and a replica built before the field drops it, so a
        // record changes, and the version moves, the same way on both (item 327)
        if progress.installed.is_none() {
            progress.installed = current.installed;
        }
        if *current == progress {
            return self.applied();
        }
        *current = progress;
        self.topology_version += 1;
        self.applied()
    }

    /// Apply a driver's word on how far an archive's objects are put again (F93)
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `archive` - The archive
    /// * `node` - The driver
    /// * `incarnation` - The incarnation it drives at
    /// * `progress` - Where it stands
    fn apply_archive_progress(
        &mut self,
        op: Uuid,
        archive: &str,
        node: NodeId,
        incarnation: u64,
        progress: &ArchiveRestore,
    ) -> ControlResponse {
        let Some(member) = self.members.get(&node) else {
            return ControlResponse::refused(
                RefusalKind::NotMember,
                format!("{node} is not a member, so cannot drive a restore"),
            );
        };
        if incarnation < member.record.incarnation {
            return ControlResponse::Fenced {
                node,
                committed: member.record.incarnation,
                offered: incarnation,
            };
        }
        let Some(record) = self.restores.get_mut(&op) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no restore operation {op} is recorded"),
            );
        };
        let Some(current) = record.archives.get_mut(archive) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("{archive} is not part of restore {op}"),
            );
        };
        // a done archive stays done, and a driver of an earlier try is too late to say anything
        if current.is_done() || progress.generation != current.generation || current == progress {
            return self.applied();
        }
        // and nothing goes backwards: an archive put past a point is never put from before it
        if progress.put < current.put {
            return self.applied();
        }
        *current = progress.clone();
        self.topology_version += 1;
        self.applied()
    }

    /// Put every group a finished restore failed back to be driven, from where each failed
    ///
    /// Under the same operation and against the same files, so nothing about what is restored
    /// is judged again; a group that was restored or skipped is not touched. Refused below
    /// wire version 6, while any group is still running, and when nothing failed
    /// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md)).
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The topology version the request was written against
    /// * `restore` - The restore operation
    fn apply_retry_restore(&mut self, expected_version: u64, restore: Uuid) -> ControlResponse {
        // the command and the record's new fields are version 6's
        let needed = crate::server::control::backup::RESTORE_RETRY_FROM_WIRE;
        if self.activated_wire() < needed {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a restore retry needs wire version {needed} activated, and the cluster has activated {}",
                    self.activated_wire()
                ),
            );
        }
        let Some(record) = self.restores.get(&restore) else {
            return ControlResponse::refused(
                RefusalKind::UnknownOperation,
                format!("no restore operation {restore} is recorded"),
            );
        };
        // a running restore is still trying; its failures are not final yet
        if !record.is_done() {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("restore {restore} is still running; a retry is of a finished one"),
            );
        }
        if !record.groups.values().any(GroupRestore::failed)
            && !record.archives.values().any(ArchiveRestore::failed)
        {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("no group or archive of restore {restore} failed, so there is nothing to retry"),
            );
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // every failed group back from where it failed, the rest as they are
        let Some(record) = self.restores.get_mut(&restore) else {
            return self.applied();
        };
        for progress in record
            .groups
            .values_mut()
            .filter(|progress| progress.failed())
        {
            progress.retry();
        }
        // and every failed archive, from past the objects it put (F93)
        for progress in record
            .archives
            .values_mut()
            .filter(|progress| progress.failed())
        {
            progress.retry();
        }
        self.topology_version += 1;
        self.applied()
    }

    /// Apply an operator's recovery: every lost member tombstoned and removing, the evidence kept
    ///
    /// A lost member is refused at every door from here on, by its tombstone, and is
    /// `Removing` under a `Remove` plan of its own - derived from the recovery's identity, so
    /// every replica derives the same - which the leader drives once a member outside its
    /// sets is there to take them, exactly as an expired grace is
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md)); the plan's end is what
    /// moves it to `Removed`.
    ///
    /// # Arguments
    ///
    /// * `op` - The recovery
    /// * `survivors` - The members kept
    /// * `lost` - The members lost
    /// * `at` - The node the recovery was run on
    /// * `last_committed` - The index the survivor had committed
    /// * `recovered_ms` - When
    fn apply_force_recovered(
        &mut self,
        op: Uuid,
        survivors: &[NodeId],
        lost: &[NodeId],
        at: NodeId,
        last_committed: u64,
        recovered_ms: u64,
    ) -> ControlResponse {
        if self.cluster.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no cluster has been bootstrapped to recover".to_string(),
            );
        }
        // every survivor has to be a member, and no survivor is lost
        for node in survivors {
            if !self.members.contains_key(node) {
                return ControlResponse::refused(
                    RefusalKind::NotMember,
                    format!("{node} is not a member, so cannot survive a recovery"),
                );
            }
            if lost.contains(node) {
                return ControlResponse::refused(
                    RefusalKind::Duplicate,
                    format!("{node} is named both surviving and lost"),
                );
            }
        }
        self.topology_version += 1;
        let version = self.topology_version;
        // the lost members are tombstoned, so their identities never return, and removing
        // under a plan each, so their sets are rebuilt once there is somebody to rebuild on
        for node in lost {
            let Some(member) = self.members.get_mut(node) else {
                continue;
            };
            if member.phase == MemberPhase::Removed {
                continue;
            }
            member.phase = MemberPhase::Removing;
            member.health = MemberHealth::Down;
            member.since = version;
            member.grace = None;
            member.role = MemberRole::Learner;
            let incarnation = member.record.incarnation;
            self.tombstones.entry(*node).or_insert(Tombstone {
                incarnation,
                removed_at: version,
                op: Some(op),
            });
            // the plan's identity, derived from the recovery's and the member's so every
            // replica derives the same one
            let mut seed = Vec::with_capacity(32);
            seed.extend_from_slice(op.as_bytes());
            seed.extend_from_slice(node.0.as_bytes());
            let plan = Uuid::from_u64_pair(gxhash::gxhash64(&seed, 0), gxhash::gxhash64(&seed, 1));
            if self.check_one_plan(Some(*node)).is_none() {
                self.record_plan(PlanRecord::new(
                    plan,
                    PlanKind::Remove {
                        node: *node,
                        replacement: None,
                    },
                    "recovery",
                    version,
                ));
            }
        }
        self.recoveries.push(RecoveryRecord {
            survivors: survivors.to_vec(),
            lost: lost.to_vec(),
            at,
            last_committed,
            applied_at: version,
            recovered_ms,
        });
        self.applied()
    }

    /// Activate a wire version, once every member speaks it
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The version the request was written against
    /// * `wire` - The version to activate
    /// * `members` - The newest version each member reported, as the leader heard it
    fn apply_activate(
        &mut self,
        expected_version: u64,
        wire: u8,
        members: &BTreeMap<NodeId, u8>,
    ) -> ControlResponse {
        if self.cluster.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no cluster has been bootstrapped to activate a wire version in".to_string(),
            );
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // an activation never goes down: the boundary is what nobody rolls back past
        let current = self.activated_wire();
        if wire < current {
            return ControlResponse::refused(RefusalKind::WireVersion, format!(
                    "wire version {wire} is below the activated {current}; an activation never lowers"
                ));
        }
        // and it never goes below the floor every member reads
        if wire < MIN_PEER_VERSION {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!("wire version {wire} is below the floor {MIN_PEER_VERSION}"),
            );
        }
        // every member in any phase but removed has to speak it, by what the command says
        // it reported; one the command does not name is not known to
        let behind: Vec<String> = self
            .members
            .values()
            .filter(|member| member.phase != MemberPhase::Removed)
            .map(|member| {
                (
                    member.record.node,
                    members
                        .get(&member.record.node)
                        .copied()
                        .unwrap_or(0)
                        .max(MIN_PEER_VERSION),
                )
            })
            .filter(|(_, reported)| *reported < wire)
            .map(|(node, reported)| format!("{node} at {reported}"))
            .collect();
        if !behind.is_empty() {
            return ControlResponse::refused(RefusalKind::WireVersion, format!(
                    "wire version {wire} cannot be activated: {} speak{} less; restart {} on a build \
                     that speaks {wire} first",
                    behind.join(", "),
                    if behind.len() == 1 { "s" } else { "" },
                    if behind.len() == 1 { "it" } else { "them" },
                ));
        }
        // the same version again is applied without moving anything
        if wire == current {
            return self.applied();
        }
        self.activated = wire;
        // and what every member reported is written into its record, which heals a record a
        // build from before the field persisted without it
        for (node, reported) in members {
            if let Some(member) = self.members.get_mut(node) {
                if member.record.wire_max() < *reported {
                    member.record.wire_max = *reported;
                }
            }
        }
        self.topology_version += 1;
        self.applied()
    }

    /// The wire version the cluster has activated: what was committed, or the floor
    #[must_use]
    pub fn activated_wire(&self) -> u8 {
        self.activated.max(MIN_PEER_VERSION)
    }

    /// Grant or revoke a principal's privileges on a table, moving the version only on a change
    ///
    /// A grant of what is held and a revoke of what is not are applied without moving the
    /// version, as a read policy set to what it already is is, so a repeat by a fresh operation
    /// pushes no map ([F77](../../../../docs/src/features/authorization.md)).
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The topology version the request was written against
    /// * `grantee` - The principal whose grants change
    /// * `on` - Whether the grant is on a table or a bucket
    /// * `name` - The table or bucket, as the schema spells it, or `*`
    /// * `privileges` - What is granted or taken back
    /// * `grant` - Whether this is a grant rather than a revoke
    fn change_grants(
        &mut self,
        expected_version: u64,
        grantee: &str,
        on: GrantOn,
        name: &str,
        privileges: Privileges,
        grant: bool,
    ) -> ControlResponse {
        // grants are cluster policy, so there has to be a cluster to hold them
        if self.policy.is_none() {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no cluster has been bootstrapped to grant on".to_string(),
            );
        }
        // nobody, and nothing, are never what a grant meant
        if grantee.trim().is_empty() {
            return ControlResponse::refused(RefusalKind::Invalid, GrantError::NoPrincipal.to_string());
        }
        if privileges.is_empty() {
            return ControlResponse::refused(RefusalKind::Invalid, GrantError::NoPrivileges.to_string());
        }
        // the name has to be one the schema serves, once the initialization recorded which;
        // before it, the proposer resolved the name against its own schema
        if !self.tables.is_empty() {
            let served = served_of(&self.tables, &self.buckets);
            if let Err(error) = check_grant(grantee, on, name, &served) {
                return ControlResponse::refused(RefusalKind::UnknownOperation, error.to_string());
            }
        }
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // change the committed grants, and say whether that changed anything
        let grants = &mut self.policy.as_mut().expect("checked above").grants;
        let changed = if grant {
            grants.grant_on(grantee, on, name, privileges)
        } else {
            grants.revoke_on(grantee, on, name, privileges)
        };
        // a change is pushed to every coordinator in the next map
        if changed {
            self.topology_version += 1;
        }
        self.applied()
    }

    /// Move the committed schema to one that appends tables or buckets to it
    ///
    /// Refused by name below wire 15, before a schema is initialized, for a schema that does not
    /// extend the committed one, while a member's build does not hold it, while a move or a repair
    /// is open, and for pools that change a committed pool or binding or leave a bucket unbound.
    /// The same schema again moves nothing. Applied, the map serves the new tables, whose groups
    /// every shard starts, and each new bucket is bound past every consumer id given so far, at
    /// the generation its pool belongs at now
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The version the request was written against
    /// * `schema` - The schema to move to
    /// * `tables` - Every table it serves
    /// * `buckets` - Every bucket it declares
    /// * `pools` - Every pool and binding
    fn apply_extend_schema(
        &mut self,
        expected_version: u64,
        schema: &SchemaRecord,
        tables: &[(String, TableId)],
        buckets: &[BucketRecord],
        pools: &PoolsConf,
    ) -> ControlResponse {
        // nothing extends a schema until every member can read the command and what it starts
        if self.activated_wire() < SCHEMA_WIRE_VERSION {
            return ControlResponse::refused(
                RefusalKind::WireVersion,
                format!(
                    "a schema is extended once wire version {SCHEMA_WIRE_VERSION} is activated, and {} is",
                    self.activated_wire()
                ),
            );
        }
        let Some(committed) = self.schema.clone() else {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no schema has been initialized to extend".to_string(),
            );
        };
        if let Some(refusal) = self.check_version(expected_version) {
            return refusal;
        }
        // the same schema again is where the cluster already is
        if schema.id == committed.id {
            return self.applied();
        }
        // an append along the lineage, its tables and buckets the committed ones and more
        if !schema.extends(&committed)
            || tables.len() < self.tables.len()
            || tables[..self.tables.len()] != self.tables[..]
            || buckets.len() < self.buckets.len()
            || buckets[..self.buckets.len()] != self.buckets[..]
        {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!(
                    "schema {:#018x} does not append to the committed schema {:#018x}: only tables \
                     and buckets declared after every committed field are rolled onto a cluster, \
                     and any other change is a new cluster and a restore",
                    schema.id, committed.id
                ),
            );
        }
        // every member not removed runs a build that holds it, as its last start recorded
        for (node, member) in &self.members {
            if member.phase == MemberPhase::Removed {
                continue;
            }
            if !crate::shared::lineage::holds(&member.record.schema_lineage, schema.id) {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "{node} runs a build of schema {:#018x}, which does not hold {:#018x}: \
                         upgrade it first",
                        member.record.schema_id, schema.id
                    ),
                );
            }
        }
        // a move or a repair records its groups when it is applied, so a table added under one
        // would have groups it does not drive
        if self.moves.values().any(|record| !record.is_done())
            || self.repairs.values().any(|record| !record.is_done())
        {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                "a move or a repair is open; a schema is extended once it is done".to_string(),
            );
        }
        // the pools: every committed pool and binding unchanged, and every bucket bound
        let Some(policy) = self.policy.as_ref() else {
            return ControlResponse::refused(
                RefusalKind::NotInitialized,
                "no cluster has been bootstrapped to extend".to_string(),
            );
        };
        let objects = &policy.objects;
        for (name, pool) in &objects.pools {
            if pools.pools.get(name) != Some(pool) {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "pool {name} differs from the committed one: a schema extension adds \
                         pools and changes none"
                    ),
                );
            }
        }
        for (bucket, binding) in &objects.bindings {
            if pools.buckets.get(bucket).map(|spec| &spec.pool) != Some(&binding.pool) {
                return ControlResponse::refused(
                    RefusalKind::Invalid,
                    format!(
                        "bucket {bucket} is bound to pool {} and stays bound there: a schema \
                         extension binds new buckets alone",
                        binding.pool
                    ),
                );
            }
        }
        if let Err(error) = pools.check(&served_of(tables, buckets)) {
            return ControlResponse::refused(RefusalKind::Invalid, error.to_string());
        }
        // every consumer id given so far stays given, and a new binding places where its pool
        // belongs now
        let minted: BTreeMap<String, u64> = objects
            .bindings
            .iter()
            .map(|(bucket, binding)| (bucket.clone(), binding.consumer))
            .collect();
        let (mut bound, _) = ObjectPolicy::bind(pools, &minted);
        for (bucket, binding) in &mut bound.bindings {
            match objects.bindings.get(bucket) {
                Some(committed) => binding.since = committed.since,
                None => binding.since = self.pool_map.belongs_at(&binding.pool).unwrap_or(0),
            }
        }
        self.schema = Some(schema.clone());
        self.tables = tables.to_vec();
        self.buckets = buckets.to_vec();
        if let Some(policy) = self.policy.as_mut() {
            policy.objects = bound;
        }
        self.topology_version += 1;
        self.applied()
    }

    /// Refuse a request written against a version other than the current one
    ///
    /// # Arguments
    ///
    /// * `expected` - The version the request was written against
    fn check_version(&self, expected: u64) -> Option<ControlResponse> {
        // an optimistic check against the view the caller wrote the request from
        if expected == self.topology_version {
            None
        } else {
            Some(ControlResponse::refused(
                RefusalKind::StaleVersion,
                format!(
                    "stale version: the request was written against topology version {expected} \
                     and the cluster is at {}",
                    self.topology_version
                ),
            ))
        }
    }

    /// The applied response at the current version
    fn applied(&self) -> ControlResponse {
        ControlResponse::Applied {
            topology_version: self.topology_version,
        }
    }

    /// Whether a node may start on what its own log recovered, and why not by name
    ///
    /// A node whose log says the cluster activated a wire version it cannot speak, or committed a
    /// schema its build does not hold, stops before it serves anything: every peer would refuse it
    /// at the door ([F48](../../../../docs/src/features/rolling-compatibility.md),
    /// [F93](../../../../docs/src/features/operations-and-the-real-cluster.md)). So does one whose
    /// log says the cluster was initialized and names no committed schema, as a cluster before
    /// wire 15 was: nothing commits one later, and the cluster would run on with no extension
    /// possible, every table of a restore refused and no start held to its schema
    /// ([Resolved #270](../../../../docs/src/appendix/resolved/no-committed-schema-refused.md)).
    ///
    /// # Arguments
    ///
    /// * `node` - The node starting
    /// * `member` - What it advertises
    ///
    /// # Errors
    ///
    /// Why it may not start.
    pub fn judge_start(&self, node: NodeId, member: &MemberRecord) -> Result<(), ShoalError> {
        // a wire version below the one the cluster activated
        let activated = self.activated_wire();
        if member.wire_max < activated {
            return Err(ShoalError::WireBelowActivated {
                node,
                activated,
                ours: member.wire_max,
            });
        }
        // a build that does not hold the schema the cluster committed: no member rolls back past
        // an `ExtendSchema`, and an initialized cluster with none is bootstrapped again
        match &self.schema {
            Some(committed) if !crate::shared::lineage::holds(&member.schema_lineage, committed.id) => {
                Err(ShoalError::SchemaBelowCommitted {
                    node,
                    committed: committed.id,
                    ours: member.schema_id,
                })
            }
            None if self.initialized.is_some() => Err(ShoalError::SchemaNotCommitted { node }),
            _ => Ok(()),
        }
    }

    /// The members an archive of a restore is dealt to, in node order
    ///
    /// The placeable members, up and plain: a member leaving, removing or removed never puts an
    /// archive again, and a down one would hold it until it returns. With none placeable, the
    /// placement the cluster was initialized over, as before
    /// ([Resolved #263](../../../../docs/src/appendix/resolved/archives-dealt-again.md)).
    fn archive_nodes(&self) -> Vec<NodeId> {
        // up and plain, in the members' own order
        let placeable: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, member)| member.is_placeable())
            .map(|(node, _)| *node)
            .collect();
        if placeable.is_empty() {
            return self.initialized.clone().unwrap_or_default();
        }
        placeable
    }

    /// Deal every unfinished archive whose node is no longer a plain member to one that can put it
    ///
    /// Only the node an archive is dealt to puts it, and a member that is leaving, removing or
    /// removed never will, so an archive dealt to one stayed unfinished for good and its restore
    /// with it. It is dealt again a generation up, from the objects it put, so a driver of the
    /// old deal is too late to say anything. A member that is only down keeps its archives until
    /// it returns ([Resolved #263](../../../../docs/src/appendix/resolved/archives-dealt-again.md)).
    ///
    /// Returns whether any archive was dealt again.
    fn redeal_archives(&mut self) -> bool {
        // nothing to deal, or nobody to deal it to
        if self.restores.is_empty() {
            return false;
        }
        let nodes = self.archive_nodes();
        if nodes.is_empty() {
            return false;
        }
        let mut dealt = false;
        for record in self.restores.values_mut() {
            for (index, progress) in record.archives.values_mut().enumerate() {
                // a finished archive, and one whose node is a plain member, stay as they are
                let stays = self
                    .members
                    .get(&progress.node)
                    .is_some_and(|member| member.phase == MemberPhase::Member);
                if progress.is_done() || stays {
                    continue;
                }
                // dealt round the members by its place in the record, a try of its own
                progress.node = nodes[index % nodes.len()];
                progress.generation += 1;
                progress.phase = ArchivePhase::Pending;
                progress.failed = None;
                dealt = true;
            }
        }
        if dealt {
            self.topology_version += 1;
        }
        dealt
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
                // a removed identity is never re-admitted by a configuration that still names it
                None if self.tombstones.contains_key(node) => {}
                None => {
                    self.members.insert(
                        *node,
                        MemberState::fresh(
                            record.clone(),
                            MemberHealth::Joining,
                            role,
                            self.topology_version + 1,
                        ),
                    );
                    changed = true;
                }
            }
        }
        // a member the configuration no longer names votes in nothing: a removed one, taken
        // out of the group ([F46](../../../../docs/src/features/capacity-rebalancing.md))
        let named: std::collections::BTreeSet<NodeId> =
            membership.nodes().map(|(node, _)| *node).collect();
        for (node, state) in &mut self.members {
            if !named.contains(node) && state.role != MemberRole::Learner {
                state.role = MemberRole::Learner;
                changed = true;
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

    /// The learner the leader promotes next, and its record
    ///
    /// The first member in node order that is placeable, that the group's membership holds and
    /// that is not a voter yet - in a zone no voter stands in when one is, then in a rack no voter
    /// stands in, then on a host no voter stands on, so the voters spread over the widest domains
    /// there are, and the first of them otherwise, so a cluster on one machine promotes exactly as
    /// it did before hosts were recorded. A level a member leaves out is the nearest one below it
    /// names, so a cluster that names no rack or zone promotes by host alone, and a member that
    /// names nothing stands in a domain of its own
    /// ([F81](../../../../docs/src/features/failure-domains.md),
    /// [F83](../../../../docs/src/features/rack-and-zone-domains.md)).
    ///
    /// # Arguments
    ///
    /// * `voters` - The group's voters now
    /// * `in_membership` - Whether the group's membership holds a node, as a learner or a voter
    #[must_use]
    pub fn promotion_candidate(
        &self,
        voters: &std::collections::BTreeSet<NodeId>,
        in_membership: impl Fn(NodeId) -> bool,
    ) -> Option<(NodeId, MemberRecord)> {
        // the learners that may be promoted, in node order
        let learners: Vec<(&NodeId, &MemberState)> = self
            .members
            .iter()
            .filter(|(node, member)| {
                member.is_placeable() && !voters.contains(*node) && in_membership(**node)
            })
            .collect();
        // the domains the voters already stand in, at every level
        let held: std::collections::BTreeSet<(&str, (&str, &str))> = voters
            .iter()
            .filter_map(|voter| self.members.get(voter))
            .flat_map(|member| {
                LEVELS.into_iter().filter_map(|level| {
                    domain_at(&member.record.failure_domains, level).map(|domain| (level, domain))
                })
            })
            .collect();
        // whether a learner would stand beside a voter, at each level from the widest
        let beside = |member: &MemberState| {
            LEVELS.map(|level| {
                domain_at(&member.record.failure_domains, level)
                    .is_some_and(|domain| held.contains(&(level, domain)))
            })
        };
        // the learner beside the fewest voters, widest level first; the first in node order on
        // a tie, which is every learner when nothing is named
        learners
            .iter()
            .min_by_key(|(_, member)| beside(member))
            .map(|(node, member)| (**node, member.record.clone()))
    }

    /// The plans not yet done, in request order
    pub fn open_plans(&self) -> Vec<&PlanRecord> {
        let mut plans: Vec<&PlanRecord> = self
            .plans
            .values()
            .filter(|record| !record.is_done())
            .collect();
        plans.sort_by_key(|record| record.requested_at);
        plans
    }

    /// How many replica sets hold a copy on a member the cluster has given up on
    ///
    /// A removing or removed member's copy is one the cluster no longer counts; a set with
    /// one is under-replicated until its plan rebuilds the copy elsewhere
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    pub fn under_replicated_sets(&self) -> u32 {
        if self.initialized.is_none() {
            return 0;
        }
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables);
        let gone: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, state)| {
                matches!(state.phase, MemberPhase::Removing | MemberPhase::Removed)
            })
            .map(|(node, _)| *node)
            .collect();
        let short = map
            .rule_sets_served()
            .iter()
            .filter(|(members, _)| members.iter().any(|member| gone.contains(&member.node)))
            .count();
        u32::try_from(short).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BucketRecord, Consistency, ControlCommand, ControlResponse, ControlState, MemberHealth,
        MemberRecord, MemberRole, RefusalKind,
    };
    use crate::server::conf::Cluster;
    use crate::shared::domains::HOST;
    use crate::shared::grants::GrantOn;
    use crate::shared::identity::{BucketId, ClusterId, GroupId, NodeId, TableId};
    use uuid::Uuid;

    /// The device commands wait for wire 8, then commit what a device is once, move the
    /// generation only for that, and take a failure report only when it is newer than the last
    /// (F85)
    #[test]
    fn device_commands_wait_for_wire_8_and_apply_in_order() {
        use crate::shared::devices::{DeviceFacts, DeviceId, SliceId};
        use crate::shared::protocol::{MIN_PEER_VERSION, POOL_MAP_WIRE_VERSION};
        let (mut state, _, node) = bootstrapped();
        let device = DeviceId::mint();
        let facts = vec![DeviceFacts {
            id: device,
            class: "ssd".to_string(),
            slices: vec![SliceId::mint()],
            size_bytes: 1 << 40,
            weight: None,
            write_back: false,
            rotational: false,
            journal: None,
        }];
        let observe = |incarnation: u64| ControlCommand::ObserveDevices {
            node,
            incarnation,
            devices: facts.clone(),
        };
        // below wire 8 nothing commits a device, refused by name
        match state.apply(&observe(1)) {
            ControlResponse::Refused { kind, reason } => {
                assert_eq!(kind, RefusalKind::WireVersion);
                assert!(reason.contains(&POOL_MAP_WIRE_VERSION.to_string()), "{reason}");
            }
            other => panic!("a device was committed below wire 8: {other:?}"),
        }
        assert!(state.pool_map.is_empty());
        // the member restarts speaking wire 8, which is then activated
        let mut upgraded = member(node, "a");
        upgraded.wire_min = MIN_PEER_VERSION;
        upgraded.wire_max = POOL_MAP_WIRE_VERSION;
        upgraded.incarnation = 2;
        assert!(matches!(state.apply(&ControlCommand::ObserveMember(upgraded)), ControlResponse::Applied { .. }));
        let version = state.topology_version;
        let activated = state.apply(&ControlCommand::Activate {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: version,
            wire: POOL_MAP_WIRE_VERSION,
            members: [(node, POOL_MAP_WIRE_VERSION)].into_iter().collect(),
        });
        assert!(matches!(activated, ControlResponse::Applied { .. }), "{activated:?}");
        // an older start's devices are fenced, and the current one's committed once
        assert!(matches!(state.apply(&observe(1)), ControlResponse::Fenced { .. }));
        let before = (state.topology_version, state.pool_map.generation);
        assert!(matches!(state.apply(&observe(2)), ControlResponse::Applied { .. }));
        let committed = (state.topology_version, state.pool_map.generation);
        assert!(committed.0 > before.0 && committed.1 > before.1, "{before:?} {committed:?}");
        assert!(state.pool_map.devices.contains_key(&device));
        // observing the same devices again moves nothing
        assert!(matches!(state.apply(&observe(2)), ControlResponse::Applied { .. }));
        assert_eq!((state.topology_version, state.pool_map.generation), committed);
        // a failure report is taken, and an older one after it changes nothing
        let report = |seq: u64, failed: bool| ControlCommand::ReportDevices {
            node,
            incarnation: 2,
            seq,
            failed: if failed {
                [(device, "lost".to_string())].into_iter().collect()
            } else {
                std::collections::BTreeMap::new()
            },
            empty: std::collections::BTreeSet::new(),
        };
        assert!(matches!(state.apply(&report(2, true)), ControlResponse::Applied { .. }));
        assert!(state.pool_map.devices[&device].failed.is_some());
        // a failure moves the version, so it is pushed, and never the generation
        assert_eq!(state.pool_map.generation, committed.1);
        assert!(matches!(state.apply(&report(1, false)), ControlResponse::Applied { .. }));
        assert!(state.pool_map.devices[&device].failed.is_some(), "an older report cleared a newer failure");
        // and a newer one clears it
        assert!(matches!(state.apply(&report(3, false)), ControlResponse::Applied { .. }));
        assert!(state.pool_map.devices[&device].failed.is_none());
    }

    /// A drain, a removal and a reweight wait for wire 14; a drain ends a device's era once and
    /// leaves it serving; a removal is refused by name while the device is placed, while a move
    /// arrives on it, and until its node reports it empty, and admitted for a retired device or
    /// on the operator's word that it is lost; a removed device's id is refused at every door;
    /// a reweight begins an era at its weight (F92)
    #[test]
    fn a_device_is_drained_removed_and_reweighted_only_as_its_facts_allow() {
        use crate::shared::devices::{DeviceFacts, DeviceId, SliceId};
        use crate::shared::poolmap::{MoveSlot, PlacementGroup};
        use crate::shared::protocol::{MIN_PEER_VERSION, RECLAIM_WIRE_VERSION};
        let (mut state, _, node) = bootstrapped();
        let devices: Vec<DeviceId> = (0..3).map(|_| DeviceId::mint()).collect();
        let facts: Vec<DeviceFacts> = devices
            .iter()
            .map(|id| DeviceFacts {
                id: *id,
                class: "ssd".to_string(),
                slices: vec![SliceId::mint()],
                size_bytes: 1 << 40,
                weight: None,
                write_back: false,
                rotational: false,
                journal: None,
            })
            .collect();
        // the member speaking wire 14, which is activated after a drain is refused below it
        let mut upgraded = member(node, "a");
        upgraded.wire_min = MIN_PEER_VERSION;
        upgraded.wire_max = RECLAIM_WIRE_VERSION;
        upgraded.incarnation = 2;
        assert!(matches!(state.apply(&ControlCommand::ObserveMember(upgraded)), ControlResponse::Applied { .. }));
        let activate = |state: &mut ControlState, wire: u8| {
            let version = state.topology_version;
            let activated = state.apply(&ControlCommand::Activate {
                op: Uuid::new_v4(),
                principal: "admin".to_string(),
                expected_version: version,
                wire,
                members: [(node, wire)].into_iter().collect(),
            });
            assert!(matches!(activated, ControlResponse::Applied { .. }), "{activated:?}");
        };
        activate(&mut state, RECLAIM_WIRE_VERSION - 1);
        assert!(matches!(
            state.apply(&ControlCommand::ObserveDevices { node, incarnation: 2, devices: facts.clone() }),
            ControlResponse::Applied { .. }
        ));
        let drain = |device: DeviceId| ControlCommand::DrainDevice {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: 0,
            device,
            reason: "drained by admin".to_string(),
        };
        let remove = |state: &ControlState, device: DeviceId, lost: bool| ControlCommand::RemoveDevice {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: state.topology_version,
            device,
            lost,
        };
        let refused = |response: ControlResponse, kind: RefusalKind, says: &str| match response {
            ControlResponse::Refused { kind: got, reason } => {
                assert_eq!(got, kind, "{reason}");
                assert!(reason.contains(says), "{reason}");
            }
            other => panic!("expected a refusal saying {says}: {other:?}"),
        };
        refused(state.apply(&drain(devices[0])), RefusalKind::WireVersion, &RECLAIM_WIRE_VERSION.to_string());
        activate(&mut state, RECLAIM_WIRE_VERSION);
        // a placed device is not removed, named by its verb
        refused(state.apply(&remove(&state, devices[0], false)), RefusalKind::WrongPhase, "drain-device");
        // drained once: the generation moves, and a drain again changes nothing
        let generation = state.pool_map.generation;
        assert!(matches!(state.apply(&drain(devices[0])), ControlResponse::Applied { .. }));
        assert_eq!(state.pool_map.generation, generation + 1);
        assert!(matches!(state.apply(&drain(devices[0])), ControlResponse::Applied { .. }));
        assert_eq!(state.pool_map.generation, generation + 1);
        // a move arriving on it holds its removal
        let slot = MoveSlot {
            pool: "fast".to_string(),
            consumer: 1,
            pg: PlacementGroup::default(),
            generation: state.pool_map.generation,
            devices: vec![devices[0]],
        };
        assert!(matches!(state.apply(&ControlCommand::AdmitMove { slot: slot.clone() }), ControlResponse::Applied { .. }));
        refused(state.apply(&remove(&state, devices[0], false)), RefusalKind::WrongPhase, "moves arriving");
        assert!(matches!(
            state.apply(&ControlCommand::MoveDone { pool: slot.pool, consumer: slot.consumer, pg: slot.pg }),
            ControlResponse::Applied { .. }
        ));
        // and so does what it holds, until its node reports it empty
        refused(state.apply(&remove(&state, devices[0], false)), RefusalKind::WrongPhase, "still holds chunks");
        let report = ControlCommand::ReportDevices {
            node,
            incarnation: 2,
            seq: 1,
            failed: std::collections::BTreeMap::new(),
            empty: [devices[0], devices[1]].into_iter().collect(),
        };
        assert!(matches!(state.apply(&report), ControlResponse::Applied { .. }));
        assert!(state.pool_map.devices[&devices[0]].empty);
        assert!(!state.pool_map.devices[&devices[1]].empty, "a placed device was taken as empty");
        assert!(matches!(state.apply(&remove(&state, devices[0], false)), ControlResponse::Applied { .. }));
        // removed: every door refuses its id
        assert!(state.pool_map.devices[&devices[0]].tombstone.is_some());
        refused(state.apply(&drain(devices[0])), RefusalKind::Invalid, "removed");
        let generation = state.pool_map.generation;
        assert!(matches!(
            state.apply(&ControlCommand::ObserveDevices { node, incarnation: 2, devices: facts.clone() }),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.pool_map.generation, generation, "a removed device was observed back");
        // a placed device is removed on the operator's word that it is lost
        assert!(matches!(state.apply(&remove(&state, devices[1], true)), ControlResponse::Applied { .. }));
        assert!(state.pool_map.devices[&devices[1]].current().is_none());
        // a reweight begins an era at its weight, and a weight of zero is refused
        let reweight = |state: &ControlState, weight: u64| ControlCommand::ReweightDevice {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: state.topology_version,
            device: devices[2],
            weight,
        };
        refused(state.apply(&reweight(&state, 0)), RefusalKind::Invalid, "drain it");
        assert!(matches!(state.apply(&reweight(&state, 42)), ControlResponse::Applied { .. }));
        assert_eq!(state.pool_map.devices[&devices[2]].current().expect("placed").weight, 42);
    }

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
            physical: 0,
            incarnation: 1,
            weight: 0,
            lead_weight: 0,
            failure_domains: std::collections::BTreeMap::new(),
            wire_min: 0,
            wire_max: 0,
            capabilities: 0,
            schema_id: 0,
            schema_lineage: Vec::new(),
            build: String::new(),
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

    /// A node refuses to start on a log initialized with no committed schema, by name, and one
    /// never initialized starts (item 270)
    #[test]
    fn a_start_on_a_log_with_no_committed_schema_is_refused() {
        let (mut state, _, node) = bootstrapped();
        // a build that speaks every wire version, so only its schema is judged
        let record = MemberRecord {
            wire_max: u8::MAX,
            ..member(node, "a")
        };
        // initialized with no schema committed, as a log from before wire 15 is
        state.initialized = Some(vec![node]);
        state.schema = None;
        let refused = state.judge_start(node, &record);
        assert!(
            matches!(refused, Err(crate::server::errors::ShoalError::SchemaNotCommitted { node: named }) if named == node),
            "{refused:?}"
        );
        // a cluster bootstrapped and not initialized yet has no schema to commit
        state.initialized = None;
        assert!(state.judge_start(node, &record).is_ok());
    }

    /// An archive dealt to a member that leaves for good is dealt again to one that is, a
    /// generation up and from what it put, and its old driver's word changes nothing (item 263)
    #[test]
    fn an_archive_dealt_to_a_member_that_left_is_dealt_again() {
        use crate::server::control::backup::{ArchiveFile, ArchivePhase, ArchiveRestore, RestoreRecord};
        let (mut state, _, a) = bootstrapped();
        // a second member, which the archive is dealt to
        let b = NodeId::mint();
        let mut second = state.members[&a].clone();
        second.record = member(b, "b");
        state.members.insert(b, second);
        let restore = Uuid::new_v4();
        let dealt = ArchiveRestore {
            file: ArchiveFile {
                name: "Reels/9.objects".to_string(),
                bucket: "Reels".to_string(),
                bucket_id: crate::shared::identity::BucketId::of("Reels"),
                rows_layout: 0,
                format: 1,
                records: 64,
                bytes: 640,
            },
            node: b,
            phase: ArchivePhase::Putting,
            put: 32,
            bytes: 320,
            failed: None,
            generation: 0,
        };
        state.restores.insert(
            restore,
            RestoreRecord {
                op: restore,
                path: "/backups/x".to_string(),
                source: ClusterId::mint(),
                principal: "admin".to_string(),
                requested_at: state.topology_version,
                files: Vec::new(),
                groups: std::collections::BTreeMap::new(),
                archives: [("Reels/9.objects".to_string(), dealt.clone())].into_iter().collect(),
            },
        );
        // the member it was dealt to leaves for good
        state.members.get_mut(&b).expect("b").phase = super::MemberPhase::Removing;
        let tombstone = ControlCommand::Tombstone { node: b, op: None };
        assert!(matches!(state.apply(&tombstone), ControlResponse::Applied { .. }));
        // dealt again to the member that is, a generation up, from what it put
        let again = state.restores[&restore].archives["Reels/9.objects"].clone();
        assert_eq!((again.node, again.generation, again.put, again.phase), (a, 1, 32, ArchivePhase::Pending));
        // and the old deal's driver is too late to say anything
        let late = ControlCommand::ArchiveProgress {
            op: restore,
            archive: "Reels/9.objects".to_string(),
            node: b,
            incarnation: 1,
            progress: ArchiveRestore {
                phase: ArchivePhase::Done,
                put: 64,
                ..dealt
            },
        };
        let _ = state.apply(&late);
        assert_eq!(state.restores[&restore].archives["Reels/9.objects"], again);
    }

    /// A retry puts a finished restore's failed groups back, once, and fences their old drivers
    ///
    /// Refused below wire version 6, for an unknown operation, while any group runs and when
    /// nothing failed; a failed group goes back to the phase it failed in under the next
    /// generation, a restored one is left as it is, and a progress from the earlier try is
    /// applied as nothing ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md)).
    #[test]
    fn a_restore_retry_drives_only_its_failed_groups() {
        use crate::server::control::backup::{
            GroupRestore, RestoreOutcome, RestorePhase, RestoreRecord,
        };
        let (mut state, cluster, node) = bootstrapped();
        // a finished restore with one group restored and one failed at its install
        let restore = Uuid::new_v4();
        let (restored, failed) = (GroupId(1), GroupId(2));
        let done = |outcome: RestoreOutcome, failed_in: Option<RestorePhase>| GroupRestore {
            phase: RestorePhase::Done,
            driver: Some(node),
            outcome: Some(outcome),
            failed_in,
            ..GroupRestore::default()
        };
        let restored_progress = done(
            RestoreOutcome::Restored {
                boundary: 5,
                records: 3,
                bytes: 100,
                retries: 0,
                verified: 7,
            },
            None,
        );
        state.restores.insert(
            restore,
            RestoreRecord {
                op: restore,
                path: "/backups/x".to_string(),
                source: ClusterId::mint(),
                principal: "admin".to_string(),
                requested_at: state.topology_version,
                files: Vec::new(),
                archives: std::collections::BTreeMap::new(),
                groups: [
                    (restored, restored_progress.clone()),
                    (
                        failed,
                        done(
                            RestoreOutcome::Failed {
                                reason: "the file could not be read".to_string(),
                            },
                            Some(RestorePhase::Installing),
                        ),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        );
        let _ = cluster;
        let retry = |state: &ControlState, restore: Uuid| ControlCommand::RetryRestore {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: state.topology_version,
            restore,
        };
        let kind_of = |response: ControlResponse| match response {
            ControlResponse::Refused { kind, .. } => Some(kind),
            _ => None,
        };
        // below 6, refused by name
        let command = retry(&state, restore);
        assert_eq!(
            kind_of(state.apply(&command)),
            Some(RefusalKind::WireVersion)
        );
        state.activated = 6;
        // an unknown operation
        let command = retry(&state, Uuid::new_v4());
        assert_eq!(
            kind_of(state.apply(&command)),
            Some(RefusalKind::UnknownOperation)
        );
        // the retry: the failed group from its install, the restored one untouched
        let command = retry(&state, restore);
        assert!(matches!(
            state.apply(&command),
            ControlResponse::Applied { .. }
        ));
        let record = &state.restores[&restore];
        assert_eq!(record.groups[&restored], restored_progress);
        let again = &record.groups[&failed];
        assert_eq!(again.phase, RestorePhase::Installing);
        assert_eq!(
            (again.generation, again.outcome.clone(), again.driver),
            (1, None, None)
        );
        // a second while it runs is refused
        let command = retry(&state, restore);
        assert_eq!(kind_of(state.apply(&command)), Some(RefusalKind::Invalid));
        // a late word from the first try's driver changes nothing
        let progress = |generation: u32| ControlCommand::RestoreProgress {
            op: restore,
            group: failed,
            node,
            incarnation: 1,
            progress: GroupRestore {
                generation,
                ..restored_progress.clone()
            },
        };
        let before = state.restores[&restore].groups[&failed].clone();
        let _ = state.apply(&progress(0));
        assert_eq!(state.restores[&restore].groups[&failed], before);
        // this try's driver finishes it, after which there is nothing to retry
        let _ = state.apply(&progress(1));
        assert!(state.restores[&restore].groups[&failed].is_done());
        let command = retry(&state, restore);
        let refused = state.apply(&command);
        assert!(
            matches!(&refused, ControlResponse::Refused { reason, .. } if reason.contains("nothing to retry")),
            "{refused:?}"
        );
    }

    /// A group's restore progress that names no installed file keeps the one its record holds, and
    /// moves the version only for what else it changes; a retry, which installs again, clears it
    /// (item 327)
    ///
    /// A drive resumed past the install never knew the file, and a replica built before the field
    /// drops it, so a record changes, and the version moves, the same way on both.
    #[test]
    fn a_restores_installed_file_is_kept_until_its_group_installs_again() {
        use crate::server::control::backup::{
            GroupRestore, InstalledFile, RestoreOutcome, RestorePhase, RestoreRecord,
        };
        let (mut state, _, node) = bootstrapped();
        // a restore of one group, pending
        let restore = Uuid::new_v4();
        let group = GroupId(1);
        state.restores.insert(
            restore,
            RestoreRecord {
                op: restore,
                path: "/backups/x".to_string(),
                source: ClusterId::mint(),
                principal: "admin".to_string(),
                requested_at: state.topology_version,
                files: Vec::new(),
                archives: std::collections::BTreeMap::new(),
                groups: [(group, GroupRestore::default())].into_iter().collect(),
            },
        );
        let progress = |progress: GroupRestore| ControlCommand::RestoreProgress {
            op: restore,
            group,
            node,
            incarnation: 1,
            progress,
        };
        let file = InstalledFile {
            boundary: 9,
            records: 12,
            bytes: 1_216,
            retries: 12,
        };
        // the installing driver names the file with `Verifying`
        let verifying = GroupRestore {
            phase: RestorePhase::Verifying,
            driver: Some(node),
            installed: Some(file),
            ..GroupRestore::default()
        };
        assert!(matches!(
            state.apply(&progress(verifying.clone())),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.restores[&restore].groups[&group].installed, Some(file));
        // the same word from a driver that never knew the file changes nothing, the version with it
        let version = state.topology_version;
        let unknowing = GroupRestore {
            installed: None,
            ..verifying.clone()
        };
        let _ = state.apply(&progress(unknowing.clone()));
        assert_eq!(state.topology_version, version, "a word naming no file moved the version");
        assert_eq!(state.restores[&restore].groups[&group].installed, Some(file));
        // its hand-back changes the driver and keeps the file
        let _ = state.apply(&progress(GroupRestore {
            driver: None,
            ..unknowing.clone()
        }));
        assert_eq!(state.topology_version, version + 1);
        assert_eq!(state.restores[&restore].groups[&group].installed, Some(file));
        // failed in its verifying scrub and retried, it installs again, and its file is cleared
        let _ = state.apply(&progress(GroupRestore {
            phase: RestorePhase::Done,
            outcome: Some(RestoreOutcome::Failed {
                reason: "the copies disagree".to_string(),
            }),
            failed_in: Some(RestorePhase::Verifying),
            ..unknowing
        }));
        state.activated = 6;
        let retry = ControlCommand::RetryRestore {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: state.topology_version,
            restore,
        };
        assert!(matches!(state.apply(&retry), ControlResponse::Applied { .. }));
        let again = &state.restores[&restore].groups[&group];
        assert_eq!(
            (again.phase.clone(), again.installed),
            (RestorePhase::Installing, None)
        );
    }

    /// A restore's drives are fenced once wire 17 is activated, and not before (item 336)
    ///
    /// A drive under an older lead than the record's is refused, a drive behind the record never
    /// moves its phase back and is answered with the record, which takes its lead, and a word of a
    /// try retried since is refused. Below the fence every word is taken as a replica built before
    /// it takes it, the term dropped.
    #[test]
    fn a_restores_drives_are_fenced_once_its_wire_is_activated() {
        use crate::server::control::backup::{
            GroupRestore, InstalledFile, RestoreOutcome, RestorePhase, RestoreRecord,
        };
        use crate::shared::protocol::RESTORE_FENCE_WIRE_VERSION;
        let (mut state, _, node) = bootstrapped();
        // a restore of two groups, pending
        let restore = Uuid::new_v4();
        let (group, other) = (GroupId(1), GroupId(2));
        state.restores.insert(
            restore,
            RestoreRecord {
                op: restore,
                path: "/backups/x".to_string(),
                source: ClusterId::mint(),
                principal: "admin".to_string(),
                requested_at: state.topology_version,
                files: Vec::new(),
                archives: std::collections::BTreeMap::new(),
                groups: [(group, GroupRestore::default()), (other, GroupRestore::default())]
                    .into_iter()
                    .collect(),
            },
        );
        let progress = |group: GroupId, progress: GroupRestore| ControlCommand::RestoreProgress {
            op: restore,
            group,
            node,
            incarnation: 1,
            progress,
        };
        // a drive's word at a phase, under a term of its group's lead
        let at = |phase: RestorePhase, term: u64| GroupRestore {
            phase,
            driver: Some(node),
            term,
            ..GroupRestore::default()
        };
        let record = |state: &ControlState, group: GroupId| state.restores[&restore].groups[&group].clone();
        // below the fence a drive behind the record moves it back, and no term is kept
        state.activated = RESTORE_FENCE_WIRE_VERSION - 1;
        let _ = state.apply(&progress(group, at(RestorePhase::Installing, 5)));
        assert!(matches!(
            state.apply(&progress(group, at(RestorePhase::Loading, 3))),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(
            (record(&state, group).phase, record(&state, group).term),
            (RestorePhase::Loading, 0)
        );
        // at the fence the first drive, under term 2, takes the group into its install
        state.activated = RESTORE_FENCE_WIRE_VERSION;
        for phase in [RestorePhase::Loading, RestorePhase::Installing] {
            assert!(matches!(
                state.apply(&progress(group, at(phase, 2))),
                ControlResponse::Applied { .. }
            ));
        }
        assert_eq!(record(&state, group).term, 2);
        // a drive under a newer lead that started behind it is answered with the record, whose
        // lead it takes
        let version = state.topology_version;
        match state.apply(&progress(group, at(RestorePhase::Loading, 4))) {
            ControlResponse::RestoreAhead {
                topology_version,
                record: standing,
            } => {
                assert_eq!((standing.phase, standing.term), (RestorePhase::Installing, 4));
                assert_eq!(topology_version, version + 1);
            }
            other => panic!("a drive behind the record was answered {other:?}"),
        }
        assert_eq!(record(&state, group).phase, RestorePhase::Installing);
        // the first drive's late failure is refused, and changes nothing
        let late = GroupRestore {
            outcome: Some(RestoreOutcome::Failed {
                reason: "a member's checkpoint is past the restore boundary".to_string(),
            }),
            failed_in: Some(RestorePhase::Installing),
            ..at(RestorePhase::Done, 2)
        };
        let before = record(&state, group);
        assert!(matches!(
            state.apply(&progress(group, late)),
            ControlResponse::Refused {
                kind: RefusalKind::Superseded,
                ..
            }
        ));
        assert_eq!(record(&state, group), before);
        // the newer drive restores it
        let file = InstalledFile {
            boundary: 9,
            records: 12,
            bytes: 1_216,
            retries: 12,
        };
        let restored = GroupRestore {
            outcome: Some(RestoreOutcome::Restored {
                boundary: 9,
                records: 12,
                bytes: 1_216,
                retries: 12,
                verified: 11,
            }),
            installed: Some(file),
            ..at(RestorePhase::Done, 4)
        };
        assert!(matches!(
            state.apply(&progress(group, restored)),
            ControlResponse::Applied { .. }
        ));
        // and a drive that never learned it is done is told so, rather than installing over it
        match state.apply(&progress(group, at(RestorePhase::Installing, 6))) {
            ControlResponse::RestoreAhead {
                record: standing, ..
            } => assert!(standing.is_done()),
            other => panic!("a drive behind a done group was answered {other:?}"),
        }
        // the other group fails and is retried, and a word of its first try is refused
        let _ = state.apply(&progress(
            other,
            GroupRestore {
                outcome: Some(RestoreOutcome::Failed {
                    reason: "the copies disagree".to_string(),
                }),
                failed_in: Some(RestorePhase::Verifying),
                ..at(RestorePhase::Done, 1)
            },
        ));
        let retry = ControlCommand::RetryRestore {
            op: Uuid::new_v4(),
            principal: "admin".to_string(),
            expected_version: state.topology_version,
            restore,
        };
        assert!(matches!(state.apply(&retry), ControlResponse::Applied { .. }));
        assert_eq!(
            (record(&state, other).generation, record(&state, other).term),
            (1, 0)
        );
        assert!(matches!(
            state.apply(&progress(other, at(RestorePhase::Verifying, 7))),
            ControlResponse::Refused {
                kind: RefusalKind::Superseded,
                ..
            }
        ));
    }

    /// A record with its host named
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `client` - Its client address, which also names its control address
    /// * `host` - The host it stands on
    fn hosted(node: NodeId, client: &str, host: &str) -> MemberRecord {
        let mut record = member(node, client);
        record.failure_domains =
            std::collections::BTreeMap::from([(HOST.to_string(), host.to_string())]);
        record
    }

    /// A learner on a host no voter stands on is promoted before earlier ones on a voter's host,
    /// the first learner is promoted when every one shares a voter's host or none names one, a
    /// node outside the membership never is, and a host changed at a restart is committed
    /// ([F81](../../../../docs/src/features/failure-domains.md))
    #[test]
    fn promotion_spreads_the_voters_over_hosts() {
        let (mut state, _, voter) = bootstrapped();
        // the bootstrapper votes, on h1
        state.apply(&ControlCommand::ObserveMember(hosted(voter, "a", "h1")));
        assert_eq!(state.members[&voter].record.host(), Some("h1"));
        // three learners in node order, the last alone on h2
        let mut learners: Vec<NodeId> = (0..3).map(|_| NodeId::mint()).collect();
        learners.sort();
        let (early, middle, late) = (learners[0], learners[1], learners[2]);
        for (node, client, host) in [(early, "e", "h1"), (middle, "m", "h1"), (late, "l", "h2")] {
            let record = hosted(node, client, host);
            assert!(matches!(
                state.apply(&ControlCommand::Admit(record.clone())),
                ControlResponse::Applied { .. }
            ));
            assert!(matches!(
                state.apply(&ControlCommand::ObserveMember(record)),
                ControlResponse::Applied { .. }
            ));
        }
        let promoted =
            |state: &ControlState, voters: &[NodeId], member: &dyn Fn(NodeId) -> bool| {
                let voters = voters.iter().copied().collect();
                state
                    .promotion_candidate(&voters, member)
                    .map(|(node, _)| node)
            };
        let everyone = |_: NodeId| true;
        // the learner on a host of its own goes before the two on the voter's
        assert_eq!(promoted(&state, &[voter], &everyone), Some(late));
        // once it votes, every learner left shares a voter's host and the first goes
        assert_eq!(promoted(&state, &[voter, late], &everyone), Some(early));
        // a node the membership does not hold is never promoted
        assert_eq!(
            promoted(&state, &[voter], &|node: NodeId| node != late),
            Some(early)
        );
        // the middle learner restarts on h3, and the host it names is committed with it
        let mut moved = hosted(middle, "m", "h3");
        moved.incarnation = 2;
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(moved)),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.members[&middle].record.host(), Some("h3"));
        assert_eq!(promoted(&state, &[voter, late], &everyone), Some(middle));
        // with no host named anywhere, the first learner goes, as before hosts were recorded
        for member in state.members.values_mut() {
            member.record.failure_domains.clear();
        }
        assert_eq!(promoted(&state, &[voter], &everyone), Some(early));
    }

    /// A record standing in the domains given, widest level last as a configuration writes them
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `client` - Its client address, which also names its control address
    /// * `domains` - Its domains, by level
    fn placed(node: NodeId, client: &str, domains: &[(&str, &str)]) -> MemberRecord {
        let mut record = member(node, client);
        record.failure_domains = domains
            .iter()
            .map(|(level, name)| ((*level).to_string(), (*name).to_string()))
            .collect();
        record
    }

    /// A learner in a zone no voter stands in is promoted before one in a new rack, and one in a
    /// new rack before one on a new host; a level left out is the one below it, so a cluster
    /// naming no rack or zone promotes by host as F81 did
    /// ([F83](../../../../docs/src/features/rack-and-zone-domains.md))
    #[test]
    fn promotion_spreads_the_voters_over_racks_and_zones() {
        use crate::shared::domains::{RACK, ZONE};
        let (mut state, _, voter) = bootstrapped();
        // the bootstrapper votes, on h1 in rack r1 of zone z1
        state.apply(&ControlCommand::ObserveMember(placed(
            voter,
            "a",
            &[(HOST, "h1"), (RACK, "r1"), (ZONE, "z1")],
        )));
        // four learners in node order: a new host in the voter's rack, a new rack in its zone, a
        // new zone, and a second new zone
        let mut learners: Vec<NodeId> = (0..4).map(|_| NodeId::mint()).collect();
        learners.sort();
        let (host, rack, zone, later) = (learners[0], learners[1], learners[2], learners[3]);
        let layout = [
            (host, "h", [(HOST, "h2"), (RACK, "r1"), (ZONE, "z1")]),
            (rack, "r", [(HOST, "h3"), (RACK, "r2"), (ZONE, "z1")]),
            (zone, "z", [(HOST, "h4"), (RACK, "r3"), (ZONE, "z2")]),
            (later, "l", [(HOST, "h5"), (RACK, "r4"), (ZONE, "z3")]),
        ];
        for (node, client, domains) in layout {
            let record = placed(node, client, &domains);
            assert!(matches!(
                state.apply(&ControlCommand::Admit(record.clone())),
                ControlResponse::Applied { .. }
            ));
            assert!(matches!(
                state.apply(&ControlCommand::ObserveMember(record)),
                ControlResponse::Applied { .. }
            ));
        }
        let promoted = |state: &ControlState, voters: &[NodeId]| {
            let voters = voters.iter().copied().collect();
            state
                .promotion_candidate(&voters, |_| true)
                .map(|(node, _)| node)
        };
        // the first learner in a new zone, though two before it stand on new hosts
        assert_eq!(promoted(&state, &[voter]), Some(zone));
        // with both new zones voting, a new rack goes before a new host in a held rack
        assert_eq!(promoted(&state, &[voter, zone, later]), Some(rack));
        // with every rack held, the new host goes
        assert_eq!(promoted(&state, &[voter, zone, later, rack]), Some(host));
        // with no zone named, the racks decide: the first new rack in node order
        for member in state.members.values_mut() {
            member.record.failure_domains.remove(ZONE);
        }
        assert_eq!(promoted(&state, &[voter]), Some(rack));
        // with no rack named either, the hosts decide as F81's did: every learner is on a host
        // of its own, and the first goes
        for member in state.members.values_mut() {
            member.record.failure_domains.remove(RACK);
        }
        assert_eq!(promoted(&state, &[voter]), Some(host));
        // and a learner sharing the voter's host goes after every learner on a host of its own
        state
            .members
            .get_mut(&host)
            .unwrap()
            .record
            .failure_domains
            .insert(HOST.to_string(), "h1".to_string());
        assert_eq!(promoted(&state, &[voter]), Some(rack));
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
        assert!(matches!(
            state.apply(&again),
            ControlResponse::Refused { .. }
        ));
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
            buckets: Vec::new(),
            schema: Default::default(),
        };
        // a stale version is refused, and the refusal is not remembered: the same op sent again
        // against the current version is a fresh attempt, refused here for another reason
        let stale = state.apply(&initialize(op, 0, vec![node]));
        assert!(
            matches!(&stale, ControlResponse::Refused { reason, .. } if reason.contains("stale"))
        );
        assert!(matches!(
            state.apply(&initialize(op, 2, vec![node, joiner])),
            ControlResponse::Refused { reason, .. } if reason.contains("joining")
        ));
        assert!(state.initialized.is_none());
        // a joining member cannot be placed on
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 2, vec![node, joiner])),
            ControlResponse::Refused { reason, .. } if reason.contains("joining")
        ));
        state.apply(&ControlCommand::ObserveMember(member(joiner, "b")));
        // a duplicate and a stranger are refused too
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 3, vec![node, node])),
            ControlResponse::Refused { reason, .. } if reason.contains("twice")
        ));
        assert!(matches!(
            state.apply(&initialize(Uuid::new_v4(), 3, vec![NodeId::mint()])),
            ControlResponse::Refused { reason, .. } if reason.contains("not a member")
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
            ControlResponse::Refused { reason, .. } if reason.contains("Move")
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
            ControlResponse::Refused { reason, .. } if reason.contains("not one the placement")
        ));
        assert_eq!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "alice".to_string(),
                expected_version: 1,
                nodes: vec![node],
                tables: tables.clone(),
                buckets: Vec::new(),
                schema: Default::default(),
            }),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        // `All` is not a read level
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 2, TableId::of("Note"), Some(Consistency::All))),
            ControlResponse::Refused { reason, .. } if reason.contains("C6")
        ));
        // a stale version is refused before anything is recorded
        assert!(matches!(
            state.apply(&set(Uuid::new_v4(), 1, TableId::of("Note"), Some(Consistency::Quorum))),
            ControlResponse::Refused { reason, .. } if reason.contains("stale")
        ));
        // setting records, and moves the version once
        assert_eq!(
            state.apply(&set(
                Uuid::new_v4(),
                2,
                TableId::of("Note"),
                Some(Consistency::Quorum)
            )),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert_eq!(
            state.table_read_policy.get(&TableId::of("Note")),
            Some(&Consistency::Quorum)
        );
        assert_eq!(state.table_read_policy.get(&TableId::of("Row")), None);
        // the same level again applies without moving the version
        assert_eq!(
            state.apply(&set(
                Uuid::new_v4(),
                3,
                TableId::of("Note"),
                Some(Consistency::Quorum)
            )),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        // the map resolves each table on its own: Note at its policy, Row at the cluster's
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.read_level_of(TableId::of("Note")), Consistency::Quorum);
        assert_eq!(map.read_level_of(TableId::of("Row")), Consistency::One);
        assert_eq!(
            map.frame().table_read_policy,
            vec![("Note".to_string(), "quorum".to_string())]
        );
        // clearing removes it and moves the version once; clearing again moves nothing
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 3, TableId::of("Note"), None)),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        assert!(state.table_read_policy.is_empty());
        assert_eq!(
            state.apply(&set(Uuid::new_v4(), 4, TableId::of("Note"), None)),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        // and a repeated op is answered as it was the first time
        let op = Uuid::new_v4();
        assert_eq!(
            state.apply(&set(op, 4, TableId::of("Row"), Some(Consistency::One))),
            ControlResponse::Applied {
                topology_version: 5
            }
        );
        assert!(matches!(
            state.apply(&set(op, 5, TableId::of("Row"), Some(Consistency::Quorum))),
            ControlResponse::Repeated { .. }
        ));
        assert_eq!(
            state.table_read_policy.get(&TableId::of("Row")),
            Some(&Consistency::One)
        );
    }

    /// A bootstrap seeds the grants it was given; a grant and a revoke are versioned and
    /// idempotent by operation, move the version only when they change something, refuse nobody,
    /// nothing and a table the placement was not initialized with, and reach the map (F77)
    #[test]
    fn grants_are_seeded_then_changed_by_committed_operations() {
        use crate::shared::grants::{Grants, Privileges, EVERY_TABLE};
        // a cluster bootstrapped with one grant in its policy
        let mut seeded = Grants::default();
        seeded.grant("reader", "Note", Privileges::READ);
        let mut state = ControlState::default();
        let node = NodeId::mint();
        assert_eq!(
            state.apply(&ControlCommand::Bootstrap {
                cluster: ClusterId::mint(),
                policy: Cluster::default().policy().with_grants(seeded.clone()),
                member: member(node, "a"),
            }),
            ControlResponse::Applied {
                topology_version: 1
            }
        );
        assert_eq!(state.policy.as_ref().expect("a policy").grants, seeded);
        let grant = |op, expected_version, grantee: &str, table: &str, privileges| {
            ControlCommand::Grant {
                op,
                principal: "alice".to_string(),
                expected_version,
                grantee: grantee.to_string(),
                on: GrantOn::Table,
                name: table.to_string(),
                privileges,
            }
        };
        let revoke = |op, expected_version, grantee: &str, table: &str, privileges| {
            ControlCommand::Revoke {
                op,
                principal: "alice".to_string(),
                expected_version,
                grantee: grantee.to_string(),
                on: GrantOn::Table,
                name: table.to_string(),
                privileges,
            }
        };
        // before an initialization recorded the tables, the proposer's check is the one that
        // counts, so any name is taken; a stale version, nobody and nothing are refused
        assert!(matches!(
            state.apply(&grant(Uuid::new_v4(), 0, "writer", "Note", Privileges::WRITE)),
            ControlResponse::Refused { kind: RefusalKind::StaleVersion, .. }
        ));
        assert!(matches!(
            state.apply(&grant(Uuid::new_v4(), 1, " ", "Note", Privileges::WRITE)),
            ControlResponse::Refused { kind: RefusalKind::Invalid, .. }
        ));
        assert!(matches!(
            state.apply(&grant(Uuid::new_v4(), 1, "writer", "Note", Privileges::NONE)),
            ControlResponse::Refused { kind: RefusalKind::Invalid, .. }
        ));
        // a grant records and moves the version once
        let op = Uuid::new_v4();
        assert_eq!(
            state.apply(&grant(op, 1, "writer", "Note", Privileges::WRITE)),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        // the same operation again is answered as the first time and changes nothing
        assert!(matches!(
            state.apply(&grant(op, 2, "writer", "Note", Privileges::ALL)),
            ControlResponse::Repeated { .. }
        ));
        let grants = &state.policy.as_ref().expect("a policy").grants;
        assert_eq!(grants.on_table("writer", "Note"), Privileges::WRITE);
        // a fresh grant of what is held applies without moving the version
        assert_eq!(
            state.apply(&grant(Uuid::new_v4(), 2, "writer", "Note", Privileges::WRITE)),
            ControlResponse::Applied {
                topology_version: 2
            }
        );
        // once the placement records the tables, a table it does not name is refused
        let tables = vec![
            ("Row".to_string(), TableId::of("Row")),
            ("Note".to_string(), TableId::of("Note")),
        ];
        assert_eq!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "alice".to_string(),
                expected_version: 2,
                nodes: vec![node],
                tables: tables.clone(),
                buckets: Vec::new(),
                schema: Default::default(),
            }),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert!(matches!(
            state.apply(&grant(Uuid::new_v4(), 3, "writer", "Nope", Privileges::READ)),
            ControlResponse::Refused { kind: RefusalKind::UnknownOperation, reason } if reason.contains("Nope")
        ));
        assert_eq!(
            state.apply(&grant(Uuid::new_v4(), 3, "auditor", EVERY_TABLE, Privileges::READ)),
            ControlResponse::Applied {
                topology_version: 4
            }
        );
        // a revoke takes back what it names, and a revoke of nothing held moves nothing
        assert_eq!(
            state.apply(&revoke(Uuid::new_v4(), 4, "reader", "Note", Privileges::READ)),
            ControlResponse::Applied {
                topology_version: 5
            }
        );
        assert_eq!(
            state.apply(&revoke(Uuid::new_v4(), 5, "reader", "Note", Privileges::READ)),
            ControlResponse::Applied {
                topology_version: 5
            }
        );
        // every coordinator's map carries what is committed, and a client's frame does not
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.grants.on_table("reader", "Note"), Privileges::NONE);
        assert_eq!(map.grants.on_table("writer", "Note"), Privileges::WRITE);
        assert_eq!(map.grants.on_table("auditor", "Row"), Privileges::READ);
        assert!(!serde_json::to_string(&map.frame())
            .expect("a frame")
            .contains("auditor"));
        // and the operations are audited by who asked
        assert!(state
            .operations
            .values()
            .any(|operation| operation.kind == "grant" && operation.principal == "alice"));
    }

    /// A bucket's grants are committed beside a table's, and a table a bucket generated is no
    /// table grant's to name (F79)
    ///
    /// The initialization records the buckets with the tables each generated, which is what the
    /// applied state checks a grant against: a table grant on a generated table is refused with
    /// the bucket to grant on instead, a bucket grant on a declared bucket or `*` is held and
    /// reaches every coordinator's map, and one on a bucket the schema does not declare is refused.
    #[test]
    fn bucket_grants_are_committed_and_generated_tables_refused() {
        use crate::shared::grants::{Privileges, EVERY_BUCKET};
        let (mut state, _, node) = bootstrapped();
        // a schema of one table and one bucket, whose two tables are tables to the placement
        let tables = vec![
            ("Movie".to_string(), TableId::of("Movie")),
            (
                "PostersObjectMeta".to_string(),
                TableId::of("PostersObjectMeta"),
            ),
            (
                "PostersStripeMeta".to_string(),
                TableId::of("PostersStripeMeta"),
            ),
        ];
        let buckets = BucketRecord::of_schema(
            &[("Posters", BucketId::of("Posters"))],
            &[
                ("PostersObjectMeta", "Posters"),
                ("PostersStripeMeta", "Posters"),
            ],
        );
        assert_eq!(buckets.len(), 1);
        assert_eq!(
            buckets[0].tables,
            ["PostersObjectMeta", "PostersStripeMeta"]
        );
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "alice".to_string(),
                expected_version: version,
                nodes: vec![node],
                tables: tables.clone(),
                buckets: buckets.clone(),
                schema: Default::default(),
            }),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.buckets, buckets);
        // a grant of a kind on a name, by a fresh operation at the current version
        let grant = |state: &ControlState, on, name: &str, privileges| ControlCommand::Grant {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: state.topology_version,
            grantee: "viewer".to_string(),
            on,
            name: name.to_string(),
            privileges,
        };
        // a table grant on a generated table is refused, naming the bucket it is reached through
        let refused = state.apply(&grant(
            &state,
            GrantOn::Table,
            "PostersStripeMeta",
            Privileges::WRITE,
        ));
        assert!(
            matches!(
                &refused,
                ControlResponse::Refused { kind: RefusalKind::UnknownOperation, reason }
                    if reason.contains("Posters") && reason.contains("PostersStripeMeta")
            ),
            "{refused:?}"
        );
        // a bucket grant on a bucket the schema does not declare is refused by name
        assert!(matches!(
            state.apply(&grant(&state, GrantOn::Bucket, "Stills", Privileges::READ)),
            ControlResponse::Refused { kind: RefusalKind::UnknownOperation, reason } if reason.contains("Stills")
        ));
        // and a bucket's name is not a table's
        assert!(matches!(
            state.apply(&grant(&state, GrantOn::Table, "Posters", Privileges::READ)),
            ControlResponse::Refused {
                kind: RefusalKind::UnknownOperation,
                ..
            }
        ));
        // a grant on the bucket, and on every bucket, are held and move the version
        let before = state.topology_version;
        assert!(matches!(
            state.apply(&grant(&state, GrantOn::Bucket, "Posters", Privileges::READ)),
            ControlResponse::Applied { .. }
        ));
        let grant_every = grant(&state, GrantOn::Bucket, EVERY_BUCKET, Privileges::WRITE);
        assert!(matches!(
            state.apply(&grant_every),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.topology_version, before + 2);
        // every coordinator's map carries them, beside no table grant at all
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.grants.on_bucket("viewer", "Posters"), Privileges::ALL);
        assert_eq!(
            map.grants.on_table("viewer", "PostersObjectMeta"),
            Privileges::NONE
        );
        // a revoke of the named grant leaves what `*` gives
        let revoke = ControlCommand::Revoke {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: state.topology_version,
            grantee: "viewer".to_string(),
            on: GrantOn::Bucket,
            name: "Posters".to_string(),
            privileges: Privileges::READ,
        };
        assert!(matches!(
            state.apply(&revoke),
            ControlResponse::Applied { .. }
        ));
        let grants = &state.policy.as_ref().expect("a policy").grants;
        assert_eq!(grants.on_bucket("viewer", "Posters"), Privileges::WRITE);
    }

    /// A set whose one group failed its move is never published, however its others went
    /// (item 171)
    ///
    /// Three placed at a factor of three and a fourth member: a set of two groups is moved from
    /// c to d. One group activates; the other fails catching up and is committed done with the
    /// failure. The configuration naming d is not published - the failed group's committed
    /// voters never took d, so routing the set to d would name a copy the group does not count -
    /// and the record ends `Failed` ([Resolved #171](../../../../docs/src/appendix/resolved/failed-group-publishes-its-set.md)).
    #[test]
    fn a_set_with_a_failed_group_is_not_published() {
        use crate::server::control::migrate::{GroupMove, MoveOutcome, MovePhase, MoveStats};
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![
            ("Row".to_string(), TableId::of("Row")),
            ("Note".to_string(), TableId::of("Note")),
        ];
        // three placed, and d a member the placement never named
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: tables.clone(),
            buckets: Vec::new(),
            schema: Default::default(),
        });
        // move tablet zero's set off c onto d
        let op = Uuid::new_v4();
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&ControlCommand::Move {
                op,
                principal: "alice".to_string(),
                expected_version: version,
                tablet: 0,
                from: c,
                to: d,
            }),
            ControlResponse::Applied { .. }
        ));
        let groups: Vec<_> = state.moves[&op].groups.keys().copied().collect();
        assert_eq!(groups.len(), 2, "one group per table");
        let report = |group, progress: GroupMove| ControlCommand::MoveProgress {
            op,
            group,
            node,
            incarnation: 1,
            progress,
        };
        // the first group activates, and publishes nothing on its own
        state.apply(&report(
            groups[0],
            GroupMove {
                phase: MovePhase::Activated,
                driver: Some(node),
                config: Some(40),
                stats: MoveStats::default(),
                outcome: None,
            },
        ));
        assert!(state.configurations.is_empty());
        // the second fails catching up, and is committed done with the failure
        state.apply(&report(
            groups[1],
            GroupMove {
                phase: MovePhase::Done,
                driver: Some(node),
                config: None,
                stats: MoveStats::default(),
                outcome: Some(MoveOutcome::Failed {
                    reason: "did not catch up within 600s".to_string(),
                }),
            },
        ));
        // the set is still served where it was: nothing published, d holds nothing
        assert!(
            state.configurations.is_empty(),
            "a set with a failed group was published: {:?}",
            state.configurations
        );
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert!(map.holds(c, 0), "the source was routed away from");
        assert!(!map.holds(d, 0), "the destination was routed to");
        // and the record ends failed at once: the activated group would otherwise wait for a
        // publish that cannot come, and a plan would wait on the record for ever
        assert_eq!(state.moves[&op].phase, MovePhase::Done);
        assert!(matches!(
            state.moves[&op].outcome,
            Some(MoveOutcome::Failed { .. })
        ));
        // so the map no longer carries it, and the set is free for the move to be asked again
        assert!(map.moves.is_empty(), "{:?}", map.moves);
    }

    /// A move is recorded against the set as the map serves it, queued behind a transition on
    /// the same set, published by the last group activated and finished by the last group done,
    /// which releases the queue; every refusal is by name
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[test]
    fn a_move_is_recorded_queued_published_and_released() {
        use crate::server::control::migrate::{GroupMove, MoveOutcome, MovePhase, MoveStats};
        use crate::shared::identity::{GroupId, ShardAddr};
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![
            ("Row".to_string(), TableId::of("Row")),
            ("Note".to_string(), TableId::of("Note")),
        ];
        // three placed at a factor of three, and d a member the placement never named
        let version = state.topology_version;
        assert_eq!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "alice".to_string(),
                expected_version: version,
                nodes: vec![node, b, c],
                tables: tables.clone(),
                buckets: Vec::new(),
                schema: Default::default(),
            }),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        let moving = |op, expected_version, tablet, from, to| ControlCommand::Move {
            op,
            principal: "alice".to_string(),
            expected_version,
            tablet,
            from,
            to,
        };
        let version = state.topology_version;
        // refusals: a stranger, a source outside the set, a destination inside it, a stale version
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version, 0, c, NodeId::mint())),
            ControlResponse::Refused { reason, .. } if reason.contains("not a member")
        ));
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version, 0, d, b)),
            ControlResponse::Refused { reason, .. } if reason.contains("not a member of the set")
        ));
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version, 0, c, b)),
            ControlResponse::Refused { reason, .. } if reason.contains("already a member")
        ));
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version, 0, c, c)),
            ControlResponse::Refused { reason, .. } if reason.contains("itself")
        ));
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version - 1, 0, c, d)),
            ControlResponse::Refused { reason, .. } if reason.contains("stale")
        ));
        // a joining destination is refused too
        let e = NodeId::mint();
        state.apply(&ControlCommand::Admit(member(e, "e")));
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&moving(Uuid::new_v4(), version, 0, c, e)),
            ControlResponse::Refused { reason, .. } if reason.contains("joining")
        ));
        // the real one: recorded against the set the map serves tablet zero with
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        let expected = map.replicas_of(0);
        let (_, tablets) = map.rule_set_of(0);
        let slot = expected
            .iter()
            .position(|member| member.node == c)
            .expect("c is in the set");
        let op = Uuid::new_v4();
        assert_eq!(
            state.apply(&moving(op, version, 0, c, d)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        let record = state.moves.get(&op).expect("recorded").clone();
        assert_eq!(record.phase, MovePhase::Planned);
        assert_eq!(record.expected, expected);
        assert_eq!(record.tablets, tablets);
        assert_eq!(record.from, expected[slot]);
        assert_eq!(record.to, ShardAddr::new(d, 0));
        let mut target = expected.clone();
        target[slot] = record.to;
        assert_eq!(record.target, target);
        assert_eq!(record.groups.len(), 2, "one group per table");
        assert!(record
            .groups
            .values()
            .all(|progress| progress.phase == MovePhase::Planned));
        let groups: Vec<GroupId> = record.groups.keys().copied().collect();
        // the map carries it, and the destination learns from it
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.moves.len(), 1);
        assert_eq!(map.learner_of(0), Some(record.to));
        assert!(map.places(d));
        // a second move of the same set queues behind it; another set is planned
        let version = state.topology_version;
        let queued = Uuid::new_v4();
        assert_eq!(
            state.apply(&moving(queued, version, 0, b, d)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.moves[&queued].phase, MovePhase::Queued { behind: op });
        let other_tablet = (0..4096u16)
            .find(|tablet| !record.covers(*tablet))
            .expect("another set");
        let version = state.topology_version;
        let other = Uuid::new_v4();
        assert_eq!(
            state.apply(&moving(other, version, other_tablet, node, d)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.moves[&other].phase, MovePhase::Planned);
        // progress: an unknown op, a queued op and a stranger are refused; an old run is fenced
        let progress = |phase, config| GroupMove {
            phase,
            driver: Some(node),
            config,
            stats: MoveStats::default(),
            outcome: None,
        };
        let report = |op, group, incarnation, progress: GroupMove| ControlCommand::MoveProgress {
            op,
            group,
            node,
            incarnation,
            progress,
        };
        assert!(matches!(
            state.apply(&report(Uuid::new_v4(), groups[0], 1, progress(MovePhase::Learner, None))),
            ControlResponse::Refused { reason, .. } if reason.contains("no move operation")
        ));
        assert!(matches!(
            state.apply(&report(queued, groups[0], 1, progress(MovePhase::Learner, None))),
            ControlResponse::Refused { reason, .. } if reason.contains("queued")
        ));
        assert!(matches!(
            state.apply(&report(op, GroupId(1), 1, progress(MovePhase::Learner, None))),
            ControlResponse::Refused { reason, .. } if reason.contains("not part of")
        ));
        assert!(matches!(
            state.apply(&report(
                op,
                groups[0],
                0,
                progress(MovePhase::Learner, None)
            )),
            ControlResponse::Fenced { .. }
        ));
        // a phase moves the version once, and the same phase again moves nothing
        let version = state.topology_version;
        assert_eq!(
            state.apply(&report(
                op,
                groups[0],
                1,
                progress(MovePhase::Learner, None)
            )),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(
            state.apply(&report(
                op,
                groups[0],
                1,
                progress(MovePhase::Learner, None)
            )),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(
            state.moves[&op].groups[&groups[0]].phase,
            MovePhase::Learner
        );
        assert_eq!(state.moves[&op].phase, MovePhase::Planned);
        // one group activated publishes nothing; the last one publishes the configuration
        state.apply(&report(
            op,
            groups[0],
            1,
            progress(MovePhase::Activated, Some(40)),
        ));
        assert_eq!(state.moves[&op].phase, MovePhase::Planned);
        assert!(state.configurations.is_empty());
        let version = state.topology_version;
        assert_eq!(
            state.apply(&report(
                op,
                groups[1],
                1,
                progress(MovePhase::Activated, Some(41))
            )),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.moves[&op].phase, MovePhase::Published);
        let configuration = state.configurations.get(&tablets[0]).expect("published");
        assert_eq!(configuration.members, target);
        assert_eq!(configuration.tablets, tablets);
        assert_eq!(configuration.configs.get(&groups[0]), Some(&40));
        assert_eq!(configuration.configs.get(&groups[1]), Some(&41));
        assert_eq!(configuration.published_at, version + 1);
        // the map serves the set from the target now, and nobody learns it any more
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert_eq!(map.replicas_of(0), target);
        assert_eq!(map.learner_of(0), None);
        assert!(map.holds(d, 0));
        assert!(!map.holds(c, 0));
        // the last group done finishes the record, and the queued move is planned
        let done = |outcome| GroupMove {
            phase: MovePhase::Done,
            driver: Some(node),
            config: Some(40),
            stats: MoveStats::default(),
            outcome: Some(outcome),
        };
        state.apply(&report(op, groups[0], 1, done(MoveOutcome::Moved)));
        assert_eq!(state.moves[&op].phase, MovePhase::Published);
        assert_eq!(state.moves[&queued].phase, MovePhase::Queued { behind: op });
        state.apply(&report(op, groups[1], 1, done(MoveOutcome::Moved)));
        assert_eq!(state.moves[&op].phase, MovePhase::Done);
        assert_eq!(state.moves[&op].outcome, Some(MoveOutcome::Moved));
        assert_eq!(state.moves[&queued].phase, MovePhase::Planned);
        assert!(state.moves[&queued].groups.keys().eq(groups.iter()));
        // a done group stays done, whatever a late driver says
        let version = state.topology_version;
        assert_eq!(
            state.apply(&report(
                op,
                groups[0],
                1,
                progress(MovePhase::Learner, None)
            )),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        assert_eq!(state.moves[&op].groups[&groups[0]].phase, MovePhase::Done);
        // the released move's expected set is the one it was recorded against, which the
        // configuration has since replaced: its driver reconciles against the group, not here
        assert_eq!(state.moves[&queued].expected, expected);
        // the done record is no longer carried by the map, and the repeated op is remembered
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert!(map.moves.iter().all(|record| record.op != op));
        assert!(matches!(
            state.apply(&moving(op, state.topology_version, 0, c, d)),
            ControlResponse::Repeated { .. }
        ));
    }

    /// A scheduled verify scrub applies whatever the version, and nothing else does
    ///
    /// Under three leaders' scheduled scrubs every record and progress report moved the
    /// version, and a node whose map lagged proposed a stale one every time: its groups were
    /// never scrubbed ([#195](../../../../docs/src/appendix/resolved/scheduled-scrub-starved.md)).
    #[test]
    fn a_scheduled_verify_scrub_is_judged_against_no_version() {
        use crate::server::control::repair::RepairMode;
        use super::ANY_VERSION;
        let (mut state, _, node) = bootstrapped();
        let (b, c) = (NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: tables.clone(),
            buckets: Vec::new(),
            schema: Default::default(),
        });
        let scrub = |mode, expected_version| ControlCommand::Repair {
            op: Uuid::new_v4(),
            principal: "scheduler".to_string(),
            expected_version,
            table: TableId::of("Row"),
            tablet: Some(0),
            mode,
            source: None,
            release: false,
        };
        // an operator's scrub at a version the cluster has moved past is refused
        let stale = state.topology_version.saturating_sub(1);
        assert!(matches!(
            state.apply(&scrub(RepairMode::Verify, stale)),
            ControlResponse::Refused { kind: RefusalKind::StaleVersion, .. }
        ));
        // the scheduler's, at no version, applies however far the version has moved
        assert!(matches!(
            state.apply(&scrub(RepairMode::Verify, ANY_VERSION)),
            ControlResponse::Applied { .. }
        ));
        assert!(matches!(
            state.apply(&scrub(RepairMode::Verify, ANY_VERSION)),
            ControlResponse::Applied { .. }
        ));
        // a repair, which installs, is still judged against the version it was written from
        assert!(matches!(
            state.apply(&scrub(RepairMode::Repair, ANY_VERSION)),
            ControlResponse::Refused { kind: RefusalKind::StaleVersion, .. }
        ));
    }

    /// A repair of a set under a move queues behind it and is released when the move is done,
    /// a move of a set under a repair queues behind that and is released when the repair is,
    /// and neither is driven while queued ([F45](../../../../docs/src/features/replica-migration.md))
    #[test]
    fn a_repair_and_a_move_serialize_on_a_set() {
        use crate::server::control::migrate::{GroupMove, MoveOutcome, MovePhase, MoveStats};
        use crate::server::control::repair::{GroupRepair, RepairMode, RepairOutcome, RepairPhase};
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: tables.clone(),
            buckets: Vec::new(),
            schema: Default::default(),
        });
        let moving = |op, expected_version, from, to| ControlCommand::Move {
            op,
            principal: "alice".to_string(),
            expected_version,
            tablet: 0,
            from,
            to,
        };
        let repairing = |op, expected_version| ControlCommand::Repair {
            op,
            principal: "alice".to_string(),
            expected_version,
            table: TableId::of("Row"),
            tablet: Some(0),
            mode: RepairMode::Verify,
            source: None,
            release: false,
        };
        // a move, then a repair of the same set: queued behind the move
        let moved = Uuid::new_v4();
        state.apply(&moving(moved, state.topology_version, c, d));
        let group = *state.moves[&moved].groups.keys().next().expect("a group");
        let repaired = Uuid::new_v4();
        state.apply(&repairing(repaired, state.topology_version));
        assert_eq!(
            state.repairs[&repaired].groups[&group].phase,
            RepairPhase::Queued { behind: moved }
        );
        // nobody drives a queued group
        let progress = ControlCommand::RepairProgress {
            op: repaired,
            group,
            node,
            incarnation: 1,
            progress: GroupRepair {
                phase: RepairPhase::Scrubbing,
                ..GroupRepair::default()
            },
        };
        assert!(
            matches!(state.apply(&progress), ControlResponse::Refused { reason, .. } if reason.contains("queued"))
        );
        // the move done releases it to pending
        let done = |op, group| ControlCommand::MoveProgress {
            op,
            group,
            node,
            incarnation: 1,
            progress: GroupMove {
                phase: MovePhase::Done,
                driver: Some(node),
                config: Some(5),
                stats: MoveStats::default(),
                outcome: Some(MoveOutcome::Moved),
            },
        };
        state.apply(&done(moved, group));
        assert_eq!(state.moves[&moved].phase, MovePhase::Done);
        assert_eq!(
            state.repairs[&repaired].groups[&group].phase,
            RepairPhase::Pending
        );
        // a repair under way, then a move of the set: queued behind the repair
        assert!(matches!(
            state.apply(&progress),
            ControlResponse::Applied { .. }
        ));
        let back = Uuid::new_v4();
        state.apply(&moving(back, state.topology_version, d, c));
        assert_eq!(
            state.moves[&back].phase,
            MovePhase::Queued { behind: repaired }
        );
        let group_done = ControlCommand::RepairProgress {
            op: repaired,
            group,
            node,
            incarnation: 1,
            progress: GroupRepair {
                phase: RepairPhase::Done,
                driver: Some(node),
                boundary: Some(9),
                reports: Vec::new(),
                outcome: Some(RepairOutcome::Clean {
                    unreported: Vec::new(),
                }),
            },
        };
        state.apply(&group_done);
        assert!(state.repairs[&repaired].is_done());
        assert_eq!(state.moves[&back].phase, MovePhase::Planned);
        // the released move is recorded against the set as the map served it when it was
        // asked, which the earlier move had already moved
        assert!(state.moves[&back]
            .expected
            .iter()
            .any(|member| member.node == d));
    }

    /// A repair or a backup not yet done is never forgotten for the records past its bound, so what
    /// queued behind it is released when it ends, and only records that are done are forgotten
    /// (item 324)
    ///
    /// Before the fix the oldest repair and the oldest backup past 64 were forgotten whatever their
    /// state. A move queued behind a repair that 64 newer ones (a few scheduled scrubs' worth) pushed
    /// out waited for an end that could never be recorded
    /// ([Resolved #324](../../../../docs/src/appendix/resolved/unfinished-records-kept.md)).
    #[test]
    fn a_repair_or_a_backup_not_done_is_never_forgotten_past_its_bound() {
        use crate::server::control::backup::BackupPhase;
        use crate::server::control::migrate::MovePhase;
        use crate::server::control::repair::{
            GroupRepair, RepairMode, RepairOutcome, RepairPhase, KEPT_REPAIRS,
        };
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: vec![("Row".to_string(), TableId::of("Row"))],
            buckets: Vec::new(),
            schema: Default::default(),
        });
        state.activated = crate::server::replication::snapshot::SNAPSHOT_V2_FROM_WIRE;
        let repairing = |op, expected_version, tablet| ControlCommand::Repair {
            op,
            principal: "alice".to_string(),
            expected_version,
            table: TableId::of("Row"),
            tablet,
            mode: RepairMode::Verify,
            source: None,
            release: false,
        };
        // a repair of tablet 0's set, and a move of the set queued behind it
        let repaired = Uuid::new_v4();
        state.apply(&repairing(repaired, state.topology_version, Some(0)));
        let group = *state.repairs[&repaired].groups.keys().next().expect("a group");
        let moved = Uuid::new_v4();
        state.apply(&ControlCommand::Move {
            op: moved,
            principal: "alice".to_string(),
            expected_version: state.topology_version,
            tablet: 0,
            from: c,
            to: d,
        });
        assert_eq!(state.moves[&moved].phase, MovePhase::Queued { behind: repaired });
        // as many newer repairs as the bound keeps, none of them done, as scheduled scrubs make
        for _ in 0..KEPT_REPAIRS {
            state.apply(&repairing(Uuid::new_v4(), state.topology_version, None));
        }
        assert!(
            state.repairs.contains_key(&repaired),
            "a repair not done was forgotten for {KEPT_REPAIRS} newer ones, and the move queued behind it with it"
        );
        // so its end is recorded, and releases the move
        state.apply(&ControlCommand::RepairProgress {
            op: repaired,
            group,
            node,
            incarnation: 1,
            progress: GroupRepair {
                phase: RepairPhase::Done,
                driver: Some(node),
                boundary: Some(9),
                reports: Vec::new(),
                outcome: Some(RepairOutcome::Clean {
                    unreported: Vec::new(),
                }),
            },
        });
        assert_eq!(state.moves[&moved].phase, MovePhase::Planned);
        // and once done it is the one the next record past the bound forgets
        state.apply(&repairing(Uuid::new_v4(), state.topology_version, None));
        assert!(
            !state.repairs.contains_key(&repaired),
            "a done repair was kept past the bound"
        );
        // a backup not done is kept the same way for as many newer ones
        let backing = |op, expected_version| ControlCommand::Backup {
            op,
            principal: "alice".to_string(),
            expected_version,
            table: Some(TableId::of("Row")),
            path: "/backups".to_string(),
        };
        let backed = Uuid::new_v4();
        state.apply(&backing(backed, state.topology_version));
        assert!(state.backups[&backed].groups.values().any(|progress| progress.phase != BackupPhase::Done));
        for _ in 0..crate::server::control::backup::KEPT_BACKUPS {
            state.apply(&backing(Uuid::new_v4(), state.topology_version));
        }
        assert!(
            state.backups.contains_key(&backed),
            "a backup not done was forgotten for {} newer ones",
            crate::server::control::backup::KEPT_BACKUPS
        );
    }

    /// A backup's group queued behind a move of its set is released when the move is done, and one
    /// queued behind a repair when the repair is (item 323)
    ///
    /// Before the fix only repairs and moves were released, so a backup asked for while a move or a
    /// repair held one of its sets kept that group queued for good: nobody drives a queued group,
    /// and the backup never ended
    /// ([Resolved #323](../../../../docs/src/appendix/resolved/queued-backups-released.md)).
    #[test]
    fn a_backup_queued_behind_a_move_or_a_repair_is_released_when_it_ends() {
        use crate::server::control::backup::{BackupPhase, GroupBackup};
        use crate::server::control::migrate::{GroupMove, MoveOutcome, MovePhase, MoveStats};
        use crate::server::control::repair::{GroupRepair, RepairMode, RepairOutcome, RepairPhase};
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables,
            buckets: Vec::new(),
            schema: Default::default(),
        });
        // a backup file names its cluster in a header written only past wire 5
        state.activated = crate::server::replication::snapshot::SNAPSHOT_V2_FROM_WIRE;
        let backing = |op, expected_version| ControlCommand::Backup {
            op,
            principal: "alice".to_string(),
            expected_version,
            table: Some(TableId::of("Row")),
            path: "/backups".to_string(),
        };
        // a move of tablet 0's set, then a backup of the table: that set's group queued behind it
        let moved = Uuid::new_v4();
        state.apply(&ControlCommand::Move {
            op: moved,
            principal: "alice".to_string(),
            expected_version: state.topology_version,
            tablet: 0,
            from: c,
            to: d,
        });
        let group = *state.moves[&moved].groups.keys().next().expect("a group");
        let backed = Uuid::new_v4();
        let applied = state.apply(&backing(backed, state.topology_version));
        assert!(matches!(applied, ControlResponse::Applied { .. }), "{applied:?}");
        assert_eq!(
            state.backups[&backed].groups[&group].phase,
            BackupPhase::Queued { behind: moved }
        );
        // the move done releases it to pending, so its set's leader drives it
        state.apply(&ControlCommand::MoveProgress {
            op: moved,
            group,
            node,
            incarnation: 1,
            progress: GroupMove {
                phase: MovePhase::Done,
                driver: Some(node),
                config: Some(5),
                stats: MoveStats::default(),
                outcome: Some(MoveOutcome::Moved),
            },
        });
        assert_eq!(state.moves[&moved].phase, MovePhase::Done);
        assert_eq!(
            state.backups[&backed].groups[&group].phase,
            BackupPhase::Pending,
            "a backup's group queued behind a move stayed queued once the move was done"
        );
        // and driven, it is taken as any group's progress is
        let progress = ControlCommand::BackupProgress {
            op: backed,
            group,
            node,
            incarnation: 1,
            progress: GroupBackup {
                phase: BackupPhase::Cutting,
                driver: Some(node),
                boundary: None,
                outcome: None,
            },
        };
        assert!(matches!(state.apply(&progress), ControlResponse::Applied { .. }));
        // a repair of the set, then another backup: queued behind the repair, released when it ends
        let repaired = Uuid::new_v4();
        state.apply(&ControlCommand::Repair {
            op: repaired,
            principal: "alice".to_string(),
            expected_version: state.topology_version,
            table: TableId::of("Row"),
            tablet: Some(0),
            mode: RepairMode::Verify,
            source: None,
            release: false,
        });
        let again = Uuid::new_v4();
        state.apply(&backing(again, state.topology_version));
        assert_eq!(
            state.backups[&again].groups[&group].phase,
            BackupPhase::Queued { behind: repaired }
        );
        state.apply(&ControlCommand::RepairProgress {
            op: repaired,
            group,
            node,
            incarnation: 1,
            progress: GroupRepair {
                phase: RepairPhase::Done,
                driver: Some(node),
                boundary: Some(9),
                reports: Vec::new(),
                outcome: Some(RepairOutcome::Clean {
                    unreported: Vec::new(),
                }),
            },
        });
        assert!(state.repairs[&repaired].is_done());
        assert_eq!(
            state.backups[&again].groups[&group].phase,
            BackupPhase::Pending,
            "a backup's group queued behind a repair stayed queued once the repair was done"
        );
    }

    /// Asking a decommission again retries a plan its failed moves blocked (item 177)
    ///
    /// A member is decommissioned and its plan records a set that failed twice, which leaves it
    /// blocked. Before the fix the same decommission asked again was applied and changed
    /// nothing, so the set stayed blocked for good; now the failures stop counting and the
    /// leader's next pass plans the set again
    /// ([Resolved #177](../../../../docs/src/appendix/resolved/blocked-plan-retry.md)).
    #[test]
    fn asking_again_retries_a_plan_its_failures_blocked() {
        use crate::server::control::plan::{Blocked, PlanStep, StepState};
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: vec![("Row".to_string(), TableId::of("Row"))],
            buckets: Vec::new(),
            schema: Default::default(),
        });
        let decommission = |op, expected_version| ControlCommand::Decommission {
            op,
            principal: "alice".to_string(),
            expected_version,
            node: b,
        };
        let plan = Uuid::new_v4();
        let version = state.topology_version;
        state.apply(&decommission(plan, version));
        // the set at tablet zero failed twice, and the plan is blocked on it
        let failed = || PlanStep {
            tablet: 0,
            from: b,
            to: d,
            bytes: 0,
            op: None,
            state: StepState::Failed {
                reason: "did not catch up within 600s".to_string(),
            },
        };
        {
            let record = state.plans.get_mut(&plan).expect("recorded");
            record.steps = vec![failed(), failed()];
            record.blocked = Some(Blocked {
                reason: "tablet 0: its move failed 2 times".to_string(),
                since: version,
            });
        }
        assert_eq!(state.plans[&plan].failures_of(0), 2);
        // asked again: applied, and the failures so far no longer count
        let version = state.topology_version;
        assert_eq!(
            state.apply(&decommission(Uuid::new_v4(), version)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.plans[&plan].failures_of(0), 0);
        assert_eq!(state.plans[&plan].retried_from, 2);
        // and a failure after the retry counts again
        state.plans.get_mut(&plan).expect("recorded").steps.push(failed());
        assert_eq!(state.plans[&plan].failures_of(0), 1);
    }

    /// A member is decommissioned to leaving and removed to removing, a plain up member cannot
    /// be removed, a second plan on it is refused, a late up on a removing member keeps the
    /// removal, a tombstone needs its sets gone and then refuses the identity at observe and
    /// admit at any incarnation, and a plan's progress moves its record
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[test]
    fn a_member_is_decommissioned_removed_and_tombstoned() {
        use crate::server::control::plan::{
            PlanKind, PlanOutcome, PlanPhase, PlanStep, PlanUpdate, StepState,
        };
        use crate::server::control::types::MemberPhase;
        let (mut state, _, node) = bootstrapped();
        let (b, c, d) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        for (other, name) in [(b, "b"), (c, "c"), (d, "d")] {
            state.apply(&ControlCommand::Admit(member(other, name)));
            state.apply(&ControlCommand::ObserveMember(member(other, name)));
        }
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b, c],
            tables: tables.clone(),
            buckets: Vec::new(),
            schema: Default::default(),
        });
        let decommission = |op, expected_version, node| ControlCommand::Decommission {
            op,
            principal: "alice".to_string(),
            expected_version,
            node,
        };
        let remove = |op, expected_version, node, replacement| ControlCommand::Remove {
            op,
            principal: "alice".to_string(),
            expected_version,
            node,
            replacement,
        };
        // a plain up member cannot be removed, only decommissioned; a stranger neither
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&remove(Uuid::new_v4(), version, b, None)),
            ControlResponse::Refused { reason, .. } if reason.contains("decommission a live one")
        ));
        assert!(matches!(
            state.apply(&decommission(Uuid::new_v4(), version, NodeId::mint())),
            ControlResponse::Refused { reason, .. } if reason.contains("not a member")
        ));
        // decommissioned: leaving, with a plan recorded under the operation
        let plan = Uuid::new_v4();
        assert_eq!(
            state.apply(&decommission(plan, version, b)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.members[&b].phase, MemberPhase::Leaving);
        assert_eq!(state.members[&b].state_name(), "leaving");
        assert!(!state.members[&b].is_placeable());
        let record = &state.plans[&plan];
        assert_eq!(record.kind, PlanKind::Decommission { node: b });
        assert_eq!(record.phase, PlanPhase::Planned);
        assert_eq!(state.open_plans().len(), 1);
        // a leaving member cannot be moved to, and a second plan on it is refused by name
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&ControlCommand::Move { op: Uuid::new_v4(), principal: "alice".to_string(), expected_version: version, tablet: 0, from: c, to: b }),
            ControlResponse::Refused { reason, .. } if reason.contains("leaving")
        ));
        assert!(matches!(
            state.apply(&remove(Uuid::new_v4(), version, b, None)),
            ControlResponse::Refused { reason, .. } if reason.contains("already under plan")
        ));
        // decommissioning it again is applied and changes nothing
        assert_eq!(
            state.apply(&decommission(Uuid::new_v4(), version, b)),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        // a tombstone while it still holds a set is refused
        assert!(matches!(
            state.apply(&ControlCommand::Tombstone { node: b, op: Some(plan) }),
            ControlResponse::Refused { reason, .. } if reason.contains("still holds")
        ));
        // the leader's progress: steps, then a step moving, then moved, then finishing
        let progress = |progress| ControlCommand::PlanProgress {
            op: plan,
            node,
            incarnation: 1,
            progress,
        };
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        let sets: Vec<u16> = map
            .rule_sets_served()
            .iter()
            .map(|(_, tablets)| tablets[0])
            .collect();
        let steps: Vec<PlanStep> = sets
            .iter()
            .map(|tablet| PlanStep {
                tablet: *tablet,
                from: b,
                to: d,
                bytes: 5,
                op: None,
                state: StepState::Pending,
            })
            .collect();
        let version = state.topology_version;
        assert_eq!(
            state.apply(&progress(PlanUpdate::Steps {
                steps: steps.clone(),
                blocked: None
            })),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.plans[&plan].phase, PlanPhase::Running);
        assert_eq!(state.plans[&plan].steps.len(), sets.len());
        assert_eq!(state.plans[&plan].replanned, 1);
        let moved = Uuid::new_v4();
        state.apply(&progress(PlanUpdate::Step {
            tablet: sets[0],
            op: Some(moved),
            state: StepState::Moving,
        }));
        assert_eq!(state.plans[&plan].steps[0].op, Some(moved));
        assert_eq!(state.plans[&plan].steps[0].state, StepState::Moving);
        // the same step again moves nothing
        let version = state.topology_version;
        assert_eq!(
            state.apply(&progress(PlanUpdate::Step {
                tablet: sets[0],
                op: Some(moved),
                state: StepState::Moving
            })),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        // blocked and unblocked by reason
        state.apply(&progress(PlanUpdate::Blocked(Some(
            "tablet 1: nowhere".to_string(),
        ))));
        assert_eq!(state.plans[&plan].phase, PlanPhase::Blocked);
        assert_eq!(
            state.plans[&plan]
                .blocked
                .as_ref()
                .map(|blocked| blocked.reason.as_str()),
            Some("tablet 1: nowhere")
        );
        state.apply(&progress(PlanUpdate::Blocked(None)));
        assert_eq!(state.plans[&plan].phase, PlanPhase::Running);
        // an old run of the leader is fenced, an unknown plan refused
        assert!(matches!(
            state.apply(&ControlCommand::PlanProgress {
                op: plan,
                node,
                incarnation: 0,
                progress: PlanUpdate::Finishing
            }),
            ControlResponse::Fenced { .. }
        ));
        assert!(matches!(
            state.apply(&ControlCommand::PlanProgress {
                op: Uuid::new_v4(),
                node,
                incarnation: 1,
                progress: PlanUpdate::Finishing
            }),
            ControlResponse::Refused { .. }
        ));
        // a decommission that fails puts the member back; a fresh one is planned again
        state.apply(&progress(PlanUpdate::Done(PlanOutcome::Failed {
            reason: "gave up".to_string(),
        })));
        assert!(state.plans[&plan].is_done());
        assert_eq!(state.members[&b].phase, MemberPhase::Member);
        // a done plan stays done
        let version = state.topology_version;
        assert_eq!(
            state.apply(&progress(PlanUpdate::Finishing)),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        // now the removal path: b called down under the grace, then removed at an operator's word
        let episode = Uuid::new_v4();
        state.apply(&ControlCommand::SetHealth {
            node: b,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: Some(episode),
        });
        assert!(state.members[&b]
            .grace
            .as_ref()
            .is_some_and(|grace| grace.episode == episode && grace.elapsed_ms == 0));
        // a replacement has to be a placeable member outside every set of the member
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&remove(Uuid::new_v4(), version, b, Some(c))),
            ControlResponse::Refused { reason, .. } if reason.contains("already holds a set")
        ));
        assert!(matches!(
            state.apply(&remove(Uuid::new_v4(), version, b, Some(NodeId::mint()))),
            ControlResponse::Refused { reason, .. } if reason.contains("not a member")
        ));
        let removal = Uuid::new_v4();
        assert_eq!(
            state.apply(&remove(removal, version, b, Some(d))),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.members[&b].phase, MemberPhase::Removing);
        assert_eq!(state.members[&b].state_name(), "removing");
        assert!(state.members[&b]
            .grace
            .as_ref()
            .is_some_and(|grace| grace.expired && grace.plan == Some(removal)));
        assert_eq!(
            state.plans[&removal].kind,
            PlanKind::Remove {
                node: b,
                replacement: Some(d)
            }
        );
        assert_eq!(
            state.under_replicated_sets(),
            u32::try_from(sets.len()).unwrap()
        );
        // a late up keeps the removal, and its grace with it
        state.apply(&ControlCommand::ObserveMember(member(b, "b")));
        assert_eq!(state.members[&b].health, MemberHealth::Up);
        assert_eq!(state.members[&b].phase, MemberPhase::Removing);
        assert!(state.members[&b].grace.is_some());
        // maintenance cannot suspend a removal
        assert!(matches!(
            state.apply(&ControlCommand::Maintenance { op: Uuid::new_v4(), principal: "alice".to_string(), expected_version: state.topology_version, node: b, suspend: true }),
            ControlResponse::Refused { reason, .. } if reason.contains("cannot suspend a removal")
        ));
        // every set moved to d through the moves the plan issues; the configurations say so
        for tablet in &sets {
            let op = Uuid::new_v4();
            let version = state.topology_version;
            assert_eq!(
                state.apply(&ControlCommand::Move {
                    op,
                    principal: format!("plan {removal}"),
                    expected_version: version,
                    tablet: *tablet,
                    from: b,
                    to: d
                }),
                ControlResponse::Applied {
                    topology_version: version + 1
                }
            );
            let groups: Vec<GroupId> = state.moves[&op].groups.keys().copied().collect();
            for group in groups {
                state.apply(&ControlCommand::MoveProgress {
                    op,
                    group,
                    node,
                    incarnation: 1,
                    progress: crate::server::control::migrate::GroupMove {
                        phase: crate::server::control::migrate::MovePhase::Activated,
                        driver: Some(node),
                        config: Some(3),
                        stats: crate::server::control::migrate::MoveStats::default(),
                        outcome: None,
                    },
                });
            }
        }
        let map = crate::server::map::TabletMap::from_state(&state, None, &tables);
        assert!(map
            .rule_sets_served()
            .iter()
            .all(|(members, _)| members.iter().all(|member| member.node != b)));
        assert_eq!(state.under_replicated_sets(), 0);
        // tombstoned: removed, the grace gone, the identity refused at observe and admit at
        // any incarnation, and never re-added by a configuration naming it
        let version = state.topology_version;
        assert_eq!(
            state.apply(&ControlCommand::Tombstone {
                node: b,
                op: Some(removal)
            }),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.members[&b].phase, MemberPhase::Removed);
        assert_eq!(state.members[&b].state_name(), "removed");
        assert!(state.members[&b].grace.is_none());
        assert_eq!(state.tombstones[&b].op, Some(removal));
        assert_eq!(
            state.apply(&ControlCommand::Tombstone {
                node: b,
                op: Some(removal)
            }),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        let mut later = member(b, "b");
        later.incarnation = 9;
        assert_eq!(
            state.apply(&ControlCommand::ObserveMember(later.clone())),
            ControlResponse::Removed { node: b }
        );
        assert_eq!(
            state.apply(&ControlCommand::Admit(later)),
            ControlResponse::Removed { node: b }
        );
        assert_eq!(
            state.apply(&ControlCommand::SetHealth {
                node: b,
                health: MemberHealth::Up,
                incarnation: 9,
                episode: None
            }),
            ControlResponse::Removed { node: b }
        );
        assert!(matches!(
            state.apply(&remove(Uuid::new_v4(), state.topology_version, b, None)),
            ControlResponse::Refused { reason, .. } if reason.contains("already removed")
        ));
        assert!(matches!(
            state.apply(&decommission(Uuid::new_v4(), state.topology_version, b)),
            ControlResponse::Refused { reason, .. } if reason.contains("removed")
        ));
        // a membership entry still naming it does not bring it back
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(node, member(node, "a"));
        nodes.insert(c, member(c, "c"));
        state.members.remove(&b);
        nodes.insert(b, member(b, "b"));
        let membership = openraft::Membership::new(vec![[node, c].into_iter().collect()], nodes)
            .expect("a membership");
        state.observe_membership(&membership);
        assert!(!state.members.contains_key(&b));
        // a member the configuration no longer names votes in nothing
        assert_eq!(state.members[&d].role, MemberRole::Learner);
        assert_eq!(
            state.voters(),
            vec![node, c]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        // a rebalance is one at a time, and done with nothing is done
        let version = state.topology_version;
        let rebalance = Uuid::new_v4();
        assert_eq!(
            state.apply(&ControlCommand::Rebalance {
                op: rebalance,
                principal: "alice".to_string(),
                expected_version: version
            }),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert!(matches!(
            state.apply(&ControlCommand::Rebalance { op: Uuid::new_v4(), principal: "alice".to_string(), expected_version: version + 1 }),
            ControlResponse::Refused { reason, .. } if reason.contains("already under way")
        ));
        state.apply(&ControlCommand::PlanProgress {
            op: rebalance,
            node,
            incarnation: 1,
            progress: PlanUpdate::Done(PlanOutcome::Nothing {
                reason: "balanced".to_string(),
            }),
        });
        assert!(state.plans[&rebalance].is_done());
        // the removal is the one plan still open until the leader says it is done
        assert_eq!(state.open_plans().len(), 1);
        let completed = state.plans[&removal].completed();
        state.apply(&ControlCommand::PlanProgress {
            op: removal,
            node,
            incarnation: 1,
            progress: PlanUpdate::Done(completed),
        });
        assert!(state.open_plans().is_empty());
        assert!(
            state.tombstones.contains_key(&b),
            "a completed removal leaves the tombstone"
        );
    }

    /// A grace opens with a down episode, its count is monotonic and of its own episode, a
    /// suspended one is neither counted nor expired, a resumed one continues, and expiry
    /// happens once and records the plan ([F46](../../../../docs/src/features/capacity-rebalancing.md), Q7)
    #[test]
    fn grace_elapsed_is_monotonic_and_expires_once() {
        use crate::server::control::plan::PlanKind;
        use crate::server::control::types::MemberPhase;
        let (mut state, _, node) = bootstrapped();
        let b = NodeId::mint();
        state.apply(&ControlCommand::Admit(member(b, "b")));
        state.apply(&ControlCommand::ObserveMember(member(b, "b")));
        let version = state.topology_version;
        state.apply(&ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: version,
            nodes: vec![node, b],
            tables: vec![("Row".to_string(), TableId::of("Row"))],
            buckets: Vec::new(),
            schema: Default::default(),
        });
        // no grace on a member that is up
        assert!(state.members[&b].grace.is_none());
        let elapsed = |episode, elapsed_ms, expire| ControlCommand::GraceElapsed {
            node: b,
            episode,
            elapsed_ms,
            expire,
        };
        assert!(
            matches!(state.apply(&elapsed(Uuid::new_v4(), 5, None)), ControlResponse::Refused { reason, .. } if reason.contains("no grace"))
        );
        // down opens one at zero
        let episode = Uuid::new_v4();
        state.apply(&ControlCommand::SetHealth {
            node: b,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: Some(episode),
        });
        let grace = state.members[&b].grace.clone().expect("a grace");
        assert_eq!(
            (
                grace.episode,
                grace.elapsed_ms,
                grace.suspended,
                grace.expired
            ),
            (episode, 0, false, false)
        );
        // a count of another episode is refused; a count moves the version once per change
        assert!(matches!(
            state.apply(&elapsed(Uuid::new_v4(), 5, None)),
            ControlResponse::Refused { .. }
        ));
        let version = state.topology_version;
        assert_eq!(
            state.apply(&elapsed(episode, 1000, None)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(
            state.apply(&elapsed(episode, 1000, None)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        // never backwards
        assert_eq!(
            state.apply(&elapsed(episode, 500, None)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(
            state.members[&b]
                .grace
                .as_ref()
                .map(|grace| grace.elapsed_ms),
            Some(1000)
        );
        // suspended: not counted, and an operator's word alone
        let maintenance = |suspend, expected_version| ControlCommand::Maintenance {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version,
            node: b,
            suspend,
        };
        assert!(matches!(
            state.apply(&ControlCommand::Maintenance { op: Uuid::new_v4(), principal: "alice".to_string(), expected_version: state.topology_version, node, suspend: true }),
            ControlResponse::Refused { reason, .. } if reason.contains("no grace")
        ));
        let version = state.topology_version;
        assert_eq!(
            state.apply(&maintenance(true, version)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(
            state.apply(&maintenance(true, version + 1)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        let version = state.topology_version;
        assert_eq!(
            state.apply(&elapsed(episode, 5000, Some(Uuid::new_v4()))),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        assert_eq!(
            state.members[&b]
                .grace
                .as_ref()
                .map(|grace| grace.elapsed_ms),
            Some(1000)
        );
        assert_eq!(state.members[&b].phase, MemberPhase::Member);
        // resumed: continues from the committed count
        assert_eq!(
            state.apply(&maintenance(false, version)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        state.apply(&elapsed(episode, 2000, None));
        assert_eq!(
            state.members[&b]
                .grace
                .as_ref()
                .map(|grace| grace.elapsed_ms),
            Some(2000)
        );
        // a return clears the grace; a second episode is a fresh one
        state.apply(&ControlCommand::ObserveMember(member(b, "b")));
        assert!(state.members[&b].grace.is_none());
        let second = Uuid::new_v4();
        state.apply(&ControlCommand::SetHealth {
            node: b,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: Some(second),
        });
        assert_eq!(
            state.members[&b]
                .grace
                .as_ref()
                .map(|grace| (grace.episode, grace.elapsed_ms)),
            Some((second, 0))
        );
        assert!(matches!(
            state.apply(&elapsed(episode, 3000, None)),
            ControlResponse::Refused { .. }
        ));
        // expiry: once, removing under the plan named, and a second word changes nothing
        let plan = Uuid::new_v4();
        let version = state.topology_version;
        assert_eq!(
            state.apply(&elapsed(second, 9000, Some(plan))),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        let grace = state.members[&b].grace.clone().expect("a grace");
        assert!(grace.expired);
        assert_eq!(grace.plan, Some(plan));
        assert_eq!(state.members[&b].phase, MemberPhase::Removing);
        assert_eq!(
            state.plans[&plan].kind,
            PlanKind::Expiry {
                node: b,
                episode: second
            }
        );
        assert_eq!(state.plans[&plan].principal, "policy");
        assert_eq!(
            state.apply(&elapsed(second, 9500, Some(Uuid::new_v4()))),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.plans.len(), 1);
        // and a policy that never removes opens no grace at all
        let mut never = ControlState::default();
        let mut policy = Cluster::default().policy();
        policy.auto_remove_after = None;
        let cluster = ClusterId::mint();
        never.apply(&ControlCommand::Bootstrap {
            cluster,
            policy,
            member: member(node, "a"),
        });
        never.apply(&ControlCommand::Admit(member(b, "b")));
        never.apply(&ControlCommand::ObserveMember(member(b, "b")));
        never.apply(&ControlCommand::SetHealth {
            node: b,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: Some(Uuid::new_v4()),
        });
        assert!(never.members[&b].grace.is_none());
    }

    /// A membership entry sets roles, admits configured strangers as joining, and moves once
    #[test]
    fn a_membership_entry_is_reflected_in_the_roles() {
        let (mut state, _, node) = bootstrapped();
        let learner = NodeId::mint();
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(node, member(node, "a"));
        nodes.insert(learner, member(learner, "b"));
        let membership =
            openraft::Membership::new(vec![[node].into_iter().collect()], nodes.clone())
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
        let promoted =
            openraft::Membership::new(vec![[node, learner].into_iter().collect()], nodes)
                .expect("a membership");
        assert!(state.observe_membership(&promoted));
        assert_eq!(state.members[&learner].role, MemberRole::Voter);
        let mut both = vec![node, learner];
        both.sort();
        assert_eq!(state.voters(), both);
        assert!(state.learners().is_empty());
    }

    /// An activation needs every member at the wire as reported, never lowers, ignores a
    /// removed member, heals a record a build from before the field persisted without it,
    /// and a member below the activated wire is refused at observe and at admit
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md))
    #[test]
    fn activation_needs_every_member_at_the_wire() {
        use crate::shared::protocol::{MIN_PEER_VERSION, PROTOCOL_VERSION};
        let (mut state, _, node) = bootstrapped();
        // a record from before F48 reads as the floor, and so does the state
        assert_eq!(state.members[&node].record.wire_max(), MIN_PEER_VERSION);
        assert_eq!(state.activated_wire(), MIN_PEER_VERSION);
        // a member speaking a version, by name
        let speaking = |node: NodeId, client: &str, max: u8| MemberRecord {
            wire_min: MIN_PEER_VERSION,
            wire_max: max,
            ..member(node, client)
        };
        // an activation carrying what the members reported, as the leader heard them
        let activate = |op: Uuid, expected_version: u64, wire: u8, reported: &[(NodeId, u8)]| {
            ControlCommand::Activate {
                op,
                principal: "admin".to_string(),
                expected_version,
                wire,
                members: reported.iter().copied().collect(),
            }
        };
        // an activation of a version the one member reports less than is refused naming it
        let version = state.topology_version;
        match state.apply(&activate(
            Uuid::new_v4(),
            version,
            PROTOCOL_VERSION,
            &[(node, MIN_PEER_VERSION)],
        )) {
            ControlResponse::Refused { reason, .. } => {
                assert!(
                    reason.contains(&node.to_string()) && reason.contains("speaks less"),
                    "{reason}"
                );
            }
            other => panic!("an activation above a member's wire applied: {other:?}"),
        }
        // and so is one that does not name it at all
        assert!(matches!(
            state.apply(&activate(Uuid::new_v4(), version, PROTOCOL_VERSION, &[])),
            ControlResponse::Refused { .. }
        ));
        assert_eq!(state.activated_wire(), MIN_PEER_VERSION);
        // the floor itself is already activated: applied without moving the version
        assert_eq!(
            state.apply(&activate(
                Uuid::new_v4(),
                version,
                MIN_PEER_VERSION,
                &[(node, MIN_PEER_VERSION)]
            )),
            ControlResponse::Applied {
                topology_version: version
            }
        );
        // the member restarts on a build that speaks the newest, and a second member joins
        // speaking it too; a third is admitted at the floor and then removed
        let mut upgraded = speaking(node, "a", PROTOCOL_VERSION);
        upgraded.incarnation = 2;
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(upgraded)),
            ControlResponse::Applied { .. }
        ));
        let second = NodeId::mint();
        assert!(matches!(
            state.apply(&ControlCommand::Admit(speaking(
                second,
                "b",
                PROTOCOL_VERSION
            ))),
            ControlResponse::Applied { .. }
        ));
        let third = NodeId::mint();
        assert!(matches!(
            state.apply(&ControlCommand::Admit(speaking(
                third,
                "c",
                MIN_PEER_VERSION
            ))),
            ControlResponse::Applied { .. }
        ));
        // the third holds the activation back, by what it reported
        let version = state.topology_version;
        let reported = [
            (node, PROTOCOL_VERSION),
            (second, PROTOCOL_VERSION),
            (third, MIN_PEER_VERSION),
        ];
        match state.apply(&activate(
            Uuid::new_v4(),
            version,
            PROTOCOL_VERSION,
            &reported,
        )) {
            ControlResponse::Refused { reason, .. } => {
                assert!(reason.contains(&third.to_string()), "{reason}")
            }
            other => panic!("an activation above a member's wire applied: {other:?}"),
        }
        // until it is removed, which takes it out of the judgment; and the second member's
        // record, persisted by an older build without the field, is healed by the apply
        state.members.get_mut(&third).expect("the third").phase = super::MemberPhase::Removed;
        state
            .members
            .get_mut(&second)
            .expect("the second")
            .record
            .wire_max = 0;
        assert_eq!(state.members[&second].record.wire_max(), MIN_PEER_VERSION);
        let op = Uuid::new_v4();
        let version = state.topology_version;
        let reported = [(node, PROTOCOL_VERSION), (second, PROTOCOL_VERSION)];
        assert_eq!(
            state.apply(&activate(op, version, PROTOCOL_VERSION, &reported)),
            ControlResponse::Applied {
                topology_version: version + 1
            }
        );
        assert_eq!(state.activated_wire(), PROTOCOL_VERSION);
        assert_eq!(state.members[&second].record.wire_max(), PROTOCOL_VERSION);
        // a repeat of the operation is answered as the first was
        assert!(matches!(
            state.apply(&activate(op, version, PROTOCOL_VERSION, &reported)),
            ControlResponse::Repeated { .. }
        ));
        // it never lowers
        let version = state.topology_version;
        match state.apply(&activate(
            Uuid::new_v4(),
            version,
            MIN_PEER_VERSION,
            &reported,
        )) {
            ControlResponse::Refused { reason, .. } => {
                assert!(reason.contains("never lowers"), "{reason}")
            }
            other => panic!("an activation lowered: {other:?}"),
        }
        // and a stale version is refused before anything is judged
        assert!(matches!(
            state.apply(&activate(
                Uuid::new_v4(),
                version - 1,
                PROTOCOL_VERSION,
                &reported
            )),
            ControlResponse::Refused { .. }
        ));
        // a member below the activated wire is refused at observe, at any incarnation, and at admit
        let mut rolled_back = speaking(node, "a", MIN_PEER_VERSION);
        rolled_back.incarnation = 3;
        match state.apply(&ControlCommand::ObserveMember(rolled_back)) {
            ControlResponse::Refused { reason, .. } => {
                assert!(reason.contains("activated"), "{reason}")
            }
            other => panic!("a member below the activated wire was observed: {other:?}"),
        }
        assert_eq!(state.members[&node].record.incarnation, 2);
        let fourth = NodeId::mint();
        assert!(matches!(
            state.apply(&ControlCommand::Admit(speaking(
                fourth,
                "d",
                MIN_PEER_VERSION
            ))),
            ControlResponse::Refused { .. }
        ));
        assert!(!state.members.contains_key(&fourth));
        // one at the wire is admitted
        assert!(matches!(
            state.apply(&ControlCommand::Admit(speaking(
                fourth,
                "d",
                PROTOCOL_VERSION
            ))),
            ControlResponse::Applied { .. }
        ));
        // the map carries the activation
        let map = crate::server::map::TabletMap::from_state(&state, None, &[]);
        assert_eq!(map.activated_wire, PROTOCOL_VERSION);
    }

    /// The kind a refusal carries, so an assertion reads the kind and not the sentence
    ///
    /// # Arguments
    ///
    /// * `response` - What the state machine answered
    fn kind_of(response: &ControlResponse) -> RefusalKind {
        match response {
            ControlResponse::Refused { kind, .. } => *kind,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Every refusal names its kind, and the kind is what the code is derived from
    ///
    /// Before this a refusal was a sentence and the control thread read the sentence to pick
    /// a code, so every kind but a stale version reached the client as `Internal`
    /// ([Resolved #98](../../../../docs/src/appendix/resolved/admin-refusal-kinds.md)). One
    /// refusal of each kind the admin path can meet is driven here and read by kind alone.
    #[test]
    fn a_refusal_names_its_kind() {
        // nothing bootstrapped yet, so there is nothing to observe a member of
        let mut empty = ControlState::default();
        let stranger = NodeId::mint();
        assert_eq!(
            kind_of(&empty.apply(&ControlCommand::ObserveMember(member(stranger, "z")))),
            RefusalKind::NotInitialized
        );
        let (mut state, _, node) = bootstrapped();
        // a second bootstrap would fork the cluster
        let again = ControlCommand::Bootstrap {
            cluster: ClusterId::mint(),
            policy: Cluster::default().policy(),
            member: member(NodeId::mint(), "b"),
        };
        assert_eq!(
            kind_of(&state.apply(&again)),
            RefusalKind::AlreadyInitialized
        );
        // a voter count outside the policy
        let voters = ControlCommand::SetControlVoters {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version: 1,
            count: 4,
        };
        assert_eq!(kind_of(&state.apply(&voters)), RefusalKind::BadVoterCount);
        // a member the cluster does not have
        let health = ControlCommand::SetHealth {
            node: stranger,
            health: MemberHealth::Down,
            incarnation: 1,
            episode: None,
        };
        assert_eq!(kind_of(&state.apply(&health)), RefusalKind::NotMember);
        // the placement, refused for every reason it can be
        let joiner = NodeId::mint();
        state.apply(&ControlCommand::Admit(member(joiner, "b")));
        let tables = vec![("Row".to_string(), TableId::of("Row"))];
        let initialize = |expected_version, nodes: Vec<NodeId>| ControlCommand::Initialize {
            op: Uuid::new_v4(),
            principal: "alice".to_string(),
            expected_version,
            nodes,
            tables: tables.clone(),
            buckets: Vec::new(),
            schema: Default::default(),
        };
        // written against a version the cluster has moved past
        assert_eq!(
            kind_of(&state.apply(&initialize(1, vec![node]))),
            RefusalKind::StaleVersion
        );
        // with no node at all
        assert_eq!(
            kind_of(&state.apply(&initialize(2, Vec::new()))),
            RefusalKind::Invalid
        );
        // on a member still joining
        assert_eq!(
            kind_of(&state.apply(&initialize(2, vec![node, joiner]))),
            RefusalKind::NotUp
        );
        // naming a member twice
        assert_eq!(
            kind_of(&state.apply(&initialize(2, vec![node, node]))),
            RefusalKind::Duplicate
        );
        // naming a stranger
        assert_eq!(
            kind_of(&state.apply(&initialize(2, vec![stranger]))),
            RefusalKind::NotMember
        );
        // and once it is initialized, initializing it again
        assert_eq!(
            state.apply(&initialize(2, vec![node])),
            ControlResponse::Applied {
                topology_version: 3
            }
        );
        assert_eq!(
            kind_of(&state.apply(&initialize(3, vec![node]))),
            RefusalKind::AlreadyInitialized
        );
        // a kind that crossed from a peer that predates it decodes to the default
        let decoded: ControlResponse =
            serde_json::from_str(r#"{"Refused":{"reason":"an old peer's refusal"}}"#)
                .expect("a refusal without a kind still decodes");
        assert_eq!(kind_of(&decoded), RefusalKind::Other);
    }

    /// A schema is extended only along its lineage, once every member's build holds it (F93)
    ///
    /// Refused below wire 15, while the one member's build does not hold the schema, for a
    /// schema that does not append to the committed one, and for pools that change a committed
    /// binding; applied, the new tables and buckets are the cluster's, the new bucket is bound
    /// past every consumer id given, a repeat is answered as the first, and the same schema again
    /// moves nothing.
    #[test]
    fn a_schema_extends_only_along_its_lineage_once_every_member_holds_it() {
        use crate::server::control::schema::SchemaRecord;
        use crate::shared::pools::PoolsConf;
        use crate::shared::protocol::PROTOCOL_VERSION;
        let (mut state, _, node) = bootstrapped();
        // the one member at the newest wire, and the wire activated, building schema [1]
        let mut record = MemberRecord {
            wire_min: crate::shared::protocol::MIN_PEER_VERSION,
            wire_max: PROTOCOL_VERSION,
            schema_id: 1,
            schema_lineage: vec![1],
            incarnation: 2,
            ..member(node, "a")
        };
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(record.clone())),
            ControlResponse::Applied { .. }
        ));
        // the schema the cluster is initialized with, and the one appended to it
        let table = |name: &str| (name.to_string(), TableId::of(name));
        let schema = |lineage: &[u64], tables: &[&str]| SchemaRecord {
            id: *lineage.last().unwrap(),
            lineage: lineage.to_vec(),
            fingerprints: tables
                .iter()
                .enumerate()
                .map(|(at, name)| (TableId::of(name), at as u64 + 100))
                .collect(),
        };
        let base = schema(&[1], &["Movie"]);
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&ControlCommand::Initialize {
                op: Uuid::new_v4(),
                principal: "admin".to_string(),
                expected_version: version,
                nodes: vec![node],
                tables: vec![table("Movie")],
                buckets: Vec::new(),
                schema: base.clone(),
            }),
            ControlResponse::Applied { .. }
        ));
        assert_eq!(state.schema.as_ref(), Some(&base));
        // the appended schema: a table and a bucket bound to a new pool
        let appended = schema(
            &[1, 2],
            &["Movie", "Review", "PostersObjectMeta", "PostersStripeMeta"],
        );
        let tables = vec![
            table("Movie"),
            table("Review"),
            table("PostersObjectMeta"),
            table("PostersStripeMeta"),
        ];
        let buckets = vec![BucketRecord {
            name: "Posters".to_string(),
            id: BucketId::of("Posters"),
            tables: vec![
                "PostersObjectMeta".to_string(),
                "PostersStripeMeta".to_string(),
            ],
        }];
        let pools: PoolsConf = serde_yaml::from_str(
            "pools:\n  fast: {class: ssd, redundancy: {replicas: 3}}\nbuckets:\n  Posters: {pool: fast}\n",
        )
        .expect("pools parse");
        let extend = |op: Uuid, expected_version: u64, schema: &SchemaRecord, pools: &PoolsConf| {
            ControlCommand::ExtendSchema {
                op,
                principal: "admin".to_string(),
                expected_version,
                schema: schema.clone(),
                tables: tables.clone(),
                buckets: buckets.clone(),
                pools: pools.clone(),
            }
        };
        // below wire 15 nothing is extended
        let version = state.topology_version;
        match state.apply(&extend(Uuid::new_v4(), version, &appended, &pools)) {
            ControlResponse::Refused { reason, .. } => assert!(reason.contains("wire"), "{reason}"),
            other => panic!("an extension below wire 15 applied: {other:?}"),
        }
        let version = state.topology_version;
        assert!(matches!(
            state.apply(&ControlCommand::Activate {
                op: Uuid::new_v4(),
                principal: "admin".to_string(),
                expected_version: version,
                wire: PROTOCOL_VERSION,
                members: [(node, PROTOCOL_VERSION)].into_iter().collect(),
            }),
            ControlResponse::Applied { .. }
        ));
        // the member's build does not hold the appended schema yet, and is named
        let version = state.topology_version;
        match state.apply(&extend(Uuid::new_v4(), version, &appended, &pools)) {
            ControlResponse::Refused { reason, .. } => {
                assert!(reason.contains(&node.to_string()) && reason.contains("upgrade"), "{reason}");
            }
            other => panic!("an extension past a member's build applied: {other:?}"),
        }
        // it restarts on the appended build
        record.schema_id = 2;
        record.schema_lineage = vec![1, 2];
        record.incarnation = 3;
        assert!(matches!(
            state.apply(&ControlCommand::ObserveMember(record.clone())),
            ControlResponse::Applied { .. }
        ));
        // a schema that changes the committed field rather than appending is refused
        let version = state.topology_version;
        let changed = schema(&[9, 2], &["Movie", "Review"]);
        match state.apply(&extend(Uuid::new_v4(), version, &changed, &pools)) {
            ControlResponse::Refused { reason, .. } => {
                assert!(reason.contains("does not append"), "{reason}");
            }
            other => panic!("a changed schema extended: {other:?}"),
        }
        // and so are pools that leave the new bucket unbound
        let unbound: PoolsConf =
            serde_yaml::from_str("pools:\n  fast: {class: ssd, redundancy: {replicas: 3}}\n")
                .expect("pools parse");
        assert!(matches!(
            state.apply(&extend(Uuid::new_v4(), version, &appended, &unbound)),
            ControlResponse::Refused { .. }
        ));
        // the extension applies: the tables, the bucket and its binding are the cluster's
        let op = Uuid::new_v4();
        let applied = state.apply(&extend(op, version, &appended, &pools));
        assert!(matches!(applied, ControlResponse::Applied { .. }), "{applied:?}");
        assert_eq!(state.schema.as_ref(), Some(&appended));
        assert_eq!(state.tables, tables);
        assert_eq!(state.buckets, buckets);
        let policy = state.policy.as_ref().expect("a policy");
        let binding = policy.objects.bindings.get("Posters").expect("a binding");
        assert_eq!(binding.pool, "fast");
        assert!(binding.consumer > 0);
        // a repeat is answered as the first, moving nothing
        let moved = state.topology_version;
        assert!(matches!(
            state.apply(&extend(op, version, &appended, &pools)),
            ControlResponse::Repeated { .. }
        ));
        assert_eq!(state.topology_version, moved);
        // a later binding change is refused: the bucket stays where it was bound
        let rebound: PoolsConf = serde_yaml::from_str(
            "pools:\n  fast: {class: ssd, redundancy: {replicas: 3}}\n  slow: {class: hdd, redundancy: {replicas: 3}}\nbuckets:\n  Posters: {pool: slow}\n",
        )
        .expect("pools parse");
        match state.apply(&extend(Uuid::new_v4(), moved, &appended, &rebound)) {
            // the same schema again is where the cluster is, whatever else the request says
            ControlResponse::Applied { topology_version } => assert_eq!(topology_version, moved),
            other => panic!("{other:?}"),
        }
        // and the map serves every table the extension added
        let map = crate::server::map::TabletMap::from_state(&state, Some(node), &[]);
        assert_eq!(map.tables, tables);
        assert_eq!(map.schema, Some(2));
    }
}
