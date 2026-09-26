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

use super::backup::{
    judge_coverage, BackupFile, BackupPhase, BackupRecord, GroupBackup, GroupRestore,
    RecoveryRecord, RestoreRecord, KEPT_BACKUPS, KEPT_RESTORES,
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
use crate::shared::identity::{ClusterId, GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::MIN_PEER_VERSION;

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
        /// The structural fingerprint of the schema the files were cut from
        source_schema: u64,
        /// Every file, as the leader read the manifests
        files: Vec<BackupFile>,
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
    /// A reason the kinds above do not name, which a peer on an older wire version decodes to
    #[default]
    Other,
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
            ControlResponse::Applied { topology_version } => Some(*topology_version),
            ControlResponse::Repeated { first } => first.applied_version(),
            ControlResponse::Refused { .. }
            | ControlResponse::Fenced { .. }
            | ControlResponse::Removed { .. } => None,
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
    /// The repair operations, by identity, the newest `KEPT_REPAIRS` of them
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
    /// The backup operations, by identity, the newest `KEPT_BACKUPS` of them
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
                if let Some(refusal) = self.check_version(*expected_version) {
                    return refusal;
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
                // forget the oldest once too many are kept
                while self.repairs.len() > KEPT_REPAIRS {
                    let oldest = self
                        .repairs
                        .values()
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
                source_schema,
                files,
            } => self.apply_restore(
                *op,
                principal,
                *expected_version,
                path,
                *source,
                *source_schema,
                files,
            ),
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
        }
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
            MemberPhase::Leaving => return self.applied(),
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
            MemberPhase::Removing => return self.applied(),
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
        // the last group done finishes the record and releases what waited behind it
        let record = self.moves.get_mut(&op).expect("checked above");
        if record.groups.values().all(GroupMove::is_done) {
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
        // every group of the table or the tables, queued behind whatever holds its set
        let map = crate::server::map::TabletMap::from_state(self, None, &self.tables.clone());
        let mut groups: BTreeMap<GroupId, GroupBackup> = BTreeMap::new();
        for (_, id) in self.tables.clone() {
            if table.is_some_and(|wanted| wanted != id) {
                continue;
            }
            for (group, _, _) in map.groups_of(id) {
                let behind = self
                    .moves
                    .values()
                    .filter(|record| !record.is_done() && record.groups.contains_key(&group))
                    .map(|record| (record.requested_at, record.op))
                    .chain(
                        self.repairs
                            .values()
                            .filter(|record| {
                                !record.is_done() && record.groups.contains_key(&group)
                            })
                            .map(|record| (record.requested_at, record.op)),
                    )
                    .max()
                    .map(|(_, op)| op);
                let progress = match behind {
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
            },
        );
        // forget the oldest once too many are kept
        while self.backups.len() > KEPT_BACKUPS {
            let oldest = self
                .backups
                .values()
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
    /// * `source_schema` - The schema they were cut from
    /// * `files` - Every file, as the leader read the manifests
    #[allow(clippy::too_many_arguments)]
    fn apply_restore(
        &mut self,
        op: Uuid,
        principal: &str,
        expected_version: u64,
        path: &str,
        source: ClusterId,
        source_schema: u64,
        files: &[BackupFile],
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
        if groups.is_empty() {
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
                source_schema,
                principal: principal.to_string(),
                requested_at: self.topology_version,
                files: files.to_vec(),
                groups,
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
            return self.applied();
        }
        // a driver of an earlier try, whose group was retried since, is too late to say
        // anything ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md))
        if progress.generation != current.generation {
            return self.applied();
        }
        if current == progress {
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
        if !record.groups.values().any(GroupRestore::failed) {
            return ControlResponse::refused(
                RefusalKind::Invalid,
                format!("no group of restore {restore} failed, so there is nothing to retry"),
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

    /// Refuse a request written against a version other than the current one
    ///
    /// # Arguments
    ///
    /// * `expected` - The version the request was written against
    fn check_version(&self, expected: u64) -> Option<ControlResponse> {
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
        Consistency, ControlCommand, ControlResponse, ControlState, MemberHealth, MemberRecord,
        MemberRole, RefusalKind,
    };
    use crate::server::conf::Cluster;
    use crate::shared::identity::{ClusterId, GroupId, NodeId, TableId};
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
            physical: 0,
            incarnation: 1,
            weight: 0,
            wire_min: 0,
            wire_max: 0,
            capabilities: 0,
            schema_id: 0,
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
                source_schema: 0,
                principal: "admin".to_string(),
                requested_at: state.topology_version,
                files: Vec::new(),
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
}
