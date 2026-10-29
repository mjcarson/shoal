//! The frames a client uses to see and change a cluster, and the topology every client can hold
//!
//! ```text
//!  Topology       : [header][query id (16 B)][json TopologyFrame]     server → client
//!  Admin          : [header][request id (16 B)][json AdminRequest]    client → server
//!  AdminResponse  : [header][request id (16 B)][json AdminResponse]   server → client
//! ```
//!
//! Every one of them carries the sixteen byte id every server frame carries after its header,
//! so the client reads one preamble for these as it does for a response ([`decode_server_frame`]).
//! A topology pushed because the cluster changed carries the nil id, the way a connection level
//! error does: it is about the connection's view of the cluster, not about any query.
//!
//! The bodies are JSON rather than rkyv on purpose. A topology is a few hundred bytes read as
//! often by a person as by a program, an admin request is a handful of fields with a uuid in
//! them, and neither is on any path a query takes; the query bytes stay rkyv and this module
//! never touches them. JSON is also what the control lane already speaks
//! ([F38](../../../../docs/src/features/inter-node-transport.md)), so the same serde types
//! serve both.
//!
//! # Invariants
//!
//! **Nothing here decides anything.** Whether a principal may mutate the cluster, whether a
//! version is current and whether an operation was already applied are the server's to judge;
//! this module only names the request, the answer and the refusal so that both ends spell them
//! the same way ([C9](../../../../docs/src/distributed/operations.md)).
//!
//! [`decode_server_frame`]: super::decode_server_frame

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::ErrorCode;
use super::QUERY_ID_LEN;
use crate::shared::grants::{GrantOn, Privileges};
use crate::shared::identity::{ClusterId, NodeId, ShardAddr, TableId};

/// A request a client makes of the cluster rather than of a table
///
/// Every mutation carries an operation id and the topology version it was written against:
/// the id makes a repeat harmless, since the cluster answers a seen id with what it answered
/// before, and the version makes a request written against a stale view refused rather than
/// applied to a cluster it never saw.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminRequest {
    /// The identity of this operation, minted by the client and kept across a retry
    pub op: Uuid,
    /// The topology version this request was written against
    ///
    /// Ignored by a read; a mutation is refused `StaleVersion` when it differs from the
    /// cluster's current version.
    pub expected_version: u64,
    /// What is asked
    pub kind: AdminKind,
}

/// What an administrative request asks for
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdminKind {
    /// Every member the cluster knows, with its role and health
    Members,
    /// Whether this node is live, joined and able to serve default reads and writes
    Readiness,
    /// What the failure detector currently believes, committed and local
    Detector,
    /// What the node's tablet groups look like: leaders, applied lag, pending bytes
    ///
    /// The "lag" record [C9](../../../../docs/src/distributed/operations.md) asks for, as every
    /// shard last reported it ([F40](../../../../docs/src/features/replication.md)).
    Replication,
    /// Place every tablet across these nodes, once, in this order
    ///
    /// Explicit by design: nothing places data on a node because it happened to join
    /// ([C4](../../../../docs/src/distributed/tablet-map.md)).
    Initialize {
        /// The members to place tablets over, in placement order
        nodes: Vec<NodeId>,
    },
    /// Change how many nodes vote in the control group
    SetControlVoters {
        /// The new count, which has to be one, three or five
        count: u32,
    },
    /// Set, or clear, the level one table's reads are served at when a bundle does not say
    ///
    /// `one` or `quorum`, or none to fall back to the cluster's `read_consistency`
    /// ([F41](../../../../docs/src/features/read-consistency.md)).
    SetTableReadPolicy {
        /// The table, by the name the schema spells it
        table: String,
        /// The level, or none to clear it
        level: Option<String>,
    },
    /// Scrub a table's groups at a committed boundary, judge the copies, and repair or release
    ///
    /// `verify` scrubs, judges and quarantines what the verdict names; `repair` goes on to
    /// replace every quarantined copy from a verified source. A `source` overrides the
    /// majority rule with the operator's word, and `release` lifts the quarantines the
    /// operation's groups hold after an explicit outcome rather than judging them
    /// ([F44](../../../../docs/src/features/repair.md)).
    Repair {
        /// The table, by the name the schema spells it
        table: String,
        /// One tablet, or every tablet of the table
        tablet: Option<u16>,
        /// `verify` or `repair`
        mode: String,
        /// The node whose copies are to be trusted, or none for the majority rule
        source: Option<NodeId>,
        /// Whether to lift the quarantines rather than judge
        #[serde(default)]
        release: bool,
    },
    /// The record of a repair operation, as the control state holds it
    RepairStatus {
        /// The operation
        op: Uuid,
    },
    /// Move the replica set holding a tablet from one member to another
    ///
    /// The set is every table's group over the tablets the rule placed together; `from` has
    /// to be a member of it and `to` an up member that is not, placed or not. The destination
    /// is fed as a learner, made a voter through the group's own membership transition, and
    /// published as the set's configuration before the source's copy retires
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    Move {
        /// A tablet the set serves
        tablet: u16,
        /// The member leaving the set
        from: NodeId,
        /// The member replacing it
        to: NodeId,
    },
    /// The record of a move operation, as the control state holds it
    MoveStatus {
        /// The operation
        op: Uuid,
    },
    /// Drain a live member: it is `leaving` from the commit, takes no new placement, and the
    /// control leader moves every set it holds elsewhere before it is removed and tombstoned
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    Decommission {
        /// The member
        node: NodeId,
    },
    /// Remove a down or leaving member: it is `removing` from the commit and the control
    /// leader rebuilds every set it holds from the surviving copies, the named replacement
    /// first; `Replace` is this with a replacement named
    Remove {
        /// The member
        node: NodeId,
        /// The member to take its place first, if one is named
        #[serde(default)]
        replacement: Option<NodeId>,
    },
    /// Fail one device, as an I/O error on it would: its node stops serving it, reports it
    /// failed, and its placement groups move off it ([F88](../../../../docs/src/features/recovery-and-moves.md))
    ///
    /// Committed as an order on the device's record, which its node carries out, so any member
    /// may be asked. ~~A restart that finds the device whole brings it back.~~ Since
    /// [F92](../../../../docs/src/features/reclamation-and-device-lifecycle.md) the device is
    /// retired for good once its failure is committed, and a restart refuses it by name.
    FailDevice {
        /// The device
        device: crate::shared::devices::DeviceId,
    },
    /// Drain one device: its era ends so its placement groups move off it, copied from it while
    /// it serves, and what they leave on it is released (F92)
    DrainDevice {
        /// The device
        device: crate::shared::devices::DeviceId,
    },
    /// Remove one device from the pool map for good: refused by name unless it was retired, or
    /// drained with nothing left on it, or `lost` says it is gone whatever it held (F92)
    RemoveDevice {
        /// The device
        device: crate::shared::devices::DeviceId,
        /// The operator's word that the device is gone
        #[serde(default)]
        lost: bool,
    },
    /// Give one device the weight an operator chose, which its placement groups move by (F92)
    ReweightDevice {
        /// The device
        device: crate::shared::devices::DeviceId,
        /// Its share of a pool
        weight: u64,
    },
    /// Scrub a pool's placement groups now, light or deep, whatever their schedule and window
    /// ([F89](../../../../docs/src/features/scrub-and-repair.md))
    ///
    /// Committed as a count of requests on the pool map: the leader of every tablet group of a
    /// bucket the pool serves begins a pass for it, and `pool_status` says when none is pending.
    Scrub {
        /// The pool
        pool: String,
        /// Light or deep
        kind: crate::shared::scrub::ScrubKind,
        /// A bucket, a device or a slice the pass is narrowed to, or none for the whole pool
        /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<crate::shared::scrub::ScrubTarget>,
    },
    /// An operator's word on a stripe whose row is behind its chunks, which every read and write
    /// of it is refused `Behind` for until it is given
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    ///
    /// Answered by the shard the request reached, as an object operation is: `accept` commits the
    /// label `k + f` of the stripe's holders agree on, past its row, and marks the rest missed for
    /// recovery; `lost` declares the stripe lost on its row, which every read and write of it is
    /// answered by name. A conditional commit on the row read, under the operation's id, so a
    /// repeat is answered as the first was. Admins only.
    ResolveStripe {
        /// The bucket, by the name the schema spells it
        bucket: String,
        /// The object's path
        path: String,
        /// The stripe, by its index in the object
        stripe: u64,
        /// What to trust
        resolution: StripeResolution,
    },
    /// Suspend or resume the grace a down member is removed after
    Maintenance {
        /// The member
        node: NodeId,
        /// Whether to suspend the grace, or resume it
        suspend: bool,
    },
    /// Spread the replica sets over the members by their weights and measured bytes
    ///
    /// The one way data spreads onto a member that joined after the placement: nothing moves
    /// onto a new node until an operator asks
    Rebalance,
    /// The record of a plan - a decommission, a removal, an expiry or a rebalance
    PlanStatus {
        /// The operation
        op: Uuid,
    },
    /// Every plan the control state holds, done or not
    Plans,
    /// Back a table, or every table, up under a directory: one file per group, each cut at a
    /// committed boundary of its own and written by the group's leader on its node
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md))
    Backup {
        /// The table, by the name the schema spells it, or none for every table
        #[serde(default)]
        table: Option<String>,
        /// The directory the files go under, on every node that writes one
        path: String,
    },
    /// The record of a backup operation, as the control state holds it
    BackupStatus {
        /// The operation
        op: Uuid,
    },
    /// Every backup record the control state holds, done or not
    Backups,
    /// Restore a backup into this cluster from a directory every node can read
    ///
    /// Once per cluster, into a cluster that is not the one the backup was cut in, and into
    /// tables that hold nothing; refused by name otherwise. The old cluster's members are
    /// refused at every door from the commit ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    Restore {
        /// The directory the files are read from
        path: String,
    },
    /// The record of a restore operation, as the control state holds it
    RestoreStatus {
        /// The operation
        op: Uuid,
    },
    /// Drive every group a finished restore failed again, from the phase each failed in
    ///
    /// Under the same operation and against the same files; a group that was restored is not
    /// touched. Refused while the restore is still running, when nothing in it failed, and
    /// below wire version 6 ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md)).
    RetryRestore {
        /// The restore operation to retry
        restore: Uuid,
    },
    /// Every recovery an operator ran on a survivor, oldest first
    Recoveries,
    /// Read this node's peer certificate, key and authority again and use them from now on
    ///
    /// Node-local: the node the connection reached reloads its own material and answers what
    /// it read; nothing is committed. Needs an admin principal, since it changes what the node
    /// proves itself with ([F50](../../../../docs/src/features/cluster-operations.md)).
    ReloadTls,
    /// Activate a wire version: every member speaks it from the commit, and none rolls back
    /// past it ([F48](../../../../docs/src/features/rolling-compatibility.md))
    ///
    /// Refused by name while any member not removed reports a newest version below it, and
    /// never lowers what is activated. `Members` carries `wire` - the activated version, the
    /// floor and the newest this build speaks, and the lowest and highest a member reports -
    /// which is what an operator reads first.
    Activate {
        /// The version to activate
        wire: u8,
    },
    /// Move the cluster's committed schema to one that appends tables or buckets to it
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    ///
    /// The schema moved to is the one the node asked runs, which the request names by its id so
    /// that a request sent to a node of another build is refused rather than extending to that
    /// build's. Refused by name unless it extends the committed schema, unless every member not
    /// removed runs a build that holds it, while a move or a repair is open, and below wire 15.
    /// Applied, the new tables' groups start on every member and each new bucket is bound to its
    /// pool; no member rolls back past it.
    ExtendSchema {
        /// The schema to move to, by its structural id
        schema: u64,
        /// The storage pools and every bucket's binding, the committed ones unchanged, or none
        /// for the configuration of the node asked
        #[serde(default)]
        pools: Option<crate::shared::pools::PoolsConf>,
    },
    /// What every member holds and does: its standing, its tablets, partitions and bytes, its
    /// write rates, and how far and how fast every plan has got
    /// ([F52](../../../../docs/src/features/cluster-stats.md))
    ///
    /// Answered in full by the control leader, which holds every member's figures from its
    /// status reports; any other node answers the committed half and its own figures, and
    /// names the leader. Only sent to a node whose `Members` frame lists `stats` in its
    /// `admin_reads`, since a node that cannot decode a kind closes the connection.
    Stats {
        /// The table to narrow the figures to, by the name the schema spells it, or none for
        /// every table
        #[serde(default)]
        table: Option<String>,
    },
    /// Grant a principal privileges on a table or a bucket, on top of what it already holds
    /// there ([F77](../../../../docs/src/features/authorization.md),
    /// [F79](../../../../docs/src/features/bucket-schema.md))
    ///
    /// Committed control state, so every coordinator holds a principal to the same grants. A
    /// grant of what is already held is applied without moving the version. The principal need
    /// not be one any node accepts yet: the grant is held for it until one does.
    Grant {
        /// The principal granted, by the name it authenticates as
        principal: String,
        /// Whether the grant is on a table or a bucket
        #[serde(default)]
        on: GrantOn,
        /// The table or bucket, by the name the schema spells it, or `*` for every one
        name: String,
        /// What is granted
        privileges: Privileges,
    },
    /// Take back privileges a grant on a table or a bucket gave a principal
    ///
    /// A revoke on one name takes nothing from what a grant on `*` gives, since there are no
    /// negative grants; one that takes back nothing held is applied without moving the version.
    Revoke {
        /// The principal revoked, by the name it authenticates as
        principal: String,
        /// Whether the grant was on a table or a bucket
        #[serde(default)]
        on: GrantOn,
        /// The name the grant was made on, as the schema spells it, or `*`
        name: String,
        /// What is taken back
        privileges: Privileges,
    },
    /// The grants the cluster holds every principal to
    ///
    /// Answered in full to an admin, and to a connection on a cluster that requires no
    /// authentication; any other principal is answered its own grants alone. Only sent to a node
    /// whose `Members` frame lists `grants` in its `admin_reads`.
    Grants,
    /// The storage pools and bucket bindings the cluster serves objects by
    ///
    /// Answered from the asked node's own applied state, so it shows what that node holds - the
    /// committed policy, whatever its own file says
    /// ([F80](../../../../docs/src/features/object-wire.md)). Only sent to a node whose `Members`
    /// frame lists `pools` in its `admin_reads`.
    Pools,
    /// Every member's devices, with their slices, health and figures, and the pool map
    ///
    /// The figures are the leader's, as its members last reported them; another node answers its
    /// own and the committed map ([F85](../../../../docs/src/features/devices-and-pools.md)).
    /// Only sent to a node whose `Members` frame lists `devices` in its `admin_reads`.
    Devices,
    /// Whether each storage pool can take writes, and what it is short of, by name
    ///
    /// Only sent to a node whose `Members` frame lists `pool_status` in its `admin_reads`.
    PoolStatus,
    /// What each bucket holds and what its clients were answered, summed over every member
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    ///
    /// Its objects and bytes as of each member's last reclamation pass, its operations by kind
    /// since each member started, with their bytes in and out and their latency. Answered whole by
    /// the control leader; another node answers its own figures and says so. Only sent to a node
    /// whose `Members` frame lists `bucket_stats` in its `admin_reads`.
    BucketStats {
        /// The bucket to narrow the figures to, or none for every bucket
        #[serde(default)]
        bucket: Option<String>,
    },
    /// One page of a walk of a tablet's rows, read from the copy the asked node holds
    ///
    /// Answered by the shard hosting the tablet on the node asked, whether that copy leads its
    /// group or not, with the keys in ascending order, the applied index the page was read at
    /// and where the next page starts ([F87](../../../../docs/src/features/tablet-walk.md)). On
    /// a cluster only a principal `cluster.admins` names may ask, since a walk reads a table
    /// whole, a bucket's generated tables included; a standalone node answers only a connection
    /// that is not authenticated, which on a node requiring none is its operator. Only sent to
    /// a node whose `Members` frame lists `walk` in its `admin_reads`.
    Walk {
        /// The table, by the name the schema spells it
        table: String,
        /// The tablet
        tablet: u16,
        /// The first key of the range walked, the tablet's first unless set
        #[serde(default)]
        low: Option<u64>,
        /// The last key of the range walked, the tablet's last unless set
        #[serde(default)]
        high: Option<u64>,
        /// The last key the previous page handed out, or none for the first page
        #[serde(default)]
        after: Option<u64>,
        /// The most partitions the page holds, [`WALK_DEFAULT_LIMIT`] unless set
        #[serde(default)]
        limit: Option<u32>,
        /// The bytes past which the page holds no more partitions, [`WALK_DEFAULT_BYTES`] unless set
        #[serde(default)]
        bytes: Option<u64>,
        /// The lowest applied index the page may be read at: the highest any page of the walk was
        ///
        /// A copy behind it refuses the page, naming where it is, so a walk resumed on a copy
        /// that restarted or a leader that lags is never read from a state older than its floor.
        #[serde(default)]
        at_least: Option<u64>,
        /// Whose count the page is admitted by: a driver's or an operator's unless set, which is
        /// every walk before [F116](../../../../docs/src/features/scan-admission.md)
        ///
        /// Left out of a driver's request, so its JSON is what it was. Only sent to a node whose
        /// `Members` frame lists `walk_class` in its `admin_reads`: an older node ignores the
        /// field and takes a scan's page as a driver's, which could shed a driver's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        class: Option<WalkClass>,
    },
}

/// What the `BucketStats` read answers: each bucket's figures, summed over the members heard
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketStatsAnswer {
    /// `leader` when the answering node leads and holds every member's figures, `local` when it
    /// holds only its own
    pub source: String,
    /// How many members' figures were summed
    pub members: u32,
    /// Each bucket's figures, by name
    pub buckets: std::collections::BTreeMap<String, crate::shared::protocol::stats::BucketFigures>,
}

/// What an operator says of a stripe whose row is behind its chunks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StripeResolution {
    /// Trust the label `k + f` of its holders agree on
    Accept,
    /// Give its bytes up: the stripe reads as lost
    Lost,
}

/// The name of the record every backup writes beside its files once it ends, under
/// `<path>/<op>/`: every group the backup was asked for, where each was placed and what it came
/// to, which a restore reads before anything else and refuses a backup without (item 322)
///
/// Here rather than beside the record's type, so `shoaladm ship-backup`, which links no engine,
/// can refuse to ship a backup no host holds the record of.
pub const BACKUP_RECORD_FILE: &str = "backup.json";

/// Whose count a page of a walk is admitted by ([F116](../../../../docs/src/features/scan-admission.md))
///
/// A shard reads a few pages at once of each class and sheds the rest. A scan's pages have a count
/// of their own, apart from the pages the node's own drivers - backfill, copies, scrubs,
/// reclamation, an archive's backup - and an operator's walks ask for, so a client's scan never
/// sheds the walk that brings a stale copy current.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalkClass {
    /// A node's own driver's page, or an operator's: the count every walk shared before scans
    #[default]
    Driver,
    /// A scan's page: a count of its own, never a driver's
    Scan,
}

impl WalkClass {
    /// What the class is called, as a refusal and a figure name it
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        // the serialized spelling
        match self {
            WalkClass::Driver => "driver",
            WalkClass::Scan => "scan",
        }
    }
}

/// The most bytes a scan's page holds, whatever it asks for
///
/// A scan's archived reads wait for its device's idle time and draw on its budget, so a page of
/// a few seconds of a busy device's trickle at most still lands well inside an admin read's
/// deadline ([F116](../../../../docs/src/features/scan-admission.md)).
pub const SCAN_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// Whether a count is zero, which a field left out at zero is written as
///
/// # Arguments
///
/// * `count` - The count
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(count: &u64) -> bool {
    *count == 0
}

/// The most partitions a page of a walk holds when it does not say
pub const WALK_DEFAULT_LIMIT: u32 = 1000;

/// The most partitions any page of a walk may ask for
pub const WALK_MAX_LIMIT: u32 = 10_000;

/// The bytes past which a page of a walk holds no more partitions when it does not say
pub const WALK_DEFAULT_BYTES: u64 = 1024 * 1024;

/// The most bytes any page of a walk may ask for
pub const WALK_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// One page of a walk of a tablet's rows, as an admin read answers it
///
/// The keys alone and what each partition holds, not the rows: an admin read is JSON, and what
/// a person or a check reads of a walk is which partitions are there
/// ([F87](../../../../docs/src/features/tablet-walk.md)). An engine driver reads the rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkAnswer {
    /// The table walked, by name
    pub table: String,
    /// The tablet walked
    pub tablet: u16,
    /// The first key of the range walked
    pub low: u64,
    /// The last key of the range walked
    pub high: u64,
    /// The key this page started after, if any
    pub after: Option<u64>,
    /// Every partition the page holds, by key, ascending
    pub keys: Vec<u64>,
    /// The live rows of each partition, in the order of `keys`
    pub rows: Vec<u32>,
    /// The archived bytes of every partition the page holds
    pub bytes: u64,
    /// The applied index the copy had reached when the page began, or none on a standalone node
    pub boundary: Option<u64>,
    /// Whether the copy led its group when the page began, or none on a standalone node
    pub led: Option<bool>,
    /// The key the next page starts after, or none once the range is walked
    pub next: Option<u64>,
    /// The group the copy belongs to, or none on a standalone node
    pub group: Option<String>,
    /// How many partitions came from memory
    pub resident: u32,
    /// How many came from an archive
    pub archived: u32,
    /// How many archived records had moved under the page and were looked up again
    pub reprobed: u32,
    /// How long the page held its shard's loop, capturing it, in microseconds
    pub held_us: u64,
    /// How long the page's reads took off the loop, in microseconds
    pub read_us: u64,
    /// How long a scan's page waited from its admission to its capture, for its shard's loop to be
    /// idle, in microseconds ([F117](../../../../docs/src/features/scan-class.md)); left out at
    /// zero, which a driver's page always is, and absent from an answer from before F117
    #[serde(default, skip_serializing_if = "is_zero")]
    pub queued_us: u64,
    /// Each partition's stripe row, for a bucket's `StripeMeta` table
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stripes: Option<Vec<WalkedStripe>>,
}

/// One stripe row a walk of a bucket's `StripeMeta` table handed out
///
/// The object id is written as 32 hex digits: a JSON number does not hold a `u128`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkedStripe {
    /// The consumer whose stripe it is
    pub consumer: u64,
    /// The object, as 32 hex digits
    pub object: String,
    /// The stripe's index in its object
    pub stripe: u64,
    /// The row's sequence
    pub sequence: u64,
}

impl AdminKind {
    /// Whether this request changes the cluster, and so needs an admin principal and a version
    #[must_use]
    pub const fn is_mutation(&self) -> bool {
        matches!(
            self,
            AdminKind::Initialize { .. }
                | AdminKind::SetControlVoters { .. }
                | AdminKind::SetTableReadPolicy { .. }
                | AdminKind::Repair { .. }
                | AdminKind::Move { .. }
                | AdminKind::Decommission { .. }
                | AdminKind::Remove { .. }
                | AdminKind::Maintenance { .. }
                | AdminKind::Rebalance
                | AdminKind::Activate { .. }
                | AdminKind::ExtendSchema { .. }
                | AdminKind::Backup { .. }
                | AdminKind::Restore { .. }
                | AdminKind::RetryRestore { .. }
                | AdminKind::ReloadTls
                | AdminKind::Grant { .. }
                | AdminKind::Revoke { .. }
                | AdminKind::Scrub { .. }
                | AdminKind::FailDevice { .. }
                | AdminKind::DrainDevice { .. }
                | AdminKind::RemoveDevice { .. }
                | AdminKind::ReweightDevice { .. }
                | AdminKind::ResolveStripe { .. }
        )
    }

    /// The name of this request kind, for a log line
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            AdminKind::Members => "members",
            AdminKind::Readiness => "readiness",
            AdminKind::Detector => "detector",
            AdminKind::Replication => "replication",
            AdminKind::Initialize { .. } => "initialize",
            AdminKind::SetControlVoters { .. } => "set_control_voters",
            AdminKind::SetTableReadPolicy { .. } => "set_table_read_policy",
            AdminKind::Repair { .. } => "repair",
            AdminKind::RepairStatus { .. } => "repair_status",
            AdminKind::Move { .. } => "move",
            AdminKind::MoveStatus { .. } => "move_status",
            AdminKind::Decommission { .. } => "decommission",
            AdminKind::Remove { .. } => "remove",
            AdminKind::Maintenance { .. } => "maintenance",
            AdminKind::FailDevice { .. } => "fail_device",
            AdminKind::DrainDevice { .. } => "drain_device",
            AdminKind::RemoveDevice { .. } => "remove_device",
            AdminKind::ReweightDevice { .. } => "reweight_device",
            AdminKind::Scrub { .. } => "scrub",
            AdminKind::Rebalance => "rebalance",
            AdminKind::PlanStatus { .. } => "plan_status",
            AdminKind::Plans => "plans",
            AdminKind::Activate { .. } => "activate",
            AdminKind::ExtendSchema { .. } => "extend_schema",
            AdminKind::Backup { .. } => "backup",
            AdminKind::BackupStatus { .. } => "backup_status",
            AdminKind::Backups => "backups",
            AdminKind::Restore { .. } => "restore",
            AdminKind::RestoreStatus { .. } => "restore_status",
            AdminKind::RetryRestore { .. } => "retry_restore",
            AdminKind::Recoveries => "recoveries",
            AdminKind::ReloadTls => "reload_tls",
            AdminKind::Stats { .. } => "stats",
            AdminKind::Grant { .. } => "grant",
            AdminKind::Revoke { .. } => "revoke",
            AdminKind::Grants => "grants",
            AdminKind::Pools => "pools",
            AdminKind::Devices => "devices",
            AdminKind::PoolStatus => "pool_status",
            AdminKind::BucketStats { .. } => "bucket_stats",
            AdminKind::Walk { .. } => "walk",
            AdminKind::ResolveStripe { .. } => "resolve_stripe",
        }
    }
}

/// Why an administrative request was refused
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminError {
    /// What class of refusal this is, as the number it is written as
    pub code: u16,
    /// What the server said about it, for a person
    pub msg: String,
}

impl AdminError {
    /// Build a refusal
    ///
    /// # Arguments
    ///
    /// * `code` - What class of refusal this is
    /// * `msg` - What to say about it
    pub fn new<M: Into<String>>(code: ErrorCode, msg: M) -> Self {
        AdminError {
            code: code.as_u16(),
            msg: msg.into(),
        }
    }

    /// What class of refusal this is
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code)
    }
}

/// The answer to an administrative request
///
/// Every answer says which node answered and what topology version it held, so a reader can
/// tell a stale answer from a current one whatever the body says.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminResponse {
    /// The node that answered
    pub node: NodeId,
    /// The topology version that node held when it answered
    pub topology_version: u64,
    /// The answer, or why there is none
    pub outcome: Result<AdminOutcome, AdminError>,
}

/// What an administrative request produced
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdminOutcome {
    /// A read, as the JSON the server built for it
    Read(serde_json::Value),
    /// A mutation applied, and the topology version it moved the cluster to
    Applied {
        /// The version after the change
        version: u64,
    },
    /// A mutation seen before under this operation id, answered as it was the first time
    Repeated {
        /// The version the first application moved the cluster to
        version: u64,
    },
}

/// One member of the cluster, as a client sees it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyMember {
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
    /// Whether it votes in the control group or only learns from it
    pub role: String,
    /// Whether it is joining, up or down, as the control group has committed it
    pub health: String,
    /// Whether it is a plain `member`, `leaving`, `removing` or `removed`
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default = "default_phase")]
    pub phase: String,
    /// The one name of its state: the phase past plain membership, the health otherwise
    #[serde(default)]
    pub state: String,
    /// Which start of it the cluster has admitted
    pub incarnation: u64,
    /// The shards that have failed on it, by index
    pub shards_failed: Vec<u16>,
    /// The copies it holds that are quarantined, which reads are routed around
    /// ([F44](../../../../docs/src/features/repair.md))
    #[serde(default)]
    pub quarantined: Vec<QuarantinedMember>,
    /// Its share of the leads of the groups it votes in, one when it sets none
    ///
    /// What a client needs to name a group's preferred leader the way the server does, and so
    /// route a write to it ([F58](../../../../docs/src/features/weighted-leadership.md),
    /// [F74](../../../../docs/src/features/client-routing.md)). A frame from before F74 reads
    /// as zero, which counts as one.
    #[serde(default)]
    pub lead_weight: u32,
}

/// The phase a member frame from before F46 is read with
fn default_phase() -> String {
    "member".to_string()
}

/// One quarantined copy on a member, as a client sees it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantinedMember {
    /// The table
    pub table: TableId,
    /// The group, as its identity's number
    pub group: u64,
    /// The tablets the copy serves
    pub tablets: Vec<u16>,
    /// Why
    pub reason: String,
}

/// A replica set that no longer follows the placement rule, as a client sees it
/// ([F45](../../../../docs/src/features/replica-migration.md))
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfiguredSet {
    /// The tablets the set serves, ascending
    pub tablets: Vec<u16>,
    /// Its members, the primary first
    pub members: Vec<ShardAddr>,
    /// The topology version it was published at
    pub published_at: u64,
}

/// A move not yet done, as a client sees it
/// ([F45](../../../../docs/src/features/replica-migration.md))
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveSummary {
    /// The operation
    pub op: Uuid,
    /// The tablets the replica set serves, ascending
    pub tablets: Vec<u16>,
    /// The member leaving the set
    pub from: ShardAddr,
    /// The member replacing it
    pub to: ShardAddr,
    /// Where the move stands, by the phase's name
    pub phase: String,
}

/// The cluster as a client sees it
///
/// The placement is the ordered node list every node builds its ring from, not a table of
/// tablets: tablet `t` belongs to `placement[t % N]` and, on that node, to shard
/// `(t / N) % shards`, the same rule the server routes with. Empty before the placement is
/// initialized. A replica set that moved is listed in `configurations` and served by the
/// members named there instead ([F45](../../../../docs/src/features/replica-migration.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyFrame {
    /// The cluster
    pub cluster: ClusterId,
    /// How many committed changes the topology has seen
    pub version: u64,
    /// The control leader, if the answering node knows one
    pub leader: Option<NodeId>,
    /// Every member, in node order
    pub members: Vec<TopologyMember>,
    /// The nodes tablets are placed over, in placement order; empty before initialization
    pub placement: Vec<NodeId>,
    /// How many replicas each tablet is meant to have
    pub desired_rf: u32,
    /// How many replicas each tablet has
    pub active_rf: u32,
    /// What a write waits for, as the policy spells it
    pub write_consistency: String,
    /// What a read is served at
    pub read_consistency: String,
    /// The tables the schema serves, with their stable identities
    pub tables: Vec<(String, TableId)>,
    /// The tables whose reads are served at a level of their own, by name
    /// ([F41](../../../../docs/src/features/read-consistency.md))
    #[serde(default)]
    pub table_read_policy: Vec<(String, String)>,
    /// The replica sets that no longer follow the placement rule
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub configurations: Vec<ConfiguredSet>,
    /// The moves not yet done
    #[serde(default)]
    pub moves: Vec<MoveSummary>,
    /// The identities removed for good, which vote in no group again
    ///
    /// A group's voters are its members less these, and its preferred leader is drawn from its
    /// voters, so a client routing writes by topology needs them
    /// ([F74](../../../../docs/src/features/client-routing.md)).
    #[serde(default)]
    pub tombstones: Vec<NodeId>,
}

/// Write a body of `[id][json]` for any of the three frames
///
/// # Arguments
///
/// * `id` - The query or request id, or nil for a pushed topology
/// * `value` - What to serialize after it
///
/// # Errors
///
/// Fails only if the value cannot be serialized, which none of the types here can fail at.
pub fn encode_body<T: Serialize>(id: &Uuid, value: &T) -> Result<Vec<u8>, serde_json::Error> {
    // the id first, so the client's one preamble read finds it where a response's is
    let mut body = Vec::with_capacity(QUERY_ID_LEN + 256);
    body.extend_from_slice(id.as_bytes());
    serde_json::to_writer(&mut body, value)?;
    Ok(body)
}

/// Read the JSON that follows the id of one of the three frames
///
/// # Arguments
///
/// * `rest` - The bytes after the preamble, which is where the client's reader leaves them
///
/// # Errors
///
/// Fails if the bytes are not the JSON of `T`.
pub fn decode_rest<T: for<'a> Deserialize<'a>>(rest: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A walk is a read named `walk`, so a node answers it to an admin without a version (F87)
    #[test]
    fn walk_is_a_read_named_walk() {
        let walk = AdminKind::Walk {
            table: "Movie".to_string(),
            tablet: 7,
            low: None,
            high: None,
            after: None,
            limit: None,
            bytes: None,
            at_least: None,
            class: None,
        };
        assert!(!walk.is_mutation());
        assert_eq!(walk.name(), "walk");
        // the bounds a page may ask hold its default inside them
        assert!(WALK_DEFAULT_LIMIT <= WALK_MAX_LIMIT);
        assert!(WALK_DEFAULT_BYTES <= WALK_MAX_BYTES);
    }

    /// A walk names its class only when it is a scan's, so a driver's request and an operator's are
    /// the JSON they were and an absent class reads as a driver's (F116)
    #[test]
    fn a_walk_names_its_class_only_for_a_scan() {
        let driver = AdminKind::Walk {
            table: "Movie".to_string(),
            tablet: 3,
            low: None,
            high: None,
            after: None,
            limit: None,
            bytes: None,
            at_least: None,
            class: None,
        };
        let json = serde_json::to_string(&driver).expect("a walk encodes");
        assert!(!json.contains("class"), "{json}");
        // a scan's says so, in the spelling a person reads
        let scan: AdminKind =
            serde_json::from_str(r#"{"Walk":{"table":"Movie","tablet":3,"class":"scan"}}"#).expect("a scan's walk");
        assert!(matches!(scan, AdminKind::Walk { class: Some(WalkClass::Scan), .. }));
        assert_eq!(WalkClass::default(), WalkClass::Driver);
        assert_eq!((WalkClass::Driver.as_str(), WalkClass::Scan.as_str()), ("driver", "scan"));
        // and a scan's page is held to fewer bytes than any page may ask
        assert!(SCAN_MAX_BYTES < WALK_MAX_BYTES && SCAN_MAX_BYTES >= WALK_DEFAULT_BYTES);
    }

    /// A walk request leaves out every bound it does not set, and its answer round trips, a
    /// stripe's object id as hex (F87)
    #[test]
    fn a_walk_request_round_trips_json() {
        // a request naming the table and the tablet alone decodes with every bound unset
        let bare: AdminKind =
            serde_json::from_str(r#"{"Walk":{"table":"Movie","tablet":3}}"#).expect("a bare walk");
        assert_eq!(
            bare,
            AdminKind::Walk {
                table: "Movie".to_string(),
                tablet: 3,
                low: None,
                high: None,
                after: None,
                limit: None,
                bytes: None,
                at_least: None,
                class: None,
            }
        );
        // a request with every bound round trips
        let full = AdminKind::Walk {
            table: "PostersStripeMeta".to_string(),
            tablet: 4095,
            low: Some(u64::MAX - 10),
            high: Some(u64::MAX),
            after: Some(u64::MAX - 5),
            limit: Some(2),
            bytes: Some(1),
            at_least: Some(77),
            class: Some(WalkClass::Scan),
        };
        let json = serde_json::to_vec(&full).expect("a walk encodes");
        assert_eq!(decode_rest::<AdminKind>(&json).expect("a walk decodes"), full);
        // an answer with a stripe whose object needs every bit of a u128
        let answer = WalkAnswer {
            table: "PostersStripeMeta".to_string(),
            tablet: 4095,
            low: 1,
            high: u64::MAX,
            after: None,
            keys: vec![u64::MAX],
            rows: vec![1],
            bytes: 300,
            boundary: Some(12),
            led: Some(true),
            next: None,
            group: Some("g".to_string()),
            resident: 1,
            archived: 0,
            reprobed: 0,
            held_us: 5,
            read_us: 9,
            queued_us: 0,
            stripes: Some(vec![WalkedStripe {
                consumer: 1,
                object: format!("{:032x}", u128::MAX),
                stripe: 2,
                sequence: 3,
            }]),
        };
        let json = serde_json::to_value(&answer).expect("an answer encodes");
        assert_eq!(serde_json::from_value::<WalkAnswer>(json).expect("an answer decodes"), answer);
    }

    /// A frame's body is the id and then the JSON, and each type round trips through it
    #[test]
    fn admin_bodies_round_trip() {
        let id = Uuid::new_v4();
        // a request, both kinds
        let request = AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 7,
            kind: AdminKind::Initialize {
                nodes: vec![NodeId::mint(), NodeId::mint()],
            },
        };
        let body = encode_body(&id, &request).expect("a request encodes");
        assert_eq!(&body[..QUERY_ID_LEN], id.as_bytes());
        let back: AdminRequest = decode_rest(&body[QUERY_ID_LEN..]).expect("a request decodes");
        assert_eq!(back, request);
        assert!(request.kind.is_mutation());
        assert!(!AdminKind::Members.is_mutation());
        // a move is a mutation and its status a read, and both round trip
        let moving = AdminKind::Move {
            tablet: 7,
            from: NodeId::mint(),
            to: NodeId::mint(),
        };
        assert!(moving.is_mutation());
        assert_eq!(moving.name(), "move");
        let status = AdminKind::MoveStatus { op: Uuid::new_v4() };
        assert!(!status.is_mutation());
        for kind in [moving, status] {
            let json = serde_json::to_vec(&kind).expect("a kind encodes");
            assert_eq!(
                decode_rest::<AdminKind>(&json).expect("a kind decodes"),
                kind
            );
        }
        // the placement operations are mutations and the plan reads are not, and all round trip (F46)
        let node = NodeId::mint();
        let mutations = [
            AdminKind::Activate { wire: 5 },
            AdminKind::Backup {
                table: None,
                path: "/backups".to_string(),
            },
            AdminKind::Restore {
                path: "/backups/x".to_string(),
            },
            AdminKind::RetryRestore {
                restore: Uuid::new_v4(),
            },
            AdminKind::Decommission { node },
            AdminKind::Remove {
                node,
                replacement: Some(NodeId::mint()),
            },
            AdminKind::Remove {
                node,
                replacement: None,
            },
            AdminKind::Maintenance {
                node,
                suspend: true,
            },
            AdminKind::Rebalance,
            AdminKind::ReloadTls,
            AdminKind::Scrub {
                pool: "fast".to_string(),
                kind: crate::shared::scrub::ScrubKind::Deep,
                target: None,
            },
            // a scrub narrowed to a bucket, a device and a slice (F93)
            AdminKind::Scrub {
                pool: "fast".to_string(),
                kind: crate::shared::scrub::ScrubKind::Light,
                target: Some(crate::shared::scrub::ScrubTarget::Bucket(
                    crate::shared::identity::BucketId::of("Posters"),
                )),
            },
            AdminKind::Scrub {
                pool: "fast".to_string(),
                kind: crate::shared::scrub::ScrubKind::Deep,
                target: Some(crate::shared::scrub::ScrubTarget::Device(
                    crate::shared::devices::DeviceId(Uuid::new_v4()),
                )),
            },
            AdminKind::Scrub {
                pool: "fast".to_string(),
                kind: crate::shared::scrub::ScrubKind::Deep,
                target: Some(crate::shared::scrub::ScrubTarget::Slice(
                    crate::shared::poolmap::SliceRef {
                        device: crate::shared::devices::DeviceId(Uuid::new_v4()),
                        index: 1,
                    },
                )),
            },
            AdminKind::ExtendSchema {
                schema: 42,
                pools: None,
            },
            AdminKind::ResolveStripe {
                bucket: "Posters".to_string(),
                path: "a/b".to_string(),
                stripe: 3,
                resolution: StripeResolution::Accept,
            },
            AdminKind::FailDevice {
                device: crate::shared::devices::DeviceId(Uuid::new_v4()),
            },
            AdminKind::DrainDevice {
                device: crate::shared::devices::DeviceId(Uuid::new_v4()),
            },
            AdminKind::RemoveDevice {
                device: crate::shared::devices::DeviceId(Uuid::new_v4()),
                lost: true,
            },
            AdminKind::ReweightDevice {
                device: crate::shared::devices::DeviceId(Uuid::new_v4()),
                weight: 7,
            },
        ];
        for kind in mutations {
            assert!(kind.is_mutation(), "{}", kind.name());
            let json = serde_json::to_vec(&kind).expect("a kind encodes");
            assert_eq!(
                decode_rest::<AdminKind>(&json).expect("a kind decodes"),
                kind
            );
        }
        for kind in [
            AdminKind::PlanStatus { op: Uuid::new_v4() },
            AdminKind::Plans,
            AdminKind::BackupStatus { op: Uuid::new_v4() },
            AdminKind::Backups,
            AdminKind::RestoreStatus { op: Uuid::new_v4() },
            AdminKind::Recoveries,
            AdminKind::Stats { table: None },
            AdminKind::Stats {
                table: Some("notes".to_string()),
            },
        ] {
            assert!(!kind.is_mutation(), "{}", kind.name());
            let json = serde_json::to_vec(&kind).expect("a kind encodes");
            assert_eq!(
                decode_rest::<AdminKind>(&json).expect("a kind decodes"),
                kind
            );
        }
        // a remove without a replacement decodes from a frame that leaves it out
        let bare: AdminKind =
            serde_json::from_value(serde_json::json!({ "Remove": { "node": node } }))
                .expect("decodes");
        assert_eq!(
            bare,
            AdminKind::Remove {
                node,
                replacement: None
            }
        );
        // a member frame from before F46 reads as a plain member
        let older: TopologyMember = serde_json::from_value(serde_json::json!({
            "node": node, "client": "a", "data": "b", "control": "c", "shards": 1,
            "role": "voter", "health": "up", "incarnation": 1, "shards_failed": []
        }))
        .expect("an older member decodes");
        assert_eq!(older.phase, "member");
        // an answer, applied and refused
        let response = AdminResponse {
            node: NodeId::mint(),
            topology_version: 8,
            outcome: Ok(AdminOutcome::Applied { version: 8 }),
        };
        let body = encode_body(&Uuid::nil(), &response).expect("a response encodes");
        let back: AdminResponse = decode_rest(&body[QUERY_ID_LEN..]).expect("a response decodes");
        assert_eq!(back, response);
        let refused = AdminResponse {
            node: NodeId::mint(),
            topology_version: 8,
            outcome: Err(AdminError::new(
                ErrorCode::StaleVersion,
                "the cluster is at 8",
            )),
        };
        let body = encode_body(&Uuid::nil(), &refused).expect("a refusal encodes");
        let back: AdminResponse = decode_rest(&body[QUERY_ID_LEN..]).expect("a refusal decodes");
        assert_eq!(back, refused);
        assert_eq!(
            back.outcome.as_ref().expect_err("a refusal").code(),
            ErrorCode::StaleVersion
        );
        // a topology
        let frame = TopologyFrame {
            cluster: ClusterId::mint(),
            version: 3,
            leader: Some(NodeId::mint()),
            members: vec![TopologyMember {
                node: NodeId::mint(),
                client: "127.0.0.1:12000".to_string(),
                data: "127.0.0.1:12001".to_string(),
                control: "127.0.0.1:12002".to_string(),
                shards: 4,
                role: "voter".to_string(),
                health: "up".to_string(),
                phase: "member".to_string(),
                state: "up".to_string(),
                incarnation: 2,
                shards_failed: vec![1],
                quarantined: Vec::new(),
                lead_weight: 2,
            }],
            placement: vec![NodeId::mint()],
            desired_rf: 3,
            active_rf: 1,
            write_consistency: "quorum".to_string(),
            read_consistency: "one".to_string(),
            tables: vec![("Row".to_string(), TableId::of("Row"))],
            table_read_policy: vec![("Row".to_string(), "quorum".to_string())],
            configurations: vec![ConfiguredSet {
                tablets: vec![1, 4],
                members: vec![ShardAddr::from(4), ShardAddr::from(2)],
                published_at: 3,
            }],
            moves: vec![MoveSummary {
                op: Uuid::new_v4(),
                tablets: vec![2, 5],
                from: ShardAddr::from(1),
                to: ShardAddr::from(4),
                phase: "learner".to_string(),
            }],
            tombstones: vec![NodeId::mint()],
        };
        let body = encode_body(&Uuid::nil(), &frame).expect("a topology encodes");
        let back: TopologyFrame = decode_rest(&body[QUERY_ID_LEN..]).expect("a topology decodes");
        assert_eq!(back, frame);
        // a frame from before F74 names no lead weight and no tombstones, and still decodes
        let mut old = serde_json::to_value(&frame).expect("a frame is json");
        old.as_object_mut().expect("an object").remove("tombstones");
        old["members"][0]
            .as_object_mut()
            .expect("an object")
            .remove("lead_weight");
        let back: TopologyFrame = serde_json::from_value(old).expect("an old frame decodes");
        assert_eq!(back.members[0].lead_weight, 0);
        assert!(back.tombstones.is_empty());
        // garbage after the id is a decode failure, not a panic
        assert!(decode_rest::<TopologyFrame>(b"not json").is_err());
    }
}
