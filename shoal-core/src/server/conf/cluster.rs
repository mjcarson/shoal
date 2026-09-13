//! The `cluster:` block: what makes a server a member of a cluster rather than a node on its own
//!
//! Absent, the server is standalone and byte for byte what it was before
//! [F37](../../../../docs/src/features/node-identity-control-plane.md): no control thread, no
//! group, no listener, no identity beyond the one the marker mints. Present, it names how the
//! node was created (`bootstrap`), where it can be reached, which core its control thread owns,
//! and the replication policy the bootstrap seeds into the cluster's state.
//!
//! # Local settings against cluster policy
//!
//! [C1](../../../../docs/src/distributed/node-identity.md) separates the two and this block holds
//! both, which is worth being clear about. `advertise`, `port`, `control_port`,
//! `client_advertise`, `control_core` and `control_core_shared` are *this node's*, and every
//! node's differ. `control_voters`, `replication_factor`, `write_consistency`,
//! `read_consistency`, `failure_detector`, `primary_failover_after`, `auto_remove_after` and
//! `admins` are the *cluster's*: the node that bootstraps writes them into the control state as
//! [`BootstrapPolicy`], and after that a change is a versioned admin operation rather than an
//! edit to a file. A joiner's copy of them is ignored, so no node can weaken the cluster's quorum
//! by editing its own YAML. At M1 there is no joiner, so the whole block is read by the one node
//! there is, and the policy is recorded and reported rather than enforced.
//!
//! # What is refused rather than ignored
//!
//! A setting this build does not act on yet is refused at startup by [`Cluster::validate`],
//! naming the milestone that delivers it. That is the difference between a configuration that
//! describes the server and one that describes a wish: a file that says `seeds:` and starts a
//! node that never contacts them has lied to whoever wrote it.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::super::errors::ShoalError;
use super::super::ServerError;
use crate::shared::identity::NodeId;
use crate::utils;

/// The certificate a node presents to its peers and the authority it checks theirs against
///
/// The proto crate's shape, re-exported so that a config names one type and the peer lanes build
/// their TLS from the same struct they were configured with.
pub use crate::shared::tls::PeerTlsOptions as PeerTls;

/// The default data peer port
fn default_peer_port() -> u16 {
    12001
}

/// The default control listener port
fn default_control_port() -> u16 {
    12002
}

/// The default number of control voters
fn default_control_voters() -> u32 {
    3
}

/// The default replication factor
fn default_replication_factor() -> u32 {
    3
}

/// The default write consistency
fn default_write_consistency() -> Consistency {
    Consistency::Quorum
}

/// The default read consistency
fn default_read_consistency() -> Consistency {
    Consistency::One
}

/// The default base data election timeout
fn default_primary_failover_after() -> DurationSpec {
    DurationSpec::from(Duration::from_secs(5))
}

/// The default grace before a Down node is removed automatically
fn default_auto_remove_after() -> Option<DurationSpec> {
    Some(DurationSpec::from(Duration::from_secs(30 * 60)))
}

/// The default failure detector interval
fn default_detector_interval_ms() -> u64 {
    500
}

/// The default phi accrual threshold
fn default_phi_threshold() -> f64 {
    8.0
}

/// The default number of report arrivals the detector keeps per node
fn default_detector_window() -> usize {
    100
}

/// The default number of arrivals the detector needs before it will suspect anybody
fn default_detector_min_samples() -> usize {
    5
}

/// A consistency level a write waits for or a read is served at
///
/// The two the bootstrap policy names. `Quorum` for writes is P3's durable quorum; `One` for
/// reads is the local read the contract permits and M5 gives a name to. Neither is enforced at
/// M1, where every read and write is local; they are recorded so that the policy a cluster was
/// created with is on record before anything acts on it.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// One replica: the local one for a read, any acknowledgement for a write
    One,
    /// A majority of the replicas
    Quorum,
    /// Every replica
    All,
}

impl Consistency {
    /// The lowercase name of this level, which is what the benchmark artifact records
    pub fn as_str(&self) -> &'static str {
        match self {
            Consistency::One => "one",
            Consistency::Quorum => "quorum",
            Consistency::All => "all",
        }
    }
}

/// A duration written the way a person writes one: `500ms`, `5s`, `30m`, `2h`
///
/// Its own type rather than a `u64` of milliseconds so that the file reads the way C1's block
/// is written, and so that a bare number - which could be any unit - is refused. No dependency:
/// the grammar is a number and one of four suffixes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurationSpec(pub Duration);

impl DurationSpec {
    /// Parse a duration from its written form
    ///
    /// # Arguments
    ///
    /// * `text` - The written form, a whole number followed by `ms`, `s`, `m` or `h`
    ///
    /// # Errors
    ///
    /// Refuses anything without a suffix, with an unknown suffix, or without a whole number.
    pub fn parse(text: &str) -> Result<Self, String> {
        // the suffix is whatever is not a digit, and it decides the unit
        let text = text.trim();
        let split = text
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| format!("{text:?} has no unit; write it as 500ms, 5s, 30m or 2h"))?;
        let (number, unit) = text.split_at(split);
        let number: u64 = number
            .parse()
            .map_err(|_| format!("{text:?} does not start with a whole number"))?;
        // one of four units, and nothing else
        let duration = match unit {
            "ms" => Duration::from_millis(number),
            "s" => Duration::from_secs(number),
            "m" => Duration::from_secs(number * 60),
            "h" => Duration::from_secs(number * 60 * 60),
            other => return Err(format!("{text:?} has unit {other:?}; use ms, s, m or h")),
        };
        Ok(DurationSpec(duration))
    }

    /// The duration this spells
    pub fn duration(&self) -> Duration {
        self.0
    }
}

impl From<Duration> for DurationSpec {
    /// Wrap a duration
    fn from(duration: Duration) -> Self {
        DurationSpec(duration)
    }
}

impl std::fmt::Display for DurationSpec {
    /// Write the duration back in the largest unit that divides it exactly
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let millis = self.0.as_millis();
        // largest unit first, so thirty minutes reads as 30m and not 1800000ms
        for (unit, suffix) in [(3_600_000, "h"), (60_000, "m"), (1_000, "s")] {
            if millis > 0 && millis % unit == 0 {
                return write!(f, "{}{suffix}", millis / unit);
            }
        }
        write!(f, "{millis}ms")
    }
}

impl Serialize for DurationSpec {
    /// Serialize as the written form
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    /// Deserialize from the written form
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        DurationSpec::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// The failure detector's settings
///
/// Phi accrual, the shape [C3](../../../../docs/src/distributed/membership.md) describes and
/// [F39](../../../../docs/src/features/membership.md) built: every member sends the control
/// leader a status report every `interval_ms`, the leader keeps the last `window` arrival
/// intervals per member, and once it has `min_samples` of them it computes how unlikely the
/// current silence is against that distribution; past `phi_threshold` it commits `Down`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FailureDetector {
    /// How often a member reports to the leader, in milliseconds
    #[serde(default = "default_detector_interval_ms")]
    pub interval_ms: u64,
    /// The suspicion level at which a node is called Down
    #[serde(default = "default_phi_threshold")]
    pub phi_threshold: f64,
    /// How many arrival intervals the leader keeps per member
    #[serde(default = "default_detector_window")]
    pub window: usize,
    /// How many arrivals the leader needs from a member before it will suspect it
    ///
    /// A fresh leader starts with none, so no verdict is reached on stale evidence.
    #[serde(default = "default_detector_min_samples")]
    pub min_samples: usize,
}

impl Default for FailureDetector {
    /// The defaults C1's block writes down
    fn default() -> Self {
        FailureDetector {
            interval_ms: default_detector_interval_ms(),
            phi_threshold: default_phi_threshold(),
            window: default_detector_window(),
            min_samples: default_detector_min_samples(),
        }
    }
}

/// Where this node dials one member, if not where that member advertises itself
///
/// For a network where a member's advertised address is not the one this node can reach it at
/// - a split horizon, a NAT, or a test's fault proxy standing in one direction between two
/// nodes ([F39](../../../../docs/src/features/membership.md)). Either lane may be overridden
/// alone; an absent one dials what the member advertises.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct DialOverride {
    /// Where to dial that member's control lane
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<String>,
    /// Where to dial that member's data and bulk lanes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// The byte bounds and timers of the peer lanes
///
/// Every queue between this node and a peer is bounded in bytes, and every wait has a deadline.
/// The bounds are what make one slow peer's cost a number rather than the node's whole memory;
/// the deadline is what turns a peer that never answers into an answer the client can act on.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Transport {
    /// The most bytes queued to one peer on the data lane before forwards are shed
    #[serde(
        default = "default_data_queue_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub data_queue_bytes: usize,
    /// The most bytes queued to one peer on the control lane before RPCs are refused
    #[serde(
        default = "default_control_queue_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub control_queue_bytes: usize,
    /// The most bytes queued to one peer on the bulk lane before chunks are shed
    #[serde(
        default = "default_bulk_queue_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub bulk_queue_bytes: usize,
    /// The most bytes queued to one peer on the replication lane before RPCs are refused
    ///
    /// A refused append is one openraft retries; the bound is what keeps a follower that is
    /// not reading from holding a shard's memory ([F40](../../../../docs/src/features/replication.md)).
    #[serde(
        default = "default_replication_queue_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub replication_queue_bytes: usize,
    /// The most forwarded bytes one peer connection may have in hand, unanswered
    ///
    /// Past this the connection stops reading, so the peer's own queue fills and sheds rather
    /// than this node's memory growing with what it has not answered yet.
    #[serde(
        default = "default_inflight_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub inflight_bytes: usize,
    /// How long a forwarded query is waited on before its outcome is reported unknown
    #[serde(default = "default_forward_timeout")]
    pub forward_timeout: DurationSpec,
    /// The shortest wait before a lost link is dialled again
    #[serde(default = "default_reconnect_min")]
    pub reconnect_min: DurationSpec,
    /// The longest wait before a lost link is dialled again, which the backoff grows to
    #[serde(default = "default_reconnect_max")]
    pub reconnect_max: DurationSpec,
    /// How long a peer has to finish its handshake
    #[serde(default = "default_handshake_timeout")]
    pub handshake_timeout: DurationSpec,
    /// How often the control thread pings every placed peer
    #[serde(default = "default_ping_interval")]
    pub ping_interval: DurationSpec,
}

/// The default data lane queue bound
fn default_data_queue_bytes() -> usize {
    64 * 1024 * 1024
}

/// The default control lane queue bound
fn default_control_queue_bytes() -> usize {
    8 * 1024 * 1024
}

/// The default bulk lane queue bound
fn default_bulk_queue_bytes() -> usize {
    64 * 1024 * 1024
}

/// The default replication lane queue bound
fn default_replication_queue_bytes() -> usize {
    64 * 1024 * 1024
}

/// The default in-flight bound per accepted connection
fn default_inflight_bytes() -> usize {
    64 * 1024 * 1024
}

/// The default forward deadline
fn default_forward_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(5))
}

/// The default shortest reconnect wait
fn default_reconnect_min() -> DurationSpec {
    DurationSpec(Duration::from_millis(100))
}

/// The default longest reconnect wait
fn default_reconnect_max() -> DurationSpec {
    DurationSpec(Duration::from_secs(5))
}

/// The default handshake deadline
fn default_handshake_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(10))
}

/// The default ping interval
fn default_ping_interval() -> DurationSpec {
    DurationSpec(Duration::from_secs(1))
}

impl Default for Transport {
    /// The bounds the configuration page writes down
    fn default() -> Self {
        Transport {
            data_queue_bytes: default_data_queue_bytes(),
            control_queue_bytes: default_control_queue_bytes(),
            bulk_queue_bytes: default_bulk_queue_bytes(),
            replication_queue_bytes: default_replication_queue_bytes(),
            inflight_bytes: default_inflight_bytes(),
            forward_timeout: default_forward_timeout(),
            reconnect_min: default_reconnect_min(),
            reconnect_max: default_reconnect_max(),
            handshake_timeout: default_handshake_timeout(),
            ping_interval: default_ping_interval(),
        }
    }
}

/// The default proposal deadline
fn default_write_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(5))
}

/// The default bound on bytes proposed and not yet answered, per shard
fn default_pending_bytes() -> usize {
    64 * 1024 * 1024
}

/// The default WAL segment size
fn default_segment_bytes() -> u64 {
    10 * 1024 * 1024
}

/// The default number of entries between snapshots
fn default_checkpoint_entries() -> u64 {
    1024
}

/// The default number of entries kept behind a snapshot
fn default_retained_entries() -> u64 {
    10_000
}

/// The default bound on the WAL's in-memory tail, per shard
fn default_log_cache_bytes() -> usize {
    16 * 1024 * 1024
}

/// The default bound on every volatile group's log together, per shard
fn default_volatile_log_bytes() -> usize {
    256 * 1024 * 1024
}

/// The default size of one chunk of a snapshot stream
fn default_snapshot_chunk_bytes() -> usize {
    1024 * 1024
}

/// The default deadline for one snapshot transfer
fn default_snapshot_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(300))
}

/// The default bound on partial snapshot bytes held on disk, per shard
fn default_install_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

/// The default bound on sealed WAL bytes a slow member may pin, per shard
fn default_retained_bytes() -> u64 {
    1024 * 1024 * 1024
}

/// The tablet groups' timers and bounds, which are this node's and not the cluster's
///
/// Every field is node-local: a deadline, a byte bound, a segment size. None of them enters
/// the [`BootstrapPolicy`], since none of them is something two nodes have to agree about
/// ([F40](../../../../docs/src/features/replication.md)). The election and heartbeat timers
/// are derived from the policy's `primary_failover_after`, which two nodes do have to agree
/// about, and are not here.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Replication {
    /// How long a proposal waits for the group before its outcome is reported unknown
    ///
    /// Has to be no longer than `transport.forward_timeout`, since the node that forwarded a
    /// write answers its client unknown at that deadline and drops a later answer.
    #[serde(default = "default_write_timeout")]
    pub write_timeout: DurationSpec,
    /// The most bytes a shard holds proposed and unanswered before it sheds a write
    #[serde(
        default = "default_pending_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub pending_bytes: usize,
    /// How large a WAL segment grows before the next append opens a new one
    #[serde(
        default = "default_segment_bytes",
        deserialize_with = "utils::deserialize_byte_size_u64"
    )]
    pub segment_bytes: u64,
    /// How many entries a group commits between snapshots at its checkpoint
    #[serde(default = "default_checkpoint_entries")]
    pub checkpoint_entries: u64,
    /// How many entries a group keeps behind its snapshot, for a slow member to catch up from
    #[serde(default = "default_retained_entries")]
    pub retained_entries: u64,
    /// How many bytes of entries the WAL keeps in memory past its durable tail
    #[serde(
        default = "default_log_cache_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub log_cache_bytes: usize,
    /// How many bytes every volatile group's log may hold together before proposals are shed
    #[serde(
        default = "default_volatile_log_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub volatile_log_bytes: usize,
    /// How many bytes of a snapshot one chunk on the bulk lane carries
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    #[serde(
        default = "default_snapshot_chunk_bytes",
        deserialize_with = "utils::deserialize_byte_size"
    )]
    pub snapshot_chunk_bytes: usize,
    /// How long one snapshot transfer may take before the sender gives up and tries again
    #[serde(default = "default_snapshot_timeout")]
    pub snapshot_timeout: DurationSpec,
    /// The most bytes of partial snapshots a shard holds on disk before it refuses a new one
    #[serde(
        default = "default_install_bytes",
        deserialize_with = "utils::deserialize_byte_size_u64"
    )]
    pub install_bytes: u64,
    /// The most sealed WAL bytes a shard keeps for slow members before it forces a snapshot
    /// and a purge, so a member behind the purge point catches up by snapshot rather than
    /// pinning the log
    #[serde(
        default = "default_retained_bytes",
        deserialize_with = "utils::deserialize_byte_size_u64"
    )]
    pub retained_bytes: u64,
}

impl Default for Replication {
    /// The defaults the configuration page writes down
    fn default() -> Self {
        Replication {
            write_timeout: default_write_timeout(),
            pending_bytes: default_pending_bytes(),
            segment_bytes: default_segment_bytes(),
            checkpoint_entries: default_checkpoint_entries(),
            retained_entries: default_retained_entries(),
            log_cache_bytes: default_log_cache_bytes(),
            volatile_log_bytes: default_volatile_log_bytes(),
            snapshot_chunk_bytes: default_snapshot_chunk_bytes(),
            snapshot_timeout: default_snapshot_timeout(),
            install_bytes: default_install_bytes(),
            retained_bytes: default_retained_bytes(),
        }
    }
}

/// The replication policy a bootstrap seeds into the cluster
///
/// Everything in the `cluster:` block that belongs to the cluster rather than to one node, in
/// the shape the control state holds it. Built once, at bootstrap, from the bootstrapping node's
/// configuration; a node that joins later adopts what is committed and its own copy of these
/// fields is ignored.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BootstrapPolicy {
    /// How many nodes vote in the control group
    pub control_voters: u32,
    /// How many replicas each tablet is meant to have
    pub replication_factor: u32,
    /// What a write waits for before it is acknowledged
    pub write_consistency: Consistency,
    /// What a read is served at
    pub read_consistency: Consistency,
    /// How suspicion of a node is accrued
    pub failure_detector: FailureDetector,
    /// The base data election timeout
    pub primary_failover_after: DurationSpec,
    /// How long a Down node is kept before it is removed, or none to never remove one
    pub auto_remove_after: Option<DurationSpec>,
    /// The principals allowed to change this policy
    pub admins: Vec<String>,
}

/// The cluster settings
///
/// # Invariants
///
/// **Unknown fields are refused**, the way every other section of this config refuses them, and
/// here a misspelling would be a node with a different replication policy than the one written
/// down, which is a quieter failure than most.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Whether this node creates the cluster
    ///
    /// The one explicit act that mints a cluster identity, taken once on an empty directory.
    /// Left `true` on an established directory it does nothing: the cluster already exists and a
    /// restart keeps it. Never set on a node that is meant to join one.
    #[serde(default)]
    pub bootstrap: bool,
    /// The control addresses of members to join through
    ///
    /// Discovery addresses, not identities: a seed's identity is what it proves in the handshake,
    /// and the cluster a joiner adopts is the one the seed proves. A node that bootstraps has
    /// nothing to discover and is refused a seed list; a node that has been admitted keeps its
    /// list and never needs it again, since its peers are the committed members.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// The address peers reach this node at
    ///
    /// Defaults to `networking.interface`, unless that is unspecified (`0.0.0.0` or `::`), in
    /// which case it has to be given: a peer cannot be told to connect to every interface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise: Option<String>,
    /// The port the data peer endpoint would listen on
    #[serde(default = "default_peer_port")]
    pub port: u16,
    /// The port the control listener would listen on
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    /// The client address this node advertises in the topology, if it differs from the bound one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_advertise: Option<String>,
    /// The cpu the control thread is pinned to
    ///
    /// Zero by default, which is the coordinator cpu the shards already leave alone. It is
    /// checked against the process's affinity, and its whole physical core is kept away from the
    /// shards unless `control_core_shared` says otherwise.
    #[serde(default)]
    pub control_core: usize,
    /// Whether the control thread may share its physical core with a shard
    ///
    /// For a machine too small to give the control plane a core of its own. The sharing is
    /// recorded in the topology view and in every benchmark artifact, so a number taken on a
    /// shared core is never mistaken for one taken on an isolated one.
    #[serde(default)]
    pub control_core_shared: bool,
    /// How many nodes vote in the control group
    #[serde(default = "default_control_voters")]
    pub control_voters: u32,
    /// How many replicas each tablet is meant to have
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    /// What a write waits for before it is acknowledged
    #[serde(default = "default_write_consistency")]
    pub write_consistency: Consistency,
    /// What a read is served at
    #[serde(default = "default_read_consistency")]
    pub read_consistency: Consistency,
    /// How suspicion of a node is accrued
    #[serde(default)]
    pub failure_detector: FailureDetector,
    /// The base data election timeout
    #[serde(default = "default_primary_failover_after")]
    pub primary_failover_after: DurationSpec,
    /// How long a Down node is kept before it is removed, or `null` to never remove one
    ///
    /// The serde shape matters: absent is the default of thirty minutes, and an explicit `null`
    /// is a deliberate never.
    #[serde(default = "default_auto_remove_after")]
    pub auto_remove_after: Option<DurationSpec>,
    /// The principals allowed to change the cluster's policy
    #[serde(default)]
    pub admins: Vec<String>,
    /// The certificate this node presents to its peers, and the authority it checks theirs against
    ///
    /// Present, every lane is mutual TLS 1.3 handed to the kernel, exactly as `networking.tls`
    /// does for clients; absent, the lanes are plaintext and peer identity is trusted inside
    /// whatever boundary the deployment draws around them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<PeerTls>,
    /// The byte bounds and timers of the peer lanes
    #[serde(default)]
    pub transport: Transport,
    /// The tablet groups' timers and bounds, which are this node's alone
    #[serde(default)]
    pub replication: Replication,
    /// Where this node dials particular members, keyed by their identity, when not where they advertise
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub dial: std::collections::BTreeMap<NodeId, DialOverride>,
}

impl Default for Cluster {
    /// The defaults C1's block writes down, with `bootstrap` off
    fn default() -> Self {
        Cluster {
            bootstrap: false,
            seeds: Vec::new(),
            advertise: None,
            port: default_peer_port(),
            control_port: default_control_port(),
            client_advertise: None,
            control_core: 0,
            control_core_shared: false,
            control_voters: default_control_voters(),
            replication_factor: default_replication_factor(),
            write_consistency: default_write_consistency(),
            read_consistency: default_read_consistency(),
            failure_detector: FailureDetector::default(),
            primary_failover_after: default_primary_failover_after(),
            auto_remove_after: default_auto_remove_after(),
            admins: Vec::new(),
            tls: None,
            transport: Transport::default(),
            replication: Replication::default(),
            dial: std::collections::BTreeMap::new(),
        }
    }
}

impl Cluster {
    /// Create the cluster this node is a member of
    pub fn bootstrap(mut self, bootstrap: bool) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    /// Set the control addresses of members to join through
    pub fn seeds(mut self, seeds: Vec<String>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Dial one member somewhere other than where it advertises itself
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `control` - Where to dial its control lane, or none to dial what it advertises
    /// * `data` - Where to dial its data and bulk lanes, or none to dial what it advertises
    pub fn dial(mut self, node: NodeId, control: Option<String>, data: Option<String>) -> Self {
        self.dial.insert(node, DialOverride { control, data });
        self
    }

    /// Set the failure detector's report interval
    pub fn detector_interval_ms(mut self, interval_ms: u64) -> Self {
        self.failure_detector.interval_ms = interval_ms;
        self
    }

    /// Set the address peers reach this node at
    pub fn advertise(mut self, advertise: impl Into<String>) -> Self {
        self.advertise = Some(advertise.into());
        self
    }

    /// Set the data peer port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the control listener port
    pub fn control_port(mut self, control_port: u16) -> Self {
        self.control_port = control_port;
        self
    }

    /// Set the client address this node advertises
    pub fn client_advertise(mut self, client_advertise: impl Into<String>) -> Self {
        self.client_advertise = Some(client_advertise.into());
        self
    }

    /// Set the cpu the control thread is pinned to
    pub fn control_core(mut self, control_core: usize) -> Self {
        self.control_core = control_core;
        self
    }

    /// Allow the control thread to share its physical core with a shard
    pub fn control_core_shared(mut self, shared: bool) -> Self {
        self.control_core_shared = shared;
        self
    }

    /// Set how many nodes vote in the control group
    pub fn control_voters(mut self, control_voters: u32) -> Self {
        self.control_voters = control_voters;
        self
    }

    /// Set how many replicas each tablet is meant to have
    pub fn replication_factor(mut self, replication_factor: u32) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Set what a write waits for
    pub fn write_consistency(mut self, consistency: Consistency) -> Self {
        self.write_consistency = consistency;
        self
    }

    /// Set what a read is served at
    pub fn read_consistency(mut self, consistency: Consistency) -> Self {
        self.read_consistency = consistency;
        self
    }

    /// Set how long a Down node is kept, or `None` to never remove one
    pub fn auto_remove_after(mut self, grace: Option<Duration>) -> Self {
        self.auto_remove_after = grace.map(DurationSpec::from);
        self
    }

    /// Set the certificate this node presents to its peers
    pub fn tls(mut self, tls: PeerTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the byte bounds and timers of the peer lanes
    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    /// Set the tablet groups' timers and bounds
    pub fn replication(mut self, replication: Replication) -> Self {
        self.replication = replication;
        self
    }

    /// Set the base data election timeout, which the groups' heartbeat is a tenth of
    pub fn primary_failover_after(mut self, after: Duration) -> Self {
        self.primary_failover_after = DurationSpec::from(after);
        self
    }

    /// The policy this configuration would seed into a cluster it bootstraps
    pub fn policy(&self) -> BootstrapPolicy {
        BootstrapPolicy {
            control_voters: self.control_voters,
            replication_factor: self.replication_factor,
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
            failure_detector: self.failure_detector.clone(),
            primary_failover_after: self.primary_failover_after,
            auto_remove_after: self.auto_remove_after,
            admins: self.admins.clone(),
        }
    }

    /// The address peers are told to reach this node at
    ///
    /// # Arguments
    ///
    /// * `interface` - The interface the client listener binds, which is the fallback
    ///
    /// # Errors
    ///
    /// Refuses when neither `advertise` nor the interface names a reachable address.
    pub fn advertised(&self, interface: &str) -> Result<String, ServerError> {
        // an explicit advertise address wins
        if let Some(advertise) = &self.advertise {
            return Ok(advertise.clone());
        }
        // an unspecified interface is every interface, which is not an address a peer can dial
        if matches!(interface, "0.0.0.0" | "::" | "[::]") {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "cluster.advertise is required when networking.interface is {interface}: a peer \
                 cannot be told to connect to every interface"
            ))));
        }
        Ok(interface.to_string())
    }

    /// Refuse what this build does not implement, naming the milestone that does
    ///
    /// # Arguments
    ///
    /// * `interface` - The interface the client listener binds, for the advertise check
    ///
    /// # Errors
    ///
    /// Every refusal is a [`ShoalError::NotImplemented`] or a [`ShoalError::InvalidConfig`],
    /// and the first one found is returned.
    pub fn validate(&self, interface: &str, max_frame_bytes: u32) -> Result<(), ServerError> {
        // a snapshot chunk rides one frame, so it has to fit one with its heads
        // ([F43](../../../../docs/src/features/node-recovery.md))
        if self.replication.snapshot_chunk_bytes + 4096 > max_frame_bytes as usize {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "cluster.replication.snapshot_chunk_bytes is {} bytes, which does not fit networking.max_frame_bytes ({max_frame_bytes}) with its heads",
                self.replication.snapshot_chunk_bytes
            ))));
        }
        // the strong read level is Quorum; there is no read that waits on every replica
        // ([C6](../../../../docs/src/distributed/reads.md))
        if self.read_consistency == Consistency::All {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.read_consistency: All is not a read level anything serves; C6 names One                  and Quorum, and Quorum is the strong one"
                    .to_string(),
            )));
        }
        // a node bootstraps or joins; one that has a cluster to create has nothing to discover
        if !self.seeds.is_empty() && self.bootstrap {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "a cluster: block names both bootstrap: true and a seeds: list; a node creates a \
                 cluster or joins one, not both"
                    .to_string(),
            )));
        }
        // every seed has to be an address something can dial
        for seed in &self.seeds {
            if seed.parse::<std::net::SocketAddr>().is_err() {
                return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "cluster.seeds names {seed:?}, which is not a control address of the form \
                     host:port"
                ))));
            }
        }
        // every dial override has to be one too
        for (node, target) in &self.dial {
            for (lane, addr) in [("control", &target.control), ("data", &target.data)] {
                if let Some(addr) = addr {
                    if addr.parse::<std::net::SocketAddr>().is_err() {
                        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                            "cluster.dial names {addr:?} for the {lane} lane of {node}, which \
                             is not an address of the form host:port"
                        ))));
                    }
                }
            }
        }
        // the detector cannot suspect anybody without samples, and cannot keep fewer than it needs
        if self.failure_detector.min_samples == 0
            || self.failure_detector.window < self.failure_detector.min_samples
        {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.failure_detector needs min_samples of at least one and a window no \
                 smaller than it"
                    .to_string(),
            )));
        }
        // a cluster node that neither bootstraps nor joins is a node that would run a group of
        // one with no way for anything to ever find it
        if self.seeds.is_empty() && !self.bootstrap {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "a cluster: block needs bootstrap: true or a seeds: list; a node that does \
                 neither belongs to nothing"
                    .to_string(),
            )));
        }
        // a certificate that cannot be read is found now, before a peer dials in
        if let Some(tls) = &self.tls {
            crate::shared::tls::peer_server_config(tls)?;
            crate::shared::tls::peer_client_config(tls)?;
        }
        // the reconnect backoff has to be a range
        if self.transport.reconnect_min.duration() > self.transport.reconnect_max.duration() {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.transport.reconnect_min is longer than reconnect_max".to_string(),
            )));
        }
        // a control group votes with an odd, small number of members
        if !matches!(self.control_voters, 1 | 3 | 5) {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "cluster.control_voters is {}; it has to be 1, 3 or 5",
                self.control_voters
            ))));
        }
        // a replication factor of zero is a tablet with no copies
        if self.replication_factor == 0 {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.replication_factor is 0; a tablet needs at least one replica".to_string(),
            )));
        }
        // a write acknowledged by one replica is one a later leader may roll back, and the
        // accepted-or-pending API that would make that legible is not built: refused, as C5
        // says a `One` write is in v1 (F40)
        if self.write_consistency == Consistency::One {
            return Err(ServerError::Shoal(ShoalError::NotImplemented {
                setting: "cluster.write_consistency: One (C5 refuses a One write in v1: a durable quorum cannot be built from an acknowledgement that precedes fdatasync)".to_string(),
                milestone: "a distinct accepted/pending write API",
            }));
        }
        // the node that forwarded a write answers its client at the forward deadline, so a
        // proposal that waits longer than that answers nobody
        if self.replication.write_timeout.duration() > self.transport.forward_timeout.duration() {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.replication.write_timeout is longer than cluster.transport.forward_timeout; a proposal that outlives the forward deadline answers nobody".to_string(),
            )));
        }
        // a chunk has to fit the bulk queue, or a stream waits for room that never comes
        if self.replication.snapshot_chunk_bytes + 4096 > self.transport.bulk_queue_bytes {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.replication.snapshot_chunk_bytes does not fit cluster.transport.bulk_queue_bytes with its heads".to_string(),
            )));
        }
        // a snapshot transfer that gives up before a write would is one that never completes
        // under load
        if self.replication.snapshot_timeout.duration() < self.replication.write_timeout.duration() {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.replication.snapshot_timeout is shorter than write_timeout".to_string(),
            )));
        }
        // the retention budget has to hold the active segment and one sealed one, or every
        // sweep forces a snapshot
        if self.replication.retained_bytes < 2 * self.replication.segment_bytes {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.replication.retained_bytes is under twice segment_bytes; the budget cannot hold two segments".to_string(),
            )));
        }
        // the timers the groups derive from the failover base have to be timers at all
        if self.primary_failover_after.duration() < Duration::from_millis(100) {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(
                "cluster.primary_failover_after is under 100ms; the groups' heartbeat is a tenth of it".to_string(),
            )));
        }
        // the advertised address has to be one, whether or not anything dials it yet
        self.advertised(interface)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Cluster, Consistency, DurationSpec};
    use std::time::Duration;

    /// A duration parses from each unit and refuses what is not one
    #[test]
    fn a_duration_parses_its_units() {
        assert_eq!(DurationSpec::parse("500ms").unwrap().duration(), Duration::from_millis(500));
        assert_eq!(DurationSpec::parse("5s").unwrap().duration(), Duration::from_secs(5));
        assert_eq!(DurationSpec::parse("30m").unwrap().duration(), Duration::from_secs(1800));
        assert_eq!(DurationSpec::parse("2h").unwrap().duration(), Duration::from_secs(7200));
        assert!(DurationSpec::parse("5").is_err(), "a bare number has no unit");
        assert!(DurationSpec::parse("5d").is_err(), "days are not a unit");
        assert!(DurationSpec::parse("ms").is_err(), "a unit with no number");
    }

    /// A duration is written back in the largest unit that fits, and round trips
    #[test]
    fn a_duration_round_trips() {
        for text in ["500ms", "5s", "30m", "2h", "90s", "1500ms"] {
            let parsed = DurationSpec::parse(text).unwrap();
            let again = DurationSpec::parse(&parsed.to_string()).unwrap();
            assert_eq!(parsed, again, "{text} did not round trip");
        }
        assert_eq!(DurationSpec::parse("30m").unwrap().to_string(), "30m");
        assert_eq!(DurationSpec::parse("1800s").unwrap().to_string(), "30m");
    }

    /// The defaults are the ones C1 writes down
    #[test]
    fn the_defaults_are_c1s() {
        let cluster = Cluster::default();
        assert_eq!(cluster.control_voters, 3);
        assert_eq!(cluster.replication_factor, 3);
        assert_eq!(cluster.write_consistency, Consistency::Quorum);
        assert_eq!(cluster.read_consistency, Consistency::One);
        assert_eq!(cluster.port, 12001);
        assert_eq!(cluster.control_port, 12002);
        assert_eq!(cluster.control_core, 0);
        assert!(!cluster.control_core_shared);
        assert_eq!(
            cluster.auto_remove_after.map(|grace| grace.duration()),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(cluster.primary_failover_after.duration(), Duration::from_secs(5));
        assert_eq!(cluster.failure_detector.interval_ms, 500);
        assert!((cluster.failure_detector.phi_threshold - 8.0).abs() < f64::EPSILON);
        assert_eq!(cluster.failure_detector.window, 100);
        assert_eq!(cluster.failure_detector.min_samples, 5);
        assert!(cluster.dial.is_empty());
        // the transport bounds and timers are the ones the configuration page writes down
        assert_eq!(cluster.transport.data_queue_bytes, 64 * 1024 * 1024);
        assert_eq!(cluster.transport.control_queue_bytes, 8 * 1024 * 1024);
        assert_eq!(cluster.transport.bulk_queue_bytes, 64 * 1024 * 1024);
        assert_eq!(cluster.transport.forward_timeout.duration(), Duration::from_secs(5));
        assert_eq!(cluster.transport.reconnect_min.duration(), Duration::from_millis(100));
        assert_eq!(cluster.transport.reconnect_max.duration(), Duration::from_secs(5));
    }

    /// What this build does not implement is refused by name, and what it does is accepted
    #[test]
    fn validation_refuses_what_is_not_built() {
        // a bootstrapping node on a real interface is what runs
        Cluster::default().bootstrap(true).validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).expect("a bootstrap was refused");
        // a joiner names the control addresses it discovers the cluster through
        Cluster::default()
            .seeds(vec!["10.0.0.1:12002".to_string()])
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a joiner was refused");
        // and a seed has to be an address
        let error = Cluster::default()
            .seeds(vec!["seed-one".to_string()])
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a seed that is not an address was accepted");
        assert!(format!("{error}").contains("host:port"), "{error}");
        // a node creates a cluster or joins one, not both
        let error = Cluster::default()
            .bootstrap(true)
            .seeds(vec!["10.0.0.1:12002".to_string()])
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a bootstrapper with seeds started");
        assert!(format!("{error}").contains("not both"), "{error}");
        // a node that does neither is nothing
        assert!(Cluster::default().validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).is_err());
        // the strong read level is Quorum; All is refused naming C6 (F41)
        let error = Cluster::default()
            .bootstrap(true)
            .read_consistency(Consistency::All)
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("read_consistency All was accepted");
        assert!(format!("{error}").contains("C6"), "{error}");
        Cluster::default()
            .bootstrap(true)
            .read_consistency(Consistency::Quorum)
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("read_consistency Quorum was refused");
        // a dial override has to be an address, and one that is parses
        let node = super::NodeId::mint();
        assert!(Cluster::default()
            .bootstrap(true)
            .dial(node, Some("nowhere".to_string()), None)
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .is_err());
        Cluster::default()
            .bootstrap(true)
            .dial(node, Some("127.0.0.1:1".to_string()), None)
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a dial override was refused");
        // the detector needs samples before it can suspect anybody
        let mut no_samples = Cluster::default().bootstrap(true);
        no_samples.failure_detector.min_samples = 0;
        assert!(no_samples.validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).is_err());
        // peer tls is accepted now, but a certificate that cannot be read is refused as it is read
        let mut with_missing_tls = Cluster::default().bootstrap(true);
        with_missing_tls.tls = Some(super::PeerTls {
            cert: "/does/not/exist/cert.pem".into(),
            key: "/does/not/exist/key.pem".into(),
            ca: "/does/not/exist/ca.pem".into(),
        });
        assert!(
            with_missing_tls.validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).is_err(),
            "an unreadable certificate was accepted"
        );
        // an even voter count is not a quorum anyone wants
        assert!(Cluster::default().bootstrap(true).control_voters(2).validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).is_err());
        // an unspecified interface with nothing advertised is not an address
        assert!(Cluster::default().bootstrap(true).validate("0.0.0.0", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).is_err());
        Cluster::default()
            .bootstrap(true)
            .advertise("10.0.0.2")
            .validate("0.0.0.0", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("an advertised address on 0.0.0.0 was refused");
        // a write acknowledged by one replica is refused by name until there is an API for it
        // ([F40](../../../../docs/src/features/replication.md))
        let error = Cluster::default()
            .bootstrap(true)
            .write_consistency(Consistency::One)
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a One write policy was accepted");
        assert!(format!("{error}").contains("C5"), "{error}");
        // a proposal that outlives the forward deadline answers nobody
        let mut long_write = Cluster::default().bootstrap(true);
        long_write.replication.write_timeout = DurationSpec(Duration::from_secs(6));
        let error = long_write.validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES).expect_err("a write timeout past the forward timeout was accepted");
        assert!(format!("{error}").contains("forward_timeout"), "{error}");
        // and the groups' timers derive from the failover base, which has to be a timer
        let error = Cluster::default()
            .bootstrap(true)
            .primary_failover_after(Duration::from_millis(50))
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a failover base under 100ms was accepted");
        assert!(format!("{error}").contains("heartbeat"), "{error}");
        Cluster::default()
            .bootstrap(true)
            .primary_failover_after(Duration::from_millis(100))
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a failover base of 100ms was refused");
        // the snapshot settings have bounds of their own ([F43](../../../../docs/src/features/node-recovery.md))
        let mut big_chunk = Cluster::default().bootstrap(true);
        big_chunk.replication.snapshot_chunk_bytes = 1024 * 1024;
        let error = big_chunk.validate("127.0.0.1", 64 * 1024).expect_err("a chunk larger than a frame was accepted");
        assert!(format!("{error}").contains("snapshot_chunk_bytes"), "{error}");
        let mut short_transfer = Cluster::default().bootstrap(true);
        short_transfer.replication.snapshot_timeout = DurationSpec(Duration::from_secs(1));
        let error = short_transfer
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a snapshot timeout under the write timeout was accepted");
        assert!(format!("{error}").contains("snapshot_timeout"), "{error}");
        let mut tight_budget = Cluster::default().bootstrap(true);
        tight_budget.replication.retained_bytes = tight_budget.replication.segment_bytes;
        let error = tight_budget
            .validate("127.0.0.1", crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect_err("a retention budget under two segments was accepted");
        assert!(format!("{error}").contains("retained_bytes"), "{error}");
    }

    /// The replication block's defaults are the documented ones, and every field parses
    #[test]
    fn the_replication_block_parses_with_its_defaults() {
        let defaults = super::Replication::default();
        assert_eq!(defaults.write_timeout.duration(), Duration::from_secs(5));
        assert_eq!(defaults.pending_bytes, 64 * 1024 * 1024);
        assert_eq!(defaults.segment_bytes, 10 * 1024 * 1024);
        assert_eq!(defaults.checkpoint_entries, 1024);
        assert_eq!(defaults.retained_entries, 10_000);
        assert_eq!(defaults.log_cache_bytes, 16 * 1024 * 1024);
        assert_eq!(defaults.volatile_log_bytes, 256 * 1024 * 1024);
        // the snapshot and retention settings ([F43](../../../../docs/src/features/node-recovery.md))
        assert_eq!(defaults.snapshot_chunk_bytes, 1024 * 1024);
        assert_eq!(defaults.snapshot_timeout.duration(), Duration::from_secs(300));
        assert_eq!(defaults.install_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(defaults.retained_bytes, 1024 * 1024 * 1024);
        // a block naming every field, in the sizes an operator writes
        let parsed: super::Replication = serde_yaml::from_str(
            "write_timeout: \"2s\"\npending_bytes: \"8MiB\"\nsegment_bytes: \"1MiB\"\ncheckpoint_entries: 64\nretained_entries: 128\nlog_cache_bytes: \"1MiB\"\nvolatile_log_bytes: \"4MiB\"\nsnapshot_chunk_bytes: \"256KiB\"\nsnapshot_timeout: \"1m\"\ninstall_bytes: \"64MiB\"\nretained_bytes: \"4MiB\"\n",
        )
        .expect("a full replication block parses");
        assert_eq!(parsed.snapshot_chunk_bytes, 256 * 1024);
        assert_eq!(parsed.snapshot_timeout.duration(), Duration::from_secs(60));
        assert_eq!(parsed.install_bytes, 64 * 1024 * 1024);
        assert_eq!(parsed.retained_bytes, 4 * 1024 * 1024);
        assert_eq!(parsed.write_timeout.duration(), Duration::from_secs(2));
        assert_eq!(parsed.pending_bytes, 8 * 1024 * 1024);
        assert_eq!(parsed.segment_bytes, 1024 * 1024);
        assert_eq!(parsed.checkpoint_entries, 64);
        assert_eq!(parsed.retained_entries, 128);
        assert_eq!(parsed.volatile_log_bytes, 4 * 1024 * 1024);
        // an empty block is the defaults, and an unknown field is refused
        let empty: super::Replication = serde_yaml::from_str("{}").expect("an empty block parses");
        assert_eq!(empty, defaults);
        assert!(serde_yaml::from_str::<super::Replication>("fsync_every: 3\n").is_err());
    }
}
