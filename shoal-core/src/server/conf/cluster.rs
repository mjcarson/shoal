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
/// Phi accrual, the shape [C3](../../../../docs/src/distributed/membership.md) describes. Recorded
/// at M1; the detector itself is M3's.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FailureDetector {
    /// How often a node is pinged, in milliseconds
    #[serde(default = "default_detector_interval_ms")]
    pub interval_ms: u64,
    /// The suspicion level at which a node is called Down
    #[serde(default = "default_phi_threshold")]
    pub phi_threshold: f64,
}

impl Default for FailureDetector {
    /// The defaults C1's block writes down
    fn default() -> Self {
        FailureDetector {
            interval_ms: default_detector_interval_ms(),
            phi_threshold: default_phi_threshold(),
        }
    }
}

/// One node of a static placement
///
/// What a node has to know about a peer before it can route to it: who it is, where its two
/// listeners are, and how many shards it runs - the last because a tablet's shard on that node
/// is `tablet % shards`, the same rule the node itself uses, and the handshake refuses a peer
/// whose count differs from this entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlacedNode {
    /// The node's identity, which its marker holds and its hello proves
    pub node: NodeId,
    /// Its data peer endpoint, `host:port`, which the bulk lane dials too
    pub data: String,
    /// Its control listener, `host:port`
    pub control: String,
    /// How many shards it runs
    pub shards: u16,
}

/// A static placement of tablets over nodes
///
/// [F38](../../../../docs/src/features/inter-node-transport.md)'s stand-in for the membership M3
/// commits: a list of every node in the cluster, this one included, in an order every node's
/// file agrees on. Tablet `t` belongs to `nodes[t % N]`, and on that node to shard `t % shards`.
/// A placement of one node is exactly the standalone tablet map, which is what keeps a one node
/// cluster's routing byte for byte what it was.
///
/// This is test-shaped on purpose: it names node identities in a file, which only a fixture that
/// staged the markers can do. It is replaced, not extended, when the control plane commits
/// membership.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// Every node of the cluster, in placement order
    #[serde(default)]
    pub nodes: Vec<PlacedNode>,
}

impl Placement {
    /// Find this node's own entry
    ///
    /// # Arguments
    ///
    /// * `me` - This node's identity
    pub fn entry(&self, me: NodeId) -> Option<&PlacedNode> {
        self.nodes.iter().find(|placed| placed.node == me)
    }

    /// Find a peer's entry
    ///
    /// # Arguments
    ///
    /// * `node` - The peer's identity
    pub fn peer(&self, node: NodeId) -> Option<&PlacedNode> {
        self.entry(node)
    }

    /// The nodes other than this one
    ///
    /// # Arguments
    ///
    /// * `me` - This node's identity
    pub fn peers(&self, me: NodeId) -> impl Iterator<Item = &PlacedNode> {
        self.nodes.iter().filter(move |placed| placed.node != me)
    }

    /// Check that this placement can be routed against by the node reading it
    ///
    /// # Arguments
    ///
    /// * `me` - This node's identity
    /// * `my_shards` - How many shards this node runs
    ///
    /// # Errors
    ///
    /// Refuses a placement that does not name this node, names it with another shard count,
    /// names a node twice, names a node with no shards, or names an address that is not one.
    pub fn validate(&self, me: NodeId, my_shards: usize) -> Result<(), ServerError> {
        // every node once
        for (i, placed) in self.nodes.iter().enumerate() {
            if self.nodes[..i].iter().any(|other| other.node == placed.node) {
                return Err(ServerError::Shoal(ShoalError::PlacementDuplicateNode {
                    node: placed.node,
                }));
            }
            // a node with no shards owns no tablet
            if placed.shards == 0 {
                return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "cluster.placement names {} with zero shards",
                    placed.node
                ))));
            }
            // both addresses have to be addresses
            for (what, addr) in [("data", &placed.data), ("control", &placed.control)] {
                if addr.parse::<std::net::SocketAddr>().is_err() {
                    return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                        "cluster.placement names {} with a {what} address that is not one: {addr}",
                        placed.node
                    ))));
                }
            }
        }
        // and this node among them, with the shard count it actually runs
        let Some(mine) = self.entry(me) else {
            return Err(ServerError::Shoal(ShoalError::PlacementMissingSelf { node: me }));
        };
        if usize::from(mine.shards) != my_shards {
            return Err(ServerError::Shoal(ShoalError::PlacementShardCount {
                node: me,
                entry: mine.shards,
                actual: my_shards,
            }));
        }
        Ok(())
    }
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
            inflight_bytes: default_inflight_bytes(),
            forward_timeout: default_forward_timeout(),
            reconnect_min: default_reconnect_min(),
            reconnect_max: default_reconnect_max(),
            handshake_timeout: default_handshake_timeout(),
            ping_interval: default_ping_interval(),
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
    /// The addresses of nodes to contact when joining
    ///
    /// Discovery addresses, not identities: a seed's identity is what it proves in the handshake.
    /// Refused until M3 delivers joining.
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
    /// The static placement of tablets over nodes
    ///
    /// Absent, this node is placed alone and its tablet map is the standalone one. Present, it
    /// has to name this node, and every other node in it is a peer to dial.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<Placement>,
    /// The byte bounds and timers of the peer lanes
    #[serde(default)]
    pub transport: Transport,
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
            placement: None,
            transport: Transport::default(),
        }
    }
}

impl Cluster {
    /// Create the cluster this node is a member of
    pub fn bootstrap(mut self, bootstrap: bool) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    /// Set the nodes to contact when joining
    pub fn seeds(mut self, seeds: Vec<String>) -> Self {
        self.seeds = seeds;
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

    /// Set the static placement of tablets over nodes
    pub fn placement(mut self, placement: Placement) -> Self {
        self.placement = Some(placement);
        self
    }

    /// Set the byte bounds and timers of the peer lanes
    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    /// The placement this node routes against, which is itself alone when none was given
    ///
    /// # Arguments
    ///
    /// * `me` - This node's identity
    /// * `interface` - The interface the client listener binds, for the advertised address
    /// * `shards` - How many shards this node runs
    pub fn placement_for(
        &self,
        me: NodeId,
        interface: &str,
        shards: usize,
    ) -> Result<Placement, ServerError> {
        // a placement that was written down is used as it is
        if let Some(placement) = &self.placement {
            return Ok(placement.clone());
        }
        // none is this node alone, which is the standalone map with a name on it
        let advertise = self.advertised(interface)?;
        // a node runs fewer shards than a u16 holds, which `Ring::new` refuses otherwise
        #[allow(clippy::cast_possible_truncation)]
        Ok(Placement {
            nodes: vec![PlacedNode {
                node: me,
                data: format!("{advertise}:{}", self.port),
                control: format!("{advertise}:{}", self.control_port),
                shards: shards as u16,
            }],
        })
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
    pub fn validate(&self, interface: &str) -> Result<(), ServerError> {
        // joining is M3's: a seed list on a node that is not bootstrapping means join
        if !self.seeds.is_empty() && !self.bootstrap {
            return Err(ServerError::Shoal(ShoalError::NotImplemented {
                setting: "cluster.seeds (joining an existing cluster)".to_string(),
                milestone: "M3",
            }));
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
        // the transport bounds and timers are the ones the configuration page writes down
        assert!(cluster.placement.is_none());
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
        Cluster::default().bootstrap(true).validate("127.0.0.1").expect("a bootstrap was refused");
        // joining is M3
        let error = Cluster::default()
            .seeds(vec!["10.0.0.1:12001".to_string()])
            .validate("127.0.0.1")
            .expect_err("a joiner started");
        assert!(format!("{error}").contains("M3"), "{error}");
        // a node that does neither is nothing
        assert!(Cluster::default().validate("127.0.0.1").is_err());
        // peer tls is accepted now, but a certificate that cannot be read is refused as it is read
        let mut with_missing_tls = Cluster::default().bootstrap(true);
        with_missing_tls.tls = Some(super::PeerTls {
            cert: "/does/not/exist/cert.pem".into(),
            key: "/does/not/exist/key.pem".into(),
            ca: "/does/not/exist/ca.pem".into(),
        });
        assert!(
            with_missing_tls.validate("127.0.0.1").is_err(),
            "an unreadable certificate was accepted"
        );
        // an even voter count is not a quorum anyone wants
        assert!(Cluster::default().bootstrap(true).control_voters(2).validate("127.0.0.1").is_err());
        // an unspecified interface with nothing advertised is not an address
        assert!(Cluster::default().bootstrap(true).validate("0.0.0.0").is_err());
        Cluster::default()
            .bootstrap(true)
            .advertise("10.0.0.2")
            .validate("0.0.0.0")
            .expect("an advertised address on 0.0.0.0 was refused");
    }
}
