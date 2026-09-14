//! Any errors tht can be encountered when running Shoal

use glommio::GlommioError;
use glommio::{BuilderErrorKind, ExecutorErrorKind, ReactorErrorKind};
use std::num::TryFromIntError;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

use crate::shared::auth::AuthError;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{Lane, PeerRefusal};
use crate::shared::protocol::ProtocolError;
use crate::shared::tls::TlsError;

/// Any errors tht can be encountered when running Shoal
#[derive(Debug)]
pub enum ServerError {
    /// An error specific to Shoal code
    Shoal(ShoalError),
    /// An IO error
    IO(std::io::Error),
    /// A glommio enhanced IO errors
    GlommioIO {
        source: std::io::Error,
        op: &'static str,
        path: Option<PathBuf>,
        fd: Option<RawFd>,
    },
    /// A glommio executor error
    GlommioExectorError(ExecutorErrorKind),
    /// A glommio builder error
    GlommioBuilderError(BuilderErrorKind),
    /// A glommio reactor error
    GlommioReactorError(ReactorErrorKind),
    /// Glommio time out error
    GlommioTimedOut(Duration),
    /// An error from glommio with a generic
    GlommioGeneric(String),
    /// An config parsing error
    Config(config::ConfigError),
    /// An rkyv error
    Rkyv(rkyv::rancor::Error),
    /// An error casting a vec of bytes to a slice
    IntoSlice(std::array::TryFromSliceError),
    /// A conversion error
    Conversion(std::convert::Infallible),
    /// An error sending a message over a kanal channel
    KanalSend(kanal::SendError),
    /// An error receiving a message over a kanal channel
    KanalRecv(kanal::ReceiveError),
    /// An error parsing a byte unit from a string
    ByteUnitParse(byte_unit::ParseError),
    /// An error converting an integer
    TryFromInt(TryFromIntError),
    /// An error reading or writing json
    SerdeJson(serde_json::Error),
    /// A frame that could not be written or read
    Protocol(ProtocolError),
    /// A client that could not prove who it is
    ///
    /// This keeps the distinction the wire deliberately throws away. A client is told only that
    /// authentication failed, so that a login cannot be used to ask whether an account exists,
    /// while this server's own log — which nobody untrusted is reading — says which of the several
    /// failures it actually was.
    Auth(AuthError),
    /// A connection that could not be encrypted
    ///
    /// This covers both halves of taking the wire: a certificate that could not be loaded, which
    /// stops the server before it listens, and a handshake that failed, which ends one connection.
    Tls(TlsError),
    /// A shard that failed to start, or died after it had
    ///
    /// The error is carried as text because it crossed a thread boundary from the shard that hit
    /// it, and what the pool's owner needs is to be told at all - which nothing did before
    /// [item 58](../../../docs/src/appendix/resolved/unreported-shard-death.md) - rather than to
    /// match on it.
    ShardFailed { shard: usize, error: String },
    /// Not every shard reported ready before the deadline
    ///
    /// `ready` of `of` shards had bound and joined; the rest were still starting, or wedged. A
    /// shard that failed outright is reported as [`ServerError::ShardFailed`] instead, so this
    /// is only ever the slow case.
    ReadyTimeout { ready: usize, of: usize, timeout: Duration },
    /// The control plane failed to start, or died after it had
    ///
    /// Carried as text for the reason [`ServerError::ShardFailed`] is: it crossed from the
    /// control thread, and what the pool's owner needs is to be told.
    ControlFailed { error: String },
}

impl std::fmt::Display for ServerError {
    /// Render this error for a person
    ///
    /// The variants that carry a message of their own render it; the wrappers around another
    /// crate's error render that error, and anything else falls back to its `Debug`, which is
    /// what every caller printed before this existed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Shoal(error) => write!(f, "{error}"),
            ServerError::IO(error) => write!(f, "io error: {error}"),
            ServerError::Config(error) => write!(f, "config error: {error}"),
            ServerError::SerdeJson(error) => write!(f, "json error: {error}"),
            ServerError::ShardFailed { shard, error } => write!(f, "shard {shard} failed: {error}"),
            ServerError::ControlFailed { error } => write!(f, "control plane failed: {error}"),
            ServerError::ReadyTimeout { ready, of, timeout } => {
                write!(f, "{ready} of {of} shards ready after {timeout:?}")
            }
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<TlsError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: TlsError) -> Self {
        ServerError::Tls(error)
    }
}

impl From<AuthError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: AuthError) -> Self {
        ServerError::Auth(error)
    }
}

impl From<ProtocolError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: ProtocolError) -> Self {
        ServerError::Protocol(error)
    }
}

// convert all of our external error types to our error type
impl<T> From<glommio::GlommioError<T>> for ServerError {
    fn from(ext: glommio::GlommioError<T>) -> Self {
        match ext {
            GlommioError::IoError(error) => Self::IO(error),
            GlommioError::EnhancedIoError {
                source,
                op,
                path,
                fd,
            } => Self::GlommioIO {
                source,
                op,
                path,
                fd,
            },
            GlommioError::ExecutorError(error) => Self::GlommioExectorError(error),
            GlommioError::BuilderError(error) => Self::GlommioBuilderError(error),
            GlommioError::ReactorError(error) => Self::GlommioReactorError(error),
            GlommioError::TimedOut(duration) => Self::GlommioTimedOut(duration),
            GlommioError::Closed(_) => Self::GlommioGeneric(ext.to_string()),
            GlommioError::CanNotBeClosed(_, _) => Self::GlommioGeneric(ext.to_string()),
            GlommioError::WouldBlock(_) => Self::GlommioGeneric(ext.to_string()),
        }
    }
}

impl From<config::ConfigError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: config::ConfigError) -> Self {
        ServerError::Config(error)
    }
}

impl From<std::io::Error> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::io::Error) -> Self {
        ServerError::IO(error)
    }
}

impl From<rkyv::rancor::Error> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: rkyv::rancor::Error) -> Self {
        ServerError::Rkyv(error)
    }
}

impl From<std::array::TryFromSliceError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::array::TryFromSliceError) -> Self {
        ServerError::IntoSlice(error)
    }
}

impl From<std::convert::Infallible> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::convert::Infallible) -> Self {
        ServerError::Conversion(error)
    }
}

impl From<kanal::SendError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::SendError) -> Self {
        ServerError::KanalSend(error)
    }
}

impl From<kanal::ReceiveError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::ReceiveError) -> Self {
        ServerError::KanalRecv(error)
    }
}

impl From<byte_unit::ParseError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: byte_unit::ParseError) -> Self {
        ServerError::ByteUnitParse(error)
    }
}

impl From<TryFromIntError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: TryFromIntError) -> Self {
        ServerError::TryFromInt(error)
    }
}

impl From<serde_json::Error> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: serde_json::Error) -> Self {
        ServerError::SerdeJson(error)
    }
}

/// The errors specific to Shoal server code
#[derive(Debug)]
pub enum ShoalError {
    /// An invalid non binary message type was recieved
    NonBinaryMessage,
    /// The map hash was not valid
    MapCorruption { found: u64, expected: u64 },
    /// An intent log or map file was shorter than expected (truncated on disk)
    TruncatedIntentLog,
    /// A partition was not found in the archive map (corrupt or missing map entry)
    PartitionNotFound { partition_id: u64 },
    /// An archive a partition entry points at is not on disk
    ///
    /// The archive map is what says which archive holds a partition, and the compactor
    /// deletes an archive once it has rewritten what was still live in it - so a read
    /// holding an entry from before that re-point looks for a file that is gone. Naming
    /// the archive is the whole point of this variant: creating the file instead makes
    /// the failure surface much later, as a validation error on bytes nobody wrote.
    ArchiveMissing { archive: Uuid, path: PathBuf },
    /// A partition's record did not hash to the checksum written beside it
    ///
    /// Every record of a format 2 archive carries a checksum over its payload, and this is
    /// the read that found one that does not match: the bytes on disk are not the bytes the
    /// compactor wrote ([F44](../../../docs/src/features/repair.md)). Both hashes are zero
    /// for a record the read came back short on, which is a torn record rather than a
    /// flipped byte. Named by archive and partition so an operator can find the copy and
    /// the repair can quarantine the group holding it.
    CorruptArchive { archive: Uuid, partition_id: u64, expected: u64, found: u64 },
    /// A table was not found in the archive map (corrupt or missing map)
    TableMapMissing,
    /// This node has no shards, so nothing could own any data
    NoShards,
    /// This node has more shards than a tablet can name an owner for
    TooManyShards { shards: usize },
    /// A cluster node's slot count is claimed once and cannot be changed by configuration
    ///
    /// Every peer's group identities, WAL and checkpoints are keyed by this node's slots, so
    /// a `cluster.slots` that differs from what the directory was claimed with would re-cut
    /// every replica set on every peer. Growth past the slots is M9b's `Replace`
    /// ([F47](../../../docs/src/features/local-rehome.md)).
    SlotsFixed { claimed: usize, configured: usize },
    /// A cluster node was configured with more cores than it has slots to host
    ///
    /// A slot is what an address names, and an executor with no slot to host would own
    /// nothing; the ceiling is the slot count the directory was claimed with.
    CoresExceedSlots { cores: usize, slots: usize },
    /// A fresh cluster node asked for fewer slots than it has cores
    SlotsBelowCores { slots: usize, cores: usize },
    /// A rehome towards one core count is on disk and the configuration names another
    ///
    /// The manifest is resumed only by the count it was planned for; a start under a third
    /// count is refused rather than planned over an unfinished move.
    RehomeInProgress { from: usize, to: usize, configured: usize },
    /// This storage directorys marker was written in a format we cannot read
    ///
    /// Every other field in the marker only means what we think it means if we agree
    /// about the shape it was written in, so an unreadable format has to be refused
    /// before the shard count inside it is trusted. `supported` is what this build reads, so
    /// the message can say what would be accepted as well as what was not.
    StorageFormatMismatch { found: u32, supported: &'static [u32] },
    /// This storage directory lays its data out under a shard layout this build does not
    ///
    /// The same refusal as a shard count mismatch, one level up: the layout says how tablets
    /// map to shards, and data under another layout is in places this build will not look.
    ShardLayoutMismatch { found: u32, expected: u32 },
    /// This storage directory belongs to a cluster, and the configuration is standalone
    ///
    /// A cluster member started without its `cluster:` block would serve its data as if it were
    /// the only copy, which is a quiet way to fork a cluster.
    ClusterDirectoryInStandalone { cluster: ClusterId },
    /// This storage directory is a standalone node's, and the configuration names a cluster
    ///
    /// Turning single node data into a cluster member is the migration M10 owns; nothing here
    /// can do it, and refusing is what keeps a node from claiming a cluster it was never
    /// bootstrapped into.
    StandaloneDirectoryInCluster { node: NodeId },
    /// This storage directory is a joiner's that was never admitted, and the configuration bootstraps
    ///
    /// A directory started with seeds was meant for the cluster those seeds name. Creating a
    /// cluster of its own on it would turn that node into another cluster, which is the fork
    /// [C1](../../../docs/src/distributed/node-identity.md) forbids an unreachable seed list from
    /// causing.
    JoiningDirectoryBootstrapped { node: NodeId },
    /// This storage directory is a joiner's that was never admitted, and the configuration is standalone
    JoiningDirectoryInStandalone { node: NodeId },
    /// A cluster was adopted into a directory that is not joining one
    ///
    /// A member already has its cluster and a standalone directory never adopts one, so the
    /// adoption is refused rather than becoming a mode change.
    MarkerNotJoining { node: NodeId, mode: String },
    /// A peer proved it belongs to a cluster other than the one this directory is in
    ///
    /// `expected` is `None` for a standalone directory, which belongs to no cluster at all.
    WrongCluster { found: ClusterId, expected: Option<ClusterId> },
    /// A topology version was observed that is older than the one already recorded
    ///
    /// The marker's topology field is a high water mark, and a recovery that resumed from an
    /// older one would re-observe things it has already acted on.
    TopologyWentBackwards { found: u64, observed: u64 },
    /// Another process holds this storage directory
    ///
    /// Two servers on one directory would both claim the same node identity and write the same
    /// files. The lock is what refuses the second one.
    StorageDirectoryLocked { path: PathBuf },
    /// The control core the configuration names is not one this process may run on
    ///
    /// Checked against the process's actual affinity - a container's cpuset, a `taskset` - rather
    /// than against what is online, so a control thread is never pinned somewhere it cannot go.
    ControlCoreNotAllowed { cpu: usize, allowed: Vec<usize> },
    /// The control core is also a shard's, and the configuration did not say that was allowed
    ///
    /// Isolation is a claim, and a claim that left the control thread sharing a core with a
    /// shard without saying so would be a false one. `control_core_shared` is how it is said.
    ControlCoreOverlapsShards { cpu: usize },
    /// A cluster setting that this build does not implement yet, and the milestone that does
    ///
    /// Refused at startup rather than accepted and ignored, so a configuration never claims a
    /// property the server does not have.
    NotImplemented { setting: String, milestone: &'static str },
    /// Something asked the control plane, and there is no control plane
    ///
    /// A standalone server runs no control thread and no group; the topology it would report
    /// does not exist.
    NotClustered,
    /// The placement a ring was built for does not name this node
    ///
    /// A node routes against a placement it is part of; one that leaves it out would send every
    /// query to a peer, including the ones for its own tablets.
    PlacementMissingSelf { node: NodeId },
    /// The placement names this node with a shard count other than the one it runs
    ///
    /// A peer routes to a shard on this node from the count the cluster recorded, so a count
    /// that differs from the truth would name shards that do not exist or leave some unowned.
    PlacementShardCount { node: NodeId, entry: u16, actual: usize },
    /// A contact names a shard on another node, and was handed to the node local mesh
    ///
    /// The mesh carries messages between this node's shards and nothing else; a remote contact
    /// goes through the shard's peer links. Reaching this is a routing bug, not a peer's doing.
    NotLocal { node: NodeId, shard: u16 },
    /// A peer refused this node's hello, and why
    PeerRefused { node: NodeId, reason: PeerRefusal },
    /// A peer's hello named an identity other than the one this node dialled or placed
    PeerIdentity { expected: NodeId, found: NodeId },
    /// A peer's hello named a shard count other than the placement's
    PeerShardCount { node: NodeId, placed: u16, claimed: u16 },
    /// A peer's hello named a schema other than this node's
    PeerSchema { node: NodeId, ours: u64, theirs: u64 },
    /// A peer's hello named a lane this listener does not serve
    PeerLane { node: NodeId, lane: Lane },
    /// A peer's hello carried an incarnation the cluster has superseded
    PeerFenced { node: NodeId, committed: u64, offered: u64 },
    /// A peer's newest wire version is below the one the cluster has activated
    ///
    /// The activation is the rollback boundary: past it a member speaking only an older
    /// version is refused at every door ([F48](../../../docs/src/features/rolling-compatibility.md)).
    BelowActivatedWire { node: NodeId, activated: u8, offered: u8 },
    /// This node's own newest wire version is below the one the cluster has activated
    ///
    /// The other half of the same rule: a build or a pin that cannot speak the activated
    /// version cannot serve this cluster, so the pool refuses to start rather than run a
    /// member every peer refuses.
    WireBelowActivated { node: NodeId, activated: u8, ours: u8 },
    /// This node's own incarnation has been superseded by a later start of it
    ///
    /// The fencing rule's other half: a run the cluster has replaced stops serving, since two
    /// runs of one identity cannot both be the replica it names.
    Fenced { node: NodeId, committed: u64, ours: u64 },
    /// This node's identity has been removed from the cluster and tombstoned
    ///
    /// A removed member never rejoins under its identity, at any incarnation and from any
    /// copy of its directory; the directory is left where it is and a replacement joins as a
    /// new identity ([F46](../../../docs/src/features/capacity-rebalancing.md)).
    Removed { node: NodeId },
    /// A command could not be committed because no control leader could be reached
    NoLeader { what: String },
    /// A joiner was refused admission, and why
    JoinRefused { reason: String },
    /// A lane's queue to a peer is at its byte bound, so nothing more was accepted for it
    PeerQueueFull { node: NodeId, lane: Lane, bound: usize },
    /// A peer link is down and the frame was never written to it
    PeerUnavailable { node: NodeId, lane: Lane },
    /// The peer handshake did not finish, and what stopped it
    PeerHandshake(String),
    /// The config file says something this server cannot act on
    ///
    /// This is for the checks a type cannot make. A user named in the `auth` section with neither
    /// a password nor a derived credential is the one that exists today, and it is only reachable
    /// because the shape that reads correctly out of the `config` crate is two optional fields
    /// rather than the enum it wants to be.
    InvalidConfig(String),
}

impl std::fmt::Display for ShoalError {
    /// Render this error for a person
    ///
    /// The refusals a server makes at startup are the ones somebody is going to read, so they
    /// say what was found, what was expected, and where the way out is when there is one.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShoalError::StorageFormatMismatch { found, supported } => write!(
                f,
                "the storage marker is format {found} and this build reads {supported:?}; a marker \
                 in another format is unsupported and never migrated in place: serve the directory \
                 with the build that wrote it, or start a new directory and import or restore \
                 into it"
            ),
            ShoalError::SlotsFixed { claimed, configured } => write!(
                f,
                "the storage directory was claimed with {claimed} slots and `cluster.slots` names \
                 {configured}; a cluster node's slots are fixed at its first claim, and growth past \
                 them is a `Replace` onto a fresh identity (M9b)"
            ),
            ShoalError::CoresExceedSlots { cores, slots } => write!(
                f,
                "this cluster node has {slots} slots and `resources.cores` asks for {cores} executors; \
                 an executor cannot host a slot the node does not have, so grow onto a fresh identity \
                 with a `Replace` (M9b) or run at most {slots} cores"
            ),
            ShoalError::SlotsBelowCores { slots, cores } => write!(
                f,
                "`cluster.slots` is {slots} and `resources.cores` is {cores}; every executor hosts at \
                 least one slot, so the slots cannot be fewer than the cores"
            ),
            ShoalError::RehomeInProgress { from, to, configured } => write!(
                f,
                "a rehome from {from} to {to} executors is on disk and `resources.cores` is {configured}; \
                 start with {to} cores to finish it before changing the count again"
            ),
            ShoalError::ShardLayoutMismatch { found, expected } => write!(
                f,
                "the storage directory is under shard layout {found} and this build lays data out \
                 under layout {expected}"
            ),
            ShoalError::ClusterDirectoryInStandalone { cluster } => write!(
                f,
                "the storage directory belongs to cluster {cluster} and the configuration has no \
                 `cluster:` block; a cluster member cannot be served standalone"
            ),
            ShoalError::StandaloneDirectoryInCluster { node } => write!(
                f,
                "the storage directory belongs to standalone node {node} and the configuration \
                 names a cluster; converting single node data into a cluster member is the \
                 migration M10 owns, and there is no supported path yet"
            ),
            ShoalError::JoiningDirectoryBootstrapped { node } => write!(
                f,
                "the storage directory belongs to node {node}, which was started as a joiner and \
                 never admitted to its cluster; bootstrapping it would create a second cluster, so \
                 start it with its seeds again or use an empty directory"
            ),
            ShoalError::JoiningDirectoryInStandalone { node } => write!(
                f,
                "the storage directory belongs to node {node}, which was started as a joiner and \
                 never admitted to its cluster; it cannot be served standalone"
            ),
            ShoalError::MarkerNotJoining { node, mode } => write!(
                f,
                "node {node} was asked to adopt a cluster but its directory is {mode}, not joining"
            ),
            ShoalError::WrongCluster { found, expected } => match expected {
                Some(expected) => write!(
                    f,
                    "a peer belongs to cluster {found} and this directory is in cluster {expected}"
                ),
                None => write!(
                    f,
                    "a peer belongs to cluster {found} and this directory is standalone"
                ),
            },
            ShoalError::TopologyWentBackwards { found, observed } => write!(
                f,
                "topology version {observed} was observed after {found} had already been recorded"
            ),
            ShoalError::StorageDirectoryLocked { path } => write!(
                f,
                "another process holds the storage directory, locked at {}",
                path.display()
            ),
            ShoalError::ControlCoreNotAllowed { cpu, allowed } => write!(
                f,
                "cluster.control_core names cpu {cpu}, which is outside this process's affinity \
                 {allowed:?}"
            ),
            ShoalError::ControlCoreOverlapsShards { cpu } => write!(
                f,
                "cluster.control_core names cpu {cpu}, which a shard also runs on; set \
                 cluster.control_core_shared to run them together and have it recorded"
            ),
            ShoalError::NotImplemented { setting, milestone } => write!(
                f,
                "{setting} is not implemented in this build; {milestone} delivers it"
            ),
            ShoalError::NotClustered => write!(f, "this server is standalone and has no control plane"),
            ShoalError::PlacementMissingSelf { node } => write!(
                f,
                "the placement does not name this node, {node}; a node routes against a \
                 placement it is part of"
            ),
            ShoalError::PlacementShardCount { node, entry, actual } => write!(
                f,
                "the placement names this node, {node}, with {entry} shards but it runs \
                 {actual}; every peer routes to a shard on it from that count"
            ),
            ShoalError::NotLocal { node, shard } => write!(
                f,
                "shard {shard} of {node} is on another node and was handed to the local mesh"
            ),
            ShoalError::PeerRefused { node, reason } => {
                write!(f, "{node} refused our hello: {reason}")
            }
            ShoalError::PeerIdentity { expected, found } => write!(
                f,
                "a peer identified itself as {found} where the placement expected {expected}"
            ),
            ShoalError::PeerShardCount { node, placed, claimed } => write!(
                f,
                "{node} runs {claimed} shards but the placement says {placed}; a query routed \
                 by that entry would name shards it does not have"
            ),
            ShoalError::PeerSchema { node, ours, theirs } => write!(
                f,
                "{node} was built from a different schema: ours is {ours:#018x} and theirs is \
                 {theirs:#018x}"
            ),
            ShoalError::PeerFenced { node, committed, offered } => write!(
                f,
                "{node} presented incarnation {offered} and the cluster holds {committed}; a \
                 later start of it has been admitted"
            ),
            ShoalError::BelowActivatedWire { node, activated, offered } => write!(
                f,
                "{node} speaks wire version {offered} at most and the cluster has activated \
                 {activated}; an activation is the boundary no member rolls back past"
            ),
            ShoalError::WireBelowActivated { node, activated, ours } => write!(
                f,
                "this node, {node}, speaks wire version {ours} at most and the cluster has \
                 activated {activated}; raise the build or lift `cluster.transport.wire_version`"
            ),
            ShoalError::Fenced { node, committed, ours } => write!(
                f,
                "this node, {node}, is incarnation {ours} and the cluster has admitted \
                 incarnation {committed} of it; another run of this directory has replaced this \
                 one, so it stops"
            ),
            ShoalError::Removed { node } => write!(
                f,
                "this node, {node}, was removed from the cluster and its identity is tombstoned; \
                 it cannot rejoin, and a replacement joins as a new identity"
            ),
            ShoalError::NoLeader { what } => write!(
                f,
                "{what} could not be committed: no control leader could be reached within the \
                 deadline"
            ),
            ShoalError::JoinRefused { reason } => write!(f, "the cluster refused this joiner: {reason}"),
            ShoalError::PeerLane { node, lane } => {
                write!(f, "{node} asked for the {lane} lane on a listener that does not serve it")
            }
            ShoalError::PeerQueueFull { node, lane, bound } => write!(
                f,
                "the {lane} lane to {node} holds its whole bound of {bound} bytes unsent"
            ),
            ShoalError::PeerUnavailable { node, lane } => {
                write!(f, "the {lane} lane to {node} is down and the frame was never written")
            }
            ShoalError::PeerHandshake(what) => write!(f, "the peer handshake failed: {what}"),
            ShoalError::InvalidConfig(reason) => write!(f, "invalid config: {reason}"),
            ShoalError::NoShards => write!(f, "this node has no cores to run a shard on"),
            other => write!(f, "{other:?}"),
        }
    }
}
