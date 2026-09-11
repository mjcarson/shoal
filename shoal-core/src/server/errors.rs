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
    /// A table was not found in the archive map (corrupt or missing map)
    TableMapMissing,
    /// This node has no shards, so nothing could own any data
    NoShards,
    /// This node has more shards than a tablet can name an owner for
    TooManyShards { shards: usize },
    /// This storage directory was written by a different number of shards
    ///
    /// The shard that owns a partition is decided by the shard count, and a shards data
    /// is stored under its own name, so reading a directory back with a different count
    /// looks for every partition in the wrong place.
    ShardCountMismatch { found: usize, expected: usize },
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
                "the storage marker is format {found} and this build reads {supported:?}; there is \
                 no migration between marker formats yet (M10 owns one), so a directory in another \
                 format has to be served by the build that wrote it"
            ),
            ShoalError::ShardCountMismatch { found, expected } => write!(
                f,
                "the storage directory was written by {found} shards and this server has {expected}"
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
            ShoalError::InvalidConfig(reason) => write!(f, "invalid config: {reason}"),
            ShoalError::NoShards => write!(f, "this node has no cores to run a shard on"),
            other => write!(f, "{other:?}"),
        }
    }
}
