//! The identity of a Shoal storage directory
//!
//! A partition is owned by the shard its tablet is assigned to, and that assignment comes
//! from the shard count. A shard also stores its data under its own name, so a directory
//! written by one shard count and read back under another looks for every partition in
//! the wrong place and finds nothing. Nothing about the data on disk says what count wrote
//! it, so this records it.
//!
//! Since [F37](../../../docs/src/features/node-identity-control-plane.md) it records who the
//! directory *is* as well as how it is laid out: the [`NodeId`] minted the first time the
//! directory was claimed, the [`ClusterId`] it was bootstrapped into if it was, the version of
//! the shard layout the data is under, and the last topology version the node observed. That is
//! format 2. A format 1 marker - the shape before any of that existed - is refused rather than
//! upgraded, with an error naming the format and the fact that no migration tool exists yet;
//! [C1](../../../docs/src/distributed/node-identity.md) permits that for a development build and
//! M10 owns the real path. There is deliberately no migration of the shard count either. This
//! turns a silent loss into a refusal to start; moving data between shard counts needs tablet
//! migration, which does not exist yet.
//!
//! **Exactly one field is ever rewritten in place: `topology`.** The identities, the shard count
//! and the layout are written once, at the claim, and never again. Every write of the file -
//! the claim and each topology observation - goes through the same temp file, fsync, rename and
//! directory fsync, so a crash at any point leaves either the old marker or the new one and
//! never a torn one.

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use tracing::{event, instrument, Level};

use super::errors::ShoalError;
use super::ServerError;
use crate::shared::identity::{ClusterId, NodeId};

/// The version of this metadata file's own format
///
/// Bumped from 1 when the identities, the layout and the topology version were added, since a
/// reader of format 1 has no idea what those fields mean and a reader of format 2 cannot invent
/// a node id for a directory that has none.
pub const META_FORMAT: u32 = 2;

/// The formats this build can read
///
/// One entry today. The refusal of any other names this list, so an operator holding a marker
/// from another build is told what this one understands rather than only what it does not.
pub const SUPPORTED_FORMATS: &[u32] = &[META_FORMAT];

/// The version of the shard layout the data is under
///
/// Layout 1 is the `tablet % shard_count` ring [`super::ring::Ring::new`] builds and the per
/// shard directories under it. A rehome that changed how tablets map to shards would be layout 2,
/// and a marker naming a layout this build does not lay data out in is refused the same way a
/// format is.
pub const SHARD_LAYOUT: u32 = 1;

/// The name of the metadata file within a storage directory
const META_FILE: &str = "shoal-meta.json";

/// The name the marker is staged under before it is renamed into place
const META_TEMP_FILE: &str = "shoal-meta.json.tmp";

/// The name of the lock file a running server holds in its storage directory
const LOCK_FILE: &str = "shoal.lock";

/// The identity of a Shoal storage directory
///
/// This is the shape on disk. What a running server holds is the [`Identity`] read out of it,
/// which is the part that never changes while the server runs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageMeta {
    /// The version of this metadata file's own format
    pub format: u32,
    /// The number of shards the data in this directory was written by
    pub shards: usize,
    /// The node this directory belongs to, minted when the directory was first claimed
    pub node: NodeId,
    /// The cluster this directory was bootstrapped into, or none for a standalone node
    pub cluster: Option<ClusterId>,
    /// The version of the shard layout the data is under
    pub layout: u32,
    /// The last topology version this node observed, or zero if it has never seen one
    ///
    /// Standalone nodes never observe one and keep zero. This is the only field the control
    /// plane rewrites, through [`StorageMeta::observe_topology`], and it is a hint about where
    /// to resume from rather than proof of anything: tablet freshness is settled by the tablet's
    /// own term and vote, never by this counter.
    pub topology: u64,
}

/// Whether a server is being started as a standalone node or as a member of a cluster
///
/// Derived from the `cluster:` block of the configuration, and the thing the claim checks a
/// directory against: a directory bootstrapped into a cluster is refused by a standalone
/// configuration and the other way round, because either would be a node quietly changing what
/// its data means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterIntent {
    /// No `cluster:` block: a single node that never mints a cluster
    Standalone,
    /// `cluster.bootstrap: true`: mint a cluster on an empty directory, keep it on an established one
    Bootstrap,
}

/// What a storage directory says about who it belongs to
///
/// The part of the marker that never changes while a server runs, read once at the claim and
/// held by the pool for its lifetime. The topology version is deliberately not here: it moves,
/// and a copy of it would go stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The node this directory belongs to
    pub node: NodeId,
    /// The cluster it was bootstrapped into, or none for a standalone node
    pub cluster: Option<ClusterId>,
    /// The version of the shard layout the data is under
    pub layout: u32,
    /// The topology version the marker held when the directory was claimed
    ///
    /// A starting point for the control plane's recovery, not a live value: the live one is on
    /// disk and moves without this being told.
    pub topology_at_claim: u64,
    /// Whether the directory was minted by this claim rather than reopened
    pub fresh: bool,
}

impl Identity {
    /// Check that a cluster a peer has proved it belongs to is the one this directory is in
    ///
    /// The seam the M2 handshake calls once a peer has authenticated: a directory in cluster
    /// `expected` refuses a peer from cluster `found`, and the refusal never touches the marker.
    /// A standalone directory belongs to no cluster and refuses every peer.
    ///
    /// # Arguments
    ///
    /// * `authenticated` - The cluster the peer has proved it belongs to
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::WrongCluster`] when the two differ.
    pub fn verify_cluster(&self, authenticated: ClusterId) -> Result<(), ServerError> {
        // the cluster this directory is in, if it is in one
        match self.cluster {
            // the same cluster, which is the only thing a peer may be
            Some(expected) if expected == authenticated => Ok(()),
            // a different cluster, or none at all
            expected => Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: authenticated,
                expected,
            })),
        }
    }
}

/// The lock a running server holds on its storage directory
///
/// An advisory `flock` on `shoal.lock`, held for the pool's lifetime and released by the kernel
/// if the process dies. Two processes opening one path would each claim the same node identity
/// and write the same files, and nothing else about the marker can tell them apart; this is what
/// refuses the second one. It says nothing about a cloned disk on another machine, which needs
/// the cluster wide fencing [C1](../../../docs/src/distributed/node-identity.md) leaves to Q11.
#[derive(Debug)]
pub struct DirectoryLock {
    /// The open lock file, whose descriptor carries the lock
    _file: File,
    /// Where the lock file is, for the error a second opener is shown
    path: PathBuf,
}

impl DirectoryLock {
    /// Take the lock on a storage directory, refusing if another process holds it
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::StorageDirectoryLocked`] when another process holds the lock,
    /// and with an IO error when the lock file cannot be opened.
    pub fn acquire(root: &Path) -> Result<Self, ServerError> {
        // the lock file lives beside the marker, and is created if this is a fresh directory
        std::fs::create_dir_all(root)?;
        let path = root.join(LOCK_FILE);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        // an exclusive, non blocking lock: another holder means refuse now rather than wait
        // SAFETY: `flock` on a descriptor this function just opened and still owns
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            // the one failure that means another process has the directory is reported as that;
            // anything else is the io error it was
            let error = std::io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::EWOULDBLOCK) => {
                    Err(ServerError::Shoal(ShoalError::StorageDirectoryLocked { path }))
                }
                _ => Err(ServerError::IO(error)),
            };
        }
        Ok(DirectoryLock { _file: file, path })
    }

    /// Where the lock file is
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StorageMeta {
    /// Build the metadata for a directory claimed by a node, in the current format
    ///
    /// The only constructor, so that every marker ever written carries the format this build
    /// writes and the layout it lays data out in.
    ///
    /// # Arguments
    ///
    /// * `shards` - The number of shards writing to this directory
    /// * `node` - The node the directory belongs to
    /// * `cluster` - The cluster it is bootstrapped into, if any
    #[must_use]
    pub fn new(shards: usize, node: NodeId, cluster: Option<ClusterId>) -> Self {
        StorageMeta {
            format: META_FORMAT,
            shards,
            node,
            cluster,
            layout: SHARD_LAYOUT,
            topology: 0,
        }
    }

    /// Get the path to the metadata file within a storage directory
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    pub fn path(root: &Path) -> PathBuf {
        root.join(META_FILE)
    }

    /// Read the marker a directory carries, if it carries one
    ///
    /// Only the format is checked here, because every other field means what it means only
    /// under a format this build reads. The checks that depend on what the server is about to
    /// do are [`StorageMeta::claim`]'s.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Refuses a format this build does not read, and fails if the file cannot be read or
    /// parsed.
    pub fn read(root: &Path) -> Result<Option<StorageMeta>, ServerError> {
        // read whatever metadata this directory already carries
        let raw = match std::fs::read(Self::path(root)) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ServerError::IO(error)),
        };
        // settle the format before anything else in the file is trusted. the format is the first
        // thing checked and is checked alone, so a marker from another build fails on its format
        // and not on a field that build spelled differently
        let format: FormatOnly = serde_json::from_slice(&raw)?;
        if !SUPPORTED_FORMATS.contains(&format.format) {
            return Err(ServerError::Shoal(ShoalError::StorageFormatMismatch {
                found: format.format,
                supported: SUPPORTED_FORMATS,
            }));
        }
        // now the rest of it can be read as this build understands it
        let found: StorageMeta = serde_json::from_slice(&raw)?;
        Ok(Some(found))
    }

    /// Write a marker into a directory so that a crash leaves the old one or the new one
    ///
    /// Staged under a temporary name, synced, renamed over the marker, and then the directory is
    /// synced so the rename itself is durable. Blocking IO, deliberately: this runs before any
    /// executor exists at the claim, and on a blocking thread when the control plane calls it.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    fn write(&self, root: &Path) -> Result<(), ServerError> {
        // make sure the directory we are writing into exists
        std::fs::create_dir_all(root)?;
        // stage the new marker beside the old one
        let staged = root.join(META_TEMP_FILE);
        let mut file = File::create(&staged)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        drop(file);
        // and swap it in, which is the atomic step
        std::fs::rename(&staged, Self::path(root))?;
        // the rename is only durable once the directory entry is
        File::open(root)?.sync_all()?;
        Ok(())
    }

    /// Check this storage directory was written by the shard count and the mode we are starting with
    ///
    /// A directory with no metadata is new to us and is claimed by writing it: a node identity is
    /// minted, and a cluster identity too if the intent is to bootstrap one. This is called once,
    /// before any shard is spawned, so it uses blocking IO deliberately: there is no glommio
    /// executor yet, and doing it here rather than per shard is what keeps every shard from
    /// racing to write the same file.
    ///
    /// An established directory is held to what it already says. Bootstrapping on top of one is
    /// idempotent - the identities and everything under them are kept, and a second cluster is
    /// never minted - because "bootstrap" in a configuration file is a statement about how the
    /// cluster was created and not an instruction to create another every restart.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `shards` - The number of shards about to be started
    /// * `intent` - Whether the server is standalone or a cluster member
    ///
    /// # Errors
    ///
    /// This will fail if the directory was marked in a format we cannot read, if it was
    /// written by a different number of shards or under a different layout, if it belongs to a
    /// cluster and the configuration is standalone or the reverse, or if the metadata cannot be
    /// read or written.
    #[instrument(name = "StorageMeta::claim", skip_all, err(Debug))]
    pub fn claim(root: &Path, shards: usize, intent: ClusterIntent) -> Result<Identity, ServerError> {
        // read whatever metadata this directory already carries, format settled first
        match Self::read(root)? {
            // this directory has been written before, so it has a shard count and an identity
            // to honour
            Some(found) => {
                // a different shard count would look for every partition in the wrong place
                if found.shards != shards {
                    return Err(ServerError::Shoal(ShoalError::ShardCountMismatch {
                        found: found.shards,
                        expected: shards,
                    }));
                }
                // a different layout would too, even at the same count
                if found.layout != SHARD_LAYOUT {
                    return Err(ServerError::Shoal(ShoalError::ShardLayoutMismatch {
                        found: found.layout,
                        expected: SHARD_LAYOUT,
                    }));
                }
                // the mode the directory was claimed in has to be the mode it is reopened in
                match (intent, found.cluster) {
                    // a standalone directory reopened standalone, the ordinary restart
                    (ClusterIntent::Standalone, None) => {}
                    // a cluster member reopened as one, which keeps its cluster and never mints
                    // another however the configuration spells bootstrap
                    (ClusterIntent::Bootstrap, Some(_)) => {}
                    // a directory bootstrapped into a cluster, opened by a standalone config
                    (ClusterIntent::Standalone, Some(cluster)) => {
                        return Err(ServerError::Shoal(ShoalError::ClusterDirectoryInStandalone {
                            cluster,
                        }));
                    }
                    // a standalone directory opened by a cluster config, which is the migration
                    // M10 owns and nothing here can do
                    (ClusterIntent::Bootstrap, None) => {
                        return Err(ServerError::Shoal(ShoalError::StandaloneDirectoryInCluster {
                            node: found.node,
                        }));
                    }
                }
                Ok(Identity {
                    node: found.node,
                    cluster: found.cluster,
                    layout: found.layout,
                    topology_at_claim: found.topology,
                    fresh: false,
                })
            }
            // this directory has never been written to, so claim it for this node
            None => {
                // mint who this directory is going to be, and the cluster it starts if it does
                let node = NodeId::mint();
                let cluster = match intent {
                    ClusterIntent::Standalone => None,
                    ClusterIntent::Bootstrap => Some(ClusterId::mint()),
                };
                // build the metadata describing who is about to write here
                let meta = StorageMeta::new(shards, node, cluster);
                // write it before any shard has had the chance to store anything
                meta.write(root)?;
                // say what we claimed, since it is what a later start is held to
                event!(
                    Level::INFO,
                    msg = "Claimed a new storage directory",
                    path = Self::path(root).display().to_string(),
                    shards,
                    node = node.to_string(),
                    cluster = cluster.map(|cluster| cluster.to_string()),
                );
                Ok(Identity {
                    node,
                    cluster,
                    layout: SHARD_LAYOUT,
                    topology_at_claim: 0,
                    fresh: true,
                })
            }
        }
    }

    /// Record the topology version this node has now observed
    ///
    /// The one rewrite the marker ever sees. Everything else in the file is carried across
    /// unchanged, and the write is the same staged, synced and renamed path the claim uses. A
    /// version below the one on disk is refused rather than written, since the marker is a
    /// high water mark and going backwards would make a later recovery resume from too early.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `version` - The topology version now observed
    ///
    /// # Errors
    ///
    /// Fails if there is no marker to update, if it is in another format, if the version goes
    /// backwards, or if the file cannot be written.
    #[instrument(name = "StorageMeta::observe_topology", skip_all, err(Debug))]
    pub fn observe_topology(root: &Path, version: u64) -> Result<(), ServerError> {
        // the marker has to exist already: observing a topology is something a claimed node does
        let Some(mut found) = Self::read(root)? else {
            return Err(ServerError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no storage marker to record a topology version in",
            )));
        };
        // a high water mark never goes down
        if version < found.topology {
            return Err(ServerError::Shoal(ShoalError::TopologyWentBackwards {
                found: found.topology,
                observed: version,
            }));
        }
        // the same version again is nothing to write
        if version == found.topology {
            return Ok(());
        }
        // move the one field that moves, and swap the marker
        found.topology = version;
        found.write(root)
    }
}

/// The one field read before a marker's format is trusted
///
/// A marker from another format may spell every other field differently or not have it at
/// all, so the format is read on its own and everything else only once it is known.
#[derive(Deserialize)]
struct FormatOnly {
    /// The version of the marker's own format
    format: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory nothing has written to is claimed rather than refused
    #[test]
    fn an_unclaimed_directory_is_claimed() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claiming it has to succeed, and mint an identity
        let identity = StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        assert!(identity.fresh);
        assert_eq!(identity.cluster, None);
        // and has to leave behind what it claimed
        let found = StorageMeta::read(dir.path())
            .expect("no metadata written")
            .expect("no marker written");
        assert_eq!(found, StorageMeta::new(4, identity.node, None));
        assert_eq!(found.format, 2);
        // with nothing staged left behind
        assert!(!dir.path().join(META_TEMP_FILE).exists());
    }

    /// A directory reopened by the shard count that wrote it starts, as the same node
    #[test]
    fn the_same_shard_count_is_allowed_back() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of shards
        let first = StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        // reopening it with that same count is the ordinary restart, and the node is the same
        let again = StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect("failed to reopen with the same shard count");
        assert_eq!(again.node, first.node);
        assert!(!again.fresh);
    }

    /// A marker written by a format we do not understand is refused, naming what we do
    ///
    /// The shard count in a marker is only meaningful if we agree about what the fields
    /// around it mean, so the format has to be settled before the count is read. Format 1 is
    /// the case that exists in the wild: every directory written before F37.
    #[test]
    fn an_unknown_format_is_refused() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        std::fs::create_dir_all(dir.path()).expect("failed to build our storage directory");
        // stage a format 1 marker, exactly as the old build wrote one
        std::fs::write(
            StorageMeta::path(dir.path()),
            b"{\n  \"format\": 1,\n  \"shards\": 4\n}",
        )
        .expect("failed to stage our marker");
        // reading it has to refuse, and say which format it could not read and which it can
        let error = StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect_err("a format 1 marker started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageFormatMismatch {
                found: 1,
                supported: &[2]
            })
        ));
        // and the message has to say there is no migration yet, since that is what an operator
        // holding one of these needs to know
        let rendered = format!("{error}");
        assert!(rendered.contains("format 1"), "{rendered}");
        assert!(rendered.contains("no migration"), "{rendered}");
        // a format from the future is refused the same way
        let future = StorageMeta {
            format: META_FORMAT + 1,
            ..StorageMeta::new(4, NodeId::mint(), None)
        };
        std::fs::write(
            StorageMeta::path(dir.path()),
            serde_json::to_vec_pretty(&future).expect("failed to build our marker"),
        )
        .expect("failed to stage our marker");
        let error = StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect_err("an unknown marker format started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageFormatMismatch { found: 3, .. })
        ));
    }

    /// A directory reopened by a different shard count is refused
    #[test]
    fn a_different_shard_count_is_refused() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of shards
        StorageMeta::claim(dir.path(), 4, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        // reopening it with another count would look for data in the wrong place
        let error = StorageMeta::claim(dir.path(), 5, ClusterIntent::Standalone)
            .expect_err("a shard count change started");
        // and has to say so rather than start and lose the data
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::ShardCountMismatch {
                found: 4,
                expected: 5
            })
        ));
    }

    /// A bootstrap mints a cluster once, and a restart keeps it rather than minting another
    #[test]
    fn a_bootstrap_mints_one_cluster_and_keeps_it() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // the first bootstrap creates the cluster
        let first = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap a new directory");
        let cluster = first.cluster.expect("a bootstrap minted no cluster");
        // a restart with bootstrap still set is the same cluster and the same node
        let again = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to reopen a bootstrapped directory");
        assert_eq!(again.cluster, Some(cluster));
        assert_eq!(again.node, first.node);
        assert!(!again.fresh);
    }

    /// A directory claimed in one mode is refused in the other
    #[test]
    fn a_mode_change_is_refused_both_ways() {
        // a cluster directory opened standalone
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let bootstrapped = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        let error = StorageMeta::claim(dir.path(), 2, ClusterIntent::Standalone)
            .expect_err("a cluster directory started standalone");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::ClusterDirectoryInStandalone { cluster })
                if Some(cluster) == bootstrapped.cluster
        ));
        // a standalone directory opened as a cluster member, which names the migration path
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let standalone = StorageMeta::claim(dir.path(), 2, ClusterIntent::Standalone)
            .expect("failed to claim");
        let error = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect_err("a standalone directory joined a cluster");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StandaloneDirectoryInCluster { node })
                if node == standalone.node
        ));
        assert!(format!("{error}").contains("M10"), "{error}");
    }

    /// A peer from another cluster is refused, and the marker is not touched by the refusal
    #[test]
    fn the_wrong_cluster_is_refused_without_a_write() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let identity = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        let before = std::fs::read(StorageMeta::path(dir.path())).expect("a marker");
        // the right cluster is accepted
        identity
            .verify_cluster(identity.cluster.expect("a cluster"))
            .expect("our own cluster was refused");
        // any other is refused, naming both
        let other = ClusterId::mint();
        let error = identity
            .verify_cluster(other)
            .expect_err("another cluster was accepted");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::WrongCluster { found, expected })
                if found == other && expected == identity.cluster
        ));
        // and the bytes on disk are exactly what they were
        let after = std::fs::read(StorageMeta::path(dir.path())).expect("a marker");
        assert_eq!(before, after);
        // a standalone directory belongs to no cluster and refuses every peer
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let standalone = StorageMeta::claim(dir.path(), 2, ClusterIntent::Standalone)
            .expect("failed to claim");
        assert!(standalone.verify_cluster(other).is_err());
    }

    /// Observing a topology rewrites that one field, never goes backwards, and survives a restart
    #[test]
    fn a_topology_observation_moves_one_field() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let identity = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        // record a version
        StorageMeta::observe_topology(dir.path(), 3).expect("failed to observe a topology");
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.topology, 3);
        // and nothing else moved
        assert_eq!(found.node, identity.node);
        assert_eq!(found.cluster, identity.cluster);
        assert_eq!(found.shards, 2);
        assert_eq!(found.layout, SHARD_LAYOUT);
        // going backwards is refused
        let error = StorageMeta::observe_topology(dir.path(), 2)
            .expect_err("a topology version went backwards");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::TopologyWentBackwards {
                found: 3,
                observed: 2
            })
        ));
        // the same version again is a no-op
        StorageMeta::observe_topology(dir.path(), 3).expect("a repeat observation failed");
        // and the claim that follows sees what was recorded
        let again = StorageMeta::claim(dir.path(), 2, ClusterIntent::Bootstrap)
            .expect("failed to reopen");
        assert_eq!(again.topology_at_claim, 3);
        // an unclaimed directory has nothing to observe into
        let empty = tempfile::tempdir().expect("failed to build a temp dir");
        assert!(StorageMeta::observe_topology(empty.path(), 1).is_err());
    }

    /// Two locks on one directory cannot both be held, and a released one can be retaken
    #[test]
    fn a_directory_lock_is_exclusive() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let first = DirectoryLock::acquire(dir.path()).expect("failed to take the lock");
        assert!(first.path().ends_with(LOCK_FILE));
        // `flock` is per open file description, so a second descriptor in this process is as
        // much another holder as another process would be
        let error =
            DirectoryLock::acquire(dir.path()).expect_err("a second lock on one directory was taken");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageDirectoryLocked { .. })
        ));
        // releasing the first frees the directory
        drop(first);
        DirectoryLock::acquire(dir.path()).expect("failed to retake a released lock");
    }
}
