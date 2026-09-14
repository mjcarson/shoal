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
//! upgraded, with an error naming the format; ~~[C1](../../../docs/src/distributed/node-identity.md)
//! permits that for a development build and M10 owns the real path~~ since
//! [F48](../../../docs/src/features/rolling-compatibility.md) that is the supported answer,
//! not a gap: a marker format is never migrated in place, and a directory in a format this
//! build does not read is served by the build that wrote it or its data brought over into a
//! new directory by an import or a restore. ~~There is deliberately no migration of the shard count either. This
//! turns a silent loss into a refusal to start; moving data between shard counts needs tablet
//! migration, which does not exist yet.~~ The shard count moves since F47; see below.
//!
//! Since [F39](../../../docs/src/features/membership.md) it is format 3: the same fields plus
//! the `mode` the directory was claimed in - standalone, a cluster member, or a joiner that has
//! not been admitted yet - and an `incarnation` that counts the starts of this directory. The
//! mode is what tells a joiner's directory before admission (a node id and no cluster) apart
//! from a standalone one, which format 2 could not; the incarnation is what fences two
//! processes running one copy of a directory on two machines, which the lock cannot see
//! ([C1](../../../docs/src/distributed/node-identity.md), Q11). A format 2 marker is read as
//! format 3 with its mode inferred from whether it names a cluster and its incarnation at zero,
//! and the first rewrite writes it as 3; nothing about it has to be invented, which is why this
//! is an upgrade where format 1 was a refusal.
//!
//! Since [F47](../../../docs/src/features/local-rehome.md) the shard count is two numbers. ~~A
//! directory reopened with a different `resources.cores` is refused (`ShardCountMismatch`)~~
//! `shards` is the count the directory was laid out as - on a cluster node the *slots*, the
//! shard every peer records for this node and every identity was minted from - and `physical`
//! is how many executors the files are laid out on now. A reopen at another core count is a
//! *pending rehome*, not a refusal: the claim reports it and the pool moves the files before a
//! shard starts ([`super::rehome`]). What is still refused by name is a change to the slots
//! (`SlotsFixed`), more cores than slots on a cluster node (`CoresExceedSlots`), and a start
//! under a third count while a rehome to a second is on disk (`RehomeInProgress`). The marker
//! stays at format 3: `physical` is optional and absent means `shards`, which is what every
//! marker written before this meant.
//!
//! **Four fields are ever rewritten in place: `topology`, `incarnation`, `physical` - by the
//! rehome's finalize - and, once, for a joiner, `cluster`.** The identities, the shard count and
//! the layout are written once, at the claim, and never again; a joiner's cluster is filled in
//! exactly once, when the cluster it dialled proves its identity, and `mode` moves from joining
//! to cluster with it. Every write of the file - the claim, each start's incarnation bump, each
//! topology observation, the adoption and the finalize - goes through the same temp file, fsync,
//! rename and directory fsync, so a crash at any point leaves either the old marker or the new
//! one and never a torn one.

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
/// a node id for a directory that has none. Bumped from 2 when the mode and the incarnation
/// were added; a format 2 marker is read as 3, since both can be inferred rather than invented.
pub const META_FORMAT: u32 = 3;

/// The format a joiner's mode and the incarnation were added in
const FORMAT_WITH_MODE: u32 = 3;

/// The formats this build can read
///
/// Format 2 is read and upgraded on its first rewrite; format 3 is what this build writes. The
/// refusal of any other names this list, so an operator holding a marker from another build is
/// told what this one understands rather than only what it does not.
pub const SUPPORTED_FORMATS: &[u32] = &[2, META_FORMAT];

/// The version of the shard layout a standalone node's data is under
///
/// Layout 1 is the `tablet % shard_count` ring [`super::ring::Ring::new`] builds and the per
/// shard directories under it, each table with an intent log of its own. ~~A rehome that changed
/// how tablets map to shards would be layout 2~~ Layout 2 is a cluster node's
/// ([`CLUSTER_LAYOUT`]), and a marker naming a layout this build does not lay data out in is
/// refused the same way a format is. A rehome ([F47](../../../docs/src/features/local-rehome.md))
/// does not bump the layout: the files are the same files on more or fewer executors, and which
/// executor owns a tablet is the hosting table's to say ([`super::hosting::Hosting`]), not the
/// layout's.
pub const SHARD_LAYOUT: u32 = 1;

/// The version of the shard layout a cluster node's data is under
///
/// The same archives, with the per table intent logs replaced by one shared WAL per shard under
/// `wal/` that every tablet group's log lives in ([F40](../../../docs/src/features/replication.md)).
/// A cluster directory written at layout 1 - by a build before F40 - is refused, since its
/// intent logs would be replayed by nothing; there is no migration tool, which is the same
/// policy a format 1 marker met at M1.
pub const CLUSTER_LAYOUT: u32 = 2;

/// The layout a node lays data out in, by what it is
///
/// # Arguments
///
/// * `intent` - Whether the node is standalone or a cluster member
#[must_use]
pub const fn layout_for(intent: ClusterIntent) -> u32 {
    match intent {
        ClusterIntent::Standalone => SHARD_LAYOUT,
        ClusterIntent::Bootstrap | ClusterIntent::Join => CLUSTER_LAYOUT,
    }
}

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
    /// The number of shards the data in this directory was laid out as
    ///
    /// On a cluster node this is the slot count: the shard every peer records for this node,
    /// the modulus of the placement rule and the shard in every address, minted once at the
    /// claim and never moved ([F47](../../../docs/src/features/local-rehome.md)). On a
    /// standalone node it is the count of the first claim, kept as the layout's origin.
    pub shards: usize,
    /// The number of executors the files are laid out on now, if it has ever differed
    ///
    /// Absent means `shards`, which is what every marker before F47 meant. Rewritten by the
    /// rehome's finalize and by nothing else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical: Option<usize>,
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
    /// The mode this directory was claimed in
    ///
    /// Absent from a format 2 marker, where it is inferred from `cluster`: a directory naming a
    /// cluster is a member, one naming none is standalone. A joiner is the third case format 2
    /// could not spell, since before admission it too names no cluster.
    #[serde(default)]
    pub mode: MarkerMode,
    /// How many times this directory has been started, counting this start
    ///
    /// Bumped on every claim of an established directory, so two processes started from one
    /// copy of a directory carry the same number and a later start carries a higher one; the
    /// control plane's fencing rule is that the highest wins
    /// ([C1](../../../docs/src/distributed/node-identity.md), Q11). Zero in a format 2 marker.
    #[serde(default)]
    pub incarnation: u64,
}

/// What kind of node a storage directory belongs to
///
/// Recorded at the claim so that a joiner's directory before admission, which has a node id and
/// no cluster, is never mistaken for a standalone one, which looks the same without this.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MarkerMode {
    /// A single node that never mints a cluster and belongs to none
    #[default]
    Standalone,
    /// A member of the cluster the marker names, whether it bootstrapped it or joined it
    Cluster,
    /// A node started with seeds that has not been admitted to a cluster yet
    Joining,
}

/// Whether a server is being started as a standalone node, a cluster's creator or a joiner
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
    /// `cluster.seeds`: mint a node identity on an empty directory and adopt a cluster later
    ///
    /// On an established directory that already belongs to a cluster this is an ordinary member
    /// restart: seeds are discovery, and a node that has been admitted no longer needs them.
    Join,
}

/// A rehome the claim found waiting: the files are laid out on one count and the node runs another
///
/// Reported by [`StorageMeta::claim`] rather than refused; the pool runs the rehome
/// ([`super::rehome`]) before any shard starts ([F47](../../../docs/src/features/local-rehome.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRehome {
    /// The executor count the files are laid out on
    pub from: usize,
    /// The executor count the node runs
    pub to: usize,
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
    /// How many slots this node has: the shard count every peer records for it
    ///
    /// The marker's `shards` on a cluster node. On a standalone node the slots are the
    /// executors, since nobody records anything for it, and the marker's `shards` is only where
    /// the layout began ([F47](../../../docs/src/features/local-rehome.md)).
    pub slots: usize,
    /// How many executors this node runs: the cores the claim was made with
    pub physical: usize,
    /// The rehome the files need before a shard starts, if the executor count changed
    pub rehome: Option<PendingRehome>,
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
    /// The mode the directory is in
    ///
    /// A joiner reads `Joining` here until it is admitted, and the marker moves to `Cluster`
    /// through [`StorageMeta::adopt_cluster`] without this copy being told.
    pub mode: MarkerMode,
    /// Which start of this directory this is
    ///
    /// One for a directory minted by this claim, and one more than the marker held for a
    /// reopened one. Carried in every peer hello and in the committed member record.
    pub incarnation: u64,
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
        // a directory that names a cluster is a member of it, one that names none is standalone
        let mode = match cluster {
            Some(_) => MarkerMode::Cluster,
            None => MarkerMode::Standalone,
        };
        StorageMeta {
            format: META_FORMAT,
            shards,
            physical: None,
            node,
            cluster,
            // a cluster member lays its data out under a shared WAL, a standalone node under
            // intent logs of its own
            layout: match mode {
                MarkerMode::Cluster => CLUSTER_LAYOUT,
                MarkerMode::Standalone => SHARD_LAYOUT,
                MarkerMode::Joining => CLUSTER_LAYOUT,
            },
            topology: 0,
            mode,
            incarnation: 1,
        }
    }

    /// Build the metadata for a directory claimed by a joiner that has not been admitted yet
    ///
    /// A node id and no cluster, exactly what a standalone marker holds, told apart by the mode.
    /// The cluster is filled in by [`StorageMeta::adopt_cluster`] once a seed has proved it.
    ///
    /// # Arguments
    ///
    /// * `shards` - The number of shards writing to this directory
    /// * `node` - The node the directory belongs to
    #[must_use]
    pub fn joining(shards: usize, node: NodeId) -> Self {
        StorageMeta {
            mode: MarkerMode::Joining,
            layout: CLUSTER_LAYOUT,
            ..StorageMeta::new(shards, node, None)
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
        let mut found: StorageMeta = serde_json::from_slice(&raw)?;
        // a marker from before the mode existed is read as the mode its cluster field implies:
        // a joiner never wrote one of these, so no cluster means standalone
        if found.format < FORMAT_WITH_MODE {
            found.mode = match found.cluster {
                Some(_) => MarkerMode::Cluster,
                None => MarkerMode::Standalone,
            };
            found.incarnation = 0;
            found.format = META_FORMAT;
        }
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
    /// cluster was created and not an instruction to create another every restart. Joining on
    /// top of an admitted directory is a member restart for the same reason. Every reopen bumps
    /// the incarnation and rewrites the marker before the identity is handed out, so a start
    /// that is fenced by a later one has already recorded that it happened.
    ///
    /// A directory reopened at another core count is not refused since
    /// [F47](../../../docs/src/features/local-rehome.md): the claim reports a pending rehome
    /// and the pool moves the files before a shard starts. What is refused is a change to the
    /// slots a cluster node was claimed with, more cores than slots, and a start under a third
    /// count while a rehome towards a second is on disk.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `cores` - The number of executors about to be started
    /// * `slots` - The slots a cluster node asks to claim, or none for one per core
    /// * `intent` - Whether the server is standalone, a cluster's creator or a joiner
    ///
    /// # Errors
    ///
    /// This will fail if the directory was marked in a format we cannot read, if it was
    /// written under a different layout, if it belongs to a cluster and the configuration is
    /// standalone or the reverse, if it is a joiner's that was never admitted and the
    /// configuration is anything but a joiner's, if its slots are changed or exceeded, if a
    /// rehome towards another count is in progress, or if the metadata cannot be read or written.
    #[instrument(name = "StorageMeta::claim", skip_all, err(Debug))]
    pub fn claim(
        root: &Path,
        cores: usize,
        slots: Option<usize>,
        intent: ClusterIntent,
    ) -> Result<Identity, ServerError> {
        // read whatever metadata this directory already carries, format settled first
        match Self::read(root)? {
            // this directory has been written before, so it has a shard count and an identity
            // to honour
            Some(mut found) => {
                // a cluster node's slots are claimed once: every peer's identities are keyed by
                // them, so a configuration naming another count is refused rather than obeyed
                if intent != ClusterIntent::Standalone {
                    if let Some(configured) = slots {
                        if configured != found.shards {
                            return Err(ServerError::Shoal(ShoalError::SlotsFixed {
                                claimed: found.shards,
                                configured,
                            }));
                        }
                    }
                    // and an executor with no slot to host would own nothing
                    if cores > found.shards {
                        return Err(ServerError::Shoal(ShoalError::CoresExceedSlots {
                            cores,
                            slots: found.shards,
                        }));
                    }
                }
                // a rehome on disk is resumed by the count it was planned for and refused by
                // any other; without one, a changed count is a rehome to plan
                let rehome = match super::rehome::manifest::Manifest::read(root)? {
                    Some(manifest) if manifest.to != cores => {
                        return Err(ServerError::Shoal(ShoalError::RehomeInProgress {
                            from: manifest.from,
                            to: manifest.to,
                            configured: cores,
                        }));
                    }
                    Some(manifest) => Some(PendingRehome {
                        from: manifest.from,
                        to: cores,
                    }),
                    None if found.physical() != cores => Some(PendingRehome {
                        from: found.physical(),
                        to: cores,
                    }),
                    None => None,
                };
                // the mode the directory was claimed in has to be the mode it is reopened in
                match (intent, found.mode) {
                    // a standalone directory reopened standalone, the ordinary restart
                    (ClusterIntent::Standalone, MarkerMode::Standalone) => {}
                    // a cluster member reopened as one, which keeps its cluster and never mints
                    // another however the configuration spells bootstrap; a member restarted
                    // with seeds is the same restart, since it no longer needs them
                    (ClusterIntent::Bootstrap | ClusterIntent::Join, MarkerMode::Cluster) => {}
                    // a joiner that never finished joining, started as a joiner again: the join
                    // resumes under the identity it minted
                    (ClusterIntent::Join, MarkerMode::Joining) => {}
                    // a directory bootstrapped into a cluster, opened by a standalone config
                    (ClusterIntent::Standalone, MarkerMode::Cluster) => {
                        return Err(ServerError::Shoal(ShoalError::ClusterDirectoryInStandalone {
                            // a cluster directory always names its cluster
                            cluster: found.cluster.unwrap_or_default(),
                        }));
                    }
                    // a standalone directory opened by a cluster config, which is the migration
                    // M10 owns and nothing here can do
                    (ClusterIntent::Bootstrap | ClusterIntent::Join, MarkerMode::Standalone) => {
                        return Err(ServerError::Shoal(ShoalError::StandaloneDirectoryInCluster {
                            node: found.node,
                        }));
                    }
                    // a joiner's directory that was never admitted, asked to create a cluster of
                    // its own: that would turn a node meant for one cluster into another
                    (ClusterIntent::Bootstrap, MarkerMode::Joining) => {
                        return Err(ServerError::Shoal(ShoalError::JoiningDirectoryBootstrapped {
                            node: found.node,
                        }));
                    }
                    // the same directory opened standalone, which is a node changing what it is
                    (ClusterIntent::Standalone, MarkerMode::Joining) => {
                        return Err(ServerError::Shoal(ShoalError::JoiningDirectoryInStandalone {
                            node: found.node,
                        }));
                    }
                }
                // a different layout would too, even at the same count: a cluster directory
                // at layout 1 was written by a build before the shared WAL, and its intent
                // logs would be replayed by nothing (F40)
                if found.layout != layout_for(intent) {
                    return Err(ServerError::Shoal(ShoalError::ShardLayoutMismatch {
                        found: found.layout,
                        expected: layout_for(intent),
                    }));
                }
                // this is one more start of the directory, and the marker says so before the
                // identity is handed out: a start that is later fenced has already been counted
                found.incarnation += 1;
                found.write(root)?;
                if let Some(pending) = &rehome {
                    event!(
                        Level::INFO,
                        msg = "the executor count changed; the files will be rehomed before a shard starts",
                        from = pending.from,
                        to = pending.to,
                        slots = found.shards,
                    );
                }
                Ok(Identity {
                    node: found.node,
                    // a standalone node's slots are its executors: nobody records them, so
                    // there is nothing to keep still
                    slots: match intent {
                        ClusterIntent::Standalone => cores,
                        ClusterIntent::Bootstrap | ClusterIntent::Join => found.shards,
                    },
                    physical: cores,
                    rehome,
                    cluster: found.cluster,
                    layout: found.layout,
                    topology_at_claim: found.topology,
                    fresh: false,
                    mode: found.mode,
                    incarnation: found.incarnation,
                })
            }
            // this directory has never been written to, so claim it for this node
            None => {
                // the slots a cluster node claims: what it asked for, or one per core; a
                // standalone node's slots are its cores, since nobody records them
                let shards = match intent {
                    ClusterIntent::Standalone => cores,
                    ClusterIntent::Bootstrap | ClusterIntent::Join => slots.unwrap_or(cores),
                };
                // every executor hosts at least one slot
                if shards < cores {
                    return Err(ServerError::Shoal(ShoalError::SlotsBelowCores {
                        slots: shards,
                        cores,
                    }));
                }
                // mint who this directory is going to be, and the cluster it starts if it does
                let node = NodeId::mint();
                let mut meta = match intent {
                    ClusterIntent::Standalone => StorageMeta::new(shards, node, None),
                    ClusterIntent::Bootstrap => {
                        StorageMeta::new(shards, node, Some(ClusterId::mint()))
                    }
                    ClusterIntent::Join => StorageMeta::joining(shards, node),
                };
                // a node claiming headroom is laid out on fewer executors than it has slots
                if shards != cores {
                    meta.physical = Some(cores);
                }
                // write it before any shard has had the chance to store anything
                meta.write(root)?;
                // say what we claimed, since it is what a later start is held to
                event!(
                    Level::INFO,
                    msg = "Claimed a new storage directory",
                    path = Self::path(root).display().to_string(),
                    shards,
                    cores,
                    node = node.to_string(),
                    cluster = meta.cluster.map(|cluster| cluster.to_string()),
                    mode = ?meta.mode,
                );
                Ok(Identity {
                    node,
                    slots: shards,
                    physical: cores,
                    rehome: None,
                    cluster: meta.cluster,
                    layout: meta.layout,
                    topology_at_claim: 0,
                    fresh: true,
                    mode: meta.mode,
                    incarnation: meta.incarnation,
                })
            }
        }
    }

    /// How many executors the files are laid out on
    ///
    /// The `physical` field, or `shards` for a marker that never recorded one.
    #[must_use]
    pub fn physical(&self) -> usize {
        self.physical.unwrap_or(self.shards)
    }

    /// Record that the files are laid out on another executor count: the rehome's finalize
    ///
    /// The one rewrite `physical` sees. Everything else is carried across unchanged
    /// ([F47](../../../docs/src/features/local-rehome.md)).
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `physical` - The executor count the files are on now
    ///
    /// # Errors
    ///
    /// Fails if there is no marker, or if the file cannot be written.
    #[instrument(name = "StorageMeta::finish_rehome", skip_all, err(Debug))]
    pub fn finish_rehome(root: &Path, physical: usize) -> Result<(), ServerError> {
        // the marker has to exist already: a rehome is something a claimed node does
        let Some(mut found) = Self::read(root)? else {
            return Err(ServerError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no storage marker to record a rehome in",
            )));
        };
        // the same count as the layout's origin needs no field at all
        found.physical = (physical != found.shards).then_some(physical);
        found.write(root)
    }

    /// Fill in the cluster a joiner has been admitted to, once
    ///
    /// The one time the marker's cluster field is written after the claim. A directory that is
    /// not joining is refused: a member already has its cluster and a standalone directory
    /// never adopts one, so either would be a mode change and not an adoption.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `cluster` - The cluster a seed proved and the leader admitted this node to
    ///
    /// # Errors
    ///
    /// Fails if there is no marker, if the directory is not a joiner's, or if the file cannot be
    /// written.
    #[instrument(name = "StorageMeta::adopt_cluster", skip_all, err(Debug))]
    pub fn adopt_cluster(root: &Path, cluster: ClusterId) -> Result<(), ServerError> {
        // the marker has to exist already: adopting a cluster is something a claimed node does
        let Some(mut found) = Self::read(root)? else {
            return Err(ServerError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no storage marker to adopt a cluster into",
            )));
        };
        // only a joiner adopts; anything else already knows what it is
        if found.mode != MarkerMode::Joining {
            return Err(ServerError::Shoal(ShoalError::MarkerNotJoining {
                node: found.node,
                mode: format!("{:?}", found.mode).to_lowercase(),
            }));
        }
        // move the two fields that move together, and swap the marker
        found.cluster = Some(cluster);
        found.mode = MarkerMode::Cluster;
        found.write(root)
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
        let identity = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        assert!(identity.fresh);
        assert_eq!(identity.cluster, None);
        // and has to leave behind what it claimed
        let found = StorageMeta::read(dir.path())
            .expect("no metadata written")
            .expect("no marker written");
        assert_eq!(found, StorageMeta::new(4, identity.node, None));
        assert_eq!(found.format, 3);
        assert_eq!(found.mode, MarkerMode::Standalone);
        assert_eq!(found.incarnation, 1);
        assert_eq!(identity.incarnation, 1);
        // with nothing staged left behind
        assert!(!dir.path().join(META_TEMP_FILE).exists());
    }

    /// A directory reopened by the shard count that wrote it starts, as the same node
    #[test]
    fn the_same_shard_count_is_allowed_back() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of shards
        let first = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        // reopening it with that same count is the ordinary restart, and the node is the same
        let again = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("failed to reopen with the same shard count");
        assert_eq!(again.node, first.node);
        assert!(!again.fresh);
        // and each reopen is one more start of the directory, on disk before it is handed out
        assert_eq!(again.incarnation, 2);
        let third = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("failed to reopen a third time");
        assert_eq!(third.incarnation, 3);
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.incarnation, 3);
    }

    /// A format 2 marker is read as format 3, with its mode inferred and no incarnation yet
    #[test]
    fn a_format_2_marker_is_upgraded_on_its_first_rewrite() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        std::fs::create_dir_all(dir.path()).expect("failed to build our storage directory");
        let node = NodeId::mint();
        let cluster = ClusterId::mint();
        // a cluster member's marker exactly as F37 wrote one
        let raw = format!(
            "{{\"format\": 2, \"shards\": 4, \"node\": \"{node}\", \"cluster\": \"{cluster}\", \
             \"layout\": 2, \"topology\": 7}}"
        );
        std::fs::write(StorageMeta::path(dir.path()), raw).expect("failed to stage our marker");
        // read, it is a member of its cluster with no starts counted
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.mode, MarkerMode::Cluster);
        assert_eq!(found.incarnation, 0);
        assert_eq!(found.format, 3);
        // claimed, it is that member's first counted start, and the file is now format 3
        let identity = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Bootstrap)
            .expect("a format 2 marker was refused");
        assert_eq!(identity.node, node);
        assert_eq!(identity.cluster, Some(cluster));
        assert_eq!(identity.mode, MarkerMode::Cluster);
        assert_eq!(identity.incarnation, 1);
        assert_eq!(identity.topology_at_claim, 7);
        let raw = std::fs::read_to_string(StorageMeta::path(dir.path())).expect("a marker");
        assert!(raw.contains("\"format\": 3"), "{raw}");
        assert!(raw.contains("\"mode\": \"cluster\""), "{raw}");
        // a standalone one the same way
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        std::fs::create_dir_all(dir.path()).expect("failed to build our storage directory");
        let raw = format!(
            "{{\"format\": 2, \"shards\": 4, \"node\": \"{node}\", \"cluster\": null, \
             \"layout\": 1, \"topology\": 0}}"
        );
        std::fs::write(StorageMeta::path(dir.path()), raw).expect("failed to stage our marker");
        let identity = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("a standalone format 2 marker was refused");
        assert_eq!(identity.mode, MarkerMode::Standalone);
        assert_eq!(identity.incarnation, 1);
    }

    /// A joiner mints a node and no cluster, adopts a cluster once, and is refused any other mode
    #[test]
    fn a_joiner_adopts_its_cluster_once() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // the claim mints an identity that belongs to nothing yet
        let joiner = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Join)
            .expect("failed to claim a joiner's directory");
        assert!(joiner.fresh);
        assert_eq!(joiner.cluster, None);
        assert_eq!(joiner.mode, MarkerMode::Joining);
        assert_eq!(joiner.incarnation, 1);
        // a joiner that never finished can be started as a joiner again, as the same node
        let again = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Join)
            .expect("failed to resume a join");
        assert_eq!(again.node, joiner.node);
        assert_eq!(again.mode, MarkerMode::Joining);
        assert_eq!(again.incarnation, 2);
        // but not as a cluster's creator, and not standalone
        let error = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect_err("a joiner's directory bootstrapped a cluster");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::JoiningDirectoryBootstrapped { node }) if node == joiner.node
        ));
        assert!(format!("{error}").contains("second cluster"), "{error}");
        let error = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone)
            .expect_err("a joiner's directory started standalone");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::JoiningDirectoryInStandalone { .. })
        ));
        // admission fills in the cluster, exactly once
        let cluster = ClusterId::mint();
        StorageMeta::adopt_cluster(dir.path(), cluster).expect("failed to adopt a cluster");
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.cluster, Some(cluster));
        assert_eq!(found.mode, MarkerMode::Cluster);
        assert_eq!(found.node, joiner.node);
        assert_eq!(found.incarnation, 2);
        let error = StorageMeta::adopt_cluster(dir.path(), ClusterId::mint())
            .expect_err("a member adopted a second cluster");
        assert!(matches!(error, ServerError::Shoal(ShoalError::MarkerNotJoining { .. })));
        // and from then on it is a member, restarted with seeds or with bootstrap alike
        let member = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Join)
            .expect("failed to restart a joined member with its seeds");
        assert_eq!(member.cluster, Some(cluster));
        assert_eq!(member.mode, MarkerMode::Cluster);
        assert_eq!(member.incarnation, 3);
        StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect("failed to restart a joined member as a bootstrapper");
        // a standalone directory never adopts one either
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone).expect("failed to claim");
        assert!(StorageMeta::adopt_cluster(dir.path(), cluster).is_err());
        // and a standalone directory refuses a joiner's configuration, naming the migration
        let error = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Join)
            .expect_err("a standalone directory joined a cluster");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StandaloneDirectoryInCluster { .. })
        ));
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
        let error = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect_err("a format 1 marker started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageFormatMismatch {
                found: 1,
                supported: &[2, 3]
            })
        ));
        // and the message has to say a marker is never migrated in place and where the way
        // out is, since that is what an operator holding one of these needs to know
        // ([F48](../../../docs/src/features/rolling-compatibility.md))
        let rendered = format!("{error}");
        assert!(rendered.contains("format 1"), "{rendered}");
        assert!(rendered.contains("never migrated") && rendered.contains("import or restore"), "{rendered}");
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
        let error = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect_err("an unknown marker format started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageFormatMismatch { found: 4, .. })
        ));
    }

    /// A directory reopened by a different core count is a pending rehome, not a refusal
    ///
    /// ~~A directory reopened by a different shard count is refused~~ Since F47 the claim
    /// reports where the files are and where they have to go, and the pool moves them before a
    /// shard starts. The marker's `physical` moves only when the rehome finalizes, so a claim
    /// that dies before then finds the same pending rehome again.
    #[test]
    fn a_changed_core_count_is_a_pending_rehome() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of cores
        let first = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("failed to claim a new directory");
        assert_eq!(first.slots, 4);
        assert_eq!(first.physical, 4);
        assert_eq!(first.rehome, None);
        // reopening it with another count is a rehome from the old count to the new
        let again = StorageMeta::claim(dir.path(), 5, None, ClusterIntent::Standalone)
            .expect("a changed core count was refused");
        assert_eq!(again.node, first.node);
        assert_eq!(again.rehome, Some(PendingRehome { from: 4, to: 5 }));
        assert_eq!(again.physical, 5);
        assert_eq!(again.slots, 5, "a standalone node's slots are its executors");
        // nothing moved on disk: the marker still says four until the rehome finalizes
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.physical(), 4);
        assert_eq!(found.physical, None);
        // and a third claim at five finds the same rehome pending
        let third = StorageMeta::claim(dir.path(), 5, None, ClusterIntent::Standalone)
            .expect("a pending rehome was refused");
        assert_eq!(third.rehome, Some(PendingRehome { from: 4, to: 5 }));
        // once the rehome finalizes, five is the ordinary restart and four is a rehome back
        StorageMeta::finish_rehome(dir.path(), 5).expect("failed to finish a rehome");
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.physical, Some(5));
        assert_eq!(found.shards, 4);
        let settled = StorageMeta::claim(dir.path(), 5, None, ClusterIntent::Standalone)
            .expect("the finished count was refused");
        assert_eq!(settled.rehome, None);
        let back = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Standalone)
            .expect("a rehome back was refused");
        assert_eq!(back.rehome, Some(PendingRehome { from: 5, to: 4 }));
        // a rehome back to the origin count clears the field rather than recording it
        StorageMeta::finish_rehome(dir.path(), 4).expect("failed to finish a rehome");
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.physical, None);
        // a manifest towards one count on disk refuses a start under another, by name
        let hosting = crate::server::hosting::Hosting::identity(4);
        let after = hosting.plan(2, false).expect("a plan");
        let manifest = super::super::rehome::manifest::Manifest::plan(&hosting, &after, &[], false);
        manifest.write(dir.path()).expect("a manifest");
        let error = StorageMeta::claim(dir.path(), 3, None, ClusterIntent::Standalone)
            .expect_err("a third count started over a rehome in progress");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::RehomeInProgress { from: 4, to: 2, configured: 3 })
        ));
        // and resumes it under the count it was planned for
        let resumed = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone)
            .expect("the planned count was refused");
        assert_eq!(resumed.rehome, Some(PendingRehome { from: 4, to: 2 }));
        // an unclaimed directory has no rehome to finish
        let empty = tempfile::tempdir().expect("failed to build a temp dir");
        assert!(StorageMeta::finish_rehome(empty.path(), 1).is_err());
    }

    /// A cluster node's slots are claimed once and bound the cores it may run
    ///
    /// The default is one slot per core; `cluster.slots` above the cores reserves headroom;
    /// below them it is refused, and so is any later change to it and any core count past it.
    #[test]
    fn slots_are_claimed_once_and_bound_the_cores() {
        // one slot per core by default
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let plain = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        assert_eq!(plain.slots, 2);
        assert_eq!(plain.physical, 2);
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.shards, 2);
        assert_eq!(found.physical, None);
        // headroom: four slots on two cores, recorded as laid out on two
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let roomy = StorageMeta::claim(dir.path(), 2, Some(4), ClusterIntent::Bootstrap)
            .expect("failed to bootstrap with headroom");
        assert_eq!(roomy.slots, 4);
        assert_eq!(roomy.physical, 2);
        assert_eq!(roomy.rehome, None);
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.shards, 4);
        assert_eq!(found.physical, Some(2));
        // the same slots again is the ordinary restart, and no slots at all is too
        StorageMeta::claim(dir.path(), 2, Some(4), ClusterIntent::Bootstrap).expect("a restart");
        StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap).expect("a restart");
        // other slots are refused by name
        let error = StorageMeta::claim(dir.path(), 2, Some(3), ClusterIntent::Bootstrap)
            .expect_err("a slot change started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::SlotsFixed { claimed: 4, configured: 3 })
        ));
        assert!(format!("{error}").contains("Replace"), "{error}");
        // growth up to the slots is a rehome; past them a refusal naming the ceiling
        let grown = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Bootstrap)
            .expect("growth to the slots was refused");
        assert_eq!(grown.rehome, Some(PendingRehome { from: 2, to: 4 }));
        let error = StorageMeta::claim(dir.path(), 5, None, ClusterIntent::Bootstrap)
            .expect_err("growth past the slots started");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::CoresExceedSlots { cores: 5, slots: 4 })
        ));
        // fewer slots than cores is refused at the claim
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let error = StorageMeta::claim(dir.path(), 4, Some(2), ClusterIntent::Bootstrap)
            .expect_err("fewer slots than cores were claimed");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::SlotsBelowCores { slots: 2, cores: 4 })
        ));
        assert!(StorageMeta::read(dir.path()).expect("a read").is_none(), "a refused claim wrote a marker");
        // a standalone node ignores slots: its slots are its cores
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let alone = StorageMeta::claim(dir.path(), 3, Some(8), ClusterIntent::Standalone)
            .expect("failed to claim standalone");
        assert_eq!(alone.slots, 3);
    }

    /// A bootstrap mints a cluster once, and a restart keeps it rather than minting another
    #[test]
    fn a_bootstrap_mints_one_cluster_and_keeps_it() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // the first bootstrap creates the cluster
        let first = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap a new directory");
        let cluster = first.cluster.expect("a bootstrap minted no cluster");
        // a restart with bootstrap still set is the same cluster and the same node
        let again = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
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
        let bootstrapped = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        let error = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone)
            .expect_err("a cluster directory started standalone");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::ClusterDirectoryInStandalone { cluster })
                if Some(cluster) == bootstrapped.cluster
        ));
        // a standalone directory opened as a cluster member, which names the migration path
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let standalone = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone)
            .expect("failed to claim");
        let error = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
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
        let identity = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
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
        let standalone = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Standalone)
            .expect("failed to claim");
        assert!(standalone.verify_cluster(other).is_err());
    }

    /// Observing a topology rewrites that one field, never goes backwards, and survives a restart
    ///
    /// The incarnation is the other field a restart moves, and the claim above already holds
    /// it; an observation leaves it where the claim put it.
    #[test]
    fn a_topology_observation_moves_one_field() {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let identity = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
            .expect("failed to bootstrap");
        // record a version
        StorageMeta::observe_topology(dir.path(), 3).expect("failed to observe a topology");
        let found = StorageMeta::read(dir.path()).expect("a marker").expect("a marker");
        assert_eq!(found.topology, 3);
        // and nothing else moved
        assert_eq!(found.node, identity.node);
        assert_eq!(found.cluster, identity.cluster);
        assert_eq!(found.shards, 2);
        assert_eq!(found.layout, CLUSTER_LAYOUT);
        assert_eq!(found.incarnation, identity.incarnation);
        assert_eq!(found.mode, MarkerMode::Cluster);
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
        let again = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap)
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
