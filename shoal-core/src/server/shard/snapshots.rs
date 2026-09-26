//! The snapshot half of a shard: receiving a stream, assembling it, and installing it
//!
//! A leader whose follower is behind the purge point sends it a snapshot
//! ([F43](../../../../docs/src/features/node-recovery.md)): a begin RPC on the replication
//! lane, chunks on the bulk lane, an end RPC that waits for the install. This file is the
//! receiving side, on the shard that hosts the group. The begin is judged on the loop - the
//! schema, the table, the tablets, the bound on partial bytes, whether the boundary is already
//! applied - and answered with where to start. Chunks arrive as
//! [`ServerMsg::SnapshotBytes`] and are queued on the group's [`Partial`]; a writer task
//! drains the queue into `wal/Shard-N/install/<group>.part`, judging each chunk by the
//! assembler. The end waits on a task until the prefix is whole, verifies it against the
//! manifest, writes the pending marker, and hands the file to openraft, which asks the
//! state machine to install it; the state machine posts [`ServerMsg::InstallSnapshot`] and
//! the loop installs it in the steps the marker makes redoable.
//!
//! # Invariants
//!
//! **The marker is durable before openraft is told.** openraft purges the group's log through
//! the boundary once it has the snapshot, and a purge that lands before a marker would leave a
//! restart with neither the log nor the file it needs. From the marker on, an install is
//! redone at open.
//!
//! **One partial per group, one install per group at a time, and a begin for a different
//! stream replaces the partial.** A source that failed over sends a new stream; the old
//! prefix is nothing the new stream can continue.
//!
//! **Nothing here holds a `RefCell` borrow across an `.await`.** The writer task takes a chunk
//! out of the queue, drops the borrow, writes, and borrows again to judge the next.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::io::{BufferedFile, OpenOptions};
use openraft::{Snapshot, SnapshotMeta};
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level};

use super::Shard;
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::peer::ReplicateReply;
use crate::server::replication::install::{crash_point, CrashPoint, Offer, Partial};
use crate::server::replication::snapshot::{
    SnapshotAnswer, SnapshotManifest, SnapshotRpc, INSTALL_DIR,
};
use crate::server::replication::{SnapshotData, SnapshotStats};
use crate::server::wal::write_atomic;
use crate::server::ServerError;
use crate::shared::identity::{GroupId, NodeId};
use crate::shared::protocol::peer::ReplicateRequestHead;
use crate::shared::traits::{QuerySupport, TableNameSupport};

/// How often the end of a stream checks whether its prefix is whole
const END_POLL: Duration = Duration::from_millis(5);

/// How long an end waits for the prefix to grow before it answers a resume
///
/// A second: the sender said every byte was sent, so bytes still to come are in a buffer
/// somewhere and arrive within it; a prefix that stands still that long is short of bytes
/// that were lost, and the sender resends from it.
const END_STALL: Duration = Duration::from_secs(1);

/// The most bytes queued for one partial's writer before a chunk is dropped
///
/// Sixteen mebibytes: the writer drains a chunk in the time a disk write takes, so the queue
/// only grows when the disk is slower than the lane, and a chunk dropped here is recovered by
/// the resume offset at the stream's end. Not the bulk lane's queue bound, which is the
/// sender's flow-control window and may be set far smaller.
const INSTALL_QUEUE_BYTES: usize = 16 * 1024 * 1024;

/// A snapshot install in progress on the loop
pub(super) struct ActiveInstall {
    /// What is being installed
    pub(super) manifest: SnapshotManifest,
    /// The verified file
    pub(super) path: PathBuf,
    /// Whoever asked for the install: openraft's worker, or the task building the group
    pub(super) done: Option<futures_channel::oneshot::Sender<Result<(), String>>>,
    /// The checkpoint write that carries the installed state, once the state is set
    pub(super) checkpoint_version: Option<u64>,
    /// Whether this is a redo from a marker found at open
    pub(super) redone: bool,
    /// Whether this is a repair's install, whose merged tail is merged again once it lands
    /// ([F44](../../../../docs/src/features/repair.md))
    pub(super) repair: bool,
}

/// Find the markers a crash left behind and judge each against the checkpoint file
///
/// A marker whose file verifies and whose boundary is past the group's checkpoint is kept for
/// the group to install as it is built; one at or below the checkpoint was installed already
/// and is cleaned up; one whose file does not verify is dropped with a warning, since the
/// marker precedes every archive write and the old generation is whole. A partial with no
/// marker is a stream that never finished, and goes too
/// ([F43](../../../../docs/src/features/node-recovery.md)).
///
/// # Arguments
///
/// * `installs` - Where the markers and partials live
/// * `checkpoint` - The checkpoint file as read at open
pub(super) async fn scan_pending(
    installs: &Installs,
    checkpoint: &crate::server::wal::Checkpoint,
) -> HashMap<GroupId, (PathBuf, SnapshotManifest)> {
    let mut pending = HashMap::new();
    let Ok(entries) = std::fs::read_dir(&installs.dir) else {
        return pending;
    };
    let mut markers = Vec::new();
    let mut parts = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(stem) = name.strip_suffix(".pending") {
            if let Ok(group) = u64::from_str_radix(stem, 16) {
                markers.push((GroupId(group), entry.path()));
            }
        } else if let Some(stem) = name.strip_suffix(".part") {
            if let Ok(group) = u64::from_str_radix(stem, 16) {
                parts.push((GroupId(group), entry.path()));
            }
        }
    }
    for (group, marker) in markers {
        let part = installs.part_path(group);
        // the manifest, and the file it describes
        let manifest = std::fs::read(&marker)
            .ok()
            .and_then(|bytes| postcard::from_bytes::<SnapshotManifest>(&bytes).ok());
        let verified = match &manifest {
            Some(manifest) => crate::server::replication::snapshot::verify(&part, manifest).await,
            None => Err(std::io::Error::other("the marker does not decode")),
        };
        match (manifest, verified) {
            (Some(manifest), Ok(())) => {
                let installed = checkpoint
                    .get(group)
                    .and_then(|point| point.applied.as_ref())
                    .is_some_and(|applied| applied.index >= manifest.boundary.index);
                if installed {
                    // the checkpoint passed it: the install completed and only the cleanup is left
                    event!(Level::INFO, msg = "a snapshot install's marker outlived its checkpoint; cleaning up", group = %group);
                    let _ = std::fs::remove_file(&marker);
                    let _ = std::fs::remove_file(&part);
                } else {
                    event!(Level::WARN, msg = "a snapshot install was interrupted; redoing it", group = %group, boundary = manifest.boundary.index);
                    pending.insert(group, (part, manifest));
                }
            }
            (manifest, verified) => {
                // the file is not what the marker says, or the marker says nothing: the old
                // generation is whole, since the marker precedes every archive write, so the
                // install is simply forgotten
                let error = verified
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default();
                event!(Level::WARN, msg = "a snapshot install's file does not verify; dropping the marker", group = %group, error, decoded = manifest.is_some());
                let _ = std::fs::remove_file(&marker);
                let _ = std::fs::remove_file(&part);
            }
        }
    }
    // a partial with no marker is a stream that never finished
    for (group, part) in parts {
        if !pending.contains_key(&group) && !installs.marker_path(group).exists() {
            let _ = std::fs::remove_file(&part);
        }
    }
    pending
}

/// The partials a shard is assembling, and the bound on them
pub(super) struct Installs {
    /// The directory the partials and the markers live in
    pub(super) dir: PathBuf,
    /// The most partial bytes held on disk before a begin is refused
    pub(super) bound: u64,
    /// The most bytes queued for a writer before a chunk is dropped
    pub(super) queue_bound: usize,
    /// One partial per group
    pub(super) partials: HashMap<GroupId, Rc<RefCell<Partial>>>,
    /// The stream each group last installed, so an end sent again after its answer was lost
    /// is answered installed rather than refused
    pub(super) installed: HashMap<GroupId, [u8; 16]>,
    /// What the partials that were installed or discarded counted
    pub(super) stats: SnapshotStats,
}

impl Installs {
    /// The partials' state, fresh
    ///
    /// # Arguments
    ///
    /// * `dir` - The shard's WAL directory, under which the install directory goes
    /// * `bound` - The most partial bytes held on disk
    pub(super) fn new(dir: &std::path::Path, bound: u64) -> Self {
        Installs {
            dir: dir.join(INSTALL_DIR),
            bound,
            queue_bound: INSTALL_QUEUE_BYTES,
            partials: HashMap::new(),
            installed: HashMap::new(),
            stats: SnapshotStats::default(),
        }
    }

    /// The partial file of a group
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn part_path(&self, group: GroupId) -> PathBuf {
        self.dir.join(format!("{group}.part"))
    }

    /// The pending marker of a group
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn marker_path(&self, group: GroupId) -> PathBuf {
        self.dir.join(format!("{group}.pending"))
    }

    /// The marker's file name
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn marker_name(group: GroupId) -> String {
        format!("{group}.pending")
    }

    /// Forget a group's partial, folding what it counted into the totals
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn retire(&mut self, group: GroupId) {
        if let Some(partial) = self.partials.remove(&group) {
            let partial = partial.borrow();
            self.stats.chunks += partial.assembler.chunks;
            self.stats.duplicate_chunks += partial.assembler.duplicates;
            self.stats.dropped_chunks += partial.assembler.dropped;
            self.stats.bytes_received += partial.assembler.next;
            self.stats.resumed += u64::from(partial.resumed);
        }
    }

    /// What the partials have counted, the retired ones and the live ones together
    pub(super) fn stats(&self) -> SnapshotStats {
        let mut stats = self.stats;
        for partial in self.partials.values() {
            let partial = partial.borrow();
            stats.chunks += partial.assembler.chunks;
            stats.duplicate_chunks += partial.assembler.duplicates;
            stats.dropped_chunks += partial.assembler.dropped;
            stats.bytes_received += partial.assembler.next;
            stats.resumed += u64::from(partial.resumed);
        }
        stats
    }
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Answer a snapshot RPC a peer sent this shard: the begin now, the end from a task
    ///
    /// # Arguments
    ///
    /// * `origin` - The peer
    /// * `head` - The request's fixed fields
    /// * `payload` - The RPC body
    /// * `version` - The wire version the payload is encoded at, from the frame's header
    /// * `reply` - Where the answer goes
    pub(super) fn handle_snapshot_rpc(
        &mut self,
        origin: NodeId,
        head: ReplicateRequestHead,
        payload: Vec<u8>,
        version: u8,
        reply: kanal::AsyncSender<ReplicateReply>,
    ) {
        let group = GroupId(head.group);
        // decoded at the version the frame named, which is the manifest's codec
        // ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let rpc = match SnapshotRpc::decode_at(&payload, version) {
            Ok(rpc) => rpc,
            Err(error) => {
                let _ = reply.try_send(ReplicateReply::error(
                    head.id,
                    format!("decoding a snapshot rpc at wire version {version}: {error}"),
                ));
                return;
            }
        };
        let answer = match rpc {
            // a manifest from a version 4 link names no cluster: it is this cluster's, from
            // the sender it was heard from
            SnapshotRpc::Begin {
                vote,
                stream,
                manifest,
                repair,
            } => {
                let cluster = self.map.get().cluster.unwrap_or_default();
                let manifest = manifest.filled(cluster, origin);
                self.begin_snapshot(origin, group, stream, vote, manifest, repair)
            }
            SnapshotRpc::End {
                stream,
                total,
                checksum,
            } => {
                self.end_snapshot(origin, group, stream, total, checksum, head, reply);
                return;
            }
        };
        let _ = reply.try_send(encode_answer(head.id, &answer));
    }

    /// Judge a begin: what is coming, against what this shard hosts and holds
    ///
    /// # Arguments
    ///
    /// * `origin` - The peer
    /// * `group` - The group
    /// * `stream` - The stream
    /// * `vote` - The sender's vote
    /// * `manifest` - What is coming
    /// * `repair` - The repair operation the stream serves, if it is one
    fn begin_snapshot(
        &mut self,
        origin: NodeId,
        group: GroupId,
        stream: [u8; 16],
        vote: crate::server::wal::Vote,
        manifest: SnapshotManifest,
        repair: Option<uuid::Uuid>,
    ) -> SnapshotAnswer {
        let schema_id = <D::ClientType as QuerySupport>::SCHEMA_ID;
        let Some(replication) = self.replication.as_mut() else {
            return SnapshotAnswer::Refused("this node hosts no tablet groups".to_string());
        };
        let Some(slot) = replication.groups.get(&group) else {
            return SnapshotAnswer::Refused(format!("group {group} is not hosted on this shard"));
        };
        // what is coming has to be this group of this table under this schema
        if manifest.schema_id != schema_id {
            return SnapshotAnswer::Refused(format!(
                "the snapshot is of schema {:016x} and this node serves {schema_id:016x}",
                manifest.schema_id
            ));
        }
        if manifest.group != group || manifest.table != slot.table.table_id() {
            return SnapshotAnswer::Refused(format!(
                "the snapshot names group {} of table {} and this is group {group} of table {}",
                manifest.group,
                manifest.table,
                slot.table.table_id()
            ));
        }
        if manifest.tablets != slot.spec.tablets {
            return SnapshotAnswer::Refused(format!(
                "the snapshot covers tablets {:?} and the group serves {:?}",
                manifest.tablets, slot.spec.tablets
            ));
        }
        let (applied, installing, checkpoint, quarantined) = {
            let state = slot.state.borrow();
            (
                state.applied_index(),
                state.installing,
                state.checkpoint_index(),
                state.quarantined.is_some(),
            )
        };
        if installing {
            return SnapshotAnswer::Refused(format!(
                "group {group} is installing a snapshot already"
            ));
        }
        // the group's own replication streams nothing to a copy with no handle, and never over
        // a repair's stream: there is one partial a group, and on the lab openraft's transmitter
        // replaced a stalled copy's repair stream every few seconds, so each repair's end found
        // the other stream and was refused ([Resolved #163](../../../../docs/src/appendix/resolved/repair-stream-replaced.md))
        if repair.is_none() {
            if slot.raft.is_none() {
                return SnapshotAnswer::Refused(format!("group {group} is still starting"));
            }
            let repairing = replication
                .installs
                .partials
                .get(&group)
                .is_some_and(|partial| {
                    let partial = partial.borrow();
                    partial.repair.is_some() && partial.is_assembling()
                });
            if repairing {
                return SnapshotAnswer::Refused(format!(
                    "a repair's snapshot of group {group} is being received"
                ));
            }
        }
        // a repair stream replaces a quarantined copy that is live and applied past the
        // boundary: judged against the checkpoint the group will be restarted from, which is
        // held where it is until the restart ([F44](../../../../docs/src/features/repair.md))
        if repair.is_some() {
            if !quarantined {
                return SnapshotAnswer::Refused(format!(
                    "this shard's copy of group {group} is not quarantined"
                ));
            }
            if manifest.boundary.index <= checkpoint {
                return SnapshotAnswer::Behind { checkpoint };
            }
            let hold = self
                .conf
                .cluster
                .as_ref()
                .map_or(Duration::from_secs(300), |cluster| {
                    cluster.replication.snapshot_timeout.duration()
                });
            slot.state.borrow_mut().hold_checkpoint_until = Some(Instant::now() + hold);
        } else if manifest.boundary.index <= applied {
            // a boundary already applied here needs nothing: the sender moves on from it
            let vote = slot
                .raft
                .as_ref()
                .map(|raft| raft.metrics().borrow_watched().vote.clone());
            return match vote {
                Some(vote) => SnapshotAnswer::Installed { vote },
                None => SnapshotAnswer::Refused(format!("group {group} is still starting")),
            };
        }
        // the same stream again resumes from the prefix held; another stream replaces it
        if let Some(partial) = replication.installs.partials.get(&group) {
            let mut partial = partial.borrow_mut();
            if partial.stream == stream && partial.from == origin && partial.failed.is_none() {
                partial.resumed = true;
                return SnapshotAnswer::Resume {
                    from: partial.assembler.next,
                };
            }
        }
        // the stream budget: as many streams assembling at once as the node allows, another
        // is refused and the sender's backoff tries again; and the disk reserve: what the
        // stream would land has to leave the reserve free, judged here where the bytes would
        // go so a stale report at the planner is caught ([F46](../../../../docs/src/features/capacity-rebalancing.md))
        let assembling = replication
            .installs
            .partials
            .iter()
            .filter(|(other, partial)| **other != group && partial.borrow().is_assembling())
            .count();
        let (concurrent_streams, disk_reserve) =
            self.conf.cluster.as_ref().map_or((u32::MAX, 0), |cluster| {
                (
                    cluster.migration.concurrent_streams,
                    cluster.migration.disk_reserve,
                )
            });
        if assembling >= concurrent_streams as usize {
            replication.installs.stats.refused_budget += 1;
            return SnapshotAnswer::Refused(format!(
                "stream budget: {assembling} streams installing on this shard, which is as many as it takes at once"
            ));
        }
        let root = &self.conf.storage.default.filesystem.latency_sensitive.path;
        if let Some(free) = crate::server::control::capacity::free_bytes(root) {
            let need = manifest.total.saturating_add(disk_reserve);
            if free < need {
                replication.installs.stats.refused_reserve += 1;
                return SnapshotAnswer::Refused(format!(
                    "disk reserve: free {free}, need {} for the stream plus the {disk_reserve} byte reserve",
                    manifest.total
                ));
            }
        }
        // the bound on partial bytes, over every other group's partial and this one
        let held: u64 = replication
            .installs
            .partials
            .iter()
            .filter(|(other, _)| **other != group)
            .map(|(_, partial)| partial.borrow().manifest.total)
            .sum();
        if held + manifest.total > replication.installs.bound {
            return SnapshotAnswer::Refused(format!(
                "{} bytes of partial snapshots are held and {} more would pass the {} byte bound",
                held, manifest.total, replication.installs.bound
            ));
        }
        replication.installs.retire(group);
        let mut partial = Partial::new(origin, stream, vote, manifest);
        partial.repair = repair;
        replication
            .installs
            .partials
            .insert(group, Rc::new(RefCell::new(partial)));
        // the most at once, for the budget test and the capture
        let assembling = u64::try_from(assembling + 1).unwrap_or(u64::MAX);
        replication.installs.stats.peak_streams =
            replication.installs.stats.peak_streams.max(assembling);
        SnapshotAnswer::Resume { from: 0 }
    }

    /// Take a chunk of a stream onto its group's partial, and start its writer if none runs
    ///
    /// # Arguments
    ///
    /// * `node` - The peer sending the stream
    /// * `stream` - The stream
    /// * `offset` - Where the bytes go
    /// * `bytes` - The bytes
    pub(super) fn handle_snapshot_bytes(
        &mut self,
        node: NodeId,
        stream: [u8; 16],
        offset: u64,
        bytes: Vec<u8>,
    ) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        // the partial this stream fills, if one is being assembled
        let found = replication
            .installs
            .partials
            .iter()
            .find(|(_, partial)| {
                let partial = partial.borrow();
                partial.stream == stream && partial.from == node
            })
            .map(|(group, partial)| (*group, partial.clone()));
        let Some((group, partial)) = found else {
            replication.installs.stats.dropped_chunks += 1;
            return;
        };
        let path = replication.installs.part_path(group);
        let queue_bound = replication.installs.queue_bound;
        let spawn = {
            let mut partial = partial.borrow_mut();
            // a queue at its bound drops the chunk; the resume offset recovers it
            if partial.queued_bytes + bytes.len() > queue_bound {
                partial.assembler.dropped += 1;
                return;
            }
            partial.queued_bytes += bytes.len();
            partial.queue.push_back((offset, bytes));
            partial.lane_lost = false;
            !partial.writing
        };
        if spawn {
            partial.borrow_mut().writing = true;
            glommio::spawn_local(write_partial(partial, path)).detach();
        }
    }

    /// Note that a peer's bulk lane ended, so every partial it fed answers a resume at its end
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    pub(super) fn handle_bulk_lane_ended(&mut self, node: NodeId) {
        let Some(replication) = self.replication.as_ref() else {
            return;
        };
        for partial in replication.installs.partials.values() {
            let mut partial = partial.borrow_mut();
            if partial.from == node && !partial.assembler.complete() {
                partial.lane_lost = true;
            }
        }
    }

    /// Wait for a stream's end on a task, verify it, and hand it to openraft
    ///
    /// # Arguments
    ///
    /// * `origin` - The peer
    /// * `group` - The group
    /// * `stream` - The stream
    /// * `total` - How many bytes the sender says it sent
    /// * `checksum` - What it says they hash to
    /// * `head` - The request's fixed fields
    /// * `reply` - Where the answer goes
    #[allow(clippy::too_many_arguments)]
    fn end_snapshot(
        &mut self,
        origin: NodeId,
        group: GroupId,
        stream: [u8; 16],
        total: u64,
        checksum: u64,
        head: ReplicateRequestHead,
        reply: kanal::AsyncSender<ReplicateReply>,
    ) {
        let Some(replication) = self.replication.as_ref() else {
            let _ = reply.try_send(ReplicateReply::error(
                head.id,
                "this node hosts no tablet groups",
            ));
            return;
        };
        let (partial, raft, path, marker_dir, marker_name) = {
            let Some(partial) = replication.installs.partials.get(&group).cloned() else {
                // a stream installed already, whose end is asked again because its answer
                // was lost with the link, is installed; anything else is nothing
                let installed = replication
                    .installs
                    .installed
                    .get(&group)
                    .is_some_and(|last| *last == stream);
                let vote = replication
                    .groups
                    .get(&group)
                    .and_then(|slot| slot.raft.as_ref())
                    .map(|raft| raft.metrics().borrow_watched().vote.clone());
                let answer = match (installed, vote) {
                    (true, Some(vote)) => SnapshotAnswer::Installed { vote },
                    _ => SnapshotAnswer::Refused(format!(
                        "no stream of group {group} is being assembled"
                    )),
                };
                let _ = reply.try_send(encode_answer(head.id, &answer));
                return;
            };
            let raft = replication
                .groups
                .get(&group)
                .and_then(|slot| slot.raft.clone());
            (
                partial,
                raft,
                replication.installs.part_path(group),
                replication.installs.dir.clone(),
                Installs::marker_name(group),
            )
        };
        // a repair stream is installed by restarting the group, which a copy whose start
        // stalled can be without a handle; any other install is handed to the handle
        // ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md))
        if raft.is_none() && partial.borrow().repair.is_none() {
            let _ = reply.try_send(encode_answer(
                head.id,
                &SnapshotAnswer::Refused(format!("group {group} is still starting")),
            ));
            return;
        }
        {
            let partial = partial.borrow();
            if partial.stream != stream || partial.from != origin {
                let _ = reply.try_send(encode_answer(
                    head.id,
                    &SnapshotAnswer::Refused(
                        "the end names a stream that is not being assembled".to_string(),
                    ),
                ));
                return;
            }
            if partial.manifest.total != total || partial.manifest.checksum != checksum {
                let _ = reply.try_send(encode_answer(
                    head.id,
                    &SnapshotAnswer::Refused(
                        "the end does not describe the stream its begin announced".to_string(),
                    ),
                ));
                return;
            }
        }
        let deadline = Duration::from_millis(u64::from(head.deadline_ms.max(1)));
        // the install is handed to openraft under the sender's vote, which it judges as it
        // judges an append: a stale one is refused with this replica's own
        let vote = partial.borrow().vote.clone();
        let loop_tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            let started = Instant::now();
            // where the prefix stood when it last grew, and when
            let mut last_next = partial.borrow().assembler.next;
            let mut last_grew = Instant::now();
            // wait until the prefix is whole and written, or the sender's deadline passes
            let answer = loop {
                let (complete, writing, failed, next, lane_lost) = {
                    let partial = partial.borrow();
                    (
                        partial.assembler.complete(),
                        partial.writing,
                        partial.failed.clone(),
                        partial.assembler.next,
                        partial.lane_lost,
                    )
                };
                if let Some(failed) = failed {
                    break SnapshotAnswer::Refused(format!(
                        "the partial could not be written: {failed}"
                    ));
                }
                if complete && !writing {
                    break install_received(
                        &partial,
                        raft.as_ref(),
                        &path,
                        &marker_dir,
                        &marker_name,
                        vote.clone(),
                        &loop_tx,
                    )
                    .await;
                }
                if next != last_next {
                    last_next = next;
                    last_grew = Instant::now();
                }
                // short of bytes with the lane gone, with the prefix standing still, or at the
                // sender's deadline: the sender resends from the prefix, which is a resume
                let stalled = !writing && last_grew.elapsed() >= END_STALL;
                if (lane_lost && !writing) || stalled || started.elapsed() >= deadline {
                    let mut partial = partial.borrow_mut();
                    partial.resumed = true;
                    partial.lane_lost = false;
                    break SnapshotAnswer::Resume { from: next };
                }
                glommio::timer::sleep(END_POLL).await;
            };
            let _ = reply.send(encode_answer(head.id, &answer)).await;
        })
        .detach();
    }
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// The group installing a snapshot that a read's tablets belong to, if any
    ///
    /// # Arguments
    ///
    /// * `query` - The read
    pub(super) fn installing_group(
        &self,
        query: &<D::ClientType as QuerySupport>::QueryKinds,
    ) -> Option<GroupId> {
        use crate::shared::traits::ShoalQuerySupport as _;
        let replication = self.replication.as_ref()?;
        let table = D::ClientType::query_table_name(query).table_id();
        for key in query.partition_keys() {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            let Some(group) = replication.tablets.get(&(table, tablet)) else {
                continue;
            };
            if replication
                .groups
                .get(group)
                .is_some_and(|slot| slot.state.borrow().installing)
            {
                return Some(*group);
            }
        }
        None
    }

    /// Start installing a received snapshot: mark the group, and hand the file to its table
    ///
    /// A persistent table's compactor streams the records into the archives; an ephemeral
    /// table's records are read on a task and put in place by the loop. Either way the loop
    /// hears back as a message and carries on from there
    /// ([F43](../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `path` - The verified file
    /// * `manifest` - What it is
    /// * `meta` - What openraft was told the snapshot is
    /// * `done` - Fired once the install is durable, or with why it is not
    pub(super) async fn handle_install_snapshot(
        &mut self,
        group: GroupId,
        path: PathBuf,
        manifest: SnapshotManifest,
        meta: openraft::type_config::alias::SnapshotMetaOf<crate::server::replication::DataConfig>,
        done: futures_channel::oneshot::Sender<Result<(), String>>,
    ) -> Result<(), ServerError> {
        let _ = meta;
        let Some(replication) = self.replication.as_mut() else {
            let _ = done.send(Err("this node hosts no tablet groups".to_string()));
            return Ok(());
        };
        let Some(slot) = replication.groups.get(&group) else {
            let _ = done.send(Err(format!("group {group} is not hosted on this shard")));
            return Ok(());
        };
        if replication.active_installs.contains_key(&group) {
            let _ = done.send(Err(format!(
                "group {group} is installing a snapshot already"
            )));
            return Ok(());
        }
        // a redo is one whose file the open found under a marker; a repair's is one the
        // group was restarted for
        let (redone, repair) = {
            let state = slot.state.borrow();
            (
                state
                    .pending_install
                    .as_ref()
                    .is_some_and(|(pending, _)| *pending == path)
                    && !state.repair_pending,
                state.repair_pending,
            )
        };
        slot.state.borrow_mut().installing = true;
        let table = slot.table;
        let tablets = manifest.tablets.clone();
        let volatile = slot.store.is_volatile();
        replication.active_installs.insert(
            group,
            ActiveInstall {
                manifest,
                path: path.clone(),
                done: Some(done),
                checkpoint_version: None,
                redone,
                repair,
            },
        );
        event!(Level::INFO, msg = "installing a snapshot", group = %group, %table, redone);
        if volatile {
            // the records are read on a task, and put in place here once they are
            let tx = self.shard_local_tx.clone();
            glommio::spawn_local(async move {
                let outcome = read_records(&path).await.map_err(|error| error.to_string());
                let _ = tx.send(ServerMsg::SnapshotRecords { group, outcome }).await;
            })
            .detach();
            return Ok(());
        }
        let sink = self
            .tables
            .compaction_sinks()
            .into_iter()
            .find(|(name, _)| *name == table)
            .map(|(_, sink)| sink);
        match sink {
            Some(sink) => {
                sink.send(crate::storage::CompactionJob::Install {
                    group,
                    tablets,
                    path,
                })
                .await?;
            }
            None => {
                self.fail_install(
                    group,
                    format!("{table} has no compactor to install a snapshot with"),
                );
            }
        }
        Ok(())
    }

    /// Put a volatile group's records in its ephemeral table, then carry on as an install does
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `outcome` - The records and the trailer, or why the file could not be read
    pub(super) async fn handle_snapshot_records(
        &mut self,
        group: GroupId,
        outcome: Result<
            (
                Vec<(u64, Vec<u8>)>,
                Vec<(
                    crate::shared::protocol::peer::RequestId,
                    crate::server::replication::Remembered,
                )>,
            ),
            String,
        >,
    ) -> Result<(), ServerError> {
        let (table, tablets) = {
            let Some(replication) = self.replication.as_ref() else {
                return Ok(());
            };
            let Some(slot) = replication.groups.get(&group) else {
                return Ok(());
            };
            let Some(active) = replication.active_installs.get(&group) else {
                return Ok(());
            };
            (slot.table, active.manifest.tablets.clone())
        };
        let (records, trailer) = match outcome {
            Ok(read) => read,
            Err(error) => {
                self.fail_install(group, format!("reading the snapshot file: {error}"));
                return Ok(());
            }
        };
        if let Err(error) = self.tables.install_partitions(table, &tablets, records) {
            self.fail_install(group, format!("installing the records: {error:?}"));
            return Ok(());
        }
        self.handle_snapshot_installed(table, group, Ok(trailer))
            .await
    }

    /// Take the archives' word that the records are in, move the group's state, and checkpoint
    ///
    /// Every resident partition of the covered tablets is evicted, the applied position and
    /// the checkpoint become the boundary, the memberships follow the manifest, the retry
    /// table is re-seeded from the trailer, and the checkpoint file is written; the install
    /// completes once that write lands ([F43](../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `group` - The group
    /// * `outcome` - The trailer, or why the records are not in
    pub(super) async fn handle_snapshot_installed(
        &mut self,
        table: D::TableNames,
        group: GroupId,
        outcome: Result<
            Vec<(
                crate::shared::protocol::peer::RequestId,
                crate::server::replication::Remembered,
            )>,
            String,
        >,
    ) -> Result<(), ServerError> {
        let trailer = match outcome {
            Ok(trailer) => trailer,
            Err(error) => {
                self.fail_install(group, error);
                return Ok(());
            }
        };
        let (tablets, boundary, membership, volatile, expired_before) = {
            let Some(replication) = self.replication.as_ref() else {
                return Ok(());
            };
            let (Some(slot), Some(active)) = (
                replication.groups.get(&group),
                replication.active_installs.get(&group),
            ) else {
                return Ok(());
            };
            (
                active.manifest.tablets.clone(),
                active.manifest.boundary.clone(),
                active.manifest.membership.clone(),
                slot.store.is_volatile(),
                active.manifest.expired_before,
            )
        };
        // a persistent table's resident copies are the old generation; the archives are the new
        if !volatile {
            self.tables.evict_tablets(table, &tablets);
        }
        let replication = self.replication.as_mut().expect("still here");
        let slot = replication.groups.get(&group).expect("still here");
        {
            let mut state = slot.state.borrow_mut();
            // what the snapshot covered, for a catch-up split by path
            replication.snapshots.entries_installed +=
                boundary.index.saturating_sub(state.applied_index());
            state.applied = Some(boundary.clone());
            state.checkpoint = Some(boundary.clone());
            state.membership = membership.clone();
            state.checkpoint_membership = membership;
            // nothing applied since the checkpoint is left: the snapshot is the checkpoint
            state.memberships.clear();
            state.snapshot_at = Some(boundary.clone());
            state.dedup.clear();
            for (request, remembered) in trailer {
                state.dedup.put(request, remembered);
            }
            // what the sender had forgotten, this copy has too
            // ([F45](../../../../docs/src/features/replica-migration.md))
            state.expired_before = state.expired_before.max(expired_before);
            // a volatile group's checkpoint is never written; a persistent one's is next
            state.checkpoint_durable = volatile;
        }
        crash_point::hit(CrashPoint::BeforeCheckpoint);
        if volatile {
            self.cleanup_install(group);
            return Ok(());
        }
        replication.checkpoint_dirty = true;
        let started = self.write_checkpoint();
        let replication = self.replication.as_mut().expect("still here");
        // the write that carries this state: the one started now, or the next if one is in flight
        let version = if started {
            replication.checkpoint_version
        } else {
            replication.checkpoint_version + 1
        };
        if let Some(active) = replication.active_installs.get_mut(&group) {
            active.checkpoint_version = Some(version);
        }
        Ok(())
    }

    /// Clean up every install whose state a checkpoint write carried
    ///
    /// # Arguments
    ///
    /// * `version` - The write that landed
    pub(super) fn checkpoint_carried_installs(&mut self, version: u64) {
        let Some(replication) = self.replication.as_ref() else {
            return;
        };
        let carried: Vec<GroupId> = replication
            .active_installs
            .iter()
            .filter(|(_, active)| {
                active
                    .checkpoint_version
                    .is_some_and(|wanted| wanted <= version)
            })
            .map(|(group, _)| *group)
            .collect();
        for group in carried {
            crash_point::hit(CrashPoint::AfterCheckpoint);
            self.cleanup_install(group);
        }
    }

    /// Remove an install's marker and file on a task, now that its state is durable
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    fn cleanup_install(&mut self, group: GroupId) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let Some(active) = replication.active_installs.get_mut(&group) else {
            return;
        };
        // the marker is already gone from a cleanup started twice
        if active.checkpoint_version == Some(u64::MAX) {
            return;
        }
        active.checkpoint_version = Some(u64::MAX);
        let marker = replication.installs.marker_path(group);
        let part = active.path.clone();
        let dir = replication.installs.dir.clone();
        let tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            let outcome = remove_install_files(&marker, &part, &dir)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(ServerMsg::SnapshotCleaned { group, outcome }).await;
        })
        .detach();
    }

    /// Finish an install: the group serves again, and whoever waited hears it
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `outcome` - Whether the cleanup landed
    pub(super) fn handle_snapshot_cleaned(&mut self, group: GroupId, outcome: Result<(), String>) {
        if let Err(error) = outcome {
            self.fail_install(group, format!("cleaning up the install: {error}"));
            return;
        }
        crash_point::hit(CrashPoint::AfterCleanup);
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let Some(mut active) = replication.active_installs.remove(&group) else {
            return;
        };
        // the stream that was installed, for an end asked again after its answer was lost
        if let Some(partial) = replication.installs.partials.get(&group) {
            let stream = partial.borrow().stream;
            replication.installs.installed.insert(group, stream);
        }
        replication.installs.retire(group);
        replication.snapshots.installed += 1;
        if active.redone {
            replication.snapshots.redone += 1;
        }
        if let Some(slot) = replication.groups.get(&group) {
            let mut state = slot.state.borrow_mut();
            state.installing = false;
            state.pending_install = None;
            state.repair_pending = false;
        }
        event!(Level::INFO, msg = "a snapshot is installed", group = %group, boundary = active.manifest.boundary.index, records = active.manifest.records, repair = active.repair);
        if let Some(done) = active.done.take() {
            let _ = done.send(Ok(()));
        }
        // a repair replaced the archives at the boundary: what the old generation had merged
        // above it is merged again into the new one, before the resident copies the log
        // rebuilds can be evicted ([F44](../../../../docs/src/features/repair.md))
        if active.repair {
            let boundary = active.manifest.boundary.index;
            if let Err(error) = self.rehand_segments(group, boundary) {
                event!(Level::ERROR, msg = "the segments above a repair install could not be handed again", group = %group, boundary, ?error);
            }
        }
    }

    /// Give up on an install: the group serves again from what it had, and the asker hears why
    ///
    /// The marker stays, so the install is redone at the next open; what failed here is a
    /// storage error, which openraft stops the group on.
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `error` - Why
    fn fail_install(&mut self, group: GroupId, error: String) {
        event!(Level::ERROR, msg = "a snapshot could not be installed", group = %group, error);
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        if let Some(slot) = replication.groups.get(&group) {
            slot.state.borrow_mut().installing = false;
        }
        if let Some(mut active) = replication.active_installs.remove(&group) {
            if let Some(done) = active.done.take() {
                let _ = done.send(Err(error));
            }
        }
    }
}

/// Read a snapshot file's records and trailer, for a volatile install
///
/// # Arguments
///
/// * `path` - The file
async fn read_records(
    path: &std::path::Path,
) -> std::io::Result<(
    Vec<(u64, Vec<u8>)>,
    Vec<(
        crate::shared::protocol::peer::RequestId,
        crate::server::replication::Remembered,
    )>,
)> {
    let mut reader = crate::server::replication::snapshot::SnapshotReader::open(path).await?;
    let mut records = Vec::with_capacity(usize::try_from(reader.header().records).unwrap_or(0));
    while let Some(record) = reader.next_record().await? {
        records.push(record);
    }
    let trailer = reader.trailer().await?;
    reader.close().await?;
    Ok((records, trailer))
}

/// Verify a whole partial, write its marker, and hand it to openraft to install
///
/// # Arguments
///
/// * `partial` - The partial, whole
/// * `raft` - The group's handle, which a repair stream to a copy whose start stalled has none of
/// * `path` - The partial's file
/// * `marker_dir` - Where the marker goes
/// * `marker_name` - The marker's name
/// * `vote` - The vote to install under
/// * `loop_tx` - The loop, which restarts the group for a repair stream
async fn install_received<D: ShoalDatabase>(
    partial: &Rc<RefCell<Partial>>,
    raft: Option<
        &openraft::Raft<
            crate::server::replication::DataConfig,
            impl openraft::storage::RaftStateMachine<
                crate::server::replication::DataConfig,
                SnapshotData = SnapshotData,
            >,
        >,
    >,
    path: &std::path::Path,
    marker_dir: &std::path::Path,
    marker_name: &str,
    vote: crate::server::wal::Vote,
    loop_tx: &kanal::AsyncSender<ServerMsg<D>>,
) -> SnapshotAnswer {
    let (manifest, checksum, group, repair) = {
        let partial = partial.borrow();
        (
            partial.manifest.clone(),
            partial.assembler.checksum(),
            partial.manifest.group,
            partial.repair,
        )
    };
    if checksum != manifest.checksum {
        partial.borrow_mut().failed = Some(format!(
            "the assembled file hashes to {checksum:016x} and the manifest says {:016x}",
            manifest.checksum
        ));
        return SnapshotAnswer::Refused(
            "the assembled file does not hash to what the manifest says".to_string(),
        );
    }
    // the file durable, then the marker that makes the install redoable
    let synced = async {
        let file = OpenOptions::new().write(true).buffered_open(path).await?;
        file.fdatasync().await?;
        file.close().await?;
        Ok::<(), glommio::GlommioError<()>>(())
    }
    .await;
    if let Err(error) = synced {
        return SnapshotAnswer::Refused(format!("the partial could not be synced: {error}"));
    }
    partial.borrow_mut().synced = true;
    crash_point::hit(CrashPoint::BeforePending);
    let marker = match postcard::to_allocvec(&manifest) {
        Ok(marker) => marker,
        Err(error) => {
            return SnapshotAnswer::Refused(format!("encoding the pending marker: {error}"))
        }
    };
    if let Err(error) = write_atomic(marker_dir, marker_name, marker).await {
        return SnapshotAnswer::Refused(format!(
            "the pending marker could not be written: {error}"
        ));
    }
    crash_point::hit(CrashPoint::PendingWritten);
    event!(Level::INFO, msg = "a snapshot was received whole; installing", group = %group, boundary = manifest.boundary.index, bytes = manifest.total, repair = ?repair);
    // a repair stream is installed by restarting the group from its held checkpoint, since
    // openraft refuses a snapshot at or below what the live group has committed
    // ([F44](../../../../docs/src/features/repair.md)); the answer says the restart is under
    // way, and the driver's verifying scrub is what waits for the install
    if repair.is_some() {
        let (reply, done) = futures_channel::oneshot::channel();
        if loop_tx
            .send(ServerMsg::RepairInstall {
                group,
                path: path.to_path_buf(),
                manifest,
                reply,
            })
            .await
            .is_err()
        {
            return SnapshotAnswer::Refused("the shard loop is gone".to_string());
        }
        return match done.await {
            Ok(Ok(())) => SnapshotAnswer::Installed { vote },
            Ok(Err(error)) => SnapshotAnswer::Refused(format!(
                "the group could not be restarted for the install: {error}"
            )),
            Err(_) => SnapshotAnswer::Refused("the shard loop dropped the restart".to_string()),
        };
    }
    // openraft judges the vote, purges the log through the boundary, and asks the state
    // machine to install, which the loop does and answers once it is durable
    let snapshot = Snapshot {
        meta: SnapshotMeta {
            last_log_id: Some(manifest.boundary.clone()),
            last_membership: manifest.membership.clone(),
        },
        snapshot: SnapshotData::Received {
            path: path.to_path_buf(),
            manifest,
        },
    };
    let Some(raft) = raft else {
        return SnapshotAnswer::Refused(format!("group {group} is still starting"));
    };
    match raft.install_full_snapshot(vote, snapshot).await {
        Ok(response) => SnapshotAnswer::Installed {
            vote: response.vote,
        },
        Err(error) => {
            SnapshotAnswer::Refused(format!("the snapshot could not be installed: {error}"))
        }
    }
}

/// Drain a partial's queue into its file, judging every chunk, until the queue is empty
///
/// # Arguments
///
/// * `partial` - The partial
/// * `path` - Its file
async fn write_partial(partial: Rc<RefCell<Partial>>, path: PathBuf) {
    let mut file: Option<BufferedFile> = None;
    loop {
        // the next chunk, and what to do with it
        let next = {
            let mut partial = partial.borrow_mut();
            match partial.queue.pop_front() {
                Some((offset, bytes)) => {
                    partial.queued_bytes = partial.queued_bytes.saturating_sub(bytes.len());
                    let offer = partial.assembler.offer(offset, bytes.len() as u64);
                    Some((offset, bytes, offer))
                }
                None => None,
            }
        };
        let Some((offset, bytes, offer)) = next else {
            break;
        };
        if offer != Offer::Write {
            continue;
        }
        // the file: created fresh for a new stream, opened as it stands for a resumed one
        if file.is_none() {
            let opened = partial.borrow().opened;
            let open = if opened {
                OpenOptions::new().write(true).buffered_open(&path).await
            } else {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                BufferedFile::create(&path).await
            };
            match open {
                Ok(handle) => {
                    file = Some(handle);
                    partial.borrow_mut().opened = true;
                }
                Err(error) => {
                    partial.borrow_mut().failed = Some(format!("opening the partial: {error}"));
                    break;
                }
            }
        }
        let Some(handle) = file.as_ref() else { break };
        // the bytes are the prefix's next: folded before the write takes them, since a write
        // that fails ends the partial whatever the fold says
        let len = bytes.len();
        partial.borrow_mut().assembler.advance(&bytes);
        match handle.write_at(bytes, offset).await {
            Ok(written) if written == len => {}
            Ok(written) => {
                partial.borrow_mut().failed =
                    Some(format!("a short write of {written} of {len} bytes"));
                break;
            }
            Err(error) => {
                partial.borrow_mut().failed = Some(format!("writing the partial: {error}"));
                break;
            }
        }
    }
    if let Some(handle) = file {
        let _ = handle.close().await;
    }
    partial.borrow_mut().writing = false;
}

/// Encode a snapshot answer for the replication lane
///
/// # Arguments
///
/// * `id` - The request's correlation id
/// * `answer` - The answer
fn encode_answer(id: u64, answer: &SnapshotAnswer) -> ReplicateReply {
    match postcard::to_allocvec(answer) {
        Ok(bytes) => ReplicateReply::ok(id, bytes),
        Err(error) => ReplicateReply::error(id, format!("encoding a snapshot answer: {error}")),
    }
}

/// Remove an install's marker and partial file, whichever of them exist
///
/// A restore installs the leader's own copy from the file it built, and never writes a partial
/// or a marker into the shard's install directory. On a shard that had received no stream the
/// directory did not exist, the sync of it failed, the install failed with it, and the shard
/// died ([Resolved #153](../../../../docs/src/appendix/resolved/install-dir-absent.md)).
///
/// # Arguments
///
/// * `marker` - The install's pending marker
/// * `part` - The install's partial file
/// * `dir` - The shard's install directory
///
/// # Errors
///
/// When a file that exists cannot be removed, or a directory that exists cannot be synced.
pub(super) async fn remove_install_files(
    marker: &std::path::Path,
    part: &std::path::Path,
    dir: &std::path::Path,
) -> std::io::Result<()> {
    // the marker first: once it is gone the install is complete and the file is nobody's
    if marker.exists() {
        glommio::io::remove(marker).await?;
    }
    // the removal made durable, where there is a directory that could hold it
    if dir.exists() {
        crate::server::replication::snapshot::sync_dir(dir).await?;
    }
    if part.exists() {
        glommio::io::remove(part).await?;
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::remove_install_files;

    /// Cleaning up an install that wrote nothing to an install directory that was never made succeeds (item 153)
    ///
    /// A restore installs the leader's own copy from the file it built, so on a shard that had
    /// received no stream there was no install directory, and syncing it failed the install and
    /// killed the shard in the middle of the lab's restore
    /// ([Resolved #153](../../../../docs/src/appendix/resolved/install-dir-absent.md)).
    #[test]
    fn cleaning_up_an_install_needs_no_install_directory() {
        glommio::LocalExecutor::default().run(async {
            let root = tempfile::tempdir().expect("a temp dir");
            let dir = root.path().join("install");
            // nothing there at all
            remove_install_files(&dir.join("g.pending"), &dir.join("g.part"), &dir)
                .await
                .expect("an install that wrote nothing cleans up");
            // and an install that wrote both has both removed
            std::fs::create_dir_all(&dir).expect("a dir");
            std::fs::write(dir.join("g.pending"), b"x").expect("a marker");
            std::fs::write(dir.join("g.part"), b"x").expect("a part");
            remove_install_files(&dir.join("g.pending"), &dir.join("g.part"), &dir)
                .await
                .expect("a clean up");
            assert!(!dir.join("g.pending").exists() && !dir.join("g.part").exists());
        });
    }
}
