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
use crate::server::peer::ReplicateReply;
use crate::server::replication::install::{crash_point, CrashPoint, Offer, Partial};
use crate::server::replication::snapshot::{SnapshotAnswer, SnapshotManifest, SnapshotRpc, INSTALL_DIR};
use crate::server::replication::{SnapshotData, SnapshotStats};
use crate::server::wal::write_atomic;
use crate::shared::identity::{GroupId, NodeId};
use crate::shared::protocol::peer::ReplicateRequestHead;
use crate::shared::traits::{QuerySupport, TableNameSupport};

/// How often the end of a stream checks whether its prefix is whole
const END_POLL: Duration = Duration::from_millis(5);

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
    /// * `queue_bound` - The most bytes queued for a writer
    pub(super) fn new(dir: &std::path::Path, bound: u64, queue_bound: usize) -> Self {
        Installs {
            dir: dir.join(INSTALL_DIR),
            bound,
            queue_bound,
            partials: HashMap::new(),
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
    /// * `reply` - Where the answer goes
    pub(super) fn handle_snapshot_rpc(
        &mut self,
        origin: NodeId,
        head: ReplicateRequestHead,
        payload: Vec<u8>,
        reply: kanal::AsyncSender<ReplicateReply>,
    ) {
        let group = GroupId(head.group);
        let rpc: SnapshotRpc = match postcard::from_bytes(&payload) {
            Ok(rpc) => rpc,
            Err(error) => {
                let _ = reply.try_send(ReplicateReply::error(head.id, format!("decoding a snapshot rpc: {error}")));
                return;
            }
        };
        let answer = match rpc {
            SnapshotRpc::Begin { vote, stream, manifest } => {
                let _ = vote;
                self.begin_snapshot(origin, group, stream, manifest)
            }
            SnapshotRpc::End { stream, total, checksum } => {
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
    /// * `manifest` - What is coming
    fn begin_snapshot(&mut self, origin: NodeId, group: GroupId, stream: [u8; 16], manifest: SnapshotManifest) -> SnapshotAnswer {
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
        // a boundary already applied here needs nothing: the sender moves on from it
        let (applied, installing) = {
            let state = slot.state.borrow();
            (state.applied_index(), state.installing)
        };
        if manifest.boundary.index <= applied {
            let vote = slot.raft.as_ref().map(|raft| raft.metrics().borrow_watched().vote.clone());
            return match vote {
                Some(vote) => SnapshotAnswer::Installed { vote },
                None => SnapshotAnswer::Refused(format!("group {group} is still starting")),
            };
        }
        if installing {
            return SnapshotAnswer::Refused(format!("group {group} is installing a snapshot already"));
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
        replication
            .installs
            .partials
            .insert(group, Rc::new(RefCell::new(Partial::new(origin, stream, manifest))));
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
    pub(super) fn handle_snapshot_bytes(&mut self, node: NodeId, stream: [u8; 16], offset: u64, bytes: Vec<u8>) {
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
            !partial.writing
        };
        if spawn {
            partial.borrow_mut().writing = true;
            glommio::spawn_local(write_partial(partial, path)).detach();
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
            let _ = reply.try_send(ReplicateReply::error(head.id, "this node hosts no tablet groups"));
            return;
        };
        let (partial, raft, path, marker_dir, marker_name) = {
            let Some(partial) = replication.installs.partials.get(&group).cloned() else {
                let _ = reply.try_send(encode_answer(head.id, &SnapshotAnswer::Refused(format!("no stream of group {group} is being assembled"))));
                return;
            };
            let raft = replication.groups.get(&group).and_then(|slot| slot.raft.clone());
            (
                partial,
                raft,
                replication.installs.part_path(group),
                replication.installs.dir.clone(),
                Installs::marker_name(group),
            )
        };
        let Some(raft) = raft else {
            let _ = reply.try_send(encode_answer(head.id, &SnapshotAnswer::Refused(format!("group {group} is still starting"))));
            return;
        };
        {
            let partial = partial.borrow();
            if partial.stream != stream || partial.from != origin {
                let _ = reply.try_send(encode_answer(head.id, &SnapshotAnswer::Refused("the end names a stream that is not being assembled".to_string())));
                return;
            }
            if partial.manifest.total != total || partial.manifest.checksum != checksum {
                let _ = reply.try_send(encode_answer(
                    head.id,
                    &SnapshotAnswer::Refused("the end does not describe the stream its begin announced".to_string()),
                ));
                return;
            }
        }
        let deadline = Duration::from_millis(u64::from(head.deadline_ms.max(1)));
        // the install is handed to openraft under the group's current vote, which is the
        // sender's if it still leads and a higher one it answers with if not
        let vote = raft.metrics().borrow_watched().vote.clone();
        glommio::spawn_local(async move {
            let started = Instant::now();
            // wait until the prefix is whole and written, or the sender's deadline passes
            let answer = loop {
                let (complete, writing, failed, next) = {
                    let partial = partial.borrow();
                    (
                        partial.assembler.complete(),
                        partial.writing,
                        partial.failed.clone(),
                        partial.assembler.next,
                    )
                };
                if let Some(failed) = failed {
                    break SnapshotAnswer::Refused(format!("the partial could not be written: {failed}"));
                }
                if complete && !writing {
                    break install_received(&partial, &raft, &path, &marker_dir, &marker_name, vote.clone()).await;
                }
                if started.elapsed() >= deadline {
                    break SnapshotAnswer::Resume { from: next };
                }
                glommio::timer::sleep(END_POLL).await;
            };
            let _ = reply.send(encode_answer(head.id, &answer)).await;
        })
        .detach();
    }
}

/// Verify a whole partial, write its marker, and hand it to openraft to install
///
/// # Arguments
///
/// * `partial` - The partial, whole
/// * `raft` - The group's handle
/// * `path` - The partial's file
/// * `marker_dir` - Where the marker goes
/// * `marker_name` - The marker's name
/// * `vote` - The vote to install under
async fn install_received(
    partial: &Rc<RefCell<Partial>>,
    raft: &openraft::Raft<crate::server::replication::DataConfig, impl openraft::storage::RaftStateMachine<crate::server::replication::DataConfig, SnapshotData = SnapshotData>>,
    path: &std::path::Path,
    marker_dir: &std::path::Path,
    marker_name: &str,
    vote: crate::server::wal::Vote,
) -> SnapshotAnswer {
    let (manifest, checksum, group) = {
        let partial = partial.borrow();
        (partial.manifest.clone(), partial.assembler.checksum(), partial.manifest.group)
    };
    if checksum != manifest.checksum {
        partial.borrow_mut().failed = Some(format!(
            "the assembled file hashes to {checksum:016x} and the manifest says {:016x}",
            manifest.checksum
        ));
        return SnapshotAnswer::Refused("the assembled file does not hash to what the manifest says".to_string());
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
        Err(error) => return SnapshotAnswer::Refused(format!("encoding the pending marker: {error}")),
    };
    if let Err(error) = write_atomic(marker_dir, marker_name, marker).await {
        return SnapshotAnswer::Refused(format!("the pending marker could not be written: {error}"));
    }
    crash_point::hit(CrashPoint::PendingWritten);
    event!(Level::INFO, msg = "a snapshot was received whole; installing", group = %group, boundary = manifest.boundary.index, bytes = manifest.total);
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
    match raft.install_full_snapshot(vote, snapshot).await {
        Ok(response) => SnapshotAnswer::Installed { vote: response.vote },
        Err(error) => SnapshotAnswer::Refused(format!("the snapshot could not be installed: {error}")),
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
                partial.borrow_mut().failed = Some(format!("a short write of {written} of {len} bytes"));
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
