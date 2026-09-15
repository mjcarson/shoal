//! Driving a group's restore: build a file for its tablets from a backup's files and install
//! it on every member
//!
//! A restore record rides the pushed map, and the leader of each of its groups drives that
//! group ([F49](../../../../docs/src/features/backup-and-recovery.md)) in three phases, each
//! committed before the next so a driver that dies leaves a phase the next leader resumes from:
//!
//! - **Loading**: a scrub proves every member's copy holds nothing - a restore is into an
//!   empty table, and a populated one is refused by name rather than overwritten.
//! - **Installing**: a nudge entry gives the group a committed boundary of its own; every
//!   backup file covering the group's tablets is verified against its manifest and read, the
//!   records of the group's tablets and every remembered request kept, and one snapshot file
//!   written at the boundary. Every member's copy is quarantined under the operation and the
//!   file installed on each through the repair install path - a peer over the bulk lane, this
//!   shard through its own loop - which restarts each group from its checkpoint with the file
//!   pending, the same atomic install the crash matrix proves. Resumed, the phase is redone
//!   whole at a fresh boundary, which every install is judged against.
//! - **Verifying**: a scrub through the restarted group; every member reporting one verified
//!   digest lifts the quarantines and the group is done, and anything else fails it by name.
//!
//! An ephemeral table's groups are skipped by name: their memory log is what a restart empties,
//! and a backup of one holds nothing a restore could keep.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use kanal::AsyncSender;
use openraft::Raft;
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level};
use uuid::Uuid;

use super::repair::{scrub_group, NOT_LEADER};
use super::ServerMsg;
use crate::server::control::backup::{BackupManifest, GroupRestore, RestoreOutcome, RestorePhase};
use crate::server::control::plane::ControlRequest;
use crate::server::control::repair::{Quarantine, QuarantineAction, QuarantineReason};
use crate::server::control::types::{ControlCommand, ControlResponse};
use crate::server::replication::network::RepairSend;
use crate::server::replication::snapshot::{
    self, SnapshotManifest, SnapshotProvenance, SnapshotReader, SnapshotWriter, SNAPSHOTS_DIR,
};
use crate::server::replication::{
    DataConfig, DigestIntegrity, GroupMachine, GroupPeer, MachineState, Remembered, ShardNetwork,
    ShardPeer,
};
use crate::server::ring::Ring;
use crate::server::ShoalDatabase;
use crate::shared::identity::{ClusterId, GroupId, ShardAddr, TableId};
use crate::shared::protocol::peer::RequestId;

/// How long a driver waits for the control plane to commit its progress
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a driver waits for a member to persist a quarantine
const QUARANTINE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a driver waits for its own group to come back after the install restarts it
const RESTART_TIMEOUT: Duration = Duration::from_secs(60);

/// Everything a group's restore driver needs, gathered on the loop before the task starts
pub struct RestoreContext<D: ShoalDatabase> {
    /// The group's handle, which leads; replaced once the install restarts the group
    pub raft: Raft<DataConfig, GroupMachine<D>>,
    /// The shard's network
    pub network: ShardNetwork,
    /// This shard
    pub me: ShardAddr,
    /// This node's incarnation
    pub incarnation: u64,
    /// The cluster
    pub cluster: ClusterId,
    /// The group
    pub group: GroupId,
    /// The group's table
    pub table: TableId,
    /// The tablets the group serves
    pub tablets: Vec<u16>,
    /// Every member
    pub members: Vec<ShardAddr>,
    /// This shard's copy of the group's state
    pub state: Rc<RefCell<MachineState>>,
    /// The record's operation
    pub op: Uuid,
    /// The backup directory
    pub path: PathBuf,
    /// The backup files covering the group's tablets, relative to the directory
    pub files: Vec<String>,
    /// The phase the record stands at, which decides where the driver starts
    pub phase: RestorePhase,
    /// The structural fingerprint of the schema this node serves
    pub schema_id: u64,
    /// Where the built file goes
    pub snapshots_dir: PathBuf,
    /// The wire version the cluster has activated, which decides the file's format
    pub activated_wire: u8,
    /// Whether the group's log lives in memory alone
    pub volatile: bool,
    /// How long one phase may take
    pub timeout: Duration,
    /// How long one snapshot transfer may take
    pub snapshot_timeout: Duration,
    /// The control thread, which commits the progress
    pub control: kanal::Sender<ControlRequest>,
    /// The loop, which holds this shard's own copy
    pub loop_tx: AsyncSender<ServerMsg<D>>,
}

impl<D: ShoalDatabase> RestoreContext<D> {
    /// Commit where the group stands
    ///
    /// # Arguments
    ///
    /// * `progress` - Where it stands
    async fn commit(&self, progress: GroupRestore) -> Result<(), String> {
        let started = Instant::now();
        let mut last = String::new();
        while started.elapsed() < PROGRESS_TIMEOUT {
            let (reply, rx) = kanal::bounded(1);
            let command = ControlCommand::RestoreProgress {
                op: self.op,
                group: self.group,
                node: self.me.node,
                incarnation: self.incarnation,
                progress: progress.clone(),
            };
            self.control
                .try_send(ControlRequest::Propose { command, reply })
                .map_err(|_| "the control thread is not taking proposals".to_string())?;
            let remaining = PROGRESS_TIMEOUT.saturating_sub(started.elapsed());
            let answered =
                glommio::timer::timeout(remaining, async { Ok(rx.as_async().recv().await) }).await;
            last = match answered {
                Ok(Ok(Ok(ControlResponse::Applied { .. }))) => return Ok(()),
                Ok(Ok(Ok(ControlResponse::Refused { reason, .. }))) => {
                    return Err(format!("the progress was refused: {reason}"))
                }
                Ok(Ok(Ok(other))) => format!("the progress was not applied: {other:?}"),
                Ok(Ok(Err(error))) => error,
                Ok(Err(_)) => "the control thread dropped the proposal".to_string(),
                Err(_) => break,
            };
            glommio::timer::sleep(Duration::from_millis(500)).await;
        }
        Err(format!(
            "the progress was not committed within {PROGRESS_TIMEOUT:?}: {last}"
        ))
    }

    /// A progress at a phase, driven here, with the files and no outcome
    ///
    /// # Arguments
    ///
    /// * `phase` - The phase
    fn at(&self, phase: RestorePhase) -> GroupRestore {
        GroupRestore {
            phase,
            driver: Some(self.me.node),
            files: self.files.clone(),
            outcome: None,
        }
    }

    /// Tell a member what to do with its copy's quarantine, and wait until it has
    ///
    /// # Arguments
    ///
    /// * `member` - The member
    /// * `action` - What to do
    async fn quarantine(&self, member: ShardAddr, action: QuarantineAction) -> Result<(), String> {
        if member == self.me {
            // this shard's own copy, through the loop
            let (reply, done) = oneshot::channel();
            self.loop_tx
                .send(ServerMsg::Quarantine {
                    group: self.group,
                    action,
                    reply: Some(reply),
                })
                .await
                .map_err(|error| format!("{error:?}"))?;
            return done
                .await
                .unwrap_or_else(|_| Err("the loop dropped the quarantine".to_string()));
        }
        let peer = ShardPeer::new(member, self.network.clone());
        let payload = postcard::to_allocvec(&action).map_err(|error| error.to_string())?;
        match peer
            .quarantine(self.group, payload, QUARANTINE_TIMEOUT)
            .await
        {
            Ok(_) => Ok(()),
            Err(failure) => Err(format!("{member} did not take the quarantine: {failure}")),
        }
    }

    /// This shard's current handle and state for the group, once the install's restart has them back
    ///
    /// The restart built a new state for the group, so the driver's copy is stale from then on
    /// and the verifying scrub reads this one.
    async fn handle_after_restart(
        &self,
    ) -> Result<(Raft<DataConfig, GroupMachine<D>>, Rc<RefCell<MachineState>>), String> {
        let started = Instant::now();
        loop {
            let (reply, rx) = oneshot::channel();
            self.loop_tx
                .send(ServerMsg::GroupHandle {
                    group: self.group,
                    reply,
                })
                .await
                .map_err(|error| format!("{error:?}"))?;
            if let Ok(Some((raft, state))) = rx.await {
                // the restarted group has to elect somebody, and this shard has to be it to go on
                let leader = raft.metrics().borrow_watched().current_leader;
                match leader {
                    Some(leader) if leader == self.me => return Ok((raft, state)),
                    Some(leader) => {
                        return Err(format!(
                            "{NOT_LEADER}group {} after its restart: the leader is {leader}",
                            self.group
                        ))
                    }
                    None => {}
                }
            }
            if started.elapsed() > RESTART_TIMEOUT {
                return Err(format!(
                    "group {} did not come back from its install within {RESTART_TIMEOUT:?}",
                    self.group
                ));
            }
            glommio::timer::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Drive one group's restore to the end, and tell the loop
///
/// # Arguments
///
/// * `context` - Everything the driver needs
pub async fn drive_group_restore<D: ShoalDatabase>(context: RestoreContext<D>) {
    let outcome = drive_inner(&context).await;
    let phase = match outcome {
        Ok(phase) => phase,
        Err(error) if error.starts_with(NOT_LEADER) => {
            // a driver that lost the lead leaves the group where it stood for the new leader
            event!(Level::INFO, msg = "a group's restore is left for its new leader", op = %context.op, group = %context.group, error);
            let _ = context
                .commit(GroupRestore {
                    driver: None,
                    ..context.at(context.phase.clone())
                })
                .await;
            context.phase.clone()
        }
        Err(error) => {
            event!(Level::ERROR, msg = "a group's restore failed", op = %context.op, group = %context.group, error);
            let _ = context
                .commit(GroupRestore {
                    phase: RestorePhase::Done,
                    outcome: Some(RestoreOutcome::Failed { reason: error }),
                    ..context.at(RestorePhase::Done)
                })
                .await;
            RestorePhase::Done
        }
    };
    let _ = context
        .loop_tx
        .send(ServerMsg::RestoreDone {
            op: context.op,
            group: context.group,
            phase,
        })
        .await;
}

/// The phases, from where the record stands, each committed before the next
///
/// # Arguments
///
/// * `context` - Everything the driver needs
async fn drive_inner<D: ShoalDatabase>(
    context: &RestoreContext<D>,
) -> Result<RestorePhase, String> {
    // an ephemeral table's group holds nothing a restore could keep
    if context.volatile {
        context
            .commit(GroupRestore {
                outcome: Some(RestoreOutcome::Skipped {
                    reason: "an ephemeral table's group is not restored: its rows do not survive a restart to begin with".to_string(),
                }),
                ..context.at(RestorePhase::Done)
            })
            .await?;
        return Ok(RestorePhase::Done);
    }
    let mut raft = context.raft.clone();
    let mut state = context.state.clone();
    // loading: the copies have to hold nothing
    if context.phase.rank() <= RestorePhase::Loading.rank() {
        context.commit(context.at(RestorePhase::Loading)).await?;
        let scrub = scrub_group(
            &raft,
            &context.network,
            context.me,
            context.state.clone(),
            context.table,
            context.group,
            &context.members,
            Uuid::new_v4(),
            context.timeout,
        )
        .await?;
        for (member, report) in &scrub.reports {
            match report {
                Ok(report) if report.rows > 0 => {
                    return Err(format!(
                        "{member} holds {} rows of the group's tablets; a restore is into an empty table, and this one is not",
                        report.rows
                    ));
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(format!(
                        "{member} did not report before the restore: {error}"
                    ))
                }
            }
        }
    }
    // installing: the file at a boundary of this run's own, on every member
    let mut installed = None;
    if context.phase.rank() <= RestorePhase::Installing.rank() {
        context.commit(context.at(RestorePhase::Installing)).await?;
        // the boundary: an entry of this run's, so every install is judged against it
        let nudge = scrub_group(
            &raft,
            &context.network,
            context.me,
            context.state.clone(),
            context.table,
            context.group,
            &context.members,
            Uuid::new_v4(),
            context.timeout,
        )
        .await?;
        let (path, manifest) = build_restore_file(context, nudge.log_id).await?;
        event!(Level::INFO, msg = "built a group's restore file", op = %context.op, group = %context.group, boundary = manifest.boundary.index, records = manifest.records, bytes = manifest.total);
        // every copy quarantined under the operation, so the install path takes the file
        for member in &context.members {
            context
                .quarantine(
                    *member,
                    QuarantineAction::Set(Quarantine {
                        reason: QuarantineReason::Operator,
                        at: manifest.boundary.index,
                        op: context.op,
                    }),
                )
                .await?;
        }
        // every other member over the bulk lane
        let vote = raft.metrics().borrow_watched().vote.clone();
        for member in context
            .members
            .iter()
            .filter(|member| **member != context.me)
        {
            let mut peer = GroupPeer::for_repair(context.group, *member, context.network.clone());
            match peer
                .repair_snapshot(
                    vote.clone(),
                    path.clone(),
                    manifest.clone(),
                    context.op,
                    context.snapshot_timeout,
                )
                .await?
            {
                RepairSend::Installed => {}
                RepairSend::Behind { checkpoint } => {
                    return Err(format!(
                        "{member}'s checkpoint {checkpoint} is past the restore boundary {}",
                        manifest.boundary.index
                    ));
                }
            }
        }
        // this shard's own, through the loop, which restarts the group with the file pending
        let (reply, done) = oneshot::channel();
        context
            .loop_tx
            .send(ServerMsg::RepairInstall {
                group: context.group,
                path: path.clone(),
                manifest: manifest.clone(),
                reply,
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        done.await
            .unwrap_or_else(|_| Err("the loop dropped the install".to_string()))?;
        (raft, state) = context.handle_after_restart().await?;
        installed = Some(manifest);
        context.commit(context.at(RestorePhase::Verifying)).await?;
    }
    // verifying: every member holds one verified digest, and the quarantines are lifted
    let scrub = scrub_group(
        &raft,
        &context.network,
        context.me,
        state,
        context.table,
        context.group,
        &context.members,
        Uuid::new_v4(),
        context.timeout,
    )
    .await?;
    let mut digests = Vec::with_capacity(context.members.len());
    for (member, report) in &scrub.reports {
        match report {
            Ok(report) if report.integrity == DigestIntegrity::Verified => {
                digests.push((*member, report.digest, report.rows))
            }
            Ok(report) => {
                return Err(format!(
                    "{member}'s restored copy did not verify: {:?}",
                    report.integrity
                ))
            }
            Err(error) => {
                return Err(format!(
                    "{member} did not report after the restore: {error}"
                ))
            }
        }
    }
    if digests.windows(2).any(|pair| pair[0].1 != pair[1].1) {
        return Err(format!("the restored copies disagree: {digests:?}"));
    }
    for member in &context.members {
        context
            .quarantine(
                *member,
                QuarantineAction::Lift {
                    op: Some(context.op),
                },
            )
            .await?;
    }
    let (boundary, records, bytes, retries) = installed.as_ref().map_or((0, 0, 0, 0), |manifest| {
        (
            manifest.boundary.index,
            manifest.records,
            manifest.total,
            manifest.retries,
        )
    });
    event!(Level::INFO, msg = "restored a group", op = %context.op, group = %context.group, boundary, records, verified = scrub.boundary);
    context
        .commit(GroupRestore {
            outcome: Some(RestoreOutcome::Restored {
                boundary,
                records,
                bytes,
                retries,
                verified: scrub.boundary,
            }),
            ..context.at(RestorePhase::Done)
        })
        .await?;
    Ok(RestorePhase::Done)
}

/// Build one snapshot file of the group's tablets from the backup's files, at a boundary
///
/// Every file named is verified against its manifest before a record of it is trusted; the
/// records of the group's tablets are kept and the rest left, and every remembered request
/// of every file is kept, since a retry table is by request and not by tablet.
///
/// # Arguments
///
/// * `context` - The driver's context, which names the files and the tablets
/// * `boundary` - The log id the file is cut at
async fn build_restore_file<D: ShoalDatabase>(
    context: &RestoreContext<D>,
    boundary: crate::server::wal::WalLogId,
) -> Result<(PathBuf, SnapshotManifest), String> {
    let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut remembered: Vec<(RequestId, Remembered)> = Vec::new();
    let mut expired_before = 0u64;
    for name in &context.files {
        let file = context.path.join(name);
        let manifest = read_manifest(&file)?;
        // the file has to be what its manifest says, byte for byte
        snapshot::verify(&file, &manifest.to_snapshot())
            .await
            .map_err(|error| {
                format!(
                    "{} does not verify against its manifest: {error}",
                    file.display()
                )
            })?;
        if manifest.table != context.table {
            return Err(format!(
                "{} holds table {}, not {}",
                file.display(),
                manifest.table,
                context.table
            ));
        }
        expired_before = expired_before.max(manifest.expired_before);
        let mut reader = SnapshotReader::open(&file)
            .await
            .map_err(|error| format!("opening {}: {error}", file.display()))?;
        while let Some((key, bytes)) = reader
            .next_record()
            .await
            .map_err(|error| format!("reading {}: {error}", file.display()))?
        {
            // a tablet id is twelve bits, so it fits a u16
            #[allow(clippy::cast_possible_truncation)]
            let tablet = Ring::tablet_of(key) as u16;
            if context.tablets.contains(&tablet) {
                records.push((key, bytes));
            }
        }
        remembered.extend(
            reader
                .trailer()
                .await
                .map_err(|error| format!("reading the trailer of {}: {error}", file.display()))?,
        );
        reader
            .close()
            .await
            .map_err(|error| format!("closing {}: {error}", file.display()))?;
    }
    records.sort_by_key(|(key, _)| *key);
    remembered.sort_by_key(|(_, remembered)| remembered.applied);
    // the file, at this group's boundary, under this cluster's provenance
    std::fs::create_dir_all(&context.snapshots_dir).map_err(|error| format!("{error}"))?;
    let path = context
        .snapshots_dir
        .join(format!("restore-{}-{}.snap", context.group, boundary.index));
    let provenance =
        SnapshotProvenance::at(context.cluster, context.me.node, context.activated_wire);
    let header = provenance.header(
        context.table,
        context.group,
        boundary.index,
        records.len() as u64,
        context.schema_id,
    );
    let mut writer = SnapshotWriter::create(&path, header)
        .await
        .map_err(|error| format!("creating {}: {error}", path.display()))?;
    for (key, bytes) in &records {
        writer
            .record(*key, bytes)
            .await
            .map_err(|error| format!("writing {}: {error}", path.display()))?;
    }
    let (total, checksum) = writer
        .finish(&remembered)
        .await
        .map_err(|error| format!("finishing {}: {error}", path.display()))?;
    snapshot::sync_dir(&context.snapshots_dir)
        .await
        .map_err(|error| format!("{error}"))?;
    // the membership as of the boundary, from this shard's copy
    let membership = {
        let state = context.state.borrow();
        let mut candidates = vec![state.checkpoint_membership.clone()];
        candidates.extend(state.memberships.iter().cloned());
        super::membership_as_of(&candidates, boundary.index)
    };
    let manifest = SnapshotManifest {
        group: context.group,
        table: context.table,
        schema_id: context.schema_id,
        boundary,
        membership,
        tablets: context.tablets.clone(),
        records: records.len() as u64,
        total,
        checksum,
        retries: u32::try_from(remembered.len()).unwrap_or(u32::MAX),
        expired_before,
        cluster: ClusterId::default(),
        origin: crate::shared::identity::NodeId::default(),
        created_ms: 0,
    };
    Ok((path, provenance.stamp(manifest, &header)))
}

/// Read the manifest beside a backup file
///
/// # Arguments
///
/// * `file` - The `.snap`
fn read_manifest(file: &Path) -> Result<BackupManifest, String> {
    let path = super::backup::manifest_path(file);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("{} is not a backup manifest: {error}", path.display()))
}

impl<D: ShoalDatabase> super::Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Forget a restore driver that finished, and look for the next group to drive
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `phase` - The phase it committed last
    pub(super) fn handle_restore_done(&mut self, op: Uuid, group: GroupId, phase: RestorePhase) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving_restores.remove(&(op, group));
            replication.driven_restores.insert((op, group), phase);
        }
        self.drive_restores();
    }

    /// Start a driver for every group of every pending restore this shard leads
    ///
    /// One at a time per shard, since a restore's install restarts the group and a scrub reads
    /// its archives whole. A group whose phase is not done, under a driver that is not running
    /// here, is this shard's to drive from that phase when its handle leads
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    pub(super) fn drive_restores(&mut self) {
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self.local.as_ref().map_or(0, |local| local.borrow().incarnation);
        let schema_id = <D::ClientType as crate::shared::traits::QuerySupport>::SCHEMA_ID;
        // a restore's phases are scrubs and a transfer, so they run under the repair's
        // deadline and the snapshot's
        let (timeout, snapshot_timeout) = self.conf.cluster.as_ref().map_or(
            (Duration::from_secs(300), Duration::from_secs(300)),
            |cluster| (cluster.repair.timeout.duration(), cluster.replication.snapshot_timeout.duration()),
        );
        let loop_tx = self.shard_local_tx.clone();
        let cluster = map.cluster.unwrap_or_default();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        replication
            .driven_restores
            .retain(|(op, _), _| map.restores.iter().any(|record| record.op == *op));
        let snapshots_dir = replication.wal.dir().join(SNAPSHOTS_DIR);
        for record in &map.restores {
            for (group, progress) in &record.groups {
                if !replication.driving_restores.is_empty() {
                    return;
                }
                // the phase as this shard last committed it, when the map is behind it
                let phase = match replication.driven_restores.get(&(record.op, *group)) {
                    Some(driven) if driven.rank() > progress.phase.rank() => driven.clone(),
                    _ => progress.phase.clone(),
                };
                if phase == RestorePhase::Done {
                    continue;
                }
                let Some(slot) = replication.groups.get(group) else {
                    continue;
                };
                let Some(raft) = slot.raft.clone() else {
                    continue;
                };
                let me = slot.spec.me(node);
                if raft.metrics().borrow_watched().current_leader != Some(me) {
                    continue;
                }
                replication.driving_restores.insert((record.op, *group));
                event!(Level::INFO, msg = "driving a group's restore", op = %record.op, group = %group, phase = ?phase, files = ?progress.files);
                let context = RestoreContext {
                    raft,
                    network: replication.network.clone(),
                    me,
                    incarnation,
                    cluster,
                    group: *group,
                    table: slot.spec.table,
                    tablets: slot.spec.tablets.clone(),
                    members: slot.spec.members.clone(),
                    state: slot.state.clone(),
                    op: record.op,
                    path: PathBuf::from(&record.path),
                    files: progress.files.clone(),
                    phase,
                    schema_id,
                    snapshots_dir: snapshots_dir.clone(),
                    activated_wire: map.activated_wire,
                    volatile: slot.store.is_volatile(),
                    timeout,
                    snapshot_timeout,
                    control: control.clone(),
                    loop_tx: loop_tx.clone(),
                };
                glommio::spawn_local(drive_group_restore(context)).detach();
            }
        }
    }
}
