//! Driving a group's backup: cut a snapshot at a committed boundary and copy it under the
//! backup directory
//!
//! A backup record rides the pushed map, and the leader of each of its groups drives that
//! group ([F49](../../../../docs/src/features/backup-and-recovery.md)): it commits `Cutting`,
//! nudges the group's checkpoint past what it applied so there is a boundary to cut at, asks
//! the shard loop for the group's snapshot - the compactor's cut, at or past the checkpoint -
//! holds the file so the sweep cannot delete it, commits `Writing` with the boundary, copies
//! the file under
//! `<path>/<op>/<table>/` with a manifest beside it, verifies the copy against its manifest,
//! and commits `Done` with the bytes and the checksum. A driver that loses the lead leaves the
//! group `Pending` for the next leader; one that meets a refusal for now - a nudge, a cut, a copy
//! or a commit that did not finish in time, or an archive's read refused while its group could
//! not confirm its leader - is driven again after a pause, up to twelve tries (item 322); one that
//! fails otherwise commits `Failed` with the reason. An ephemeral table's group is `Skipped` by
//! name: its rows do not survive a restart to begin with, so a file of them would be one nothing
//! could restore.
//!
//! The copy is the file the group would send a member behind the purge point, so a backup is
//! exactly what a restore installs: a version 2 file whose header names the cluster it was cut
//! in, since a backup is refused until wire version 5 is activated.

use shoal_channel::KeptReceiver;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::io::BufferedFile;
use kanal::AsyncSender;
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level};
use uuid::Uuid;

use super::ServerMsg;
use crate::server::control::backup::{
    BackupManifest, BackupOutcome, BackupPhase, GroupBackup, BACKUP_MANIFEST_SUFFIX,
};
use crate::server::control::plane::ControlRequest;
use crate::server::control::types::{ControlCommand, ControlResponse};
use crate::server::replication::snapshot::{self, SnapshotManifest};
use crate::server::replication::{
    BuiltSnapshot, DataConfig, GroupMachine, MachineState, ShardNetwork,
};
use crate::server::ShoalDatabase;
use crate::shared::identity::{ClusterId, GroupId, ShardAddr, TableId};
use crate::shared::protocol::peer::Command;

/// How long a driver waits for the control plane to commit its progress
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How many bytes one read of the copy takes
const COPY_BYTES: usize = 1024 * 1024;

/// The prefix a driver's error carries when it lost the lead, which is not a failure
const NOT_LEADER: &str = "not the leader: ";

/// What a group's backup error begins with when it was refused in a way a later try may not be:
/// an archive's read refused while its group could not confirm its leader, a member away, a shed
/// or a deadline, or a table group's nudge, cut, copy or commit that did not finish in time. Such a
/// group is driven again after a pause rather than failed (item 322)
pub const RETRIABLE: &str = "refused for now: ";

/// How many times a group's backup is tried before a refusal for now fails it, as a restore's
/// group is driven again for want of a member
pub const BACKUP_ATTEMPTS: u32 = 12;

/// How long a group's backup that met a refusal for now waits before it is driven again
const BACKUP_RETRY_PAUSE: Duration = Duration::from_secs(5);

/// Everything a group's backup driver needs, gathered on the loop before the task starts
pub struct BackupContext<D: ShoalDatabase> {
    /// The group's handle, which leads
    pub raft: Raft<DataConfig, GroupMachine<D>>,
    /// The shard's network, which the cut is asked through
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
    /// The table's name, which names the directory
    pub table_name: String,
    /// This shard's copy of the group's state, whose checkpoint the cut is judged by
    pub state: Rc<RefCell<MachineState>>,
    /// The record's operation
    pub op: Uuid,
    /// The directory the files go under
    pub path: PathBuf,
    /// How long the cut and the copy may take together
    pub timeout: Duration,
    /// Whether the group's log lives in memory alone
    pub volatile: bool,
    /// The control thread, which commits the progress
    pub control: kanal::Sender<ControlRequest>,
    /// The loop, told when the driver is done
    pub loop_tx: AsyncSender<ServerMsg<D>>,
    /// Which try of the group's backup this is, from zero: the tries before it met a refusal for
    /// now (item 322)
    pub attempt: u32,
}

impl<D: ShoalDatabase> BackupContext<D> {
    /// Commit where the group stands
    ///
    /// # Arguments
    ///
    /// * `progress` - Where it stands
    async fn commit(&self, progress: GroupBackup) -> Result<(), String> {
        let started = Instant::now();
        let mut last = String::new();
        // a progress is idempotent, so a proposal that found no control leader is sent again
        while started.elapsed() < PROGRESS_TIMEOUT {
            let (reply, rx) = kanal::bounded(1);
            let command = ControlCommand::BackupProgress {
                op: self.op,
                group: self.group,
                node: self.me.node,
                incarnation: self.incarnation,
                progress: progress.clone(),
            };
            // a control thread too busy to take it is a refusal for now (item 322)
            self.control
                .try_send(ControlRequest::Propose { command, reply })
                .map_err(|_| format!("{RETRIABLE}the control thread is not taking proposals"))?;
            let remaining = PROGRESS_TIMEOUT.saturating_sub(started.elapsed());
            // the answer's receive is kept, never raced bare: a timer that wins after the answer
            // was handed over would drop it (Resolved #152)
            let mut answer = KeptReceiver::new(rx.to_async());
            let answered =
                glommio::timer::timeout(remaining, async { Ok(answer.next().await) }).await;
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
        // a progress the control plane did not commit in time may be committed by a later try
        Err(format!(
            "{RETRIABLE}the progress was not committed within {PROGRESS_TIMEOUT:?}: {last}"
        ))
    }

    /// Whether this shard still leads the group
    fn leads(&self) -> bool {
        self.raft.metrics().borrow_watched().current_leader == Some(self.me)
    }

    /// Move this shard's checkpoint for the group past an index, so the cut holds everything
    /// applied before the backup was asked for
    ///
    /// A persistent group's cut is the archives at the compactor's boundary, which lags the
    /// log by whatever the active segment holds; an entry is proposed so the group has a frame
    /// past the index, the WAL is rotated and swept so the segment is handed to the compactor,
    /// and the checkpoint is polled until it passes or the deadline does - the repair driver's
    /// nudge ([F44](../../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `past` - The index to pass
    async fn advance_past(&self, past: u64) -> Result<(), String> {
        let started = Instant::now();
        while self.state.borrow().checkpoint_index() <= past {
            // a checkpoint a busy compactor has not moved yet may have by a later try (item 322)
            if started.elapsed() > self.timeout {
                return Err(format!(
                    "{RETRIABLE}the checkpoint did not pass {past} within {:?}",
                    self.timeout
                ));
            }
            let written = glommio::timer::timeout(self.timeout, async {
                Ok(self
                    .raft
                    .client_write(Command::scrub(
                        self.table,
                        crate::server::replication::digest::NUDGE,
                    ))
                    .await)
            })
            .await;
            match written {
                Ok(Ok(_)) => {}
                Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))) => {
                    return Err(format!(
                        "{NOT_LEADER}group {}: the leader is {:?}",
                        self.group,
                        forward.leader_node.or(forward.leader_id)
                    ));
                }
                Ok(Err(error)) => return Err(format!("proposing a nudge: {error}")),
                // a group that could not commit a nudge in time may by a later try (item 322)
                Err(_) => return Err(format!("{RETRIABLE}the nudge did not commit in time")),
            }
            let (reply, done) = futures_channel::oneshot::channel();
            self.loop_tx
                .send(ServerMsg::RepairRotate { reply })
                .await
                .map_err(|error| format!("{error:?}"))?;
            let _ = done.await;
            // the compactor's merge moves the checkpoint on its own time
            let waited = Instant::now();
            while self.state.borrow().checkpoint_index() <= past
                && waited.elapsed() < Duration::from_secs(5)
            {
                glommio::timer::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }
}

/// Drive one group's backup to the end, and tell the loop
///
/// # Arguments
///
/// * `context` - Everything the driver needs
pub async fn drive_group_backup<D: ShoalDatabase>(context: BackupContext<D>) {
    let outcome = drive_inner(&context).await;
    let mut retry = false;
    let phase = match outcome {
        Ok(()) => BackupPhase::Done,
        Err(error) if error.starts_with(NOT_LEADER) => {
            // a driver that lost the lead leaves the group for the new leader
            event!(Level::INFO, msg = "a group's backup is left for its new leader", op = %context.op, group = %context.group, error);
            let _ = context.commit(GroupBackup::default()).await;
            BackupPhase::Pending
        }
        // a refusal for now leaves the group as it stands, and its backup is cut again after a
        // pause, until its tries run out, as an archive's is (item 322)
        Err(error) if error.starts_with(RETRIABLE) && context.attempt + 1 < BACKUP_ATTEMPTS => {
            event!(Level::WARN, msg = "a group's backup met a refusal for now and is driven again", op = %context.op, group = %context.group, attempt = context.attempt + 1, of = BACKUP_ATTEMPTS, error);
            retry = true;
            BackupPhase::Cutting
        }
        Err(error) => {
            event!(Level::ERROR, msg = "a group's backup failed", op = %context.op, group = %context.group, error);
            let _ = context
                .commit(GroupBackup {
                    phase: BackupPhase::Done,
                    driver: Some(context.me.node),
                    boundary: None,
                    outcome: Some(BackupOutcome::Failed { reason: error }),
                })
                .await;
            BackupPhase::Done
        }
    };
    let _ = context
        .loop_tx
        .send(ServerMsg::BackupDone {
            op: context.op,
            group: context.group,
            phase,
            retry,
        })
        .await;
}

/// Everything an `ObjectMeta` group's archive driver needs (F93)
pub struct ArchiveContext<D: ShoalDatabase> {
    /// What every group's driver needs
    pub backup: BackupContext<D>,
    /// What its object tasks are made from
    pub(crate) base: crate::server::shard::objects::archive::ObjectsBase<D>,
    /// The bucket, by name
    pub bucket: String,
    /// The group's tablets
    pub tablets: Vec<u16>,
}

/// Drive one `ObjectMeta` group's archive to the end, and tell the loop
///
/// A bucket is backed up as its objects, never as its rows: every live object the group leads,
/// its entry and its bytes at one state, into `<path>/<op>/<Bucket>/<group>.objects` with a
/// manifest of kind `objects` beside it ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
///
/// # Arguments
///
/// * `context` - Everything the driver needs
pub async fn drive_group_archive<D: ShoalDatabase>(context: ArchiveContext<D>)
where
    <D::ClientType as crate::shared::traits::QuerySupport>::QueryKinds: Clone,
{
    let backup = &context.backup;
    let outcome = glommio::timer::timeout(backup.timeout, async { Ok(archive_inner(&context).await) })
        .await
        .unwrap_or_else(|_| Err(format!("the archive did not finish within {:?}", backup.timeout)));
    let mut retry = false;
    let phase = match outcome {
        Ok(()) => BackupPhase::Done,
        Err(error) if error.starts_with(NOT_LEADER) => {
            event!(Level::INFO, msg = "a group's archive is left for its new leader", op = %backup.op, group = %backup.group, error);
            let _ = backup.commit(GroupBackup::default()).await;
            BackupPhase::Pending
        }
        // a refusal for now leaves the group writing, and its archive is driven again after a
        // pause, until its tries run out (item 322)
        Err(error) if error.starts_with(RETRIABLE) && backup.attempt + 1 < BACKUP_ATTEMPTS => {
            event!(Level::WARN, msg = "a group's archive met a refusal for now and is driven again", op = %backup.op, group = %backup.group, attempt = backup.attempt + 1, of = BACKUP_ATTEMPTS, error);
            retry = true;
            BackupPhase::Writing
        }
        Err(error) => {
            event!(Level::ERROR, msg = "a group's archive failed", op = %backup.op, group = %backup.group, error);
            let _ = backup
                .commit(GroupBackup {
                    phase: BackupPhase::Done,
                    driver: Some(backup.me.node),
                    boundary: None,
                    outcome: Some(BackupOutcome::Failed { reason: error }),
                })
                .await;
            BackupPhase::Done
        }
    };
    let _ = backup
        .loop_tx
        .send(ServerMsg::BackupDone {
            op: backup.op,
            group: backup.group,
            phase,
            retry,
        })
        .await;
}

/// The archive's phases: writing, then done with the file
///
/// # Arguments
///
/// * `context` - Everything the driver needs
async fn archive_inner<D: ShoalDatabase>(context: &ArchiveContext<D>) -> Result<(), String>
where
    <D::ClientType as crate::shared::traits::QuerySupport>::QueryKinds: Clone,
{
    use crate::server::objects::archive::{
        write_manifest, ArchiveWriter, ObjectsManifest, ARCHIVE_FORMAT, ARCHIVE_SUFFIX,
        OBJECTS_KIND,
    };
    use crate::server::shard::walk::{TabletWalk, WalkAsk};
    let backup = &context.backup;
    let me = backup.me.node;
    if !backup.leads() {
        return Err(format!("{NOT_LEADER}group {} is led elsewhere", backup.group));
    }
    // said first, so a driver that dies here is visibly the one that was writing
    backup
        .commit(GroupBackup {
            phase: BackupPhase::Writing,
            driver: Some(me),
            boundary: None,
            outcome: None,
        })
        .await?;
    // the archive, under <path>/<op>/<Bucket>/
    let target = backup
        .path
        .join(backup.op.to_string())
        .join(&context.bucket)
        .join(format!("{}{ARCHIVE_SUFFIX}", backup.group));
    let mut writer = ArchiveWriter::create(&target, context.base.bucket, backup.group, backup.op).await?;
    // every live object of every tablet the group serves, as the walk hands them out
    for tablet in &context.tablets {
        let mut walk = TabletWalk::<D>::new(
            context.base.shard_tx.clone(),
            WalkAsk::tablet(backup.table, *tablet),
        );
        let mut shed = 0u32;
        loop {
            let page = match walk.next().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                // a shard reading as many pages as it reads at once is asked again shortly
                Err(refusal)
                    if refusal.code == crate::shared::protocol::error::ErrorCode::Shedding
                        && shed < 1000 =>
                {
                    shed += 1;
                    glommio::timer::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                Err(refusal) => {
                    return Err(crate::server::shard::objects::archive::refusal(
                        refusal.code,
                        format!("walking tablet {tablet}: {}", refusal.msg),
                    ));
                }
            };
            let rows = page
                .objects::<D>()
                .map_err(|error| format!("reading a page of tablet {tablet}: {error:?}"))?;
            for (_, row) in rows {
                for entry in row.entries.iter().filter(|entry| entry.state.live) {
                    // a driver that lost the lead hands the archive to the next leader
                    if !backup.leads() {
                        return Err(format!("{NOT_LEADER}group {} moved its lead", backup.group));
                    }
                    context.base.archive(entry, &mut writer).await?;
                }
            }
        }
    }
    let totals = writer.finish().await?;
    // the manifest beside it, which a restore judges it by
    let manifest = ObjectsManifest {
        kind: OBJECTS_KIND.to_string(),
        format: ARCHIVE_FORMAT,
        op: backup.op,
        cluster: backup.cluster,
        origin: me,
        bucket: context.bucket.clone(),
        bucket_id: context.base.bucket,
        group: backup.group,
        // the tablets walked, which a restore judges the bucket's archives whole by (item 315)
        tablets: context.tablets.clone(),
        rows_layout: crate::shared::objects::OBJECT_ROWS_LAYOUT,
        records: totals.records,
        bytes: totals.bytes,
        total: totals.total,
        checksum: totals.checksum,
        created_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX)),
    };
    write_manifest(&target, &manifest).await?;
    event!(Level::INFO, msg = "wrote a group's archive of objects", op = %backup.op, group = %backup.group, objects = totals.records, bytes = totals.bytes, file = %target.display());
    backup
        .commit(GroupBackup {
            phase: BackupPhase::Done,
            driver: Some(me),
            boundary: None,
            outcome: Some(BackupOutcome::Written {
                file: target.to_string_lossy().into_owned(),
                bytes: totals.total,
                checksum: totals.checksum,
                records: totals.records,
                // an archive holds no remembered requests, and says how many tries it took apart
                retries: 0,
                attempts: backup.attempt + 1,
            }),
        })
        .await?;
    Ok(())
}

/// The phases, each committed before the next
///
/// # Arguments
///
/// * `context` - Everything the driver needs
async fn drive_inner<D: ShoalDatabase>(context: &BackupContext<D>) -> Result<(), String> {
    let me = context.me.node;
    if !context.leads() {
        return Err(format!(
            "{NOT_LEADER}group {} is led elsewhere",
            context.group
        ));
    }
    // an ephemeral table's group holds nothing a restart keeps, so a backup of it would be a
    // file nothing could ever restore into a group that had lost it
    if context.volatile {
        context
            .commit(GroupBackup {
                phase: BackupPhase::Done,
                driver: Some(me),
                boundary: None,
                outcome: Some(BackupOutcome::Skipped {
                    reason: "an ephemeral table's group is not backed up: its rows do not survive a restart to begin with".to_string(),
                }),
            })
            .await?;
        return Ok(());
    }
    // the cut: said first, so a driver that dies here is visibly the one that was cutting
    context
        .commit(GroupBackup {
            phase: BackupPhase::Cutting,
            driver: Some(me),
            boundary: None,
            outcome: None,
        })
        .await?;
    let started = Instant::now();
    // everything applied before the backup was asked for has to be in the cut, so the
    // checkpoint is moved past it first
    let applied = context.state.borrow().applied_index();
    context.advance_past(applied).await?;
    // a test's refusal of the cut, for now or for good (item 322)
    if let Some(for_now) = crate::server::objects::faults::backup_cut_refused() {
        let why = "the group's cut was refused by a test".to_string();
        return Err(if for_now { format!("{RETRIABLE}{why}") } else { why });
    }
    // the group's snapshot at or past its checkpoint, held so the sweep cannot delete it
    let built: Rc<BuiltSnapshot> = glommio::timer::timeout(context.timeout, async {
        Ok(context.network.build(context.group, applied).await)
    })
    .await
    .map_err(|_| format!("{RETRIABLE}the cut did not land within {:?}", context.timeout))?
    .map_err(|error| format!("cutting the snapshot: {error}"))?;
    let boundary = built.manifest.boundary.index;
    context
        .commit(GroupBackup {
            phase: BackupPhase::Writing,
            driver: Some(me),
            boundary: Some(boundary),
            outcome: None,
        })
        .await?;
    // the copy, under <path>/<op>/<table>/, with its manifest beside it
    let dir = context
        .path
        .join(context.op.to_string())
        .join(&context.table_name);
    let name = snapshot::snapshot_name(context.group, boundary);
    let target = dir.join(&name);
    let remaining = context.timeout.saturating_sub(started.elapsed());
    let manifest = built.manifest.clone().stamped(context.cluster, me);
    glommio::timer::timeout(remaining, async {
        Ok(write_backup(
            &built.path,
            &target,
            &manifest,
            context.op,
            &context.table_name,
        )
        .await)
    })
    .await
    .map_err(|_| format!("{RETRIABLE}the copy did not finish within {:?}", context.timeout))?
    .map_err(|error| format!("writing {}: {error}", target.display()))?;
    event!(Level::INFO, msg = "wrote a group's backup", op = %context.op, group = %context.group, boundary, bytes = manifest.total, file = %target.display());
    context
        .commit(GroupBackup {
            phase: BackupPhase::Done,
            driver: Some(me),
            boundary: Some(boundary),
            outcome: Some(BackupOutcome::Written {
                file: target.to_string_lossy().into_owned(),
                bytes: manifest.total,
                checksum: manifest.checksum,
                records: manifest.records,
                retries: manifest.retries,
                attempts: context.attempt + 1,
            }),
        })
        .await?;
    Ok(())
}

/// Copy a snapshot file under the backup directory, write its manifest, and verify the copy
///
/// The file is written to a `.tmp` beside its final name, synced, renamed, and the directory
/// synced, so a crash leaves either the whole file or none of it; the manifest is written the
/// same way after it, so a manifest without its file never exists.
///
/// # Arguments
///
/// * `from` - The built snapshot
/// * `to` - Where the copy goes
/// * `manifest` - The snapshot's manifest, stamped with the cluster and the node that cut it
/// * `op` - The backup operation
/// * `table_name` - The table's name
async fn write_backup(
    from: &Path,
    to: &Path,
    manifest: &SnapshotManifest,
    op: Uuid,
    table_name: &str,
) -> std::io::Result<()> {
    let dir = to
        .parent()
        .ok_or_else(|| std::io::Error::other("a backup file has a parent"))?;
    std::fs::create_dir_all(dir)?;
    // an earlier try of this group's backup, cut at another boundary, leaves nothing beside this
    // one, since a restore refuses two files of one group (item 322)
    let earlier = format!("{}-", manifest.group);
    let this = to
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| std::io::Error::other("a backup file has a name"))?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(&earlier) && !name.starts_with(this) {
            std::fs::remove_file(entry.path())?;
        }
    }
    // the bytes, a buffer at a time, into a temporary beside the target
    let tmp = to.with_extension("snap.tmp");
    copy_file(from, &tmp).await?;
    std::fs::rename(&tmp, to)?;
    snapshot::sync_dir(dir).await?;
    // the copy has to verify against the manifest it is written beside
    snapshot::verify(to, manifest).await?;
    // and the manifest, the same way
    let record = BackupManifest::of(op, table_name, manifest);
    let json = serde_json::to_vec_pretty(&record).map_err(std::io::Error::other)?;
    let manifest_path = manifest_path(to);
    let tmp = manifest_path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &manifest_path)?;
    snapshot::sync_dir(dir).await?;
    Ok(())
}

/// The manifest's path beside a backup file
///
/// # Arguments
///
/// * `file` - The `.snap`
#[must_use]
pub fn manifest_path(file: &Path) -> PathBuf {
    let mut name = file.as_os_str().to_os_string();
    name.push(BACKUP_MANIFEST_SUFFIX);
    PathBuf::from(name)
}

/// Copy a file whole, a buffer at a time, and sync the copy
///
/// # Arguments
///
/// * `from` - The file
/// * `to` - The copy
async fn copy_file(from: &Path, to: &Path) -> std::io::Result<()> {
    let source = BufferedFile::open(from).await.map_err(to_io)?;
    let size = source.file_size().await.map_err(to_io)?;
    let target = BufferedFile::create(to).await.map_err(to_io)?;
    let mut pos = 0u64;
    while pos < size {
        let want = COPY_BYTES.min((size - pos) as usize);
        let read = source.read_at(pos, want).await.map_err(to_io)?;
        if read.is_empty() {
            break;
        }
        target.write_at(read.to_vec(), pos).await.map_err(to_io)?;
        pos += read.len() as u64;
    }
    target.fdatasync().await.map_err(to_io)?;
    target.close().await.map_err(to_io)?;
    source.close().await.map_err(to_io)?;
    Ok(())
}

/// Turn a glommio error into an io error
///
/// # Arguments
///
/// * `error` - The glommio error
fn to_io<T>(error: glommio::GlommioError<T>) -> std::io::Error {
    match error {
        glommio::GlommioError::IoError(error) => error,
        other => std::io::Error::other(other.to_string()),
    }
}

impl<D: ShoalDatabase> super::Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Forget a backup driver that finished, and look for the next group to drive
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `phase` - The phase it committed last
    /// * `retry` - Whether it met a refusal for now and is to be driven again after a pause (item 322)
    pub(super) fn handle_backup_done(&mut self, op: Uuid, group: GroupId, phase: BackupPhase, retry: bool) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving_backups.remove(&(op, group));
            replication.driven_backups.insert((op, group), phase);
            // a try that met a refusal for now is counted, and the group held until its pause is over
            if retry {
                let tries = replication.backup_retries.get(&(op, group)).map_or(0, |(tries, _)| *tries);
                replication
                    .backup_retries
                    .insert((op, group), (tries + 1, Instant::now() + BACKUP_RETRY_PAUSE));
            } else {
                replication.backup_retries.remove(&(op, group));
            }
        }
        self.drive_backups();
    }

    /// Start a driver for every pending group of every pending backup this shard leads
    ///
    /// Up to `cluster.backup.concurrent` at a time. A group whose phase is pending, or
    /// cutting or writing under a driver that is not running here - the previous leader, or
    /// this process before a restart - is this shard's to drive when its handle leads
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    pub(super) fn drive_backups(&mut self) {
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self.local.as_ref().map_or(0, |local| local.borrow().incarnation);
        let settings = self.conf.cluster.as_ref().map(|cluster| cluster.backup.clone()).unwrap_or_default();
        let loop_tx = self.shard_local_tx.clone();
        let cluster = map.cluster.unwrap_or_default();
        // what a bucket's archive driver makes its object tasks from, for every bucket a record
        // archives, before the replication state is borrowed (F93)
        let bases: std::collections::BTreeMap<String, Option<crate::server::shard::objects::archive::ObjectsBase<D>>> =
            map.backups
                .iter()
                .flat_map(|record| record.archives.values())
                .map(|bucket| (bucket.clone(), self.objects_base(&map, bucket)))
                .collect();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        // what this shard committed for a record the map no longer carries is forgotten, and the
        // tries its groups' backups met
        replication
            .driven_backups
            .retain(|(op, _), _| map.backups.iter().any(|record| record.op == *op));
        replication
            .backup_retries
            .retain(|(op, _), _| map.backups.iter().any(|record| record.op == *op));
        let now = Instant::now();
        for record in &map.backups {
            for (group, progress) in &record.groups {
                if replication.driving_backups.len() >= settings.concurrent as usize {
                    return;
                }
                if replication.driving_backups.contains(&(record.op, *group)) {
                    continue;
                }
                // a group's backup that met a refusal for now waits out its pause (item 322)
                let attempt = match replication.backup_retries.get(&(record.op, *group)) {
                    Some((_, after)) if *after > now => continue,
                    Some((tries, _)) => *tries,
                    None => 0,
                };
                // the phase as this shard last committed it, when the map is behind it
                let phase = match replication.driven_backups.get(&(record.op, *group)) {
                    Some(driven) if driven.rank() > progress.phase.rank() => driven,
                    _ => &progress.phase,
                };
                if !matches!(phase, BackupPhase::Pending | BackupPhase::Cutting | BackupPhase::Writing) {
                    continue;
                }
                let Some(slot) = replication.groups.get(group) else {
                    continue;
                };
                let Some(raft) = slot.raft.clone() else {
                    continue;
                };
                // only the leader drives, as the slot hosting the group
                let me = slot.spec.me(node);
                if raft.metrics().borrow_watched().current_leader != Some(me) {
                    continue;
                }
                let table_name = map
                    .tables
                    .iter()
                    .find(|(_, id)| *id == slot.spec.table)
                    .map(|(name, _)| name.clone())
                    .unwrap_or_else(|| slot.spec.table.to_string());
                replication.driving_backups.insert((record.op, *group));
                event!(Level::INFO, msg = "driving a group's backup", op = %record.op, group = %group, path = %record.path);
                let context = BackupContext {
                    raft,
                    network: replication.network.clone(),
                    me,
                    incarnation,
                    cluster,
                    group: *group,
                    table: slot.spec.table,
                    table_name,
                    state: slot.state.clone(),
                    op: record.op,
                    path: PathBuf::from(&record.path),
                    timeout: settings.timeout.duration(),
                    volatile: slot.store.is_volatile(),
                    control: control.clone(),
                    loop_tx: loop_tx.clone(),
                    attempt,
                };
                // a bucket's `ObjectMeta` group archives its objects, never its rows (F93)
                if let Some(bucket) = record.archives.get(group) {
                    let tablets = slot.spec.tablets.clone();
                    let Some(base) = bases.get(bucket).cloned().flatten() else {
                        replication.driving_backups.remove(&(record.op, *group));
                        let why = format!("bucket {bucket} is bound to no pool this node serves");
                        glommio::spawn_local(async move {
                            let _ = context
                                .commit(GroupBackup {
                                    phase: BackupPhase::Done,
                                    driver: Some(node),
                                    boundary: None,
                                    outcome: Some(BackupOutcome::Failed { reason: why }),
                                })
                                .await;
                        })
                        .detach();
                        continue;
                    };
                    glommio::spawn_local(drive_group_archive(ArchiveContext {
                        backup: context,
                        base,
                        bucket: bucket.clone(),
                        tablets,
                    }))
                    .detach();
                    continue;
                }
                glommio::spawn_local(drive_group_backup(context)).detach();
            }
        }
    }
}
