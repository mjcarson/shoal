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
//!   digest lifts the quarantines and the group is done. A member that did not report, or a scrub
//!   that could not be proposed, is ridden out as before the install, the group driven again from
//!   here (item 297); a copy that does not verify or stalled, and copies that disagree, fail it by
//!   name. The installed file's figures ride the record from the commit that names this phase, so
//!   a drive resumed here reports what was installed (item 327).
//!
//! An ephemeral table's groups are skipped by name: their memory log is what a restart empties,
//! and a backup of one holds nothing a restore could keep.

use shoal_channel::KeptReceiver;
use std::cell::{Cell, RefCell};
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
use super::snapshots::INSTALLING_ALREADY;
use crate::server::objects::faults::VerifyingFault;

/// What an error begins with when a member could not be reached, as against refusing
///
/// A group's restore that meets one is driven again rather than failed
/// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retries-unreachable.md)). Since
/// item 297 a verifying scrub after the install that missed a member's report, or could not be
/// proposed, is one too.
pub const TRANSIENT: &str = "a member could not be reached: ";

/// What an error begins with when the control plane did not take a driver's progress
///
/// Nothing about the group changed in the record, so the group is driven again from the phase
/// committed last rather than failed, which the control plane would not take either
/// ([Resolved #183](../../../../docs/src/appendix/resolved/restore-driver-uncommitted-done.md)).
pub const UNCOMMITTED: &str = "the progress was not committed: ";

/// What an error begins with when another drive took the group over
///
/// A drive under an older lead than the record's, or of a try the operation retried since, stops
/// where it is and commits nothing more: its word could fail a group a newer drive restores
/// (item 336).
pub const SUPERSEDED: &str = "a newer drive took the group over: ";

/// What an error begins with when the group's record stands past the phase this drive named
///
/// The drive started from a map that had not seen another drive's commit. It ends at once, and the
/// loop drives the group again from where the record stands (item 336).
pub const AHEAD: &str = "the group's record stands past this drive: ";

/// How many times a group's restore is driven again for want of a member before it fails
const RESTORE_ATTEMPTS: u32 = 12;

/// The first pause before a member at its stream budget is asked again (item 257)
const BUSY_PAUSE_MIN: Duration = Duration::from_millis(250);

/// The longest pause between asks of a member at its stream budget
const BUSY_PAUSE_MAX: Duration = Duration::from_secs(4);

/// How long a group's restore waits for an unreachable member before it is driven again
const RESTORE_RETRY_PAUSE: Duration = Duration::from_secs(5);
use super::ServerMsg;
use crate::server::control::backup::{
    BackupManifest, GroupRestore, InstalledFile, RestoreOutcome, RestorePhase,
};
use crate::server::control::plane::ControlRequest;
use crate::server::control::repair::{Quarantine, QuarantineAction, QuarantineReason};
use crate::server::control::types::{ControlCommand, ControlResponse, RefusalKind};
use crate::server::replication::network::{RepairSend, RpcFailure};
use crate::server::replication::snapshot::{
    self, SnapshotManifest, SnapshotProvenance, SnapshotReader, SnapshotTrailer, SnapshotWriter, SNAPSHOTS_DIR,
};
use crate::server::replication::{
    DataConfig, DigestIntegrity, GroupMachine, GroupPeer, MachineState, Remembered, ShardNetwork,
    ShardPeer,
};
use crate::server::ring::Ring;
use crate::server::ShoalDatabase;
use crate::shared::identity::{ClusterId, GroupId, ShardAddr, TableId};
use crate::shared::placement_state::PlacementState;
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
    /// The phase this driver last committed, which is where a group it hands back stands
    pub reached: RefCell<RestorePhase>,
    /// The file the group's install put on every member, as the record names it or as this driver
    /// installed it, which every commit carries and the outcome reports (item 327)
    pub installed: RefCell<Option<InstalledFile>>,
    /// The term of the group's lead this drive drives under, which every commit carries and the
    /// install's restart moves on (item 336)
    pub term: Cell<u64>,
    /// Where the group's record stands, once the control plane answered that it is past this drive
    /// (item 336)
    pub standing: RefCell<Option<GroupRestore>>,
    /// Whether a newer drive took the group over, which every later commit of this one is refused
    /// for (item 336)
    pub superseded: Cell<bool>,
    /// How many drives of this group were handed back for want of a member, before this one
    pub attempts: u32,
    /// Which retry of the operation's failed groups this drive belongs to
    /// ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retry.md))
    pub generation: u32,
    /// The fingerprint of the table this node serves the group under
    pub table_fingerprint: u64,
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
                .map_err(|_| format!("{UNCOMMITTED}the control thread is not taking proposals"))?;
            let remaining = PROGRESS_TIMEOUT.saturating_sub(started.elapsed());
            // the answer's receive is kept, never raced bare: a timer that wins after the answer
            // was handed over would drop it (Resolved #152)
            let mut answer = KeptReceiver::new(rx.to_async());
            let answered =
                glommio::timer::timeout(remaining, async { Ok(answer.next().await) }).await;
            last = match answered {
                Ok(Ok(Ok(ControlResponse::Applied { .. }))) => {
                    // the phase a hand-back resumes from is the one committed last, never one
                    // only asked for: a phase the control plane never took is not where the
                    // group stands
                    if progress.phase != RestorePhase::Done {
                        *self.reached.borrow_mut() = progress.phase.clone();
                    }
                    return Ok(());
                }
                // the record stands past this drive, which goes on from it rather than moving it
                // back: the loop drives the group again from where it stands (item 336)
                Ok(Ok(Ok(ControlResponse::RestoreAhead { record, .. }))) => {
                    let why = format!(
                        "{AHEAD}it stands at {:?} under term {}",
                        record.phase, record.term
                    );
                    *self.standing.borrow_mut() = Some(record);
                    return Err(why);
                }
                // a newer drive took the group over, and this one says nothing more (item 336)
                Ok(Ok(Ok(ControlResponse::Refused {
                    reason,
                    kind: RefusalKind::Superseded,
                }))) => {
                    self.superseded.set(true);
                    return Err(format!("{SUPERSEDED}{reason}"));
                }
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
        Err(format!("{UNCOMMITTED}not within {PROGRESS_TIMEOUT:?}: {last}"))
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
            attempts: self.attempts,
            failed_in: None,
            generation: self.generation,
            installed: *self.installed.borrow(),
            term: self.term.get(),
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
            // a peer that refused has said no; one that could not be reached may yet say yes
            Err(RpcFailure::Remote(msg)) => {
                Err(format!("{member} did not take the quarantine: {msg}"))
            }
            Err(failure) => Err(format!(
                "{TRANSIENT}{member} did not take the quarantine: {failure}"
            )),
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
    // the attempts as this driver leaves them, which only a hand-back for want of a member moves
    let mut attempts = context.attempts;
    let mut phase = match outcome {
        Ok(phase) => phase,
        Err(error) if error.starts_with(NOT_LEADER) => {
            // a driver that lost the lead leaves the group where it got to for the new leader:
            // the phase it reached, since a phase it passed may not be run twice (a loading
            // phase refuses a table an install already filled)
            event!(Level::INFO, msg = "a group's restore is left for its new leader", op = %context.op, group = %context.group, error);
            let reached = context.reached.borrow().clone();
            let _ = context
                .commit(GroupRestore {
                    driver: None,
                    ..context.at(reached.clone())
                })
                .await;
            reached
        }
        // a drive another took over stops where it is: anything it committed now could fail a
        // group a newer drive restores (item 336)
        Err(error) if error.starts_with(SUPERSEDED) => {
            event!(Level::INFO, msg = "a group's restore drive was taken over by a newer one", op = %context.op, group = %context.group, error);
            context.reached.borrow().clone()
        }
        // a drive that started behind the record ends, and the loop drives the group again from
        // where the record stands, which the end of this function hands it (item 336)
        Err(error) if error.starts_with(AHEAD) => {
            event!(Level::INFO, msg = "a group's restore drive started behind its record; it is driven again from the record", op = %context.op, group = %context.group, error);
            context.reached.borrow().clone()
        }
        // progress the control plane did not take changed nothing in the record: the group is
        // left at the phase committed last, and driven again from it, which is what the loop
        // does with the phase this answers
        Err(error) if error.starts_with(UNCOMMITTED) => {
            event!(Level::WARN, msg = "a group's restore progress was not committed; it will be driven again", op = %context.op, group = %context.group, error);
            glommio::timer::sleep(RESTORE_RETRY_PAUSE).await;
            context.reached.borrow().clone()
        }
        // a member that could not be reached - restarting, cut off - is waited for and the
        // group driven again from where it got to, a bounded number of times, rather than
        // failed: on the lab one member's restart failed a group, and a restore cannot be run
        // twice ([Resolved #155](../../../../docs/src/appendix/resolved/restore-retries-unreachable.md))
        Err(error) if error.starts_with(TRANSIENT) && context.attempts < RESTORE_ATTEMPTS => {
            event!(Level::WARN, msg = "a group's restore could not reach a member; trying again", op = %context.op, group = %context.group, attempt = context.attempts + 1, error);
            glommio::timer::sleep(RESTORE_RETRY_PAUSE).await;
            let reached = context.reached.borrow().clone();
            let committed = context
                .commit(GroupRestore {
                    driver: None,
                    attempts: context.attempts + 1,
                    ..context.at(reached.clone())
                })
                .await;
            // the loop drives the group again from these, since its map may not carry them yet
            // and a drive counted from the map's would set the attempts back (item 297)
            if committed.is_ok() {
                attempts = context.attempts + 1;
            }
            reached
        }
        Err(error) => {
            event!(Level::ERROR, msg = "a group's restore failed", op = %context.op, group = %context.group, error);
            // the phase it failed in is kept, which is where a retry drives it from
            let failed_in = context.reached.borrow().clone();
            let committed = context
                .commit(GroupRestore {
                    phase: RestorePhase::Done,
                    outcome: Some(RestoreOutcome::Failed { reason: error }),
                    failed_in: Some(failed_in.clone()),
                    ..context.at(RestorePhase::Done)
                })
                .await;
            match committed {
                // a newer drive, or a record past this one, holds the group: this drive's failure
                // is not the group's (item 336)
                Err(why) if why.starts_with(SUPERSEDED) || why.starts_with(AHEAD) => {
                    event!(Level::INFO, msg = "a failed restore drive was not the group's to fail", op = %context.op, group = %context.group, why);
                    failed_in
                }
                committed => settled_phase(committed, failed_in, context.op, context.group),
            }
        }
    };
    // whatever ended the drive, a record the control plane answered it with stands where the group
    // is: the loop drives it from there, with the record's attempts and file (item 336)
    let mut installed = *context.installed.borrow();
    if let Some(record) = context.standing.borrow().as_ref() {
        if record.phase.rank() > phase.rank() {
            phase = record.phase.clone();
        }
        attempts = attempts.max(record.attempts);
        installed = installed.or(record.installed);
    }
    let _ = context
        .loop_tx
        .send(ServerMsg::RestoreDone {
            op: context.op,
            group: context.group,
            generation: context.generation,
            phase,
            attempts,
            installed,
            // the lead this drive was taken over under, which this shard drives the group under
            // no more: a deposed shard that still believes it leads would otherwise propose a
            // refused progress at every tick (item 336)
            taken_over: context.superseded.get().then(|| context.term.get()),
        })
        .await;
}

/// How a group's last restore driver on this shard left it, which the map may be behind on
pub struct DrivenRestore {
    /// The try of the operation's failed groups it drove
    pub generation: u32,
    /// The phase it committed last
    pub phase: RestorePhase,
    /// The drives of the group handed back for want of a member, as it committed them last
    /// (item 297)
    pub attempts: u32,
    /// The file the group's install put on every member, as the drive or the record it was
    /// answered with names it (item 336)
    pub installed: Option<InstalledFile>,
    /// The term of the lead a newer drive took the group over from, under which this shard drives
    /// it no more (item 336)
    pub taken_over: Option<u64>,
}

/// The phase a finished driver tells its loop the group stands at, once it tried to commit it done
///
/// The loop remembers this phase and drives the group from it, or not at all once it is done.
/// Only a done the control plane took may say done: a failure it never recorded leaves the
/// record where it stood, and a loop that believed it done would never drive the group again
/// ([Resolved #183](../../../../docs/src/appendix/resolved/restore-driver-uncommitted-done.md)).
///
/// # Arguments
///
/// * `committed` - Whether the done was committed
/// * `reached` - The phase the driver committed last
/// * `op` - The restore, for the log line
/// * `group` - The group, for the log line
fn settled_phase(
    committed: Result<(), String>,
    reached: RestorePhase,
    op: Uuid,
    group: GroupId,
) -> RestorePhase {
    match committed {
        Ok(()) => RestorePhase::Done,
        Err(error) => {
            event!(Level::WARN, msg = "a group's failed restore could not be recorded; it will be driven again", %op, %group, phase = ?reached, error);
            reached
        }
    }
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
                        "{TRANSIENT}{member} did not report before the restore: {error}"
                    ))
                }
            }
        }
    }
    // installing: the file at a boundary of this run's own, on every member
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
            // a member at its stream budget is waited out within the transfer's deadline, as a
            // move's step waits for its stream: a refusal that passes never fails the group
            // (item 257)
            let started = std::time::Instant::now();
            let mut pause = BUSY_PAUSE_MIN;
            loop {
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
                    RepairSend::Installed => break,
                    RepairSend::Behind { checkpoint } => {
                        return Err(format!(
                            "{member}'s checkpoint {checkpoint} is past the restore boundary {}",
                            manifest.boundary.index
                        ));
                    }
                    RepairSend::Busy(why) if started.elapsed() < context.snapshot_timeout => {
                        event!(Level::DEBUG, msg = "a restore's install waits for its member's stream budget", group = %context.group, %member, why);
                        glommio::timer::sleep(pause).await;
                        pause = (pause * 2).min(BUSY_PAUSE_MAX);
                    }
                    RepairSend::Busy(why) => {
                        return Err(format!("{member} stayed busy for the whole transfer deadline: {why}"));
                    }
                }
            }
        }
        // this shard's own, through the loop, which restarts the group with the file pending; a
        // copy installing a snapshot of its own already is waited out within the transfer's
        // deadline, as a member's is (item 297)
        let started = std::time::Instant::now();
        let mut pause = BUSY_PAUSE_MIN;
        loop {
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
            match done
                .await
                .unwrap_or_else(|_| Err("the loop dropped the install".to_string()))
            {
                Ok(()) => break,
                Err(why)
                    if why.contains(INSTALLING_ALREADY)
                        && started.elapsed() < context.snapshot_timeout =>
                {
                    event!(Level::DEBUG, msg = "a restore's own install waits for the snapshot its copy is installing", group = %context.group, why);
                    glommio::timer::sleep(pause).await;
                    pause = (pause * 2).min(BUSY_PAUSE_MAX);
                }
                Err(why) => return Err(why),
            }
        }
        (raft, state) = context.handle_after_restart().await?;
        // the restart elected this shard again, under a term of its own, which its commits carry
        // from here (item 336)
        context
            .term
            .set(raft.metrics().borrow_watched().vote.leader_id().term);
        // the file's figures go in the record with `Verifying`, so a drive resumed there reports
        // them, where only this drive could count them (item 327)
        *context.installed.borrow_mut() = Some(InstalledFile {
            boundary: manifest.boundary.index,
            records: manifest.records,
            bytes: manifest.total,
            retries: manifest.retries,
        });
        context.commit(context.at(RestorePhase::Verifying)).await?;
        // a test's hand-back, the group left as a driver that lost its lead leaves it (item 327)
        if crate::server::objects::faults::restore_handed_back() {
            return Err(format!(
                "{NOT_LEADER}a test handed the group back after its install"
            ));
        }
    }
    // verifying: every member holds one verified digest, and the quarantines are lifted
    let fault = crate::server::objects::faults::verifying_fault();
    let scrub = match fault {
        // a test's stopped handle, failing the proposal as a restart under the driver does (item 297)
        Some(VerifyingFault::StoppedHandle) => Err(format!(
            "proposing the scrub of group {}: a test stopped its handle",
            context.group
        )),
        _ => {
            scrub_group(
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
            .await
        }
    };
    // a scrub that could not be proposed - its handle stopped under a restart, its entry not
    // committed in time - is ridden out as a member out of reach is before the install: the group
    // is driven again from `Verifying`, with the handle its shard holds then (item 297)
    let mut scrub = scrub.map_err(|error| {
        if error.starts_with(NOT_LEADER) {
            error
        } else {
            format!("{TRANSIENT}{error}")
        }
    })?;
    // a test's missing report, as a member that did not answer in time leaves one (item 297)
    if fault == Some(VerifyingFault::MissingReport) {
        if let Some((_, report)) = scrub
            .reports
            .iter_mut()
            .find(|(member, _)| **member != context.me)
        {
            *report = Err(format!(
                "no report within {:?}: a test dropped it",
                context.timeout
            ));
        }
    }
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
            // a copy that cannot read a partition never will: the restore fails by name
            Err(error) if scrub.stalled.contains(member) => {
                return Err(format!("{member}'s restored copy stalled: {error}"))
            }
            // a member that did not report in time is ridden out as one out of reach before the
            // install is, the group driven again from `Verifying` (item 297)
            Err(error) => {
                return Err(format!(
                    "{TRANSIENT}{member} did not report after the restore: {error}"
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
    // the file this drive installed, or the one the record names for a drive resumed past it
    let file = context.installed.borrow().unwrap_or_default();
    event!(Level::INFO, msg = "restored a group", op = %context.op, group = %context.group, boundary = file.boundary, records = file.records, verified = scrub.boundary);
    context
        .commit(GroupRestore {
            outcome: Some(RestoreOutcome::Restored {
                boundary: file.boundary,
                records: file.records,
                bytes: file.bytes,
                retries: file.retries,
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
    let placement = PlacementState::default();
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
        let trailer = reader
            .trailer()
            .await
            .map_err(|error| format!("reading the trailer of {}: {error}", file.display()))?;
        remembered.extend(trailer.retries);
        // nothing a cluster minted travels: a placement group's state names the source's pool
        // map, and only a bucket's tables carry one, which a restore never takes as table files
        // (F93); a destination places every stripe by its own map
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
        context.table_fingerprint,
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
    let remembered_count = remembered.len();
    let (total, checksum) = writer
        .finish(&SnapshotTrailer::new(remembered, placement))
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
        table_fingerprint: context.table_fingerprint,
        boundary,
        membership,
        tablets: context.tablets.clone(),
        records: records.len() as u64,
        total,
        checksum,
        retries: u32::try_from(remembered_count).unwrap_or(u32::MAX),
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
    /// * `generation` - The try of the operation's failed groups it drove
    /// * `phase` - The phase it committed last
    /// * `attempts` - The drives handed back for want of a member, as it committed them last
    /// * `installed` - The file the group's install put on every member, if it names one
    /// * `taken_over` - The term of the lead the drive was taken over under, if it was
    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_restore_done(
        &mut self,
        op: Uuid,
        group: GroupId,
        generation: u32,
        phase: RestorePhase,
        attempts: u32,
        installed: Option<InstalledFile>,
        taken_over: Option<u64>,
    ) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving_restores.remove(&(op, group));
            replication.driven_restores.insert(
                (op, group),
                DrivenRestore {
                    generation,
                    phase,
                    attempts,
                    installed,
                    taken_over,
                },
            );
        }
        self.drive_restores();
    }

    /// Forget an archive's restore driver that finished, and look for the next to drive (F93)
    ///
    /// The map may still show the archive as it stood before its end: the control leader
    /// answers the driver's last commit before this node's map carries it. So the end is
    /// remembered here and trusted over the map, and an archive whose end did not commit is
    /// driven again only after a wait, never at once
    /// ([Resolved #261](../../../../docs/src/appendix/resolved/archive-driven-once.md)).
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `archive` - The archive
    /// * `committed` - The try whose end the control plane applied, if it did
    pub(super) fn handle_archive_done(&mut self, op: Uuid, archive: String, committed: Option<u32>) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving_archives.remove(&(op, archive.clone()));
            // ended for this try, or to be driven again once the wait is over
            let driven = match committed {
                Some(generation) => DrivenArchive::Ended(generation),
                None => DrivenArchive::RetryAt(Instant::now() + ARCHIVE_REDRIVE_AFTER),
            };
            replication.driven_archives.insert((op, archive), driven);
        }
        self.drive_archives();
    }

    /// Start a driver for every archive of objects a restore deals this node, once its tables are
    ///
    /// Shard zero of the node a restore dealt an archive to puts its objects again through its own
    /// coordinator, one archive at a time, resuming past the objects its last committed progress
    /// counted; every host holds every archive once the backup is shipped
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
    pub(super) fn drive_archives(&mut self) {
        if self.shard_id != 0 {
            return;
        }
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self.local.as_ref().map_or(0, |local| local.borrow().incarnation);
        let loop_tx = self.shard_local_tx.clone();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        // what a restore the map no longer carries ended is forgotten
        replication
            .driven_archives
            .retain(|(op, _), _| map.restores.iter().any(|record| record.op == *op));
        // every archive dealt here and not done, of a restore whose tables are, but one a
        // driver here ended for this try or is waiting to drive again
        let now = Instant::now();
        let mut ready = Vec::new();
        for record in map.restores.iter().filter(|record| record.tables_done()) {
            for (name, progress) in &record.archives {
                if progress.node != node || progress.is_done() {
                    continue;
                }
                let driven = replication.driven_archives.get(&(record.op, name.clone()));
                if !archive_held(driven, progress.generation, now) {
                    ready.push((record.op, record.path.clone(), name.clone(), progress.clone()));
                }
            }
        }
        for (op, path, name, progress) in ready {
            let Some(replication) = self.replication.as_mut() else {
                return;
            };
            // one at a time on the shard: each is a stream of puts
            if !replication.driving_archives.is_empty() {
                return;
            }
            let Some(base) = self.objects_base(&map, &progress.file.bucket) else {
                continue;
            };
            if let Some(replication) = self.replication.as_mut() {
                replication.driving_archives.insert((op, name.clone()));
            }
            event!(Level::INFO, msg = "putting an archive's objects again", %op, archive = %name, from = progress.put);
            glommio::spawn_local(drive_archive::<D>(ArchiveDriver {
                base,
                control: control.clone(),
                op,
                path: PathBuf::from(path),
                name,
                progress,
                node,
                incarnation,
                loop_tx: loop_tx.clone(),
            }))
            .detach();
        }
    }

    /// Start a driver for every group of every pending restore this shard leads
    ///
    /// One at a time per shard, since a restore's install restarts the group and a scrub reads
    /// its archives whole. A group whose phase is not done, under a driver that is not running
    /// here, is this shard's to drive from that phase when its handle leads
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    pub(super) fn drive_restores(&mut self) {
        // and the archives of objects a restore puts again once its tables are restored (F93)
        self.drive_archives();
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self.local.as_ref().map_or(0, |local| local.borrow().incarnation);
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
                // the phase as this shard last committed it, when the map is behind it - for the
                // same try: a group retried since starts again from the record
                let driven = replication
                    .driven_restores
                    .get(&(record.op, *group))
                    .filter(|driven| driven.generation == progress.generation);
                let phase = match driven {
                    Some(driven) if driven.phase.rank() > progress.phase.rank() => {
                        driven.phase.clone()
                    }
                    _ => progress.phase.clone(),
                };
                // and the attempts, which the map is behind on in the same way: counted from the
                // map's, a drive straight after a hand-back would set them back (item 297)
                let attempts = driven.map_or(progress.attempts, |driven| {
                    driven.attempts.max(progress.attempts)
                });
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
                // the lead, and the term of it the drive drives under, which fences it from any
                // other drive of the group (item 336)
                let (leader, term) = {
                    let metrics = raft.metrics();
                    let watched = metrics.borrow_watched();
                    (watched.current_leader, watched.vote.leader_id().term)
                };
                if leader != Some(me) {
                    continue;
                }
                // a lead a newer drive took the group over from drives it no more; a newer lead
                // of this shard's may (item 336)
                if taken_over_under(driven, term) {
                    continue;
                }
                // the file a drive here learned of, which the map may not carry yet (item 336)
                let installed = driven
                    .and_then(|driven| driven.installed)
                    .or(progress.installed);
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
                    reached: RefCell::new(phase.clone()),
                    installed: RefCell::new(installed),
                    term: Cell::new(term),
                    standing: RefCell::new(None),
                    superseded: Cell::new(false),
                    attempts,
                    generation: progress.generation,
                    phase,
                    // the table's own fingerprint, which the restored file carries
                    table_fingerprint: <D::ClientType as crate::shared::traits::QuerySupport>::table_fingerprint_of(
                        slot.spec.table,
                    )
                    .unwrap_or(0),
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

/// Everything an archive's restore driver needs, gathered on the loop before the task starts
pub struct ArchiveDriver<D: ShoalDatabase> {
    /// What its puts are made from
    pub(crate) base: crate::server::shard::objects::archive::ObjectsBase<D>,
    /// The control thread, which commits the progress
    pub control: kanal::Sender<crate::server::control::plane::ControlRequest>,
    /// The restore operation
    pub op: Uuid,
    /// The backup directory
    pub path: PathBuf,
    /// The archive, by its name under it
    pub name: String,
    /// Where the archive stood when the driver started
    pub progress: crate::server::control::backup::ArchiveRestore,
    /// This node
    pub node: crate::shared::identity::NodeId,
    /// This node's incarnation
    pub incarnation: u64,
    /// The loop, told when the driver is done
    pub loop_tx: kanal::AsyncSender<ServerMsg<D>>,
}

/// How many objects are put again between two commits of an archive's progress
const ARCHIVE_PROGRESS_EVERY: u64 = 32;

/// Put an archive's objects again to its end, and tell the loop
///
/// # Arguments
///
/// * `driver` - Everything the driver needs
pub async fn drive_archive<D: ShoalDatabase>(driver: ArchiveDriver<D>)
where
    <D::ClientType as crate::shared::traits::QuerySupport>::QueryKinds: Clone,
{
    use crate::server::control::backup::ArchivePhase;
    let outcome = put_archive(&driver).await;
    let generation = driver.progress.generation;
    let mut done = driver.progress.clone();
    done.phase = ArchivePhase::Done;
    match outcome {
        Ok((put, bytes)) => {
            done.put = put;
            done.bytes = bytes;
            event!(Level::INFO, msg = "an archive's objects are put again", op = %driver.op, archive = %driver.name, objects = put, bytes);
        }
        Err((put, bytes, why)) => {
            done.put = put;
            done.bytes = bytes;
            event!(Level::ERROR, msg = "an archive's restore failed", op = %driver.op, archive = %driver.name, put, why);
            done.failed = Some(why);
        }
    }
    // the end committed, tried again a few times before the loop is told it did not
    let mut committed = None;
    for wait in ARCHIVE_END_WAITS {
        match commit_archive(&driver, done.clone()).await {
            Ok(()) => {
                committed = Some(generation);
                break;
            }
            Err(why) => {
                event!(Level::WARN, msg = "an archive's end was not committed", op = %driver.op, archive = %driver.name, why);
                glommio::timer::sleep(wait).await;
            }
        }
    }
    let _ = driver
        .loop_tx
        .send(ServerMsg::ArchiveDone {
            op: driver.op,
            archive: driver.name.clone(),
            committed,
        })
        .await;
}

/// How an archive's last driver on this shard ended, which the map may not carry yet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrivenArchive {
    /// Its end was committed for this try of the archive
    Ended(u32),
    /// Its end was not committed, and it is driven again from what the map says after this
    RetryAt(Instant),
}

/// Whether an archive the map says is not done is held from being driven here
///
/// # Arguments
///
/// * `driven` - How its last driver here ended, if one did
/// * `generation` - The try of the archive the map names
/// * `now` - The time now
fn archive_held(driven: Option<&DrivenArchive>, generation: u32, now: Instant) -> bool {
    match driven {
        // ended for this try: the map is behind; a retry since is a try of its own
        Some(DrivenArchive::Ended(ended)) => *ended == generation,
        // its end did not commit: driven again once the wait is over
        Some(DrivenArchive::RetryAt(at)) => *at > now,
        None => false,
    }
}

/// Whether a group's lead on this shard is one a newer drive took the group over from (item 336)
///
/// A drive refused `Superseded` ends at once, and a shard that still believes it leads would drive
/// the group again at its next tick, each drive a refused entry in the control log. It drives the
/// group again only once it leads under a newer term than the one its drive was taken over under.
///
/// # Arguments
///
/// * `driven` - How this shard's last drive of the group ended, for the try the map names
/// * `term` - The term of the lead this shard holds now
fn taken_over_under(driven: Option<&DrivenRestore>, term: u64) -> bool {
    driven
        .and_then(|driven| driven.taken_over)
        .is_some_and(|taken| term <= taken)
}

/// The waits between the tries of an archive's last commit
const ARCHIVE_END_WAITS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(4),
    Duration::from_secs(16),
];

/// How long an archive whose end did not commit waits before it is driven again
///
/// A drive again verifies the whole archive and skips what was put, so it is not started the
/// moment the last one gave up, which ran one after another while a commit failed at once.
const ARCHIVE_REDRIVE_AFTER: Duration = Duration::from_secs(30);

/// Put every object of an archive again past the ones already put
///
/// Answers how many objects and bytes are put, or how many were when it failed and why.
///
/// # Arguments
///
/// * `driver` - Everything the driver needs
async fn put_archive<D: ShoalDatabase>(driver: &ArchiveDriver<D>) -> Result<(u64, u64), (u64, u64, String)>
where
    <D::ClientType as crate::shared::traits::QuerySupport>::QueryKinds: Clone,
{
    use crate::server::control::backup::ArchivePhase;
    use crate::server::objects::archive::{manifest_path, read_manifest, ArchiveReader};
    let (mut put, mut bytes) = (driver.progress.put, driver.progress.bytes);
    let file = driver.path.join(&driver.name);
    // the archive, every record checked against its manifest before any is put
    let manifest = read_manifest(&manifest_path(&file)).map_err(|why| (put, bytes, why))?;
    let mut reader = ArchiveReader::open(&file, &manifest)
        .await
        .map_err(|why| (put, bytes, why))?;
    reader.skip(put).await.map_err(|why| (put, bytes, why))?;
    let mut progress = driver.progress.clone();
    progress.phase = ArchivePhase::Putting;
    commit_archive(driver, progress.clone()).await.map_err(|why| (put, bytes, why))?;
    loop {
        let Some(object) = reader.next().await.map_err(|why| (put, bytes, why))? else {
            return Ok((put, bytes));
        };
        // the identity of the put, the same on every resume that reaches this record
        let identity = format!("{}/{}/{put}", driver.op, driver.name);
        driver
            .base
            .put_again(&object, &identity)
            .await
            .map_err(|why| (put, bytes, why))?;
        put += 1;
        bytes += object.bytes.len() as u64;
        // a resumed driver starts past what was committed, and puts the rest again as the same puts
        if put % ARCHIVE_PROGRESS_EVERY == 0 {
            progress.put = put;
            progress.bytes = bytes;
            commit_archive(driver, progress.clone()).await.map_err(|why| (put, bytes, why))?;
        }
    }
}

/// Commit where an archive's restore stands
///
/// # Arguments
///
/// * `driver` - The driver
/// * `progress` - Where it stands
async fn commit_archive<D: ShoalDatabase>(
    driver: &ArchiveDriver<D>,
    progress: crate::server::control::backup::ArchiveRestore,
) -> Result<(), String> {
    use crate::server::control::plane::ControlRequest;
    use crate::server::control::types::{ControlCommand, ControlResponse};
    let started = std::time::Instant::now();
    let deadline = Duration::from_secs(30);
    let mut last = String::new();
    while started.elapsed() < deadline {
        let (reply, rx) = kanal::bounded(1);
        let command = ControlCommand::ArchiveProgress {
            op: driver.op,
            archive: driver.name.clone(),
            node: driver.node,
            incarnation: driver.incarnation,
            progress: progress.clone(),
        };
        driver
            .control
            .try_send(ControlRequest::Propose { command, reply })
            .map_err(|_| "the control thread is not taking proposals".to_string())?;
        // the answer's receive is kept, never raced bare (Resolved #152)
        let mut answer = shoal_channel::KeptReceiver::new(rx.to_async());
        let remaining = deadline.saturating_sub(started.elapsed());
        let answered = glommio::timer::timeout(remaining, async { Ok(answer.next().await) }).await;
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
    Err(format!("the progress was not committed within {deadline:?}: {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An archive whose end a driver here committed is not driven again while the map is behind,
    /// one whose end did not commit waits before it is, and a retry since is driven (item 261)
    #[test]
    fn an_archive_is_driven_once_its_map_is_behind() {
        let now = Instant::now();
        // never driven here: driven
        assert!(!archive_held(None, 0, now));
        // ended for this try: held however far behind the map is
        assert!(archive_held(Some(&DrivenArchive::Ended(0)), 0, now));
        // a retry put it back since: driven
        assert!(!archive_held(Some(&DrivenArchive::Ended(0)), 1, now));
        // its end did not commit: held for the wait, then driven
        let retry = DrivenArchive::RetryAt(now + ARCHIVE_REDRIVE_AFTER);
        assert!(archive_held(Some(&retry), 0, now));
        assert!(!archive_held(Some(&retry), 0, now + ARCHIVE_REDRIVE_AFTER));
    }

    /// A shard whose drive was taken over drives the group again only under a newer lead (item 336)
    ///
    /// A deposed shard that still believes it leads would otherwise propose a refused progress at
    /// every tick.
    #[test]
    fn a_taken_over_drive_waits_for_a_newer_lead() {
        // how a drive ended here, under the lead of term 3
        let ended = |taken_over: Option<u64>| DrivenRestore {
            generation: 0,
            phase: RestorePhase::Installing,
            attempts: 0,
            installed: None,
            taken_over,
        };
        // never driven here, or ended without being taken over: driven
        assert!(!taken_over_under(None, 3));
        assert!(!taken_over_under(Some(&ended(None)), 3));
        // taken over under term 3: not driven under it, nor under an older one it still believes in
        assert!(taken_over_under(Some(&ended(Some(3))), 3));
        assert!(taken_over_under(Some(&ended(Some(3))), 2));
        // elected again since, under term 4: driven
        assert!(!taken_over_under(Some(&ended(Some(3))), 4));
    }

    /// A failed group's done is only reported to the loop when the control plane took it
    ///
    /// The loop remembers the reported phase and never drives a group it believes done, so a
    /// done reported for a failure nobody recorded left the group at its phase for good
    /// (item 183).
    #[test]
    fn an_unrecorded_failure_is_not_reported_done() {
        let (op, group) = (Uuid::new_v4(), GroupId(7));
        // recorded: done, and nothing more to drive
        assert_eq!(
            settled_phase(Ok(()), RestorePhase::Loading, op, group),
            RestorePhase::Done
        );
        // not recorded: the phase committed last, which the loop drives again from
        assert_eq!(
            settled_phase(
                Err(format!("{UNCOMMITTED}not within 30s")),
                RestorePhase::Pending,
                op,
                group
            ),
            RestorePhase::Pending
        );
    }
}
