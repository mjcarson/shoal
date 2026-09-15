//! A move of one tablet group: the learner fed, caught up, made a voter, activated and retired
//!
//! The driver half of a replica migration ([F45](../../../../docs/src/features/replica-migration.md)),
//! mirrored on the repair driver. Only the group's current leader drives, every phase is
//! committed through `MoveProgress` before the step it names, a driver that loses the lead
//! hands the record to the next leader at the phase it reached, and any other failure commits
//! `Done` with the reason. The source never drives its own removal: a leader that is the
//! source transfers the lead to a member of the target once the destination has caught up.
//!
//! # Invariants
//!
//! **The group's committed membership wins over the record.** A driver reconciles first: a
//! group whose committed voters are already the target is at least `Configured`, whatever the
//! record says, and nothing here ever proposes a membership that is not the target.
//!
//! **The activation barrier is the destination's own apply.** A lag report says bytes
//! arrived; `Activated` is committed only once the destination has answered that it applied
//! past the uniform membership's index.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use kanal::AsyncSender;
use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::{ChangeMembers, Raft};
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level};
use uuid::Uuid;

use super::repair::NOT_LEADER;
use super::Shard;
use crate::server::control::migrate::{GroupMove, MoveOutcome, MovePhase};
use crate::server::control::plane::ControlRequest;
use crate::server::control::types::{ControlCommand, ControlResponse, MemberHealth};
use crate::server::database::ShoalDatabase;
use crate::server::map::MapCell;
use crate::server::messages::ServerMsg;
use crate::server::replication::{DataConfig, GroupMachine, MachineState, ShardNetwork, ShardPeer};
use crate::server::ServerError;
use crate::shared::identity::{GroupId, ShardAddr};
use crate::shared::traits::QuerySupport;

/// How long a driver waits for the control plane to commit its progress
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How long between two polls of a lag, a membership or a peer
const POLL: Duration = Duration::from_millis(100);

/// How long one probe of a peer may take
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a driver that handed the lead over waits before the group is looked at again
const HANDOFF_PAUSE: Duration = Duration::from_millis(500);

/// A copy of a group this shard no longer serves, kept for a grace before it is reclaimed
///
/// Retired when a published configuration drops this shard from the group's members under a
/// move naming it as the source. While retired, the group's tablets are refused by name on
/// this shard rather than served from files the cluster no longer counts.
#[derive(Debug, Clone)]
pub struct RetiredCopy {
    /// The table the group serves, in the schema's identity
    pub table: crate::shared::identity::TableId,
    /// The tablets it served
    pub tablets: Vec<u16>,
    /// The move that retired it
    pub op: Uuid,
    /// When the grace started
    pub at: Instant,
    /// Whether the group's log lives in memory alone
    pub volatile: bool,
    /// Whether the files are being reclaimed
    pub reclaiming: bool,
}

/// The move phase this process dies right after committing, for the crash matrix
pub mod crash_point {
    use super::MovePhase;
    use crate::shared::identity::GroupId;
    use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

    /// The armed phase, as its rank plus one; zero for none
    static ARMED: AtomicU8 = AtomicU8::new(0);

    /// The group the armed phase is for, or zero for any group
    static GROUP: AtomicU64 = AtomicU64::new(0);

    /// Arm a phase by name, for one group or any, or disarm with `none`
    ///
    /// # Arguments
    ///
    /// * `name` - The phase's name
    /// * `group` - The group whose driver dies, or none for whichever commits the phase first
    pub fn arm(name: &str, group: Option<GroupId>) -> Result<(), String> {
        GROUP.store(group.map_or(0, |group| group.0), Ordering::Relaxed);
        if name == "none" {
            ARMED.store(0, Ordering::Relaxed);
            return Ok(());
        }
        let phases = [
            MovePhase::Learner,
            MovePhase::CatchingUp,
            MovePhase::Reconfiguring,
            MovePhase::Configured,
            MovePhase::Activated,
            MovePhase::Retiring,
            MovePhase::Done,
        ];
        match phases.iter().find(|phase| phase.name() == name) {
            Some(phase) => {
                ARMED.store(phase.rank() + 1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(format!("{name} is not a move phase a driver commits")),
        }
    }

    /// Die here if this phase is armed for this group
    ///
    /// Exits with 137, the way a kill does, right after the phase's commit was acknowledged.
    ///
    /// # Arguments
    ///
    /// * `phase` - The phase just committed
    /// * `group` - The group it was committed for
    pub fn hit(phase: &MovePhase, group: GroupId) {
        let wanted = GROUP.load(Ordering::Relaxed);
        if ARMED.load(Ordering::Relaxed) == phase.rank() + 1 && (wanted == 0 || wanted == group.0) {
            tracing::error!(msg = "dying at an armed move phase", phase = phase.name(), group = %group);
            std::process::exit(137);
        }
    }
}

/// Everything a group's move driver needs, gathered on the loop before the task starts
pub struct MoveContext<D: ShoalDatabase> {
    /// The group's handle, which leads
    pub raft: Raft<DataConfig, GroupMachine<D>>,
    /// The shard's network
    pub network: ShardNetwork,
    /// The map this shard holds
    pub map: MapCell,
    /// This shard
    pub me: ShardAddr,
    /// This node's incarnation
    pub incarnation: u64,
    /// This shard's copy of the group's state
    pub state: Rc<RefCell<MachineState>>,
    /// The group
    pub group: GroupId,
    /// The record's operation
    pub op: Uuid,
    /// The member leaving
    pub from: ShardAddr,
    /// The member replacing it
    pub to: ShardAddr,
    /// The set as it was recorded
    pub expected: Vec<ShardAddr>,
    /// The set as it will be
    pub target: Vec<ShardAddr>,
    /// Where the group stands, as last committed
    pub start: GroupMove,
    /// Whether the record as a whole is published
    pub published: bool,
    /// How far behind the destination may be when it is made a voter
    pub catchup_lag: u64,
    /// How long one phase may take
    pub timeout: Duration,
    /// How long the source keeps its retired files
    pub retire_after: Duration,
    /// The control thread, which commits the progress
    pub control: kanal::Sender<ControlRequest>,
    /// The loop, which hears when the driver is done
    pub loop_tx: AsyncSender<ServerMsg<D>>,
}

impl<D: ShoalDatabase> MoveContext<D> {
    /// Commit where the group stands, and die here if the fixture armed this phase
    ///
    /// # Arguments
    ///
    /// * `progress` - Where it stands
    async fn commit(&self, progress: &GroupMove) -> Result<(), String> {
        let started = Instant::now();
        let mut last = String::new();
        // a progress is idempotent, so a proposal that found no control leader - one being
        // elected after a kill - is sent again until the deadline
        while started.elapsed() < PROGRESS_TIMEOUT {
            let (reply, rx) = kanal::bounded(1);
            let command = ControlCommand::MoveProgress {
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
                Ok(Ok(Ok(ControlResponse::Applied { .. }))) => {
                    crash_point::hit(&progress.phase, self.group);
                    return Ok(());
                }
                Ok(Ok(Ok(ControlResponse::Refused { reason }))) => {
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

    /// Whether this shard still leads the group
    fn leads(&self) -> bool {
        self.raft.metrics().borrow_watched().current_leader == Some(self.me)
    }

    /// Every member the committed configuration names, voter or learner
    fn committed_nodes(&self) -> BTreeSet<ShardAddr> {
        let watch = self.raft.metrics();
        let metrics = watch.borrow_watched();
        metrics
            .committed_membership_config
            .membership()
            .nodes()
            .map(|(addr, _)| *addr)
            .collect()
    }

    /// The group's committed voters, and whether the committed configuration is joint
    fn committed_voters(&self) -> (BTreeSet<ShardAddr>, bool, Option<u64>) {
        let watch = self.raft.metrics();
        let metrics = watch.borrow_watched();
        let committed = &metrics.committed_membership_config;
        let joint = committed.membership().get_joint_config().len() > 1;
        let index = committed.log_id().as_ref().map(|log_id| log_id.index);
        (committed.voter_ids().collect(), joint, index)
    }

    /// The destination's matched index, as the leader's replication has it, and the leader's last
    fn destination_lag(&self) -> (Option<u64>, u64) {
        let watch = self.raft.metrics();
        let metrics = watch.borrow_watched();
        let matched = metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&self.to))
            .and_then(|log_id| log_id.as_ref())
            .map(|log_id| log_id.index);
        (matched, metrics.last_log_index.unwrap_or(0))
    }

    /// Whether the map calls a member down
    ///
    /// # Arguments
    ///
    /// * `member` - The member
    fn is_down(&self, member: ShardAddr) -> bool {
        self.map
            .get()
            .members
            .get(&member.node)
            .is_some_and(|state| state.health == MemberHealth::Down)
    }
}

/// Drive one group's move from where the record stands to where this driver can take it
///
/// # Arguments
///
/// * `context` - Everything the driver needs
pub async fn drive_group<D: ShoalDatabase>(context: MoveContext<D>) {
    let mut progress = context.start.clone();
    let outcome = drive_group_inner(&context, &mut progress).await;
    if let Err(error) = outcome {
        // a driver that lost the lead leaves the group for the new leader, at the phase reached
        if error.starts_with(NOT_LEADER) {
            event!(Level::INFO, msg = "a group's move is left for its new leader", op = %context.op, group = %context.group, phase = progress.phase.name(), error);
            progress.driver = None;
            let _ = context.commit(&progress).await;
            // a lead being handed over is still this shard's for a moment; the next look
            // waits for the election rather than committing a hand-off per tick
            glommio::timer::sleep(HANDOFF_PAUSE).await;
        } else {
            event!(Level::ERROR, msg = "a group's move failed", op = %context.op, group = %context.group, phase = progress.phase.name(), error);
            progress.phase = MovePhase::Done;
            progress.driver = Some(context.me.node);
            progress.outcome = Some(MoveOutcome::Failed { reason: error });
            let _ = context.commit(&progress).await;
        }
    }
    let _ = context
        .loop_tx
        .send(ServerMsg::MoveDone {
            op: context.op,
            group: context.group,
            progress,
        })
        .await;
}

/// The phases, each committed before the next
///
/// # Arguments
///
/// * `context` - Everything the driver needs
/// * `progress` - Where the group stands, moved as the phases go
async fn drive_group_inner<D: ShoalDatabase>(
    context: &MoveContext<D>,
    progress: &mut GroupMove,
) -> Result<(), String> {
    let me = context.me.node;
    // the voters proposed: the record's target, less any member the group's committed
    // configuration does not name. a move adds its destination and nothing else, so a member
    // a recovery rewrote out of the group is not brought back by a record that still lists
    // it ([F49](../../../../docs/src/features/backup-and-recovery.md))
    let named = context.committed_nodes();
    let target: BTreeSet<ShardAddr> = context
        .target
        .iter()
        .copied()
        .filter(|member| *member == context.to || named.contains(member))
        .collect();
    progress.driver = Some(me);
    // reconcile first: the group's committed configuration wins over the record. A group
    // whose committed voters are the target is at least configured, and its uniform index is
    // the committed membership's, whatever a crashed driver failed to say
    let (voters, joint, index) = context.committed_voters();
    if voters == target && !joint && progress.phase.rank() < MovePhase::Configured.rank() {
        event!(Level::INFO, msg = "a group's committed membership is already the move's target; resuming from configured", op = %context.op, group = %context.group, index);
        progress.phase = MovePhase::Configured;
        progress.config = index;
        context.commit(progress).await?;
    }
    // the learner: the destination added to the group's nodes, fed by the leader's replication
    if progress.phase.rank() < MovePhase::Learner.rank() {
        step(progress, MovePhase::Learner);
        context.commit(progress).await?;
    }
    if progress.phase.rank() < MovePhase::Configured.rank() {
        add_learner(context).await?;
    }
    // the catch-up: the destination within the lag, its bytes and entries charged to the record
    if progress.phase.rank() < MovePhase::Reconfiguring.rank() {
        catch_up(context, progress).await?;
        // the source never drives its own removal: hand the lead to a member of the target
        if me == context.from.node {
            let successor = target
                .iter()
                .find(|member| {
                    **member != context.to && **member != context.me && !context.is_down(**member)
                })
                .copied()
                .ok_or_else(|| "no member of the target is up to take the lead".to_string())?;
            event!(Level::INFO, msg = "this leader is the move's source; transferring the lead before reconfiguring", op = %context.op, group = %context.group, to = %successor);
            progress.driver = None;
            context.commit(progress).await?;
            context
                .raft
                .trigger()
                .transfer_leader(successor)
                .await
                .map_err(|error| format!("transferring the lead to {successor}: {error}"))?;
            return Err(format!(
                "{NOT_LEADER}group {}: the lead was handed to {successor}",
                context.group
            ));
        }
        step(progress, MovePhase::Reconfiguring);
        context.commit(progress).await?;
    }
    // the transition: the library's joint configuration, then the uniform one
    if progress.phase.rank() < MovePhase::Configured.rank() {
        let index = reconfigure(context, &target).await?;
        progress.config = Some(index);
        step(progress, MovePhase::Configured);
        context.commit(progress).await?;
    }
    // the activation barrier: the destination's own apply past the uniform membership
    if progress.phase.rank() < MovePhase::Activated.rank() {
        let config = progress
            .config
            .ok_or_else(|| "a configured group records no uniform index".to_string())?;
        activate(context, config).await?;
        step(progress, MovePhase::Activated);
        context.commit(progress).await?;
    }
    // the publication is the control plane's, once every group of the set is activated: the
    // driver leaves the group here, freeing its slot for the set's other groups on this
    // shard, and is started again once the map carries the record published
    if !context.published {
        return Ok(());
    }
    if progress.phase.rank() < MovePhase::Retiring.rank() {
        step(progress, MovePhase::Retiring);
        context.commit(progress).await?;
    }
    // the retirement is the source's; the driver waits for its word, or its absence
    if progress.phase.rank() < MovePhase::Done.rank() {
        wait_retired(context).await?;
        step(progress, MovePhase::Done);
        progress.outcome = Some(MoveOutcome::Moved);
        context.commit(progress).await?;
    }
    Ok(())
}

/// Move a group's progress to a phase, charging the time spent in the phase left
///
/// # Arguments
///
/// * `progress` - The progress
/// * `phase` - The phase entered
fn step(progress: &mut GroupMove, phase: MovePhase) {
    let now = now_ms();
    let started = if progress.stats.since == 0 {
        now
    } else {
        progress.stats.since
    };
    let spent = now.saturating_sub(started);
    *progress
        .stats
        .phase_ms
        .entry(progress.phase.name().to_string())
        .or_default() += spent;
    progress.stats.since = now;
    progress.phase = phase;
}

/// Milliseconds since the epoch, for the record's timings and an identity's age
pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Add the destination as a learner, which a repeat re-adds harmlessly
///
/// # Arguments
///
/// * `context` - The driver
async fn add_learner<D: ShoalDatabase>(context: &MoveContext<D>) -> Result<(), String> {
    match context
        .raft
        .add_learner(context.to, context.to, false)
        .await
    {
        Ok(_) => Ok(()),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => Err(format!(
            "{NOT_LEADER}group {}: the leader is {:?}",
            context.group,
            forward.leader_node.or(forward.leader_id)
        )),
        Err(error) => Err(format!(
            "adding {} as a learner of group {}: {error}",
            context.to, context.group
        )),
    }
}

/// Wait until the destination's matched index is within the lag of the leader's last
///
/// `CatchingUp` is committed when the first progress is seen; the snapshot bytes sent and the
/// entries matched are charged to the record as they land.
///
/// # Arguments
///
/// * `context` - The driver
/// * `progress` - The progress, charged as the catch-up goes
async fn catch_up<D: ShoalDatabase>(
    context: &MoveContext<D>,
    progress: &mut GroupMove,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        if !context.leads() {
            return Err(format!(
                "{NOT_LEADER}group {}: the lead was lost while the destination caught up",
                context.group
            ));
        }
        if started.elapsed() > context.timeout {
            return Err(format!(
                "{} did not catch up within {:?}",
                context.to, context.timeout
            ));
        }
        let (matched, last) = context.destination_lag();
        progress.stats.bytes = context.network.bytes_sent_to(context.group, context.to);
        if let Some(matched) = matched {
            // the destination's log position once it is caught up: everything it was fed, by
            // snapshot and by log together, since a learner starts from nothing
            progress.stats.entries = matched;
            if progress.phase.rank() < MovePhase::CatchingUp.rank() {
                step(progress, MovePhase::CatchingUp);
                context.commit(progress).await?;
            }
            if last.saturating_sub(matched) <= context.catchup_lag {
                event!(Level::INFO, msg = "the destination caught up", op = %context.op, group = %context.group, to = %context.to, matched, last, bytes = progress.stats.bytes);
                return Ok(());
            }
        }
        glommio::timer::sleep(POLL).await;
    }
}

/// Write the group's membership transition to the target, and wait until it is committed
///
/// Whatever the group is in - the old uniform configuration, the joint one a lost leader
/// left behind, or the target already - the library's next coherent step from it toward the
/// target is what is proposed, so a repeat converges. Returns the uniform membership's index.
///
/// # Arguments
///
/// * `context` - The driver
/// * `target` - The voters the group ends with
async fn reconfigure<D: ShoalDatabase>(
    context: &MoveContext<D>,
    target: &BTreeSet<ShardAddr>,
) -> Result<u64, String> {
    let started = Instant::now();
    loop {
        if started.elapsed() > context.timeout {
            return Err(format!(
                "the membership transition did not commit within {:?}",
                context.timeout
            ));
        }
        let (voters, joint, index) = context.committed_voters();
        if voters == *target && !joint {
            let index =
                index.ok_or_else(|| "a committed membership records no index".to_string())?;
            event!(Level::INFO, msg = "the uniform membership naming the target is committed", op = %context.op, group = %context.group, index);
            return Ok(index);
        }
        let change = ChangeMembers::ReplaceAllVoters(target.clone());
        match context.raft.change_membership(change, false).await {
            Ok(_) => {}
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                return Err(format!(
                    "{NOT_LEADER}group {}: the leader is {:?}",
                    context.group,
                    forward.leader_node.or(forward.leader_id)
                ))
            }
            // a change still uncommitted is waited for, not failed
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::InProgress(_),
            ))) => {
                glommio::timer::sleep(POLL).await;
            }
            Err(error) => {
                return Err(format!(
                    "changing the membership of group {}: {error}",
                    context.group
                ))
            }
        }
        glommio::timer::sleep(POLL).await;
    }
}

/// Wait until the destination has the uniform membership durable and applied
///
/// # Arguments
///
/// * `context` - The driver
/// * `config` - The uniform membership's index
async fn activate<D: ShoalDatabase>(context: &MoveContext<D>, config: u64) -> Result<(), String> {
    let started = Instant::now();
    let peer = ShardPeer::new(context.to, context.network.clone());
    loop {
        if !context.leads() {
            return Err(format!(
                "{NOT_LEADER}group {}: the lead was lost while the destination activated",
                context.group
            ));
        }
        if started.elapsed() > context.timeout {
            return Err(format!(
                "{} did not apply the uniform membership at {config} within {:?}",
                context.to, context.timeout
            ));
        }
        // durable on the destination first, by the leader's own replication
        let (matched, _) = context.destination_lag();
        if matched.is_some_and(|matched| matched >= config) {
            // then applied there, by its own word
            match peer
                .applied(context.group, context.op, config, PROBE_TIMEOUT)
                .await
            {
                Ok(applied) if applied >= config => {
                    event!(Level::INFO, msg = "the destination applied the uniform membership", op = %context.op, group = %context.group, to = %context.to, applied, config);
                    return Ok(());
                }
                Ok(applied) => {
                    event!(Level::DEBUG, msg = "the destination has not applied the uniform membership yet", op = %context.op, group = %context.group, applied, config);
                }
                Err(failure) => {
                    event!(Level::DEBUG, msg = "the destination did not answer an applied probe", op = %context.op, group = %context.group, %failure);
                }
            }
        }
        glommio::timer::sleep(POLL).await;
    }
}

/// Wait until the source says its retired copy is gone, or is down, or the grace and a
/// timeout have passed
///
/// # Arguments
///
/// * `context` - The driver
async fn wait_retired<D: ShoalDatabase>(context: &MoveContext<D>) -> Result<(), String> {
    let started = Instant::now();
    let peer = ShardPeer::new(context.from, context.network.clone());
    let budget = context.retire_after + context.timeout;
    loop {
        if started.elapsed() > budget {
            event!(Level::WARN, msg = "the source never said its retired copy was gone; finishing the move without it", op = %context.op, group = %context.group, from = %context.from);
            return Ok(());
        }
        if context.is_down(context.from) {
            event!(Level::INFO, msg = "the source is down; it retires its copy when it returns", op = %context.op, group = %context.group, from = %context.from);
            return Ok(());
        }
        match peer.retired(context.group, PROBE_TIMEOUT).await {
            Ok(true) => {
                event!(Level::INFO, msg = "the source's retired copy is gone", op = %context.op, group = %context.group, from = %context.from);
                return Ok(());
            }
            Ok(false) => {}
            Err(failure) => {
                event!(Level::DEBUG, msg = "the source did not answer a retired probe", op = %context.op, group = %context.group, %failure);
            }
        }
        glommio::timer::sleep(POLL.max(Duration::from_millis(500))).await;
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
    /// Forget a move driver that finished, and look for the next group to drive
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    /// * `progress` - Where the driver left the group
    pub(super) fn handle_move_done(&mut self, op: Uuid, group: GroupId, progress: GroupMove) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving_moves.remove(&(op, group));
            // the map is behind the commit for a moment; what was committed is what counts
            replication.driven_moves.insert((op, group), progress);
        }
        self.drive_moves();
    }

    /// Start a driver for every group of every move this shard leads, up to the concurrency
    ///
    /// A group whose phase is not done, under a record that is planned or published, is this
    /// shard's to drive when its handle leads; a queued record waits for the transition ahead
    /// of it ([F45](../../../../docs/src/features/replica-migration.md)).
    pub(super) fn drive_moves(&mut self) {
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self
            .local
            .as_ref()
            .map_or(0, |local| local.borrow().incarnation);
        let migration = self
            .conf
            .cluster
            .as_ref()
            .map(|cluster| cluster.migration.clone())
            .unwrap_or_default();
        let loop_tx = self.shard_local_tx.clone();
        let map_cell = self.map.clone();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        // what this shard committed for a record the map no longer carries is forgotten
        replication
            .driven_moves
            .retain(|(op, _), _| map.moves.iter().any(|record| record.op == *op));
        for record in &map.moves {
            if record.is_queued() || record.is_done() {
                continue;
            }
            for (group, committed) in &record.groups {
                if replication.driving_moves.len() >= migration.concurrent as usize {
                    return;
                }
                if replication.driving_moves.contains(&(record.op, *group)) {
                    continue;
                }
                // the phase as this shard last committed it, when the map is behind it
                let progress = match replication.driven_moves.get(&(record.op, *group)) {
                    Some(driven) if driven.phase.rank() > committed.phase.rank() => driven.clone(),
                    _ => committed.clone(),
                };
                if progress.is_done() {
                    continue;
                }
                // a group activated under a record not yet published has nothing to do
                if progress.is_activated() && !record.is_published() {
                    continue;
                }
                let Some(slot) = replication.groups.get(group) else {
                    continue;
                };
                let Some(raft) = slot.raft.clone() else {
                    continue;
                };
                // only the leader drives, and only once per group at a time; this node's
                // member is the slot hosting the group ([F47](../../../../docs/src/features/local-rehome.md))
                let me = slot.spec.me(node);
                if raft.metrics().borrow_watched().current_leader != Some(me) {
                    continue;
                }
                replication.driving_moves.insert((record.op, *group));
                event!(Level::INFO, msg = "driving a group's move", op = %record.op, group = %group, phase = progress.phase.name(), from = %record.from, to = %record.to);
                let context = MoveContext {
                    raft,
                    network: replication.network.clone(),
                    map: map_cell.clone(),
                    me,
                    incarnation,
                    state: slot.state.clone(),
                    group: *group,
                    op: record.op,
                    from: record.from,
                    to: record.to,
                    expected: record.expected.clone(),
                    target: record.target.clone(),
                    start: progress,
                    published: record.is_published(),
                    catchup_lag: migration.catchup_lag,
                    timeout: migration.timeout.duration(),
                    retire_after: migration.retire_after.duration(),
                    control: control.clone(),
                    loop_tx: loop_tx.clone(),
                };
                glommio::spawn_local(drive_group(context)).detach();
            }
        }
    }
}

/// The directory under `wal/Shard-N/` a retired copy's marker lives in
pub const RETIRED_DIR: &str = "retired";

/// A retired copy's marker, as it is persisted
///
/// Written when the copy retires and removed once its files are reclaimed, so a restart
/// inside the grace resumes the retirement rather than serving the files or leaking them.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RetiredMarker {
    /// The move that retired the copy
    op: Uuid,
    /// The table the group serves
    table: crate::shared::identity::TableId,
    /// The tablets it served
    tablets: Vec<u16>,
    /// Whether the group's log lived in memory alone
    volatile: bool,
    /// When the grace started, in milliseconds since the epoch
    at_ms: u64,
}

/// Persist a retired copy's marker
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
/// * `group` - The group
/// * `copy` - The retired copy
pub async fn write_retired(
    wal_dir: &std::path::Path,
    group: GroupId,
    copy: &RetiredCopy,
) -> std::io::Result<()> {
    let dir = wal_dir.join(RETIRED_DIR);
    std::fs::create_dir_all(&dir)?;
    let marker = RetiredMarker {
        op: copy.op,
        table: copy.table,
        tablets: copy.tablets.clone(),
        volatile: copy.volatile,
        at_ms: now_ms().saturating_sub(u64::try_from(copy.at.elapsed().as_millis()).unwrap_or(0)),
    };
    let bytes = postcard::to_allocvec(&marker)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    crate::server::wal::write_atomic(&dir, &format!("{group}"), bytes).await
}

/// Remove a retired copy's marker, once its files are reclaimed
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
/// * `group` - The group
pub async fn clear_retired(wal_dir: &std::path::Path, group: GroupId) -> std::io::Result<()> {
    let path = wal_dir.join(RETIRED_DIR).join(format!("{group}"));
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    // the removal is made durable like the write was
    let directory = glommio::io::Directory::open(wal_dir.join(RETIRED_DIR))
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    directory
        .sync()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    directory
        .close()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(())
}

/// Every retired copy's marker under a shard's WAL directory, by group
///
/// A marker that does not decode still retires the group by name: its files are never served,
/// and the grace is counted from now.
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
#[must_use]
pub fn scan_retired(wal_dir: &std::path::Path) -> std::collections::HashMap<GroupId, RetiredCopy> {
    let mut found = std::collections::HashMap::new();
    let Ok(entries) = std::fs::read_dir(wal_dir.join(RETIRED_DIR)) else {
        return found;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        // a staged write that never renamed is not a marker
        let Ok(group) = u64::from_str_radix(&name, 16) else {
            continue;
        };
        let marker = std::fs::read(entry.path())
            .ok()
            .and_then(|bytes| postcard::from_bytes::<RetiredMarker>(&bytes).ok());
        let copy = match marker {
            Some(marker) => RetiredCopy {
                table: marker.table,
                tablets: marker.tablets,
                op: marker.op,
                at: Instant::now() - Duration::from_millis(now_ms().saturating_sub(marker.at_ms)),
                volatile: marker.volatile,
                reclaiming: false,
            },
            None => {
                tracing::event!(
                    tracing::Level::ERROR,
                    msg = "a retired marker does not decode; the copy is held retired by name",
                    group = name
                );
                RetiredCopy {
                    table: crate::shared::identity::TableId(0),
                    tablets: Vec::new(),
                    op: Uuid::nil(),
                    at: Instant::now(),
                    volatile: false,
                    reclaiming: false,
                }
            }
        };
        found.insert(GroupId(group), copy);
    }
    found
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Retire this shard's copy of a group a published move took from it
    ///
    /// The handle is shut down - openraft would otherwise keep a removed member as a
    /// candidate that never wins - the resident partitions are evicted, the log is forgotten,
    /// and the copy is held with its files for the grace, refusing every query of its tablets
    /// by name. A volatile copy has no files, so its grace is the marker alone
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `slot` - The copy, taken out of the groups
    /// * `op` - The move that retired it
    pub(super) async fn retire_group(
        &mut self,
        group: GroupId,
        slot: super::groups::Group<D>,
        op: Uuid,
    ) -> Result<(), ServerError> {
        use crate::shared::traits::TableNameSupport as _;
        let table = slot.table;
        let tablets = slot.spec.tablets.clone();
        let volatile = slot.store.is_volatile();
        event!(Level::WARN, msg = "retiring this shard's copy of a moved group", group = %group, %op, tablets = tablets.len(), volatile);
        if let Some(raft) = slot.raft {
            glommio::spawn_local(async move {
                let _ = raft.shutdown().await;
            })
            .detach();
        }
        // the resident copies go now; the archived ones after the grace
        self.tables.evict_tablets(table, &tablets);
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        // the log is dead history: forgotten, so a copy of the group added here later starts
        // with none, and never handed to a compactor again
        if volatile {
            replication.volatile.forget(group);
        } else if let Err(error) = replication.wal.forget(group) {
            event!(Level::ERROR, msg = "a retired group's log could not be forgotten", group = %group, %error);
        }
        for batches in replication.parked.values_mut() {
            batches.retain(|batch| batch.group != group);
        }
        replication.parked.retain(|_, batches| !batches.is_empty());
        let copy = RetiredCopy {
            table: table.table_id(),
            tablets,
            op,
            at: Instant::now(),
            volatile,
            reclaiming: false,
        };
        let wal_dir = replication.wal_dir.clone();
        if let Err(error) = write_retired(&wal_dir, group, &copy).await {
            event!(Level::ERROR, msg = "a retired copy's marker could not be written", group = %group, %error);
        }
        if let Some(replication) = self.replication.as_mut() {
            replication.retired.insert(group, copy);
            // the checkpoint no longer names the group
            replication.checkpoint_dirty = true;
        }
        self.write_checkpoint();
        Ok(())
    }

    /// Start reclaiming every retired copy whose grace is over
    ///
    /// A durable copy's archived partitions are dropped by its table's compactor, which the
    /// loop hears back from; a volatile copy has nothing on disk and is finished at once.
    pub(super) async fn sweep_retired(&mut self) -> Result<(), ServerError> {
        let grace = self
            .conf
            .cluster
            .as_ref()
            .map_or(Duration::from_secs(300), |cluster| {
                cluster.migration.retire_after.duration()
            });
        let sinks: std::collections::HashMap<
            crate::shared::identity::TableId,
            kanal::AsyncSender<crate::storage::CompactionJob>,
        > = {
            use crate::shared::traits::TableNameSupport as _;
            self.tables
                .compaction_sinks()
                .into_iter()
                .map(|(table, sink)| (table.table_id(), sink))
                .collect()
        };
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        let mut finished = Vec::new();
        for (group, copy) in replication.retired.iter_mut() {
            if copy.reclaiming || copy.at.elapsed() < grace {
                continue;
            }
            copy.reclaiming = true;
            if copy.volatile {
                finished.push(*group);
                continue;
            }
            match sinks.get(&copy.table) {
                Some(sink) => {
                    event!(Level::INFO, msg = "a retired copy's grace is over; dropping its archived partitions", group = %group);
                    let job = crate::storage::CompactionJob::Drop {
                        group: *group,
                        tablets: copy.tablets.clone(),
                    };
                    if sink.try_send(job).is_err() {
                        event!(Level::WARN, msg = "a retired copy's compactor is not taking jobs; trying again next sweep", group = %group);
                        copy.reclaiming = false;
                    }
                }
                // a table this schema does not have holds nothing here
                None => finished.push(*group),
            }
        }
        for group in finished {
            self.finish_retirement(group).await?;
        }
        Ok(())
    }

    /// Note that a retired copy's archived partitions are gone, or not
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `outcome` - Whether the drop landed
    pub(super) async fn handle_tablets_dropped(
        &mut self,
        group: GroupId,
        outcome: Result<u64, String>,
    ) -> Result<(), ServerError> {
        match outcome {
            Ok(removed) => {
                event!(Level::INFO, msg = "a retired copy's archived partitions are gone", group = %group, removed);
                self.finish_retirement(group).await
            }
            Err(error) => {
                event!(Level::ERROR, msg = "a retired copy's partitions could not be dropped; trying again next sweep", group = %group, error);
                if let Some(copy) = self
                    .replication
                    .as_mut()
                    .and_then(|replication| replication.retired.get_mut(&group))
                {
                    copy.reclaiming = false;
                }
                Ok(())
            }
        }
    }

    /// Finish a retirement: the group's snapshot, install and quarantine files and its marker
    /// go, the copy is forgotten, and the groups are rebuilt in case the map names it again
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    async fn finish_retirement(&mut self, group: GroupId) -> Result<(), ServerError> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        let wal_dir = replication.wal_dir.clone();
        // every file the group left: its cut snapshots, a partial or pending install, a
        // quarantine marker
        let snapshots = wal_dir.join(crate::server::replication::snapshot::SNAPSHOTS_DIR);
        if let Ok(entries) = std::fs::read_dir(&snapshots) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&format!("{group}-"))
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        let _ = std::fs::remove_file(replication.installs.part_path(group));
        let _ = std::fs::remove_file(replication.installs.marker_path(group));
        replication.installs.retire(group);
        replication.pending_installs.remove(&group);
        replication.quarantines.remove(&group);
        if let Err(error) = super::repair::clear_quarantine(&wal_dir, group).await {
            event!(Level::WARN, msg = "a retired copy's quarantine marker could not be removed", group = %group, %error);
        }
        if let Err(error) = clear_retired(&wal_dir, group).await {
            event!(Level::ERROR, msg = "a retired copy's marker could not be removed; it is retired again at the next start", group = %group, %error);
        }
        if let Some(replication) = self.replication.as_mut() {
            replication.retired.remove(&group);
            replication.checkpoint_dirty = true;
            replication.sweep_due = true;
        }
        event!(Level::INFO, msg = "a retired copy is reclaimed", group = %group);
        self.write_checkpoint();
        // the map may have brought the group back here meanwhile
        self.rebuild_groups().await
    }

    /// Refuse a query of a tablet no group on this shard serves, by name
    ///
    /// A query a peer forwarded is refused on a frame of its own, so the origin can send it
    /// to another holder under the same attempt and slot; a client's is answered in the
    /// query's own variant, for its retry budget to cover
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `meta` - The query's metadata
    /// * `query` - The query
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share
    /// * `tablet` - The tablet nobody here serves
    pub(super) async fn answer_stale(
        &mut self,
        mut meta: crate::server::messages::QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: tracing::Span,
        gathered_meta: Option<crate::server::messages::QueryMetadata>,
        tablet: u16,
    ) -> Result<(), ServerError> {
        use crate::shared::protocol::error::ErrorCode;
        self.read_stats.stale_served += 1;
        let version = self.map.get().version;
        let table = D::ClientType::query_table_name(&query);
        event!(Level::INFO, msg = "refusing a query of a tablet no group here serves", tablet, %table, version, from_peer = meta.from_peer);
        let msg = format!(
            "tablet {tablet} of {table} is not served on this node at map version {version}: its copy retired here or was never here"
        );
        if meta.from_peer {
            let payload = crate::shared::protocol::peer::encode_error_payload(
                ErrorCode::StaleTopology.as_u16(),
                &msg,
            );
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(&payload);
            // a share answers under the gather's client, index and route; a whole answer under its own
            let (client, id, index, end, route) = match &gathered_meta {
                Some(gathered) => (
                    gathered.client,
                    gathered.id,
                    gathered.index,
                    gathered.end,
                    (gathered.read.attempt, gathered.read.slot),
                ),
                None => (
                    meta.client,
                    meta.id,
                    meta.index,
                    meta.end,
                    (meta.read.attempt, 0),
                ),
            };
            meta.stamps.mark_exec_done();
            meta.stamps.mark_replied();
            return self
                .reply_sealed(
                    client,
                    id,
                    index,
                    end,
                    crate::server::messages::ReplyKind::Stale,
                    span,
                    meta.stamps,
                    aligned,
                    None,
                    route,
                )
                .await;
        }
        let error = crate::shared::responses::ResponseError::new(ErrorCode::StaleTopology, msg);
        self.answer_read_failure(meta, query, span, gathered_meta, error)
            .await
    }

    /// Whether a group on this shard serves a table's tablet
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `tablet` - The tablet
    pub(super) fn serves_tablet(&self, table: D::TableNames, tablet: u16) -> bool {
        use crate::shared::traits::TableNameSupport as _;
        self.replication.as_ref().is_some_and(|replication| {
            replication
                .tablets
                .contains_key(&(table.table_id(), tablet))
        })
    }

    /// The tablet of a query no group on this shard serves, if there is one
    ///
    /// A query routed here by a map older than the configuration its tablet now lives under:
    /// the copy retired, or was never here. Refused by name rather than answered from files
    /// the cluster no longer counts ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    pub(super) fn stale_tablet(
        &self,
        query: &<D::ClientType as QuerySupport>::QueryKinds,
    ) -> Option<u16> {
        use crate::shared::traits::{ShoalQuerySupport as _, TableNameSupport as _};
        let replication = self.replication.as_ref()?;
        let table = D::ClientType::query_table_name(query).table_id();
        for key in query.partition_keys() {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            if !replication.tablets.contains_key(&(table, tablet)) {
                return Some(tablet);
            }
        }
        None
    }
}
