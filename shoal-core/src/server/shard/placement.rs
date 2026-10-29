//! A placement group's state, read from its tablet group for an object task
//!
//! A writer stages a stripe on the holders its placement group's state names and claims the
//! group's epoch in its commit; a put checks the state again once its pieces are durable. Both read
//! it here: through this shard's own copy of the group if it leads, else from the group's leader
//! over the replication lane, as a linearizable read that waits for the leader's apply through its
//! read index ([F88](../../../../docs/src/features/recovery-and-moves.md)).
//!
//! # Invariants
//!
//! **A placement read is linearizable.** A put that wrote no row is safe across a move only if the
//! read it makes after its pieces are durable sees every move begun before it; a read served from a
//! copy that is behind would miss one, and the move's arrival would never get the stripe.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use openraft::error::{LinearizableReadError, RaftError};
use openraft::{Raft, ReadPolicy};
use openraft_rt::WatchReceiver as _;

use super::Shard;
use crate::server::database::ShoalDatabase;
use crate::server::replication::{
    ApplyOutcome, DataConfig, GroupMachine, MachineState, PlacementAnswer, RpcFailure,
    ShardNetwork, ShardPeer,
};
use crate::shared::placement_state::{Missed, PlacementCommand, StripeKey};
use crate::shared::protocol::peer::{Command, RequestId};
use crate::shared::identity::{GroupId, ShardAddr, TableId};
use crate::shared::placement_state::{PgKey, PgView};
use crate::shared::traits::{QuerySupport, TableNameSupport};

/// How long a placement read waits for a group's leader before it gives up
const PLACEMENT_READ_DEADLINE: Duration = Duration::from_secs(10);

/// How long a placement read pauses before asking again, while a group elects
const PLACEMENT_READ_POLL: Duration = Duration::from_millis(20);

/// What a placement read answers: the index it was read at, and each group's state
pub(crate) type PlacementRead = (u64, Vec<(PgKey, PgView)>);

/// This shard's copy of a group: its handle, its state and its address
type LocalCopy<D> = (
    Raft<DataConfig, GroupMachine<D>>,
    Rc<RefCell<MachineState>>,
    ShardAddr,
);

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Read some placement groups' state for an object task, on a task of its own
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `tablet` - The tablet whose group holds the placement groups
    /// * `keys` - The placement groups
    /// * `reply` - Where the answer goes
    pub(super) fn handle_placement_read(
        &mut self,
        table: TableId,
        tablet: u16,
        keys: Vec<PgKey>,
        reply: oneshot::Sender<Result<PlacementRead, String>>,
    ) {
        let node = self.node_id();
        // a standalone node's groups never move: every one is at its pool's pin
        let Some(replication) = self.replication.as_ref() else {
            let base = keys.into_iter().map(|key| (key, PgView::default())).collect();
            let _ = reply.send(Ok((0, base)));
            return;
        };
        // this shard's copy of the group, if it holds one that has started
        let local = replication
            .tablets
            .get(&(table, tablet))
            .and_then(|group| replication.groups.get(group))
            .and_then(|slot| {
                slot.raft
                    .clone()
                    .map(|raft| (slot.spec.id, (raft, slot.state.clone(), slot.spec.me(node))))
            });
        // the group from the map, with the member most likely to lead it
        let map = self.map.get();
        let (rule, _) = map.rule_set_of(usize::from(tablet));
        let group = local
            .as_ref()
            .map_or_else(|| GroupId::of(table, &rule), |(group, _)| *group);
        let members = map.replicas_of(usize::from(tablet));
        let weight = |node: crate::shared::identity::NodeId| {
            map.members
                .get(&node)
                .map_or(1, |member| member.lead_weight.max(1))
        };
        let first = crate::shared::placement::preferred_leader(group, &members, weight, |node| {
            map.is_up(node)
        })
        .or_else(|| members.first().copied());
        let network = replication.network.clone();
        glommio::spawn_local(async move {
            let local = local.map(|(_, copy)| copy);
            let answer = placement_read::<D>(local, network, group, first, keys).await;
            let _ = reply.send(answer);
        })
        .detach();
    }
}

/// What the missed record of a stripe group this shard leads asks to be done (F88)
#[derive(Debug, Clone)]
pub enum RecoveryWork {
    /// A stripe a position is stale on, made current by a rebuild
    Rebuild {
        /// The stripe's placement group
        key: PgKey,
        /// The position
        position: usize,
        /// The stripe, by its object and index
        stripe: StripeKey,
        /// What the record says the position missed
        missed: Missed,
    },
    /// A position whose record dropped stripes past its bound, made whole by a walk of its group
    Backfill {
        /// The bucket's stripe table
        table: TableId,
        /// The placement group
        key: PgKey,
        /// The position
        position: usize,
        /// The drops its record had counted when the work was read
        drops: u64,
    },
}

/// A stripe group this shard leads, as its move driver reads it (F88)
#[derive(Debug, Clone)]
pub struct LedStripes {
    /// The bucket's stripe table
    pub table: TableId,
    /// The tablets the group serves
    pub tablets: Vec<u16>,
    /// Every placement group of it not at its base, by group
    pub moved: Vec<(PgKey, PgView)>,
}

/// A stripe group this shard leads, as its scrub driver reads it (F89)
#[derive(Debug, Clone)]
pub struct ScrubGroup {
    /// The group, whose identity staggers its first passes
    pub group: GroupId,
    /// The bucket's stripe table
    pub table: TableId,
    /// The tablets the group serves
    pub tablets: Vec<u16>,
    /// Where each kind of scrub is in the group's passes, as committed
    pub scrub: crate::shared::scrub::ScrubState,
    /// The placement groups of it that are moving, which a pass waits for
    pub moving: Vec<PgKey>,
}

/// A bucket's object table and the tablets of it this shard leads or hosts, whose entries the
/// reclamation driver walks (M20)
#[derive(Debug, Clone)]
pub struct ReclaimTarget {
    /// The bucket's object table
    pub table: TableId,
    /// The bucket, by name
    pub bucket: String,
    /// The tablets
    pub tablets: Vec<u16>,
}

/// The most rebuilds one read of a shard's recovery work names
const MAX_RECOVERY_WORK: usize = 256;

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// The work the missed record of every stripe group this shard leads holds
    ///
    /// Only the leader's: progress is the record itself, on every copy, so a new leader resumes
    /// where the old one stopped. A placement group that is moving is left alone until its switch
    /// or its abort, since its positions are in two generations at once.
    pub(super) fn recovery_work(&self) -> Vec<RecoveryWork> {
        let node = self.node_id();
        let mut work = Vec::new();
        let Some(replication) = self.replication.as_ref() else {
            return work;
        };
        for slot in replication.groups.values() {
            // a bucket's stripe group this shard leads
            if slot.table.bucket().is_none() {
                continue;
            }
            let Some(raft) = slot.raft.as_ref() else {
                continue;
            };
            let me = slot.spec.me(node);
            if raft.metrics().borrow_watched().current_leader != Some(me) {
                continue;
            }
            let state = slot.state.borrow();
            for (key, group) in &state.placement.groups {
                if group.moving.is_some() {
                    continue;
                }
                for (position, part) in group.record.iter().enumerate() {
                    if part.overflowed {
                        work.push(RecoveryWork::Backfill {
                            table: slot.spec.table,
                            key: *key,
                            position,
                            drops: part.drops,
                        });
                    }
                    for (stripe, missed) in &part.stripes {
                        if work.len() >= MAX_RECOVERY_WORK {
                            return work;
                        }
                        work.push(RecoveryWork::Rebuild {
                            key: *key,
                            position,
                            stripe: *stripe,
                            missed: missed.clone(),
                        });
                    }
                }
            }
        }
        work
    }

    /// Every bucket stripe group this shard leads, with its scrub cursors and the placement groups
    /// of it that are moving, for the scrub driver (F89)
    pub(super) fn scrub_groups(&self) -> Vec<ScrubGroup> {
        let node = self.node_id();
        let Some(replication) = self.replication.as_ref() else {
            return Vec::new();
        };
        replication
            .groups
            .values()
            .filter(|slot| slot.table.bucket().is_some())
            .filter(|slot| {
                slot.raft.as_ref().is_some_and(|raft| {
                    raft.metrics().borrow_watched().current_leader == Some(slot.spec.me(node))
                })
            })
            .map(|slot| {
                let state = slot.state.borrow();
                ScrubGroup {
                    group: slot.spec.id,
                    table: slot.spec.table,
                    tablets: slot.spec.tablets.clone(),
                    scrub: state.placement.scrub,
                    moving: state
                        .placement
                        .groups
                        .iter()
                        .filter(|(_, group)| group.moving.is_some())
                        .map(|(key, _)| *key)
                        .collect(),
                }
            })
            .collect()
    }

    /// Every bucket object table's tablets this shard leads on a cluster, or hosts on a standalone
    /// node, for the reclamation driver (M20)
    ///
    /// Only the leader's: a reclamation's commits are conditional, so a second driver is only
    /// wasted work, and the leader is the one copy every commit passes through.
    pub(super) fn reclaim_targets(&self) -> Vec<ReclaimTarget> {
        let installed = self.map.get();
        // every bound bucket's object table, by its identity
        let mut tables: Vec<(TableId, String)> = Vec::new();
        for name in installed.objects.bindings.keys() {
            let Some(ops) = <D::ClientType as QuerySupport>::object_queries(crate::shared::identity::BucketId::of(name)) else {
                continue;
            };
            tables.push((TableId::of(ops.object_table()), name.clone()));
        }
        let node = self.node_id();
        match self.replication.as_ref() {
            // a cluster: the object groups this shard leads
            Some(replication) => replication
                .groups
                .values()
                .filter_map(|slot| {
                    let (_, bucket) = tables.iter().find(|(table, _)| *table == slot.spec.table)?;
                    let led = slot.raft.as_ref().is_some_and(|raft| {
                        raft.metrics().borrow_watched().current_leader == Some(slot.spec.me(node))
                    });
                    led.then(|| ReclaimTarget {
                        table: slot.spec.table,
                        bucket: bucket.clone(),
                        tablets: slot.spec.tablets.clone(),
                    })
                })
                .collect(),
            // a standalone node: the tablets this shard hosts
            None => {
                let tablets: Vec<u16> = (0..crate::shared::placement::TABLET_COUNT)
                    .filter(|tablet| self.hosting.owner_of_tablet(*tablet) == self.shard_id)
                    .filter_map(|tablet| u16::try_from(tablet).ok())
                    .collect();
                tables
                    .into_iter()
                    .map(|(table, bucket)| ReclaimTarget {
                        table,
                        bucket,
                        tablets: tablets.clone(),
                    })
                    .collect()
            }
        }
    }

    /// Every bucket stripe group this shard leads: its table, its tablets, and every placement
    /// group of it that moved or is moving, for the move driver
    pub(super) fn led_stripes(&self) -> Vec<LedStripes> {
        let node = self.node_id();
        let Some(replication) = self.replication.as_ref() else {
            return Vec::new();
        };
        replication
            .groups
            .values()
            .filter(|slot| slot.table.bucket().is_some())
            .filter(|slot| {
                slot.raft.as_ref().is_some_and(|raft| {
                    raft.metrics().borrow_watched().current_leader == Some(slot.spec.me(node))
                })
            })
            .map(|slot| {
                let state = slot.state.borrow();
                LedStripes {
                    table: slot.spec.table,
                    tablets: slot.spec.tablets.clone(),
                    moved: state
                        .placement
                        .groups
                        .iter()
                        // any group a move began, switched or gave up, whose epoch moved off zero
                        .filter(|(_, group)| {
                            group.epoch > 0 || group.generation.is_some() || group.moving.is_some()
                        })
                        .map(|(key, group)| (*key, group.view()))
                        .collect(),
                }
            })
            .collect()
    }

    /// Propose a placement command through the group that holds its placement group, on a task
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `command` - The command
    /// * `reply` - Where the outcome goes
    pub(super) fn propose_placement(
        &mut self,
        table: TableId,
        command: PlacementCommand,
        reply: oneshot::Sender<Result<(), String>>,
    ) {
        let tablet = command.key().1.tablet;
        let Some(replication) = self.replication.as_ref() else {
            let _ = reply.send(Err("this node hosts no tablet groups".to_string()));
            return;
        };
        let raft = replication
            .tablets
            .get(&(table, tablet))
            .and_then(|group| replication.groups.get(group))
            .and_then(|slot| slot.raft.clone());
        let Some(raft) = raft else {
            let _ = reply.send(Err(format!("no copy of tablet {tablet}'s group is up on this shard")));
            return;
        };
        glommio::spawn_local(async move {
            let request = RequestId {
                bundle: uuid::Uuid::now_v7().into_bytes(),
                index: 0,
            };
            let outcome = match raft.client_write(Command::placement(table, request, &command)).await {
                Ok(response) => match response.data {
                    ApplyOutcome::Applied(result) if result.ok => Ok(()),
                    other => Err(format!("{command:?} was not applied: {other:?}")),
                },
                Err(error) => Err(format!("proposing {command:?}: {error}")),
            };
            let _ = reply.send(outcome);
        })
        .detach();
    }
}

/// Read some placement groups' state from their tablet group's leader
///
/// # Arguments
///
/// * `local` - This shard's copy of the group, if it holds one
/// * `network` - The shard's network, for a leader elsewhere
/// * `group` - The group
/// * `first` - The member to ask first when this shard holds no copy
/// * `keys` - The placement groups
async fn placement_read<D: ShoalDatabase>(
    local: Option<LocalCopy<D>>,
    network: ShardNetwork,
    group: GroupId,
    first: Option<ShardAddr>,
    keys: Vec<PgKey>,
) -> Result<PlacementRead, String> {
    let started = Instant::now();
    // a leader another member named, asked directly on the next turn
    let mut redirect: Option<ShardAddr> = None;
    loop {
        if started.elapsed() > PLACEMENT_READ_DEADLINE {
            return Err(format!(
                "no leader of group {group} answered a placement read within {PLACEMENT_READ_DEADLINE:?}"
            ));
        }
        // this shard's own copy first, unless another member was named
        let target = match (redirect.take(), &local) {
            (Some(leader), _) => Some(leader),
            (None, Some((raft, state, me))) => match raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
                // it leads: its own apply through the read's index, then what it holds
                Ok(linearizer) => {
                    let ready = linearizer
                        .await_ready(raft)
                        .await
                        .map_err(|fatal| format!("group {group} stopped: {fatal}"))?;
                    let placed = state.borrow();
                    return Ok((
                        ready.read_log_id().index(),
                        keys.iter().map(|key| (*key, placed.placement.view(key))).collect(),
                    ));
                }
                Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                    forward.leader_node.or(forward.leader_id).filter(|leader| leader != me)
                }
                Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(short))) => {
                    return Err(format!("group {group} could not confirm its leader: {short}"));
                }
                Err(RaftError::Fatal(fatal)) => return Err(format!("group {group} stopped: {fatal}")),
            },
            (None, None) => first,
        };
        // nobody named yet: the group is electing, so ask again shortly
        let Some(leader) = target else {
            glommio::timer::sleep(PLACEMENT_READ_POLL).await;
            continue;
        };
        let left = PLACEMENT_READ_DEADLINE.saturating_sub(started.elapsed());
        let peer = ShardPeer::new(leader, network.clone());
        match peer.placement(group, &keys, left).await {
            Ok(bytes) => match postcard::from_bytes::<PlacementAnswer>(&bytes) {
                Ok(PlacementAnswer::Ready { index, groups }) => return Ok((index, groups)),
                // the member asked does not lead and names who does
                Ok(PlacementAnswer::NotLeader(Some(next))) if next != leader => {
                    redirect = Some(next);
                    glommio::timer::sleep(PLACEMENT_READ_POLL).await;
                }
                // it names nobody, or itself: ask again shortly
                Ok(PlacementAnswer::NotLeader(_)) => {
                    if local.is_none() {
                        redirect = first;
                    }
                    glommio::timer::sleep(PLACEMENT_READ_POLL).await;
                }
                Ok(PlacementAnswer::NoQuorum(msg)) => {
                    return Err(format!("the leader of group {group} could not confirm its term: {msg}"))
                }
                Err(error) => {
                    return Err(format!("decoding group {group}'s placement answer: {error}"))
                }
            },
            // a link that is down may come back: a pause and another try, which the deadline ends
            Err(RpcFailure::NotSent(_)) => glommio::timer::sleep(PLACEMENT_READ_POLL).await,
            Err(RpcFailure::Unreachable(msg) | RpcFailure::Remote(msg)) => {
                return Err(format!("the leader of group {group} did not answer a placement read: {msg}"))
            }
        }
    }
}
