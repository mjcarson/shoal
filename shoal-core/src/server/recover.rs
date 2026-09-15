//! Recovering a stopped survivor after a permanent majority loss
//!
//! A cluster whose control voters, or whose data groups' voters, are permanently gone does
//! not recover by itself: the control group has no quorum to commit anything, every write
//! through a survivor is refused or unknown, and no empty bootstrap and no automatic choice
//! ever happens ([C9](../../../docs/src/distributed/operations.md), Q12). What exists is
//! this: an operator stops a survivor and runs [`force_recover`] on its directory, which
//! rewrites its membership to itself alone - in the control log and in every tablet group's
//! log - tombstones every lost member, and records what it did, so the survivor starts as a
//! cluster of one that leads, commits and serves, and every key it had acknowledged before the
//! loss reads back. Fresh identities then join it and a `Rebalance` rebuilds every set
//! ([F49](../../../docs/src/features/backup-and-recovery.md)).
//!
//! One survivor, on purpose. A recovery keeps the node it runs on and nothing else: two
//! survivors each recovered to themselves would be two clusters, and one recovered to both
//! would need the other's log to agree at a term neither leads, so the choice of which log
//! is the history is the operator's and made by naming one node. A lost member started again
//! from its directory is refused at every door, since its identity is tombstoned; a clone of
//! it is still it.
//!
//! Every step is idempotent by inspection, so a run interrupted anywhere is run again whole:
//! the control log whose last entry is already this recovery is left alone, and a group whose
//! membership is already the survivor alone is left alone.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use glommio::{LocalExecutorBuilder, Placement};
use openraft::entry::RaftEntry as _;
use openraft::storage::{RaftLogReader as _, RaftLogStorage as _, RaftLogStorageExt as _};
use openraft::vote::{RaftLeaderId as _, RaftVote as _};
use openraft::{EntryPayload, Membership};
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};

use super::conf::Conf;
use super::control::store::{self, CONTROL_DIR};
use super::control::types::{ControlCommand, MemberPhase, MemberRecord};
use super::errors::ShoalError;
use super::meta::{DirectoryLock, MarkerMode, StorageMeta};
use super::rehome::shard_name;
use super::replication::DataConfig;
use super::wal::{Checkpoint, ShardWal, WAL_DIR};
use super::ServerError;
use crate::shared::identity::{GroupId, NodeId, ShardAddr};

/// What a recovery did
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// The node recovered, which is the whole membership now
    pub survivor: NodeId,
    /// The members lost, tombstoned by the recovery
    pub lost: Vec<NodeId>,
    /// The control log index the survivor had committed when it was recovered
    pub last_committed: u64,
    /// The control log index the recovery was written at
    pub recovered_at: u64,
    /// The tablet groups whose membership was rewritten to the survivor alone, by shard
    pub groups_rewritten: Vec<(u16, GroupId)>,
    /// The tablet groups that already named the survivor alone, by shard
    pub groups_kept: Vec<(u16, GroupId)>,
}

/// Rewrite a stopped survivor's directory so it starts as a cluster of itself
///
/// Takes the directory's lock, so a running server on it is refused, and refuses by name a
/// directory that is not a cluster member's, a survivor list that is not this node alone, a
/// node that is not a committed member, and a cluster whose only member is already this node.
/// Runs on an executor of its own, since the stores are glommio's.
///
/// # Arguments
///
/// * `conf` - The Shoal config, which names the directory
/// * `survivors` - The members to keep: this node, and nothing else
///
/// # Errors
///
/// Fails naming what was wrong; a directory it refuses is not touched.
#[instrument(name = "recover::force_recover", skip_all, err(Debug))]
pub fn force_recover(conf: &Conf, survivors: &[NodeId]) -> Result<RecoveryReport, ServerError> {
    let root = conf
        .storage
        .default
        .filesystem
        .latency_sensitive
        .path
        .clone();
    // the lock, so nothing serves the directory while its logs are rewritten
    let _lock = DirectoryLock::acquire(&root)?;
    // the marker: a cluster member's, and the node it names
    let marker = StorageMeta::read(&root)?.ok_or_else(|| {
        ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} holds no storage marker to recover",
            root.display()
        )))
    })?;
    if marker.mode != MarkerMode::Cluster || marker.cluster.is_none() {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} is not a cluster member's directory; a recovery rewrites a member's membership",
            root.display()
        ))));
    }
    let me = marker.node;
    // one survivor, and it has to be this node
    if survivors != [me] {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "a recovery keeps the node it runs on and nothing else: this is {me} and the survivors named are {survivors:?}"
        ))));
    }
    let segment_bytes = conf.cluster.as_ref().map_or(10 * 1024 * 1024, |cluster| {
        cluster.replication.segment_bytes
    });
    let executors = marker.physical.unwrap_or(marker.shards);
    let executor = LocalExecutorBuilder::new(Placement::Unbound)
        .name("shoal-recover")
        .make()?;
    let report =
        executor.run(async move { recover_steps(&root, me, segment_bytes, executors).await })?;
    event!(
        Level::WARN,
        msg = "recovered a survivor as a cluster of one",
        survivor = %report.survivor,
        lost = ?report.lost,
        last_committed = report.last_committed,
        recovered_at = report.recovered_at,
        groups_rewritten = report.groups_rewritten.len(),
        groups_kept = report.groups_kept.len(),
    );
    Ok(report)
}

/// The steps: the control log, then every shard's groups
///
/// # Arguments
///
/// * `root` - The storage directory
/// * `me` - The survivor
/// * `segment_bytes` - The WAL segment size, to open the WALs with
/// * `executors` - How many executors the files are laid out on
async fn recover_steps(
    root: &Path,
    me: NodeId,
    segment_bytes: u64,
    executors: usize,
) -> Result<RecoveryReport, ServerError> {
    // the control store, and what it has committed
    let (mut log, machine) = store::open(&root.join(CONTROL_DIR))
        .await
        .map_err(ServerError::IO)?;
    let state = machine.state();
    let Some(mine) = state.members.get(&me) else {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{me} is not a committed member of its own cluster, so there is nothing to recover it as"
        ))));
    };
    let lost: Vec<NodeId> = state
        .members
        .values()
        .filter(|member| member.record.node != me && member.phase != MemberPhase::Removed)
        .map(|member| member.record.node)
        .collect();
    if lost.is_empty() {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{me} is already the only member of its cluster; there is nothing to recover from"
        ))));
    }
    let last_committed = machine.applied_index();
    // the membership: this node alone, with its committed record
    let mut nodes: BTreeMap<NodeId, MemberRecord> = BTreeMap::new();
    nodes.insert(me, mine.record.clone());
    let membership = Membership::new(vec![BTreeSet::from([me])], nodes).map_err(|error| {
        ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "a membership of one: {error}"
        )))
    })?;
    let command = ControlCommand::ForceRecovered {
        op: uuid::Uuid::new_v4(),
        survivors: vec![me],
        lost: lost.clone(),
        at: me,
        last_committed,
        recovered_ms: now_ms(),
    };
    let recovered_at = store::force_recover(&mut log, &machine, me, membership, command)
        .await
        .map_err(ServerError::IO)?;
    // then every tablet group on every executor whose members include a lost node
    let mut rewritten = Vec::new();
    let mut kept = Vec::new();
    for executor in 0..executors {
        // an executor count is small; the shard name is a u16
        #[allow(clippy::cast_possible_truncation)]
        let executor = executor as u16;
        let dir = root.join(WAL_DIR).join(shard_name(executor));
        if !dir.exists() {
            continue;
        }
        let wal = ShardWal::open(&dir, segment_bytes, 1 << 20)
            .await
            .map_err(ServerError::IO)?;
        let checkpoint = Checkpoint::read(&dir).await.map_err(ServerError::IO)?;
        let mut groups: BTreeSet<GroupId> = wal.groups().into_iter().collect();
        groups.extend(
            checkpoint
                .groups
                .keys()
                .filter_map(|hex| u64::from_str_radix(hex, 16).ok().map(GroupId)),
        );
        for group in groups {
            let mut store = wal.store(group);
            // the membership as the log last saw it: the newest membership entry, else the checkpoint's
            let entries = store
                .try_get_log_entries(0..)
                .await
                .map_err(ServerError::IO)?;
            let last_membership = entries
                .iter()
                .rev()
                .find_map(|entry| match &entry.payload {
                    EntryPayload::Membership(membership) => Some(membership.clone()),
                    _ => None,
                })
                .or_else(|| {
                    checkpoint
                        .get(group)
                        .map(|point| point.membership().membership().clone())
                });
            let Some(current) = last_membership else {
                continue;
            };
            // this node's address in the group, from the membership itself
            let Some(mine) = current
                .nodes()
                .map(|(addr, _)| *addr)
                .find(|addr| addr.node == me)
            else {
                continue;
            };
            let members: BTreeSet<ShardAddr> = current.nodes().map(|(addr, _)| *addr).collect();
            if members.len() == 1 && members.contains(&mine) {
                kept.push((executor, group));
                continue;
            }
            // a term past every term the group's log and vote have seen, led by this shard
            let last = entries.last().map(|entry| entry.log_id());
            let last_index = last.as_ref().map_or_else(
                || {
                    checkpoint
                        .get(group)
                        .and_then(|point| point.applied.as_ref())
                        .map_or(0, |applied| applied.index)
                },
                |log_id| log_id.index,
            );
            let last_term = last.as_ref().map_or(0, |log_id| log_id.leader_id.term);
            let vote_term = wal.vote_of(group).map_or(0, |vote| vote.leader_id().term);
            let term = last_term.max(vote_term) + 1;
            let leader = crate::server::wal::LeaderId::new(term, mine);
            let log_id = openraft::LogId::new(leader.clone(), last_index + 1);
            let alone = Membership::<ShardAddr, ShardAddr>::new(
                vec![BTreeSet::from([mine])],
                BTreeMap::from([(mine, mine)]),
            )
            .map_err(|error| {
                ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "a group membership of one: {error}"
                )))
            })?;
            let entry = openraft::type_config::alias::EntryOf::<DataConfig>::new_membership(
                log_id.clone(),
                alone,
            );
            store.blocking_append(vec![entry]).await.map_err(|error| {
                ServerError::GlommioGeneric(format!("appending group {group}'s recovery: {error}"))
            })?;
            store
                .save_vote(&crate::server::wal::Vote::from_leader_id(leader, true))
                .await
                .map_err(ServerError::IO)?;
            store
                .save_committed(Some(log_id))
                .await
                .map_err(ServerError::IO)?;
            rewritten.push((executor, group));
        }
        wal.flush().await.map_err(ServerError::IO)?;
        wal.close().await.map_err(ServerError::IO)?;
    }
    Ok(RecoveryReport {
        survivor: me,
        lost,
        last_committed,
        recovered_at,
        groups_rewritten: rewritten,
        groups_kept: kept,
    })
}

/// Milliseconds since the epoch, or zero on a clock before it
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
