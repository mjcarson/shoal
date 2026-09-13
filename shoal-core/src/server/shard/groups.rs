//! The tablet groups a shard hosts: building them, proposing through them, applying for them
//!
//! The replication half of a shard ([F40](../../../../docs/src/features/replication.md)). A
//! cluster node's shard computes from every map it installs which groups it is a member of,
//! builds a `Raft` per group on a task of its own, proposes every write it is routed through
//! the group serving the write's tablet, applies every committed batch the group's state machine
//! hands the loop, and resolves the shard's WAL segments as the groups apply past them. A
//! standalone node never enters this file: it has no WAL, no groups and no replication lane.
//!
//! # Invariants
//!
//! **The loop never awaits a `Raft` method.** `Raft::new` re-applies the checkpoint up to the
//! committed log id on the calling task, through `apply`, which posts to this loop and waits;
//! `client_write` waits for a commit that needs the loop to apply. Both are spawned, and the
//! loop hears their outcome as a message. Every `Raft` method call in this file is inside a
//! `spawn_local`.
//!
//! **An apply batch is applied in order, and parks whole.** A command that needs a partition
//! read from disk stops the batch where it stands; the read lands, the batch resumes at that
//! command, and nothing after it is applied first. Committed order is what makes every replica
//! derive the same result.
//!
//! **A segment is handed to the compactors in generation order, once, when every group's frames
//! in it are applied or gone.** That is [P4](../../../../docs/src/distributed/protocol.md) by
//! construction: nothing an archive holds was uncommitted when it was written.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use openraft::error::{ClientWriteError, LinearizableReadError, RaftError};
use openraft::storage::EntryResponder;
use openraft::{Config, EntryPayload, Raft, ReadPolicy, SnapshotPolicy, StoredMembership};
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level, Span};
use uuid::Uuid;

use super::Shard;
use crate::server::database::ShoalDatabase;
use crate::server::map::GroupSpec;
use crate::server::messages::{QueryMetadata, ServerMsg};
use crate::server::peer::{LinkEvent, ReplicateReply};
use crate::server::replication::snapshot::{self, BuiltSnapshot, SnapshotHeader, SnapshotManifest, SnapshotWriter, SNAPSHOTS_DIR};
use crate::server::replication::{
    ApplyOutcome, BarrierAnswer, DataConfig, GroupMachine, GroupNetwork, GroupReport, IntegrityStats, Lease,
    MachineState, ProposalOutcome, Remembered, ReplicationVerb, RpcFailure, ShardNetwork, ShardPeer, ShardReplication,
    SnapshotStats,
};
use crate::server::ring::Ring;
use crate::server::stage_profile::{StageDurability, StageOp, Stamp};
use crate::server::tables::ApplyStep;
use crate::server::wal::{Checkpoint, GroupCheckpoint, GroupRetries, GroupStore, MemoryWal, Retries, ShardWal};
use crate::server::ServerError;
use crate::shared::identity::{GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::{Command, ReplicateKind, ReplicateRequestHead, RequestId};
use crate::shared::protocol::read::SessionToken;
use crate::shared::responses::ResponseError;
use crate::shared::traits::{QuerySupport, TableNameSupport};
use std::path::PathBuf;
use crate::storage::CompactionJob;

/// How long a proposer waits before asking its own group again while its lease starts
const LEASE_POLL: Duration = Duration::from_millis(20);

/// How many deadline ticks pass between two sweeps of the WAL segments
const REPORT_EVERY_TICKS: u32 = 10;

/// One group this shard hosts
pub(super) struct Group<D: ShoalDatabase> {
    /// What the map says the group is
    pub(super) spec: GroupSpec,
    /// The table it serves, in the schema's naming
    pub(super) table: D::TableNames,
    /// Its handle, once the task that builds it has posted it
    pub(super) raft: Option<Raft<DataConfig, GroupMachine<D>>>,
    /// What the loop and the group's state machine share
    pub(super) state: Rc<RefCell<MachineState>>,
    /// Its log store
    pub(super) store: GroupStore,
    /// Bytes proposed through this shard for it and not yet answered
    pub(super) pending_bytes: usize,
    /// Writes that arrived while the handle was still being built, proposed once it is
    ///
    /// A map install rebuilds a group's handle on a task of its own, and a write in the gap
    /// between the install and the handle is a write to a group that exists and is not up yet;
    /// it waits here rather than being refused, and is refused only if the build fails.
    pub(super) waiting: Vec<(QueryMetadata, D::TableNames, u64, Vec<u8>)>,
    /// The newest snapshot file cut for it, held for transfers
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    pub(super) snapshot: Option<Rc<BuiltSnapshot>>,
    /// Older files a transfer still holds, deleted once it lets go
    pub(super) retired: Vec<Rc<BuiltSnapshot>>,
    /// Whether a cut is in flight
    pub(super) snapshot_building: bool,
    /// Transfers waiting for the cut in flight
    pub(super) snapshot_waiting: Vec<oneshot::Sender<Result<Rc<BuiltSnapshot>, String>>>,
}

/// An apply batch stopped on a partition read
pub(super) struct ParkedApply {
    /// The group the batch belongs to
    group: GroupId,
    /// The batch, the command that parked it at the front
    entries: VecDeque<EntryResponder<DataConfig>>,
    /// Fired once the batch is through
    done: oneshot::Sender<()>,
}

/// What proposals have come to on this shard, for the report
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ProposalStats {
    /// Proposals answered unknown
    pub(super) unknown: u64,
    /// Proposals refused
    pub(super) rejected: u64,
}

/// Everything the replication half of a shard owns
pub(super) struct Replication<D: ShoalDatabase> {
    /// The shard's WAL, which every persistent group's log lives in
    pub(super) wal: ShardWal,
    /// The volatile logs, which every ephemeral group's log lives in
    pub(super) volatile: MemoryWal,
    /// The network the groups' RPCs go over
    pub(super) network: ShardNetwork,
    /// Every group, by identity
    pub(super) groups: BTreeMap<GroupId, Group<D>>,
    /// Which group serves which tablet of which table
    pub(super) tablets: HashMap<(TableId, u16), GroupId>,
    /// Apply batches stopped on a partition read, by the table and partition they wait on
    pub(super) parked: HashMap<(D::TableNames, u64), Vec<ParkedApply>>,
    /// The checkpoint file as it was last written or read
    pub(super) checkpoint: Checkpoint,
    /// The retry sidecar as it was last written or read
    pub(super) retries: Retries,
    /// Which write of the checkpoint file is the latest
    pub(super) checkpoint_version: u64,
    /// Whether a checkpoint write is in flight
    pub(super) checkpoint_writing: bool,
    /// Whether the checkpoint moved while a write was in flight
    pub(super) checkpoint_dirty: bool,
    /// The tables still compacting each handed segment
    pub(super) compacting: HashMap<u64, HashSet<D::TableNames>>,
    /// What proposals have come to
    pub(super) stats: ProposalStats,
    /// What snapshots have done ([F43](../../../../docs/src/features/node-recovery.md))
    pub(super) snapshots: SnapshotStats,
    /// What the loop found of its storage's integrity: the counters the archive maps do not
    /// keep ([F44](../../../../docs/src/features/repair.md))
    pub(super) integrity: IntegrityStats,
    /// The snapshots being received, one partial per group at most
    pub(super) installs: super::snapshots::Installs,
    /// The installs in progress, one per group at most
    pub(super) active_installs: HashMap<GroupId, super::snapshots::ActiveInstall>,
    /// The received snapshots verified at open and not yet installed, by group
    pub(super) pending_installs: HashMap<GroupId, (PathBuf, SnapshotManifest)>,
    /// How many committed write replies to drop before answering clients again, for the fixture
    pub(super) drop_replies: u64,
    /// How many rebuilds of the groups there have been
    pub(super) epoch: u64,
    /// Deadline ticks since the last segment sweep
    pub(super) ticks: u32,
    /// The last report the control thread was sent, so an unchanged one is not sent again
    pub(super) last_report: Option<ShardReplication>,
    /// Whether the groups are being stopped
    pub(super) stopping: bool,
    /// Whether a segment sweep is wanted before the next message
    pub(super) sweep_due: bool,
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// This node's identity, on a cluster node
    pub(super) fn node_id(&self) -> NodeId {
        self.local
            .as_ref()
            .map_or(NodeId::default(), |local| local.borrow().node)
    }

    /// This shard's address
    pub(super) fn my_addr(&self) -> ShardAddr {
        // a node runs fewer shards than a u16 holds; the ring refuses more
        #[allow(clippy::cast_possible_truncation)]
        ShardAddr::new(self.node_id(), self.shard_id as u16)
    }

    /// Open the WAL and build the network, on a cluster node, then the groups the map names
    ///
    /// # Arguments
    ///
    /// * `network` - The replication network the peer listener built
    pub(super) async fn open_replication(&mut self, network: ShardNetwork) -> Result<(), ServerError> {
        let Some(setup) = self.peer_setup.clone() else {
            return Ok(());
        };
        let Some(cluster) = self.conf.cluster.clone() else {
            return Ok(());
        };
        // the WAL under the latency sensitive path, per shard
        let dir = self
            .conf
            .storage
            .default
            .filesystem
            .latency_sensitive
            .path
            .join(crate::server::wal::WAL_DIR)
            .join(&self.info.name);
        let wal = ShardWal::open(
            &dir,
            cluster.replication.segment_bytes,
            cluster.replication.log_cache_bytes,
        )
        .await
        .map_err(ServerError::IO)?;
        // a sealed segment is the loop's to judge, so the writer tells it
        let sealed_tx = self.shard_local_tx.clone_sync();
        wal.on_sealed(Rc::new(move |generation| {
            let _ = sealed_tx.try_send(ServerMsg::WalSealed { generation });
        }));
        let checkpoint = Checkpoint::read(&dir).await.map_err(ServerError::IO)?;
        // the retry tables as of that checkpoint, written before it
        let retries = Retries::read(&dir).await.map_err(ServerError::IO)?;
        // where received snapshots are assembled, bounded in bytes on disk and in the queue
        let installs = super::snapshots::Installs::new(&dir, cluster.replication.install_bytes);
        let _ = setup;
        // a received snapshot whose marker survived a crash is installed again when its group
        // is built, or cleaned up if the checkpoint passed it meanwhile
        // ([F43](../../../../docs/src/features/node-recovery.md))
        let pending_installs = super::snapshots::scan_pending(&installs, &checkpoint).await;
        let _ = setup;
        self.replication = Some(Replication {
            wal,
            volatile: MemoryWal::new(),
            network,
            groups: BTreeMap::new(),
            tablets: HashMap::new(),
            parked: HashMap::new(),
            checkpoint,
            retries,
            checkpoint_version: 0,
            checkpoint_writing: false,
            checkpoint_dirty: false,
            compacting: HashMap::new(),
            stats: ProposalStats::default(),
            snapshots: SnapshotStats::default(),
            integrity: IntegrityStats::default(),
            installs,
            active_installs: HashMap::new(),
            pending_installs,
            drop_replies: 0,
            epoch: 0,
            ticks: 0,
            last_report: None,
            stopping: false,
            sweep_due: false,
        });
        self.rebuild_groups().await?;
        Ok(())
    }

    /// Whether a table stores rows on disk, and so logs through the WAL rather than memory
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    fn is_persistent(table: D::TableNames) -> bool {
        D::persistent_tables()
            .iter()
            .any(|name| TableId::of(name) == table.table_id())
    }

    /// Rebuild the groups from the map this shard holds
    ///
    /// A group the map still names is kept, one it no longer names is stopped, and one it
    /// newly names is built on a task of its own. Called on every installed map and once at
    /// start.
    pub(super) async fn rebuild_groups(&mut self) -> Result<(), ServerError> {
        let me = self.node_id();
        let cluster = self.conf.cluster.clone().unwrap_or_default();
        let shard_id = self.shard_id;
        let placed = self.placed;
        let tx = self.shard_local_tx.clone();
        let map = self.map.get();
        let table_map = &self.table_map;
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        if replication.stopping {
            return Ok(());
        }
        replication.epoch += 1;
        // the groups this shard hosts under the map, if it is placed at all
        let specs: Vec<GroupSpec> = if placed {
            map.replica_groups(me)
                .into_iter()
                .filter(|spec| usize::from(spec.mine) == shard_id)
                .collect()
        } else {
            Vec::new()
        };
        let wanted: HashSet<GroupId> = specs.iter().map(|spec| spec.id).collect();
        // the failover base the cluster agreed, or this node's own until a map carries one
        // truncation cannot happen: a failover base is seconds, not weeks
        #[allow(clippy::cast_possible_truncation)]
        let failover_ms = if map.primary_failover_ms > 0 {
            map.primary_failover_ms
        } else {
            cluster.primary_failover_after.duration().as_millis() as u64
        };
        // stop what the map no longer names
        let gone: Vec<GroupId> = replication
            .groups
            .keys()
            .filter(|id| !wanted.contains(id))
            .copied()
            .collect();
        // a write waiting on a group the map dropped is answered below, once the borrow is done
        let mut orphaned = Vec::new();
        for id in gone {
            if let Some(group) = replication.groups.remove(&id) {
                event!(Level::INFO, msg = "stopping a tablet group the map no longer names", group = %id);
                orphaned.extend(group.waiting.into_iter().map(|waiting| (id, waiting)));
                if let Some(raft) = group.raft {
                    glommio::spawn_local(async move {
                        let _ = raft.shutdown().await;
                    })
                    .detach();
                }
            }
        }
        replication.tablets.clear();
        // build what it newly names
        for spec in specs {
            for tablet in &spec.tablets {
                replication.tablets.insert((spec.table, *tablet), spec.id);
            }
            if let Some(existing) = replication.groups.get_mut(&spec.id) {
                // the same identity is the same table over the same members; the tablets are
                // the same too, since the identity is a function of the members
                existing.spec = spec;
                continue;
            }
            let Some(table) = D::table_of_id(spec.table) else {
                event!(Level::WARN, msg = "the map names a table this schema does not have", table = %spec.table);
                continue;
            };
            // the store: the WAL for a persistent table, memory for an ephemeral one
            let store = if Self::is_persistent(table) {
                replication.wal.store(spec.id)
            } else {
                replication.volatile.store(spec.id)
            };
            // the checkpoint the group starts from, if its table's archives hold one, and the
            // retry table as of it, which the log above the checkpoint cannot rebuild
            let (checkpoint, membership, seed) = match replication.checkpoint.get(spec.id) {
                Some(point) if store.is_volatile() => {
                    let _ = point;
                    (None, StoredMembership::default(), Vec::new())
                }
                Some(point) => (
                    point.applied.clone(),
                    point.membership(),
                    replication.retries.seed_for(spec.id, point),
                ),
                None => (None, StoredMembership::default(), Vec::new()),
            };
            // a durable group whose checkpoint or archives say this shard held it, with no
            // log behind them, lost its WAL: what it acknowledged is gone, and the leader
            // feeds it again rather than stopping - said here, once, by name, since the
            // leader's side of a reversion is a library log line
            // ([Resolved #99](../../../../docs/src/appendix/resolved/durable-log-reversion.md))
            if !store.is_volatile() && replication.wal.last_log_id_of(spec.id).is_none() {
                let held = match &checkpoint {
                    Some(point) => Some(format!("checkpoint {}", point.index)),
                    None if table_map.holds_any(table, &spec.tablets) => Some("archives".to_string()),
                    None => None,
                };
                if let Some(held) = held {
                    replication.integrity.log_lost += 1;
                    event!(
                        Level::ERROR,
                        msg = "a durable group has no log behind what this shard holds of it: its WAL was lost, and its leader will feed it again",
                        group = %spec.id,
                        table = %table,
                        held,
                    );
                }
            }
            if !seed.is_empty() {
                event!(Level::DEBUG, msg = "seeded a group's retry table from its sidecar", group = %spec.id, entries = seed.len());
            }
            let mut machine_state = MachineState::at(checkpoint, membership, seed);
            // a received snapshot past the checkpoint, which openraft installs as it builds
            // the group ([F43](../../../../docs/src/features/node-recovery.md))
            machine_state.pending_install = replication.pending_installs.remove(&spec.id);
            let state = Rc::new(RefCell::new(machine_state));
            let machine = GroupMachine::new(spec.id, state.clone(), tx.clone());
            let group = Group {
                spec: spec.clone(),
                table,
                raft: None,
                state,
                store: store.clone(),
                pending_bytes: 0,
                waiting: Vec::new(),
                snapshot: None,
                retired: Vec::new(),
                snapshot_building: false,
                snapshot_waiting: Vec::new(),
            };
            replication.groups.insert(spec.id, group);
            // the handle is built on a task of its own, since building it applies
            let network = GroupNetwork {
                group: spec.id,
                network: replication.network.clone(),
            };
            let config = group_config(&cluster, failover_ms, spec.id);
            let tx = tx.clone();
            let addr = ShardAddr::new(me, spec.mine);
            let primary = spec.is_primary(me);
            glommio::spawn_local(async move {
                let outcome = start_group(addr, spec, config, network, store, machine, primary).await;
                let _ = tx
                    .send(ServerMsg::GroupUp {
                        group: network_group(&outcome),
                        raft: outcome.map(|(_, raft)| raft).map_err(|(_, error)| error),
                    })
                    .await;
            })
            .detach();
        }
        // the writes that waited on a group this node no longer hosts are refused by name
        for (id, (meta, table, key, _)) in orphaned {
            let outcome = ProposalOutcome::NotLeader(format!("group {id} left this node before it was up"));
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = Ring::tablet_of(key) as u16;
            self.answer_proposal(meta, table, tablet, None, outcome, 0).await?;
        }
        Ok(())
    }

    /// Take a group's handle from the task that built it
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `raft` - Its handle, or why it could not be built
    pub(super) async fn handle_group_up(
        &mut self,
        group: GroupId,
        raft: Result<Raft<DataConfig, GroupMachine<D>>, String>,
    ) -> Result<(), ServerError> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        match (replication.groups.get_mut(&group), raft) {
            // a handle for a group the map still names, and the writes that waited for it
            (Some(slot), Ok(raft)) => {
                event!(Level::INFO, msg = "a tablet group is up", group = %group, members = slot.spec.members.len());
                slot.raft = Some(raft);
                let waiting = std::mem::take(&mut slot.waiting);
                for (meta, table, key, payload) in waiting {
                    self.propose_write(meta, table, key, payload).await?;
                }
            }
            // a handle built for a group the map has since dropped
            (None, Ok(raft)) => {
                glommio::spawn_local(async move {
                    let _ = raft.shutdown().await;
                })
                .detach();
            }
            // a group that could not be built is a shard that cannot serve its tablets
            (_, Err(error)) => {
                return Err(ServerError::GlommioGeneric(format!(
                    "tablet group {group} could not be built: {error}"
                )));
            }
        }
        Ok(())
    }

    /// Apply a batch of committed entries a group's state machine handed the loop
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `entries` - The entries, in order
    /// * `done` - Fired once the batch is through
    pub(super) async fn handle_apply(
        &mut self,
        group: GroupId,
        entries: Vec<EntryResponder<DataConfig>>,
        done: oneshot::Sender<()>,
    ) -> Result<(), ServerError> {
        self.run_apply(
            ParkedApply {
                group,
                entries: VecDeque::from(entries),
                done,
            },
            false,
        )
        .await
    }

    /// Apply a batch from its front until it is through or a command needs a read
    ///
    /// # Arguments
    ///
    /// * `batch` - The batch
    /// * `resumed` - Whether the front command already asked for a read, and applies without one
    async fn run_apply(&mut self, mut batch: ParkedApply, mut resumed: bool) -> Result<(), ServerError> {
        while let Some((entry, responder)) = batch.entries.pop_front() {
            let log_id = entry.log_id.clone();
            let group = batch.group;
            // the group the entry belongs to, which a batch for a dropped group no longer has
            let Some((table, generation, state, volatile)) = self.group_apply_context(group, log_id.index) else {
                // a dropped group's batch is let go: its responders fail, its machine's apply
                // returns, and the handle being shut down is what asked for both
                return Ok(());
            };
            let outcome = match &entry.payload {
                // a blank or a membership entry moves the applied position and nothing else
                EntryPayload::Blank => None,
                EntryPayload::Membership(membership) => {
                    state.borrow_mut().membership = StoredMembership::new(Some(log_id.clone()), membership.clone());
                    None
                }
                EntryPayload::Normal(command) => {
                    // a repeat of a remembered identity is answered as the first was
                    let remembered = state.borrow_mut().dedup.get(&command.request).copied();
                    match remembered {
                        Some(remembered) if remembered.digest == command.digest() => {
                            Some(ApplyOutcome::Duplicate(remembered.result))
                        }
                        Some(_) => Some(ApplyOutcome::Refused(
                            "the request identity was reused with a different payload".to_string(),
                        )),
                        None => match self.tables.apply_command(table, command, generation, resumed) {
                            ApplyStep::Done(result) => {
                                event!(Level::DEBUG, msg = "applied a command", group = %group, index = log_id.index, table = %table, ok = result.ok);
                                let remembered = Remembered {
                                    digest: command.digest(),
                                    result,
                                    applied: log_id.index,
                                };
                                state.borrow_mut().dedup.put(command.request, remembered);
                                Some(ApplyOutcome::Applied(result))
                            }
                            ApplyStep::Refused(reason) => Some(ApplyOutcome::Refused(reason)),
                            ApplyStep::NeedsLoad(partition) => {
                                // the read is asked for and the batch waits where it stands
                                let span = Span::current();
                                let coming = self.tables.request_load(table, partition, &span).await?;
                                batch.entries.push_front((entry, responder));
                                if coming {
                                    self.replication
                                        .as_mut()
                                        .expect("a group has a replication half")
                                        .parked
                                        .entry((table, partition))
                                        .or_default()
                                        .push(batch);
                                    return Ok(());
                                }
                                // nothing on disk: apply again without asking
                                resumed = true;
                                continue;
                            }
                        },
                    }
                }
            };
            // the entry is applied, whatever it was
            resumed = false;
            let mut applied = state.borrow_mut();
            applied.applied = Some(log_id);
            // a volatile group's checkpoint is its applied position: nothing of it reaches a
            // disk, so nothing else ever moves it, and without a moving checkpoint the snapshot
            // builder is never offered and the log is never purged
            // ([Resolved #105](../../../../docs/src/appendix/resolved/volatile-groups-never-purged.md))
            if volatile {
                applied.checkpoint = applied.applied.clone();
                applied.checkpoint_membership = applied.membership.clone();
                applied.checkpoint_durable = true;
            }
            drop(applied);
            if let Some(responder) = responder {
                responder.send(outcome.unwrap_or(ApplyOutcome::Refused(
                    "a blank or membership entry has no result".to_string(),
                )));
            }
        }
        let _ = batch.done.send(());
        // an applied position may have resolved a sealed segment
        if let Some(replication) = self.replication.as_mut() {
            replication.sweep_due = true;
        }
        Ok(())
    }

    /// What applying an entry of a group needs: its table, its frame's generation, its state,
    /// and whether its log lives in memory alone
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `index` - The entry's index
    fn group_apply_context(&self, group: GroupId, index: u64) -> Option<(D::TableNames, u64, Rc<RefCell<MachineState>>, bool)> {
        let replication = self.replication.as_ref()?;
        let slot = replication.groups.get(&group)?;
        // the generation the entry's frame is in, or the active one for a volatile group
        let generation = replication
            .wal
            .generation_of(group, index)
            .unwrap_or_else(|| replication.wal.active_generation());
        Some((slot.table, generation, slot.state.clone(), slot.store.is_volatile()))
    }

    /// Resume every apply batch parked on a partition, now that its read has landed or failed
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `partition` - The partition
    /// * `failed` - Whether the read failed outright, which no replica can serve past
    pub(super) async fn resume_parked(
        &mut self,
        table: D::TableNames,
        partition: u64,
        failed: bool,
    ) -> Result<(), ServerError> {
        let parked = match self.replication.as_mut() {
            Some(replication) => replication.parked.remove(&(table, partition)),
            None => None,
        };
        let Some(parked) = parked else {
            return Ok(());
        };
        // a replica that cannot read its archive is not a replica: a result computed without
        // the read would diverge from the leader's
        if failed {
            return Err(ServerError::GlommioGeneric(format!(
                "partition {partition} of {table} could not be read for a replicated apply"
            )));
        }
        for batch in parked {
            self.run_apply(batch, true).await?;
        }
        Ok(())
    }

    /// Propose a write through the group serving its tablet
    ///
    /// Admission is judged here - the shard's pending bytes and a volatile log's bound - and a
    /// task waits on the group, so the loop keeps serving meanwhile; the outcome comes back as
    /// [`ServerMsg::Proposed`].
    ///
    /// # Arguments
    ///
    /// * `meta` - The write's metadata
    /// * `table` - The table it names
    /// * `key` - The partition key it names
    /// * `payload` - The table's serialized intent
    pub(super) async fn propose_write(
        &mut self,
        mut meta: QueryMetadata,
        table: D::TableNames,
        key: u64,
        payload: Vec<u8>,
    ) -> Result<(), ServerError> {
        // a write's stamps say what it was and how it was made durable
        meta.stamps.set_op(StageOp::Insert);
        meta.stamps.set_durability(StageDurability::Fsync);
        let tablet = Ring::tablet_of(key);
        // truncation cannot happen: a tablet id is twelve bits
        #[allow(clippy::cast_possible_truncation)]
        let tablet = tablet as u16;
        let cluster = self.conf.cluster.clone().unwrap_or_default();
        let Some(replication) = self.replication.as_mut() else {
            return self
                .answer_proposal(meta, table, tablet, None, ProposalOutcome::Failed("this node hosts no tablet groups".to_string()), 0)
                .await;
        };
        let Some(id) = replication.tablets.get(&(table.table_id(), tablet)).copied() else {
            let outcome = ProposalOutcome::NotLeader(format!("no group serves tablet {tablet} of {table} on this node"));
            return self.answer_proposal(meta, table, tablet, None, outcome, 0).await;
        };
        let Some(group) = replication.groups.get_mut(&id) else {
            let outcome = ProposalOutcome::NotLeader(format!("group {id} is not hosted here"));
            return self.answer_proposal(meta, table, tablet, None, outcome, 0).await;
        };
        // a group whose handle is still being built takes the write once it is up
        if group.raft.is_none() {
            group.waiting.push((meta, table, key, payload));
            return Ok(());
        }
        let bytes = payload.len();
        // admission: a definite refusal, judged before anything is recorded
        if group.pending_bytes + bytes > cluster.replication.pending_bytes {
            let outcome = ProposalOutcome::Shed(format!(
                "{} bytes are proposed and unanswered for group {id}, past the {} byte bound",
                group.pending_bytes, cluster.replication.pending_bytes
            ));
            return self.answer_proposal(meta, table, tablet, None, outcome, 0).await;
        }
        if group.store.is_volatile() && group.store.bytes() + bytes > cluster.replication.volatile_log_bytes {
            let outcome = ProposalOutcome::Shed(format!(
                "the volatile logs hold {} bytes, past the {} byte bound",
                group.store.bytes(),
                cluster.replication.volatile_log_bytes
            ));
            return self.answer_proposal(meta, table, tablet, None, outcome, 0).await;
        }
        group.pending_bytes += bytes;
        let command = Command {
            table: table.table_id(),
            tablet,
            request: RequestId {
                bundle: *meta.id.as_bytes(),
                index: meta.index as u64,
            },
            payload,
        };
        let raft = group.raft.clone();
        let network = replication.network.clone();
        let me = self.my_addr();
        // the write's budget: the proposal deadline, or what is left of the bundle's if that is
        // shorter - a forwarded write counts down from the origin's budget, never up from a
        // fresh one ([C2](../../../../docs/src/distributed/transport.md))
        let bundle_left = Duration::from_nanos(meta.read.deadline.since(Stamp::now()));
        let deadline = cluster.replication.write_timeout.duration().min(bundle_left);
        let all = self.map.get().write_consistency == crate::server::conf::cluster::Consistency::All;
        let tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            let outcome = propose_through(raft.as_ref(), &network, id, me, command, deadline, true, all).await;
            let _ = tx
                .send(ServerMsg::Proposed {
                    meta,
                    table,
                    tablet,
                    group: Some(id),
                    outcome,
                    bytes,
                })
                .await;
        })
        .detach();
        Ok(())
    }

    /// Answer a client whose proposal resolved, with the session token it minted if it committed
    ///
    /// A committed write's answer carries the cluster, the table, the tablet, the group and the
    /// index it committed at, so a later read can be served past it on any replica. A
    /// duplicate carries the index its repeat committed at rather than the original's: a bound
    /// past the repeat is past the original too, so it is never wrong, only later than it need
    /// be ([F41](../../../../docs/src/features/read-consistency.md)).
    ///
    /// # Arguments
    ///
    /// * `meta` - The write's metadata
    /// * `table` - The table it named
    /// * `tablet` - The tablet its key hashed to
    /// * `group` - The group it went through, if admission let it that far
    /// * `outcome` - What the proposal came to
    /// * `bytes` - How many bytes were held pending for it
    pub(super) async fn answer_proposal(
        &mut self,
        mut meta: QueryMetadata,
        table: D::TableNames,
        tablet: u16,
        group: Option<GroupId>,
        outcome: ProposalOutcome,
        bytes: usize,
    ) -> Result<(), ServerError> {
        // the bytes are no longer pending, and the stats say what happened
        if let Some(replication) = self.replication.as_mut() {
            if let Some(slot) = group.and_then(|group| replication.groups.get_mut(&group)) {
                slot.pending_bytes = slot.pending_bytes.saturating_sub(bytes);
            }
            match &outcome {
                ProposalOutcome::Unknown(_) => replication.stats.unknown += 1,
                ProposalOutcome::Shed(_) | ProposalOutcome::NotLeader(_) | ProposalOutcome::Failed(_) => {
                    replication.stats.rejected += 1;
                }
                ProposalOutcome::Answered {
                    outcome: ApplyOutcome::Refused(_),
                    ..
                } => replication.stats.rejected += 1,
                ProposalOutcome::Answered { .. } => {}
            }
        }
        let (client, id, index, end) = (meta.client, meta.id, meta.index, meta.end);
        let committed = matches!(
            &outcome,
            ProposalOutcome::Answered {
                outcome: ApplyOutcome::Applied(_) | ApplyOutcome::Duplicate(_),
                ..
            }
        );
        // a committed write mints a token naming where it committed
        let token = match (&outcome, group) {
            (
                ProposalOutcome::Answered {
                    outcome: ApplyOutcome::Applied(_) | ApplyOutcome::Duplicate(_),
                    index: committed,
                },
                Some(group),
            ) => Some(SessionToken {
                cluster: self.map.get().cluster.unwrap_or_default(),
                table: table.table_id(),
                tablet,
                group,
                index: *committed,
            }),
            _ => None,
        };
        let response = match outcome {
            ProposalOutcome::Answered {
                outcome: ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result),
                ..
            } => D::write_response(table, id, index, end, result),
            ProposalOutcome::Answered {
                outcome: ApplyOutcome::Refused(msg),
                ..
            } => D::ClientType::failed(table, id, index, end, ResponseError::new(ErrorCode::Internal, msg)),
            ProposalOutcome::Shed(msg) => {
                D::ClientType::failed(table, id, index, end, ResponseError::new(ErrorCode::Shedding, msg))
            }
            ProposalOutcome::NotLeader(msg) => {
                D::ClientType::failed(table, id, index, end, ResponseError::new(ErrorCode::NotLeader, msg))
            }
            ProposalOutcome::Unknown(msg) => {
                D::ClientType::failed(table, id, index, end, ResponseError::new(ErrorCode::OutcomeUnknown, msg))
            }
            ProposalOutcome::Failed(msg) => {
                D::ClientType::failed(table, id, index, end, ResponseError::new(ErrorCode::Unavailable, msg))
            }
        };
        meta.stamps.mark_exec_done();
        let span = meta.span.clone();
        // the fixture's lost response: the write committed and applied, and its answer goes
        // nowhere, which is what a retry under the same identity has to recover from
        if committed {
            if let Some(replication) = self.replication.as_mut() {
                if replication.drop_replies > 0 {
                    replication.drop_replies -= 1;
                    event!(Level::WARN, msg = "dropping a committed write's reply, as the fixture asked", id = %id, index);
                    return Ok(());
                }
            }
        }
        // the token rides the answer to a client that asked for one, and the answer head to a
        // peer that forwarded the write
        self.reply_with_token(client, id, span, meta.stamps, response, token).await
    }

    /// Answer a replication request a peer sent this shard
    ///
    /// # Arguments
    ///
    /// * `origin` - The peer
    /// * `head` - The request's fixed fields
    /// * `payload` - Its payload
    /// * `reply` - Where the answer goes
    pub(super) fn handle_replication(
        &mut self,
        origin: NodeId,
        head: ReplicateRequestHead,
        payload: Vec<u8>,
        reply: kanal::AsyncSender<ReplicateReply>,
    ) {
        let group = GroupId(head.group);
        let Some(replication) = self.replication.as_ref() else {
            let _ = reply.try_send(ReplicateReply::error(head.id, "this node hosts no tablet groups"));
            return;
        };
        let Some(slot) = replication.groups.get(&group) else {
            let _ = reply.try_send(ReplicateReply::error(head.id, format!("group {group} is not hosted on this shard")));
            return;
        };
        let Some(raft) = slot.raft.clone() else {
            let _ = reply.try_send(ReplicateReply::error(head.id, format!("group {group} is still starting")));
            return;
        };
        // a snapshot rpc is the receiver's: judged on the loop against what it holds
        // ([F43](../../../../docs/src/features/node-recovery.md))
        if head.kind == ReplicateKind::Snapshot {
            self.handle_snapshot_rpc(origin, head, payload, reply);
            return;
        }
        let network = replication.network.clone();
        let me = self.my_addr();
        let deadline = Duration::from_millis(u64::from(head.deadline_ms.max(1)));
        let all = self.map.get().write_consistency == crate::server::conf::cluster::Consistency::All;
        let _ = origin;
        glommio::spawn_local(async move {
            let answer = match head.kind {
                ReplicateKind::AppendEntries => match postcard::from_bytes(&payload) {
                    Ok(rpc) => match raft.append_entries(rpc).await {
                        Ok(response) => encode_reply(head.id, &response),
                        Err(error) => ReplicateReply::error(head.id, format!("append_entries: {error}")),
                    },
                    Err(error) => ReplicateReply::error(head.id, format!("decoding append_entries: {error}")),
                },
                ReplicateKind::Vote => match postcard::from_bytes(&payload) {
                    Ok(rpc) => match raft.vote(rpc).await {
                        Ok(response) => encode_reply(head.id, &response),
                        Err(error) => ReplicateReply::error(head.id, format!("vote: {error}")),
                    },
                    Err(error) => ReplicateReply::error(head.id, format!("decoding vote: {error}")),
                },
                ReplicateKind::Propose => match Command::decode(&payload) {
                    Ok(command) => {
                        // one hop only: a proposal that arrived here is not forwarded again
                        let outcome = propose_through(Some(&raft), &network, group, me, command, deadline, false, all).await;
                        encode_reply(head.id, &outcome)
                    }
                    Err(error) => ReplicateReply::error(head.id, format!("decoding a proposal: {error}")),
                },
                // a snapshot rpc is judged on the loop, so it is answered before this task
                ReplicateKind::Snapshot => unreachable!("a snapshot rpc is answered on the loop"),
                // a read barrier: confirm leadership with a heartbeat round and answer the
                // read log id, or say who leads instead. A lapsed lease is answered by name
                // rather than waited out, since its heartbeat round cannot complete
                ReplicateKind::ReadBarrier => {
                    if Lease::of(&raft, me) == Lease::Lapsed {
                        let answer = BarrierAnswer::NoQuorum(format!(
                            "the lease of {me} on the group lapsed: no quorum acknowledged it within {:?}",
                            Lease::length(&raft)
                        ));
                        encode_reply(head.id, &answer)
                    } else {
                        match raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
                            Ok(linearizer) => encode_reply(head.id, &BarrierAnswer::Ready(linearizer.read_log_id().clone())),
                            Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                                encode_reply(head.id, &BarrierAnswer::NotLeader(forward.leader_node.or(forward.leader_id)))
                            }
                            Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(short))) => {
                                encode_reply(head.id, &BarrierAnswer::NoQuorum(short.to_string()))
                            }
                            Err(RaftError::Fatal(fatal)) => ReplicateReply::error(head.id, format!("read_barrier: {fatal}")),
                        }
                    }
                }
            };
            let _ = reply.send(answer).await;
        })
        .detach();
    }

    /// Act on what a replication link learned
    ///
    /// # Arguments
    ///
    /// * `event` - The link event
    pub(super) fn handle_replication_link(&mut self, event: LinkEvent) {
        let Some(replication) = self.replication.as_ref() else {
            return;
        };
        match event {
            LinkEvent::Frame { node, head, payload, .. } => {
                replication.network.answered(node, &head, payload.to_vec());
            }
            LinkEvent::Down { node, reason, unsent, .. } => replication.network.down(node, &reason, &unsent),
            LinkEvent::Up { .. } => {}
        }
    }

    /// Judge every sealed segment: hand a resolved one to the compactors, delete a purged one
    ///
    /// Segments are handed in generation order and only once: a segment behind an unresolved
    /// one waits, since compacting it first would apply a later write under an earlier one.
    pub(super) async fn sweep_segments(&mut self) -> Result<(), ServerError> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        replication.sweep_due = false;
        let segments = replication.wal.segments();
        let mut handoffs = Vec::new();
        let mut deletions = Vec::new();
        for segment in segments {
            if !segment.sealed {
                break;
            }
            if segment.handed {
                // handed and compacted: gone once every group in it purged past it
                let compacting = replication.compacting.get(&segment.generation).is_some_and(|tables| !tables.is_empty());
                let purged = segment.last.iter().all(|(group, last)| {
                    replication.groups.get(group).is_none_or(|slot| {
                        slot.state.borrow().checkpoint_index() >= last.index
                            && slot
                                .store
                                .purged_index()
                                .is_some_and(|purged| purged >= last.index)
                    })
                });
                if !compacting && purged {
                    deletions.push(segment.generation);
                }
                continue;
            }
            // resolved: every group with frames in it applied past them, or is gone
            let resolved = segment.last.iter().all(|(group, last)| {
                replication
                    .groups
                    .get(group)
                    .is_none_or(|slot| slot.state.borrow().applied_index() >= last.index)
            });
            if !resolved {
                break;
            }
            handoffs.push(segment);
        }
        let sinks: HashMap<D::TableNames, kanal::AsyncSender<CompactionJob>> =
            self.tables.compaction_sinks().into_iter().collect();
        let mut compacted_now = Vec::new();
        for segment in handoffs {
            // this table's frames, per table, in log order per group, each group from its
            // checkpoint: a frame at or below it was merged before, and after a restart every
            // sealed segment looks unhanded ([Resolved #104](../../../../docs/src/appendix/resolved/segments-recompacted-after-restart.md))
            let mut by_table: HashMap<D::TableNames, Vec<(GroupId, u64)>> = HashMap::new();
            for group in segment.last.keys() {
                if let Some(slot) = replication.groups.get(group) {
                    let since = slot.state.borrow().checkpoint_index();
                    by_table.entry(slot.table).or_default().push((*group, since));
                }
            }
            let mut tables = HashSet::new();
            for (table, groups) in by_table {
                let mut frames = replication.wal.frames_in(segment.generation, &groups);
                frames.sort_by_key(|frame| (frame.group, frame.index));
                let refs: Vec<(u64, u32)> = frames.iter().map(|frame| (frame.offset, frame.len)).collect();
                // where each group stands once these frames are merged, for a snapshot cut
                // after them ([F43](../../../../docs/src/features/node-recovery.md))
                let positions: Vec<(GroupId, crate::server::wal::WalLogId)> = groups
                    .iter()
                    .filter_map(|(group, _)| segment.last.get(group).map(|last| (*group, last.clone())))
                    .collect();
                match sinks.get(&table) {
                    Some(sink) if !refs.is_empty() => {
                        tables.insert(table);
                        sink.send(CompactionJob::Segment {
                            path: replication.wal.segment_path(segment.generation),
                            generation: segment.generation,
                            frames: refs,
                            positions,
                        })
                        .await?;
                        sink.send(CompactionJob::Archives).await?;
                    }
                    // a table with no compactor, or no frames, is done with the segment now
                    _ => {}
                }
            }
            replication.wal.mark_handed(segment.generation);
            event!(Level::DEBUG, msg = "handed a wal segment to the compactors", generation = segment.generation, tables = tables.len());
            if tables.is_empty() {
                compacted_now.push(segment.generation);
            }
            replication.compacting.insert(segment.generation, tables);
        }
        for generation in deletions {
            replication.compacting.remove(&generation);
            if let Err(error) = replication.wal.delete_segment(generation).await {
                event!(Level::WARN, msg = "failed to delete a purged wal segment", generation, ?error);
            }
        }
        // a segment with nothing to compact moves every checkpoint in it now
        for generation in compacted_now {
            self.advance_checkpoints(generation, None);
        }
        self.enforce_retention();
        Ok(())
    }

    /// Force the groups pinning the oldest sealed segments past them once the budget is passed
    ///
    /// The entries budget is a preference and the bytes budget is a bound
    /// ([Q9](../../../../docs/src/distributed/protocol.md)): when the sealed segments still on
    /// disk hold more than `retained_bytes`, every group with frames in the oldest of them is
    /// asked to snapshot at its checkpoint and purge through it, so the next sweep can delete
    /// them. A member behind the forced purge point falls to the snapshot path; nothing pins
    /// the leader's log for a follower ([F43](../../../../docs/src/features/node-recovery.md)).
    fn enforce_retention(&mut self) {
        let budget = self.conf.cluster.as_ref().map_or(u64::MAX, |cluster| cluster.replication.retained_bytes);
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let sealed: Vec<_> = replication.wal.segments().into_iter().filter(|segment| segment.sealed).collect();
        let mut held: u64 = sealed.iter().map(|segment| segment.bytes).sum();
        if held <= budget {
            return;
        }
        // the oldest segments, until what is left fits the budget
        let mut forced = 0u64;
        for segment in &sealed {
            if held <= budget {
                break;
            }
            held = held.saturating_sub(segment.bytes);
            for (group, last) in &segment.last {
                let Some(slot) = replication.groups.get(group) else { continue };
                let (checkpoint, purged) = {
                    let state = slot.state.borrow();
                    (state.checkpoint_index(), slot.store.purged_index().unwrap_or(0))
                };
                // a group not yet compacted past the segment cannot purge it; one purged past
                // it already is not what pins it
                if checkpoint < last.index || purged >= last.index {
                    continue;
                }
                let Some(raft) = slot.raft.clone() else { continue };
                forced += 1;
                event!(
                    Level::WARN,
                    msg = "the retention budget is passed; forcing a group past a sealed segment",
                    group = %group,
                    generation = segment.generation,
                    checkpoint,
                    purged,
                    lag = checkpoint.saturating_sub(purged),
                    held_bytes = held,
                    budget,
                );
                glommio::spawn_local(async move {
                    // the snapshot first, so the purge has one to stop at
                    let _ = raft.trigger().snapshot().await;
                    let _ = raft.trigger().purge_log(checkpoint).await;
                })
                .detach();
            }
        }
        replication.snapshots.forced += forced;
    }

    /// Note that a table's compactor finished a segment
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `generation` - The segment
    pub(super) fn handle_segment_compacted(&mut self, table: D::TableNames, generation: u64) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        if let Some(tables) = replication.compacting.get_mut(&generation) {
            tables.remove(&table);
        }
        self.advance_checkpoints(generation, Some(table));
    }

    /// Move the checkpoint of every group of a table with frames in a segment to its last entry there
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    /// * `table` - The table whose compactor finished, or none for every table at once
    fn advance_checkpoints(&mut self, generation: u64, table: Option<D::TableNames>) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let Some(segment) = replication
            .wal
            .segments()
            .into_iter()
            .find(|segment| segment.generation == generation)
        else {
            return;
        };
        let mut moved = false;
        for (group, last) in &segment.last {
            let Some(slot) = replication.groups.get(group) else {
                continue;
            };
            if table.is_some_and(|table| slot.table != table) {
                continue;
            }
            let mut state = slot.state.borrow_mut();
            if state.checkpoint_index() < last.index {
                state.checkpoint = Some(last.clone());
                state.checkpoint_membership = state.membership.clone();
                state.checkpoint_durable = false;
                moved = true;
            }
        }
        if moved {
            replication.checkpoint_dirty = true;
            self.write_checkpoint();
        }
        if let Some(replication) = self.replication.as_mut() {
            replication.sweep_due = true;
        }
    }

    /// Write the retry sidecar and then the checkpoint file from every group's checkpoint, on a task of its own
    ///
    /// The sidecar goes first: a checkpoint that names a sidecar index must find one complete
    /// to it at open, and a crash between the two leaves a sidecar ahead of its checkpoint,
    /// which the seed rule ignores. The checkpoint counts as durable only once the checkpoint
    /// file itself landed.
    ///
    /// Returns whether a write was started now; if not, a dirty checkpoint is written by the
    /// next write, which is the version after the one in flight.
    pub(super) fn write_checkpoint(&mut self) -> bool {
        let Some(replication) = self.replication.as_mut() else {
            return false;
        };
        if replication.checkpoint_writing || !replication.checkpoint_dirty {
            return false;
        }
        replication.checkpoint_writing = true;
        replication.checkpoint_dirty = false;
        replication.checkpoint_version += 1;
        let version = replication.checkpoint_version;
        // what every persistent group says its checkpoint is, and what it remembers as of it
        let mut file = Checkpoint::default();
        let mut retries = Retries::default();
        for (id, slot) in &replication.groups {
            if slot.store.is_volatile() {
                continue;
            }
            let state = slot.state.borrow();
            if let Some(applied) = &state.checkpoint {
                // the entries at or below the checkpoint; the rest the log replay re-derives
                let entries = state.remembered_through(applied.index);
                retries.groups.insert(
                    id.to_string(),
                    GroupRetries {
                        retries_at: applied.index,
                        entries,
                    },
                );
                file.groups.insert(
                    id.to_string(),
                    GroupCheckpoint::new(Some(applied.clone()), &state.checkpoint_membership)
                        .retries(applied.index, state.retry_floor()),
                );
            }
        }
        replication.checkpoint = file.clone();
        replication.retries = retries.clone();
        let dir = replication.wal.dir();
        let tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            // the sidecar first, then the checkpoint that names it
            let outcome = match retries.write(&dir).await {
                Ok(()) => file.write(&dir).await.map_err(|error| error.to_string()),
                Err(error) => Err(format!("the retry sidecar could not be written: {error}")),
            };
            let _ = tx.send(ServerMsg::CheckpointWritten { version, outcome }).await;
        })
        .detach();
        true
    }

    /// Note that a checkpoint write landed, so the groups it covered may snapshot
    ///
    /// # Arguments
    ///
    /// * `version` - Which write
    /// * `outcome` - Whether it landed
    pub(super) fn handle_checkpoint_written(&mut self, version: u64, outcome: Result<(), String>) -> Result<(), ServerError> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        replication.checkpoint_writing = false;
        if let Err(error) = outcome {
            return Err(ServerError::GlommioGeneric(format!("the checkpoint file could not be written: {error}")));
        }
        if version == replication.checkpoint_version && !replication.checkpoint_dirty {
            // every group's checkpoint is on disk as it stands, and a group whose checkpoint
            // moved past its last snapshot is asked to build one now: openraft's own policy
            // is judged as entries commit, and a checkpoint that becomes durable after the
            // writes stopped would otherwise never be snapshotted or purged behind
            // ([F43](../../../../docs/src/features/node-recovery.md))
            for slot in replication.groups.values() {
                let moved = {
                    let mut state = slot.state.borrow_mut();
                    state.checkpoint_durable = true;
                    state.checkpoint.is_some() && state.checkpoint != state.snapshot_at
                };
                if moved {
                    if let Some(raft) = slot.raft.clone() {
                        glommio::spawn_local(async move {
                            let _ = raft.trigger().snapshot().await;
                        })
                        .detach();
                    }
                }
            }
        }
        // an install whose state this write carried is durable now, and can be cleaned up
        self.checkpoint_carried_installs(version);
        // a move that happened meanwhile is written next
        self.write_checkpoint();
        Ok(())
    }

    /// What this shard's groups look like, for readiness and the fixture
    pub(super) fn replication_report(&self) -> ShardReplication {
        let Some(replication) = self.replication.as_ref() else {
            return ShardReplication {
                shard: self.shard_id,
                reads: self.read_stats,
                ..ShardReplication::default()
            };
        };
        let me = self.my_addr();
        let groups = replication
            .groups
            .values()
            .map(|slot| {
                let state = slot.state.borrow();
                let metrics = slot.raft.as_ref().map(|raft| raft.metrics().borrow_watched().clone());
                let leader = metrics.as_ref().and_then(|metrics| metrics.current_leader.clone());
                GroupReport {
                    group: slot.spec.id,
                    table: slot.spec.table,
                    table_name: slot.table.to_string(),
                    tablets: u32::try_from(slot.spec.tablets.len()).unwrap_or(u32::MAX),
                    tablet_ids: slot.spec.tablets.clone(),
                    members: slot.spec.members.clone(),
                    leader: leader.clone(),
                    is_leader: leader == Some(me),
                    applied: state.applied_index(),
                    committed: metrics
                        .as_ref()
                        .and_then(|metrics| metrics.cluster_committed.as_ref().map(|log_id| log_id.index))
                        .unwrap_or(0),
                    last_log: metrics
                        .as_ref()
                        .and_then(|metrics| metrics.last_log_index)
                        .unwrap_or(0),
                    checkpoint: state.checkpoint_index(),
                    purged: slot.store.purged_index().unwrap_or(0),
                    pending_bytes: slot.pending_bytes,
                    volatile: slot.store.is_volatile(),
                    up: slot.raft.is_some(),
                    installing: state.installing,
                }
            })
            .collect::<Vec<_>>();
        ShardReplication {
            shard: self.shard_id,
            pending_bytes: groups.iter().map(|group| group.pending_bytes).sum(),
            volatile_bytes: replication.volatile.bytes(),
            segments: replication.wal.segments().len(),
            compacting: replication
                .compacting
                .iter()
                .filter(|(_, tables)| !tables.is_empty())
                .map(|(generation, _)| *generation)
                .collect(),
            unknown_outcomes: replication.stats.unknown,
            rejected: replication.stats.rejected,
            reads: self.read_stats,
            snapshots: {
                // what the loop counted, what the partials counted, what the sender counted
                let mut stats = replication.snapshots;
                stats.absorb(&replication.installs.stats());
                stats.absorb(&replication.network.snapshot_stats());
                stats
            },
            integrity: {
                // what the loop found at open, and what the archive maps counted since
                let mut stats = replication.integrity;
                stats.absorb(&self.table_map.integrity());
                stats
            },
            groups,
        }
    }

    /// Post a replication report to the control thread every so many deadline ticks
    pub(super) fn maybe_report_replication(&mut self) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        replication.ticks += 1;
        // a segment sweep every so many ticks; a report on every tick something moved, so the
        // view an admin read folds is at most a tick behind the shard
        if replication.ticks >= REPORT_EVERY_TICKS {
            replication.ticks = 0;
            replication.sweep_due = true;
        }
        let report = self.replication_report();
        let replication = self.replication.as_mut().expect("still here");
        if replication.last_report.as_ref() == Some(&report) {
            return;
        }
        if let Some(control) = &self.control {
            if control.try_send(crate::server::control::ControlRequest::Replication(report.clone())).is_ok() {
                replication.last_report = Some(report);
            }
        }
    }

    /// Drive a replication verb, for the fixture, answering on the reply channel
    ///
    /// Every verb but the snapshot cut answers before returning; the cut answers from a task
    /// once the file is built.
    ///
    /// # Arguments
    ///
    /// * `verb` - What to do
    /// * `reply` - Where the answer goes
    pub(super) async fn handle_replication_verb(
        &mut self,
        verb: ReplicationVerb,
        reply: std::sync::mpsc::Sender<Result<serde_json::Value, String>>,
    ) -> Result<(), ServerError> {
        let answer = self.replication_verb(verb, &reply).await;
        // a deferred answer is sent from its task; everything else is answered now
        if let Some(answer) = answer {
            let _ = reply.send(answer);
        }
        Ok(())
    }

    /// The verb itself: an answer, or none for one answered later from a task
    ///
    /// # Arguments
    ///
    /// * `verb` - What to do
    /// * `reply` - Where a deferred answer goes
    async fn replication_verb(
        &mut self,
        verb: ReplicationVerb,
        reply: &std::sync::mpsc::Sender<Result<serde_json::Value, String>>,
    ) -> Option<Result<serde_json::Value, String>> {
        let Some(replication) = self.replication.as_mut() else {
            return Some(Err("this node hosts no tablet groups".to_string()));
        };
        let reply = reply.clone();
        let answer: Result<serde_json::Value, String> = match verb {
            ReplicationVerb::Digest { table } => {
                let Some(name) = D::table_of_id(table) else {
                    return Some(Err(format!("no table has identity {table}")));
                };
                let (rows, hash) = match self.tables.digest_table(name).await {
                    Ok(digest) => digest,
                    Err(error) => return Some(Err(format!("{error:?}"))),
                };
                let groups: BTreeMap<String, u64> = replication
                    .groups
                    .values()
                    .filter(|slot| slot.spec.table == table)
                    .map(|slot| (slot.spec.id.to_string(), slot.state.borrow().applied_index()))
                    .collect();
                Ok(serde_json::json!({ "rows": rows, "hash": hash, "groups": groups }))
            }
            ReplicationVerb::Rotate => {
                replication.wal.rotate();
                let _ = replication.wal.flush().await;
                Ok(serde_json::json!({ "generation": replication.wal.active_generation() }))
            }
            ReplicationVerb::Compact => {
                if let Err(error) = self.sweep_segments().await {
                    return Some(Err(format!("{error:?}")));
                }
                let replication = self.replication.as_ref().expect("still here");
                let handed = replication.wal.segments().iter().filter(|segment| segment.handed).count();
                Ok(serde_json::json!({ "handed": handed, "segments": replication.wal.segments().len() }))
            }
            ReplicationVerb::Stall { group } => {
                replication.wal.stall(group);
                Ok(serde_json::json!({ "stalled": group.to_string() }))
            }
            ReplicationVerb::Release { group } => {
                replication.wal.release(group);
                Ok(serde_json::json!({ "released": group.to_string() }))
            }
            ReplicationVerb::DropReplies { n } => {
                replication.drop_replies = n;
                Ok(serde_json::json!({ "dropping": n }))
            }
            ReplicationVerb::Snapshot { group } => {
                // cut now, on the loop's own request; the manifest is answered from a task once
                // the cut lands, since the loop hears about it as a message
                if !replication.groups.contains_key(&group) {
                    return Some(Err(format!("group {group} is not hosted on this shard")));
                }
                let (built_tx, built) = oneshot::channel();
                if let Err(error) = self.handle_build_snapshot(group, built_tx).await {
                    return Some(Err(format!("{error:?}")));
                }
                glommio::spawn_local(async move {
                    let outcome = match built.await {
                        Ok(Ok(built)) => Ok(serde_json::json!({
                            "boundary": built.manifest.boundary.index,
                            "records": built.manifest.records,
                            "total": built.manifest.total,
                            "checksum": built.manifest.checksum,
                            "retries": built.manifest.retries,
                            "path": built.path.display().to_string(),
                        })),
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err("the cut was dropped".to_string()),
                    };
                    let _ = reply.send(outcome);
                })
                .detach();
                return None;
            }
        };
        Some(answer)
    }

    /// Hand a transfer a snapshot file of a group, cutting one if none is held
    ///
    /// A held file at or past the group's checkpoint is answered at once; otherwise a cut is
    /// asked for - of the compactor for a persistent group, of the loop's own task for a
    /// volatile one - and the reply waits with any other transfer wanting the same cut
    /// ([F43](../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `reply` - Where the file goes
    pub(super) async fn handle_build_snapshot(
        &mut self,
        group: GroupId,
        reply: oneshot::Sender<Result<Rc<BuiltSnapshot>, String>>,
    ) -> Result<(), ServerError> {
        let schema_id = <D::ClientType as QuerySupport>::SCHEMA_ID;
        let Some(replication) = self.replication.as_mut() else {
            let _ = reply.send(Err("this node hosts no tablet groups".to_string()));
            return Ok(());
        };
        let Some(slot) = replication.groups.get_mut(&group) else {
            let _ = reply.send(Err(format!("group {group} is not hosted on this shard")));
            return Ok(());
        };
        // a held file at the checkpoint or past it is the answer
        let checkpoint = slot.state.borrow().checkpoint_index();
        if let Some(built) = &slot.snapshot {
            if built.manifest.boundary.index >= checkpoint {
                let _ = reply.send(Ok(built.clone()));
                return Ok(());
            }
        }
        slot.snapshot_waiting.push(reply);
        if slot.snapshot_building {
            return Ok(());
        }
        slot.snapshot_building = true;
        let (at_least, membership, retries) = {
            let state = slot.state.borrow();
            (
                state.checkpoint.clone(),
                state.membership.clone(),
                state.dedup.iter().rev().map(|(request, remembered)| (*request, *remembered)).collect::<Vec<_>>(),
            )
        };
        let tablets = slot.spec.tablets.clone();
        let table = slot.table;
        let dir = replication.wal.dir().join(SNAPSHOTS_DIR);
        if slot.store.is_volatile() {
            // a volatile group's rows are the ephemeral table's resident partitions, cut here
            // where they are all visible, and written on a task of its own
            let Some(boundary) = at_least else {
                self.handle_snapshot_built(group, Err(format!("group {group} has applied nothing to cut"))).await?;
                return Ok(());
            };
            let records = self.tables.snapshot_partitions(table, &tablets);
            let remembered: Vec<_> = retries
                .into_iter()
                .filter(|(_, remembered)| remembered.applied <= boundary.index)
                .collect();
            let tx = self.shard_local_tx.clone();
            let table_id = table.table_id();
            glommio::spawn_local(async move {
                let outcome = write_volatile_snapshot(&dir, group, table_id, schema_id, boundary, membership, tablets, records, remembered)
                    .await
                    .map_err(|error| format!("{error:?}"));
                let _ = tx.send(ServerMsg::SnapshotBuilt { group, outcome }).await;
            })
            .detach();
            return Ok(());
        }
        // a persistent group's rows are its archives, and the compactor cuts them between jobs
        let sink = self
            .tables
            .compaction_sinks()
            .into_iter()
            .find(|(name, _)| *name == table)
            .map(|(_, sink)| sink);
        let Some(sink) = sink else {
            self.handle_snapshot_built(group, Err(format!("{table} has no compactor to cut a snapshot with"))).await?;
            return Ok(());
        };
        sink.send(CompactionJob::Snapshot {
            group,
            schema_id,
            tablets,
            at_least,
            membership,
            retries,
            dir,
        })
        .await?;
        Ok(())
    }

    /// Take a cut that landed, answer everybody waiting for it, and retire the file it replaces
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `outcome` - The file and its manifest, or why there is none
    pub(super) async fn handle_snapshot_built(
        &mut self,
        group: GroupId,
        outcome: Result<(PathBuf, SnapshotManifest), String>,
    ) -> Result<(), ServerError> {
        let Some(replication) = self.replication.as_mut() else {
            return Ok(());
        };
        let Some(slot) = replication.groups.get_mut(&group) else {
            // a cut for a group the map dropped meanwhile: the file is nobody's
            if let Ok((path, _)) = outcome {
                let _ = glommio::io::remove(&path).await;
            }
            return Ok(());
        };
        slot.snapshot_building = false;
        let waiting = std::mem::take(&mut slot.snapshot_waiting);
        match outcome {
            Ok((path, manifest)) => {
                replication.snapshots.built += 1;
                let built = Rc::new(BuiltSnapshot { path, manifest });
                // the file it replaces goes once no transfer holds it
                if let Some(old) = slot.snapshot.replace(built.clone()) {
                    slot.retired.push(old);
                }
                for reply in waiting {
                    let _ = reply.send(Ok(built.clone()));
                }
            }
            Err(error) => {
                event!(Level::WARN, msg = "a snapshot could not be cut", group = %group, error);
                for reply in waiting {
                    let _ = reply.send(Err(error.clone()));
                }
            }
        }
        self.sweep_retired_snapshots().await;
        Ok(())
    }

    /// Delete every retired snapshot file no transfer holds any more
    pub(super) async fn sweep_retired_snapshots(&mut self) {
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let mut gone = Vec::new();
        for slot in replication.groups.values_mut() {
            // a strong count of one is this registry's own handle
            let (free, held): (Vec<_>, Vec<_>) = slot.retired.drain(..).partition(|built| Rc::strong_count(built) == 1);
            slot.retired = held;
            gone.extend(free);
        }
        for built in gone {
            if let Err(error) = glommio::io::remove(&built.path).await {
                event!(Level::DEBUG, msg = "a retired snapshot file could not be removed", path = %built.path.display(), ?error);
            }
        }
    }

    /// Stop every group, on a task that posts back once they are down
    ///
    /// Parked batches are dropped first, so a state machine waiting on one returns and its
    /// group can stop.
    pub(super) fn stop_groups(&mut self) -> bool {
        let Some(replication) = self.replication.as_mut() else {
            return false;
        };
        replication.stopping = true;
        replication.parked.clear();
        let rafts: Vec<Raft<DataConfig, GroupMachine<D>>> = replication
            .groups
            .values_mut()
            .filter_map(|slot| slot.raft.take())
            .collect();
        let tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            for raft in rafts {
                let _ = raft.shutdown().await;
            }
            let _ = tx.send(ServerMsg::GroupsDown).await;
        })
        .detach();
        true
    }

    /// Close the WAL once the groups are down
    pub(super) async fn close_replication(&mut self) {
        if let Some(replication) = self.replication.as_mut() {
            if let Err(error) = replication.wal.close().await {
                event!(Level::WARN, msg = "the wal did not close cleanly", ?error);
            }
        }
    }
}

/// Write a volatile group's snapshot from its resident partitions, on a task of its own
///
/// # Arguments
///
/// * `dir` - The directory the file goes in
/// * `group` - The group
/// * `table` - Its table
/// * `schema_id` - The schema's fingerprint
/// * `boundary` - The applied position the rows are cut at
/// * `membership` - The membership as of it
/// * `tablets` - The tablets the group serves
/// * `records` - Every resident partition of those tablets, as its key and archived bytes
/// * `remembered` - The remembered requests at or below the boundary, oldest first
#[allow(clippy::too_many_arguments)]
async fn write_volatile_snapshot(
    dir: &std::path::Path,
    group: GroupId,
    table: TableId,
    schema_id: u64,
    boundary: crate::server::wal::WalLogId,
    membership: openraft::type_config::alias::StoredMembershipOf<DataConfig>,
    tablets: Vec<u16>,
    records: Vec<(u64, Vec<u8>)>,
    remembered: Vec<(RequestId, Remembered)>,
) -> std::io::Result<(PathBuf, SnapshotManifest)> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(snapshot::snapshot_name(group, boundary.index));
    let header = SnapshotHeader {
        table,
        group,
        boundary: boundary.index,
        records: records.len() as u64,
    };
    let mut writer = SnapshotWriter::create(&path, header).await?;
    for (key, bytes) in &records {
        writer.record(*key, bytes).await?;
    }
    let (total, checksum) = writer.finish(&remembered).await?;
    snapshot::sync_dir(dir).await?;
    Ok((
        path,
        SnapshotManifest {
            group,
            table,
            schema_id,
            boundary,
            membership,
            tablets,
            records: records.len() as u64,
            total,
            checksum,
            retries: u32::try_from(remembered.len()).unwrap_or(u32::MAX),
        },
    ))
}

/// The openraft configuration a group runs under
///
/// The heartbeat is a tenth of the failover base and the election timeout is one to two of
/// it, so the policy every node agreed about is what decides how fast a leader is missed. A
/// volatile group's members lose their log on every restart by design, so its leader is told
/// to take a follower that comes back empty as a follower to feed from the start rather than
/// as the bug openraft otherwise stops on; a durable group's log survives, and a member of one
/// that comes back short has lost what it acknowledged, which the leader refuses to paper over.
///
/// # Arguments
///
/// * `cluster` - The cluster block
/// * `failover_ms` - The failover base, in milliseconds: the map's, or the block's until a map carries one
/// * `group` - The group
fn group_config(cluster: &crate::server::conf::Cluster, failover_ms: u64, group: GroupId) -> Arc<Config> {
    let base = failover_ms.max(100);
    let config = Config {
        cluster_name: format!("group-{group}"),
        heartbeat_interval: (base / 10).max(10),
        election_timeout_min: base,
        election_timeout_max: base * 2,
        enable_leader_restore: Some(false),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(cluster.replication.checkpoint_entries),
        max_in_snapshot_log_to_keep: cluster.replication.retained_entries,
        // a whole transfer's budget: openraft's default is a fifth of a second, which no
        // snapshot of any size completes in ([F43](../../../../docs/src/features/node-recovery.md))
        // truncation cannot happen: a transfer's budget is minutes, not weeks
        #[allow(clippy::cast_possible_truncation)]
        install_snapshot_timeout: cluster.replication.snapshot_timeout.duration().as_millis() as u64,
        // a follower whose log is shorter than what it acknowledged lost its disk: the
        // follower is the one that is wrong, and the leader resets its progress and feeds it
        // again - from the log, or past the purge point from a snapshot - rather than stopping
        // the process every group on this shard shares
        // ([Resolved #99](../../../../docs/src/appendix/resolved/durable-log-reversion.md)).
        // a volatile group's members lose their log on every restart by design
        allow_log_reversion: Some(true),
        ..Config::default()
    };
    // the defaults validate, and every field set above is within what validate accepts
    Arc::new(config.validate().unwrap_or_default())
}

/// The group a start outcome is for
///
/// # Arguments
///
/// * `outcome` - What starting the group came to
fn network_group<D: ShoalDatabase>(outcome: &Result<(GroupId, Raft<DataConfig, GroupMachine<D>>), (GroupId, String)>) -> GroupId {
    match outcome {
        Ok((group, _)) => *group,
        Err((group, _)) => *group,
    }
}

/// Build a group's handle, initialize it if it is fresh, and elect the primary
///
/// # Arguments
///
/// * `me` - This shard's address
/// * `spec` - The group
/// * `config` - The openraft configuration
/// * `network` - The group's network
/// * `store` - Its log store
/// * `machine` - Its state machine
/// * `primary` - Whether this shard is the placement primary, which elects first
async fn start_group<D: ShoalDatabase>(
    me: ShardAddr,
    spec: GroupSpec,
    config: Arc<Config>,
    network: GroupNetwork,
    store: GroupStore,
    machine: GroupMachine<D>,
    primary: bool,
) -> Result<(GroupId, Raft<DataConfig, GroupMachine<D>>), (GroupId, String)> {
    let group = spec.id;
    let raft = Raft::<DataConfig, GroupMachine<D>>::new(me, config, network, store, machine)
        .await
        .map_err(|error| (group, format!("building the group: {error}")))?;
    // a fresh group is initialized by its placement primary alone: openraft's initialize writes
    // a membership entry whose log id names the node that wrote it, so two members initializing
    // the same group would each hold a different entry at index zero and refuse each other's
    // votes until the greater address won. The others start with no membership at all, which
    // is what accepts the primary's first append; initialize elects, so the primary asks for the
    // lead at once and the timer retries until its peers have the group up
    let initialized = match raft.is_initialized().await {
        Ok(initialized) => initialized,
        Err(error) => return Err((group, format!("asking whether the group is initialized: {error}"))),
    };
    let members: BTreeMap<ShardAddr, ShardAddr> = spec.members.iter().map(|member| (*member, *member)).collect();
    if !initialized && (primary || spec.members.len() == 1) {
        if let Err(error) = raft.initialize(members.clone()).await {
            event!(Level::DEBUG, msg = "a group was initialized already", group = %group, %error);
        }
    }
    // the others grant the primary a head start: they do not stand for election themselves
    // until two election timeouts have passed, so the first leader of a healthy group is the
    // placement primary. After that any member may win, which is what a failover needs - and
    // a group the primary never brought up is initialized by whichever member notices first
    if spec.members.len() > 1 && !primary {
        raft.runtime_config().elect(false);
        let head_start = Duration::from_millis(raft.config().election_timeout_max * 2);
        let handle = raft.clone();
        glommio::spawn_local(async move {
            glommio::timer::sleep(head_start).await;
            if let Ok(false) = handle.is_initialized().await {
                event!(Level::WARN, msg = "a group's primary never initialized it; initializing", group = %group);
                if let Err(error) = handle.initialize(members).await {
                    event!(Level::DEBUG, msg = "a group was initialized already", group = %group, %error);
                }
            }
            handle.runtime_config().elect(true);
        })
        .detach();
    }
    Ok((group, raft))
}

/// Propose a command through a group, following the leader one hop if it is elsewhere
///
/// # Arguments
///
/// * `raft` - This shard's handle on the group, if it is up
/// * `network` - The shard's network, for the hop
/// * `group` - The group
/// * `me` - This shard's address
/// * `command` - The command
/// * `deadline` - How long to wait in all
/// * `may_hop` - Whether a leader elsewhere may be asked; a proposal that already hopped may not
/// * `all` - Whether every voter has to have the entry durable before it is answered
#[allow(clippy::too_many_arguments)]
async fn propose_through<D: ShoalDatabase>(
    raft: Option<&Raft<DataConfig, GroupMachine<D>>>,
    network: &ShardNetwork,
    group: GroupId,
    me: ShardAddr,
    command: Command,
    deadline: Duration,
    may_hop: bool,
    all: bool,
) -> ProposalOutcome {
    let Some(raft) = raft else {
        return ProposalOutcome::NotLeader(format!("group {group} is still starting on this shard"));
    };
    let started = Instant::now();
    let outcome = loop {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return ProposalOutcome::NotLeader(format!("no leader of group {group} took the write within the deadline"));
        }
        // a lease that lapsed is a definite refusal before anything is appended: openraft would
        // refuse the write with an empty hint, and waiting for "a leader" on a handle that
        // names itself is satisfied at once ([F42](../../../../docs/src/features/primary-failover.md))
        if Lease::of(raft, me) == Lease::Lapsed {
            return ProposalOutcome::NotLeader(format!(
                "the lease of {me} on group {group} lapsed: no quorum acknowledged it within {:?}",
                Lease::length(raft)
            ));
        }
        let written = glommio::timer::timeout(remaining, async { Ok(raft.client_write(command.clone()).await) }).await;
        match written {
            // the leader took it and did not commit it in time: it may yet
            Err(_) => return ProposalOutcome::Unknown(format!("group {group} did not commit the write within the deadline")),
            Ok(Ok(response)) => {
                let index = response.log_id.index;
                // `All` waits, after the commit, until every voter's matched index covers
                // the entry: a follower acknowledges an append only once its own sync
                // completed, so a matched index is a durable one
                // ([C5](../../../../docs/src/distributed/replication.md), "the quorum gate")
                if all {
                    let remaining = deadline.saturating_sub(started.elapsed());
                    let voters: Vec<ShardAddr> = raft.voter_ids().collect();
                    let covered = raft
                        .wait(Some(remaining))
                        .metrics(
                            |metrics| {
                                metrics.replication.as_ref().is_some_and(|matched| {
                                    voters.iter().all(|voter| {
                                        *voter == me
                                            || matched
                                                .get(voter)
                                                .and_then(|log_id| log_id.as_ref())
                                                .is_some_and(|log_id| log_id.index >= index)
                                    })
                                })
                            },
                            "every voter durable",
                        )
                        .await;
                    if covered.is_err() {
                        return ProposalOutcome::Unknown(format!(
                            "group {group} committed the write but not every voter made it durable within the deadline"
                        ));
                    }
                }
                break ProposalOutcome::Answered {
                    outcome: response.data,
                    index,
                };
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))) => {
                match forward.leader_node.or(forward.leader_id) {
                    // a hint naming this shard is a lease that has not started: wait for it
                    Some(leader) if leader == me => {
                        glommio::timer::sleep(LEASE_POLL).await;
                    }
                    // the leader is elsewhere: one hop, and no more
                    Some(leader) => {
                        if !may_hop {
                            return ProposalOutcome::NotLeader(format!("group {group} is led by {leader}, not here"));
                        }
                        let remaining = deadline.saturating_sub(started.elapsed());
                        let peer = ShardPeer::new(leader, network.clone());
                        break match peer.propose(group, command.encode(), remaining).await {
                            Ok(bytes) => postcard::from_bytes::<ProposalOutcome>(&bytes).unwrap_or_else(|error| {
                                ProposalOutcome::Failed(format!("decoding the leader's answer: {error}"))
                            }),
                            Err(RpcFailure::NotSent(msg)) => ProposalOutcome::NotLeader(msg),
                            Err(RpcFailure::Remote(msg)) => ProposalOutcome::Failed(msg),
                            Err(RpcFailure::Unreachable(msg)) => ProposalOutcome::Unknown(msg),
                        };
                    }
                    // an empty hint: this shard's own lease has not started or has lapsed, or
                    // nobody leads - judged from the handle rather than waited on, since a
                    // wait for "a leader" on a handle that names itself returns at once
                    None => match Lease::of(raft, me) {
                        Lease::Lapsed => {
                            return ProposalOutcome::NotLeader(format!(
                                "the lease of {me} on group {group} lapsed: no quorum acknowledged it within {:?}",
                                Lease::length(raft)
                            ));
                        }
                        Lease::NotStarted | Lease::Leads | Lease::Elsewhere(_) => glommio::timer::sleep(LEASE_POLL).await,
                        Lease::Electing => {
                            let remaining = deadline.saturating_sub(started.elapsed());
                            let elected = raft
                                .wait(Some(remaining))
                                .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
                                .await;
                            if elected.is_err() {
                                return ProposalOutcome::NotLeader(format!("group {group} elected no leader within the deadline"));
                            }
                        }
                    },
                }
            }
            Ok(Err(error)) => return ProposalOutcome::Failed(format!("writing to group {group}: {error}")),
        }
    };
    // a write is acknowledged once this shard's own replica has applied it, so a read that
    // follows on the node that accepted the write sees it; a leader has by the time it
    // answers, a follower once the commit reaches it, which is what the wait is for
    if let ProposalOutcome::Answered { index, .. } = &outcome {
        let remaining = deadline.saturating_sub(started.elapsed());
        let _ = raft
            .wait(Some(remaining))
            .applied_index_at_least(Some(*index), "the write applied on this replica")
            .await;
    }
    outcome
}

/// Encode an answer for the replication lane
///
/// # Arguments
///
/// * `id` - The request's correlation id
/// * `value` - The answer
fn encode_reply<T: serde::Serialize>(id: u64, value: &T) -> ReplicateReply {
    match postcard::to_allocvec(value) {
        Ok(bytes) => ReplicateReply::ok(id, bytes),
        Err(error) => ReplicateReply::error(id, format!("encoding an answer: {error}")),
    }
}

/// The client id a peer connection's proposal answers under, which is the correlation id
#[allow(dead_code)]
fn peer_client(id: u64) -> Uuid {
    Uuid::from_u64_pair(id, 0)
}
