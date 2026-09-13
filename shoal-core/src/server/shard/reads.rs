//! The read half of a shard: deadlines, plans, expiry, and the fixture's hold on shares
//!
//! What a bundle says about its reads is resolved here, once, on the coordinating shard, into
//! a [`ReadPlan`] every share carries; what waits too long is answered here, once, with
//! `Timeout`; and the fixture's `HoldShares` verb keeps a shard's shares back so a gather can be
//! seen to expire ([F41](../../../../docs/src/features/read-consistency.md)).
//!
//! # Invariants
//!
//! **A gather expires before its pendings do.** The tick sweeps gathers first, and an expired
//! gather forgets every pending forward for its query, so the forward sweep cannot answer a
//! query the gather already answered `Timeout`.
//!
//! **Held shares are released in the order they were held.** The fixture uses the hold to
//! make a share late; the order shares arrive in after the release still has to be one the
//! gather would have accepted, or the test would prove nothing about lateness.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use openraft::error::{LinearizableReadError, RaftError};
use openraft::raft::linearizable_read::Linearizer;
use openraft::{Raft, ReadPolicy};
use rkyv::util::AlignedVec;
use tracing::{event, Level, Span};
use uuid::Uuid;

use super::{Shard, ShardContact};
use crate::server::database::ShoalDatabase;
use crate::server::messages::{QueryMetadata, ReadPlan, ReadWaits, ReplyKind, ServerMsg};
use crate::server::replication::{
    BarrierAnswer, DataConfig, GroupMachine, Lease, ReadVerb, RpcFailure, ShardNetwork, ShardPeer,
};
use crate::server::ring::Ring;
use crate::server::routing::ArchivedShardRouting;
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::server::ServerError;
use crate::shared::identity::{GroupId, ShardAddr};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::read::{ReadLevel, ReadOptions, SessionToken};
use crate::shared::responses::ResponseError;
use crate::shared::traits::{QuerySupport, RkyvSupport, ShoalQuerySupport, TableNameSupport};

/// How long a barrier waits before asking again after a leader hint that has not started
const LEASE_POLL: Duration = Duration::from_millis(20);

/// A share a shard is holding back rather than sending, for the fixture
pub(super) enum HeldShare<D: ShoalDatabase> {
    /// A share bound for a shard on this node, over the mesh
    Local {
        /// The shard collecting it
        contact: ShardContact,
        /// The share's metadata
        meta: QueryMetadata,
        /// The share
        response: <D::ClientType as QuerySupport>::ResponseKinds,
        /// Whether the share is a failure, which fails its slot at once
        failed: bool,
    },
    /// A share bound for the node that forwarded the query, down its peer connection
    Remote {
        /// The peer connection
        client: Uuid,
        /// The bundle
        id: Uuid,
        /// The index
        index: usize,
        /// Whether the query was the last of its stream
        end: bool,
        /// The span to reply under
        span: Span,
        /// The share's stamps
        stamps: StageStamps,
        /// The share, sealed
        archived: AlignedVec<16>,
        /// The attempt and slot the answer head echoes
        route: (u64, u16),
    },
}

/// The shares a shard is holding back, and how to let them go
pub(super) struct HeldShares<D: ShoalDatabase> {
    /// Whether each share is sent twice when released
    dup: bool,
    /// The shares, in the order they were held
    shares: Vec<HeldShare<D>>,
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// When a bundle stops waiting: the server's budget, or a shorter one the bundle named
    ///
    /// Measured from when the bundle's last byte came off the socket, so the time a bundle
    /// spent queued on the coordinator counts against it. A bundle never gets a longer budget
    /// than the server's.
    ///
    /// # Arguments
    ///
    /// * `base` - When the bundle arrived
    /// * `options` - What the bundle said, if anything
    pub(super) fn bundle_deadline(&self, base: Stamp, options: Option<&ReadOptions>) -> Stamp {
        // the server's budget, in nanoseconds
        let server = self.conf.networking.query_deadline.duration();
        let server_ns = server.as_nanos().min(u128::from(u64::MAX)) as u64;
        // the bundle's, if it named one and it is shorter
        let budget_ns = match options.map(|options| options.deadline_ms).filter(|ms| *ms > 0) {
            Some(ms) => server_ns.min(u64::from(ms) * 1_000_000),
            None => server_ns,
        };
        base.plus_nanos(budget_ns)
    }

    /// How a query's shares are served as reads
    ///
    /// The level is the bundle's override if it gave one, else the table's policy, else the
    /// cluster's default; a standalone node serves everything at `One`. The tokens are the
    /// bundle's for this query's table. Resolution happens once, here, and the plan travels
    /// resolved ([F41](../../../../docs/src/features/read-consistency.md)).
    ///
    /// # Arguments
    ///
    /// * `table` - The table the query names
    /// * `kind` - The query, as archived
    /// * `deadline` - When the bundle stops waiting
    /// * `attempt` - This attempt at the bundle
    /// * `options` - What the bundle said, if anything
    ///
    /// # Errors
    ///
    /// A token from another cluster, or any token on a standalone node, refuses the read by
    /// name rather than being ignored: a lower bound from another history bounds nothing here.
    pub(super) fn read_plan(
        &self,
        table: D::TableNames,
        kind: &<<D::ClientType as QuerySupport>::QueryKinds as rkyv::Archive>::Archived,
        deadline: Stamp,
        attempt: u64,
        options: Option<&ReadOptions>,
    ) -> Result<ReadPlan, ResponseError> {
        // a write waits on nothing a read does; the deadline and the attempt are still its
        let is_write = <<D::ClientType as QuerySupport>::QueryKinds as ArchivedShardRouting>::archived_is_write(kind);
        if is_write {
            return Ok(ReadPlan {
                attempt,
                ..ReadPlan::one(deadline)
            });
        }
        // the tokens bounding this query's table
        let table_id = table.table_id();
        let tokens: Vec<SessionToken> = options
            .map(|options| {
                options
                    .tokens
                    .iter()
                    .filter(|token| token.table == table_id)
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        // a standalone node is in no cluster, so a token names a history it does not have
        if self.peer_setup.is_none() {
            if !tokens.is_empty() {
                return Err(ResponseError::new(
                    ErrorCode::WrongCluster,
                    "a session token was sent to a node that is in no cluster",
                ));
            }
            return Ok(ReadPlan {
                attempt,
                ..ReadPlan::one(deadline)
            });
        }
        // and a token from another cluster names a history this one does not have
        let cluster = self.map.get().cluster.unwrap_or_default();
        if let Some(foreign) = tokens.iter().find(|token| token.cluster != cluster) {
            return Err(ResponseError::new(
                ErrorCode::WrongCluster,
                format!("a session token names cluster {}, and this node is in {cluster}", foreign.cluster),
            ));
        }
        // the level: the bundle's, then the table's, then the cluster's
        let level = match options.and_then(|options| options.level) {
            Some(level) => level,
            None => self.policy_level(table),
        };
        Ok(ReadPlan {
            level,
            deadline,
            tokens: Rc::from(tokens),
            slot: 0,
            attempt,
            ready: false,
        })
    }

    /// Wait for a read's barrier and its lower bounds on a task, then run it on the loop
    ///
    /// The task holds `Raft` and network handles alone and never the loop's state: the loop
    /// never awaits a `Raft` method ([F40](../../../../docs/src/features/replication.md)), and
    /// a group's machine state is loop-side only. The outcome comes back as
    /// [`ServerMsg::ReadReady`] with the plan marked ready, so a read parked on a disk load
    /// afterwards never waits twice ([F41](../../../../docs/src/features/read-consistency.md)).
    ///
    /// # Arguments
    ///
    /// * `meta` - The read's metadata
    /// * `query` - The read
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share of a split query
    pub(super) fn await_read_barrier(
        &mut self,
        mut meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: Span,
        gathered_meta: Option<QueryMetadata>,
    ) -> Result<(), ServerError> {
        // whatever happens below, this read waits once
        meta.read.ready = true;
        let tx = self.shard_local_tx.clone();
        // the groups the read's tablets are served by on this shard, with the lowest index
        // each has to have applied before the read is served past its tokens
        let Some(replication) = self.replication.as_ref() else {
            let outcome = Err(ResponseError::new(ErrorCode::Unavailable, "this node hosts no tablet groups"));
            return self.post_read_ready(meta, query, span, gathered_meta, outcome);
        };
        let table = D::ClientType::query_table_name(&query);
        let table_id = table.table_id();
        let mut groups: BTreeMap<GroupId, u64> = BTreeMap::new();
        for key in query.partition_keys() {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = Ring::tablet_of(*key) as u16;
            let Some(group) = replication.tablets.get(&(table_id, tablet)).copied() else {
                let outcome = Err(ResponseError::new(
                    ErrorCode::Unavailable,
                    format!("no group serves tablet {tablet} of {table} on this shard"),
                ));
                return self.post_read_ready(meta, query, span, gathered_meta, outcome);
            };
            groups.entry(group).or_insert(0);
        }
        // every token has to name the lineage this shard holds for its tablet
        for token in meta.read.tokens.iter() {
            match replication.tablets.get(&(table_id, token.tablet)) {
                Some(group) if *group == token.group => {
                    let bound = groups.entry(token.group).or_insert(0);
                    *bound = (*bound).max(token.index);
                }
                Some(group) => {
                    self.read_stats.lineage_refusals += 1;
                    let outcome = Err(ResponseError::new(
                        ErrorCode::UnknownLineage,
                        format!(
                            "the session token names group {} for tablet {} of {table}, which group {group} serves here",
                            token.group, token.tablet
                        ),
                    ));
                    return self.post_read_ready(meta, query, span, gathered_meta, outcome);
                }
                // a token for a tablet this read does not name still has to be honest
                None => {
                    self.read_stats.lineage_refusals += 1;
                    let outcome = Err(ResponseError::new(
                        ErrorCode::UnknownLineage,
                        format!("the session token names tablet {} of {table}, which no group serves here", token.tablet),
                    ));
                    return self.post_read_ready(meta, query, span, gathered_meta, outcome);
                }
            }
        }
        // the handles the task needs: one raft per group, and the network for a hop
        let mut rafts = Vec::with_capacity(groups.len());
        for (group, bound) in groups {
            let Some(raft) = replication.groups.get(&group).and_then(|slot| slot.raft.clone()) else {
                let outcome = Err(ResponseError::new(ErrorCode::Unavailable, format!("group {group} is still starting")));
                return self.post_read_ready(meta, query, span, gathered_meta, outcome);
            };
            rafts.push((group, raft, bound));
        }
        let network = replication.network.clone();
        let me = self.my_addr();
        let level = meta.read.level;
        let session = !meta.read.tokens.is_empty();
        let deadline = meta.read.deadline;
        glommio::spawn_local(async move {
            // every group in turn: no cross-tablet snapshot is promised, so nothing is gained
            // by joining them, and one task per read keeps the executor's queues short
            let mut waits = ReadWaits {
                session,
                ..ReadWaits::default()
            };
            let mut outcome = Ok(());
            for (group, raft, bound) in rafts {
                match wait_on_group(&raft, &network, group, me, level, bound, deadline, &mut waits).await {
                    Ok(()) => {}
                    Err(error) => {
                        outcome = Err(error);
                        break;
                    }
                }
            }
            let _ = tx
                .send(ServerMsg::ReadReady {
                    meta,
                    query,
                    span,
                    gathered_meta,
                    outcome: outcome.map(|()| waits),
                })
                .await;
        })
        .detach();
        Ok(())
    }

    /// Post a read's outcome to the loop without a task, for a refusal decided on the spot
    ///
    /// # Arguments
    ///
    /// * `meta` - The read's metadata
    /// * `query` - The read
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share
    /// * `outcome` - Why the read cannot be served, or what it waited
    fn post_read_ready(
        &mut self,
        meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: Span,
        gathered_meta: Option<QueryMetadata>,
        outcome: Result<ReadWaits, ResponseError>,
    ) -> Result<(), ServerError> {
        let tx = self.shard_local_tx.clone();
        glommio::spawn_local(async move {
            let _ = tx
                .send(ServerMsg::ReadReady {
                    meta,
                    query,
                    span,
                    gathered_meta,
                    outcome,
                })
                .await;
        })
        .detach();
        Ok(())
    }

    /// Run a read whose waits are done, or answer why it cannot be served
    ///
    /// A refusal is answered exactly where the rows would have been - as a share to the shard
    /// collecting them, or whole to the client - in the query's own table variant. It does not
    /// ride the metadata's carried failure: that path swaps a failure into an *open* answer, and
    /// a get whose partitions are resident is sealed in place and never open.
    ///
    /// # Arguments
    ///
    /// * `meta` - The read's metadata, its plan marked ready
    /// * `query` - The read
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share
    /// * `outcome` - What the waits came to
    pub(super) async fn handle_read_ready(
        &mut self,
        mut meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: Span,
        mut gathered_meta: Option<QueryMetadata>,
        outcome: Result<ReadWaits, ResponseError>,
    ) -> Result<(), ServerError> {
        // the read waited once, whichever way it went
        meta.read.ready = true;
        if let Some(gathered) = gathered_meta.as_mut() {
            gathered.read.ready = true;
        }
        match outcome {
            Ok(waits) => {
                // what the waits cost, on the shard and on the read's own record
                for _ in 0..waits.barriers {
                    self.read_stats.barriers += 1;
                }
                self.read_stats.barrier_hops += waits.barrier_hops;
                self.read_stats.barrier_wait_ns_total += waits.barrier_ns;
                self.read_stats.barrier_wait_ns_max = self.read_stats.barrier_wait_ns_max.max(waits.barrier_ns);
                self.read_stats.record_apply_wait(waits.apply_ns);
                if waits.session {
                    self.read_stats.session_waits += 1;
                }
                meta.stamps.set_read_waits(waits.barrier_ns, waits.apply_ns);
            }
            Err(error) => {
                // a timeout is counted with the gathers that expire; the failure is answered
                // where the rows would have been, and the read never runs
                if error.code() == ErrorCode::Timeout {
                    self.read_stats.timeouts += 1;
                }
                return self.answer_read_failure(meta, query, span, gathered_meta, error).await;
            }
        }
        // and the read runs, with nothing left to wait on
        self.execute_query(meta, query, span, gathered_meta).await
    }

    /// Answer a read that cannot be served, where its rows would have gone
    ///
    /// # Arguments
    ///
    /// * `meta` - The read's metadata
    /// * `query` - The read, for the table it names
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share
    /// * `error` - Why it cannot be served
    async fn answer_read_failure(
        &mut self,
        mut meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: Span,
        gathered_meta: Option<QueryMetadata>,
        error: ResponseError,
    ) -> Result<(), ServerError> {
        let table = D::ClientType::query_table_name(&query);
        let response = <D::ClientType as QuerySupport>::failed(table, meta.id, meta.index, meta.end, error);
        meta.stamps.mark_exec_done();
        // a share of a split query fails its slot on the shard collecting it
        let Some(gathered_meta) = gathered_meta else {
            return self.reply(meta.client, meta.id, span, meta.stamps, response).await;
        };
        let contact = gathered_meta
            .gather
            .clone()
            .expect("A gathered query always names the shard collecting it");
        let share = if contact.remote_node().is_some() {
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&response)?;
            meta.stamps.mark_replied();
            HeldShare::Remote {
                client: gathered_meta.client,
                id: gathered_meta.id,
                index: gathered_meta.index,
                end: gathered_meta.end,
                span,
                stamps: meta.stamps,
                archived,
                route: (gathered_meta.read.attempt, gathered_meta.read.slot),
            }
        } else {
            HeldShare::Local {
                contact,
                meta: gathered_meta,
                response,
                failed: true,
            }
        };
        self.send_share(share).await
    }

    /// The level a table's reads are served at when a bundle does not say
    ///
    /// The table's own policy if the control plane set one, else the cluster's default.
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    fn policy_level(&self, table: D::TableNames) -> ReadLevel {
        // the table's own level from the committed state, else the cluster's default
        match self.map.get().read_level_of(table.table_id()) {
            crate::server::conf::cluster::Consistency::Quorum | crate::server::conf::cluster::Consistency::All => {
                ReadLevel::Quorum
            }
            crate::server::conf::cluster::Consistency::One => ReadLevel::One,
        }
    }

    /// Send one share to the shard collecting it, or hold it if the fixture asked
    ///
    /// # Arguments
    ///
    /// * `share` - The share
    pub(super) async fn send_share(&mut self, share: HeldShare<D>) -> Result<(), ServerError> {
        // a hold in force keeps the share, in order, until it is released
        if let Some(held) = self.held.as_mut() {
            held.shares.push(share);
            return Ok(());
        }
        match share {
            // over the mesh to the shard collecting it
            HeldShare::Local {
                contact,
                meta,
                response,
                failed,
            } => {
                let msg = ServerMsg::Gathered { meta, response, failed };
                self.comms.send(&contact, msg).await
            }
            // down the peer connection the query came in on
            HeldShare::Remote {
                client,
                id,
                index,
                end,
                span,
                stamps,
                archived,
                route,
            } => {
                self.reply_sealed(client, id, index, end, ReplyKind::Share, span, stamps, archived, None, route)
                    .await
            }
        }
    }

    /// Let go of every held share, in the order they were held, twice each if asked
    pub(super) async fn release_held(&mut self) -> Result<(), ServerError> {
        // take the hold off first, so sending does not put the shares straight back
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        event!(Level::INFO, msg = "releasing held shares", shares = held.shares.len(), dup = held.dup);
        for share in held.shares {
            // a duplicate is the same share sent twice: a local one goes through its bytes
            // and back, since a response is not otherwise cloneable, a remote one is
            // re-sealed from the same bytes
            if held.dup {
                let again = match &share {
                    HeldShare::Local {
                        contact,
                        meta,
                        response,
                        failed,
                    } => {
                        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(response)?;
                        let copy = <<D::ClientType as QuerySupport>::ResponseKinds as RkyvSupport>::deserialize(
                            <<D::ClientType as QuerySupport>::ResponseKinds as RkyvSupport>::access(&bytes)?,
                        )?;
                        HeldShare::Local {
                            contact: contact.clone(),
                            meta: meta.clone(),
                            response: copy,
                            failed: *failed,
                        }
                    }
                    HeldShare::Remote {
                        client,
                        id,
                        index,
                        end,
                        span,
                        stamps,
                        archived,
                        route,
                    } => {
                        let mut copy = AlignedVec::<16>::with_capacity(archived.len());
                        copy.extend_from_slice(archived);
                        HeldShare::Remote {
                            client: *client,
                            id: *id,
                            index: *index,
                            end: *end,
                            span: span.clone(),
                            stamps: *stamps,
                            archived: copy,
                            route: *route,
                        }
                    }
                };
                self.send_share(again).await?;
            }
            self.send_share(share).await?;
        }
        Ok(())
    }

    /// Drive a read verb, for the fixture
    ///
    /// # Arguments
    ///
    /// * `verb` - What to do
    pub(super) fn handle_read_verb(&mut self, verb: ReadVerb) -> Result<serde_json::Value, String> {
        match verb {
            // hold every share for a while, then post the release to the loop
            ReadVerb::HoldShares { ms, dup } => {
                self.held = Some(HeldShares { dup, shares: Vec::new() });
                let tx = self.shard_local_tx.clone();
                glommio::spawn_local(async move {
                    glommio::timer::sleep(std::time::Duration::from_millis(ms)).await;
                    let _ = tx.send(ServerMsg::ReleaseHeld).await;
                })
                .detach();
                Ok(serde_json::json!({ "holding": true, "ms": ms, "dup": dup }))
            }
            // the loop releases on the next message it handles
            ReadVerb::ReleaseShares => {
                let held = self.held.as_ref().map_or(0, |held| held.shares.len());
                let tx = self.shard_local_tx.clone();
                glommio::spawn_local(async move {
                    let _ = tx.send(ServerMsg::ReleaseHeld).await;
                })
                .detach();
                Ok(serde_json::json!({ "releasing": held }))
            }
            // what is resident, and what the reads have cost and dropped
            ReadVerb::Gathers => Ok(serde_json::json!({
                "resident": self.gathering.len(),
                "held": self.held.as_ref().map_or(0, |held| held.shares.len()),
                "stats": serde_json::to_value(self.read_stats).unwrap_or_default(),
            })),
            // block this shard's executor: every group on it falls silent while the control
            // thread keeps reporting. The reply goes out first, on the loop's next yield
            ReadVerb::StallShard { ms } => {
                glommio::spawn_local(async move {
                    glommio::timer::sleep(std::time::Duration::from_millis(50)).await;
                    event!(Level::WARN, msg = "stalling this shard's executor, as the fixture asked", ms);
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                    event!(Level::WARN, msg = "the shard's executor is running again", ms);
                })
                .detach();
                Ok(serde_json::json!({ "stalling": true, "ms": ms }))
            }
        }
    }

    /// Answer every gather whose deadline has passed, once each
    ///
    /// The answer is `Timeout` in the query's own table variant, saying how many of its shares
    /// arrived; the pending forwards for it are forgotten so the forward sweep cannot answer
    /// it a second time ([Resolved #33](../../../../docs/src/appendix/resolved/gather-expiry.md)).
    pub(super) async fn sweep_gathers(&mut self) -> Result<(), ServerError> {
        let expired = self.gathering.expire(Stamp::now());
        for ((bundle, index), gather) in expired {
            self.read_stats.timeouts += 1;
            // nothing owed for this query is waited on any more
            if let Some(peers) = self.peers.as_mut() {
                peers.forget(bundle, index as u64);
            }
            event!(
                Level::WARN,
                msg = "a split query did not complete within its deadline",
                id = %bundle,
                index,
                arrived = gather.arrived(),
                slots = gather.slots.len(),
            );
            let error = ResponseError::new(
                ErrorCode::Timeout,
                format!(
                    "the query did not complete within its deadline; {} of {} shares arrived",
                    gather.arrived(),
                    gather.slots.len()
                ),
            );
            let response = <D::ClientType as QuerySupport>::failed(gather.table, bundle, index, gather.end, error);
            let mut stamps = gather.stamps;
            stamps.mark_exec_done();
            self.reply(gather.client, bundle, gather.span, stamps, response).await?;
        }
        Ok(())
    }
}

/// Wait on one group for a read: its barrier under `Quorum`, and its lower bound under a token
///
/// # Arguments
///
/// * `raft` - This shard's handle on the group
/// * `network` - The shard's network, for a barrier asked of a leader elsewhere
/// * `group` - The group
/// * `me` - This shard's address
/// * `level` - The read's level
/// * `bound` - The lowest index a token asks this replica to have applied, or zero
/// * `deadline` - When the read stops waiting
/// * `waits` - Where what the waits cost is added up
#[allow(clippy::too_many_arguments)]
async fn wait_on_group<D: ShoalDatabase>(
    raft: &Raft<DataConfig, GroupMachine<D>>,
    network: &ShardNetwork,
    group: GroupId,
    me: ShardAddr,
    level: ReadLevel,
    bound: u64,
    deadline: Stamp,
    waits: &mut ReadWaits,
) -> Result<(), ResponseError> {
    // the index this replica has to have applied before the read is served
    let mut need = bound;
    // a strong read establishes current authority first: a barrier from the group's leader
    if level == ReadLevel::Quorum {
        let started = Instant::now();
        let (index, hopped) = read_barrier(raft, network, group, me, deadline).await?;
        let barrier_ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        waits.barriers += 1;
        if hopped {
            waits.barrier_hops += 1;
        }
        waits.barrier_ns += barrier_ns;
        need = need.max(index);
    }
    // then applies through it, and past every token, before anything is read
    if need > 0 {
        let started = Instant::now();
        let left = remaining(deadline);
        if left.is_zero() {
            return Err(timeout(group, need));
        }
        let applied = raft
            .wait(Some(left))
            .applied_index_at_least(Some(need), "the read's lower bound applied on this replica")
            .await;
        waits.apply_ns += started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        if applied.is_err() {
            return Err(timeout(group, need));
        }
    }
    Ok(())
}

/// Obtain a read barrier for one group: from this shard if it leads, else from the leader
///
/// Returns the index the read has to apply through, and whether a hop was needed.
///
/// # Arguments
///
/// * `raft` - This shard's handle on the group
/// * `network` - The shard's network
/// * `group` - The group
/// * `me` - This shard's address
/// * `deadline` - When the read stops waiting
async fn read_barrier<D: ShoalDatabase>(
    raft: &Raft<DataConfig, GroupMachine<D>>,
    network: &ShardNetwork,
    group: GroupId,
    me: ShardAddr,
    deadline: Stamp,
) -> Result<(u64, bool), ResponseError> {
    let mut hopped = false;
    // a leader another member named, asked directly on the next turn rather than through
    // this shard's own handle, which may not have heard of the election yet
    let mut redirect: Option<ShardAddr> = None;
    loop {
        let left = remaining(deadline);
        if left.is_zero() {
            return Err(ResponseError::new(
                ErrorCode::Timeout,
                format!("no leader of group {group} confirmed a read barrier within the deadline"),
            ));
        }
        let hint = match redirect.take() {
            // follow the hint this shard was given
            Some(leader) => Some(leader),
            None => {
                // a lease that lapsed cannot complete a heartbeat round: answered by name rather
                // than waited out ([F42](../../../../docs/src/features/primary-failover.md))
                if Lease::of(raft, me) == Lease::Lapsed {
                    return Err(ResponseError::new(
                        ErrorCode::QuorumUnavailable,
                        format!(
                            "the lease of {me} on group {group} lapsed: no quorum acknowledged it within {:?}",
                            Lease::length(raft)
                        ),
                    ));
                }
                // ask this shard's own handle first: if it leads, the heartbeat round is its own
                let asked = glommio::timer::timeout(left, async { Ok(raft.get_read_linearizer(ReadPolicy::ReadIndex).await) }).await;
                match asked {
                    Err(_) => {
                        return Err(ResponseError::new(
                            ErrorCode::Timeout,
                            format!("group {group} did not confirm a read barrier within the deadline"),
                        ))
                    }
                    Ok(Ok(linearizer)) => return Ok((linearizer.read_log_id().index(), hopped)),
                    Ok(Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward)))) => {
                        forward.leader_node.or(forward.leader_id)
                    }
                    Ok(Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(short)))) => {
                        return Err(ResponseError::new(
                            ErrorCode::QuorumUnavailable,
                            format!("group {group} could not confirm its leader: {short}"),
                        ))
                    }
                    Ok(Err(RaftError::Fatal(fatal))) => {
                        return Err(ResponseError::new(ErrorCode::Unavailable, format!("group {group} is stopped: {fatal}")))
                    }
                }
            }
        };
        match hint {
            // a hint naming this shard is a lease that has not started: wait for it
            Some(leader) if leader == me => glommio::timer::sleep(LEASE_POLL).await,
            // the leader is elsewhere: ask it for its read log id, and apply through it here
            Some(leader) => {
                hopped = true;
                let left = remaining(deadline);
                let peer = ShardPeer::new(leader, network.clone());
                match peer.read_barrier(group, left).await {
                    Ok(bytes) => match postcard::from_bytes::<BarrierAnswer>(&bytes) {
                        Ok(BarrierAnswer::Ready(read_log_id)) => {
                            // rebuild the linearizer over the leader's id, so the wait is the
                            // library's own rule for a follower read
                            let linearizer = Linearizer::<DataConfig>::new(me, read_log_id, None);
                            return Ok((linearizer.read_log_id().index(), hopped));
                        }
                        // the member we asked does not lead either, and names who does: ask
                        // that member next, after a pause so a group mid-election is not hammered
                        Ok(BarrierAnswer::NotLeader(Some(next))) if next != me => {
                            redirect = Some(next);
                            glommio::timer::sleep(LEASE_POLL).await;
                        }
                        // it names nobody, or this shard: ask this shard's own handle again
                        Ok(BarrierAnswer::NotLeader(_)) => glommio::timer::sleep(LEASE_POLL).await,
                        Ok(BarrierAnswer::NoQuorum(msg)) => {
                            return Err(ResponseError::new(
                                ErrorCode::QuorumUnavailable,
                                format!("the leader of group {group} could not confirm its term: {msg}"),
                            ))
                        }
                        Err(error) => {
                            return Err(ResponseError::new(
                                ErrorCode::Unavailable,
                                format!("decoding the leader's barrier for group {group}: {error}"),
                            ))
                        }
                    },
                    Err(RpcFailure::Unreachable(msg)) => {
                        return Err(ResponseError::new(
                            ErrorCode::Timeout,
                            format!("the leader of group {group} did not answer a read barrier: {msg}"),
                        ))
                    }
                    // a link that is down may come back within the budget, so this is a
                    // pause and another try rather than an answer; the deadline is what ends it
                    Err(RpcFailure::NotSent(_)) => glommio::timer::sleep(LEASE_POLL).await,
                    Err(RpcFailure::Remote(msg)) => {
                        return Err(ResponseError::new(
                            ErrorCode::Unavailable,
                            format!("the leader of group {group} could not be asked for a read barrier: {msg}"),
                        ))
                    }
                }
            }
            // an empty hint: this shard's own lease has not started, or nobody leads - judged
            // from the handle, since a wait for "a leader" on a handle that names itself
            // returns at once
            None => match Lease::of(raft, me) {
                Lease::Lapsed => {
                    return Err(ResponseError::new(
                        ErrorCode::QuorumUnavailable,
                        format!(
                            "the lease of {me} on group {group} lapsed: no quorum acknowledged it within {:?}",
                            Lease::length(raft)
                        ),
                    ))
                }
                Lease::NotStarted | Lease::Leads | Lease::Elsewhere(_) => glommio::timer::sleep(LEASE_POLL).await,
                Lease::Electing => {
                    let left = remaining(deadline);
                    let elected = raft
                        .wait(Some(left))
                        .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
                        .await;
                    if elected.is_err() {
                        return Err(ResponseError::new(
                            ErrorCode::Timeout,
                            format!("group {group} elected no leader within the deadline"),
                        ));
                    }
                }
            },
        }
    }
}

/// How long is left before a deadline, or nothing
///
/// # Arguments
///
/// * `deadline` - The deadline
fn remaining(deadline: Stamp) -> Duration {
    Duration::from_nanos(deadline.since(Stamp::now()))
}

/// The failure a read answers when its replica did not apply far enough in time
///
/// # Arguments
///
/// * `group` - The group
/// * `need` - The index it had to reach
fn timeout(group: GroupId, need: u64) -> ResponseError {
    ResponseError::new(
        ErrorCode::Timeout,
        format!("this replica of group {group} did not apply index {need} within the deadline"),
    )
}
