//! A tablet group's network: `RaftNetworkV2` over the replication lane
//!
//! Every shard owns one [`ReplicationLink`] per peer node it hosts a group with, dialled
//! lazily on the first RPC, and every group's [`ShardPeer`] to a member on that node shares
//! it ([F40](../../../../docs/src/features/replication.md)). The lane is the data port's
//! third lane, served by every shard's peer listener beside data and bulk, and a frame on it
//! names the group and the shard on the peer that hosts it so the listener can hand it to
//! that shard without decoding the body. The body is openraft's own RPC type in `postcard`,
//! with the command's bytes inline - the one place the workspace serializes with it, chosen
//! over the control lane's JSON because a command's payload would otherwise be an array of
//! numbers on the write path.
//!
//! Everything here runs on the shard's executor and is `Rc`/`RefCell` by construction, the way
//! the control network is on the control thread. A link's answers arrive on the shard's own
//! mesh channel as [`PeerEvent::Link`](crate::server::messages::PeerEvent) frames, and the
//! shard hands a `ReplicateResponse` to [`ShardNetwork::answered`], which completes the RPC
//! waiting under its correlation id.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use glommio::io::BufferedFile;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use tracing::{event, Level};
use uuid::Uuid;

use super::machine::SnapshotData;
use super::report::SnapshotStats;
use super::snapshot::{BuiltSnapshot, BulkRoute, SnapshotAnswer, SnapshotManifest, SnapshotRpc};
use super::types::DataConfig;
use crate::server::conf::cluster::{DialOverride, Replication, Transport};
use crate::server::map::MapCell;
use crate::server::peer::handshake::PeerAddr;
use crate::server::peer::{self, Frame, FrameKey, Lane, LinkEvent, LinkView, Local};
use crate::shared::identity::{GroupId, NodeId, ShardAddr};
use crate::shared::protocol::peer::{
    checksum, ReplicateKind, ReplicateRequestHead, ReplicateResponseHead, ReplicateStatus,
    SnapshotBegin, SnapshotChunk, SnapshotEnd, SnapshotStatus, CAP_PRE_VOTE_V1,
    REPLICATE_RESPONSE_HEAD_LEN,
};
use crate::shared::protocol::MessageType;
use crate::shared::tls::PeerTlsHolder;

/// How long a sender waits before offering a shed chunk to the bulk queue again
const SNAPSHOT_SHED_BACKOFF: Duration = Duration::from_millis(10);

/// How long a sender waits before sending a snapshot RPC again after its link dropped
const SNAPSHOT_RPC_RETRY: Duration = Duration::from_millis(100);

/// What a group's network asks the shard loop for a snapshot file with
pub type SnapshotBuilder =
    Rc<dyn Fn(GroupId, u64, oneshot::Sender<Result<Rc<BuiltSnapshot>, String>>)>;

/// What a replication RPC's answer resolves to
enum Outcome {
    /// The peer answered, and this is the response payload
    Ok(Vec<u8>),
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The link went down before the request was written: a definite non-answer
    NotSent(String),
    /// The link went down after the request was written, before an answer arrived
    Unreachable(String),
}

/// Why a replication RPC produced no answer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcFailure {
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The request was never written: the queue was full or the link was down
    ///
    /// A definite non-answer, which for a proposal means nothing was accepted.
    NotSent(String),
    /// The link dropped after the request was written, or the deadline passed
    ///
    /// The peer may have acted on it.
    Unreachable(String),
}

impl std::fmt::Display for RpcFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcFailure::Remote(msg) => write!(f, "the peer refused the rpc: {msg}"),
            RpcFailure::NotSent(msg) | RpcFailure::Unreachable(msg) => write!(f, "{msg}"),
        }
    }
}

/// One replication connection to one peer node, shared by every group's peer on that node
///
/// A bounded queue and a task that dials, shakes hands and carries frames, reused from the
/// data lane; plus the RPCs in flight, keyed by the id each frame carries, each waiting on a
/// oneshot. A link that drops fails every one, because a replication RPC that cannot be
/// answered is one openraft retries.
pub struct ReplicationLink {
    /// The connection
    link: peer::Link,
    /// The RPCs in flight, by correlation id
    pending: Rc<RefCell<HashMap<u64, oneshot::Sender<Outcome>>>>,
    /// The next correlation id to hand out
    next_id: Cell<u64>,
    /// The largest frame the peer accepts
    max_frame_bytes: u32,
    /// Since when RPCs have been outstanding on this link with no answer to any of them
    ///
    /// A peer cut off by dropped packets leaves the connection up and answers nothing, so every
    /// RPC to it waits its whole deadline. This is what lets a caller see that before it sends
    /// ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md)).
    waiting_since: Rc<Cell<Option<Instant>>>,
}

impl ReplicationLink {
    /// Open a replication connection to a peer node
    ///
    /// # Arguments
    ///
    /// * `entry` - Where to dial and who to expect there
    /// * `local` - What this node says about itself
    /// * `transport` - The bounds and timers
    /// * `tls` - What to dial with, read at every dial
    /// * `on_event` - Where the link delivers what it learns
    fn new<F: Fn(LinkEvent) + 'static>(
        entry: PeerAddr,
        local: Rc<RefCell<Local>>,
        transport: &Transport,
        tls: PeerTlsHolder,
        on_event: F,
    ) -> Self {
        let max_frame_bytes = local.borrow().max_frame_bytes;
        let link = peer::Link::spawn(Lane::Replication, entry, local, transport, tls, on_event);
        ReplicationLink {
            link,
            pending: Rc::new(RefCell::new(HashMap::new())),
            next_id: Cell::new(1),
            max_frame_bytes,
            waiting_since: Rc::new(Cell::new(None)),
        }
    }

    /// Whether this link has had RPCs outstanding for `after` with no answer to any of them
    ///
    /// # Arguments
    ///
    /// * `after` - How long silence has to last to count
    #[must_use]
    pub fn silent_for(&self, after: Duration) -> Option<Duration> {
        self.waiting_since
            .get()
            .map(|since| since.elapsed())
            .filter(|silent| *silent >= after)
    }

    /// Note that the pending set changed, keeping `waiting_since` in step with it
    ///
    /// # Arguments
    ///
    /// * `progress` - Whether an answer arrived, which restarts the silence
    fn note_pending(&self, progress: bool) {
        let empty = self.pending.borrow().is_empty();
        match (empty, progress, self.waiting_since.get()) {
            // nothing outstanding is nothing to be silent about
            (true, _, _) => self.waiting_since.set(None),
            // an answer is the peer speaking, so the silence starts again from now
            (false, true, _) => self.waiting_since.set(Some(Instant::now())),
            // the first request outstanding starts the clock; later ones do not move it
            (false, false, None) => self.waiting_since.set(Some(Instant::now())),
            (false, false, Some(_)) => {}
        }
    }

    /// Complete the RPC a response frame answers
    ///
    /// # Arguments
    ///
    /// * `head` - The response's head
    /// * `payload` - Its payload
    fn answered(&self, head: &ReplicateResponseHead, payload: Vec<u8>) {
        let removed = self.pending.borrow_mut().remove(&head.id);
        // any answer is the peer speaking, known id or not
        self.note_pending(true);
        if let Some(tx) = removed {
            let outcome = match head.status {
                ReplicateStatus::Ok => Outcome::Ok(payload),
                ReplicateStatus::Error => {
                    Outcome::Remote(String::from_utf8_lossy(&payload).into_owned())
                }
            };
            let _ = tx.send(outcome);
        }
    }

    /// Fail every RPC in flight, because the link dropped
    ///
    /// A request whose frame the link still held is a definite non-answer - nothing was
    /// written, so the peer never saw it - and is failed as [`RpcFailure::NotSent`], which a
    /// proposal answers `NotLeader` at once rather than `OutcomeUnknown` at its deadline. One
    /// the link had written may have been acted on and stays unknown
    /// ([F42](../../../../docs/src/features/primary-failover.md)).
    ///
    /// # Arguments
    ///
    /// * `reason` - Why
    /// * `unsent` - Every frame the link never wrote
    fn down(&self, reason: &str, unsent: &[FrameKey]) {
        let drained: Vec<_> = self.pending.borrow_mut().drain().collect();
        self.waiting_since.set(None);
        for (id, tx) in drained {
            // never written is never seen, which is the one thing a caller can act on at once
            let outcome = if unsent.contains(&FrameKey::Replication(id)) {
                Outcome::NotSent(format!(
                    "the replication link went down before the request was written: {reason}"
                ))
            } else {
                Outcome::Unreachable(reason.to_string())
            };
            let _ = tx.send(outcome);
        }
    }

    /// Send one RPC and wait for its answer, or for the deadline
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `group` - The group it is for
    /// * `target_shard` - The shard on the peer that hosts the group
    /// * `payload` - Its serialized request
    /// * `deadline` - How long to wait before giving up
    pub async fn rpc(
        &self,
        kind: ReplicateKind,
        group: GroupId,
        target_shard: u16,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        self.rpc_at(kind, group, target_shard, None, payload, deadline)
            .await
    }

    /// The wire version this link negotiated, or the floor while it is not up
    ///
    /// What a body that differs between versions is encoded at before it is sent through
    /// [`ReplicationLink::rpc_at`] ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    #[must_use]
    pub fn wire_version(&self) -> u8 {
        self.link.negotiated().version
    }

    /// Whether the peer answers pre-votes, or nothing while the link is not up
    ///
    /// ([Resolved #144](../../../../docs/src/appendix/resolved/post-heal-elections.md))
    #[must_use]
    pub fn answers_pre_votes(&self) -> Option<bool> {
        self.link
            .negotiated_if_up()
            .map(|negotiated| negotiated.has(CAP_PRE_VOTE_V1))
    }

    /// Send one RPC whose payload was encoded at a named version, and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `group` - The group it is for
    /// * `target_shard` - The shard on the peer that hosts the group
    /// * `version` - The version the payload is encoded at, or none for a body the same at every version
    /// * `payload` - Its serialized request
    /// * `deadline` - How long to wait for an answer
    pub async fn rpc_at(
        &self,
        kind: ReplicateKind,
        group: GroupId,
        target_shard: u16,
        version: Option<u8>,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        // the peer is given exactly as long as this side waits
        self.rpc_budgeted(
            kind,
            group,
            target_shard,
            version,
            payload,
            deadline,
            deadline,
        )
        .await
    }

    /// Send one RPC telling the peer one budget, and wait for its answer for another
    ///
    /// A peer that acts on a request until its budget runs out and then answers needs this side
    /// to wait past that budget, or its answer always loses the race to this side's timer and a
    /// peer that was up and answering is reported as having timed out
    /// ([Resolved #128](../../../../docs/src/appendix/resolved/hop-deadline-margin.md)).
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `group` - The group it is for
    /// * `target_shard` - The shard on the peer that hosts the group
    /// * `version` - The version the payload is encoded at, or none for a body the same at every version
    /// * `payload` - Its serialized request
    /// * `budget` - How long the peer is told it has
    /// * `deadline` - How long to wait for an answer, no shorter than `budget`
    #[allow(clippy::too_many_arguments)]
    pub async fn rpc_budgeted(
        &self,
        kind: ReplicateKind,
        group: GroupId,
        target_shard: u16,
        version: Option<u8>,
        payload: Vec<u8>,
        budget: Duration,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        // mint an id and a oneshot for the answer
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(id, tx);
        self.note_pending(false);
        // truncation cannot happen for any deadline a replication RPC uses
        #[allow(clippy::cast_possible_truncation)]
        let deadline_ms = budget.as_millis().min(u128::from(u32::MAX)) as u32;
        let head = ReplicateRequestHead {
            id,
            group: group.0,
            target_shard,
            kind,
            deadline_ms,
        }
        .encode();
        // a body encoded at a version of its own names it in the header; the rest go out at
        // whatever the link negotiated ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let parts = vec![
            bytes::Bytes::copy_from_slice(&head),
            bytes::Bytes::from(payload),
        ];
        let frame = match version {
            Some(version) => Frame::at(
                version,
                MessageType::Replicate,
                parts,
                FrameKey::Replication(id),
                self.max_frame_bytes,
            ),
            None => Frame::new(
                MessageType::Replicate,
                parts,
                FrameKey::Replication(id),
                self.max_frame_bytes,
            ),
        }
        .map_err(|error| {
            RpcFailure::Unreachable(format!("framing a replication request: {error:?}"))
        })?;
        // a queue that is full or a link that is down is a definite non-answer
        if self.link.enqueue(frame).is_err() {
            self.pending.borrow_mut().remove(&id);
            self.note_pending(false);
            return Err(RpcFailure::NotSent(
                "the replication link's queue is full or its link is down".to_string(),
            ));
        }
        // wait for the answer, or the deadline, whichever comes first
        match glommio::timer::timeout(deadline, async { Ok(rx.await) }).await {
            Ok(Ok(Outcome::Ok(payload))) => Ok(payload),
            Ok(Ok(Outcome::Remote(msg))) => Err(RpcFailure::Remote(msg)),
            Ok(Ok(Outcome::NotSent(msg))) => Err(RpcFailure::NotSent(msg)),
            Ok(Ok(Outcome::Unreachable(msg))) => Err(RpcFailure::Unreachable(msg)),
            Ok(Err(_)) => {
                self.pending.borrow_mut().remove(&id);
                self.note_pending(false);
                Err(RpcFailure::Unreachable(
                    "the replication rpc was cancelled".to_string(),
                ))
            }
            Err(_) => {
                self.pending.borrow_mut().remove(&id);
                self.note_pending(false);
                Err(RpcFailure::Unreachable(
                    "the replication rpc timed out".to_string(),
                ))
            }
        }
    }

    /// What this link looks like from outside
    #[must_use]
    pub fn view(&self) -> LinkView {
        self.link.view()
    }
}

/// A token bucket over bytes, refilled from the clock, that paces every stream a node sends
///
/// One per shard network, across every stream and every group, so a node's snapshot traffic
/// is bounded whatever it is feeding - a move's learner, a returning member, a repair. Zero
/// is unlimited ([F46](../../../../docs/src/features/capacity-rebalancing.md)). Per device is
/// not built: a node with two storage devices shares one budget.
#[derive(Debug)]
pub struct RateLimiter {
    /// Bytes per second, or zero for no limit
    rate: u64,
    /// Bytes that may be taken now
    tokens: f64,
    /// When the bucket was last refilled
    refilled: Instant,
    /// The most the bucket holds: one second's worth
    burst: f64,
}

impl RateLimiter {
    /// A bucket at a rate, full
    ///
    /// # Arguments
    ///
    /// * `rate` - Bytes per second, or zero for no limit
    #[must_use]
    pub fn new(rate: u64) -> Self {
        // precision is not a concern for a byte budget
        #[allow(clippy::cast_precision_loss)]
        let burst = rate as f64;
        RateLimiter {
            rate,
            tokens: burst,
            refilled: Instant::now(),
            burst,
        }
    }

    /// Refill from the clock, and say how long a take of some bytes has to wait
    ///
    /// Takes the bytes when they are there - or when the bucket can never hold them at once,
    /// in which case the debt is carried - and otherwise says how long until they will be.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to take
    /// * `now` - The time
    pub fn take(&mut self, bytes: u64, now: Instant) -> Option<Duration> {
        if self.rate == 0 {
            return None;
        }
        // precision is not a concern for a byte budget
        #[allow(clippy::cast_precision_loss)]
        let wanted = bytes as f64;
        #[allow(clippy::cast_precision_loss)]
        let rate = self.rate as f64;
        let elapsed = now.saturating_duration_since(self.refilled).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(self.burst);
        self.refilled = now;
        if self.tokens >= wanted {
            self.tokens -= wanted;
            return None;
        }
        // short: the wait until the bucket has them, and the take is charged when it does
        let short = wanted - self.tokens;
        Some(Duration::from_secs_f64(short / rate))
    }
}

/// The state every `ShardPeer` shares
struct Shared {
    /// One link per peer node, opened on first use
    links: RefCell<HashMap<NodeId, Rc<ReplicationLink>>>,
    /// When this shard last heard anything from each peer node over the replication lane,
    /// a request it sent or an answer to one of this shard's
    ///
    /// A follower expects a heartbeat from its group's leader every tenth of the failover
    /// base, so a leader heard from not at all for seconds is cut off, whatever this shard's
    /// own link to it has outstanding ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md)).
    heard: RefCell<HashMap<NodeId, Instant>>,
    /// How long a peer may be silent before a write is not hopped to it: four heartbeats at the
    /// failover base the map carries, and never under `HOP_SILENCE`
    hop_silence: Cell<Duration>,
    /// One bulk link per peer node, opened on the first snapshot sent to it, each with the
    /// number it was opened under ([F43](../../../../docs/src/features/node-recovery.md))
    bulk: RefCell<HashMap<NodeId, (u64, Rc<peer::Link>)>>,
    /// The number the next bulk link is opened under
    next_bulk: Cell<u64>,
    /// How to ask the shard loop for a snapshot file
    builder: SnapshotBuilder,
    /// The groups' bounds: the chunk size a stream is cut into
    replication: Replication,
    /// What this shard's transfers have done, as the sender
    snapshots: RefCell<SnapshotStats>,
    /// Snapshot bytes sent per group and member, which a move's record is charged with
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    stream_bytes: RefCell<HashMap<(GroupId, ShardAddr), u64>>,
    /// The byte budget every stream this shard sends draws on
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    limiter: RefCell<RateLimiter>,
    /// The map this shard holds, which is where every member's address comes from
    map: MapCell,
    /// Where particular members are dialled instead of where they advertise
    dial: BTreeMap<NodeId, DialOverride>,
    /// What this node says about itself
    local: Rc<RefCell<Local>>,
    /// What to dial with, read at every dial so a reload reaches the next one
    tls: PeerTlsHolder,
    /// The bounds and timers
    transport: Transport,
    /// Where a link delivers what it learns: the shard's own mesh channel
    on_event: Rc<dyn Fn(LinkEvent)>,
}

/// A shard's replication network: the factory openraft asks for a peer, and the links it owns
#[derive(Clone)]
pub struct ShardNetwork {
    /// The shared state
    shared: Rc<Shared>,
}

impl ShardNetwork {
    /// Build the network for a shard
    ///
    /// # Arguments
    ///
    /// * `map` - The map the shard holds
    /// * `dial` - Where particular members are dialled instead of where they advertise
    /// * `local` - What this node says about itself
    /// * `tls` - What to dial peers with, read at every dial
    /// * `transport` - The bounds and timers
    /// * `replication` - The groups' bounds
    /// * `on_event` - Where a link delivers what it learns
    /// * `builder` - How to ask the shard loop for a snapshot file
    /// * `stream_bytes_per_sec` - The byte budget every stream draws on, or zero for none
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        map: MapCell,
        dial: BTreeMap<NodeId, DialOverride>,
        local: Rc<RefCell<Local>>,
        tls: PeerTlsHolder,
        transport: Transport,
        replication: Replication,
        on_event: Rc<dyn Fn(LinkEvent)>,
        builder: SnapshotBuilder,
        stream_bytes_per_sec: u64,
    ) -> Self {
        ShardNetwork {
            shared: Rc::new(Shared {
                links: RefCell::new(HashMap::new()),
                heard: RefCell::new(HashMap::new()),
                hop_silence: Cell::new(HOP_SILENCE),
                bulk: RefCell::new(HashMap::new()),
                next_bulk: Cell::new(1),
                builder,
                replication,
                snapshots: RefCell::new(SnapshotStats::default()),
                stream_bytes: RefCell::new(HashMap::new()),
                limiter: RefCell::new(RateLimiter::new(stream_bytes_per_sec)),
                map,
                dial,
                local,
                tls,
                transport,
                on_event,
            }),
        }
    }

    /// The snapshot bytes this shard has sent one member of a group
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `target` - The member
    #[must_use]
    pub fn bytes_sent_to(&self, group: GroupId, target: ShardAddr) -> u64 {
        self.shared
            .stream_bytes
            .borrow()
            .get(&(group, target))
            .copied()
            .unwrap_or(0)
    }

    /// The move a stream to a member of a group serves, if the map carries one
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `target` - The member
    #[must_use]
    pub fn transition_of(&self, group: GroupId, target: ShardAddr) -> Option<Uuid> {
        self.shared
            .map
            .get()
            .moves
            .iter()
            .find(|record| record.to == target && record.groups.contains_key(&group))
            .map(|record| record.op)
    }

    /// Get or open the bulk link to a peer node, for a snapshot stream
    ///
    /// A link that goes down forgets itself, so the next stream dials afresh: the link's own
    /// number is what it is forgotten by, since a transfer still holding an older link may
    /// see that one drop after a newer one was opened, and must not forget the newer one.
    ///
    /// # Arguments
    ///
    /// * `node` - The peer node
    pub fn bulk_link(&self, node: NodeId) -> Option<Rc<peer::Link>> {
        let entry = self.addr_of(node)?;
        if let Some((_, link)) = self.shared.bulk.borrow().get(&node) {
            if *link.target() == entry && !link.is_closed() {
                return Some(link.clone());
            }
        }
        let on_event = self.shared.on_event.clone();
        let id = self.shared.next_bulk.get();
        self.shared.next_bulk.set(id.wrapping_add(1));
        let shared = Rc::downgrade(&self.shared);
        let link = Rc::new(peer::Link::spawn(
            Lane::Bulk,
            entry,
            self.shared.local.clone(),
            &self.shared.transport,
            self.shared.tls.clone(),
            move |event| {
                // this link going down forgets this link, and no other
                if let LinkEvent::Down { node, .. } = &event {
                    if let Some(shared) = shared.upgrade() {
                        let mut bulk = shared.bulk.borrow_mut();
                        if bulk.get(node).is_some_and(|(held, _)| *held == id) {
                            bulk.remove(node);
                        }
                    }
                }
                on_event(event);
            },
        ));
        self.shared
            .bulk
            .borrow_mut()
            .insert(node, (id, link.clone()));
        Some(link)
    }

    /// What this shard's transfers have done, as the sender
    #[must_use]
    pub fn snapshot_stats(&self) -> SnapshotStats {
        *self.shared.snapshots.borrow()
    }

    /// Ask the shard loop for a snapshot file of a group, at a boundary at least this high
    ///
    /// A file the loop holds is answered while it is at or past `at_least` and the log still
    /// holds what follows it; a cut already in flight can land below `at_least`, and is asked
    /// past again ([O71](../../../../docs/src/appendix/optimizations.md#o71-a-held-snapshot-is-cut-again-whenever-the-checkpoint-moves)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `at_least` - The lowest boundary the caller can use, or zero for any
    pub async fn build(&self, group: GroupId, at_least: u64) -> Result<Rc<BuiltSnapshot>, String> {
        // a few tries, since a cut asked for before ours may be the one that answers first
        let mut built = None;
        for _ in 0..3 {
            let (tx, rx) = oneshot::channel();
            (self.shared.builder)(group, at_least, tx);
            let answer = rx.await.unwrap_or_else(|_| {
                Err("the shard loop dropped the snapshot request".to_string())
            })?;
            if answer.manifest.boundary.index >= at_least {
                return Ok(answer);
            }
            built = Some(answer);
        }
        let boundary = built.map_or(0, |built| built.manifest.boundary.index);
        Err(format!(
            "every cut of group {group} landed at {boundary}, below the {at_least} asked for"
        ))
    }

    /// Where to dial a member, from the map and this node's overrides
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    fn addr_of(&self, node: NodeId) -> Option<PeerAddr> {
        let mut entry = self.shared.map.get().peer_addr(node)?;
        if let Some(target) = self.shared.dial.get(&node) {
            if let Some(control) = &target.control {
                entry.control.clone_from(control);
            }
            if let Some(data) = &target.data {
                entry.data.clone_from(data);
            }
        }
        Some(entry)
    }

    /// Get or open the link to a peer node
    ///
    /// A link to an address the member no longer advertises is dropped and dialled afresh.
    ///
    /// # Arguments
    ///
    /// * `node` - The peer node
    pub fn link(&self, node: NodeId) -> Option<Rc<ReplicationLink>> {
        let entry = self.addr_of(node)?;
        if let Some(link) = self.shared.links.borrow().get(&node) {
            if *link.link.target() == entry {
                return Some(link.clone());
            }
        }
        let on_event = self.shared.on_event.clone();
        let link = Rc::new(ReplicationLink::new(
            entry,
            self.shared.local.clone(),
            &self.shared.transport,
            self.shared.tls.clone(),
            move |event| on_event(event),
        ));
        self.shared.links.borrow_mut().insert(node, link.clone());
        Some(link)
    }

    /// Set the silence a hop is refused after from the failover base the groups run at
    ///
    /// A group heartbeats its followers every tenth of the base, so four heartbeats is a tenth
    /// of that times four; a long base must not make a healthy leader look silent.
    ///
    /// # Arguments
    ///
    /// * `failover_ms` - The base, in milliseconds
    pub fn set_failover_base(&self, failover_ms: u64) {
        let heartbeats = Duration::from_millis(failover_ms.saturating_mul(4) / 10);
        self.shared.hop_silence.set(heartbeats.max(HOP_SILENCE));
    }

    /// The silence after which a write is not hopped to a peer
    #[must_use]
    pub fn hop_silence(&self) -> Duration {
        self.shared.hop_silence.get()
    }

    /// Note that a peer node was just heard from over the replication lane
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    pub fn heard_from(&self, node: NodeId) {
        self.shared.heard.borrow_mut().insert(node, Instant::now());
    }

    /// How long a peer node has been silent, if it was heard from before and not for `after`
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    /// * `after` - How long silence has to last to count
    #[must_use]
    pub fn silent_for(&self, node: NodeId, after: Duration) -> Option<Duration> {
        self.shared
            .heard
            .borrow()
            .get(&node)
            .map(|heard| heard.elapsed())
            .filter(|silent| *silent >= after)
    }

    /// Complete the RPC a response frame from a node answers
    ///
    /// # Arguments
    ///
    /// * `node` - The peer that answered
    /// * `head` - The response's head
    /// * `payload` - Its payload
    pub fn answered(&self, node: NodeId, head: &[u8], payload: Vec<u8>) {
        // an answer is the peer speaking
        self.heard_from(node);
        let Ok(raw) = <[u8; REPLICATE_RESPONSE_HEAD_LEN]>::try_from(head) else {
            return;
        };
        let head = ReplicateResponseHead::decode(&raw);
        if let Some(link) = self.shared.links.borrow().get(&node) {
            link.answered(&head, payload);
        }
    }

    /// Fail every RPC in flight to a node, because its link dropped
    ///
    /// # Arguments
    ///
    /// * `node` - The peer
    /// * `reason` - Why
    /// * `unsent` - Every frame the link never wrote, which are failed as definite non-answers
    pub fn down(&self, node: NodeId, reason: &str, unsent: &[FrameKey]) {
        if let Some(link) = self.shared.links.borrow().get(&node) {
            link.down(reason, unsent);
        }
    }

    /// The bounds and timers
    #[must_use]
    pub fn transport(&self) -> &Transport {
        &self.shared.transport
    }

    /// What every link looks like from outside
    #[must_use]
    pub fn views(&self) -> Vec<LinkView> {
        self.shared
            .links
            .borrow()
            .values()
            .map(|link| link.view())
            .collect()
    }

    /// Whether this shard can reach nobody over the replication lane
    ///
    /// True when the shard has dialled at least one peer and none of its links is up. A group
    /// on such a shard has no election to win and stops standing for one
    /// ([Resolved #106](../../../../docs/src/appendix/resolved/isolated-member-term-inflation.md)).
    #[must_use]
    pub fn is_isolated(&self) -> bool {
        let links = self.shared.links.borrow();
        // a shard with no link yet has nobody to be cut off from
        !links.is_empty() && links.values().all(|link| !link.link.is_up())
    }

    /// This node's identity
    #[must_use]
    pub fn local_node(&self) -> NodeId {
        self.shared.local.borrow().node
    }
}

/// The least silence from a peer after which a write is not hopped to it
///
/// Four heartbeats at the default failover base: a leader replicating to anyone answers
/// something several times in that long, and one that answers nothing is cut off or stopped. A
/// longer base raises it to four of its own heartbeats (`ShardNetwork::set_failover_base`).
const HOP_SILENCE: Duration = Duration::from_secs(2);

/// The most a forwarded proposal holds back from the leader's budget for its answer's trip home
const HOP_MARGIN: Duration = Duration::from_millis(250);

/// How long a leader is told it has for a proposal the forwarder waits `remaining` for
///
/// A tenth of what is left, never more than [`HOP_MARGIN`], is kept back for the answer to
/// travel home, so a leader that did not commit in time says so before the forwarder's own
/// timer fires. With the whole budget handed on, the two timers were the same and the
/// forwarder's always won: a leader that was up and answering reached the client as "the
/// replication rpc timed out" ([Resolved #128](../../../../docs/src/appendix/resolved/hop-deadline-margin.md)).
///
/// # Arguments
///
/// * `remaining` - How long the forwarder will wait for the leader's answer
#[must_use]
pub fn hop_budget(remaining: Duration) -> Duration {
    // a tenth of the budget, capped, so a short deadline is not eaten by a fixed margin
    let margin = (remaining / 10).min(HOP_MARGIN);
    remaining.saturating_sub(margin)
}

/// The network to one member on one node, which a group's peer wraps
pub struct ShardPeer {
    /// Who it reaches
    target: ShardAddr,
    /// The network, whose link to the member's node this uses
    network: ShardNetwork,
}

impl ShardPeer {
    /// A peer to a member over a network
    ///
    /// # Arguments
    ///
    /// * `target` - The member
    /// * `network` - The shard's network
    #[must_use]
    pub fn new(target: ShardAddr, network: ShardNetwork) -> Self {
        ShardPeer { target, network }
    }

    /// Send a proposal to the member and wait for its answer
    ///
    /// The member is told [`hop_budget`] of `deadline`, so a leader that cannot commit in time
    /// answers why before this side gives up on it.
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `payload` - The command's bytes
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn propose(
        &self,
        group: GroupId,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        // the member is told less than this side waits, so its answer arrives before our timer
        let Some(link) = self.network.link(self.target.node) else {
            return Err(RpcFailure::Unreachable(format!(
                "{} is not a member the map knows",
                self.target.node
            )));
        };
        // a leader that has answered nothing for a while is not handed a write to wait on:
        // the refusal is definite, since nothing was sent, and the caller retries elsewhere
        // rather than holding the write for its whole deadline. silent either way counts: this
        // shard's own requests unanswered, or the leader's heartbeats to this follower stopped
        // ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md))
        let silence = self.network.hop_silence();
        if let Some(silent) = link
            .silent_for(silence)
            .or_else(|| self.network.silent_for(self.target.node, silence))
        {
            return Err(RpcFailure::NotSent(format!(
                "{} has answered nothing on the replication lane for {silent:?}; the write was not sent",
                self.target.node
            )));
        }
        link.rpc_budgeted(
            ReplicateKind::Propose,
            group,
            self.target.shard,
            None,
            payload,
            hop_budget(deadline),
            deadline,
        )
        .await
    }

    /// Ask the member, which should be the group's leader, for a read barrier
    ///
    /// The answer is the leader's read log id once a heartbeat round has confirmed its term,
    /// or a hint at who leads instead ([F41](../../../../docs/src/features/read-consistency.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn read_barrier(
        &self,
        group: GroupId,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        self.rpc(ReplicateKind::ReadBarrier, group, Vec::new(), deadline)
            .await
    }

    /// Ask the member for its canonical digest of the group at a scrub
    ///
    /// The answer is a `DigestAnswer`: pending until the member's cut has been read, then the
    /// report ([F44](../../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `op` - The operation whose scrub the digest is of
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn digest(
        &self,
        group: GroupId,
        op: uuid::Uuid,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        self.rpc(
            ReplicateKind::Digest,
            group,
            op.into_bytes().to_vec(),
            deadline,
        )
        .await
    }

    /// Tell the member what to do with its copy's quarantine
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `action` - The action, serialized
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn quarantine(
        &self,
        group: GroupId,
        action: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        self.rpc(ReplicateKind::Quarantine, group, action, deadline)
            .await
    }

    /// Ask the member how far it has applied a group's log
    ///
    /// The move's activation barrier: the answer is the index the member's own apply has
    /// passed ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `op` - The move
    /// * `index` - The index asked about
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn applied(
        &self,
        group: GroupId,
        op: uuid::Uuid,
        index: u64,
        deadline: Duration,
    ) -> Result<u64, RpcFailure> {
        let payload = postcard::to_allocvec(&(op, index))
            .map_err(|error| RpcFailure::NotSent(error.to_string()))?;
        let bytes = self
            .rpc(ReplicateKind::Applied, group, payload, deadline)
            .await?;
        postcard::from_bytes::<u64>(&bytes)
            .map_err(|error| RpcFailure::Remote(format!("decoding an applied answer: {error}")))
    }

    /// Ask the member whether its retired copy of a group is gone
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn retired(&self, group: GroupId, deadline: Duration) -> Result<bool, RpcFailure> {
        let bytes = self
            .rpc(ReplicateKind::Retired, group, Vec::new(), deadline)
            .await?;
        postcard::from_bytes::<bool>(&bytes)
            .map_err(|error| RpcFailure::Remote(format!("decoding a retired answer: {error}")))
    }

    /// Turn a link error into openraft's retriable unreachable
    ///
    /// # Arguments
    ///
    /// * `failure` - What went wrong
    fn unreachable(failure: RpcFailure) -> RPCError<DataConfig> {
        RPCError::Unreachable(Unreachable::new(&LinkFailed {
            msg: failure.to_string(),
        }))
    }

    /// Send a snapshot RPC, trying again after a lost link until the transfer's deadline
    ///
    /// A replication RPC fails the moment its link drops, which a proposal wants; a snapshot
    /// transfer is measured in its own deadline and a lane cut for a moment is what the
    /// resume offset exists for, so the begin and the end are sent again after a short wait
    /// rather than failing the transfer ([F43](../../../../docs/src/features/node-recovery.md)).
    /// A refusal the peer answered is not retried.
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `rpc` - The RPC, encoded afresh at the link's version on every attempt
    /// * `started` - When the transfer started
    /// * `deadline` - How long it may take in all
    async fn snapshot_rpc_until(
        &self,
        group: GroupId,
        rpc: &SnapshotRpc,
        started: Instant,
        deadline: Duration,
    ) -> Result<Vec<u8>, StreamingError<DataConfig>> {
        loop {
            let remaining = deadline.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(unreachable(
                    "the snapshot transfer ran out of time".to_string(),
                ));
            }
            // the link may have come back at another version since the last attempt, so the
            // body is encoded at what it speaks now and the frame names that version
            // ([F48](../../../../docs/src/features/rolling-compatibility.md))
            let Some(link) = self.network.link(self.target.node) else {
                return Err(unreachable(format!(
                    "{} is not a member the map knows",
                    self.target.node
                )));
            };
            let version = link.wire_version();
            let payload = rpc.encode_at(version).map_err(|error| {
                unreachable(format!(
                    "encoding a snapshot rpc at wire version {version}: {error}"
                ))
            })?;
            let sent = link
                .rpc_at(
                    ReplicateKind::Snapshot,
                    group,
                    self.target.shard,
                    Some(version),
                    payload,
                    remaining,
                )
                .await;
            match sent {
                Ok(answer) => return Ok(answer),
                Err(RpcFailure::Remote(msg)) => {
                    return Err(unreachable(format!("the peer refused the rpc: {msg}")))
                }
                // a lost link: wait for it to come back and ask again
                Err(RpcFailure::NotSent(_) | RpcFailure::Unreachable(_)) => {
                    glommio::timer::sleep(SNAPSHOT_RPC_RETRY).await;
                }
            }
        }
    }

    /// Send one RPC to the member and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `group` - The group
    /// * `payload` - Its serialized request
    /// * `deadline` - How long to wait
    async fn rpc(
        &self,
        kind: ReplicateKind,
        group: GroupId,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        let Some(link) = self.network.link(self.target.node) else {
            return Err(RpcFailure::Unreachable(format!(
                "{} is not a member the map knows",
                self.target.node
            )));
        };
        link.rpc(kind, group, self.target.shard, payload, deadline)
            .await
    }
}

/// The factory one group hands openraft: every peer it builds carries the group's identity
///
/// openraft's RPCs do not name a group, since a `Raft` is one group; the frame does, and the
/// group is what the peer's listener routes by. So the factory is per group over the shard's
/// one network, and a peer it builds knows which group it speaks for.
pub struct GroupNetwork {
    /// The group every peer this factory hands out belongs to
    pub group: GroupId,
    /// The shard's network
    pub network: ShardNetwork,
}

impl RaftNetworkFactory<DataConfig> for GroupNetwork {
    type Network = GroupPeer;

    /// A client for a member, over the link to its node, for this group
    async fn new_client(&mut self, target: ShardAddr, _node: &ShardAddr) -> Self::Network {
        GroupPeer {
            group: self.group,
            peer: ShardPeer {
                target,
                network: self.network.clone(),
            },
        }
    }
}

/// The network to one member of one particular group
pub struct GroupPeer {
    /// The group
    group: GroupId,
    /// The peer
    peer: ShardPeer,
}

impl RaftNetworkV2<DataConfig> for GroupPeer {
    type SnapshotData = SnapshotData;

    /// Append entries: serialize, send, deserialize the response
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<DataConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<DataConfig>, RPCError<DataConfig>> {
        let payload = postcard::to_allocvec(&rpc).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!(
                "encoding append_entries: {error}"
            )))
        })?;
        let answer = self
            .peer
            .rpc(
                ReplicateKind::AppendEntries,
                self.group,
                payload,
                option.hard_ttl(),
            )
            .await
            .map_err(ShardPeer::unreachable)?;
        postcard::from_bytes(&answer).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!(
                "decoding append_entries: {error}"
            )))
        })
    }

    /// Vote: serialize, send, deserialize the response
    async fn vote(
        &mut self,
        rpc: VoteRequest<DataConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<DataConfig>, RPCError<DataConfig>> {
        let payload = postcard::to_allocvec(&rpc).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!("encoding vote: {error}")))
        })?;
        let answer = self
            .peer
            .rpc(ReplicateKind::Vote, self.group, payload, option.hard_ttl())
            .await
            .map_err(ShardPeer::unreachable)?;
        postcard::from_bytes(&answer).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!("decoding vote: {error}")))
        })
    }

    /// Ask the member whether it would grant a vote at the next term, without it moving its term
    ///
    /// A member whose build does not answer pre-votes is granted locally, which is openraft's
    /// own default and what the election did before pre-vote was on; one that cannot be reached
    /// is an error, never a grant, or a node cut off from every peer would grant itself a
    /// quorum and stand at a new term anyway
    /// ([Resolved #144](../../../../docs/src/appendix/resolved/post-heal-elections.md)).
    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<DataConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<DataConfig>, RPCError<DataConfig>> {
        // whether the member's build answers a pre-vote, judged on the link it would go over
        let answers = self
            .peer
            .network
            .link(self.peer.target.node)
            .and_then(|link| link.answers_pre_votes());
        match answers {
            // up with a peer that answers them: ask it
            Some(true) => (),
            // up with an older build: grant, as openraft does for a network without pre-vote
            Some(false) => return Ok(VoteResponse::new(rpc.vote, None, true)),
            // down: no answer, and so no grant
            None => {
                return Err(ShardPeer::unreachable(RpcFailure::NotSent(format!(
                    "the link to {} is not up",
                    self.peer.target.node
                ))))
            }
        }
        let payload = postcard::to_allocvec(&rpc).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::NotSent(format!("encoding pre_vote: {error}")))
        })?;
        let answer = self
            .peer
            .rpc(
                ReplicateKind::PreVote,
                self.group,
                payload,
                option.hard_ttl(),
            )
            .await
            .map_err(ShardPeer::unreachable)?;
        postcard::from_bytes(&answer).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!(
                "decoding pre_vote: {error}"
            )))
        })
    }

    /// Tell the member the lead is being handed to it, or to another
    ///
    /// The library's transfer message on the lane: the member named elects at once if its
    /// log is up to date, so the lead moves before the old leader's lease lapses
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    async fn transfer_leader(
        &mut self,
        req: openraft::raft::TransferLeaderRequest<DataConfig>,
        option: RPCOption,
    ) -> Result<openraft::raft::TransferLeaderResponse<DataConfig>, RPCError<DataConfig>> {
        let payload = postcard::to_allocvec(&req).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::NotSent(format!(
                "encoding transfer_leader: {error}"
            )))
        })?;
        let answer = self
            .peer
            .rpc(
                ReplicateKind::TransferLeader,
                self.group,
                payload,
                option.hard_ttl(),
            )
            .await
            .map_err(ShardPeer::unreachable)?;
        postcard::from_bytes(&answer).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!(
                "decoding transfer_leader: {error}"
            )))
        })
    }

    /// Send a whole snapshot to the member: control on the replication lane, bytes on the bulk lane
    ///
    /// The file is the loop's, cut at or past the handle's checkpoint; the manifest's boundary
    /// may be newer than the handle's, which the follower's committed rule accepts. A begin RPC
    /// learns where to start, the chunks stream from there, an end RPC waits for the install,
    /// and a receiver short of bytes at the end answers where to resume from
    /// ([F43](../../../../docs/src/features/node-recovery.md)). Every failure is answered
    /// unreachable, which openraft retries with a backoff, except a cancellation.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<DataConfig>,
        snapshot: SnapshotOf<DataConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<DataConfig>, StreamingError<DataConfig>> {
        let group = self.group;
        let network = self.peer.network.clone();
        // a member the link has heard nothing from is not cut a snapshot it cannot take: on the
        // lab a rebuilt node's old identity, down until its removal reached each group, was cut
        // two or three files of 270 MB a group, which the moves refilling its replacement then
        // queued behind ([O73](../../../../docs/src/appendix/optimizations.md#o73-a-snapshot-is-cut-for-a-member-that-cannot-be-reached))
        let target = self.peer.target.node;
        let silence = network.hop_silence();
        let silent = network.link(target).map_or_else(
            || network.silent_for(target, silence),
            |link| {
                link.silent_for(silence)
                    .or_else(|| network.silent_for(target, silence))
            },
        );
        if let Some(silent) = silent {
            return Err(unreachable(format!(
                "{target} has answered nothing on the replication lane for {silent:?}; no snapshot is cut for it"
            )));
        }
        // the file: the loop's cut for this group's own snapshot, or a received one as it is
        let held;
        let (path, manifest): (PathBuf, SnapshotManifest) = match snapshot.snapshot {
            SnapshotData::Own { .. } => {
                held = network
                    .build(group, 0)
                    .await
                    .map_err(|msg| unreachable(format!("cutting a snapshot: {msg}")))?;
                (held.path.clone(), held.manifest.clone())
            }
            SnapshotData::Received { path, manifest } => (path, manifest),
        };
        match self
            .send_snapshot(vote, path, manifest, None, cancel, option.hard_ttl())
            .await
        {
            Ok(response) => Ok(response),
            Err(SendError::Streaming(error)) => Err(error),
            // a stream that is not a repair's is never judged against a checkpoint
            Err(SendError::Behind(checkpoint)) => Err(unreachable(format!(
                "the receiver answered a checkpoint of {checkpoint} to a plain stream"
            ))),
        }
    }
}

/// Why a snapshot stream did not complete
enum SendError {
    /// The receiver's checkpoint is at or past a repair stream's boundary
    Behind(u64),
    /// The transfer failed as openraft's would
    Streaming(StreamingError<DataConfig>),
}

impl From<StreamingError<DataConfig>> for SendError {
    /// A transfer failure
    fn from(error: StreamingError<DataConfig>) -> Self {
        SendError::Streaming(error)
    }
}

impl GroupPeer {
    /// A peer to one member of a group, for a repair transfer outside openraft's replication
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `target` - The member
    /// * `network` - The shard's network
    #[must_use]
    pub fn for_repair(group: GroupId, target: ShardAddr, network: ShardNetwork) -> Self {
        GroupPeer {
            group,
            peer: ShardPeer::new(target, network),
        }
    }

    /// Send a repair snapshot to a quarantined member, and say what it answered
    ///
    /// The driver's transfer ([F44](../../../../docs/src/features/repair.md)): the same stream
    /// as openraft's, under the repair operation, which the receiver judges against its
    /// checkpoint and installs by restarting the group. A receiver whose checkpoint is past the
    /// boundary answers where it stands, so the driver can cut again.
    ///
    /// # Arguments
    ///
    /// * `vote` - The sender's vote
    /// * `path` - The file
    /// * `manifest` - What it is
    /// * `op` - The repair operation
    /// * `deadline` - How long the transfer may take
    ///
    /// # Errors
    ///
    /// Fails as the transfer fails: a refusal, a lost link past the deadline, or the deadline.
    pub async fn repair_snapshot(
        &mut self,
        vote: VoteOf<DataConfig>,
        path: PathBuf,
        manifest: SnapshotManifest,
        op: Uuid,
        deadline: Duration,
    ) -> Result<RepairSend, String> {
        // nothing cancels a repair transfer but its deadline
        let never = std::future::pending::<ReplicationClosed>();
        match self
            .send_snapshot(vote, path, manifest, Some(op), never, deadline)
            .await
        {
            Ok(_) => Ok(RepairSend::Installed),
            Err(SendError::Behind(checkpoint)) => Ok(RepairSend::Behind { checkpoint }),
            Err(SendError::Streaming(error)) => Err(error.to_string()),
        }
    }

    /// Send a snapshot to the member: control on the replication lane, bytes on the bulk lane
    ///
    /// A begin RPC learns where to start, the chunks stream from there, an end RPC waits for
    /// the install, and a receiver short of bytes at the end answers where to resume from
    /// ([F43](../../../../docs/src/features/node-recovery.md)). Every failure is answered
    /// unreachable, which openraft retries with a backoff, except a cancellation.
    ///
    /// # Arguments
    ///
    /// * `vote` - The sender's vote
    /// * `path` - The file
    /// * `manifest` - What it is
    /// * `repair` - The repair operation this stream serves, if it is one
    /// * `cancel` - Resolves when the transfer is cancelled
    /// * `deadline` - How long the transfer may take
    async fn send_snapshot(
        &mut self,
        vote: VoteOf<DataConfig>,
        path: PathBuf,
        manifest: SnapshotManifest,
        repair: Option<Uuid>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        deadline: Duration,
    ) -> Result<SnapshotResponse<DataConfig>, SendError> {
        let started = Instant::now();
        let group = self.group;
        let network = self.peer.network.clone();
        let target = self.peer.target;
        let stream = *Uuid::new_v4().as_bytes();
        // the move this stream serves, if the receiver is a move's destination
        // ([F45](../../../../docs/src/features/replica-migration.md))
        let transition = network
            .transition_of(group, target)
            .map_or([0u8; 16], |op| *op.as_bytes());
        let mut cancel = Box::pin(cancel);
        // begin: what is coming, and where the receiver wants it from, encoded at the version
        // the link speaks ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let begin = SnapshotRpc::Begin {
            vote: vote.clone(),
            stream,
            manifest: manifest.clone(),
            repair,
        };
        let answer: SnapshotAnswer = decode(
            &self
                .peer
                .snapshot_rpc_until(group, &begin, started, deadline)
                .await?,
        )?;
        event!(Level::DEBUG, msg = "a snapshot stream begins", group = %group, %target, boundary = manifest.boundary.index, bytes = manifest.total, ?answer);
        let mut from = match answer {
            SnapshotAnswer::Resume { from } => from,
            SnapshotAnswer::Installed { vote } => return Ok(SnapshotResponse { vote }),
            SnapshotAnswer::Refused(msg) => {
                network.shared.snapshots.borrow_mut().aborted += 1;
                return Err(unreachable(format!("{target} refused the snapshot: {msg}")).into());
            }
            SnapshotAnswer::Behind { checkpoint } => return Err(SendError::Behind(checkpoint)),
        };
        let Some(link) = network.bulk_link(target.node) else {
            return Err(
                unreachable(format!("{} is not a member the map knows", target.node)).into(),
            );
        };
        let max = network.shared.local.borrow().max_frame_bytes;
        let chunk_bytes = network.shared.replication.snapshot_chunk_bytes as u64;
        let file = BufferedFile::open(&path)
            .await
            .map_err(|error| unreachable(format!("opening the snapshot file: {error}")))?;
        let outcome: Result<SnapshotResponse<DataConfig>, SendError> = async {
            loop {
                // the stream's begin, routing its chunks to the shard that hosts the group
                let route = postcard::to_allocvec(&BulkRoute {
                    group,
                    target_shard: target.shard,
                })
                .map_err(|error| unreachable(format!("encoding a bulk route: {error}")))?;
                let begin = SnapshotBegin {
                    stream,
                    transition,
                    boundary: manifest.boundary.index,
                    total: manifest.total,
                    manifest_len: u32::try_from(route.len()).unwrap_or(u32::MAX),
                }
                .encode();
                let frame = Frame::new(
                    MessageType::SnapshotBegin,
                    vec![bytes::Bytes::copy_from_slice(&begin), bytes::Bytes::from(route)],
                    FrameKey::Bulk(0),
                    max,
                )
                .map_err(|error| unreachable(format!("framing a snapshot begin: {error:?}")))?;
                enqueue_or_wait(&link, frame, &mut cancel, started, deadline).await?;
                // the chunks, from where the receiver wants them
                let mut offset = from;
                while offset < manifest.total {
                    // `usize` from `u64` is lossless on every target this runs on
                    let len = chunk_bytes.min(manifest.total - offset) as usize;
                    // the byte budget first: a chunk waits for its tokens, and the wait is
                    // charged to the stats ([F46](../../../../docs/src/features/capacity-rebalancing.md))
                    loop {
                        let wait = network.shared.limiter.borrow_mut().take(len as u64, Instant::now());
                        let Some(wait) = wait else {
                            break;
                        };
                        if started.elapsed() >= deadline {
                            return Err(SendError::from(unreachable("the snapshot transfer ran out of time waiting on its byte budget".to_string())));
                        }
                        network.shared.snapshots.borrow_mut().budget_wait_ns += u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
                        glommio::timer::sleep(wait).await;
                    }
                    let read = file
                        .read_at(offset, len)
                        .await
                        .map_err(|error| unreachable(format!("reading the snapshot file: {error}")))?;
                    let head = SnapshotChunk {
                        stream,
                        offset,
                        len: u32::try_from(read.len()).unwrap_or(u32::MAX),
                        checksum: checksum(&read),
                    }
                    .encode();
                    let frame = Frame::new(
                        MessageType::SnapshotChunk,
                        vec![bytes::Bytes::copy_from_slice(&head), bytes::Bytes::copy_from_slice(&read)],
                        FrameKey::Bulk(read.len()),
                        max,
                    )
                    .map_err(|error| unreachable(format!("framing a snapshot chunk: {error:?}")))?;
                    enqueue_or_wait(&link, frame, &mut cancel, started, deadline).await?;
                    network.shared.snapshots.borrow_mut().bytes_sent += read.len() as u64;
                    *network.shared.stream_bytes.borrow_mut().entry((group, target)).or_default() += read.len() as u64;
                    offset += read.len() as u64;
                }
                event!(Level::DEBUG, msg = "a snapshot stream's chunks are queued", group = %group, %target, from, total = manifest.total);
                // the stream's end on the lane, then the end RPC that waits for the install
                let end = SnapshotEnd {
                    stream,
                    total: manifest.total,
                    // truncation is the point: the lane's field is narrower than the manifest's
                    #[allow(clippy::cast_possible_truncation)]
                    checksum: manifest.checksum as u32,
                    status: SnapshotStatus::Complete,
                    resume_from: manifest.total,
                }
                .encode();
                let frame = Frame::new(MessageType::SnapshotEnd, vec![bytes::Bytes::copy_from_slice(&end)], FrameKey::Bulk(0), max)
                    .map_err(|error| unreachable(format!("framing a snapshot end: {error:?}")))?;
                enqueue_or_wait(&link, frame, &mut cancel, started, deadline).await?;
                let end = SnapshotRpc::End {
                    stream,
                    total: manifest.total,
                    checksum: manifest.checksum,
                };
                let answer: SnapshotAnswer = decode(&self.peer.snapshot_rpc_until(group, &end, started, deadline).await?)?;
                match answer {
                    SnapshotAnswer::Installed { vote } => {
                        network.shared.snapshots.borrow_mut().sent += 1;
                        event!(Level::INFO, msg = "a snapshot was installed on a member", group = %group, %target, boundary = manifest.boundary.index, bytes = manifest.total);
                        return Ok(SnapshotResponse { vote });
                    }
                    // the receiver is short of bytes: send again from where it stands
                    SnapshotAnswer::Resume { from: resume } => {
                        event!(Level::DEBUG, msg = "resuming a snapshot stream", group = %group, %target, from = resume);
                        from = resume;
                        if started.elapsed() >= deadline {
                            return Err(SendError::from(unreachable("the snapshot transfer ran out of time resuming".to_string())));
                        }
                    }
                    SnapshotAnswer::Refused(msg) => {
                        return Err(SendError::from(unreachable(format!("{target} refused the snapshot at its end: {msg}"))));
                    }
                    SnapshotAnswer::Behind { checkpoint } => return Err(SendError::Behind(checkpoint)),
                }
            }
        }
        .await;
        let _ = file.close().await;
        if let Err(error) = &outcome {
            // a transfer that did not complete is aborted on the lane, so the receiver can
            // drop what it holds if it wants to; a cancellation is not counted as a failure,
            // and neither is a receiver saying the cut has to be taken again
            if !matches!(
                error,
                SendError::Streaming(StreamingError::Closed(_)) | SendError::Behind(_)
            ) {
                network.shared.snapshots.borrow_mut().aborted += 1;
            }
            let end = SnapshotEnd {
                stream,
                total: manifest.total,
                checksum: 0,
                status: SnapshotStatus::Aborted,
                resume_from: from,
            }
            .encode();
            if let Ok(frame) = Frame::new(
                MessageType::SnapshotEnd,
                vec![bytes::Bytes::copy_from_slice(&end)],
                FrameKey::Bulk(0),
                max,
            ) {
                let _ = link.enqueue(frame);
            }
        }
        outcome
    }
}

/// What a repair transfer came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairSend {
    /// The receiver took the stream whole and is restarting the group to install it
    Installed,
    /// The receiver's checkpoint is at or past the boundary: cut again past it
    Behind {
        /// The receiver's checkpoint
        checkpoint: u64,
    },
}

/// Queue a frame on the bulk link, waiting out a full queue until the deadline or a cancellation
///
/// The bulk queue's bound is the flow-control window: a shed frame is offered again after a
/// short sleep rather than dropped, since the receiver's resume offset only recovers what the
/// lane lost, not what the sender never sent.
///
/// # Arguments
///
/// * `link` - The bulk link
/// * `frame` - The frame
/// * `cancel` - Resolves when openraft cancels the transfer
/// * `started` - When the transfer started
/// * `deadline` - How long it may take in all
async fn enqueue_or_wait(
    link: &peer::Link,
    mut frame: Frame,
    cancel: &mut Pin<Box<impl Future<Output = ReplicationClosed>>>,
    started: Instant,
    deadline: Duration,
) -> Result<(), StreamingError<DataConfig>> {
    let mut waited = 0u32;
    loop {
        // a link its owner let go writes nothing: fail now, and the retry dials afresh
        if link.is_closed() {
            return Err(unreachable(
                "the bulk link closed under the transfer".to_string(),
            ));
        }
        match link.enqueue(frame) {
            Ok(()) => return Ok(()),
            Err((back, _)) => frame = back,
        }
        waited += 1;
        if waited % 500 == 0 {
            event!(Level::DEBUG, msg = "a snapshot chunk is waiting for room on the bulk queue", waited, link = ?link.view());
        }
        if started.elapsed() >= deadline {
            return Err(unreachable(
                "the snapshot transfer ran out of time waiting for the bulk queue".to_string(),
            ));
        }
        // wait for room, or for the cancellation
        let sleep = glommio::timer::sleep(SNAPSHOT_SHED_BACKOFF);
        futures::pin_mut!(sleep);
        match futures::future::select(cancel.as_mut(), sleep).await {
            futures::future::Either::Left((closed, _)) => {
                return Err(StreamingError::Closed(closed))
            }
            futures::future::Either::Right(_) => {}
        }
    }
}

/// Decode an answer from the replication lane
///
/// # Arguments
///
/// * `bytes` - The answer
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StreamingError<DataConfig>> {
    postcard::from_bytes(bytes)
        .map_err(|error| unreachable(format!("decoding a snapshot answer: {error}")))
}

/// openraft's retriable failure for a snapshot transfer
///
/// # Arguments
///
/// * `msg` - What went wrong
fn unreachable(msg: String) -> StreamingError<DataConfig> {
    StreamingError::Unreachable(Unreachable::new(&LinkFailed { msg }))
}

/// A replication link that could not carry an RPC
#[derive(Debug)]
struct LinkFailed {
    /// What went wrong
    msg: String,
}

impl std::fmt::Display for LinkFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the replication link failed: {}", self.msg)
    }
}

impl std::error::Error for LinkFailed {}

#[cfg(test)]
mod tests {
    use super::{hop_budget, RateLimiter, HOP_MARGIN};
    use std::time::{Duration, Instant};

    /// The bucket admits a second's worth at once, then paces at the rate; zero is unlimited
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[test]
    fn a_rate_limiter_paces_a_stream() {
        let start = Instant::now();
        let mut bucket = RateLimiter::new(1000);
        // a full bucket takes a second's worth without waiting
        assert_eq!(bucket.take(600, start), None);
        assert_eq!(bucket.take(400, start), None);
        // empty: the next take waits for its bytes at the rate
        let wait = bucket.take(500, start).expect("an empty bucket waits");
        assert!((wait.as_secs_f64() - 0.5).abs() < 0.01, "{wait:?}");
        // and once the clock has moved that far the take goes through
        assert_eq!(bucket.take(500, start + Duration::from_millis(500)), None);
        // the bucket never holds more than a second's worth however long it rests
        assert_eq!(bucket.take(1000, start + Duration::from_secs(10)), None);
        assert!(bucket.take(1, start + Duration::from_secs(10)).is_some());
        // a take larger than the burst waits for the whole of it beyond what is held
        let mut bucket = RateLimiter::new(100);
        let wait = bucket.take(1000, start).expect("waits");
        assert!((wait.as_secs_f64() - 9.0).abs() < 0.01, "{wait:?}");
        // zero is no limit at all
        let mut unlimited = RateLimiter::new(0);
        assert_eq!(unlimited.take(u64::MAX, start), None);
    }

    /// A hop keeps a tenth of its budget back for the answer, capped, and never underflows
    /// ([Resolved #128](../../../../docs/src/appendix/resolved/hop-deadline-margin.md))
    #[test]
    fn a_hop_leaves_the_leader_less_than_it_waits() {
        // a short budget loses a tenth, not the whole fixed margin
        assert_eq!(
            hop_budget(Duration::from_millis(500)),
            Duration::from_millis(450)
        );
        // a long one loses the cap and no more
        assert_eq!(
            hop_budget(Duration::from_secs(5)),
            Duration::from_secs(5) - HOP_MARGIN
        );
        // whatever is left, the leader is told strictly less while there is anything to tell
        for millis in [1u64, 9, 10, 100, 2_499, 2_500, 60_000] {
            let remaining = Duration::from_millis(millis);
            assert!(hop_budget(remaining) < remaining, "{remaining:?}");
        }
        // nothing left is nothing handed on, not an underflow
        assert_eq!(hop_budget(Duration::ZERO), Duration::ZERO);
    }
}
