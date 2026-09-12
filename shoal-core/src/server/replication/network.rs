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
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::oneshot;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use rustls::ClientConfig;

use super::machine::SnapshotData;
use super::types::DataConfig;
use crate::server::conf::cluster::{DialOverride, Transport};
use crate::server::map::MapCell;
use crate::server::peer::handshake::PeerAddr;
use crate::server::peer::{self, Frame, FrameKey, Lane, LinkEvent, LinkView, Local};
use crate::shared::identity::{GroupId, NodeId, ShardAddr};
use crate::shared::protocol::peer::{
    ReplicateKind, ReplicateRequestHead, ReplicateResponseHead, ReplicateStatus,
    REPLICATE_RESPONSE_HEAD_LEN,
};
use crate::shared::protocol::MessageType;

/// What a replication RPC's answer resolves to
enum Outcome {
    /// The peer answered, and this is the response payload
    Ok(Vec<u8>),
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The link went down, or the queue was full, before an answer arrived
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
}

impl ReplicationLink {
    /// Open a replication connection to a peer node
    ///
    /// # Arguments
    ///
    /// * `entry` - Where to dial and who to expect there
    /// * `local` - What this node says about itself
    /// * `transport` - The bounds and timers
    /// * `tls` - What to dial with, if the lanes are encrypted
    /// * `on_event` - Where the link delivers what it learns
    fn new<F: Fn(LinkEvent) + 'static>(
        entry: PeerAddr,
        local: Rc<RefCell<Local>>,
        transport: &Transport,
        tls: Option<Arc<ClientConfig>>,
        on_event: F,
    ) -> Self {
        let max_frame_bytes = local.borrow().max_frame_bytes;
        let link = peer::Link::spawn(Lane::Replication, entry, local, transport, tls, on_event);
        ReplicationLink {
            link,
            pending: Rc::new(RefCell::new(HashMap::new())),
            next_id: Cell::new(1),
            max_frame_bytes,
        }
    }

    /// Complete the RPC a response frame answers
    ///
    /// # Arguments
    ///
    /// * `head` - The response's head
    /// * `payload` - Its payload
    fn answered(&self, head: &ReplicateResponseHead, payload: Vec<u8>) {
        if let Some(tx) = self.pending.borrow_mut().remove(&head.id) {
            let outcome = match head.status {
                ReplicateStatus::Ok => Outcome::Ok(payload),
                ReplicateStatus::Error => Outcome::Remote(String::from_utf8_lossy(&payload).into_owned()),
            };
            let _ = tx.send(outcome);
        }
    }

    /// Fail every RPC in flight, because the link dropped
    ///
    /// # Arguments
    ///
    /// * `reason` - Why
    fn down(&self, reason: &str) {
        for (_, tx) in self.pending.borrow_mut().drain() {
            let _ = tx.send(Outcome::Unreachable(reason.to_string()));
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
        // mint an id and a oneshot for the answer
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(id, tx);
        // truncation cannot happen for any deadline a replication RPC uses
        #[allow(clippy::cast_possible_truncation)]
        let deadline_ms = deadline.as_millis().min(u128::from(u32::MAX)) as u32;
        let head = ReplicateRequestHead {
            id,
            group: group.0,
            target_shard,
            kind,
            deadline_ms,
        }
        .encode();
        let frame = Frame::new(
            MessageType::Replicate,
            vec![bytes::Bytes::copy_from_slice(&head), bytes::Bytes::from(payload)],
            FrameKey::Replication(id),
            self.max_frame_bytes,
        )
        .map_err(|error| RpcFailure::Unreachable(format!("framing a replication request: {error:?}")))?;
        // a queue that is full or a link that is down is a definite non-answer
        if self.link.enqueue(frame).is_err() {
            self.pending.borrow_mut().remove(&id);
            return Err(RpcFailure::NotSent(
                "the replication link's queue is full or its link is down".to_string(),
            ));
        }
        // wait for the answer, or the deadline, whichever comes first
        match glommio::timer::timeout(deadline, async { Ok(rx.await) }).await {
            Ok(Ok(Outcome::Ok(payload))) => Ok(payload),
            Ok(Ok(Outcome::Remote(msg))) => Err(RpcFailure::Remote(msg)),
            Ok(Ok(Outcome::Unreachable(msg))) => Err(RpcFailure::Unreachable(msg)),
            Ok(Err(_)) => {
                self.pending.borrow_mut().remove(&id);
                Err(RpcFailure::Unreachable("the replication rpc was cancelled".to_string()))
            }
            Err(_) => {
                self.pending.borrow_mut().remove(&id);
                Err(RpcFailure::Unreachable("the replication rpc timed out".to_string()))
            }
        }
    }

    /// What this link looks like from outside
    #[must_use]
    pub fn view(&self) -> LinkView {
        self.link.view()
    }
}

/// The state every `ShardPeer` shares
struct Shared {
    /// One link per peer node, opened on first use
    links: RefCell<HashMap<NodeId, Rc<ReplicationLink>>>,
    /// The map this shard holds, which is where every member's address comes from
    map: MapCell,
    /// Where particular members are dialled instead of where they advertise
    dial: BTreeMap<NodeId, DialOverride>,
    /// What this node says about itself
    local: Rc<RefCell<Local>>,
    /// What to dial with, if the lanes are encrypted
    tls: Option<Arc<ClientConfig>>,
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
    /// * `tls` - What to dial peers with, if encrypted
    /// * `transport` - The bounds and timers
    /// * `on_event` - Where a link delivers what it learns
    #[must_use]
    pub fn new(
        map: MapCell,
        dial: BTreeMap<NodeId, DialOverride>,
        local: Rc<RefCell<Local>>,
        tls: Option<Arc<ClientConfig>>,
        transport: Transport,
        on_event: Rc<dyn Fn(LinkEvent)>,
    ) -> Self {
        ShardNetwork {
            shared: Rc::new(Shared {
                links: RefCell::new(HashMap::new()),
                map,
                dial,
                local,
                tls,
                transport,
                on_event,
            }),
        }
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

    /// Complete the RPC a response frame from a node answers
    ///
    /// # Arguments
    ///
    /// * `node` - The peer that answered
    /// * `head` - The response's head
    /// * `payload` - Its payload
    pub fn answered(&self, node: NodeId, head: &[u8], payload: Vec<u8>) {
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
    pub fn down(&self, node: NodeId, reason: &str) {
        if let Some(link) = self.shared.links.borrow().get(&node) {
            link.down(reason);
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
        self.shared.links.borrow().values().map(|link| link.view()).collect()
    }

    /// This node's identity
    #[must_use]
    pub fn local_node(&self) -> NodeId {
        self.shared.local.borrow().node
    }
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
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `payload` - The command's bytes
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn propose(&self, group: GroupId, payload: Vec<u8>, deadline: Duration) -> Result<Vec<u8>, RpcFailure> {
        self.rpc(ReplicateKind::Propose, group, payload, deadline).await
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
        link.rpc(kind, group, self.target.shard, payload, deadline).await
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
            ShardPeer::unreachable(RpcFailure::Unreachable(format!("encoding append_entries: {error}")))
        })?;
        let answer = self
            .peer
            .rpc(ReplicateKind::AppendEntries, self.group, payload, option.hard_ttl())
            .await
            .map_err(ShardPeer::unreachable)?;
        postcard::from_bytes(&answer).map_err(|error| {
            ShardPeer::unreachable(RpcFailure::Unreachable(format!("decoding append_entries: {error}")))
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

    /// A full snapshot is catch-up past the purge point, which M7 delivers
    ///
    /// Answered unreachable rather than sent: the receiver could not install it, and openraft
    /// treats unreachable as a reason to try again later rather than as a fault of the group.
    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<DataConfig>,
        _snapshot: SnapshotOf<DataConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<DataConfig>, StreamingError<DataConfig>> {
        Err(StreamingError::Unreachable(Unreachable::new(&LinkFailed {
            msg: "a tablet group snapshot cannot be sent: catch-up past the purge point is M7's".to_string(),
        })))
    }
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
