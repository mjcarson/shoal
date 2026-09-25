//! The control group's network: a `RaftNetworkV2` over the control lane
//!
//! At M1 this returned `Unreachable` for every peer, because a group of one never sends. M2
//! made it the real adapter ([F38](../../../../docs/src/features/inter-node-transport.md)):
//! openraft's append, vote and snapshot RPCs are serialized to JSON, framed as
//! [`ControlRequest`](crate::shared::protocol::peer::ControlRequestHead) frames with a
//! correlation id, and sent on a [`ControlLink`] to the peer's control listener, which drives
//! them into that peer's own `Raft` and answers under the same id. M3 made it dial the
//! **committed member record** openraft hands it rather than a static placement
//! ([F39](../../../../docs/src/features/membership.md)): a member's address is what the cluster
//! agreed it is, a `cluster.dial` override is where this node was told to reach it instead, and
//! a link whose address the member has moved away from is dropped and dialled afresh. The same
//! links carry the membership RPCs - a joiner's admission, a member's report, a proposal to the
//! leader - which the control loop sends through [`PeerNetwork::peer`].
//!
//! Everything here runs on the control thread's single executor, so it is `Rc`/`RefCell` by
//! construction: openraft's `single-threaded` feature empties every `Send`/`Sync` bound, and the
//! control lane never leaves the thread that owns the group. The lower layers are the data lane's,
//! reused unchanged - [`peer::codec`], [`peer::tls`], [`peer::handshake`] and [`peer::link::Link`],
//! whose reader already frames a `ControlResponse` beside a `Forwarded`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Duration;

use futures_channel::oneshot;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use tracing::{event, Level};

use super::store::SnapshotData;
use super::types::{ControlConfig, MemberRecord};
use crate::server::conf::cluster::{DialOverride, Transport};
use crate::server::peer::handshake::PeerAddr;
use crate::server::peer::{self, Frame, FrameKey, Lane, LinkEvent, Local};
use crate::shared::identity::NodeId;
use crate::shared::protocol::peer::{
    ControlKind, ControlRequestHead, ControlResponseHead, ControlStatus, PeerRefusal,
    CAP_PRE_VOTE_V1, CONTROL_HEAD_LEN,
};
use crate::shared::protocol::MessageType;
use crate::shared::tls::PeerTlsHolder;

/// What a control RPC's answer resolves to
enum ControlOutcome {
    /// The peer answered, and this is the response payload
    Ok(Vec<u8>),
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The link went down, or the queue was full, before an answer arrived
    Unreachable(String),
}

/// Why a control RPC produced no answer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcFailure {
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The link was down, its queue full, or the deadline passed
    Unreachable(String),
}

impl std::fmt::Display for RpcFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcFailure::Remote(msg) => write!(f, "the peer refused the rpc: {msg}"),
            RpcFailure::Unreachable(msg) => write!(f, "{msg}"),
        }
    }
}

/// One control connection to one peer, shared by every `ControlPeer` for that node
///
/// A bounded queue and a task that dials, shakes hands and carries frames, reused from the data
/// lane; plus a map of the RPCs in flight, keyed by the id each frame carries, each waiting on a
/// oneshot. A `ControlResponse` frame the link reads completes the matching oneshot; a link that
/// drops fails every one, because a control RPC that cannot be answered is one openraft retries.
struct ControlLink {
    /// The connection, from the data lane
    link: peer::Link,
    /// The RPCs in flight, by correlation id
    pending: Rc<RefCell<HashMap<u64, oneshot::Sender<ControlOutcome>>>>,
    /// The next correlation id to hand out
    next_id: Cell<u64>,
    /// The largest frame the peer accepts
    max_frame_bytes: u32,
}

impl ControlLink {
    /// Whether the peer answers pre-votes, or nothing while the link is not up
    ///
    /// ([Resolved #144](../../../../docs/src/appendix/resolved/post-heal-elections.md))
    fn answers_pre_votes(&self) -> Option<bool> {
        self.link
            .negotiated_if_up()
            .map(|negotiated| negotiated.has(CAP_PRE_VOTE_V1))
    }

    /// Open a control connection to a peer
    ///
    /// # Arguments
    ///
    /// * `entry` - Where to dial and who to expect there
    /// * `local` - What this node says about itself
    /// * `transport` - The bounds and timers
    /// * `tls` - What to dial with, read at every dial
    /// * `wires` - Where the newest wire version each peer's hello named is recorded
    /// * `removed` - Set when a peer refuses this node's hello as a removed identity
    fn new(
        entry: PeerAddr,
        local: Rc<RefCell<Local>>,
        transport: &Transport,
        tls: PeerTlsHolder,
        wires: Rc<RefCell<BTreeMap<NodeId, u8>>>,
        removed: Rc<Cell<bool>>,
    ) -> Self {
        let pending: Rc<RefCell<HashMap<u64, oneshot::Sender<ControlOutcome>>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let max_frame_bytes = local.borrow().max_frame_bytes;
        // the link delivers every answer to this closure, on the control executor
        let on_event = {
            let pending = pending.clone();
            move |event: LinkEvent| match event {
                // a control response completes the RPC that carries its id
                LinkEvent::Frame {
                    header,
                    head,
                    payload,
                    ..
                } => {
                    if header.kind != MessageType::ControlResponse {
                        return;
                    }
                    let Ok(raw) = <[u8; CONTROL_HEAD_LEN]>::try_from(&head[..]) else {
                        return;
                    };
                    let response = ControlResponseHead::decode(&raw);
                    if let Some(tx) = pending.borrow_mut().remove(&response.id) {
                        let outcome = match response.status {
                            ControlStatus::Ok => ControlOutcome::Ok(payload.to_vec()),
                            ControlStatus::Error => ControlOutcome::Remote(
                                String::from_utf8_lossy(&payload).into_owned(),
                            ),
                        };
                        let _ = tx.send(outcome);
                    }
                }
                // a dropped link fails every RPC in flight, unsent or written alike; a hello
                // refused as removed is a verdict on this node, remembered for the loop
                // ([F49](../../../../docs/src/features/backup-and-recovery.md))
                LinkEvent::Down {
                    reason, refused, ..
                } => {
                    if refused == Some(PeerRefusal::Removed) {
                        removed.set(true);
                    }
                    for (_, tx) in pending.borrow_mut().drain() {
                        let _ = tx.send(ControlOutcome::Unreachable(reason.clone()));
                    }
                }
                // a hello that completed says what the peer's build speaks
                // ([F48](../../../../docs/src/features/rolling-compatibility.md))
                LinkEvent::Up {
                    node, negotiated, ..
                } => {
                    if node != NodeId::default() {
                        wires.borrow_mut().insert(node, negotiated.peer_wire_max);
                    }
                }
            }
        };
        let link = peer::Link::spawn(Lane::Control, entry, local, transport, tls, on_event);
        ControlLink {
            link,
            pending,
            next_id: Cell::new(1),
            max_frame_bytes,
        }
    }

    /// Send one RPC and wait for its answer, or for the deadline
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `payload` - Its serialized request
    /// * `deadline` - How long to wait before giving up
    async fn rpc(
        &self,
        kind: ControlKind,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        // mint an id and a oneshot for the answer
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(id, tx);
        // truncation cannot happen for any deadline a control RPC uses
        #[allow(clippy::cast_possible_truncation)]
        let deadline_ms = deadline.as_millis().min(u128::from(u32::MAX)) as u32;
        let head = ControlRequestHead {
            id,
            kind,
            deadline_ms,
        }
        .encode();
        let frame = Frame::new(
            MessageType::ControlRequest,
            vec![
                bytes::Bytes::copy_from_slice(&head),
                bytes::Bytes::from(payload),
            ],
            FrameKey::Control(id),
            self.max_frame_bytes,
        )
        .map_err(|error| {
            RpcFailure::Unreachable(format!("framing a control request: {error:?}"))
        })?;
        // a queue that is full or a link that is down is a definite non-answer
        if self.link.enqueue(frame).is_err() {
            self.pending.borrow_mut().remove(&id);
            return Err(RpcFailure::Unreachable(
                "the control link's queue is full or its link is down".to_string(),
            ));
        }
        // wait for the answer, or the deadline, whichever comes first
        match glommio::timer::timeout(deadline, async { Ok(rx.await) }).await {
            Ok(Ok(ControlOutcome::Ok(payload))) => Ok(payload),
            Ok(Ok(ControlOutcome::Remote(msg))) => Err(RpcFailure::Remote(msg)),
            Ok(Ok(ControlOutcome::Unreachable(msg))) => Err(RpcFailure::Unreachable(msg)),
            // the sender was dropped without answering
            Ok(Err(_)) => {
                self.pending.borrow_mut().remove(&id);
                Err(RpcFailure::Unreachable(
                    "the control rpc was cancelled".to_string(),
                ))
            }
            // the deadline passed
            Err(_) => {
                self.pending.borrow_mut().remove(&id);
                Err(RpcFailure::Unreachable(
                    "the control rpc timed out".to_string(),
                ))
            }
        }
    }
}

/// The state every `ControlPeer` shares
struct Shared {
    /// One link per peer, opened on first use, keyed by where it dials
    links: RefCell<HashMap<String, Rc<ControlLink>>>,
    /// What this node says about itself
    local: Rc<RefCell<Local>>,
    /// Where particular members are dialled instead of where they advertise
    dial: BTreeMap<NodeId, DialOverride>,
    /// What to dial with, read at every dial so a reload reaches the next one
    tls: PeerTlsHolder,
    /// The bounds and timers
    transport: Transport,
    /// The newest wire version each peer's hello named, as this node's links heard it
    wires: Rc<RefCell<BTreeMap<NodeId, u8>>>,
    /// Whether a peer has refused this node's hello as a removed identity
    removed: Rc<Cell<bool>>,
    /// Where each member is dialled now, from the committed records as the plane applies them
    ///
    /// The library hands a client the record the member was admitted with; a member that
    /// restarted at another address is observed at a higher incarnation with a new record,
    /// and every RPC after the plane applies it dials there
    /// ([F50](../../../../docs/src/features/cluster-operations.md)).
    addresses: RefCell<BTreeMap<NodeId, PeerAddr>>,
}

/// The control group's network factory
///
/// Hands openraft a [`ControlPeer`] per target, each sharing the one [`ControlLink`] to that
/// node's control address.
#[derive(Clone)]
pub struct PeerNetwork {
    /// The shared state
    shared: Rc<Shared>,
}

impl PeerNetwork {
    /// Build the factory
    ///
    /// # Arguments
    ///
    /// * `local` - What this node says about itself
    /// * `dial` - Where particular members are dialled instead of where they advertise
    /// * `tls` - What to dial peers with, read at every dial
    /// * `transport` - The bounds and timers
    pub fn new(
        local: Rc<RefCell<Local>>,
        dial: BTreeMap<NodeId, DialOverride>,
        tls: PeerTlsHolder,
        transport: Transport,
    ) -> Self {
        PeerNetwork {
            shared: Rc::new(Shared {
                links: RefCell::new(HashMap::new()),
                local,
                dial,
                tls,
                transport,
                wires: Rc::new(RefCell::new(BTreeMap::new())),
                removed: Rc::new(Cell::new(false)),
                addresses: RefCell::new(BTreeMap::new()),
            }),
        }
    }

    /// Note where every member is dialled now, from the committed records
    ///
    /// # Arguments
    ///
    /// * `records` - Every member's committed record
    pub fn note_addresses<'a>(&self, records: impl Iterator<Item = &'a MemberRecord>) {
        let mut addresses = self.shared.addresses.borrow_mut();
        for record in records {
            let mut entry = self.addr_of(record);
            entry.node = Some(record.node);
            addresses.insert(record.node, entry);
        }
    }

    /// Where a member is dialled now: the committed record if the plane has applied one, else
    /// what the caller was given
    ///
    /// # Arguments
    ///
    /// * `target` - The member
    /// * `given` - The address the library or the caller named
    fn current_address(&self, target: NodeId, given: &PeerAddr) -> PeerAddr {
        self.shared
            .addresses
            .borrow()
            .get(&target)
            .cloned()
            .unwrap_or_else(|| given.clone())
    }

    /// Whether a peer has refused this node's hello as a removed identity
    ///
    /// A verdict on the identity rather than on a connection: the control loop stops on it,
    /// the way it stops on a `Removed` answer to its own observation
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md)).
    #[must_use]
    pub fn refused_as_removed(&self) -> bool {
        self.shared.removed.get()
    }

    /// The newest wire version each peer's hello named, as this node's links heard it
    ///
    /// One of the two live sources an activation is judged by; the other is the status
    /// reports the leader receives ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    #[must_use]
    pub fn wires(&self) -> BTreeMap<NodeId, u8> {
        self.shared.wires.borrow().clone()
    }

    /// Whether this node can reach nobody over the control lane
    ///
    /// True when this node has dialled at least one member and none of its links is up: every
    /// one is cut, reconnecting or backing off. A node that has dialled nobody yet is not
    /// isolated, it is starting. What a member does with the answer is stop standing for
    /// elections it cannot win
    /// ([Resolved #106](../../../../docs/src/appendix/resolved/isolated-member-term-inflation.md)).
    #[must_use]
    pub fn is_isolated(&self) -> bool {
        let links = self.shared.links.borrow();
        // a node with no link has nobody to be cut off from
        !links.is_empty() && links.values().all(|link| !link.link.is_up())
    }

    /// Where to dial a member, from its committed record and this node's overrides
    ///
    /// # Arguments
    ///
    /// * `record` - The member's committed record
    #[must_use]
    pub fn addr_of(&self, record: &MemberRecord) -> PeerAddr {
        // a node runs fewer shards than a u16 holds; the ring refuses more
        #[allow(clippy::cast_possible_truncation)]
        let mut addr = PeerAddr {
            node: Some(record.node),
            data: record.data.clone(),
            control: record.control.clone(),
            shards: record.shards as u16,
        };
        if let Some(target) = self.shared.dial.get(&record.node) {
            if let Some(control) = &target.control {
                addr.control.clone_from(control);
            }
            if let Some(data) = &target.data {
                addr.data.clone_from(data);
            }
        }
        addr
    }

    /// Get or open the control link to an address
    ///
    /// Keyed by the address rather than the node, so a member that moved gets a fresh link and
    /// a seed that turned out to be a member is not dialled twice.
    ///
    /// # Arguments
    ///
    /// * `entry` - Where to dial and who to expect
    fn link(&self, entry: &PeerAddr) -> Rc<ControlLink> {
        if let Some(link) = self.shared.links.borrow().get(&entry.control) {
            // the same address dialled for another node is a member that was replaced there
            if *link.link.target() == *entry {
                return link.clone();
            }
        }
        let link = Rc::new(ControlLink::new(
            entry.clone(),
            self.shared.local.clone(),
            &self.shared.transport,
            self.shared.tls.clone(),
            self.shared.wires.clone(),
            self.shared.removed.clone(),
        ));
        self.shared
            .links
            .borrow_mut()
            .insert(entry.control.clone(), link.clone());
        link
    }

    /// A peer at an address, for the control loop's own RPCs
    ///
    /// # Arguments
    ///
    /// * `entry` - Where to dial and who to expect
    #[must_use]
    pub fn peer(&self, entry: &PeerAddr) -> ControlPeer {
        ControlPeer {
            target: entry.node_or_nil(),
            given: entry.clone(),
            network: self.clone(),
        }
    }

    /// This node's identity
    #[must_use]
    pub fn local_node(&self) -> NodeId {
        self.shared.local.borrow().node
    }

    /// Drop the link to an address, so the next RPC dials afresh
    ///
    /// # Arguments
    ///
    /// * `control` - The address
    pub fn forget(&self, control: &str) {
        self.shared.links.borrow_mut().remove(control);
    }

    /// How many links are open
    #[must_use]
    pub fn link_count(&self) -> usize {
        self.shared.links.borrow().len()
    }
}

impl RaftNetworkFactory<ControlConfig> for PeerNetwork {
    type Network = ControlPeer;

    /// A client for a peer, dialling the address the committed record names
    async fn new_client(&mut self, target: NodeId, node: &MemberRecord) -> Self::Network {
        let mut entry = self.addr_of(node);
        entry.node = Some(target);
        ControlPeer {
            target,
            given: entry,
            network: self.clone(),
        }
    }
}

/// The network to one peer
///
/// Holds no link of its own: every RPC looks the link up by where the member is dialled
/// *now*, so a member that moved is reached at its new address as soon as the plane applies
/// its record, whatever address the library handed this client
/// ([F50](../../../../docs/src/features/cluster-operations.md)).
pub struct ControlPeer {
    /// Who it reaches, or the nil id for a seed
    target: NodeId,
    /// The address this client was made for, dialled until a committed record says otherwise
    given: PeerAddr,
    /// The factory, whose links and addresses are shared
    network: PeerNetwork,
}

impl ControlPeer {
    /// The link to the peer as it is dialled now
    fn link(&self) -> Rc<ControlLink> {
        // a seed named no node, and is dialled where it was given
        if self.target == NodeId::default() {
            return self.network.link(&self.given);
        }
        let entry = self.network.current_address(self.target, &self.given);
        self.network.link(&entry)
    }

    /// Turn a link error into openraft's retriable unreachable
    ///
    /// # Arguments
    ///
    /// * `failure` - What went wrong
    fn unreachable(failure: RpcFailure) -> RPCError<ControlConfig> {
        RPCError::Unreachable(Unreachable::new(&LinkFailed {
            msg: failure.to_string(),
        }))
    }

    /// Who this peer reaches
    #[must_use]
    pub fn target(&self) -> NodeId {
        self.target
    }

    /// Send one control RPC and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `kind` - Which RPC this is
    /// * `payload` - Its serialized request
    /// * `deadline` - How long to wait
    ///
    /// # Errors
    ///
    /// Says whether the peer refused it or could not be reached.
    pub async fn rpc(
        &self,
        kind: ControlKind,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcFailure> {
        self.link().rpc(kind, payload, deadline).await
    }

    /// Ping the peer over the control lane, proving its listener answers
    ///
    /// A liveness probe with no consensus meaning: the peer's control listener answers it with
    /// its incarnation and topology version, which this hands back.
    pub async fn ping(&mut self) -> Result<Vec<u8>, String> {
        self.link()
            .rpc(ControlKind::Ping, Vec::new(), Duration::from_secs(5))
            .await
            .map_err(|failure| failure.to_string())
    }
}

impl RaftNetworkV2<ControlConfig> for ControlPeer {
    type SnapshotData = SnapshotData;

    /// Append entries: serialize, send, deserialize the response
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<ControlConfig>, RPCError<ControlConfig>> {
        let payload = serde_json::to_vec(&rpc).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!(
                "encoding append_entries: {error}"
            )))
        })?;
        let answer = self
            .link()
            .rpc(ControlKind::AppendEntries, payload, option.hard_ttl())
            .await
            .map_err(Self::unreachable)?;
        serde_json::from_slice(&answer).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!(
                "decoding append_entries: {error}"
            )))
        })
    }

    /// Vote: serialize, send, deserialize the response
    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlConfig>, RPCError<ControlConfig>> {
        let payload = serde_json::to_vec(&rpc).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!("encoding vote: {error}")))
        })?;
        let answer = self
            .link()
            .rpc(ControlKind::Vote, payload, option.hard_ttl())
            .await
            .map_err(Self::unreachable)?;
        serde_json::from_slice(&answer).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!("decoding vote: {error}")))
        })
    }

    /// Ask the member whether it would grant a vote at the next term, without it moving its term
    ///
    /// A member whose build does not answer pre-votes is granted locally, openraft's own
    /// default; one that cannot be reached is an error, never a grant, or a node cut off from
    /// every peer would grant itself a quorum and stand anyway
    /// ([Resolved #144](../../../../docs/src/appendix/resolved/post-heal-elections.md)).
    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ControlConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlConfig>, RPCError<ControlConfig>> {
        // whether the member's build answers a pre-vote, judged on the link it would go over
        let link = self.link();
        match link.answers_pre_votes() {
            // up with a peer that answers them: ask it
            Some(true) => (),
            // up with an older build: grant, as openraft does for a network without pre-vote
            Some(false) => return Ok(VoteResponse::new(rpc.vote, None, true)),
            // down: no answer, and so no grant
            None => {
                return Err(Self::unreachable(RpcFailure::Unreachable(format!(
                    "the control link to {} is not up",
                    self.target
                ))))
            }
        }
        let payload = serde_json::to_vec(&rpc).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!("encoding pre_vote: {error}")))
        })?;
        let answer = link
            .rpc(ControlKind::PreVote, payload, option.hard_ttl())
            .await
            .map_err(Self::unreachable)?;
        serde_json::from_slice(&answer).map_err(|error| {
            Self::unreachable(RpcFailure::Unreachable(format!("decoding pre_vote: {error}")))
        })
    }

    /// Install a full snapshot in one request
    ///
    /// The whole snapshot - the vote, the metadata and the bytes - rides in one control request.
    /// A joiner that arrives after the leader purged its log is the first thing that exercised it.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlConfig>,
        snapshot: SnapshotOf<ControlConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ControlConfig>, StreamingError<ControlConfig>> {
        // vote, metadata and bytes, length-prefixed so the listener can split them
        let payload = encode_snapshot(&vote, &snapshot).map_err(|error| {
            StreamingError::Unreachable(Unreachable::new(&LinkFailed { msg: error }))
        })?;
        let answer = self
            .link()
            .rpc(ControlKind::Snapshot, payload, option.hard_ttl())
            .await
            .map_err(|failure| {
                StreamingError::Unreachable(Unreachable::new(&LinkFailed {
                    msg: failure.to_string(),
                }))
            })?;
        serde_json::from_slice(&answer).map_err(|error| {
            StreamingError::Unreachable(Unreachable::new(&LinkFailed {
                msg: format!("decoding a snapshot response: {error}"),
            }))
        })
    }
}

/// Encode a full snapshot for one control request
///
/// `[u32 vote_len][vote json][u32 meta_len][meta json][snapshot bytes]`.
///
/// # Arguments
///
/// * `vote` - The sender's vote
/// * `snapshot` - The snapshot to send
fn encode_snapshot(
    vote: &VoteOf<ControlConfig>,
    snapshot: &SnapshotOf<ControlConfig, SnapshotData>,
) -> Result<Vec<u8>, String> {
    let vote_json = serde_json::to_vec(vote).map_err(|error| format!("vote: {error}"))?;
    let meta_json =
        serde_json::to_vec(&snapshot.meta).map_err(|error| format!("snapshot meta: {error}"))?;
    let bytes = snapshot.snapshot.get_ref();
    let mut out = Vec::with_capacity(8 + vote_json.len() + meta_json.len() + bytes.len());
    // truncation cannot happen: neither JSON nor a control snapshot passes a u32
    #[allow(clippy::cast_possible_truncation)]
    {
        out.extend_from_slice(&(vote_json.len() as u32).to_le_bytes());
        out.extend_from_slice(&vote_json);
        out.extend_from_slice(&(meta_json.len() as u32).to_le_bytes());
        out.extend_from_slice(&meta_json);
    }
    out.extend_from_slice(bytes);
    Ok(out)
}

/// Decode a full snapshot on the receiving side
///
/// The inverse of [`encode_snapshot`].
///
/// # Arguments
///
/// * `raw` - The request payload
pub fn decode_snapshot(
    raw: &[u8],
) -> Result<
    (
        VoteOf<ControlConfig>,
        SnapshotOf<ControlConfig, SnapshotData>,
    ),
    String,
> {
    let read_len = |raw: &[u8], at: usize| -> Result<usize, String> {
        raw.get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
            .ok_or_else(|| "a snapshot request is truncated".to_string())
    };
    let vote_len = read_len(raw, 0)?;
    let vote_end = 4 + vote_len;
    let vote: VoteOf<ControlConfig> =
        serde_json::from_slice(raw.get(4..vote_end).ok_or("snapshot vote truncated")?)
            .map_err(|error| format!("snapshot vote: {error}"))?;
    let meta_len = read_len(raw, vote_end)?;
    let meta_start = vote_end + 4;
    let meta_end = meta_start + meta_len;
    let meta: SnapshotMetaOf<ControlConfig> = serde_json::from_slice(
        raw.get(meta_start..meta_end)
            .ok_or("snapshot meta truncated")?,
    )
    .map_err(|error| format!("snapshot meta: {error}"))?;
    let bytes = raw
        .get(meta_end..)
        .ok_or("snapshot bytes truncated")?
        .to_vec();
    Ok((
        vote,
        Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        },
    ))
}

/// A control link that could not carry an RPC
#[derive(Debug)]
struct LinkFailed {
    /// What went wrong
    msg: String,
}

impl std::fmt::Display for LinkFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the control link failed: {}", self.msg)
    }
}

impl std::error::Error for LinkFailed {}

/// Log that the network was built, for the control thread's startup trace
pub fn built(links: usize) {
    event!(Level::DEBUG, msg = "control network ready", links);
}
