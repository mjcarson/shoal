//! The control group's network: a `RaftNetworkV2` over the control lane
//!
//! At M1 this returned `Unreachable` for every peer, because a group of one never sends. M2
//! replaces it with the real adapter ([F38](../../../../docs/src/features/inter-node-transport.md)):
//! openraft's append, vote and snapshot RPCs are serialized to JSON, framed as
//! [`ControlRequest`](crate::shared::protocol::peer::ControlRequestHead) frames with a
//! correlation id, and sent on a [`ControlLink`] to the peer's control listener, which drives
//! them into that peer's own `Raft` and answers under the same id.
//!
//! Everything here runs on the control thread's single executor, so it is `Rc`/`RefCell` by
//! construction: openraft's `single-threaded` feature empties every `Send`/`Sync` bound, and the
//! control lane never leaves the thread that owns the group. The lower layers are the data lane's,
//! reused unchanged - [`peer::codec`], [`peer::tls`], [`peer::handshake`] and [`peer::link::Link`],
//! whose reader already frames a `ControlResponse` beside a `Forwarded`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::io::Cursor;
use std::rc::Rc;
use std::sync::Arc;
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
use rustls::ClientConfig;
use tracing::{event, Level};

use super::store::SnapshotData;
use super::types::{ControlConfig, MemberRecord};
use crate::server::conf::cluster::{Placement, Transport};
use crate::server::peer::{self, Frame, FrameKey, Lane, LinkEvent, Local};
use crate::shared::identity::NodeId;
use crate::shared::protocol::peer::{
    ControlKind, ControlRequestHead, ControlResponseHead, ControlStatus, CONTROL_HEAD_LEN,
};
use crate::shared::protocol::MessageType;

/// What a control RPC's answer resolves to
enum ControlOutcome {
    /// The peer answered, and this is the response payload
    Ok(Vec<u8>),
    /// The peer answered with a failure, and this is what it said
    Remote(String),
    /// The link went down, or the queue was full, before an answer arrived
    Unreachable(String),
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
    /// Open a control connection to a peer
    ///
    /// # Arguments
    ///
    /// * `entry` - The peer's placement entry, which is where to dial and who to expect
    /// * `local` - What this node says about itself
    /// * `transport` - The bounds and timers
    /// * `tls` - What to dial with, if the lanes are encrypted
    fn new(
        entry: crate::server::conf::cluster::PlacedNode,
        local: Local,
        transport: &Transport,
        tls: Option<Arc<ClientConfig>>,
    ) -> Self {
        let pending: Rc<RefCell<HashMap<u64, oneshot::Sender<ControlOutcome>>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let max_frame_bytes = local.max_frame_bytes;
        // the link delivers every answer to this closure, on the control executor
        let on_event = {
            let pending = pending.clone();
            move |event: LinkEvent| match event {
                // a control response completes the RPC that carries its id
                LinkEvent::Frame { header, head, payload, .. } => {
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
                            ControlStatus::Error => {
                                ControlOutcome::Remote(String::from_utf8_lossy(&payload).into_owned())
                            }
                        };
                        let _ = tx.send(outcome);
                    }
                }
                // a dropped link fails every RPC in flight, unsent or written alike
                LinkEvent::Down { reason, .. } => {
                    for (_, tx) in pending.borrow_mut().drain() {
                        let _ = tx.send(ControlOutcome::Unreachable(reason.clone()));
                    }
                }
                LinkEvent::Up { .. } => {}
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
    ) -> Result<Vec<u8>, String> {
        // mint an id and a oneshot for the answer
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(id, tx);
        // truncation cannot happen for any deadline a control RPC uses
        #[allow(clippy::cast_possible_truncation)]
        let deadline_ms = deadline.as_millis().min(u128::from(u32::MAX)) as u32;
        let head = ControlRequestHead { id, kind, deadline_ms }.encode();
        let frame = Frame::new(
            MessageType::ControlRequest,
            vec![
                bytes::Bytes::copy_from_slice(&head),
                bytes::Bytes::from(payload),
            ],
            FrameKey::Control(id),
            self.max_frame_bytes,
        )
        .map_err(|error| format!("framing a control request: {error:?}"))?;
        // a queue that is full or a link that is down is a definite non-answer
        if self.link.enqueue(frame).is_err() {
            self.pending.borrow_mut().remove(&id);
            return Err("the control link's queue is full or its link is down".to_string());
        }
        // wait for the answer, or the deadline, whichever comes first
        match glommio::timer::timeout(deadline, async { Ok(rx.await) }).await {
            Ok(Ok(ControlOutcome::Ok(payload))) => Ok(payload),
            Ok(Ok(ControlOutcome::Remote(msg))) => Err(format!("the peer refused the rpc: {msg}")),
            Ok(Ok(ControlOutcome::Unreachable(msg))) => Err(msg),
            // the sender was dropped without answering
            Ok(Err(_)) => {
                self.pending.borrow_mut().remove(&id);
                Err("the control rpc was cancelled".to_string())
            }
            // the deadline passed
            Err(_) => {
                self.pending.borrow_mut().remove(&id);
                Err("the control rpc timed out".to_string())
            }
        }
    }
}

/// The state every `ControlPeer` shares
struct Shared {
    /// One link per peer, opened on first use
    links: RefCell<HashMap<NodeId, Rc<ControlLink>>>,
    /// Every node this factory may dial
    placement: Rc<Placement>,
    /// What this node says about itself
    local: Local,
    /// What to dial with, if the lanes are encrypted
    tls: Option<Arc<ClientConfig>>,
    /// The bounds and timers
    transport: Transport,
}

/// The control group's network factory
///
/// Hands openraft a [`ControlPeer`] per target, each sharing the one [`ControlLink`] to that node.
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
    /// * `placement` - Every node this node may dial
    /// * `local` - What this node says about itself
    /// * `tls` - What to dial peers with, if encrypted
    /// * `transport` - The bounds and timers
    pub fn new(
        placement: Rc<Placement>,
        local: Local,
        tls: Option<Arc<ClientConfig>>,
        transport: Transport,
    ) -> Self {
        PeerNetwork {
            shared: Rc::new(Shared {
                links: RefCell::new(HashMap::new()),
                placement,
                local,
                tls,
                transport,
            }),
        }
    }

    /// Get or open the control link to a peer
    ///
    /// # Arguments
    ///
    /// * `target` - The peer
    fn link(&self, target: NodeId) -> Option<Rc<ControlLink>> {
        if let Some(link) = self.shared.links.borrow().get(&target) {
            return Some(link.clone());
        }
        // the control lane dials the placement's control address, keyed by node id, so it never
        // trusts openraft's own record of where a peer is
        let entry = self.shared.placement.peer(target)?.clone();
        let link = Rc::new(ControlLink::new(
            entry,
            self.shared.local.clone(),
            &self.shared.transport,
            self.shared.tls.clone(),
        ));
        self.shared.links.borrow_mut().insert(target, link.clone());
        Some(link)
    }
}

impl RaftNetworkFactory<ControlConfig> for PeerNetwork {
    type Network = ControlPeer;

    /// A client for a peer, which dials lazily on its first RPC
    async fn new_client(&mut self, target: NodeId, _node: &MemberRecord) -> Self::Network {
        ControlPeer {
            target,
            link: self.link(target),
        }
    }
}

/// The network to one peer
pub struct ControlPeer {
    /// Who it reaches
    target: NodeId,
    /// The link, or nothing if the peer is not in the placement
    link: Option<Rc<ControlLink>>,
}

impl ControlPeer {
    /// The error a peer that is not in the placement answers every RPC with
    fn no_placement(&self) -> RPCError<ControlConfig> {
        RPCError::Unreachable(Unreachable::new(&NotPlaced { target: self.target }))
    }

    /// Turn a link error into openraft's retriable unreachable
    ///
    /// # Arguments
    ///
    /// * `msg` - What went wrong
    fn unreachable(msg: String) -> RPCError<ControlConfig> {
        RPCError::Unreachable(Unreachable::new(&LinkFailed { msg }))
    }

    /// Ping the peer over the control lane, proving its listener answers
    ///
    /// A liveness probe with no consensus meaning: the peer's control listener answers it with
    /// its incarnation and topology version, which this discards - the round trip is the point.
    pub async fn ping(&mut self) -> Result<(), String> {
        let Some(link) = &self.link else {
            return Err(format!("{} is not in this node's placement", self.target));
        };
        link.rpc(ControlKind::Ping, Vec::new(), std::time::Duration::from_secs(5))
            .await
            .map(|_| ())
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
        let Some(link) = &self.link else {
            return Err(self.no_placement());
        };
        let payload = serde_json::to_vec(&rpc)
            .map_err(|error| Self::unreachable(format!("encoding append_entries: {error}")))?;
        let answer = link
            .rpc(ControlKind::AppendEntries, payload, option.hard_ttl())
            .await
            .map_err(Self::unreachable)?;
        serde_json::from_slice(&answer)
            .map_err(|error| Self::unreachable(format!("decoding append_entries: {error}")))
    }

    /// Vote: serialize, send, deserialize the response
    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlConfig>, RPCError<ControlConfig>> {
        let Some(link) = &self.link else {
            return Err(self.no_placement());
        };
        let payload = serde_json::to_vec(&rpc)
            .map_err(|error| Self::unreachable(format!("encoding vote: {error}")))?;
        let answer = link
            .rpc(ControlKind::Vote, payload, option.hard_ttl())
            .await
            .map_err(Self::unreachable)?;
        serde_json::from_slice(&answer)
            .map_err(|error| Self::unreachable(format!("decoding vote: {error}")))
    }

    /// Install a full snapshot in one request
    ///
    /// Real but unexercised at M2: a group of one never installs a snapshot on a follower, and
    /// the chunked stream C2 describes is the bulk lane's, for M7. The whole snapshot - the vote,
    /// the metadata and the bytes - rides in one control request here.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlConfig>,
        snapshot: SnapshotOf<ControlConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ControlConfig>, StreamingError<ControlConfig>> {
        let Some(link) = &self.link else {
            return Err(StreamingError::Unreachable(Unreachable::new(&NotPlaced {
                target: self.target,
            })));
        };
        // vote, metadata and bytes, length-prefixed so the listener can split them
        let payload = encode_snapshot(&vote, &snapshot).map_err(|error| {
            StreamingError::Unreachable(Unreachable::new(&LinkFailed { msg: error }))
        })?;
        let answer = link
            .rpc(ControlKind::Snapshot, payload, option.hard_ttl())
            .await
            .map_err(|msg| StreamingError::Unreachable(Unreachable::new(&LinkFailed { msg })))?;
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
) -> Result<(VoteOf<ControlConfig>, SnapshotOf<ControlConfig, SnapshotData>), String> {
    let read_len = |raw: &[u8], at: usize| -> Result<usize, String> {
        raw.get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
            .ok_or_else(|| "a snapshot request is truncated".to_string())
    };
    let vote_len = read_len(raw, 0)?;
    let vote_end = 4 + vote_len;
    let vote: VoteOf<ControlConfig> = serde_json::from_slice(
        raw.get(4..vote_end).ok_or("snapshot vote truncated")?,
    )
    .map_err(|error| format!("snapshot vote: {error}"))?;
    let meta_len = read_len(raw, vote_end)?;
    let meta_start = vote_end + 4;
    let meta_end = meta_start + meta_len;
    let meta: SnapshotMetaOf<ControlConfig> = serde_json::from_slice(
        raw.get(meta_start..meta_end).ok_or("snapshot meta truncated")?,
    )
    .map_err(|error| format!("snapshot meta: {error}"))?;
    let bytes = raw.get(meta_end..).ok_or("snapshot bytes truncated")?.to_vec();
    Ok((vote, Snapshot { meta, snapshot: Cursor::new(bytes) }))
}

/// A peer that is not in this node's placement
#[derive(Debug)]
struct NotPlaced {
    /// Who was asked for
    target: NodeId,
}

impl std::fmt::Display for NotPlaced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is not in this node's placement", self.target)
    }
}

impl std::error::Error for NotPlaced {}

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
pub fn built(peers: usize) {
    event!(Level::DEBUG, msg = "control network ready", peers);
}
