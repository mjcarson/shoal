//! Accepting data and bulk lanes from peers, on every shard
//!
//! Every shard binds the node's peer port beside its client port, with `SO_REUSEPORT` the way
//! the client listeners do, so the kernel spreads incoming lanes over the shards and no shard is
//! the node's one door. A lane that lands on a shard is served by that shard: it validates every
//! forwarded bundle - this is a process boundary, and nothing that arrived over a socket is ever
//! accessed unchecked - and hands each entry to the shard it names over the same mesh a client's
//! query takes.
//!
//! Answers go back the way a client's do. The connection is announced to every shard as a client
//! with a channel of its own, so the shard that executes a forwarded query replies into that
//! channel exactly as it would to a client, and the write relay here frames what it is handed as
//! a `Forwarded` instead of a `Response`. That is the whole reason a peer connection is a
//! "client" to the shards: one reply path, not two.
//!
//! **In-flight bytes are bounded.** A forwarded bundle counts against the connection until every
//! entry of it has been answered, and a connection at its bound stops reading. The peer's queue
//! then fills and sheds at its own bound, which is how one peer's appetite becomes that peer's
//! problem rather than this node's memory.

use futures::io::{ReadHalf, WriteHalf};
use futures::AsyncReadExt;
use glommio::net::{TcpListener, TcpStream};
use kanal::{AsyncReceiver, AsyncSender};
use rustls::ServerConfig;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tracing::{event, Level};
use uuid::Uuid;

use super::codec;
use super::handshake::{self, Local};
use super::Lane;
use crate::server::comms::Comms;
use crate::server::map::MapCell;
use crate::server::database::ShoalDatabase;
use crate::server::messages::{Reply, ReplyKind, ServerMsg};
use crate::server::request_body::RequestBody;
use crate::server::stage_profile::{self, Stamp};
use crate::server::ServerError;
use crate::shared::identity::NodeId;
use crate::shared::protocol::peer::{
    self, ForwardPreamble, ForwardedKind, ForwardedPreamble, ReplicateRequestHead,
    ReplicateResponseHead, ReplicateStatus, SnapshotBegin, SnapshotChunk, SnapshotEnd,
    FORWARD_PREAMBLE_LEN, REPLICATE_HEAD_LEN, SNAPSHOT_BEGIN_LEN, SNAPSHOT_CHUNK_LEN,
    SNAPSHOT_END_LEN,
};
use crate::shared::protocol::{MessageType, ProtocolError};

/// What every accepted lane on a shard shares
pub struct ListenerContext<S: ShoalDatabase> {
    /// The channels to every shard on this node
    pub comms: Comms<S>,
    /// The channel to hand this shard's own work on
    pub node_local_tx: AsyncSender<ServerMsg<S>>,
    /// What this node says about itself
    pub local: Rc<std::cell::RefCell<Local>>,
    /// The map this shard holds, which is what a hello is judged against
    pub map: MapCell,
    /// What to take the wire with, if the lanes are encrypted
    pub tls: Option<Arc<ServerConfig>>,
    /// How long a peer has to finish its handshake
    pub handshake_timeout: Duration,
    /// The most forwarded bytes one connection may hold unanswered
    pub inflight_bound: usize,
    /// How many shards this node runs, which every entry's shard is checked against
    pub shard_count: usize,
    /// Bytes received on bulk lanes, for the transport view
    pub bulk_received: Rc<std::cell::Cell<u64>>,
}

impl<S: ShoalDatabase> Clone for ListenerContext<S> {
    fn clone(&self) -> Self {
        ListenerContext {
            comms: self.comms.clone(),
            node_local_tx: self.node_local_tx.clone(),
            local: self.local.clone(),
            map: self.map.clone(),
            tls: self.tls.clone(),
            handshake_timeout: self.handshake_timeout,
            inflight_bound: self.inflight_bound,
            shard_count: self.shard_count,
            bulk_received: self.bulk_received.clone(),
        }
    }
}

/// What one connection has taken in and not yet answered
struct Inflight {
    /// Bytes across every bundle with an entry still unanswered
    bytes: usize,
    /// The most bytes this connection holds before it stops reading
    bound: usize,
    /// Per bundle, how many entries are still unanswered and how many bytes it took
    bundles: HashMap<Uuid, (usize, usize)>,
    /// Who to wake when a bundle is fully answered
    waker: Option<Waker>,
}

impl Inflight {
    /// Note that a bundle with this many entries has been taken in
    fn taken(&mut self, bundle: Uuid, entries: usize, bytes: usize) {
        // a bundle forwarded twice under one id is counted once, its entries added up
        let slot = self.bundles.entry(bundle).or_insert((0, 0));
        if slot.0 == 0 {
            self.bytes += bytes;
            slot.1 = bytes;
        }
        slot.0 += entries;
    }

    /// Note that one entry of a bundle has been answered
    fn answered(&mut self, bundle: Uuid) {
        let Some(slot) = self.bundles.get_mut(&bundle) else {
            return;
        };
        slot.0 = slot.0.saturating_sub(1);
        // the last answer releases the bundle's bytes
        if slot.0 == 0 {
            self.bytes = self.bytes.saturating_sub(slot.1);
            self.bundles.remove(&bundle);
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

/// A future that resolves once a connection has room for this many more bytes
struct Room {
    /// The connection's in-flight record
    inflight: Rc<RefCell<Inflight>>,
    /// How many bytes are wanted
    wanted: usize,
}

impl Future for Room {
    type Output = ();

    /// Resolve when the bytes fit under the bound
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inflight = self.inflight.borrow_mut();
        // a bundle larger than the whole bound is admitted alone, or nothing ever would be
        if inflight.bytes == 0 || inflight.bytes + self.wanted <= inflight.bound {
            return Poll::Ready(());
        }
        inflight.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// Accept peer lanes and serve each one in a task of its own
///
/// The accept loop never waits on a peer: the handshake and everything after it happen in the
/// connection's own task, for the same reason the client acceptor's do.
///
/// # Arguments
///
/// * `listener` - The bound peer socket
/// * `ctx` - What every lane shares
#[allow(clippy::future_not_send)]
pub async fn peer_acceptor<S: ShoalDatabase>(
    listener: TcpListener,
    ctx: ListenerContext<S>,
) -> Result<(), ServerError> {
    loop {
        // a per-connection error must never close the listener: a peer that connects and
        // immediately resets used to make `accept` or `set_nodelay` return an error the `?`
        // propagated, dropping the listener and the port with it
        // ([F38](../../../../docs/src/features/inter-node-transport.md))
        let mut stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(error) => {
                event!(Level::WARN, msg = "a peer connection could not be accepted", ?error);
                continue;
            }
        };
        let ctx = ctx.clone();
        glommio::spawn_local(async move {
            // nodelay's error is this connection's alone, not the listener's
            let _ = stream.set_nodelay(true);
            // take the wire and shake hands under one deadline
            let accepted = glommio::timer::timeout(ctx.handshake_timeout, async {
                if let Some(config) = &ctx.tls {
                    if let Err(error) = crate::server::tls::accept(&mut stream, config).await {
                        return Ok(Err(error));
                    }
                }
                let local = ctx.local.borrow().clone();
                Ok(handshake::accept(&mut stream, &local, &[Lane::Data, Lane::Bulk, Lane::Replication], &ctx.map).await)
            })
            .await;
            let accepted = match accepted {
                Ok(Ok(accepted)) => accepted,
                Ok(Err(error)) => {
                    event!(Level::WARN, msg = "refused a peer", ?error);
                    return;
                }
                Err(error) => {
                    event!(Level::WARN, msg = "a peer never finished its handshake", ?error);
                    return;
                }
            };
            event!(
                Level::INFO,
                msg = "accepted a peer lane",
                node = %accepted.node,
                lane = %accepted.lane,
                incarnation = accepted.incarnation
            );
            let (rx, tx) = stream.split();
            match accepted.lane {
                Lane::Data => serve_data(ctx, accepted.node, accepted.max_frame_bytes, rx, tx).await,
                Lane::Bulk => serve_bulk(ctx, accepted.node, rx).await,
                Lane::Replication => {
                    serve_replication(ctx, accepted.node, accepted.max_frame_bytes, rx, tx).await
                }
                // the handshake refused it already
                Lane::Control => (),
            }
        })
        .detach();
    }
}

/// Serve one data lane: announce it as a client, relay forwards in and answers out
///
/// # Arguments
///
/// * `ctx` - What every lane shares
/// * `origin` - The peer this lane comes from
/// * `peer_max_frame_bytes` - The largest frame the peer accepts
/// * `rx` - The read half of the connection
/// * `tx` - The write half of the connection
async fn serve_data<S: ShoalDatabase>(
    ctx: ListenerContext<S>,
    origin: NodeId,
    peer_max_frame_bytes: u32,
    rx: ReadHalf<TcpStream>,
    tx: WriteHalf<TcpStream>,
) {
    // this connection is a client to every shard, with a channel of its own
    let conn = Uuid::new_v4();
    let (client_tx, client_rx) = kanal::unbounded_async();
    if let Err(error) = ctx
        .comms
        .broadcast(&ServerMsg::NewClient {
            client: conn,
            client_tx,
        })
        .await
    {
        event!(Level::ERROR, msg = "failed to announce a peer lane", %origin, ?error);
        return;
    }
    let inflight = Rc::new(RefCell::new(Inflight {
        bytes: 0,
        bound: ctx.inflight_bound,
        bundles: HashMap::new(),
        waker: None,
    }));
    // answers go out on their own task, bounded by what the peer accepts
    let tx_task = glommio::spawn_local(peer_tx_relay(
        client_rx,
        tx,
        peer_max_frame_bytes,
        inflight.clone(),
    ));
    // forwards come in on this one, until the peer goes away or sends something refused
    if let Err(error) = peer_rx_relay(&ctx, origin, conn, rx, &inflight).await {
        event!(Level::WARN, msg = "a peer lane ended", %origin, ?error);
    }
    // stop answering a peer that is gone, and tell every shard the client is gone with it
    tx_task.cancel().await;
    if let Err(error) = ctx.comms.broadcast(&ServerMsg::ClientGone(conn)).await {
        event!(Level::ERROR, msg = "failed to retire a peer lane", %origin, ?error);
    }
}

/// Relay forwarded bundles from one peer into this node
///
/// Every frame is read in three pieces - preamble, entries, bundle - each into a buffer of its
/// own, and every length is judged before anything is sized by it. What it cannot judge, the
/// shard does once the bundle is validated: offsets against the bundle, shards against the
/// count. A refusal at either place ends this connection and nothing else.
///
/// # Arguments
///
/// * `ctx` - What every lane shares
/// * `origin` - The peer this lane comes from
/// * `conn` - The client id this lane answers under
/// * `rx` - The read half of the connection
/// * `inflight` - What this connection has taken in and not yet answered
async fn peer_rx_relay<S: ShoalDatabase>(
    ctx: &ListenerContext<S>,
    origin: NodeId,
    conn: Uuid,
    mut rx: ReadHalf<TcpStream>,
    inflight: &Rc<RefCell<Inflight>>,
) -> Result<(), ServerError> {
    loop {
        // the header, or a clean end
        let max_frame_bytes = ctx.local.borrow().max_frame_bytes;
        let Some(header) = codec::read_header(&mut rx, max_frame_bytes).await? else {
            return Ok(());
        };
        let header = codec::expect(header, MessageType::Forward)?;
        // the fixed fields, judged against the frame they came in
        let raw: [u8; FORWARD_PREAMBLE_LEN] = codec::read_array(&mut rx).await?;
        let preamble = ForwardPreamble::decode(&raw, header.body_len())?;
        // the entries, exactly as many bytes as the preamble said
        let entries_raw = codec::read_vec(&mut rx, preamble.entries_len as usize).await?;
        let entries = peer::decode_entries(&entries_raw, preamble.entries)?;
        // every shard named has to exist here, before the bundle is even read
        for entry in &entries {
            if usize::from(entry.shard) >= ctx.shard_count {
                return Err(ProtocolError::MalformedForward("an entry names a shard this node does not run").into());
            }
        }
        // wait for room under the in-flight bound, which is what makes this node's memory a
        // number rather than the peer's appetite
        let bundle_len = preamble.bundle_len(header.body_len());
        Room {
            inflight: inflight.clone(),
            wanted: bundle_len,
        }
        .await;
        // the bundle, into an allocation of its own at offset zero
        let data = RequestBody::read_from(&mut rx, bundle_len).await?;
        let base = Stamp::now();
        let bundle = Uuid::from_bytes(preamble.bundle);
        inflight
            .borrow_mut()
            .taken(bundle, entries.len(), bundle_len);
        // hand it to this shard, which validates it and routes every entry
        if ctx
            .node_local_tx
            .send(ServerMsg::Forward {
                conn,
                origin,
                preamble,
                entries,
                data,
                base,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
    }
}

/// Relay answers back to one peer
///
/// # Arguments
///
/// * `client_rx` - The channel this node's shards hand answers over
/// * `tx` - The write half of the connection
/// * `peer_max_frame_bytes` - The largest frame the peer accepts
/// * `inflight` - What this connection has taken in and not yet answered
async fn peer_tx_relay(
    client_rx: AsyncReceiver<Reply>,
    mut tx: WriteHalf<TcpStream>,
    peer_max_frame_bytes: u32,
    inflight: Rc<RefCell<Inflight>>,
) {
    loop {
        let Ok(reply) = client_rx.recv().await else {
            break;
        };
        let Reply {
            id,
            index,
            kind,
            span,
            mut stamps,
            archived,
            ..
        } = reply;
        let guard = span.enter();
        // a peer connection is never subscribed and never sends admin requests, so neither
        // kind can be queued to it; one that is would be a bug in the shard, not a frame
        let forwarded = match kind {
            ReplyKind::Whole => ForwardedKind::Whole,
            ReplyKind::Share => ForwardedKind::Share,
            ReplyKind::Topology { .. } | ReplyKind::Admin => {
                event!(Level::ERROR, msg = "a control reply was queued to a peer relay", %id);
                drop(guard);
                continue;
            }
        };
        // frame the answer for the origin: which bundle, which index, whole or share
        let preamble = ForwardedPreamble {
            bundle: *id.as_bytes(),
            index: index as u64,
            kind: forwarded,
            // what this node learned running it, for the origin's record of the same query
            served: stamps.served_byte(),
        }
        .encode();
        let header = match codec::header(
            MessageType::Forwarded,
            preamble.len() + archived.len(),
            peer_max_frame_bytes,
        ) {
            Ok(header) => header,
            Err(error) => {
                // an answer too large for the peer is answered as a failure it can carry
                event!(Level::ERROR, msg = "an answer is too large for the peer", %id, index, %error);
                let payload = peer::encode_error_payload(
                    crate::shared::protocol::error::ErrorCode::ResponseTooLarge.as_u16(),
                    "the answer is larger than the frame the peer accepts",
                );
                let preamble = ForwardedPreamble {
                    bundle: *id.as_bytes(),
                    index: index as u64,
                    kind: ForwardedKind::Error,
                    served: stamps.served_byte(),
                }
                .encode();
                let Ok(header) = codec::header(
                    MessageType::Forwarded,
                    preamble.len() + payload.len(),
                    peer_max_frame_bytes,
                ) else {
                    break;
                };
                if codec::write_frame(&mut tx, &header, &[&preamble, &payload]).await.is_err() {
                    break;
                }
                inflight.borrow_mut().answered(id);
                drop(guard);
                continue;
            }
        };
        if let Err(error) = codec::write_frame(&mut tx, &header, &[&preamble, &archived]).await {
            event!(Level::WARN, msg = "failed to write an answer to a peer", %id, index, ?error);
            drop(guard);
            break;
        }
        // this bundle has one fewer answer owed, which may release its bytes
        inflight.borrow_mut().answered(id);
        // the record this node made is a peer's, and says so
        stamps.set_served_for_peer(true);
        stamps.mark_socket_written();
        stage_profile::emit(id, stamps);
        drop(guard);
    }
}

/// The answer to one replication request, on its way to the connection's write relay
#[derive(Debug)]
pub struct ReplicateReply {
    /// The request's correlation id
    pub id: u64,
    /// Whether the payload is an answer or a failure
    pub status: ReplicateStatus,
    /// The answer, or the failure's message
    pub payload: Vec<u8>,
}

impl ReplicateReply {
    /// An answer
    ///
    /// # Arguments
    ///
    /// * `id` - The request's correlation id
    /// * `payload` - The answer
    #[must_use]
    pub fn ok(id: u64, payload: Vec<u8>) -> Self {
        ReplicateReply {
            id,
            status: ReplicateStatus::Ok,
            payload,
        }
    }

    /// A failure
    ///
    /// # Arguments
    ///
    /// * `id` - The request's correlation id
    /// * `msg` - What went wrong
    #[must_use]
    pub fn error(id: u64, msg: impl Into<String>) -> Self {
        ReplicateReply {
            id,
            status: ReplicateStatus::Error,
            payload: msg.into().into_bytes(),
        }
    }
}

/// Serve one replication lane: relay requests to the shards they name and answers back
///
/// Every request names the shard on this node that hosts its group, and is handed to that
/// shard over the mesh; the shard answers into this connection's channel and the relay here
/// frames the answer under the request's id. In-flight bytes are bounded the way a data lane's
/// are: past the bound the connection stops reading until answers drain it.
///
/// # Arguments
///
/// * `ctx` - What every lane shares
/// * `origin` - The peer this lane comes from
/// * `peer_max_frame_bytes` - The largest frame the peer accepts
/// * `rx` - The read half of the connection
/// * `tx` - The write half of the connection
async fn serve_replication<S: ShoalDatabase>(
    mut ctx: ListenerContext<S>,
    origin: NodeId,
    peer_max_frame_bytes: u32,
    mut rx: ReadHalf<TcpStream>,
    tx: WriteHalf<TcpStream>,
) {
    let (reply_tx, reply_rx) = kanal::unbounded_async::<ReplicateReply>();
    let inflight = Rc::new(RefCell::new(Inflight {
        bytes: 0,
        bound: ctx.inflight_bound,
        bundles: HashMap::new(),
        waker: None,
    }));
    // answers go out on their own task
    let tx_task = glommio::spawn_local(replication_tx_relay(reply_rx, tx, peer_max_frame_bytes, inflight.clone()));
    let outcome: Result<(), ServerError> = async {
        loop {
            let max_frame_bytes = ctx.local.borrow().max_frame_bytes;
            let Some(header) = codec::read_header(&mut rx, max_frame_bytes).await? else {
                return Ok(());
            };
            let header = codec::expect(header, MessageType::Replicate)?;
            // the head, judged against the frame it came in
            let Some(payload_len) = header.body_len().checked_sub(REPLICATE_HEAD_LEN) else {
                return Err(ProtocolError::BodyTooShort {
                    need: REPLICATE_HEAD_LEN,
                    got: header.len,
                }
                .into());
            };
            let raw: [u8; REPLICATE_HEAD_LEN] = codec::read_array(&mut rx).await?;
            let head = ReplicateRequestHead::decode(&raw)?;
            // the shard named has to exist here, before the payload is read
            if usize::from(head.target_shard) >= ctx.shard_count {
                return Err(ProtocolError::MalformedForward("a replication request names a shard this node does not run").into());
            }
            // wait for room under the in-flight bound, then read the payload
            Room {
                inflight: inflight.clone(),
                wanted: payload_len,
            }
            .await;
            let payload = codec::read_vec(&mut rx, payload_len).await?;
            inflight.borrow_mut().taken(Uuid::from_u64_pair(head.id, 0), 1, payload_len);
            // hand it to the shard that hosts the group
            let msg = ServerMsg::Replication {
                origin,
                head,
                payload,
                reply: reply_tx.clone(),
            };
            if ctx
                .comms
                .send(&crate::server::shard::ShardContact::Local(usize::from(head.target_shard)), msg)
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
    .await;
    if let Err(error) = outcome {
        event!(Level::WARN, msg = "a replication lane ended", %origin, ?error);
    }
    tx_task.cancel().await;
}

/// Relay replication answers back to one peer
///
/// # Arguments
///
/// * `reply_rx` - The channel the shards hand answers over
/// * `tx` - The write half of the connection
/// * `peer_max_frame_bytes` - The largest frame the peer accepts
/// * `inflight` - What this connection has taken in and not yet answered
async fn replication_tx_relay(
    reply_rx: AsyncReceiver<ReplicateReply>,
    mut tx: WriteHalf<TcpStream>,
    peer_max_frame_bytes: u32,
    inflight: Rc<RefCell<Inflight>>,
) {
    loop {
        let Ok(reply) = reply_rx.recv().await else {
            break;
        };
        let head = ReplicateResponseHead {
            id: reply.id,
            status: reply.status,
        }
        .encode();
        let header = match codec::header(
            MessageType::ReplicateResponse,
            head.len() + reply.payload.len(),
            peer_max_frame_bytes,
        ) {
            Ok(header) => header,
            Err(error) => {
                // an answer too large for the peer is answered as a failure it can carry
                event!(Level::ERROR, msg = "a replication answer is too large for the peer", id = reply.id, %error);
                let failure = ReplicateReply::error(reply.id, "the answer is larger than the frame the peer accepts");
                let head = ReplicateResponseHead {
                    id: failure.id,
                    status: failure.status,
                }
                .encode();
                let Ok(header) = codec::header(
                    MessageType::ReplicateResponse,
                    head.len() + failure.payload.len(),
                    peer_max_frame_bytes,
                ) else {
                    break;
                };
                if codec::write_frame(&mut tx, &header, &[&head, &failure.payload]).await.is_err() {
                    break;
                }
                inflight.borrow_mut().answered(Uuid::from_u64_pair(reply.id, 0));
                continue;
            }
        };
        if let Err(error) = codec::write_frame(&mut tx, &header, &[&head, &reply.payload]).await {
            event!(Level::WARN, msg = "failed to write a replication answer to a peer", id = reply.id, ?error);
            break;
        }
        inflight.borrow_mut().answered(Uuid::from_u64_pair(reply.id, 0));
    }
}

/// Serve one bulk lane: read a stream's frames, check them, and count them
///
/// Nothing installs a snapshot at M2. What this does is everything up to that: the framing is
/// read and bounded, every chunk's checksum is checked, and the bytes are counted where the
/// transport view can see them, so the bulk lane is a lane with a shape rather than a name.
///
/// # Arguments
///
/// * `ctx` - What every lane shares
/// * `origin` - The peer this lane comes from
/// * `rx` - The read half of the connection
async fn serve_bulk<S: ShoalDatabase>(
    ctx: ListenerContext<S>,
    origin: NodeId,
    mut rx: ReadHalf<TcpStream>,
) {
    let outcome: Result<(), ServerError> = async {
        loop {
            let max_frame_bytes = ctx.local.borrow().max_frame_bytes;
            let Some(header) = codec::read_header(&mut rx, max_frame_bytes).await? else {
                return Ok(());
            };
            match header.kind {
                MessageType::SnapshotBegin => {
                    let raw: [u8; SNAPSHOT_BEGIN_LEN] = codec::read_array(&mut rx).await?;
                    let begin = SnapshotBegin::decode(&raw, header.body_len())?;
                    let manifest = codec::read_vec(&mut rx, begin.manifest_len as usize).await?;
                    ctx.bulk_received
                        .set(ctx.bulk_received.get() + header.body_len() as u64);
                    drop(manifest);
                }
                MessageType::SnapshotChunk => {
                    let raw: [u8; SNAPSHOT_CHUNK_LEN] = codec::read_array(&mut rx).await?;
                    let chunk = SnapshotChunk::decode(&raw, header.body_len())?;
                    let bytes = codec::read_vec(&mut rx, chunk.len as usize).await?;
                    chunk.verify(&bytes)?;
                    ctx.bulk_received
                        .set(ctx.bulk_received.get() + header.body_len() as u64);
                }
                MessageType::SnapshotEnd => {
                    let raw: [u8; SNAPSHOT_END_LEN] = codec::read_array(&mut rx).await?;
                    let _end = SnapshotEnd::decode(&raw);
                    ctx.bulk_received
                        .set(ctx.bulk_received.get() + header.body_len() as u64);
                }
                other => {
                    return Err(ProtocolError::UnexpectedMessageType {
                        expected: MessageType::SnapshotChunk,
                        got: other,
                    }
                    .into());
                }
            }
        }
    }
    .await;
    if let Err(error) = outcome {
        event!(Level::WARN, msg = "a bulk lane ended", %origin, ?error);
    }
}

/// Keep the unused import lint honest about the read half the bulk lane splits off
#[allow(dead_code)]
async fn drain(mut rx: ReadHalf<TcpStream>) -> std::io::Result<()> {
    let mut scratch = [0u8; 64];
    while rx.read(&mut scratch).await? > 0 {}
    Ok(())
}
