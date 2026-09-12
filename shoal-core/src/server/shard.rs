//! A single shard in Shoal

use bytes::Bytes;
use futures::{
    io::{ReadHalf, WriteHalf},
    AsyncReadExt, AsyncWriteExt,
};
use glommio::{
    enclose,
    net::{TcpListener, TcpStream},
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, PoolThreadHandles, Shares, Task,
    TaskQueueHandle,
};
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rustls::ServerConfig;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    util::AlignedVec,
    validation::{archive::ArchiveValidator, shared::SharedValidator, Validator},
    Archive, DeserializeUnsized,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cell::Cell, cell::RefCell, hash::BuildHasherDefault};
use std::{collections::HashMap, io::IoSlice};
use tracing::{event, info_span, instrument, Instrument, Level, Span};
use uuid::Uuid;

use super::messages::{Answer, PeerEvent, QueryMetadata, Reply, ReplyKind, ServerMsg};
use super::peer::{
    self, Frame, FrameKey, Lane, LinkEvent, ListenerContext, Local, PeerSetup, Peers, Pending,
    ShardTransportView,
};
use super::request_body::RequestBody;
use super::database::ShoalDatabase;
use super::ring::Ring;
use super::routing::ArchivedShardRouting;
use super::stage_profile::{self, StageStamps, Stamp};
use super::tls;
use super::trace;
use super::{Comms, Conf, ServerError};
use crate::shared::identity::NodeId;
use crate::{
    shared::{
        auth::{
            scram::{ScramServer, ServerStep},
            AuthError, CredentialStore, Principal,
        },
        protocol::{
            self,
            auth::{self as proto_auth, AuthMechanism, AuthStatus},
            error::{self as proto_error, ErrorCode},
            handshake,
            trace::{TraceContext, TRACE_CONTEXT_LEN},
            Header, MessageType, ProtocolError,
        },
        queries::{ArchivedQueries, Queries},
        traits::{QuerySupport, ShoalResponseSupport},
    },
    storage::{FullArchiveMap, LoaderMsg, Loaders},
};

/// Read the trace context a request frame carries, if its header says it carries one
///
/// Split out of the relay so that the flag check, the read and the decode are one step there. A
/// frame with the flag clear reads nothing at all, which is every frame a client that is not
/// tracing sends.
///
/// # Arguments
///
/// * `tcp_rx` - The read half of the connection this frame is arriving on
/// * `header` - The already checked header of the frame being read
async fn read_trace_context(
    tcp_rx: &mut ReadHalf<TcpStream>,
    header: &protocol::Header,
) -> Result<Option<TraceContext>, ServerError> {
    // a frame with the flag clear carries no context, and reading one would eat its payload
    if header.trace_len() == 0 {
        return Ok(None);
    }
    // take exactly the bytes a context is, onto the stack
    let mut raw = [0u8; TRACE_CONTEXT_LEN];
    tcp_rx.read_exact(&mut raw).await?;
    // and turn them into the context the client sent
    Ok(Some(TraceContext::decode(&raw)?))
}

/// Write a trace id out the way a collector shows it
///
/// `TraceContext` holds bytes rather than an OpenTelemetry id, so that `shoal-proto` links no
/// tracing stack. This is the one place the server wants them as the 32 hex digits somebody would
/// paste into a collector's search box.
///
/// # Arguments
///
/// * `wire_trace` - The trace context to write the trace id of
fn hex_trace_id(wire_trace: &TraceContext) -> String {
    // two hex digits per byte, in the order they went on the wire
    wire_trace
        .trace_id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Relay bundles of queries from one client into this node
///
/// Nothing in here panics. Every failure ends this one connection and leaves the shard and every
/// other client it is serving alone, which is what it means for a relay to be a per connection
/// task rather than a shared one.
///
/// # Arguments
///
/// * `peer` - The client this relay is reading from
/// * `tcp_rx` - The read half of that client's connection
/// * `kanal_tx` - The channel to forward bundles into this node on
/// * `max_frame_bytes` - The largest frame this server will accept
async fn client_rx_relay<S: ShoalDatabase>(
    peer: Uuid,
    mut tcp_rx: ReadHalf<TcpStream>,
    kanal_tx: AsyncSender<ServerMsg<S>>,
    max_frame_bytes: u32,
) {
    // keep waiting for messages until  our tcp socket closes
    loop {
        // have a buffer for the header of the next frame
        let mut preamble = [0u8; protocol::REQUEST_PREAMBLE_LEN];
        // try to read the header of the next message from our tcp socket
        if let Err(error) = tcp_rx.read_exact(&mut preamble).await {
            // if this was an unexpected EOF error then assume the client died
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            // any other read error ends this connection and only this connection
            event!(Level::ERROR, msg = "failed to read a frame header", %peer, ?error);
            break;
        }
        // check the header before its length is used for anything
        //
        // this is what closes the hole where a peer could name its own allocation size, and it
        // has to happen here rather than after the allocation below
        let header = match protocol::decode_request(&preamble, max_frame_bytes) {
            Ok(header) => header,
            Err(error) => {
                event!(Level::ERROR, msg = "refused a frame", %peer, %error);
                break;
            }
        };
        // read the trace context this frame carries, if its flags say it carries one
        //
        // this is a read of its own rather than the front of the body buffer below, because that
        // buffer is an rkyv archive accessed in place: an archive starting 26 bytes into its own
        // allocation has every pointer in it misaligned
        let wire_trace = match read_trace_context(&mut tcp_rx, &header).await {
            Ok(wire_trace) => wire_trace,
            Err(error) => {
                // a peer that said a context follows and wrote something else is a peer we cannot
                // stay in step with, so this ends the connection the way a bad header does. so
                // does a read that failed, since the socket is then part way through a frame
                event!(Level::ERROR, msg = "failed to read a trace context", %peer, ?error);
                break;
            }
        };
        // work out how much of this frame is the bundle rather than what sits ahead of it
        let payload_len = match header.request_payload_len() {
            Ok(payload_len) => payload_len,
            Err(error) => {
                event!(Level::ERROR, msg = "refused a frame", %peer, %error);
                break;
            }
        };
        // open the root span every span this bundle produces hangs off
        //
        // here rather than after the body read, because this is the first instant the frame is
        // known to exist and the read of its body is part of serving it. the wait on the
        // preamble above is deliberately outside: that is idle time between requests, not time
        // this request spent anywhere
        //
        // `parent: None` is explicit rather than incidental. this task inherits nothing today,
        // but a span opened contextually would silently join whatever the connection task
        // happened to be in the day somebody instruments it
        //
        // it stays `parent: None` even when the client sent a trace context. that context is an
        // *OpenTelemetry* parent, set below and resolved by the OTLP layer, and the two parenting
        // mechanisms are independent - this one decides what the registry hangs this span off,
        // and the registry has never heard of the other process
        let span = info_span!(
            parent: None,
            "Shoal::request",
            peer = %peer,
            bytes = payload_len,
            trace = tracing::field::Empty,
        );
        // join this request to the trace the client opened, if it sent one
        if let Some(wire_trace) = &wire_trace {
            // say on the span itself which trace it was joined to, so a run with no collector
            // attached can still be followed in the console
            span.record("trace", tracing::field::display(hex_trace_id(wire_trace)));
            trace::adopt_remote_parent(&span, wire_trace);
        }
        // read this frame's body into a buffer that is exactly the right size
        //
        // the buffer is not zeroed first, because every byte of it is about to be overwritten.
        // that is what `RequestBody` is for: the read is its only constructor, so a body that
        // exists is a body a read filled
        //
        // instrumented rather than entered around: a guard held across an await would leave this
        // span current while another connection's task runs on this executor. `Instrumented`
        // enters on each poll and exits on each return, which is also what gives this span an end
        let data = match RequestBody::read_from(&mut tcp_rx, payload_len)
            .instrument(span.clone())
            .await
        {
            Ok(data) => data,
            Err(error) => {
                event!(Level::ERROR, msg = "failed to read a frame body", %peer, ?error);
                break;
            }
        };
        // start this bundles clock now that all of its bytes are here
        //
        // every stage offset a query in this bundle records is measured from here, since
        // this is the first moment we know the bundle exists
        let base = Stamp::now();
        // forward our clients message
        if let Err(error) = kanal_tx
            .send(ServerMsg::Client {
                peer,
                span,
                data,
                base,
            })
            .await
        {
            // this shards channel is gone, so there is nowhere left to put this bundle
            event!(Level::ERROR, msg = "failed to forward a bundle", %peer, ?error);
            break;
        }
    }
}

/// Tell one client that a query failed, without ending the connection it failed on
///
/// Returns whether the failure could be written. A client that cannot be told is a client that
/// cannot be served, so a write that fails here ends this connection the same way a failed
/// response write does.
///
/// This is a frame rather than a response because the relay only ever holds an opaque
/// `AlignedVec` — it has no idea which variant of the schema's response enum this query belongs
/// to, and cannot build one. That is the whole reason the protocol carries a frame level error
/// type alongside the response level one.
///
/// # Arguments
///
/// * `tcp_tx` - The write half of this client's connection
/// * `query_id` - The query this failure belongs to, or nil for the connection itself
/// * `code` - What class of failure this is
/// * `msg` - What to say about it, which is cut down to what a frame will carry
/// * `peer_max_frame_bytes` - The largest frame this client said it would accept
async fn write_error_frame(
    tcp_tx: &mut WriteHalf<TcpStream>,
    query_id: &Uuid,
    code: ErrorCode,
    msg: &str,
    peer_max_frame_bytes: u32,
) -> bool {
    // cut this message down to what a frame will carry
    let msg = proto_error::truncate_msg(msg);
    // build the header and the fixed fields that go ahead of it
    let preamble = match proto_error::error_preamble(
        query_id,
        code,
        msg.len(),
        peer_max_frame_bytes,
    ) {
        Ok(preamble) => preamble,
        // a client whose frame bound cannot hold even an empty error frame cannot be told
        // anything, so there is nothing left to do for it
        Err(error) => {
            event!(Level::ERROR, msg = "could not frame an error", %query_id, %error);
            return false;
        }
    };
    // build our vectored byte slices to send
    let mut bufs = &mut [IoSlice::new(&preamble), IoSlice::new(msg.as_bytes())][..];
    // keep sending until all of this failure has been sent
    while !bufs.is_empty() {
        match tcp_tx.write_vectored(bufs).await {
            Ok(0) => {
                event!(Level::ERROR, msg = "wrote no bytes of an error to a client", %query_id);
                return false;
            }
            Ok(n) => IoSlice::advance_slices(&mut bufs, n),
            Err(error) => {
                event!(Level::ERROR, msg = "failed to write an error", %query_id, ?error);
                return false;
            }
        }
    }
    true
}

/// Relay responses back to one client
///
/// Like the read half, nothing in here panics. A write that fails ends this connection and leaves
/// every other client this shard is serving alone.
///
/// A response that cannot be *framed* is different from a write that failed: the socket is fine
/// and only this one answer is impossible. That is answered with an error frame naming the query,
/// and this connection keeps serving everything else on it.
///
/// # Arguments
///
/// * `client_rx` - The channel this node's shards hand responses over
/// * `tcp_tx` - The write half of this client's connection
/// * `peer_max_frame_bytes` - The largest frame this client said it would accept
async fn client_tx_relay<S: ShoalDatabase>(
    client_rx: AsyncReceiver<Reply>,
    mut tcp_tx: WriteHalf<TcpStream>,
    peer_max_frame_bytes: u32,
) {
    // loop over messages to send back to our client
    loop {
        // try to get a message from our channel
        let Reply {
            id: query_id,
            span,
            mut stamps,
            archived,
            ..
        } = match client_rx.recv().await {
            Ok(msg) => msg,
            // if this channel was closed then stop our task
            // this should only happen exit/shutdown or when our client shutsdown
            Err(_) => break,
        };
        // enter this query's own span for the framing and the write
        //
        // this is what puts the end of the trace on the socket rather than at the reply that
        // queued the bytes: `tracing-opentelemetry` timestamps a span when it is *exited*, so
        // a span that is only ever held and never entered exports with no duration at all.
        // entering it here is what makes `Coordinator::route` cover the whole query
        let span_guard = span.enter();
        // build the header and query id that go ahead of this response
        //
        // a response too large for this client to accept is answered with a failure naming the
        // query and both sizes, rather than by closing a connection the client would never
        // learn the reason for. every other query on this connection is unaffected
        let preamble = match protocol::response_preamble(
            &query_id,
            archived.len(),
            peer_max_frame_bytes,
        ) {
            Ok(preamble) => preamble,
            Err(error) => {
                event!(Level::ERROR, msg = "response too large to frame", %query_id, %error);
                // say what happened in terms of sizes rather than of internals
                let told = format!(
                    "this response is {} bytes, larger than the {peer_max_frame_bytes} byte frame this connection accepts",
                    archived.len()
                );
                // a client we cannot even tell is a client we cannot serve
                if !write_error_frame(
                    &mut tcp_tx,
                    &query_id,
                    ErrorCode::ResponseTooLarge,
                    &told,
                    peer_max_frame_bytes,
                )
                .await
                {
                    drop(span_guard);
                    break;
                }
                // this query's journey ended here, so hand it to the profile before it is
                // forgotten - the failure is the last thing this server knows about it
                stamps.mark_socket_written();
                stage_profile::emit(query_id, stamps);
                drop(span_guard);
                continue;
            }
        };
        // build our vectored byte slices to send
        let mut bufs = &mut [IoSlice::new(&preamble), IoSlice::new(&archived)][..];
        // keep sending our data until all of this archive has been sent
        //
        // a short write or a write error here means this client is gone, so this connection ends
        let mut failed = false;
        while !bufs.is_empty() {
            // send this data back to our client
            match tcp_tx.write_vectored(bufs).await {
                Ok(0) => {
                    event!(Level::ERROR, msg = "wrote no bytes to a client", %query_id);
                    failed = true;
                    break;
                }
                Ok(n) => IoSlice::advance_slices(&mut bufs, n),
                Err(error) => {
                    event!(Level::ERROR, msg = "failed to write a response", %query_id, ?error);
                    failed = true;
                    break;
                }
            }
        }
        // stop relaying to a client we could not write to
        if failed {
            drop(span_guard);
            break;
        }
        // record that this responses last byte is now the sockets problem
        stamps.mark_socket_written();
        // hand this queries journey to the profile
        //
        // this is the last moment the server knows anything about the query, so it is the
        // only place a record can be emitted with every server side stage filled in
        stage_profile::emit(query_id, stamps);
        // drop our span since we are done writting
        drop(span_guard);
    }
}

/// How long a client has to finish its half of the handshake
///
/// A peer that connects and then says nothing would otherwise hold a task and a socket forever.
/// This is generous compared to a handshake that is one 24 byte write and one 24 byte read, and
/// deliberately so — it is here to bound a stalled peer, not to police a slow one.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest handshake frame this server will read
///
/// A handshake body is sixteen bytes. Anything claiming more than this is not a peer whose version
/// we are trying to be careful about, so it is closed without the courtesy of a reply.
const MAX_HANDSHAKE_BODY: usize = 4096;

/// Read a client's `Hello` and answer it
///
/// # Invariants
///
/// **The client speaks first.** This reads before it writes, and the client writes before it
/// reads. If both peers waited to read, every connection would deadlock.
///
/// **A refusal is still answered.** The server writes a `HelloAck` naming why before it closes, so
/// that a mismatched client gets a legible error instead of a reset. That is only possible because
/// the eight header bytes mean the same thing in every protocol version, which is what lets this
/// read a version it does not speak and still know how many body bytes to drain.
///
/// **The body is drained before the refusal is written.** Closing a socket that still has unread
/// bytes queued sends a reset, which would discard the very reply this went to the trouble of
/// composing.
///
/// **The mechanism is chosen here and nowhere else.** The client offers a set and this picks one
/// out of the server's own preference order, so a client cannot talk this server into the weaker
/// of two mechanisms by offering only that one — it is refused instead.
///
/// # Arguments
///
/// * `stream` - The connection to shake hands over, before it has been split
/// * `max_frame_bytes` - The largest frame this server will accept
/// * `store` - The users this server will accept, and whether it requires one
async fn server_handshake<S: ShoalDatabase>(
    stream: &mut TcpStream,
    max_frame_bytes: u32,
    store: &CredentialStore,
) -> Result<(handshake::Hello, Option<AuthMechanism>), ServerError> {
    // the fingerprint of the schema this server was built from
    let ours = <S::ClientType as QuerySupport>::SCHEMA_FINGERPRINT;
    // build the ack we will send if everything about this client checks out
    let accept = handshake::HelloAck {
        schema_fingerprint: ours,
        max_frame_bytes,
        reason: handshake::RefusalReason::Accepted,
        // filled in once we have read what this client can do
        mechanism: None,
    };
    // read the header of whatever this client opened with
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    stream.read_exact(&mut header_bytes).await?;
    let raw = protocol::RawHeader::decode(&header_bytes);
    // a frame claiming more than any handshake could need is not worth answering
    if raw.len as usize > MAX_HANDSHAKE_BODY {
        return Err(ProtocolError::FrameTooLarge {
            len: raw.len,
            max: MAX_HANDSHAKE_BODY as u32,
        }
        .into());
    }
    // drain the body before we decide anything, so that a refusal can still be written
    let mut body = vec![0u8; raw.len as usize];
    stream.read_exact(&mut body).await?;
    // refuse a version we do not speak, naming ours so the client can say what happened
    if raw.version != protocol::PROTOCOL_VERSION {
        // this reply carries our version in its header, which the client can read because the
        // header layout does not move between versions
        let refusal = handshake::HelloAck {
            reason: handshake::RefusalReason::UnsupportedVersion,
            ..accept
        };
        stream.write_all(&refusal.frame(max_frame_bytes)?).await?;
        stream.flush().await?;
        return Err(ProtocolError::UnsupportedVersion {
            got: raw.version,
            ours: protocol::PROTOCOL_VERSION,
        }
        .into());
    }
    // a connection that opens with anything but a hello is not one we know how to have
    let kind = protocol::MessageType::from_byte(raw.kind)?;
    if kind != protocol::MessageType::Hello {
        return Err(ProtocolError::UnexpectedMessageType {
            expected: protocol::MessageType::Hello,
            got: kind,
        }
        .into());
    }
    // a hello is a fixed sixteen bytes, so one of any other size is not a hello
    let body: [u8; handshake::HANDSHAKE_BODY_LEN] =
        body.try_into().map_err(|_| ProtocolError::BodyTooShort {
            need: handshake::HANDSHAKE_BODY_LEN,
            got: raw.len,
        })?;
    let hello = handshake::Hello::decode(&body);
    // refuse a client built from a different schema, naming both fingerprints
    if hello.schema_fingerprint != ours {
        let refusal = handshake::HelloAck {
            reason: handshake::RefusalReason::SchemaMismatch,
            ..accept
        };
        stream.write_all(&refusal.frame(max_frame_bytes)?).await?;
        stream.flush().await?;
        return Err(ProtocolError::SchemaMismatch {
            ours,
            theirs: hello.schema_fingerprint,
        }
        .into());
    }
    // work out what this client has to prove, if anything, before we accept it
    //
    // a server that requires nothing picks nothing whatever the client offered, so a client with
    // credentials talking to an open server is let straight in rather than made to use them
    let mechanism = store.select(hello.mechanisms);
    if store.is_required() && mechanism.is_none() {
        // this client cannot do anything we accept, and there is no exchange to have
        let refusal = handshake::HelloAck {
            reason: handshake::RefusalReason::NoCommonAuthMechanism,
            ..accept
        };
        stream.write_all(&refusal.frame(max_frame_bytes)?).await?;
        stream.flush().await?;
        return Err(AuthError::NoCredentials.into());
    }
    // this client speaks our protocol and was built from our schema, so let it in
    let accept = handshake::HelloAck {
        mechanism,
        ..accept
    };
    stream.write_all(&accept.frame(max_frame_bytes)?).await?;
    stream.flush().await?;
    Ok((hello, mechanism))
}

/// Read one `Auth` frame from a client
///
/// # Arguments
///
/// * `stream` - The connection to read from, before it has been split
async fn read_auth(
    stream: &mut TcpStream,
    selected: AuthMechanism,
) -> Result<Vec<u8>, ServerError> {
    // read the header first, so that a frame's size is known before anything allocates for it
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    stream.read_exact(&mut header_bytes).await?;
    // the auth payload bound is far tighter than the frame bound, and this peer has proved nothing
    // yet, so it is the one that is applied here
    let header = protocol::Header::decode(&header_bytes, proto_auth::MAX_AUTH_FRAME_BODY)?
        .expect(protocol::MessageType::Auth)?;
    let _ = proto_auth::payload_len(header)?;
    // now that the length has been judged, read the body it named
    let mut body = vec![0u8; header.body_len()];
    stream.read_exact(&mut body).await?;
    // check the mechanism this frame names is the one we selected, then hand back its payload
    //
    // this is checked against what the handshake chose rather than against a constant, so that a
    // client cannot switch mechanisms mid exchange once there is more than one to switch between
    let (named, payload) = proto_auth::decode_auth_body(&body)?;
    if named != selected {
        return Err(AuthError::UnsupportedMechanism(named).into());
    }
    Ok(payload.to_vec())
}

/// Run an authentication exchange with a client that has been accepted
///
/// # Invariants
///
/// **A refusal is written before the connection closes**, the same way a `HelloAck` refusal is,
/// and it carries one sentence for every way a client can fail. Which failure it actually was
/// stays in this server's log — see [`ServerError::Auth`].
///
/// **This runs before the stream is split.** Everything it reads would otherwise be handed to the
/// relays, which would decode a client's proof as a bundle of queries.
///
/// # Arguments
///
/// * `stream` - The connection to authenticate over, before it has been split
/// * `mechanism` - The mechanism this server selected in its `HelloAck`
/// * `max_frame_bytes` - The largest frame this client said it will accept
/// * `store` - The users this server will accept
async fn server_auth(
    stream: &mut TcpStream,
    mechanism: AuthMechanism,
    max_frame_bytes: u32,
    store: &CredentialStore,
) -> Result<Principal, ServerError> {
    // mutual TLS is defined on the wire and cannot be selected, since there is no TLS to read a
    // certificate off of. this is the arm it becomes when there is
    if mechanism != AuthMechanism::ScramSha256 {
        return Err(AuthError::UnsupportedMechanism(mechanism).into());
    }
    // run the exchange, answering each message the mechanism produces until it is done
    let mut scram = ScramServer::new(store);
    loop {
        // read whatever this client sent, and let the mechanism decide what it means
        let payload = read_auth(stream, mechanism).await?;
        match scram.step(&payload) {
            // another round, so answer with the challenge and wait for the next proof
            Ok(ServerStep::Challenge(challenge)) => {
                let frame =
                    proto_auth::encode_auth_response(AuthStatus::Challenge, &challenge, max_frame_bytes)?;
                stream.write_all(&frame).await?;
                stream.flush().await?;
            }
            // this client is who it says it is, and the payload proves this server is too
            Ok(ServerStep::Success { payload, principal }) => {
                let frame =
                    proto_auth::encode_auth_response(AuthStatus::Success, &payload, max_frame_bytes)?;
                stream.write_all(&frame).await?;
                stream.flush().await?;
                return Ok(principal);
            }
            // a failure is answered before the socket closes, in one sentence for every cause
            Err(error) => {
                let frame = proto_auth::encode_auth_response(
                    AuthStatus::Failed,
                    error.wire_msg().as_bytes(),
                    max_frame_bytes,
                )?;
                stream.write_all(&frame).await?;
                stream.flush().await?;
                return Err(error.into());
            }
        }
    }
}

/// Accept new clients and start a pair of relays for each one
///
/// # Invariants
///
/// **The accept loop never waits on a peer.** Everything a connection needs after `accept`
/// returns — the handshake, the broadcast, both relays — happens in a task of its own. A
/// handshake done inline would let one client that connects and then says nothing park this loop,
/// and with a single shard configured that is every subsequent connection to this server.
///
/// # Arguments
///
/// * `tcp_sock` - The socket to accept clients on
/// * `comms` - The channels to every shard on this node
/// * `node_local_tx` - The channel to forward this node's bundles on
/// * `max_frame_bytes` - The largest frame this server will accept
/// * `store` - The users this shard will accept, and whether it requires one
/// * `tls` - What to encrypt connections with, if this listener is encrypted
#[allow(clippy::future_not_send)]
async fn client_acceptor<S: ShoalDatabase>(
    tcp_sock: TcpListener,
    comms: Comms<S>,
    node_local_tx: AsyncSender<ServerMsg<S>>,
    max_frame_bytes: u32,
    store: Rc<CredentialStore>,
    tls: Option<Arc<ServerConfig>>,
) -> Result<(), ServerError> {
    loop {
        // try to read a single datagram from our udp socket
        let mut stream = tcp_sock.accept().await?;
        // disable nagles algorithm on this socket
        stream.set_nodelay(true)?;
        // generate an id for this peer
        // TODO: detect collisions?
        let client = Uuid::new_v4();
        // hand this connection everything it needs to live on its own
        let comms = comms.clone();
        let node_local_tx = node_local_tx.clone();
        // the store is shared by every connection on this shard and is never written to, so it is
        // reference counted rather than cloned. an `Rc` and not an `Arc` because a shard is a
        // thread of its own and nothing here crosses one
        let store = store.clone();
        // the tls config is shared the same way, and is an `Arc` only because rustls asks for one
        let tls = tls.clone();
        // run this whole connection under one task that owns its lifetime
        //
        // the two halves of a split stream keep the stream alive between them, so a read relay
        // that ends on its own would leave the write relay parked on an empty channel holding a
        // socket that nobody will ever read from again. owning the write task here is what lets
        // the end of the read relay actually close the connection
        //
        // TODO: do this with a task queue?
        glommio::spawn_local(async move {
            // shake hands before this stream is split, under a deadline so a peer that connects
            // and then stalls cannot hold this task open forever
            //
            // the handshake's own result is wrapped rather than converted, so that a peer that
            // stalled and a peer that was refused stay distinguishable in the log
            //
            // the deadline covers the TLS handshake and the authentication exchange as well as
            // the shoal handshake, since a peer that stalls between any two of them is holding
            // exactly as much of this server as one that stalls before all three
            let handshake = glommio::timer::timeout(HANDSHAKE_TIMEOUT, async {
                // take the wire before anything speaks the shoal protocol over it
                //
                // this has to come first: it is a handshake of its own, and everything below
                // reads and writes the socket expecting whatever this leaves behind. the
                // established session is bound rather than dropped so that rustls' record of it
                // outlives the socket, which is where a key update would be handled
                let _tls = match &tls {
                    Some(config) => match tls::accept(&mut stream, config).await {
                        Ok(established) => Some(established),
                        Err(error) => return Ok(Err(error)),
                    },
                    None => None,
                };
                // shake hands next, which is what decides whether there is anything to prove
                let (hello, mechanism) =
                    match server_handshake::<S>(&mut stream, max_frame_bytes, &store).await {
                        Ok(accepted) => accepted,
                        Err(error) => return Ok(Err(error)),
                    };
                // then prove it, if this server asked for anything
                let principal = match mechanism {
                    Some(mechanism) => {
                        match server_auth(&mut stream, mechanism, hello.max_frame_bytes, &store)
                            .await
                        {
                            Ok(principal) => Some(principal),
                            Err(error) => return Ok(Err(error)),
                        }
                    }
                    None => None,
                };
                Ok(Ok((hello, principal)))
            })
            .await;
            let (hello, principal) = match handshake {
                Ok(Ok(accepted)) => accepted,
                Ok(Err(error)) => {
                    event!(Level::WARN, msg = "refused a client", %client, ?error);
                    return;
                }
                Err(error) => {
                    event!(Level::WARN, msg = "a client never finished its handshake", %client, ?error);
                    return;
                }
            };
            // say who this connection belongs to, which is the only thing that consults a
            // principal today - authorization is what it exists for and does not exist yet
            match &principal {
                Some(principal) => {
                    event!(Level::INFO, msg = "authenticated a client", %client, %principal);
                }
                None => event!(Level::DEBUG, msg = "accepted a client", %client),
            }
            // break this stream up into a writer and a reader
            let (tcp_rx, tcp_tx) = stream.split();
            // create a channel for all of our shards to give data to send back to clients
            let (client_tx, client_rx) = kanal::unbounded_async();
            // tell every shard about this client before anything can send a query on its behalf
            //
            // the read relay pushes onto the same channels this broadcast uses, so broadcasting
            // first is what makes that ordering guaranteed on the local shard rather than
            // incidental
            let msg = ServerMsg::NewClient { client, client_tx };
            if let Err(error) = comms.broadcast(&msg).await {
                event!(Level::ERROR, msg = "failed to announce a client", %client, ?error);
                return;
            }
            // start writing responses back to this client, bounded by what it said it accepts
            let tx_task = glommio::spawn_local(client_tx_relay::<S>(
                client_rx,
                tcp_tx,
                hello.max_frame_bytes,
            ));
            // read this clients bundles until it goes away or sends something we refuse
            client_rx_relay(client, tcp_rx, node_local_tx, max_frame_bytes).await;
            // stop writing to a client that is not reading, which drops the last half of the
            // stream and closes the socket
            tx_task.cancel().await;
        })
        .detach();
    }
}

/// Watch an atomic bool for when this shard should shutdown
///
/// # Arguments
///
/// * `should_shutdown` - A flag to denote when this shoal node is shuttind down
/// * `shard_local_tx` - A channel for sending shard local messages over
async fn shutdown_watcher<S: ShoalDatabase>(
    should_shutdown: Arc<AtomicBool>,
    shard_local_tx: AsyncSender<ServerMsg<S>>,
) -> Result<(), ServerError> {
    // loop until we receive the shutdown command sleeping for 3 seconds after each check
    loop {
        // check if we should shutdown or not
        if should_shutdown.load(Ordering::Relaxed) {
            // shutdown order recieved so tell our shard
            shard_local_tx.send(ServerMsg::Shutdown).await?;
            // stop looping
            break;
        }
        // this shard is not yet shutting down so sleep for 3 seconds
        glommio::timer::sleep(std::time::Duration::from_secs(3)).await;
    }
    Ok(())
}

/// How to message a specific shard
///
/// A local contact is an index into the node's kanal mesh. A remote one names a node and a shard
/// on it, and is never an index into anything here: it is reached through the shard's peer links
/// ([F38](../../../docs/src/features/inter-node-transport.md)). `local_index` is the one way to
/// turn a contact into a mesh index, and it says no for a remote one rather than guessing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
    /// This shard is on another node of the cluster
    Remote {
        /// The node it is on
        node: NodeId,
        /// Which of that node's shards it is
        shard: u16,
    },
}

impl ShardContact {
    /// The mesh index this contact is, if it is on this node
    #[must_use]
    pub fn local_index(&self) -> Option<usize> {
        match self {
            ShardContact::Local(index) => Some(*index),
            ShardContact::Remote { .. } => None,
        }
    }

    /// The node this contact is on, if it is on another node
    #[must_use]
    pub fn remote_node(&self) -> Option<NodeId> {
        match self {
            ShardContact::Local(_) => None,
            ShardContact::Remote { node, .. } => Some(*node),
        }
    }
}

/// What a shard tells the pool that started it
///
/// Sent on a `std::sync::mpsc` channel rather than the kanal mesh because the receiver is the
/// thread that called `ShoalPool::start`, which is on no executor at all. Exactly one of these is
/// sent per shard before it enters its loop, and a second `Failed` may follow if the loop itself
/// returns an error - so a pool that has seen every shard's `Ready` can still learn of a death.
#[derive(Clone, Debug)]
pub enum ShardEvent {
    /// This shard has bound its listener, joined the mesh and started its loaders
    ///
    /// Recovery replayed in `Shard::new`, before any of that, so a shard that is `Ready` is a
    /// shard that answers.
    Ready {
        /// Which shard
        shard: usize,
        /// The address its listener actually bound, which is what a port of zero resolves to
        addr: SocketAddr,
    },
    /// This shard returned an error, either before it was ready or from its loop afterwards
    Failed {
        /// Which shard
        shard: usize,
        /// What it said
        error: String,
    },
}

/// The info for a specific shard in Shoal
#[derive(Clone, Debug)]
pub struct ShardInfo {
    /// The name for this shard
    pub name: String,
    /// How to message this shard
    pub contact: ShardContact,
}

impl ShardInfo {
    /// Build a new shard info object
    ///
    /// # Arguments
    ///
    /// * `id` - This shards id
    #[must_use]
    pub fn new(id: usize) -> Self {
        Self {
            name: format!("Shard-{id}"),
            contact: ShardContact::Local(id),
        }
    }

    /// Get this shards mesh id, if it is on this node
    ///
    /// A remote shard has no mesh id: it is reached through a peer link, never through the
    /// local channels.
    #[must_use]
    pub fn local_index(&self) -> Option<usize> {
        self.contact.local_index()
    }
}

#[instrument(name = "Shard::shutdown_tasks", skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn shutdown_tasks(tasks: Vec<Task<Result<(), ServerError>>>) -> Result<(), ServerError> {
    // cancel all of our tasks
    for task in tasks {
        // cancel this task
        if let Some(Err(error)) = task.cancel().await {
            // log this tasks error if it had one
            event!(Level::ERROR, error = format!("{error:#?}"));
        }
    }
    Ok(())
}

/// The shares of one query that was split across several shards
///
/// A query naming partitions on several shards is answered in pieces, but the client
/// is owed exactly one response for it. The shard that split the query keeps one of
/// these until every shard it sent a piece to has answered, then merges the pieces,
/// puts their rows back into the order the query named its partitions in, applies the
/// queries limit to their union, and replies once.
struct Gather<D: ShoalDatabase> {
    /// The id of the client waiting on this query
    client: Uuid,
    /// The span context for this query
    span: Span,
    /// When this query reached each stage on the shard that split it
    ///
    /// The shares each carry their own stamps and each become their own record, flagged as
    /// shares. This is the one the client actually waited on, so it is the one whose stages
    /// describe the latency the client saw.
    stamps: StageStamps,
    /// How many shards have not yet sent us their share
    outstanding: usize,
    /// The most rows this query asked for, if it set a limit
    limit: Option<usize>,
    /// The partitions this query named, in the order it named them
    ///
    /// Shares arrive in whatever order the shards answer in, so this is what the merged
    /// rows are put back into before the limit is applied to them.
    partition_order: Vec<u64>,
    /// The shares we have merged so far
    merged: Option<<D::ClientType as QuerySupport>::ResponseKinds>,
}

pub(super) struct Shard<D: ShoalDatabase> {
    /// This shards info
    info: ShardInfo,
    /// This shards index on the node's mesh, which is also its position in every ring
    shard_id: usize,
    /// The config for shoal
    conf: Conf,
    /// The token ring info for shoal
    ring: Ring,
    /// Handles communications across shoal shards/nodes
    comms: Comms<D>,
    /// The tables we are responsible for on this shard
    pub tables: D,
    /// The full archive map for all tables
    table_map: FullArchiveMap<D::TableNames>,
    /// A map of channels to send responses to our client relays over
    client_map: HashMap<Uuid, AsyncSender<Reply>>,
    /// The queries we split across several shards and are collecting the shares of
    ///
    /// Keyed by (query id, index), the pair that uniquely identifies one query within
    /// one bundle - the same key the tables use for their own partial results.
    gathering: HashMap<(Uuid, usize), Gather<D>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// The channel to Receive shard local messages on
    shard_local_rx: AsyncReceiver<ServerMsg<D>>,
    /// A map of storage systems and their loader channel
    loader_channels: HashMap<
        Loaders,
        (
            AsyncSender<LoaderMsg<D::TableNames>>,
            AsyncReceiver<LoaderMsg<D::TableNames>>,
        ),
    >,
    /// The responses whose queries have been flushed to disk
    flushed: Vec<(
        Uuid,
        Uuid,
        Span,
        StageStamps,
        <D::ClientType as QuerySupport>::ResponseKinds,
    )>,
    /// Whether a write has landed since the last time we swept our tables
    ///
    /// A durable watermark only ever advances behind a completed IO, and every
    /// completed IO sends a [`ServerMsg::DataFlushed`], so no pending response can
    /// become releasable until one of those messages has arrived.
    data_flushed: bool,
    /// The latency sensitive task queue
    high_priority: TaskQueueHandle,
    /// The medium priority task queue
    _medium_priority: TaskQueueHandle,
    /// The tasks we have spawned
    tasks: Vec<Task<Result<(), ServerError>>>,
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(D::TableNames, u64), usize, BuildHasherDefault<GxHasher>>>>,
    /// The address our client listener bound, once it has
    ///
    /// The config says which port to ask for; this says which one the kernel gave, which
    /// differs when the config asked for zero.
    bound: Option<SocketAddr>,
    /// What this shard needs to talk to peers, on a cluster node
    ///
    /// `None` on a standalone node, which builds no links and binds no peer listener. Present,
    /// it carries the placement, the identity and the bounds every link shares.
    peer_setup: Option<PeerSetup>,
    /// The outbound peer links this shard owns, once it has forwarded anything
    ///
    /// `None` on a standalone node. Built lazily on a cluster node so a shard that never
    /// forwards a query dials nothing ([F38](../../../docs/src/features/inter-node-transport.md)).
    peers: Option<Peers<D>>,
    /// Bytes this shard has received on bulk lanes, for the transport view
    bulk_received: Rc<Cell<u64>>,
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived]:
        DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Create a new shard
    ///
    /// # Arguments
    ///
    /// * `conf` - The config to build this shard with
    /// * `comms` - The channels to the other shards on this node
    /// * `shard_id` - This shards id, minted by the pool so a failure here can still name it
    /// * `shard_count` - The number of shards on this node
    #[instrument(name = "Shard::new", skip_all, err(Debug))]
    pub async fn new(
        conf: &Conf,
        comms: Comms<D>,
        shard_id: usize,
        shard_count: usize,
        peer_setup: Option<PeerSetup>,
    ) -> Result<Self, ServerError> {
        // get a handle to our current executor
        let executor = glommio::executor();
        // build our shard info
        let info = ShardInfo::new(shard_id);
        // create names for our high and low priority task queues
        let high_name = format!("HighPriority:{}", info.name);
        let medium_name = format!("MediumPriority:{}", info.name);
        // create a high priority queue for this task queue
        let high_priority = executor.create_task_queue(
            Shares::Static(1000),
            Latency::Matters(Duration::from_micros(500)),
            &high_name,
        );
        // create a medium priority queue for this task queue
        let medium_priority = executor.create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(100)),
            &medium_name,
        );
        // build an archive map across all tables
        let table_map = FullArchiveMap::default();
        // start with an empty loader channel map
        let mut loader_channels = HashMap::with_capacity(1);
        // start with an initial memory usage of 0
        let memory_usage = Arc::new(RefCell::new(0));
        // setup an xxh3 hasher for our lru cache
        let lru_hasher = BuildHasherDefault::<GxHasher>::default();
        // build our lru cache
        let lru = Arc::new(RefCell::new(LruCache::unbounded_with_hasher(lru_hasher)));
        // get the channels for this shards channel on this node
        let (shard_local_tx, shard_local_rx) = comms.get_shards_channels(shard_id);
        // build our shards tables
        let tables = D::new(
            &info.name,
            &table_map,
            &mut loader_channels,
            conf,
            medium_priority,
            &memory_usage,
            &lru,
            &shard_local_tx,
        )
        .await?;
        // build our tablet map: a standalone node's is its own shards, a cluster node's places
        // those shards among the peers the placement names, so a remote key routes to a remote
        // contact ([F38](../../../docs/src/features/inter-node-transport.md))
        let ring = match &peer_setup {
            Some(setup) => Ring::with_placement(shard_count, &setup.placement, setup.local.node)?,
            None => Ring::new(shard_count)?,
        };
        // build our shard
        let shard = Shard {
            info,
            shard_id,
            conf: conf.clone(),
            ring,
            comms,
            tables,
            table_map,
            client_map: HashMap::with_capacity(500),
            gathering: HashMap::with_capacity(100),
            shard_local_tx,
            shard_local_rx,
            loader_channels,
            flushed: Vec::with_capacity(1000),
            data_flushed: false,
            high_priority,
            _medium_priority: medium_priority,
            tasks: Vec::with_capacity(100),
            memory_usage,
            lru,
            bound: None,
            peer_setup,
            peers: None,
            bulk_received: Rc::new(Cell::new(0)),
        };
        Ok(shard)
    }

    /// Spawn our client network listener
    fn spawn_client_listener(&mut self) -> Result<(), ServerError> {
        // build this listener's tls config before it binds, if it has one
        //
        // reading a certificate off disk once per shard at startup rather than once per
        // connection, for the same reason the credential store is derived here: it is the same
        // work every time and a connection is the wrong place to discover a missing file
        let tls = match &self.conf.networking.tls {
            Some(options) => {
                // refuse to start rather than fall back to plaintext if the kernel cannot do this
                //
                // a server that asked for encryption and silently served in clear is the failure
                // this whole feature exists to prevent, so it is checked before anything binds
                if !crate::shared::tls::ktls::is_available() {
                    return Err(crate::shared::tls::TlsError::UlpUnavailable(
                        std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "the 'tls' kernel module is not loaded",
                        ),
                    )
                    .into());
                }
                Some(crate::shared::tls::server_config(options)?)
            }
            None => None,
        };
        // bind our tcp socket
        let tcp_sock = TcpListener::bind(self.conf.networking.to_addr())?;
        // remember what the kernel actually gave us, which is the only answer when the config
        // asked for port zero
        self.bound = Some(tcp_sock.local_addr()?);
        // clone our kanal transmitter
        let node_local_tx = self.shard_local_tx.clone();
        // spawn or client listener
        let handle = glommio::spawn_local_into(
            client_acceptor(
                tcp_sock,
                self.comms.clone(),
                node_local_tx,
                self.conf.networking.max_frame_bytes,
                // derive every credential this config named once per shard, at startup, rather
                // than once per connection - a PBKDF2 derivation is the whole point of the cost
                Rc::new(self.conf.auth.store()?),
                tls,
            ),
            self.high_priority,
        )?;
        // add this task to our task list
        self.tasks.push(handle);
        Ok(())
    }

    /// broadcast this join to all shards
    pub async fn join_cluster(&mut self) -> Result<(), ServerError> {
        // build our join message
        let join_msg = ServerMsg::Join(self.info.clone());
        // broadcast this message
        self.comms.broadcast(&join_msg).await?;
        Ok(())
    }

    /// Initialize this shard
    ///
    /// # Arguments
    ///
    /// * `mesh_rx` - The glommio channel to receive node local messages on
    ///
    /// # Errors
    ///
    /// This will only fail if the coordinator has not joined the local mesh.
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::init", skip_all, err(Debug))]
    async fn init(&mut self, should_shutdown: Arc<AtomicBool>) -> Result<(), ServerError> {
        // spawn our client listeners
        self.spawn_client_listener()?;
        // stand up the peer listener and links, on a cluster node
        self.spawn_peer_listener()?;
        // broadcast our join message
        self.join_cluster().await?;
        // start our loaders
        self.tables
            .init_storage_loaders(
                &self.table_map,
                &mut self.loader_channels,
                &self.shard_local_tx,
            )
            .await?;
        // spawn our shutdown watcher
        let handle = glommio::spawn_local_into(
            shutdown_watcher(should_shutdown, self.shard_local_tx.clone()),
            self._medium_priority,
        )?;
        // add this task to our task list
        self.tasks.push(handle);
        // report what our recovery discarded now that every table has been replayed
        self.report_recovery();
        Ok(())
    }

    /// Report what replaying this shards intent logs discarded
    ///
    /// Tables are built in `Shard::new`, so by the time a shard finishes initializing
    /// this is everything its recovery lost. There is no equivalent report across a
    /// whole pool yet: `ShoalPool::ready` now knows the moment every shard has finished
    /// starting, which is what such a report would need, and it is still filed in
    /// `docs/src/appendix/todos.md`.
    fn report_recovery(&self) {
        // gather what every table on this shard had to discard
        let recovery = self.tables.recovery_stats();
        // an operator has to be told about loss, so say it at a level they will see
        if recovery.is_clean() {
            event!(
                Level::INFO,
                msg = "Recovery complete",
                shard = self.info.name,
                updates_after_delete = recovery.updates_after_delete,
            );
        } else {
            event!(
                Level::WARN,
                msg = "Recovery discarded data",
                shard = self.info.name,
                orphaned_updates = recovery.orphaned_updates,
                unreplayable_entries = recovery.unreplayable_entries,
                truncated_logs = recovery.truncated_logs,
                updates_after_delete = recovery.updates_after_delete,
            );
        }
    }

    /// Forward our queries to the correct shards
    ///
    /// Nothing here deserializes a query. Every field this reads - the partition keys, the
    /// limit, the bundles id and base index - is a scalar sitting inline in the archive, so a
    /// bundle of megabyte rows is routed for the cost of its keys
    /// ([F26](../../../docs/src/features/archive-routed-requests.md)). The shard that answers a
    /// query is the shard that pays for turning it back into one.
    ///
    /// # Arguments
    ///
    /// * `client` - The client that sent this bundle
    /// * `request` - The root span the relay opened for the bundle being routed
    /// * `body` - The buffer the bundle arrived in, shared with every shard it routes to
    /// * `queries` - The bundle, read out of that buffer
    /// * `stamps` - When this bundle reached each stage so far
    ///
    /// This is deliberately **not** instrumented as a whole. The span it used to open was one
    /// per bundle and was what every query in that bundle took as its parent, so a batch of a
    /// hundred queries produced one flat list of a hundred siblings. The span opened per query
    /// below replaces it - the routing work itself is attributed to `Coordinator::handle_client`,
    /// which is the caller and covers exactly the same instants.
    async fn send_to_shard(
        &mut self,
        client: Uuid,
        request: &Span,
        body: &Bytes,
        queries: &ArchivedQueries<D::ClientType>,
        stamps: StageStamps,
    ) -> Result<(), ServerError> {
        // an empty bundle has no last query, and nothing to send either way
        let Some(last_offset) = queries.queries.len().checked_sub(1) else {
            return Ok(());
        };
        // remember how many queries this bundle held
        //
        // a queries position in its batch is uninterpretable without this beside it, since
        // position four means something very different in a batch of five than in one of five
        // hundred
        let batch_len = queries.queries.len();
        // read the bundles own scalars out of the archive
        //
        // a uuid archives to itself - rkyv's `Archived` for it is `Uuid`, since it is sixteen
        // bytes with no endianness to have - while a usize is stored little endian and has to
        // be read back into whatever this host uses
        let bundle_id = queries.id;
        let base_index = queries.base_index.to_native() as usize;
        // initialize a vec to store the per shard shares we find
        let mut found = Vec::with_capacity(3);
        // the remote shares of this bundle, gathered per node into one forward each
        let mut remote: HashMap<NodeId, Vec<(crate::shared::protocol::peer::ForwardEntry, Pending<D>)>> =
            HashMap::new();
        // get the absolute index for the last query in this bundle
        //
        // every index below is absolute, so this has to carry the base index too or a
        // streamed bundle would compare an absolute index against a relative one
        let end_index = base_index + last_offset;
        // crawl over our queries
        for (offset, kind) in queries.queries.iter().enumerate() {
            // get this queries absolute index in its stream
            //
            // this is per query and not per shard, so that every shard answering one
            // query answers it under the same index
            let index = offset + base_index;
            // check if this is the last query or not
            let end = index == end_index;
            // open the span this query and everything it causes hangs off
            //
            // per query rather than per bundle, so a batch is one trace with one subtree per
            // query in it. it is the parent every hop from here rejoins through - the shard
            // that executes the query, the loader that reads its partition, the flush that
            // makes its write durable, and the write of its response - so it lives until the
            // last of those has finished with the metadata carrying it
            let query_span = info_span!(
                parent: request,
                "Coordinator::route",
                id = %bundle_id,
                index,
            );
            // give this query its own copy of the bundles stamps to carry from here on
            let mut stamps = stamps;
            // note where in its batch this query sat, since a query near the tail of a
            // bundle waits on every query ahead of it and that is not a server side cost
            stamps.set_batch(offset, batch_len);
            // remember the index this query answers under, which is half of a records key
            stamps.set_index(index);
            // find the shards that answer this query, and the keys each of them owns
            <<D::ClientType as QuerySupport>::QueryKinds as ArchivedShardRouting>::route_archived(
                kind,
                &self.ring,
                &mut found,
            );
            // record that this query is leaving us for the shards that own its partitions
            stamps.mark_routed();
            // a query answered by one shard alone is replied to directly, so only a
            // query we actually split needs its shares collected back here
            let gather = if found.len() > 1 {
                // remember what we are owed before we send anything, so a share that
                // comes straight back to us still finds somewhere to land
                let gather = Gather {
                    client,
                    span: query_span.clone(),
                    stamps,
                    outstanding: found.len(),
                    limit: <<D::ClientType as QuerySupport>::QueryKinds as ArchivedShardRouting>::archived_limit(kind),
                    // remember the order this query named its partitions in, since the
                    // narrowed queries only carry each shards own share of them
                    partition_order: <<D::ClientType as QuerySupport>::QueryKinds as ArchivedShardRouting>::archived_partition_keys(kind),
                    merged: None,
                };
                self.gathering.insert((bundle_id, index), gather);
                // tell every shard we split this to answer back to us
                Some(self.info.contact.clone())
            } else {
                None
            };
            // note whether the share each shard carries is a share of a query we split
            //
            // a split query produces one of these per shard plus the one client visible
            // record the gather emits, so a report that counted them all would multiply
            // count it. The copy we kept in the gather above is deliberately not flagged.
            let mut share_stamps = stamps;
            share_stamps.set_share_of_gathered(gather.is_some());
            // the table this query names, so a peer that never answers can be answered with a
            // failure in the right variant
            let table = <D::ClientType as QuerySupport>::archived_query_table(kind);
            // the trace this query is part of, put on every remote entry so the remote work
            // hangs off this span rather than off the bundle's root
            let trace = trace::context_of(&query_span);
            // send each share to its shard: a local one over the mesh, a remote one into a
            // forward accumulated per node ([F38](../../../docs/src/features/inter-node-transport.md))
            for (shard_info, keys) in found.drain(..) {
                match &shard_info.contact {
                    ShardContact::Local(_) => {
                        // where this share ran, relative to the shard that accepted the bundle
                        let mut share_stamps = share_stamps;
                        share_stamps.set_hop(if shard_info.contact == self.info.contact {
                            stage_profile::StageHop::Same
                        } else {
                            stage_profile::StageHop::LocalShard
                        });
                        let meta = QueryMetadata::new(
                            client,
                            bundle_id,
                            index,
                            end,
                            gather.clone(),
                            query_span.clone(),
                            share_stamps,
                        );
                        let msg = ServerMsg::Query {
                            meta,
                            body: body.clone(),
                            offset,
                            keys,
                        };
                        self.comms.send(&shard_info.contact, msg).await?;
                    }
                    ShardContact::Remote { node, shard } => {
                        // this share crosses a node; the op it turns out to be comes back with
                        // the answer, since nothing here decodes the query
                        let mut share_stamps = share_stamps;
                        share_stamps.set_hop(stage_profile::StageHop::RemoteNode);
                        // one entry per remote share, gathered per node below
                        // truncation cannot happen: a bundle holds far fewer than a u32 of queries
                        #[allow(clippy::cast_possible_truncation)]
                        let entry = crate::shared::protocol::peer::ForwardEntry {
                            offset: offset as u32,
                            index: index as u64,
                            end,
                            shard: *shard,
                            origin_shard: self.shard_id as u16,
                            gather: gather.is_some(),
                            trace,
                            keys: keys.unwrap_or_default(),
                        };
                        let slot = remote.entry(*node).or_insert_with(Vec::new);
                        slot.push((
                            entry,
                            Pending {
                                client,
                                span: query_span.clone(),
                                stamps: share_stamps,
                                table,
                                end,
                                share: gather.is_some(),
                                sent_at: Stamp::now(),
                            },
                        ));
                    }
                }
            }
        }
        // flush one forward per node, and answer at once anything the queue could not take
        self.flush_forwards(body, bundle_id, base_index, remote).await?;
        Ok(())
    }

    /// Send the remote shares of a bundle as one forward per node, answering what is shed
    ///
    /// The queue to a peer is bounded in bytes; a forward that would pass the bound is a
    /// definite refusal, since nothing was accepted, and every entry of it is answered
    /// [`ErrorCode::Shedding`] on the spot. A forward that is queued is recorded as pending,
    /// and its fate becomes the deadline sweep's or the link's to report
    /// ([F38](../../../docs/src/features/inter-node-transport.md)).
    ///
    /// # Arguments
    ///
    /// * `body` - The bundle's bytes, shared into the forward without a copy
    /// * `bundle_id` - The bundle these queries arrived in
    /// * `base_index` - The bundle's base index
    /// * `remote` - The remote shares, grouped by node
    #[allow(clippy::future_not_send)]
    async fn flush_forwards(
        &mut self,
        body: &Bytes,
        bundle_id: Uuid,
        base_index: usize,
        remote: HashMap<NodeId, Vec<(crate::shared::protocol::peer::ForwardEntry, Pending<D>)>>,
    ) -> Result<(), ServerError> {
        for (node, shares) in remote {
            let Some(peers) = self.peers.as_mut() else {
                // a node routed to a remote contact without peers is a bug in setup, not a
                // query; answer every share so no client waits forever
                for (entry, pending) in shares {
                    self.fail_forward(node, bundle_id, entry.index, pending, ErrorCode::Internal, "this node has no peers")
                        .await?;
                }
                continue;
            };
            // the deadline the origin will wait, from now
            let deadline = peers.transport().forward_timeout.duration();
            // split the shares into the entries the frame carries and the pendings we record
            let mut entries = Vec::with_capacity(shares.len());
            let mut pendings = Vec::with_capacity(shares.len());
            let mut keys = Vec::with_capacity(shares.len());
            for (entry, pending) in shares {
                keys.push((bundle_id, entry.index));
                entries.push(entry);
                pendings.push(pending);
            }
            // build the forward: preamble, entries, then the bundle bytes shared not copied
            let entry_bytes = crate::shared::protocol::peer::encode_entries(&entries)?;
            // truncation cannot happen: a bundle holds far fewer than a u32 of entries
            #[allow(clippy::cast_possible_truncation)]
            let preamble = crate::shared::protocol::peer::ForwardPreamble {
                bundle: *bundle_id.as_bytes(),
                attempt: 0,
                base_index: base_index as u64,
                hops: 0,
                remaining_ms: deadline.as_millis().min(u128::from(u32::MAX)) as u32,
                entries: entries.len() as u16,
                entries_len: entry_bytes.len() as u32,
            }
            .encode();
            let max = self.peer_setup.as_ref().map_or(u32::MAX, |setup| setup.local.max_frame_bytes);
            let frame = Frame::new(
                MessageType::Forward,
                vec![
                    Bytes::copy_from_slice(&preamble),
                    Bytes::from(entry_bytes),
                    body.clone(),
                ],
                FrameKey::Forward(keys),
                max,
            )?;
            // try to queue it, answering every entry with a definite refusal if the queue is full
            match self.peers.as_mut().expect("peers exist here").enqueue(node, Lane::Data, frame) {
                Ok(()) => {
                    // recorded as pending, one entry at a time
                    for (entry, pending) in entries.into_iter().zip(pendings) {
                        self.peers
                            .as_mut()
                            .expect("peers exist here")
                            .expect(bundle_id, entry.index, node, pending);
                    }
                }
                Err(_) => {
                    // the queue is full, so nothing was accepted: a definite refusal
                    for (entry, pending) in entries.into_iter().zip(pendings) {
                        self.fail_forward(
                            node,
                            bundle_id,
                            entry.index,
                            pending,
                            ErrorCode::Shedding,
                            "the queue to this node is full",
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Answer one forwarded query with a failure this node produced
    ///
    /// # Arguments
    ///
    /// * `node` - The peer the query was for
    /// * `bundle_id` - The bundle it arrived in
    /// * `index` - The index it is owed under
    /// * `pending` - What is owed
    /// * `code` - The class of failure
    /// * `msg` - What to say about it
    #[allow(clippy::future_not_send)]
    async fn fail_forward(
        &mut self,
        _node: NodeId,
        bundle_id: Uuid,
        index: u64,
        pending: Pending<D>,
        code: ErrorCode,
        msg: &str,
    ) -> Result<(), ServerError> {
        // build the failure in the query's own table variant, so the client reads it as one
        let error = crate::shared::responses::ResponseError::new(code, msg);
        let response = <D::ClientType as QuerySupport>::failed(
            pending.table,
            bundle_id,
            index as usize,
            pending.end,
            error,
        );
        // a share of a split query is merged like any other; a whole answer goes to the client
        if pending.share {
            let meta = QueryMetadata::untimed(
                pending.client,
                bundle_id,
                index as usize,
                pending.end,
                Some(self.info.contact.clone()),
                pending.span,
            );
            self.handle_gathered(meta, response).await
        } else {
            self.reply(pending.client, bundle_id, pending.span, pending.stamps, response)
                .await
        }
    }

    /// Handle a client messages
    ///
    /// # Arguments
    ///
    /// * `peer` - The client this bundle came from
    /// * `span` - The root span the relay opened when this bundle came off the socket
    /// * `data` - The bundle to route
    /// * `base` - When the last byte of this bundle came off the socket
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Coordinator::handle_client",
        parent = &span,
        skip(self, peer, span, data),
        err(Debug)
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_client<'a>(
        &mut self,
        peer: Uuid,
        span: Span,
        data: RequestBody,
        base: Stamp,
    ) -> Result<(), ServerError>
    where
        for<'b> <<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // start this bundles stamps from when its last byte came off the socket
        let mut stamps = StageStamps::new(base);
        // record that we have dequeued this bundle, which closes the ingress queue stage
        stamps.mark_bundle_dequeued();
        // hand this body over as a buffer every shard that answers part of it can hold at once
        //
        // this happens before the archive is read rather than after, so that the archive and
        // the clones handed to each shard all borrow the same buffer
        let body = data.freeze();
        // check that these bytes really are a bundle of this schemas queries
        //
        // this is the only validation the bundle gets. every shard it routes to reads its own
        // query straight out of these same bytes without walking them again, which is only
        // sound because this ran first - see `ShoalDatabase::unarchive_queries`
        let archived = Queries::access(&body)?;
        // record that this bundle is readable, which is now all this stage covers
        //
        // it used to cover deserializing every query in the bundle as well. that moved to the
        // shards that execute them, where it is stamped as `query_decoded`
        // ([F26](../../../docs/src/features/archive-routed-requests.md)). this stage is still
        // paid once per bundle and charged to every query in it, so the report still has to
        // label it as a batch level cost rather than a per query one
        stamps.mark_decoded();
        // route every query in the bundle to the shards that answer it
        self.send_to_shard(peer, &span, &body, archived, stamps)
            .await
    }

    /// Send a respones back to the client
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to send this reply too
    /// * `response` - The response to send
    /// * `stamps` - When this query reached each stage so far, and its index
    #[instrument(
        name = "Shard::reply",
        parent = &span,
        skip_all,
        fields(id = %query_id),
        err(Debug)
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn reply(
        &mut self,
        client: Uuid,
        query_id: Uuid,
        span: Span,
        mut stamps: StageStamps,
        response: <D::ClientType as QuerySupport>::ResponseKinds,
    ) -> Result<(), ServerError> {
        // read the index and the end flag off the response before it is bytes
        //
        // a client relay never uses them, but a peer relay frames an answer by bundle and index,
        // and reading the index back out of the archive it just sealed would mean validating
        // what it wrote ([F38](../../../docs/src/features/inter-node-transport.md))
        let index = <<D::ClientType as QuerySupport>::ResponseKinds as ShoalResponseSupport>::index(&response);
        let end = <<D::ClientType as QuerySupport>::ResponseKinds as ShoalResponseSupport>::end(&response);
        // archive our response
        let archived = rkyv::to_bytes::<_>(&response)?;
        // record what serializing this response cost
        //
        // this is a whole row through rkyv rather than a queue hop, so it is one of the few
        // stages on the get path large enough to be worth measuring on its own
        stamps.mark_replied();
        // and hand the bytes on the same way an answer serialized in the table is
        self.reply_sealed(client, query_id, index, end, ReplyKind::Whole, span, stamps, archived)
            .await
    }

    /// Send a reply that has already been serialized back to the client
    ///
    /// A get whose partitions are all resident is serialized inside the table that found its
    /// rows, because those rows cannot outlive the scan
    /// ([O2](../../../docs/src/features/grouped-responses.md)). Its bytes arrive here having
    /// already been stamped `exec_done` and `replied`, so this is [`Self::reply`] with the
    /// serialize taken out - and [`Self::reply`] is now written in terms of it, so there is one
    /// path to the relay rather than two.
    ///
    /// # Arguments
    ///
    /// * `client` - The client to send this reply to
    /// * `query_id` - The id of the query being answered
    /// * `span` - The span to reply under
    /// * `stamps` - When this query reached each stage so far, and its index
    /// * `archived` - The serialized response
    #[allow(clippy::too_many_arguments)]
    async fn reply_sealed(
        &mut self,
        client: Uuid,
        query_id: Uuid,
        index: usize,
        end: bool,
        kind: ReplyKind,
        span: Span,
        mut stamps: StageStamps,
        archived: rkyv::util::AlignedVec<16>,
    ) -> Result<(), ServerError> {
        // get this clients channel to send replies over
        match self.client_map.get(&client) {
            Some(client_tx) => {
                // note that this response is now the relays problem rather than ours
                stamps.mark_queued_to_client();
                // a send that fails is a client whose relay has already ended - its socket
                // closed with this answer still owed. That is not this shard's failure to
                // report: it happens on every connection that leaves mid query, and a peer
                // link that is cut and reconnects makes it happen often
                // ([Resolved #32, #94](../../../docs/src/appendix/resolved/disconnected-client-cleanup.md)).
                // The channel is dropped when the client's `ClientGone` arrives; until then a
                // late answer is logged and let go
                if client_tx
                    .send(Reply {
                        id: query_id,
                        index,
                        end,
                        kind,
                        span,
                        stamps,
                        archived,
                    })
                    .await
                    .is_err()
                {
                    event!(
                        Level::DEBUG,
                        msg = "an answer was owed to a client that had left",
                        %client,
                        %query_id,
                    );
                }
            }
            // a client with no channel is one that has already gone away and been retired; its
            // answer has nowhere to go and is dropped, not panicked on. Panicking here ended
            // the shard and every other client on it, which is the defect this closes
            None => event!(
                Level::DEBUG,
                msg = "an answer was owed to a client that is gone",
                %client,
                %query_id,
            ),
        }
        Ok(())
    }

    /// Handle a query on this shard
    ///
    /// This is where a query is turned back into one. The coordinator routed it by the scalars
    /// in its archive and handed on the buffer it arrived in, so every `String`, `Vec` and
    /// filter it carries is copied out here, on the shard that is about to read them, rather
    /// than on the one core every request passes through
    /// ([F26](../../../docs/src/features/archive-routed-requests.md)).
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about the query to handle
    /// * `body` - The bundle this query arrived in
    /// * `offset` - Which query in that bundle this is
    /// * `keys` - The partition keys this shard owns, if the query was narrowed to a subset
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Shard::handle_query",
        parent = &meta.span,
        skip(self, body, keys),
        fields(index = meta.index, id = meta.id.to_string())
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_query(
        &mut self,
        mut meta: QueryMetadata,
        body: &Bytes,
        offset: usize,
        keys: Option<Vec<u64>>,
    ) -> Result<(), ServerError>
    where
        for<'a> <<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // copy our span for it we reply
        let span = meta.span.clone();
        // keep a copy of our metadata only if this query was actually split
        //
        // a share of a split query has to travel back with the metadata it came from, but
        // this used to be cloned for every query whether or not one was ever needed, which
        // paid for a gather on the overwhelming majority that never have one. See O26.
        let gathered_meta = meta.gather.is_some().then(|| meta.clone());
        // record that this shard now has this query in hand, closing the routing queue stage
        meta.stamps.mark_exec_dequeued();
        // read the bundle back out of the buffer it arrived in
        //
        // SAFETY: the coordinator validated these exact bytes with `Queries::access` before
        // sharing them, and a `Bytes` cannot be written to, so nothing has changed them since.
        // Revalidating here would mean walking the whole bundle to reach one query in it.
        let archived = unsafe { D::unarchive_queries(body) };
        // turn our own query in it back into one we can execute
        //
        // the index cannot be out of range: `send_to_shard` takes it from `enumerate` over this
        // same bundle and puts the two in one message, so an offset only ever travels with the
        // buffer it was read from
        let query = D::deserialize_query(&archived.queries[offset])?;
        // narrow it to the partitions this shard owns, if the coordinator split it
        let query = match keys {
            Some(keys) => query.narrow_to(keys),
            // a write named one partition and was routed by it, so there is nothing to narrow
            None => query,
        };
        // record what turning our share of this bundle back into a query cost
        //
        // this is the per query half of what `decoded` used to hold whole, and unlike
        // `decoded` it is paid on the shard that reads the row rather than on the coordinator
        meta.stamps.mark_query_decoded();
        // execute it now that it is a query rather than bytes
        self.execute_query(meta, query, span, gathered_meta).await
    }

    /// Run a query again after the partition it was parked on was read
    ///
    /// A released query was decoded and narrowed when it first arrived and has been sitting in
    /// a tables `blocked` map ever since, so there is no bundle to read it out of and no
    /// decode to charge it for a second time. That is the whole difference between this and
    /// [`Shard::handle_query`], and it is why the two are separate messages.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata about the query being run again
    /// * `query` - The query, as it was when it was parked
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Shard::handle_released",
        parent = &meta.span,
        skip(self, query),
        fields(index = meta.index, id = meta.id.to_string())
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_released(
        &mut self,
        mut meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
    ) -> Result<(), ServerError> {
        // copy our span for it we reply
        let span = meta.span.clone();
        // keep a copy of our metadata only if this query was actually split
        let gathered_meta = meta.gather.is_some().then(|| meta.clone());
        // record that this shard has this query in hand again
        //
        // this is deliberately not `mark_exec_dequeued`: that stamp is what `query_decode` is
        // measured from, and this query was decoded on the pass that parked it. Overwriting it
        // here would report every parked query as having decoded in no time at all, and would
        // put the whole disk wait inside `execute` as well as inside `exec_queue`
        meta.stamps.mark_exec_resumed();
        // execute it, with nothing to decode
        self.execute_query(meta, query, span, gathered_meta).await
    }

    /// Execute a query and answer whoever is owed the answer
    ///
    /// This is everything both ways into a shard have in common: a query, and whether its
    /// answer belongs to a client or to the shard collecting the shares of a split query.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `query` - The query to execute
    /// * `span` - The span to reply under
    /// * `gathered_meta` - The metadata to answer with, if this is a share of a split query
    #[allow(clippy::future_not_send)]
    async fn execute_query(
        &mut self,
        meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        span: Span,
        gathered_meta: Option<QueryMetadata>,
    ) -> Result<(), ServerError> {
        // remember the index and the end flag before `handle` consumes the metadata: a sealed
        // answer owed to a peer needs them to be framed, and it cannot read them back out of
        // the bytes it just sealed
        let (m_index, m_end) = (meta.index, meta.end);
        // try to handle this query
        if let Some((addr, query_id, mut stamps, answer)) = self.tables.handle(meta, query).await {
            // an answer the table already serialized has nothing left to do here but be sent
            //
            // it stamped `exec_done` and `replied` itself, on either side of the serialize it
            // ran while the rows were still in the partitions holding them
            // ([O2](../../../docs/src/features/grouped-responses.md)). It can only be a whole
            // answer, never a share, because a share has to be merged somewhere else and bytes
            // cannot be - but that whole answer may be owed to a client or to a peer that
            // forwarded the query, which is why the kind is carried
            let Answer::Open(response) = answer else {
                let Answer::Sealed(archived) = answer else {
                    unreachable!("an answer is either open or sealed")
                };
                return self
                    .reply_sealed(addr, query_id, m_index, m_end, ReplyKind::Whole, span, stamps, archived)
                    .await;
            };
            // record that this queries synchronous work is finished
            //
            // a query that parks on the intent log returns nothing here and stamps its own
            // `exec_done` when its commit returns, so this only covers the ones we can
            // answer in a single pass
            stamps.mark_exec_done();
            // a share of a query someone else split goes back to them, not to the client
            match gathered_meta {
                Some(gathered_meta) => {
                    // this query was split, so we know it named a shard to collect its shares
                    let contact = gathered_meta
                        .gather
                        .clone()
                        .expect("A gathered query always names the shard collecting it");
                    // a share collected on this node goes over the mesh; one collected on the
                    // node that forwarded the query goes back down the peer connection it came
                    // in on, as a share the origin merges
                    // ([F38](../../../docs/src/features/inter-node-transport.md))
                    if contact.remote_node().is_some() {
                        let archived = rkyv::to_bytes::<_>(&response)?;
                        stamps.mark_replied();
                        self.reply_sealed(
                            gathered_meta.client,
                            gathered_meta.id,
                            gathered_meta.index,
                            gathered_meta.end,
                            ReplyKind::Share,
                            span,
                            stamps,
                            archived,
                        )
                        .await?;
                    } else {
                        // build the message carrying our share of this queries answer
                        let msg = ServerMsg::Gathered {
                            meta: gathered_meta,
                            response,
                        };
                        // send our share to the shard collecting them
                        self.comms.send(&contact, msg).await?;
                    }
                }
                // this query was ours alone to answer
                None => self.reply(addr, query_id, span, stamps, response).await?,
            }
        }
        Ok(())
    }

    /// Collect one shards share of a query we split across several shards
    ///
    /// The client is owed exactly one response per query, so the shares are merged
    /// here and answered once, after the last shard we are waiting on has reported.
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for the query this is part of the answer to
    /// * `response` - This shards share of the answer
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Shard::handle_gathered",
        parent = &meta.span,
        skip(self, response),
        fields(index = meta.index, id = meta.id.to_string()),
        err(Debug)
    )]
    async fn handle_gathered(
        &mut self,
        meta: QueryMetadata,
        response: <D::ClientType as QuerySupport>::ResponseKinds,
    ) -> Result<(), ServerError> {
        // find the query this share belongs to
        let Some(gather) = self.gathering.get_mut(&(meta.id, meta.index)) else {
            // we already answered this query, so this share arrived after we stopped
            // waiting for it and there is nothing left to merge it into
            event!(
                Level::WARN,
                msg = "A share arrived for a query we already answered",
                id = meta.id.to_string(),
                index = meta.index
            );
            return Ok(());
        };
        // merge this share into what we have collected so far
        match &mut gather.merged {
            Some(merged) => merged.merge(response),
            // this is the first share we have seen for this query
            None => gather.merged = Some(response),
        }
        // we are waiting on one fewer shard than we were
        gather.outstanding -= 1;
        // wait for the rest of our shares if any are still outstanding
        if gather.outstanding > 0 {
            return Ok(());
        }
        // every shard has reported, so this query is ours to answer now
        let Some(gather) = self.gathering.remove(&(meta.id, meta.index)) else {
            return Ok(());
        };
        // a query with no shares at all has nothing to answer with
        let Some(mut merged) = gather.merged else {
            return Ok(());
        };
        // the shares were merged as they arrived, so put their rows back into the order
        // this query named its partitions in
        //
        // this has to happen before the limit is applied, or the rows kept would be the
        // first ones to arrive rather than the first ones asked for
        merged.order_by_partitions(&gather.partition_order);
        // each shard applied our limit as it scanned, but their union can still be
        // over it, so trim it back down to what was actually asked for
        if let Some(limit) = gather.limit {
            merged.truncate(limit);
        }
        // record that the work behind this query is finished
        //
        // for a split query that is the moment the last share landed and was merged, since
        // nothing before then could have answered the client
        let mut stamps = gather.stamps;
        stamps.mark_exec_done();
        // send our merged response back to the client
        self.reply(gather.client, meta.id, gather.span, stamps, merged)
            .await
    }

    /// Get all flushed messages and send their response back
    ///
    /// This is deliberately not inside a `tracing` span. It used to be, and the span was
    /// created once per message the shard handled — 711,638 times in the profiled run —
    /// on a path that almost always has nothing to do. `Shard::reply` parents itself off
    /// the query's own span rather than off this one, so nothing is orphaned by its
    /// absence.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_flushed(&mut self) -> Result<(), ServerError> {
        // consume our wakeup signal now that we are acting on it
        self.data_flushed = false;
        // get all flushed query responses
        self.tables.handle_flushed(&mut self.flushed).await?;
        // pop all of our flushed responses
        while let Some((client, query_id, span, stamps, response)) = self.flushed.pop() {
            // send our responses
            self.reply(client, query_id, span, stamps, response).await?;
        }
        Ok(())
    }

    /// Route a bundle another node forwarded to this one
    ///
    /// This is the coordinator's job seen from the far side of a hop. The listener read the
    /// frame and judged its lengths; this validates the bundle - a process boundary trusts
    /// nothing it did not check - checks every offset against it, opens a span per entry that
    /// hangs off the origin's own query span, and hands each entry to the shard it names as a
    /// query whose "client" is the peer connection ([F38](../../../docs/src/features/inter-node-transport.md)).
    ///
    /// # Arguments
    ///
    /// * `conn` - The peer connection every answer goes back down
    /// * `origin` - The node that forwarded this bundle
    /// * `preamble` - The forward's fixed fields
    /// * `entries` - Which queries this node answers, and on which shards
    /// * `data` - The bundle's bytes
    /// * `base` - When the last byte of it came off the socket
    #[allow(clippy::future_not_send)]
    async fn handle_forward(
        &mut self,
        conn: Uuid,
        origin: NodeId,
        preamble: crate::shared::protocol::peer::ForwardPreamble,
        entries: Vec<crate::shared::protocol::peer::ForwardEntry>,
        data: RequestBody,
        base: Stamp,
    ) -> Result<(), ServerError>
    where
        for<'a> <<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // hand the bundle over as a shared buffer, then validate it: this is the process
        // boundary, so these bytes are checked here whatever a peer said about them
        let body = data.freeze();
        let archived = Queries::<D::ClientType>::access(&body)?;
        let bundle = Uuid::from_bytes(preamble.bundle);
        // route every entry the peer named to the shard it named
        for entry in entries {
            // an offset past the bundle is a peer out of step with us, and ends this bundle
            if entry.offset as usize >= archived.queries.len() {
                return Err(ProtocolError::MalformedForward("a forward names a query the bundle does not hold").into());
            }
            // the span this entry's work hangs off, joined to the origin's trace if it sent one
            let span = info_span!(parent: None, "Shoal::forwarded", id = %bundle, index = entry.index);
            if let Some(trace) = &entry.trace {
                trace::adopt_remote_parent(&span, trace);
            }
            // stamps that start from when the frame arrived, marked as a peer's own record
            let mut stamps = StageStamps::new(base);
            stamps.set_index(entry.index as usize);
            stamps.set_hop(stage_profile::StageHop::RemoteNode);
            stamps.set_served_for_peer(true);
            // a share goes back to the origin as a share; a whole answer as a whole answer. Both
            // are answered to the peer connection, so both name it as the gather target when the
            // origin split the query
            let gather = if entry.gather {
                Some(ShardContact::Remote {
                    node: origin,
                    shard: entry.origin_shard,
                })
            } else {
                None
            };
            let meta = QueryMetadata::new(
                conn,
                bundle,
                entry.index as usize,
                entry.end,
                gather,
                span,
                stamps,
            );
            let keys = if entry.keys.is_empty() {
                None
            } else {
                Some(entry.keys)
            };
            // hand it to the shard that owns its partitions, over the mesh
            self.comms
                .send(&ShardContact::Local(usize::from(entry.shard)), ServerMsg::Query {
                    meta,
                    body: body.clone(),
                    offset: entry.offset as usize,
                    keys,
                })
                .await?;
        }
        Ok(())
    }

    /// Act on what one of this shard's peer links learned
    ///
    /// # Arguments
    ///
    /// * `event` - The link event, or the deadline tick
    #[allow(clippy::future_not_send)]
    async fn handle_peer_event(&mut self, event: PeerEvent) -> Result<(), ServerError> {
        match event {
            PeerEvent::Link(LinkEvent::Up { node, lane, incarnation }) => {
                event!(Level::DEBUG, msg = "a peer link came up", %node, %lane, incarnation);
            }
            PeerEvent::Link(LinkEvent::Frame { node, header, head, payload, .. }) => {
                self.handle_forwarded(node, header, &head, payload).await?;
            }
            PeerEvent::Link(LinkEvent::Down { node, unsent, reason, .. }) => {
                event!(Level::WARN, msg = "a peer link went down", %node, reason);
                self.resolve_lost_link(node, &unsent).await?;
            }
            PeerEvent::Tick => self.sweep_deadlines().await?,
        }
        Ok(())
    }

    /// Turn an answer a peer sent back into a reply, a share, or a failure
    ///
    /// # Arguments
    ///
    /// * `node` - The peer that answered
    /// * `header` - The frame's header
    /// * `head` - The forwarded preamble
    /// * `payload` - The answer's bytes
    #[allow(clippy::future_not_send)]
    async fn handle_forwarded(
        &mut self,
        node: NodeId,
        header: Header,
        head: &[u8],
        payload: AlignedVec,
    ) -> Result<(), ServerError> {
        // only a forwarded frame comes back on a data link
        if header.kind != MessageType::Forwarded {
            return Ok(());
        }
        let raw: [u8; crate::shared::protocol::peer::FORWARDED_PREAMBLE_LEN] =
            head.try_into().map_err(|_| ProtocolError::MalformedForward("a forwarded head is the wrong size"))?;
        let preamble = crate::shared::protocol::peer::ForwardedPreamble::decode(&raw)?;
        let bundle = Uuid::from_bytes(preamble.bundle);
        // find what we were owed; a frame with no pending entry is late or duplicate and dropped
        let Some(mut pending) = self
            .peers
            .as_mut()
            .and_then(|peers| peers.take(bundle, preamble.index, node))
        else {
            event!(Level::WARN, msg = "a peer answered a query we were not waiting for", %node, index = preamble.index);
            return Ok(());
        };
        // the peer ran the query, so it knows what kind it was; this record did not until now
        pending.stamps.adopt_served(preamble.served);
        use crate::shared::protocol::peer::ForwardedKind;
        match preamble.kind {
            // a whole answer is bytes for the client, never re-validated on this node
            ForwardedKind::Whole => {
                self.reply_sealed(
                    pending.client,
                    bundle,
                    preamble.index as usize,
                    pending.end,
                    ReplyKind::Whole,
                    pending.span,
                    pending.stamps,
                    payload,
                )
                .await
            }
            // a share is merged here, so it is validated and turned back into a response
            ForwardedKind::Share => {
                let response = <<D::ClientType as QuerySupport>::ResponseKinds as crate::shared::traits::RkyvSupport>::deserialize(
                    <<D::ClientType as QuerySupport>::ResponseKinds as crate::shared::traits::RkyvSupport>::access(&payload)?,
                )?;
                let meta = QueryMetadata::untimed(
                    pending.client,
                    bundle,
                    preamble.index as usize,
                    pending.end,
                    Some(self.info.contact.clone()),
                    pending.span,
                );
                self.handle_gathered(meta, response).await
            }
            // a failure the peer produced is answered in the query's own variant
            ForwardedKind::Error => {
                let (code, msg) = crate::shared::protocol::peer::decode_error_payload(&payload)?;
                self.fail_forward(node, bundle, preamble.index, pending, ErrorCode::from_u16(code), &msg)
                    .await
            }
        }
    }

    /// Answer everything a lost link owed: definite refusals and unknown outcomes
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose link was lost
    /// * `unsent` - The keys of frames the link never wrote
    #[allow(clippy::future_not_send)]
    async fn resolve_lost_link(&mut self, node: NodeId, unsent: &[FrameKey]) -> Result<(), ServerError> {
        let Some(peers) = self.peers.as_mut() else {
            return Ok(());
        };
        let (refused, unknown) = peers.drain_node(node, unsent);
        // a frame the link never wrote is a query nothing accepted: a definite refusal
        for ((bundle, index), pending) in refused {
            self.fail_forward(node, bundle, index, pending, ErrorCode::Unavailable, "the link to this node went down before the query was sent")
                .await?;
        }
        // a frame written but unanswered when the link dropped is an unknown outcome
        for ((bundle, index), pending) in unknown {
            self.fail_forward(node, bundle, index, pending, ErrorCode::OutcomeUnknown, "the link to this node went down after the query was sent")
                .await?;
        }
        Ok(())
    }

    /// Answer every forwarded query that has waited longer than its deadline
    #[allow(clippy::future_not_send)]
    async fn sweep_deadlines(&mut self) -> Result<(), ServerError> {
        let Some(peers) = self.peers.as_mut() else {
            return Ok(());
        };
        let deadline = peers.transport().forward_timeout.duration();
        let expired = peers.expired(Stamp::now(), deadline);
        for ((bundle, index, node), pending) in expired {
            self.fail_forward(node, bundle, index, pending, ErrorCode::OutcomeUnknown, "the peer did not answer within the deadline")
                .await?;
        }
        Ok(())
    }

    /// Drive a snapshot stream of a given size at a peer, for the bounded-bytes test
    ///
    /// The one producer of bulk traffic at M2. It enqueues a begin, chunks of 64 KiB until the
    /// requested bytes are accepted or the queue sheds, and an end. Nothing installs what it
    /// sends; the receiver counts and checksums it, which is enough to prove the bulk lane is a
    /// lane of its own that a stall bounds ([F38](../../../docs/src/features/inter-node-transport.md)).
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to stream at
    /// * `bytes` - How many payload bytes to stream
    fn probe_bulk(&mut self, node: NodeId, bytes: u64) {
        let Some(peers) = self.peers.as_mut() else {
            return;
        };
        let max = self.peer_setup.as_ref().map_or(u32::MAX, |setup| setup.local.max_frame_bytes);
        let stream = *Uuid::new_v4().as_bytes();
        // one begin, with an empty manifest
        let begin = crate::shared::protocol::peer::SnapshotBegin {
            stream,
            transition: [0u8; 16],
            boundary: 0,
            total: bytes,
            manifest_len: 0,
        }
        .encode();
        let _ = peers.enqueue(
            node,
            Lane::Bulk,
            match Frame::new(MessageType::SnapshotBegin, vec![Bytes::copy_from_slice(&begin)], FrameKey::Bulk(0), max) {
                Ok(frame) => frame,
                Err(_) => return,
            },
        );
        // chunks of 64 KiB until the queue sheds or the bytes are met
        const CHUNK: usize = 64 * 1024;
        let payload = vec![0xabu8; CHUNK];
        let mut sent = 0u64;
        let mut offset = 0u64;
        while sent < bytes {
            let len = CHUNK.min((bytes - sent) as usize);
            let chunk = crate::shared::protocol::peer::SnapshotChunk {
                stream,
                offset,
                len: len as u32,
                checksum: crate::shared::protocol::peer::checksum(&payload[..len]),
            }
            .encode();
            let frame = match Frame::new(
                MessageType::SnapshotChunk,
                vec![Bytes::copy_from_slice(&chunk), Bytes::copy_from_slice(&payload[..len])],
                FrameKey::Bulk(len),
                max,
            ) {
                Ok(frame) => frame,
                Err(_) => break,
            };
            // a shed chunk ends the stream: the queue is full and the point is made
            if peers.enqueue(node, Lane::Bulk, frame).is_err() {
                break;
            }
            sent += len as u64;
            offset += len as u64;
        }
        // and an end, best effort
        let end = crate::shared::protocol::peer::SnapshotEnd {
            stream,
            total: sent,
            checksum: 0,
            status: if sent == bytes {
                crate::shared::protocol::peer::SnapshotStatus::Complete
            } else {
                crate::shared::protocol::peer::SnapshotStatus::Aborted
            },
            resume_from: sent,
        }
        .encode();
        if let Ok(frame) = Frame::new(MessageType::SnapshotEnd, vec![Bytes::copy_from_slice(&end)], FrameKey::Bulk(0), max) {
            let _ = peers.enqueue(node, Lane::Bulk, frame);
        }
    }

    /// What this shard's peer links look like, for the transport view
    fn transport_view(&self) -> ShardTransportView {
        ShardTransportView {
            shard: self.shard_id,
            links: self.peers.as_ref().map(Peers::views).unwrap_or_default(),
            bulk_received: self.bulk_received.get(),
        }
    }

    /// Bind the peer listener and build the peer links, on a cluster node
    ///
    /// A standalone node calls this and does nothing, since it has no setup. A cluster node
    /// binds `advertise:port` beside its client listener with `SO_REUSEPORT`, builds the tls
    /// configs on its own executor the way the client listener does, and stands up the `Peers`
    /// its forwards go through.
    fn spawn_peer_listener(&mut self) -> Result<(), ServerError> {
        let Some(setup) = self.peer_setup.clone() else {
            return Ok(());
        };
        // the configs are built here, per shard, on this shard's executor
        let (client_tls, server_tls) = match &setup.tls {
            Some(tls) => {
                if !crate::shared::tls::ktls::is_available() {
                    return Err(crate::shared::tls::TlsError::UlpUnavailable(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "the 'tls' kernel module is not loaded",
                    ))
                    .into());
                }
                (
                    Some(crate::shared::tls::peer_client_config(tls)?),
                    Some(crate::shared::tls::peer_server_config(tls)?),
                )
            }
            None => (None, None),
        };
        // the placement, shared by the listener and the links on this shard
        let placement = Rc::new(setup.placement.clone());
        // the links this shard forwards through, delivering what they learn onto this shard
        self.peers = Some(Peers::new(
            placement.clone(),
            setup.local.clone(),
            client_tls,
            setup.transport.clone(),
            self.shard_local_tx.clone_sync(),
        ));
        // bind the peer listener, every shard on the same port with SO_REUSEPORT
        let listener = TcpListener::bind(setup.bind)?;
        let ctx = ListenerContext {
            comms: self.comms.clone(),
            node_local_tx: self.shard_local_tx.clone(),
            local: Rc::new(setup.local.clone()),
            placement,
            tls: server_tls,
            handshake_timeout: setup.transport.handshake_timeout.duration(),
            inflight_bound: setup.transport.inflight_bytes,
            shard_count: self.ring.shards.iter().filter(|info| info.contact.local_index().is_some()).count(),
            bulk_received: self.bulk_received.clone(),
        };
        let handle = glommio::spawn_local_into(peer::peer_acceptor(listener, ctx), self.high_priority)?;
        self.tasks.push(handle);
        // and a timer that sweeps forwarded queries whose deadline has passed
        let tick_tx = self.shard_local_tx.clone_sync();
        let interval = (setup.transport.forward_timeout.duration() / 10).max(Duration::from_millis(50));
        let sweeper = glommio::spawn_local_into(
            async move {
                loop {
                    glommio::timer::sleep(interval).await;
                    if tick_tx.try_send(ServerMsg::Peer(PeerEvent::Tick)).is_err() {
                        break;
                    }
                }
                Ok(())
            },
            self._medium_priority,
        )?;
        self.tasks.push(sweeper);
        Ok(())
    }

    /// Find partitions to evict
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn evict_data(&mut self) -> Result<(), ServerError> {
        // track how much data we are trying to evict
        // we will always try to evict at least 40% of our cache when we hit memory pressure
        let mut need = (*self.memory_usage.borrow() as f64 * 0.40).ceil() as usize;
        // build a map of tables and the partitions we can remove from them
        let mut evictable = HashMap::with_capacity(10);
        // keep popping from our lru cache until we have meet our eviction needs
        loop {
            // try to pop something from our lru
            match self.lru.borrow_mut().pop_lru() {
                Some(((table_name, key), size)) => {
                    // get an entry to this tables evictable partitions
                    let entry = evictable
                        .entry(table_name)
                        .or_insert_with(|| Vec::with_capacity(1000));
                    // add this partition we are going to evict
                    entry.push(key);
                    // decrement the amount of data we need to evict still
                    need = need.saturating_sub(size);
                    // if we have found enough partitions to evict then stop looking
                    // and start evicting
                    if need == 0 {
                        break;
                    }
                }
                // we have no more rows we could evict even if we wanted too
                None => break,
            }
        }
        // step over each table with evictions and evict its data
        for (table_name, victims) in evictable {
            // evict this tables data
            self.tables.evict(table_name, victims);
        }
        Ok(())
    }

    /// Start handling queries from users
    ///
    /// # Arguments
    ///
    /// * `mesh_rx` - The glommio channel to receive node local messages on
    ///
    /// # Errors
    ///
    /// This wil return an error if a message cannot be sent to a coordinator or if a query fails
    #[allow(clippy::future_not_send)]
    pub async fn start<'a>(
        mut self,
        should_shutdown: Arc<AtomicBool>,
        events: &std::sync::mpsc::Sender<ShardEvent>,
    ) -> Result<(), ServerError>
    where
        for<'b> <<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // initalize this shard
        self.init(should_shutdown).await?;
        // tell the pool we are answering, and on which address
        //
        // `init` bound the listener, so `bound` is set by now; a shard with no address after
        // that is a bug in `spawn_client_listener` rather than a state to report
        let addr = self
            .bound
            .ok_or_else(|| ServerError::GlommioGeneric("shard ready without a listener".into()))?;
        // the pool may have stopped listening for events, which is not this shard's problem
        let _ = events.send(ShardEvent::Ready {
            shard: self.shard_id,
            addr,
        });
        // keep handling messages until we get a shutdown command
        loop {
            // wait for a message on our mesh
            let msg = self.shard_local_rx.recv().await?;
            // handle this message
            match msg {
                // Join our ring
                ServerMsg::Join(info) => self.ring.add(info),
                // Add this new client to our client map
                ServerMsg::NewClient { client, client_tx } => {
                    // add this client to our client map
                    if self.client_map.insert(client, client_tx).is_some() {
                        // panic if we had a client id collision
                        panic!("Client ID collision?");
                    }
                }
                // a client has gone away, so drop the channel every shard was holding for it
                //
                // without this every shard kept every connection's channel until the process
                // ended ([Resolved #32](../../../docs/src/appendix/resolved/disconnected-client-cleanup.md))
                ServerMsg::ClientGone(client) => {
                    self.client_map.remove(&client);
                }
                // Handle this client query
                ServerMsg::Client {
                    peer,
                    span,
                    data,
                    base,
                } => self.handle_client(peer, span, data, base).await?,
                // handle this query from the user, reading it out of the bundle it arrived in
                ServerMsg::Query {
                    meta,
                    body,
                    offset,
                    keys,
                } => self.handle_query(meta, &body, offset, keys).await?,
                // run this query again now that the partition it waited on has been read
                ServerMsg::Released { meta, query } => {
                    self.handle_released(meta, query).await?
                }
                // collect this shards share of a query we split across shards
                ServerMsg::Gathered { meta, response } => {
                    self.handle_gathered(meta, response).await?
                }
                // load this partition from disk
                ServerMsg::Partition(loaded) => {
                    self.tables
                        .load_partition(loaded, &self.shard_local_tx)
                        .await?
                }
                // this partition could not be read, so release the queries waiting on it
                ServerMsg::PartitionLoadFailed {
                    span,
                    table,
                    partition_id,
                    error,
                } => {
                    self.tables
                        .fail_partition(
                            table,
                            partition_id,
                            // the read that gave up, which every query it releases is linked to
                            &span,
                            error,
                            &self.shard_local_tx,
                        )
                        .await?
                }
                // Inform a table that some of its data has been flushed to storage
                // this carries no position, it only tells us a durable watermark may
                // have moved, so the work happens in handle_flushed below
                ServerMsg::DataFlushed => self.data_flushed = true,
                // Mark some partitions as evictable
                ServerMsg::MarkEvictable {
                    generation,
                    table,
                    partitions,
                } => self.tables.mark_evictable(table, generation, partitions),
                // a bundle forwarded by another node, which this shard accepted and now routes
                ServerMsg::Forward {
                    conn,
                    origin,
                    preamble,
                    entries,
                    data,
                    base,
                } => self.handle_forward(conn, origin, preamble, entries, data, base).await?,
                // something a peer link this shard owns learned
                ServerMsg::Peer(event) => self.handle_peer_event(event).await?,
                // drive a snapshot stream at a peer, for the bounded-bytes test
                ServerMsg::BulkProbe { node, bytes } => self.probe_bulk(node, bytes),
                // report what this shard's peer links look like
                ServerMsg::Transport(reply) => {
                    let _ = reply.send(self.transport_view());
                }
                // shutdown this shard
                ServerMsg::Shutdown => {
                    // signal all of our loaders to shutdown
                    for (_, (loader_tx, _)) in &self.loader_channels {
                        // signal this loader to shutdown
                        loader_tx.send(LoaderMsg::Shutdown).await?;
                    }
                    // hand this threads buffered stage records over before it goes away
                    //
                    // records are handed over in batches as the run proceeds, so this only
                    // covers the tail sitting below that batch size. Without it the last
                    // few thousand queries of a run would be missing from the profile.
                    stage_profile::flush();
                    break;
                }
            }
            // if we have no more messages then flush our current queries to disk
            if self.shard_local_rx.is_empty() {
                self.tables.flush().await?;
            }
            // sweep our tables only when that sweep could do something
            //
            // a response can only be released once a write has landed, and every landed
            // write sends us a DataFlushed; a rotation is only due once a log has grown
            // past its size, which the tables can answer without touching storage. If
            // neither holds then the sweep would walk every table to learn nothing
            if self.data_flushed || self.tables.compaction_due() {
                // check for any flushed response to handle
                self.handle_flushed().await?;
            }
            // check if we need to evict any data
            if *self.memory_usage.borrow() > self.conf.resources.memory {
                // try to evict our least recently used data
                self.evict_data().await?;
            }
        }
        // check for any flushed response to handle
        //
        // unconditional on purpose, unlike the call in the loop: shutdown has to drain
        // whatever is still pending whether or not a wakeup happened to arrive for it
        self.handle_flushed().await?;
        // shudown our tables
        self.tables.shutdown().await?;
        // shutdown all of our tasks
        shutdown_tasks(self.tasks).await?;
        Ok(())
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn start<S: ShoalDatabase>(
    conf: Conf,
    cpus: CpuSet,
    peer_setup: Option<PeerSetup>,
) -> Result<
    (
        PoolThreadHandles<Result<(), ServerError>>,
        Arc<AtomicBool>,
        std::sync::mpsc::Receiver<ShardEvent>,
        kanal::Sender<ServerMsg<S>>,
    ),
    ServerError,
>
where
    for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    [<<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived]:
        DeserializeUnsized<
            [<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    // we will have one shard per core, which every shard builds its tablet map from
    let shard_count = cpus.len();
    // build our comms object for this nodes shards
    let comms = Comms::<S>::with_capacity(shard_count);
    // An atomic bool used to signal that shards should exit
    let should_shutdown = Arc::new(AtomicBool::new(false));
    // A counter to assign shard IDs starting from zero, independent of executor IDs
    let shard_counter = Arc::new(AtomicUsize::new(0));
    // the channel every shard reports its readiness or death on
    let (events, event_rx) = std::sync::mpsc::channel();
    // setup our executor
    let executor_builder =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_count, Some(cpus)));
    // build and spawn our shards on all of remaining available cores
    let shards = executor_builder.on_all_shards(
        enclose!((comms, should_shutdown, shard_counter, events, peer_setup) move || {
            async move {
                // mint this shards id here rather than in `Shard::new`, so that a failure in
                // there can still be reported under the id it would have had
                let shard_id = shard_counter.fetch_add(1, Ordering::Relaxed);
                // build and run this shard, keeping the outcome so it can be reported first
                let outcome = async {
                    // build an empty shard
                    let shard: Shard<S> =
                        Shard::new(&conf, comms, shard_id, shard_count, peer_setup).await?;
                    // start this shard
                    shard.start(should_shutdown.clone(), &events).await
                }
                .await;
                // whoever started the pool is told about a death, whether it happened before
                // the shard was ready or after (item 58)
                if let Err(error) = &outcome {
                    // the pool may already be gone, which is not this shard's problem
                    let _ = events.send(ShardEvent::Failed {
                        shard: shard_id,
                        error: format!("{error:?}"),
                    });
                }
                outcome
            }
        }),
    )?;
    // a sync sender to shard 0's mesh channel, so the pool (which is on no executor) can ask it
    // for the transport view or drive a bulk probe
    let control_tx = comms.get_shards_channels(0).0.clone_sync();
    Ok((shards, should_shutdown, event_rx, control_tx))
}
