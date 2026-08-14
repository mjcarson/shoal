//! A single shard in Shoal

use bytes::BytesMut;
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
use std::rc::Rc;
use std::time::Duration;
use std::{cell::RefCell, hash::BuildHasherDefault};
use std::{collections::HashMap, io::IoSlice};
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use super::messages::{QueryMetadata, ServerMsg};
use super::ring::Ring;
use super::stage_profile::{self, StageStamps, Stamp};
use super::{Comms, Conf, ServerError};
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
            handshake, ProtocolError,
        },
        queries::Queries,
        traits::{
            QuerySupport, RkyvSupport, ShoalDatabase, ShoalQuerySupport, ShoalResponseSupport,
        },
    },
    storage::{FullArchiveMap, LoaderMsg, Loaders},
};

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
        // allocate a buffer that is exactly the right size
        let mut data = BytesMut::zeroed(header.body_len());
        // wait for messages from our client
        if let Err(error) = tcp_rx.read_exact(&mut data).await {
            event!(Level::ERROR, msg = "failed to read a frame body", %peer, ?error);
            break;
        }
        // start this bundles clock now that all of its bytes are here
        //
        // every stage offset a query in this bundle records is measured from here, since
        // this is the first moment we know the bundle exists
        let base = Stamp::now();
        // forward our clients message
        if let Err(error) = kanal_tx.send(ServerMsg::Client { peer, data, base }).await {
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
    client_rx: AsyncReceiver<(Uuid, Span, StageStamps, AlignedVec)>,
    mut tcp_tx: WriteHalf<TcpStream>,
    peer_max_frame_bytes: u32,
) {
    // loop over messages to send back to our client
    loop {
        // try to get a message from our channel
        let (query_id, span, mut stamps, archived) = match client_rx.recv().await {
            Ok(msg) => msg,
            // if this channel was closed then stop our task
            // this should only happen exit/shutdown or when our client shutsdown
            Err(_) => break,
        };
        // enter our span
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
#[allow(clippy::future_not_send)]
async fn client_acceptor<S: ShoalDatabase>(
    tcp_sock: TcpListener,
    comms: Comms<S>,
    node_local_tx: AsyncSender<ServerMsg<S>>,
    max_frame_bytes: u32,
    store: Rc<CredentialStore>,
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
            // the deadline covers the authentication exchange as well as the handshake, since a
            // peer that stalls between its `Hello` and its proof is holding exactly as much of
            // this server as one that stalls before either
            let handshake = glommio::timer::timeout(HANDSHAKE_TIMEOUT, async {
                // shake hands first, which is what decides whether there is anything to prove
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
#[derive(Clone, Debug)]
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
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

    /// Get this shards mesh id
    pub fn mesh_id(&self) -> usize {
        match self.contact {
            ShardContact::Local(mesh_id) => mesh_id,
        }
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
    client_map: HashMap<Uuid, AsyncSender<(Uuid, Span, StageStamps, AlignedVec)>>,
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
    /// * `shard_counter` - The counter assigning shard ids on this node
    /// * `shard_count` - The number of shards on this node
    #[instrument(name = "Shard::new", skip_all, err(Debug))]
    pub async fn new(
        conf: &Conf,
        comms: Comms<D>,
        shard_counter: &AtomicUsize,
        shard_count: usize,
    ) -> Result<Self, ServerError> {
        // get a handle to our current executor
        let executor = glommio::executor();
        // assign a shard ID from our counter (always starts at 0 per pool)
        let shard_id = shard_counter.fetch_add(1, Ordering::Relaxed);
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
        // get our own mesh id
        let our_mesh_id = info.mesh_id();
        // get the channels for this shards channel on this node
        let (shard_local_tx, shard_local_rx) = comms.get_shards_channels(our_mesh_id);
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
        // build our shard
        let shard = Shard {
            info,
            conf: conf.clone(),
            // built from the shard count rather than from joins, so it is already
            // complete and this shard can never route against a partial map
            ring: Ring::new(shard_count)?,
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
        };
        Ok(shard)
    }

    /// Spawn our client network listener
    fn spawn_client_listener(&mut self) -> Result<(), ServerError> {
        // bind our udp socket
        let tcp_sock = TcpListener::bind(self.conf.networking.to_addr())?;
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
    /// whole pool: `ShoalPool::start` spawns its shard threads and returns without
    /// joining them, so no moment exists at which every shard has finished starting.
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
    #[instrument(name = "Coordinator::send_to_shard", skip_all)]
    async fn send_to_shard(
        &mut self,
        client: Uuid,
        queries: Queries<D::ClientType>,
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
        // initialize a vec to store the per shard queries we find
        let mut found = Vec::with_capacity(3);
        // get the absolute index for the last query in this bundle
        //
        // every index below is absolute, so this has to carry the base index too or a
        // streamed bundle would compare an absolute index against a relative one
        let end_index = queries.base_index + last_offset;
        // crawl over our queries
        for (index, kind) in queries.queries.into_iter().enumerate() {
            // get this queries absolute index in its stream
            //
            // this is per query and not per shard, so that every shard answering one
            // query answers it under the same index
            let index = index + queries.base_index;
            // check if this is the last query or not
            let end = index == end_index;
            // give this query its own copy of the bundles stamps to carry from here on
            let mut stamps = stamps;
            // note where in its batch this query sat, since a query near the tail of a
            // bundle waits on every query ahead of it and that is not a server side cost
            stamps.set_batch(index - queries.base_index, batch_len);
            // remember the index this query answers under, which is half of a records key
            stamps.set_index(index);
            // split this query into the per shard queries that answer it
            kind.split_by_shard(&self.ring, &mut found);
            // record that this query is leaving us for the shards that own its partitions
            stamps.mark_routed();
            // a query answered by one shard alone is replied to directly, so only a
            // query we actually split needs its shares collected back here
            let gather = if found.len() > 1 {
                // remember what we are owed before we send anything, so a share that
                // comes straight back to us still finds somewhere to land
                let gather = Gather {
                    client,
                    span: Span::current(),
                    stamps,
                    outstanding: found.len(),
                    limit: kind.limit(),
                    // remember the order this query named its partitions in, since the
                    // narrowed queries only carry each shards own share of them
                    partition_order: kind.partition_keys().to_vec(),
                    merged: None,
                };
                self.gathering.insert((queries.id, index), gather);
                // tell every shard we split this to answer back to us
                Some(self.info.contact.clone())
            } else {
                None
            };
            // note whether the copy each shard carries is a share of a query we split
            //
            // a split query produces one of these per shard plus the one client visible
            // record the gather emits, so a report that counted them all would multiply
            // count it. The copy we kept in the gather above is deliberately not flagged.
            let mut share_stamps = stamps;
            share_stamps.set_share_of_gathered(gather.is_some());
            // send each narrowed query to the shard that owns its partitions
            for (shard_info, query) in found.drain(..) {
                // build the metadata for this query
                let meta = QueryMetadata::new(
                    client,
                    queries.id,
                    index,
                    end,
                    gather.clone(),
                    share_stamps,
                );
                // build the mssage to send
                let msg = ServerMsg::Query { meta, query };
                // send this to correct shard
                self.comms.send(&shard_info.contact, msg).await?;
            }
        }
        Ok(())
    }

    /// Handle a client messages
    ///
    /// # Arguments
    ///
    /// * `addr` - The address
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Coordinator::handle_client",
        skip(self, peer, data),
        err(Debug)
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_client<'a>(
        &mut self,
        peer: Uuid,
        data: BytesMut,
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
        // load our arhived query from buffer
        let archived = Queries::access(&data)?;
        // deserialize our queries
        let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
        // record that this bundle is now a set of queries rather than a buffer
        //
        // this stage is paid once per bundle and charged to every query in it, so the
        // report has to label it as a batch level cost rather than a per query one
        stamps.mark_decoded();
        // send each query to the correct shard
        self.send_to_shard(peer, queries, stamps).await
    }

    /// Send a respones back to the client
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to send this reply too
    /// * `response` - The response to send
    /// * `stamps` - When this query reached each stage so far, and its index
    #[instrument(name = "Shard::reply", parent = &span, skip_all, err(Debug))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn reply(
        &mut self,
        client: Uuid,
        query_id: Uuid,
        span: Span,
        mut stamps: StageStamps,
        response: <D::ClientType as QuerySupport>::ResponseKinds,
    ) -> Result<(), ServerError> {
        // archive our response
        let archived = rkyv::to_bytes::<_>(&response)?;
        // record what serializing this response cost
        //
        // this is a whole row through rkyv rather than a queue hop, so it is one of the few
        // stages on the get path large enough to be worth measuring on its own
        stamps.mark_replied();
        // get this clients channel to send replies over
        match self.client_map.get(&client) {
            Some(client_tx) => {
                // note that this response is now the relays problem rather than ours
                stamps.mark_queued_to_client();
                client_tx.send((query_id, span, stamps, archived)).await?;
            }
            None => panic!("{} Missing client channel? {client}", self.info.name),
        }
        Ok(())
    }

    /// Handle a query on this shard
    ///
    /// # Arguments
    ///
    /// `meta` - The metadata about the query to handle
    /// `query` - The query to handle
    #[allow(clippy::future_not_send)]
    #[instrument(
        name = "Shard::handle_query",
        parent = &meta.span,
        skip(self, query),
        fields(index = meta.index, id = meta.id.to_string())
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_query(
        &mut self,
        mut meta: QueryMetadata,
        query: <D::ClientType as QuerySupport>::QueryKinds,
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
        // try to handle this query
        if let Some((addr, query_id, mut stamps, response)) = self.tables.handle(meta, query).await
        {
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
                    // build the message carrying our share of this queries answer
                    let msg = ServerMsg::Gathered {
                        meta: gathered_meta,
                        response,
                    };
                    // send our share to the shard collecting them
                    self.comms.send(&contact, msg).await?;
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
    pub async fn start<'a>(mut self, should_shutdown: Arc<AtomicBool>) -> Result<(), ServerError>
    where
        for<'b> <<<D as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // initalize this shard
        self.init(should_shutdown).await?;
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
                // Handle this client query
                ServerMsg::Client { peer, data, base } => {
                    self.handle_client(peer, data, base).await?
                }
                // handle this query from the user
                ServerMsg::Query { meta, query } => self.handle_query(meta, query).await?,
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
                    table,
                    partition_id,
                    error,
                } => {
                    self.tables
                        .fail_partition(table, partition_id, error, &self.shard_local_tx)
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
) -> Result<(PoolThreadHandles<Result<(), ServerError>>, Arc<AtomicBool>), ServerError>
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
    // setup our executor
    let executor_builder =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_count, Some(cpus)));
    // build and spawn our shards on all of remaining available cores
    let shards = executor_builder.on_all_shards(
        enclose!((comms, should_shutdown, shard_counter) move || {
            async move {
                // build an empty shard
                let shard: Shard<S> = Shard::new(&conf, comms, &shard_counter, shard_count).await?;
                // start this shard
                shard.start(should_shutdown.clone()).await
            }
        }),
    )?;
    Ok((shards, should_shutdown))
}
