//! The client for a Shoal database

use bb8::ManageConnection;
use kanal::{AsyncReceiver, AsyncSender};
use papaya::HashMap;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::option::ArchivedOption;
use rkyv::rancor::Strategy;
use rkyv::util::AlignedVec;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::vec::ArchivedVec;
use rkyv::Archive;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{ErrorKind, IoSlice};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tracing::{event, Level};
use uuid::Uuid;

pub mod messages;
pub mod tls;

// the error types are protocol, not transport - `QuerySupport` and `shared::responses` both name
// them, so they cannot live above the crate that defines those
pub use shoal_proto::client as errors;

use super::shared::queries::Queries;
use crate::shared::auth::scram::{ClientStep, ScramClient};
use crate::shared::auth::{AuthError, Credentials};
use crate::shared::protocol::auth::{self as proto_auth, AuthMechanism, AuthStatus};
use crate::shared::protocol::error::{self, ErrorCode};
use crate::shared::protocol::{self, handshake, MessageType, ProtocolError};
use crate::shared::responses::{ArchivedResponseError, ResponseActionNames};
use crate::shared::tls::{self as shared_tls, TlsClientOptions};
use crate::shared::traits::{
    ExistsQuery, QuerySupport, RkyvSupport, ShoalQuerySupport, ShoalResponseSupport,
};
pub use shoal_proto::client::{
    ChannelError, ConnectError, Errors, FromShoal, QuerySuceededOpts, ShqlParseError,
};
use messages::{BatchStamps, ClientMsg, ClientStamps};

/// Say that a send found nobody left to receive it
///
/// The channel crate's own error type is mapped here rather than through a `From` impl, because
/// [`Errors`] is shared with every peer and must not name the channels this particular client
/// happens to be built on.
///
/// # Arguments
///
/// * `error` - The send failure to describe
fn send_failed(error: kanal::SendError) -> Errors {
    // say which end went away without naming the crate that told us
    Errors::Channel(match error {
        kanal::SendError::Closed => ChannelError::Closed,
        kanal::SendError::ReceiveClosed => ChannelError::ReceiveClosed,
    })
}

/// Say that a receive found nothing left to wait for
///
/// # Arguments
///
/// * `error` - The receive failure to describe
fn receive_failed(error: kanal::ReceiveError) -> Errors {
    // say which end went away without naming the crate that told us
    Errors::Channel(match error {
        kanal::ReceiveError::Closed => ChannelError::Closed,
        kanal::ReceiveError::SendClosed => ChannelError::SendClosed,
    })
}

/// The channel a query's responses are routed through, and where they are owed from
///
/// The connection is here rather than in a map of its own so that there is exactly one place a
/// query's routing state lives. A second map would have to be inserted into and removed from in
/// step with this one, and the failure mode of getting that wrong is a leak that nothing notices.
#[derive(Clone)]
struct Waiter {
    /// The connection this query was written to, if it has been written yet
    ///
    /// A query that has been registered but not yet written has no connection to lose, which is
    /// what makes `None` a meaningful state rather than a placeholder.
    conn: Option<u64>,
    /// The channel to hand this query's responses to
    tx: AsyncSender<ClientMsg>,
}

/// The write half of a pooled connection, and which connection it is
///
/// The identity is what lets a read loop that has died fail the queries that were written to
/// *its* socket, and only those. The pool holds up to fifty connections and one map of every
/// query in flight across all of them, so a sweep without an identity to filter on would fail
/// forty nine other connections worth of healthy queries.
struct ShoalConnection {
    /// The write half of this connection
    writer: OwnedWriteHalf,
    /// Which connection this is
    id: u64,
}

impl std::ops::Deref for ShoalConnection {
    type Target = OwnedWriteHalf;

    /// Get the write half of this connection
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl std::ops::DerefMut for ShoalConnection {
    /// Get the write half of this connection mutably
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

// Connection manager for bb8
#[derive(Clone)]
struct ShoalConnectionManager {
    /// The shoal server to connect too
    server_addr: SocketAddr,
    /// The channel to send our read halves to our proxy over, with the connection they came from
    proxy_tx: AsyncSender<(u64, OwnedReadHalf)>,
    /// The id to give the next connection this manager opens
    ///
    /// Shared with every clone of this manager, since `bb8` clones it and the ids have to be
    /// unique across the whole pool rather than within one clone of it.
    next_conn_id: Arc<AtomicU64>,
    /// The connections whose read half has stopped, which the pool has not discarded yet
    ///
    /// A connection is only half broken when its reader dies: the write half still accepts bytes
    /// into the kernel's buffer, and `peer_addr` still answers, so neither of the pool's health
    /// checks notices. Without this the pool hands out a socket whose answers nobody is listening
    /// for, and the query written to it waits forever.
    ///
    /// This is self draining. An entry is removed by whichever health check reads it, and reading
    /// it is what makes the pool throw that connection away.
    dead_conns: Arc<HashMap<u64, ()>>,
    /// The largest frame the server on the other end of these connections will accept
    ///
    /// This is shared with the client that owns this manager rather than copied into it, since it
    /// is learned from the server when a connection opens and every send has to see it.
    peer_max_frame_bytes: Arc<AtomicU32>,
    /// The fingerprint of the schema this client was built from
    ///
    /// This is carried as a value rather than reached through a generic, because the manager is
    /// not generic over the database and does not need to be for this one number.
    schema_fingerprint: u64,
    /// What this client proves itself with, if the server asks it to
    ///
    /// These live on the manager rather than on the client because the manager is where a
    /// connection is *made*, and `bb8` already treats that as the place a connection becomes
    /// usable. A connection the pool replaces after a failure re-authenticates with no code
    /// anywhere else.
    credentials: Arc<Credentials>,
    /// What this client encrypts with, if it encrypts
    ///
    /// The certificate authority is read once here rather than once per connection, so a pool
    /// opening ten connections at startup parses one PEM file rather than ten. It lives beside the
    /// credentials for the reason they do: a connection the pool replaces re-encrypts for free.
    tls: Option<(Arc<rustls::ClientConfig>, TlsClientOptions)>,
}

/// How long a server has to finish its half of the handshake
///
/// `bb8`'s connection timeout bounds its retry loop and `pool.get()`, not `connect` itself, so
/// without this a server that accepts a connection and then stalls would park `Shoal::new`
/// forever. Before the handshake existed `connect` could not block at all, since it neither read
/// nor wrote — this deadline is created by the handshake and belongs to it.
///
/// It covers **all three** handshakes a connection can have: the TLS one, the Shoal one, and the
/// authentication exchange. A peer that stalls between any two of them holds exactly as much of
/// this client as one that stalls before all three.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Everything a client can be told about a connection beyond where to make it
///
/// This exists because `addr × credentials × tls` is three constructors' worth of combinations and
/// [F12](../../../docs/src/features/authentication.md) predicted that a third one would be the
/// signal that [D6](../../../docs/src/direction/connection-pool.md)'s builder is overdue. It is
/// the seam that builder should absorb rather than sit beside — deadlines, pool sizing and health
/// checks all belong on the same object and none of them are here.
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    /// What this client proves itself with, if the server asks it to
    pub credentials: Credentials,
    /// What this client encrypts with, if the server it is calling is encrypted
    pub tls: Option<TlsClientOptions>,
}

impl ClientOptions {
    /// Build options that prove nothing and encrypt nothing
    ///
    /// This is what every client had before either feature existed, and it is what a server with
    /// no `auth` and no `networking.tls` section expects.
    pub fn new() -> Self {
        ClientOptions::default()
    }

    /// Prove this client's identity with a username and password
    ///
    /// # Arguments
    ///
    /// * `credentials` - What to prove this client's identity with
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// Encrypt this client's connections
    ///
    /// # Arguments
    ///
    /// * `tls` - Which authority to trust, and what name to ask the server for
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::client::ClientOptions;
    /// use shoal_core::shared::auth::Credentials;
    /// use shoal_core::shared::tls::TlsClientOptions;
    ///
    /// let options = ClientOptions::new()
    ///     .credentials(Credentials::scram("reader", "hunter2"))
    ///     .tls(TlsClientOptions::new("/etc/shoal/ca.pem"));
    /// ```
    pub fn tls(mut self, tls: TlsClientOptions) -> Self {
        self.tls = Some(tls);
        self
    }
}

impl ShoalConnectionManager {
    /// Create a new shoal connection manager
    ///
    /// # Arguments
    ///
    /// * `server_addr` - The address of the server to connect too
    /// * `proxy_tx` - The channel to hand read halves to the proxy over
    /// * `dead_conns` - Where read loops record that their connection has stopped
    /// * `peer_max_frame_bytes` - Where to record the largest frame the server will accept
    /// * `schema_fingerprint` - The fingerprint of the schema this client was built from
    /// * `options` - What this client proves itself with and encrypts with
    pub fn new(
        server_addr: SocketAddr,
        proxy_tx: AsyncSender<(u64, OwnedReadHalf)>,
        dead_conns: &Arc<HashMap<u64, ()>>,
        peer_max_frame_bytes: &Arc<AtomicU32>,
        schema_fingerprint: u64,
        options: ClientOptions,
    ) -> Result<Self, Errors> {
        // read the certificate authority once here rather than once per connection
        let tls = match options.tls {
            Some(tls) => Some((
                shared_tls::client_config(&tls).map_err(ConnectError::Tls)?,
                tls,
            )),
            None => None,
        };
        Ok(ShoalConnectionManager {
            server_addr,
            proxy_tx,
            // start at one so that zero is never a connection, and a default can never name one
            next_conn_id: Arc::new(AtomicU64::new(1)),
            dead_conns: dead_conns.clone(),
            peer_max_frame_bytes: peer_max_frame_bytes.clone(),
            schema_fingerprint,
            credentials: Arc::new(options.credentials),
            tls,
        })
    }

    /// Shake hands with the server over a connection that has not been split yet
    ///
    /// # Invariants
    ///
    /// **The client speaks first.** This writes before it reads, and the server reads before it
    /// writes. If both peers waited to read, every connection would deadlock and nothing about
    /// the frame layout would show it.
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection to shake hands over
    async fn handshake(&self, stream: &mut TcpStream) -> Result<handshake::HelloAck, ConnectError> {
        // say who we are, how large a frame we are willing to be sent, and what we can prove
        let hello = handshake::Hello {
            schema_fingerprint: self.schema_fingerprint,
            max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
            mechanisms: self.credentials.mechanisms(),
        };
        stream
            .write_all(&hello.frame(protocol::DEFAULT_MAX_FRAME_BYTES)?)
            .await?;
        stream.flush().await?;
        // read the header of whatever the server answered with
        let mut header_bytes = [0u8; protocol::HEADER_LEN];
        stream.read_exact(&mut header_bytes).await?;
        let raw = protocol::RawHeader::decode(&header_bytes);
        // a server speaking a version we do not is refused here, from the header alone
        //
        // this works because the eight header bytes mean the same thing in every version of the
        // protocol, so a version we cannot speak is still a header we can read
        if raw.version != protocol::PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                got: raw.version,
                ours: protocol::PROTOCOL_VERSION,
            }
            .into());
        }
        // an answer that is not an ack means this server is not the one we thought we called
        let kind = protocol::MessageType::from_byte(raw.kind)?;
        if kind != protocol::MessageType::HelloAck {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: protocol::MessageType::HelloAck,
                got: kind,
            }
            .into());
        }
        // an ack is a fixed sixteen bytes, so one of any other size is not an ack
        if raw.len as usize != handshake::HANDSHAKE_BODY_LEN {
            return Err(ProtocolError::BodyTooShort {
                need: handshake::HANDSHAKE_BODY_LEN,
                got: raw.len,
            }
            .into());
        }
        let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
        stream.read_exact(&mut body).await?;
        let ack = handshake::HelloAck::decode(&body);
        // a refusal names both fingerprints, so that whichever peer's log is read says the same
        // thing the other one's does
        if !ack.reason.is_accepted() {
            return Err(match ack.reason {
                handshake::RefusalReason::SchemaMismatch => ProtocolError::SchemaMismatch {
                    ours: self.schema_fingerprint,
                    theirs: ack.schema_fingerprint,
                },
                reason => ProtocolError::Refused { reason },
            }
            .into());
        }
        // a server that accepted us but was built from a different schema is a server that is
        // not checking, so refuse it from this side too
        if ack.schema_fingerprint != self.schema_fingerprint {
            return Err(ProtocolError::SchemaMismatch {
                ours: self.schema_fingerprint,
                theirs: ack.schema_fingerprint,
            }
            .into());
        }
        // prove who we are, if this server asked us to
        //
        // the server picked one mechanism out of what we offered, so a `None` here is a server
        // that requires nothing and a name we could not read is one we cannot satisfy
        if let Some(mechanism) = ack.mechanism {
            self.authenticate(stream, mechanism, ack.max_frame_bytes)
                .await?;
        }
        Ok(ack)
    }

    /// Prove who this client is with the mechanism the server selected
    ///
    /// # Invariants
    ///
    /// **This runs before the stream is split**, for the reason the handshake does: everything it
    /// reads would otherwise be handed to the proxy, which would decode a challenge as a response
    /// to a query nobody sent.
    ///
    /// **The server is checked too.** SCRAM is mutual, and the final message is verified rather
    /// than assumed — a client that skipped it would prove itself to anything that answered.
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection to authenticate over, before it has been split
    /// * `mechanism` - The mechanism the server selected
    /// * `max_frame_bytes` - The largest frame this server said it will accept
    async fn authenticate(
        &self,
        stream: &mut TcpStream,
        mechanism: AuthMechanism,
        max_frame_bytes: u32,
    ) -> Result<(), ConnectError> {
        // work out whether we hold anything that can do what this server asked for
        let (username, password) = match (mechanism, self.credentials.as_ref()) {
            (AuthMechanism::ScramSha256, Credentials::Scram { username, password }) => {
                (username, password)
            }
            // a server that asked for a mechanism we cannot do, or asked at all when we hold
            // nothing. the second is reachable even though a server picks from what we offered,
            // since a `HelloAck` naming a mechanism byte this build cannot read decodes to `None`
            (mechanism, Credentials::None) => {
                return Err(AuthError::UnsupportedMechanism(mechanism).into())
            }
            (mechanism, _) => return Err(AuthError::UnsupportedMechanism(mechanism).into()),
        };
        // run the exchange, answering each challenge until the server accepts or refuses us
        let mut scram = ScramClient::new(username, password);
        let mut payload = scram.first()?;
        loop {
            // write whatever the mechanism produced, then wait for the server's half
            let frame = proto_auth::encode_auth(mechanism, &payload, max_frame_bytes)?;
            stream.write_all(&frame).await?;
            stream.flush().await?;
            let (status, answer) = self.read_auth_response(stream).await?;
            match status {
                // another round, so let the mechanism answer it
                AuthStatus::Challenge => {
                    let ClientStep::Send(next) = scram.step(&answer)?;
                    payload = next;
                }
                // we are in, once the server has proved it holds this credential too
                AuthStatus::Success => {
                    scram.finish(&answer)?;
                    return Ok(());
                }
                // the server refused us, and its prose is the same sentence for every cause
                AuthStatus::Failed => {
                    return Err(ConnectError::AuthFailed {
                        msg: String::from_utf8_lossy(&answer).into_owned(),
                    })
                }
            }
        }
    }

    /// Read one `AuthResponse` frame from the server
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection to read from, before it has been split
    async fn read_auth_response(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(AuthStatus, Vec<u8>), ConnectError> {
        // read the header first, so that a frame's size is known before anything allocates for it
        let mut header_bytes = [0u8; protocol::HEADER_LEN];
        stream.read_exact(&mut header_bytes).await?;
        let header = protocol::Header::decode(&header_bytes, proto_auth::MAX_AUTH_FRAME_BODY)?
            .expect(MessageType::AuthResponse)?;
        // the auth payload bound is far tighter than the frame bound, and it is what applies here
        let _ = proto_auth::payload_len(header)?;
        // now that the length has been judged, read the body it named
        let mut body = vec![0u8; header.body_len()];
        stream.read_exact(&mut body).await?;
        let (status, payload) = proto_auth::decode_auth_response_body(&body)?;
        Ok((status, payload.to_vec()))
    }
}

#[async_trait::async_trait]
impl ManageConnection for ShoalConnectionManager {
    type Connection = ShoalConnection;
    type Error = ConnectError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut stream = TcpStream::connect(&self.server_addr).await?;
        // Disable Nagle's algorithm
        stream.set_nodelay(true)?;
        // take the wire before anything speaks the shoal protocol over it
        //
        // this is inside the deadline below along with the other two handshakes. the established
        // session is bound rather than dropped so rustls' record of it outlives the socket, which
        // is where a key update would be handled
        let _tls = match &self.tls {
            Some((config, options)) => Some(
                tokio::time::timeout(
                    HANDSHAKE_TIMEOUT,
                    tls::connect(&mut stream, config, options, &self.server_addr),
                )
                .await
                .map_err(|_| ConnectError::HandshakeTimeout)??,
            ),
            None => None,
        };
        // shake hands before this stream is split
        //
        // this has to happen before the read half is handed to the proxy below, or the proxy
        // consumes the ack and decodes it as a response to a query nobody sent. moving the
        // handshake after the split is the natural looking refactor that would break that
        let ack = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.handshake(&mut stream))
            .await
            .map_err(|_| ConnectError::HandshakeTimeout)??;
        // remember how large a frame this server is willing to be sent
        self.peer_max_frame_bytes
            .store(ack.max_frame_bytes, Ordering::Relaxed);
        // claim an id for this connection, so a read loop that dies can say which one it was
        let id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        // split our stream into read and write halves
        let (tcp_rx, tcp_tx) = stream.into_split();
        // send the read half to our tcp proxy, along with which connection it belongs to
        self.proxy_tx.send((id, tcp_rx)).await.map_err(|e| {
            ConnectError::Io(std::io::Error::new(
                ErrorKind::Other,
                format!("failed to send to proxy: {e}"),
            ))
        })?;
        Ok(ShoalConnection {
            writer: tcp_tx,
            id,
        })
    }

    /// Check if a connection is still valid
    ///
    /// A connection whose read half has stopped is refused here even though its write half is
    /// still perfectly writable, because a socket nobody is reading the answers off of is not a
    /// connection this client can use. `peer_addr` cannot see that on its own — it asks the
    /// kernel about our end of the socket and never touches the wire.
    ///
    /// # Arguments
    ///
    /// * `conn` - The conn to check
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // TODO implement a ping/pong type request?
        //
        // reading this entry is what discards the connection, so it is taken rather than peeked
        if self.dead_conns.pin().remove(&conn.id).is_some() {
            return Err(ConnectError::Io(std::io::Error::new(
                ErrorKind::ConnectionAborted,
                "this connection's read half has stopped",
            )));
        }
        conn.peer_addr()?;
        Ok(())
    }

    /// Check if a connection is broken, without an async context to do it in
    ///
    /// # Arguments
    ///
    /// * `conn` - The conn to check
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        // a connection whose reader has stopped is broken however writable it still looks
        if self.dead_conns.pin().remove(&conn.id).is_some() {
            return true;
        }
        // Check if connection is broken without async context
        conn.peer_addr().is_err()
    }
}

pub struct Shoal<S: QuerySupport> {
    // A pool of tcp connections to send messages over
    pool: bb8::Pool<ShoalConnectionManager>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// A channel of channels to send streaming results over
    channel_queue_rx: AsyncReceiver<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// Whether this client is shutting down or not
    is_shutting_down: Arc<AtomicBool>,
    /// The connections whose read half has stopped, which the pool has not discarded yet
    dead_conns: Arc<HashMap<u64, ()>>,
    /// The largest frame the server will accept, which it tells us when a connection opens
    ///
    /// Every send checks against this before it writes, so that a bundle the server would refuse
    /// comes back as an error naming both sizes rather than as a closed socket.
    peer_max_frame_bytes: Arc<AtomicU32>,
    /// The handle to this clients proxy
    proxy_handle: JoinHandle<()>,
    /// The database kind we are querying
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> Shoal<S> {
    /// Create a new shoal client
    ///
    /// This offers no credentials, which is what a server with no `auth` section wants and what
    /// every client did before there was authentication. Against a server that requires proof this
    /// fails at connect time with [`ConnectError::AuthRequired`] — use
    /// [`Shoal::with_credentials`] instead.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the server to connect too
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        Shoal::connect(addr, ClientOptions::new()).await
    }

    /// Create a new shoal client that can prove who it is
    ///
    /// The credentials are used on every connection the pool opens, including the ones it opens to
    /// replace a connection that died, because they live on the connection manager and `bb8`
    /// already treats making a connection as the place one becomes usable.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the server to connect too
    /// * `credentials` - What to prove this client's identity with
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example<S: shoal_core::shared::traits::QuerySupport>() -> Result<(), shoal_core::client::Errors>
    /// # where for<'a> <<S as shoal_core::shared::traits::QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
    /// #     rkyv::bytecheck::CheckBytes<rkyv::rancor::Strategy<rkyv::validation::Validator<
    /// #         rkyv::validation::archive::ArchiveValidator<'a>,
    /// #         rkyv::validation::shared::SharedValidator>, rkyv::rancor::Error>> {
    /// use shoal_core::client::Shoal;
    /// use shoal_core::shared::auth::Credentials;
    ///
    /// let client = Shoal::<S>::with_credentials(
    ///     "127.0.0.1:12000",
    ///     Credentials::scram("reader", "hunter2"),
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_credentials<A: ToSocketAddrs>(
        addr: A,
        credentials: Credentials,
    ) -> Result<Self, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        Shoal::connect(addr, ClientOptions::new().credentials(credentials)).await
    }

    /// Create a new shoal client from a full set of options
    ///
    /// This is what [`Shoal::new`] and [`Shoal::with_credentials`] both are, spelled out. Reach for
    /// it when a connection needs more than one thing said about it — encryption, credentials, or
    /// both — rather than for a fourth constructor naming the combination.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the server to connect too
    /// * `options` - What to prove this client's identity with and what to encrypt with
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example<S: shoal_core::shared::traits::QuerySupport>() -> Result<(), shoal_core::client::Errors>
    /// # where for<'a> <<S as shoal_core::shared::traits::QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
    /// #     rkyv::bytecheck::CheckBytes<rkyv::rancor::Strategy<rkyv::validation::Validator<
    /// #         rkyv::validation::archive::ArchiveValidator<'a>,
    /// #         rkyv::validation::shared::SharedValidator>, rkyv::rancor::Error>> {
    /// use shoal_core::client::{ClientOptions, Shoal};
    /// use shoal_core::shared::auth::Credentials;
    /// use shoal_core::shared::tls::TlsClientOptions;
    ///
    /// let client = Shoal::<S>::with_options(
    ///     "127.0.0.1:12000",
    ///     ClientOptions::new()
    ///         .credentials(Credentials::scram("reader", "hunter2"))
    ///         .tls(TlsClientOptions::new("/etc/shoal/ca.pem")),
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_options<A: ToSocketAddrs>(
        addr: A,
        options: ClientOptions,
    ) -> Result<Self, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        Shoal::connect(addr, options).await
    }

    /// Build a client and its pool
    ///
    /// Every public constructor lands here rather than one calling the other, so that the ten line
    /// `where` clause each of them carries exists once.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the server to connect too
    /// * `options` - What to prove this client's identity with and what to encrypt with
    async fn connect<A: ToSocketAddrs>(addr: A, options: ClientOptions) -> Result<Self, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // convert our address into a socker addr
        let addr = tokio::net::lookup_host(addr)
            .await
            .map_err(|e| Errors::DnsResolution(format!("failed to resolve host: {e}")))?
            .next()
            .ok_or_else(|| Errors::DnsResolution("no addresses found for host".into()))?;
        // create a channel for our connection pool and our tcp proxy
        let (proxy_tx, proxy_rx) = kanal::unbounded_async();
        // assume the server accepts what we do until it tells us otherwise when we connect
        let peer_max_frame_bytes = Arc::new(AtomicU32::new(protocol::DEFAULT_MAX_FRAME_BYTES));
        // track which connections have stopped being read, so the pool stops handing them out
        let dead_conns = Arc::new(HashMap::with_capacity(16));
        // Create a new shoal connection manager
        let manager = ShoalConnectionManager::new(
            addr,
            proxy_tx,
            &dead_conns,
            &peer_max_frame_bytes,
            S::SCHEMA_FINGERPRINT,
            options,
        )?;
        // build our connection pool
        let pool = bb8::Pool::builder()
            .min_idle(10)
            .max_size(50)
            .connection_timeout(std::time::Duration::from_secs(5))
            .idle_timeout(Some(std::time::Duration::from_secs(300)))
            .max_lifetime(Some(std::time::Duration::from_secs(1800)))
            .build(manager)
            .await
            .map_err(Errors::Handshake)?;
        // build a channel for sending and recieving response streams on
        let (channel_queue_tx, channel_queue_rx) = kanal::bounded_async(8192);
        // create a map for storing what channels to send response streams on
        let channel_map = Arc::new(HashMap::with_capacity(1024));
        // create a bool to track when this client is shutting down
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        // create the response proxy for this client
        let proxy = ShoalTcpProxy::<S::QueryKinds, S::ResponseKinds>::new(
            proxy_rx,
            &channel_map,
            &dead_conns,
            &is_shutting_down,
        );
        // start our proxy
        let proxy_handle = tokio::spawn(async move { proxy.start().await });
        // build our client
        let shoal = Shoal {
            pool,
            channel_map,
            channel_queue_tx,
            channel_queue_rx,
            is_shutting_down,
            dead_conns,
            peer_max_frame_bytes,
            proxy_handle,
            phantom: PhantomData,
        };
        Ok(shoal)
    }

    /// Build a new query object
    #[allow(clippy::unused_self)]
    pub fn query(&self) -> Queries<S> {
        Queries::default()
    }

    /// Get the largest frame the server on the other end of this client will accept
    fn peer_max_frame_bytes(&self) -> u32 {
        self.peer_max_frame_bytes.load(Ordering::Relaxed)
    }

    /// Add a response stream to our channel map
    fn track_response(
        &self,
        query_id: &mut Uuid,
    ) -> Result<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>), Errors> {
        // get the next available response channel or create a new one
        let (tx, rx) = match self.channel_queue_rx.try_recv().map_err(receive_failed)? {
            Some((tx, rx)) => (tx, rx),
            None => kanal::unbounded_async(),
        };
        // keep trying new query ids until we don't hit a collision
        loop {
            // check if this id already exists in our channel map
            if self.channel_map.pin().get(&*query_id).is_none() {
                // insert this id, with no connection yet since nothing has been written
                self.channel_map.pin().insert(
                    *query_id,
                    Waiter {
                        conn: None,
                        tx: tx.clone(),
                    },
                );
                // we found a unique query id so stop trying to find a new id
                break;
            }
            // try to generate a unique query id
            *query_id = Uuid::new_v4();
        }
        // return our channels
        Ok((tx, rx))
    }

    /// Send a query to our server
    pub async fn send(&self, mut queries: Queries<S>) -> Result<ShoalResultStream<S>, Errors> {
        // archive our queries
        let archived = rkyv::to_bytes::<_>(&queries)?;
        // build the header that goes ahead of this bundle
        //
        // this is done before we take a connection from the pool, so a bundle too large to frame
        // fails without ever consuming a pool slot
        let preamble = protocol::request_preamble(archived.len(), self.peer_max_frame_bytes())?;
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut queries.id)?;
        // get a connection from our connection pool and send our query
        let mut conn = self.pool.get().await.map_err(|e| {
            Errors::ConnectionPool(format!("failed to get connection from pool: {e}"))
        })?;
        // build our vectored byte slices to send
        let mut bufs = &mut [IoSlice::new(&preamble), IoSlice::new(&archived)][..];
        // keep sending our data until all of this archive has been sent
        while !bufs.is_empty() {
            // send this data back to our client
            match conn.write_vectored(bufs).await? {
                // if n is zero then no bytes were written
                n if n == 0 => {
                    return Err(Errors::IO(std::io::Error::new(
                        ErrorKind::WriteZero,
                        "no bytes were written",
                    )));
                }
                // consume the data thats already been sent
                n => IoSlice::advance_slices(&mut bufs, n),
            }
        }
        // record which connection this bundle is owed an answer on
        //
        // this is done after the write rather than before it, because a bundle that never
        // reached the socket is not owed anything by that connection
        self.channel_map.pin().insert(
            queries.id,
            Waiter {
                conn: Some(conn.id),
                tx: response_tx.clone(),
            },
        );
        // check that this connection did not die between being handed to us and being written to
        //
        // the read loop marks itself dead before it fails what it owed, so a sweep that ran
        // before the line above found nothing to fail. checking after registering is what closes
        // that window from the other side: one of the two always sees the other
        if self.dead_conns.pin().contains_key(&conn.id) {
            // this stream is over before it started, so give its slot straight back
            self.channel_map.pin().remove(&queries.id);
            let _ = self.channel_queue_tx.send((response_tx, response_rx)).await;
            return Err(Errors::Server {
                query_id: Some(queries.id),
                index: None,
                code: ErrorCode::ConnectionLost,
                msg: "the connection this query was written to had already stopped".to_owned(),
            });
        }
        // build a new shoal result stream
        let result_stream = ShoalResultStream {
            id: queries.id,
            response_tx: Some(response_tx),
            response_rx: Some(response_rx),
            channel_map: self.channel_map.clone(),
            channel_queue_tx: self.channel_queue_tx.clone(),
            next_index: 0,
            unbounded_queries: false,
            pending: BTreeMap::default(),
            phantom: PhantomData,
        };
        Ok(result_stream)
    }

    /// Execute a query and wait for all responses.
    ///
    /// If any errors occur then all valid responses are lost. This will
    /// collect all errors into an Errors::BulkError.
    ///
    /// # Arguments
    ///
    /// * `queries` - The queries to execute
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All queries succeeded
    /// * `Err(Vec<Errors>)` - One or more queries failed, with all failures collected
    pub async fn exec(&self, queries: Queries<S>) -> Result<Vec<ShoalResponse<S>>, Errors>
    where
        <S::ResponseKinds as Archive>::Archived:
            rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // get the number of queries
        let query_count = queries.len();
        // send our queries
        let mut stream = self.send(queries).await.map_err(|e| vec![e])?;
        // collect all responses and failures
        let mut responses = Vec::with_capacity(query_count);
        let mut failures = Vec::new();
        // process all responses
        while let Some(response) = stream.next().await.map_err(|e| vec![e])? {
            // check if this response succeeded or failed
            match response.suceeded(QuerySuceededOpts::default()) {
                Ok(()) => responses.push(response),
                Err(error) => failures.push(error),
            }
        }
        // return any failures or success
        if failures.is_empty() {
            Ok(responses)
        } else {
            Err(Errors::from(failures))
        }
    }

    /// Send a single query and wait for the response.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute
    ///
    /// # Returns
    ///
    /// * `Ok(ShoalResponse)` - The query succeeded
    /// * `Err(Errors)` - The query failed
    pub async fn send_one<Q: Into<S::QueryKinds>>(
        &self,
        query: Q,
    ) -> Result<ShoalResponse<S>, Errors>
    where
        <S::ResponseKinds as Archive>::Archived:
            rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // build a query bundle with our single query
        let queries = self.query().add(query);
        // send our query
        let mut stream = self.send(queries).await?;
        // wait for our single response
        let response = stream
            .next()
            .await?
            .ok_or(Errors::StreamAlreadyTerminated)?;
        // check if this query succeeded
        response.suceeded(QuerySuceededOpts::default())?;
        // return our response
        Ok(response)
    }

    /// Check if data exists in the database
    ///
    /// This method sends an exists query and returns a boolean indicating
    /// whether any matching data was found. Missing data is not an error.
    ///
    /// # Arguments
    ///
    /// * `query` - An exists query (e.g., `TableNameExists::new(...)`)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Data matching the query exists
    /// * `Ok(false)` - No matching data exists
    /// * `Err(Errors)` - An error occurred while executing the query
    pub async fn exists<Q: ExistsQuery + Into<S::QueryKinds>>(
        &self,
        query: Q,
    ) -> Result<bool, Errors>
    where
        <S::ResponseKinds as Archive>::Archived:
            rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // build a query bundle with our single query
        let queries = self.query().add(query);
        // send our query
        let mut stream = self.send(queries).await?;
        // wait for our single response
        let response = stream
            .next()
            .await?
            .ok_or(Errors::StreamAlreadyTerminated)?;
        // a query that failed is a failure rather than a yes or a no
        //
        // without this an unreadable partition comes back as "we expected an exists and got an
        // error", which names the wrong problem entirely
        if let Some(error) = response.error() {
            return Err(Errors::Server {
                query_id: Some(response.get_query_id()),
                index: Some(response.get_index()),
                code: error.code(),
                msg: error.msg().to_owned(),
            });
        }
        // extract the exists result
        match response.get_exists() {
            Some(exists) => Ok(exists),
            // we somehow got the wrong response kind
            None => {
                // build the error to return
                let error = Errors::UnexpectedResponseKind {
                    expected: ResponseActionNames::Exists,
                    actual: response.kind(),
                };
                // return this error
                Err(error)
            }
        }
    }

    /// Create a new stream to send and receive results on
    pub fn stream(&self) -> Result<(ShoalQueryStream<S>, ShoalResultStream<S>), Errors> {
        // generate a random ID to override all of the ids used in our queries
        let mut id = Uuid::new_v4();
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut id)?;
        // build a new shoal result stream
        let result_stream = ShoalResultStream {
            id,
            response_tx: Some(response_tx.clone()),
            response_rx: Some(response_rx),
            channel_map: self.channel_map.clone(),
            channel_queue_tx: self.channel_queue_tx.clone(),
            next_index: 0,
            unbounded_queries: true,
            pending: BTreeMap::default(),
            phantom: PhantomData,
        };
        // wraap our result in a stream that supports queries and results
        let query_stream = ShoalQueryStream {
            id,
            queries_sent: 0,
            pool: self.pool.clone(),
            response_tx,
            channel_map: self.channel_map.clone(),
            data_kind: PhantomData,
            base_index: 0,
            peer_max_frame_bytes: self.peer_max_frame_bytes.clone(),
        };
        Ok((query_stream, result_stream))
    }

    /// Create a new stream to send and receive results on
    pub fn stream_unordered(
        &self,
    ) -> Result<(ShoalQueryStream<S>, ShoalUnorderedResultStream<S>), Errors> {
        // generate a random ID to override all of the ids used in our queries
        let mut id = Uuid::new_v4();
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut id)?;
        // build a new shoal result stream
        let result_stream = ShoalUnorderedResultStream {
            id,
            response_tx: Some(response_tx.clone()),
            response_rx: Some(response_rx),
            channel_map: self.channel_map.clone(),
            channel_queue_tx: self.channel_queue_tx.clone(),
            next_index: 0,
            unbounded_queries: true,
            pending: BTreeSet::default(),
            phantom: PhantomData,
            end: None,
        };
        // wraap our result in a stream that supports queries and results
        let query_stream = ShoalQueryStream {
            id,
            queries_sent: 0,
            pool: self.pool.clone(),
            response_tx,
            channel_map: self.channel_map.clone(),
            data_kind: PhantomData,
            base_index: 0,
            peer_max_frame_bytes: self.peer_max_frame_bytes.clone(),
        };
        Ok((query_stream, result_stream))
    }
}

impl<S: QuerySupport> Drop for Shoal<S> {
    fn drop(&mut self) {
        // set a flag that we are shutting down
        self.is_shutting_down.store(true, Ordering::Relaxed);
        // stop our proxy
        self.proxy_handle.abort();
    }
}

/// What one frame off a connection turned out to be
///
/// The two kinds a server sends share a header and a query id and diverge after that, so they are
/// read by the same function and told apart here rather than by the caller.
#[derive(Debug)]
enum Frame {
    /// A response to a query, as the aligned bytes of its archive
    Response(Uuid, AlignedVec<16>),
    /// A failure, for the query it names or for the connection if that id is nil
    Error(Uuid, ErrorCode, String),
}

struct TcpProxy {
    /// The reader to read messages from the shoal server from
    reader: OwnedReadHalf,
    /// Which connection this is reading, so it can fail the queries written to it and no others
    conn_id: u64,
    /// A map of channels to send messages to stream readers on
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// Where to record that this connection has stopped, so the pool stops handing it out
    dead_conns: Arc<HashMap<u64, ()>>,
    /// Whether shoal or the client is shutting down
    is_shutting_down: Arc<AtomicBool>,
    /// The largest frame this client will read before it refuses the connection
    max_frame_bytes: u32,
}

impl TcpProxy {
    /// Create a new tcp proxy
    ///
    /// # Arguments
    ///
    /// * `reader` - The read half of the connection to relay from
    /// * `conn_id` - Which connection that read half belongs to
    /// * `channel_map` - A distributed map of channels to relay messages with
    /// * `dead_conns` - Where to record that this connection has stopped
    /// * `is_shutting_down` - A flag used to tell the proxy to shutdown
    pub fn new(
        reader: OwnedReadHalf,
        conn_id: u64,
        channel_map: &Arc<HashMap<Uuid, Waiter>>,
        dead_conns: &Arc<HashMap<u64, ()>>,
        is_shutting_down: &Arc<AtomicBool>,
    ) -> Self {
        // Create a new tcp proxy
        TcpProxy {
            reader,
            conn_id,
            channel_map: channel_map.clone(),
            dead_conns: dead_conns.clone(),
            is_shutting_down: is_shutting_down.clone(),
            max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        }
    }

    /// Read a single frame off of this connection, of whichever kind it turns out to be
    ///
    /// # Invariants
    ///
    /// **The preamble and the payload are read by two separate `read_exact` calls, and the payload
    /// is read into a freshly allocated `AlignedVec<16>`.** That is what puts the archive at offset
    /// zero of a sixteen byte aligned allocation, which is what makes turning it into a response a
    /// pointer cast rather than a parse. Merging the two reads into one buffer would land the
    /// payload at offset 24, and offset 24 of a sixteen byte aligned allocation is never itself
    /// sixteen byte aligned, so the zero copy read would end silently.
    /// `the_response_payload_lands_on_a_sixteen_byte_boundary` is the test that catches that.
    ///
    /// **The type dispatch sits between those two reads, and must stay there.** One fixed size
    /// preamble is read for every kind of frame a server sends, because both kinds put their query
    /// id in the same sixteen bytes after the header. Sizing the first read by message type would
    /// need the type before the read that carries it, and reading the body first would put a
    /// response payload at the wrong offset.
    /// `an_error_frame_does_not_disturb_the_response_read` is the test that catches that.
    ///
    /// **The length is checked against our own frame bound before the allocation happens**, not
    /// after. A decoder that returned the length and left the check to the caller would be one
    /// forgotten call site away from letting a peer name its own allocation size.
    async fn read_frame(&mut self) -> Result<Option<Frame>, Errors> {
        // have a buffer for the header and the query id that follows it
        let mut preamble = [0u8; protocol::RESPONSE_PREAMBLE_LEN];
        // try to read from our tcp socket
        if let Err(error) = self.reader.read_exact(&mut preamble).await {
            // if we are shutting down then ignore EOF errors
            if self.is_shutting_down.load(Ordering::Relaxed)
                && error.kind() == ErrorKind::UnexpectedEof
            {
                return Ok(None);
            }
            return Err(Errors::IO(error));
        }
        // check the header and pull out the fields every frame a server sends carries
        //
        // this stops short of deciding what the frame is, because both kinds put their query id
        // in the same sixteen bytes and only the type byte separates them
        let frame = protocol::decode_server_frame(&preamble, self.max_frame_bytes)?;
        // read the rest of this frame according to what it turned out to be
        match frame.header.kind {
            MessageType::Response => {
                // Create an aligned vec to act as a pool of bytes
                let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.rest_len);
                // resize our aligned vec
                aligned_buff.resize(frame.rest_len, 0);
                self.reader.read_exact(&mut aligned_buff).await?;
                Ok(Some(Frame::Response(frame.query_id, aligned_buff)))
            }
            MessageType::Error => {
                // size this frame's message before anything allocates for it
                //
                // the message bound is far tighter than the frame bound, so a frame that got
                // past the check above can still be refused here
                let msg_len = error::msg_len(frame.header)?;
                // an error body is fixed bytes rather than an archive, so a plain vec is enough
                // - there is nothing in it with an alignment requirement to protect
                let mut rest = vec![0u8; msg_len + (error::ERROR_BODY_MIN - protocol::QUERY_ID_LEN)];
                self.reader.read_exact(&mut rest).await?;
                // pull the code and the message out of what we read
                let (code, msg) = error::decode_error_tail(&rest)?;
                Ok(Some(Frame::Error(frame.query_id, code, msg.into_owned())))
            }
            // a server only ever sends these two down a connection, so anything else is a peer
            // that is out of step with us rather than a frame we can act on
            got => Err(Errors::Protocol(ProtocolError::UnexpectedMessageType {
                expected: MessageType::Response,
                got,
            })),
        }
    }

    /// Tell every query written to this connection that it will not be answered
    ///
    /// Without this a caller waiting on a connection that died waits forever. A result stream
    /// holds a clone of its own sender, so the channel never closes and the receiver never
    /// observes that nothing is coming — the only way it can learn is to be told.
    ///
    /// Only the queries written to *this* connection are failed. The channel map is shared by
    /// every connection in the pool, so a sweep of all of it would fail up to forty nine other
    /// connections worth of healthy queries, and would do so on every ordinary idle reap.
    ///
    /// # Arguments
    ///
    /// * `code` - What class of failure ended this connection
    /// * `msg` - What to tell the queries that were waiting on it
    fn fail_waiting(&self, code: ErrorCode, msg: &str) {
        // say that this connection is finished before failing anything on it
        //
        // the order matters. a query written after this sweep has run would otherwise be
        // registered on a connection nobody is reading and never be failed by anything, so the
        // mark goes down first and every send checks it after it registers
        self.dead_conns.pin().insert(self.conn_id, ());
        // collect the queries this connection owes an answer to before telling any of them
        //
        // papaya's guard is bound to the thread that took it, so nothing may be awaited while
        // it is held. collecting first keeps the guard and the sends strictly apart
        let waiting: Vec<AsyncSender<ClientMsg>> = self
            .channel_map
            .pin()
            .values()
            .filter(|waiter| waiter.conn == Some(self.conn_id))
            .map(|waiter| waiter.tx.clone())
            .collect();
        // an idle connection has nobody to tell, but is still marked dead above so that the
        // pool discards it rather than handing it to the next query
        if waiting.is_empty() {
            return;
        }
        event!(
            Level::ERROR,
            msg = "failing the queries a dead connection owed",
            conn = self.conn_id,
            queries = waiting.len(),
            %code,
            reason = msg,
        );
        // tell each of them, without blocking on any of them
        //
        // these channels are unbounded, so a synchronous try_send can only fail if the receiver
        // is gone - in which case there is nobody left to tell
        for tx in waiting {
            let failure = ClientMsg::ServerError(code, msg.to_owned(), ClientStamps::arrived_now());
            let _ = tx.as_sync().try_send(failure);
        }
    }

    /// Start relaying messages from this tcp stream, failing what it owed if it stops
    pub async fn start(mut self) -> Result<(), Errors> {
        // relay until this connection ends, however it ends
        let outcome = self.relay().await;
        // whatever ended it, the queries written to it are never going to be answered
        //
        // this runs on every exit path on purpose. a clean shutdown has no waiters left to
        // fail, and every other way out of the relay loop has some
        match &outcome {
            // the server named a reason, so pass that on rather than inventing one
            Ok(Some((code, msg))) => self.fail_waiting(*code, msg),
            Ok(None) => {
                self.fail_waiting(
                    ErrorCode::ConnectionLost,
                    "the connection to the server closed",
                );
            }
            Err(error) => self.fail_waiting(
                ErrorCode::ConnectionLost,
                &format!("the connection to the server failed: {error}"),
            ),
        }
        // the reason a server gave is for the queries, not for the caller
        outcome.map(|_| ())
    }

    /// Relay messages from this tcp stream until it ends
    ///
    /// Returns the reason the server gave for ending this connection, if it gave one.
    async fn relay(&mut self) -> Result<Option<(ErrorCode, String)>, Errors> {
        // keep reading from our tcp socket
        loop {
            // read the next frame, or stop if this connection is shutting down
            //
            // a HelloAck can never reach here, because the handshake completes before this
            // connections read half is handed to the proxy. moving it after the split would
            // send the ack down this path, where it would decode as a frame with a garbage
            // query id and be dropped by the unknown query arm below
            let frame = match self.read_frame().await? {
                Some(frame) => frame,
                None => return Ok(None),
            };
            // note when this frames last byte arrived
            //
            // the servers own record ends when it hands these bytes to its socket, so this
            // is what closes the loop on the wire time between the two
            let stamps = ClientStamps::arrived_now();
            // work out which query this frame belongs to and what to hand that query
            let (query_id, wrapped) = match frame {
                Frame::Response(query_id, aligned_buff) => {
                    (query_id, ClientMsg::Response(aligned_buff, stamps))
                }
                Frame::Error(query_id, code, msg) => {
                    // a failure with no query to attach it to is about the connection itself,
                    // so it ends this read loop rather than being routed anywhere
                    if query_id.is_nil() {
                        event!(
                            Level::ERROR,
                            msg = "the server failed this connection",
                            %code,
                            reason = msg,
                        );
                        return Ok(Some((code, msg)));
                    }
                    (query_id, ClientMsg::ServerError(code, msg, stamps))
                }
            };
            // get the channel for this query
            match self.channel_map.pin_owned().get(&query_id) {
                // send our message to the right shoal stream
                Some(waiter) => waiter.tx.send(wrapped).await.map_err(send_failed)?,
                // a frame for a query nobody is waiting on is dropped, and this loop goes on
                //
                // that happens when a result stream was dropped before it was drained, which
                // leaks its slot in the channel map. ending the read loop over it would take
                // every other query multiplexed on this connection down with it, which is a
                // far worse answer to one caller's leak than losing the frame is
                None => event!(
                    Level::WARN,
                    msg = "dropped a frame for a query nobody is waiting on",
                    %query_id,
                ),
            }
        }
    }
}

struct ShoalTcpProxy<S: ShoalQuerySupport, R: ShoalResponseSupport> {
    /// The channel to listen for new tcp readers on, with the connection each one belongs to
    proxy_rx: AsyncReceiver<(u64, OwnedReadHalf)>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// Where read loops record that their connection has stopped
    dead_conns: Arc<HashMap<u64, ()>>,
    /// Whether this client is shutting down
    is_shutting_down: Arc<AtomicBool>,
    /// The database we are getting responses from
    phantom_query: PhantomData<S>,
    /// The database we are getting responses from
    phantom_response: PhantomData<R>,
}

impl<S: ShoalQuerySupport, R: ShoalResponseSupport + 'static> ShoalTcpProxy<S, R> {
    /// Create a new udp proxy
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket to listen on
    /// * `channel_map` - A distributed map of channels to relay messages with
    /// * `shutdown` - A flag used to tell the proxy to shutdown
    pub fn new(
        proxy_rx: AsyncReceiver<(u64, OwnedReadHalf)>,
        channel_map: &Arc<HashMap<Uuid, Waiter>>,
        dead_conns: &Arc<HashMap<u64, ()>>,
        is_shutting_down: &Arc<AtomicBool>,
    ) -> Self {
        // create our proxy
        ShoalTcpProxy {
            proxy_rx,
            channel_map: channel_map.clone(),
            dead_conns: dead_conns.clone(),
            is_shutting_down: is_shutting_down.clone(),
            phantom_query: PhantomData,
            phantom_response: PhantomData,
        }
    }

    /// Continuously proxy responses from shoal to the correct client channel
    pub async fn start(self)
    where
        for<'a> <R as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        // Wait for new tcp readers to read from
        loop {
            // wait for a new tcp reader to watch
            let (conn_id, reader) = match self.proxy_rx.recv().await {
                Ok(reader) => reader,
                Err(_) => return,
            };
            // build a new tcp proxy
            let tcp_proxy = TcpProxy::new(
                reader,
                conn_id,
                &self.channel_map,
                &self.dead_conns,
                &self.is_shutting_down,
            );
            // spawn a task to watch this tcp reader for results, saying so if it gives up
            //
            // this handle used to be dropped, which meant every failure in the read loop - a
            // refused frame, a dead socket, a closed channel - was discarded with nothing
            // written down anywhere and every caller on that connection left waiting
            tokio::task::spawn(async move {
                if let Err(error) = tcp_proxy.start().await {
                    event!(
                        Level::ERROR,
                        msg = "a connection to the server stopped being read",
                        conn = conn_id,
                        %error,
                    );
                }
            });
        }
    }
}

/// A accessable or deserializable ShoalResponse
pub struct ShoalResponse<S: QuerySupport> {
    /// The underlying buffer containing our serialized data
    _buff: AlignedVec,
    /// The archived type backed by this vec
    archived: *const <S::ResponseKinds as Archive>::Archived,
    /// When this response arrived on the client side
    stamps: ClientStamps,
    /// The type of data this is a response for
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> std::fmt::Debug for ShoalResponse<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.archived)
    }
}

// SAFETY: AlignedVec is Send, and our pointer points into our own buffer
// which we own and control. As long as we never expose &mut access to _buff,
// this is safe to send across threads.
unsafe impl<S: QuerySupport> Send for ShoalResponse<S>
where
    S: Send,
    <S::ResponseKinds as Archive>::Archived: Send,
{
}

// SAFETY: We never mutate the buffer after construction, and references
// obtained from get() are safe to share across threads as long as
// Archived<T> is Sync.
unsafe impl<S: QuerySupport> Sync for ShoalResponse<S>
where
    S: Sync,
    <S::ResponseKinds as Archive>::Archived: Sync,
{
}

impl<S: QuerySupport> ShoalResponse<S> {
    pub(super) fn new(buff: AlignedVec, stamps: ClientStamps) -> Result<Self, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // access our response
        let archived = S::ResponseKinds::access(&buff)?;
        // This is safe as we own our backing aligned vec
        let const_archived = archived as *const _;
        Ok(ShoalResponse {
            _buff: buff,
            archived: const_archived,
            stamps,
            phantom: PhantomData,
        })
    }

    /// Where the buffer backing this response starts in memory
    ///
    /// This exists so the zero copy property can be asserted from outside this crate, which is
    /// where the only test that can establish it under encryption lives — `shoal/tests/tls.rs`
    /// needs a real server, a real socket and a real kernel to say anything, and none of those are
    /// reachable from a unit test in here.
    ///
    /// The number is only ever interesting modulo sixteen, and it is deliberately the *buffer*
    /// rather than the archive root: rkyv puts the root at the end of the buffer, so its address
    /// carries the archived type's own alignment and not this path's. What matters here is that
    /// the socket's bytes landed at the start of an allocation this client aligned, which is the
    /// same thing `the_response_payload_lands_on_a_sixteen_byte_boundary` asserts on the plaintext
    /// path. A response whose buffer is unaligned means the read path has started copying, and
    /// that failure is otherwise completely silent — a copied response is correct in every
    /// observable way.
    pub fn buffer_address(&self) -> usize {
        self._buff.as_ptr() as usize
    }

    /// Get the inner aligned vec and the stamps that came with it
    ///
    /// Both halves are handed back together on purpose. The reorder buffer takes a response
    /// apart and puts it back together, and a version of this that returned only the buffer
    /// would silently drop the stamps on every out of order response.
    pub(super) fn inner(self) -> (AlignedVec, ClientStamps) {
        (self._buff, self.stamps)
    }

    /// Get when this response arrived on the client side
    #[must_use]
    pub fn stamps(&self) -> ClientStamps {
        self.stamps
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool {
        // get a refernce to our archived response
        let archived = unsafe { &*self.archived };
        // check if this is the end of our stream
        S::ResponseKinds::is_end_of_stream(archived)
    }

    /// Get the id of the query bundle this response belongs to
    pub fn get_query_id(&self) -> Uuid {
        // get a refernce to our archived response
        let archived = unsafe { &*self.archived };
        // get the id of the bundle this response belongs to
        S::ResponseKinds::get_query_id(archived)
    }

    /// Get the index for this response
    pub fn get_index(&self) -> usize {
        // get a refernce to our archived response
        let archived = unsafe { &*self.archived };
        // get the index for this response
        S::ResponseKinds::get_index_archived(archived)
    }

    /// Get an Archived version of this response from the DB
    pub fn access<T: FromShoal<S>>(
        &self,
    ) -> Result<Option<&ArchivedVec<<T as Archive>::Archived>>, Errors> {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // retrieve our rows type
        match T::retrieve(archived)? {
            ArchivedOption::Some(accessed) => Ok(Some(accessed)),
            ArchivedOption::None => Ok(None),
        }
    }

    /// Check if this query succeeded
    pub fn suceeded(&self, opts: QuerySuceededOpts) -> Result<(), Errors> {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // check if this query succeeded
        <S as QuerySupport>::succeeded(archived, opts)
    }

    /// Get the kind of query this is a response to
    pub fn kind(&self) -> ResponseActionNames {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // check if this query succeeded
        <S as QuerySupport>::kind(archived)
    }

    /// Get the exists result if this is an Exists response
    ///
    /// Returns `Some(bool)` if this is an Exists response, `None` otherwise
    pub fn get_exists(&self) -> Option<bool> {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // get the exists result
        <S as QuerySupport>::get_exists(archived)
    }

    /// Get the failure this query answered with, if it failed
    ///
    /// Returns `Some` only for a query the server could not run. A query that ran and found
    /// nothing is not a failure and answers `None` here, which is the distinction the whole error
    /// channel exists to make — before it, both were an empty get.
    pub fn error(&self) -> Option<&ArchivedResponseError> {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // get the failure this query answered with, if there was one
        <S as QuerySupport>::error(archived)
    }

    /// Format this response as column headers and row values
    ///
    /// Returns `Some((headers, rows))` for Get responses with data,
    /// `None` for non-Get responses or empty results.
    pub fn format_response(&self) -> Option<(Vec<&'static str>, Vec<Vec<String>>)> {
        // get a reference to our archived data
        let archived = unsafe { &*self.archived };
        // format the response
        <S as QuerySupport>::format_response(archived)
    }
}

/// The reponses from our queries in a stream
pub struct ShoalResultStream<S: QuerySupport> {
    /// Our query/response channel id
    pub id: Uuid,
    /// The transmission side of the response stream channel
    response_tx: Option<AsyncSender<ClientMsg>>,
    /// the receive side of the response stream channel
    response_rx: Option<AsyncReceiver<ClientMsg>>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// The next message to be returned
    next_index: usize,
    /// Whether this result stream is tied to an unbounded query stream
    pub unbounded_queries: bool,
    /// The messages that are to be returned later to ensure the correct order of receipt
    pending: BTreeMap<usize, ClientMsg>,
    /// The database kind we are streaming response for
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> ShoalResultStream<S>
where
    <S::ResponseKinds as Archive>::Archived:
        rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
{
    async fn wait_for_next_response(&mut self) -> Result<(bool, Option<ShoalResponse<S>>), Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // get our response channel if one exists or
        let response_rx = match self.response_rx.as_mut() {
            Some(response_rx) => response_rx,
            None => return Err(Errors::StreamAlreadyTerminated),
        };
        // loop over our responses until we get the next one
        loop {
            // get the next pending row
            if let Some((first_index, _)) = self.pending.first_key_value() {
                // check if we already have the next response
                if self.next_index == *first_index {
                    // pop and return the next response to return
                    if let Some((_, msg)) = self.pending.pop_first() {
                        // increment our index
                        self.next_index += 1;
                        // handle the different client messages
                        match msg {
                            // get this responses message
                            ClientMsg::Response(response, stamps) => {
                                // wrap our response so we don't have to keep repaying access costs
                                let response = ShoalResponse::<S>::new(response, stamps)?;
                                // only bother to check our server sent end of stream if our queries are bounded
                                let end = if self.unbounded_queries {
                                    // we have unbounded queries so set end to false
                                    false
                                } else {
                                    // check if this response is the last one in this stream
                                    response.is_end_of_stream()
                                };
                                // build our shoal response
                                return Ok((end, Some(response)));
                            }
                            // a failure ends this stream where it lands, since it names the
                            // bundle rather than a position in it
                            ClientMsg::ServerError(code, msg, _) => {
                                return Err(Errors::Server {
                                    query_id: Some(self.id),
                                    index: None,
                                    code,
                                    msg,
                                })
                            }
                            ClientMsg::End(_) => return Ok((true, None)),
                        };
                    }
                }
            }
            // get the next response from our query
            let msg = response_rx.recv().await.map_err(receive_failed)?;
            // handle the different client messages
            match msg {
                // get this responses message
                ClientMsg::Response(response, stamps) => {
                    // wrap our response so we don't have to keep repaying access costs
                    let response = ShoalResponse::<S>::new(response, stamps)?;
                    // get the index for this message
                    let index = response.get_index();
                    // if this is the next row then return it
                    if self.next_index == index {
                        // increment the index of our next response
                        self.next_index += 1;
                        // only bother to check our server sent end of stream if our queries are bounded
                        let end = if self.unbounded_queries {
                            // we have unbounded queries so set end to false
                            false
                        } else {
                            // check if this response is the last one in this stream
                            response.is_end_of_stream()
                        };
                        // this is the next response so just return it
                        return Ok((end, Some(response)));
                    }
                    // rewrap our response in a client message
                    //
                    // the stamps come back apart with the buffer here, so a response that has
                    // to wait in the reorder buffer keeps the arrival time it was read with
                    // rather than picking up a new one when it is finally returned
                    let (buff, stamps) = response.inner();
                    let rewrapped = ClientMsg::Response(buff, stamps);
                    // push this into our pending responses and wait for the next response
                    self.pending.insert(index, rewrapped);
                }
                // a failure ends this stream where it lands, since it names the bundle rather
                // than a position in it - there is no index to buffer it at
                ClientMsg::ServerError(code, msg, _) => {
                    return Err(Errors::Server {
                        query_id: Some(self.id),
                        index: None,
                        code,
                        msg,
                    })
                }
                ClientMsg::End(index) => {
                    // if this is the next row then return it
                    if self.next_index == index {
                        // this stream ended so return None
                        return Ok((true, None));
                    }
                    // rewrap our response in a client message
                    let rewrapped = ClientMsg::End(index);
                    // push this into our pending responses and wait for the next response
                    self.pending.insert(index, rewrapped);
                }
            };
        }
    }

    /// Skip some number of responses
    ///
    /// If there are less responses then the requested number of skips this will
    /// skip up to that.
    ///
    /// # Arguments
    ///
    /// * `skip` - The number of responses to skip
    pub async fn skip(&mut self, mut skip: usize) -> Result<(), Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // get the next message and throw it away
        while let Some(_) = self.next().await? {
            // decrement our skip
            skip -= 1;
            // if skip is 0 then we can return
            if skip == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Give this streams slot in the channel map and its channel pair back
    ///
    /// The entry in the channel map is what the proxy routes a response through, so a stream that
    /// ended without removing it leaves every later response for that id being delivered into a
    /// channel with no reader.
    async fn release(&mut self) -> Result<(), Errors> {
        // remove this stream id from our channel map
        self.channel_map.pin().remove(&self.id);
        // take the ends of our channel
        if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
            self.channel_queue_tx
                .send((tx, rx))
                .await
                .map_err(send_failed)?;
        }
        Ok(())
    }

    /// Get the next response to our query
    pub async fn next(&mut self) -> Result<Option<ShoalResponse<S>>, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // try to get our receive channels
        if self.response_rx.is_some() {
            // wait for the next response
            let outcome = self.wait_for_next_response().await;
            // release this stream whichever way that went
            //
            // a stream that failed has ended just as surely as one that reached its last
            // response, so leaving the release to the end arm alone is what made a failed
            // query leak the channel map entry its responses are routed through
            if matches!(outcome, Err(_) | Ok((true, _))) {
                self.release().await?;
            }
            // return our accessable response, or the failure that ended this stream
            let (_, resp) = outcome?;
            Ok(resp)
        } else {
            // this stream has already ended
            Ok(None)
        }
    }

    ///// Get the next response to our query and cast it to a specific type
    //pub async fn next_typed<T: FromShoal<S>>(&mut self) -> Result<Option<Vec<T>>, Errors>
    //where
    //    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
    //        rkyv::bytecheck::CheckBytes<
    //            Strategy<
    //                rkyv::validation::Validator<
    //                    rkyv::validation::archive::ArchiveValidator<'a>,
    //                    rkyv::validation::shared::SharedValidator,
    //                >,
    //                rkyv::rancor::Error,
    //            >,
    //        >,
    //{
    //    // try to get our receive channels
    //    if self.response_rx.is_some() {
    //        // wait for the next response
    //        let (end, resp) = self.wait_for_next_response().await?;
    //        // if this is the final response then take return our channels
    //        if end {
    //            // remove this stream id from our channel map
    //            self.channel_map.pin().remove(&self.id);
    //            // take the ends of our channel
    //            if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
    //                self.channel_queue_tx.send((tx, rx)).await?;
    //            }
    //        }
    //        // try to cast to the correct type
    //        Ok(T::retrieve(resp)?)
    //    } else {
    //        // this stream has already ended
    //        Ok(None)
    //    }
    //}

    ///// Get the next response to our query and get the first row returned and cast it to our specific type
    /////
    ///// This will ignore any remaining rows in the next response.
    //pub async fn next_typed_first<T: FromShoal<S>>(&mut self) -> Result<Option<Option<T>>, Errors>
    //where
    //    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
    //        rkyv::bytecheck::CheckBytes<
    //            Strategy<
    //                rkyv::validation::Validator<
    //                    rkyv::validation::archive::ArchiveValidator<'a>,
    //                    rkyv::validation::shared::SharedValidator,
    //                >,
    //                rkyv::rancor::Error,
    //            >,
    //        >,
    //{
    //    // try to get the next response
    //    match self.next_typed().await? {
    //        Some(mut rows) => {
    //            // check how may rows we found
    //            if rows.len() == 1 {
    //                // if we only have a single row then just remove it
    //                Ok(Some(Some(rows.remove(0))))
    //            } else {
    //                // we have more then 1 row so do a swap remove to avoid moving the items in the vec forward
    //                Ok(Some(Some(rows.swap_remove(1))))
    //            }
    //        }
    //        //Some(None) => Ok(Some(None)),
    //        None => Ok(None),
    //    }
    //}
}

/// The reponses from our queries in a stream
pub struct ShoalUnorderedResultStream<S: QuerySupport> {
    /// Our query/response channel id
    pub id: Uuid,
    /// The transmission side of the response stream channel
    response_tx: Option<AsyncSender<ClientMsg>>,
    /// the receive side of the response stream channel
    response_rx: Option<AsyncReceiver<ClientMsg>>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// The next message to be returned
    next_index: usize,
    /// Whether this result stream is tied to an unbounded query stream
    pub unbounded_queries: bool,
    /// The response indexes that we have received but have not yet reached
    pending: BTreeSet<usize>,
    /// The final message if one was set
    end: Option<usize>,
    /// The database kind we are streaming response for
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> ShoalUnorderedResultStream<S>
where
    <S::ResponseKinds as Archive>::Archived:
        rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
{
    async fn wait_for_next_response(&mut self) -> Result<(bool, Option<ShoalResponse<S>>), Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // get our response channel if one exists or
        let response_rx = match self.response_rx.as_mut() {
            Some(response_rx) => response_rx,
            None => return Err(Errors::StreamAlreadyTerminated),
        };
        // keep looping until we have a message to return
        loop {
            // wait for the next message to return
            match response_rx.recv().await.map_err(receive_failed)? {
                ClientMsg::Response(archived, stamps) => {
                    // wrap our response so we don't have to keep repaying access costs
                    let response = ShoalResponse::<S>::new(archived, stamps)?;
                    // get the index for this message
                    let index = response.get_index();
                    // if this is our next index then increment next as far as we can
                    if self.next_index == index {
                        // increment our next index since we are returning the next item
                        self.next_index += 1;
                        // this is our next index so increment our index as far as possible
                        while let Some(first_index) = self.pending.first() {
                            // if this is also our next index then pop pending
                            if self.next_index == *first_index {
                                // pop pending
                                self.pending.pop_first();
                                // increment our next index
                                self.next_index += 1;
                            } else {
                                // we are still waiting on our next index
                                break;
                            }
                        }
                    } else {
                        // add this index to our pending set
                        self.pending.insert(index);
                    }
                    // get if this is the last response
                    let is_end = Some(self.next_index) == self.end;
                    // return this response
                    return Ok((is_end, Some(response)));
                }
                // a failure ends this stream where it lands, since it names the bundle rather
                // than a position in it
                ClientMsg::ServerError(code, msg, _) => {
                    return Err(Errors::Server {
                        query_id: Some(self.id),
                        index: None,
                        code,
                        msg,
                    })
                }
                ClientMsg::End(end_index) => {
                    // check if the last return message was the end
                    if self.next_index == end_index {
                        // the last returned message was the end
                        return Ok((true, None));
                    }
                    // update our end index
                    self.end = Some(end_index)
                }
            }
        }
    }

    /// Give this streams slot in the channel map and its channel pair back
    ///
    /// The entry in the channel map is what the proxy routes a response through, so a stream that
    /// ended without removing it leaves every later response for that id being delivered into a
    /// channel with no reader.
    async fn release(&mut self) -> Result<(), Errors> {
        // remove this stream id from our channel map
        self.channel_map.pin().remove(&self.id);
        // take the ends of our channel
        if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
            self.channel_queue_tx
                .send((tx, rx))
                .await
                .map_err(send_failed)?;
        }
        Ok(())
    }

    /// Get the next available response to our query
    pub async fn next(&mut self) -> Result<Option<ShoalResponse<S>>, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // try to get our receive channels
        if self.response_rx.is_some() {
            // wait for the next response
            let outcome = self.wait_for_next_response().await;
            // release this stream whichever way that went, for the same reason the ordered
            // stream does - a stream that failed has ended and owes its slot back
            if matches!(outcome, Err(_) | Ok((true, _))) {
                self.release().await?;
            }
            // return our accessable response, or the failure that ended this stream
            let (_, resp) = outcome?;
            Ok(resp)
        } else {
            // this stream has already ended
            Ok(None)
        }
    }
}

/// A Stream in shoal where you can add queries and get back results in order
///
/// This is different then a `ShoalResultStream` as you can continue to add queries
/// to it.
pub struct ShoalQueryStream<Q: QuerySupport> {
    /// Our query/response channel id
    pub id: Uuid,
    /// The number of messages that have been sent
    pub queries_sent: usize,
    // A pool of tcp connections to send messages over
    pool: bb8::Pool<ShoalConnectionManager>,
    /// The transmission side of the response stream channel
    response_tx: AsyncSender<ClientMsg>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, Waiter>>,
    /// The data we are sending queries for
    data_kind: PhantomData<Q>,
    /// The base index to set in queries
    pub base_index: usize,
    /// The largest frame the server will accept, which it told us when a connection opened
    peer_max_frame_bytes: Arc<AtomicU32>,
}

impl<Q: QuerySupport> ShoalQueryStream<Q> {
    /// Build a new query object
    #[allow(clippy::unused_self)]
    pub fn query(&self) -> Queries<Q> {
        Queries {
            id: self.id,
            queries: Vec::with_capacity(1),
            base_index: 0,
        }
    }

    /// Build a new query object
    #[allow(clippy::unused_self)]
    pub fn query_with_capacity(&self, capacity: usize) -> Queries<Q> {
        Queries {
            id: self.id,
            queries: Vec::with_capacity(capacity),
            base_index: 0,
        }
    }

    /// Some queries to this stream without ending it
    ///
    /// # Arguments
    ///
    /// * `queries` - The queries to send
    ///
    /// Returns what this send cost, broken into serialize, pool acquire, and socket write.
    /// Those are batch level costs shared by every query in the bundle, and are a zero sized
    /// type unless the `stage-profile` feature is on, so a caller that ignores them pays
    /// nothing for them.
    pub async fn send(&mut self, mut queries: Queries<Q>) -> Result<BatchStamps, Errors> {
        // start timing this bundle
        let mut stamps = BatchStamps::entered_now();
        // override our query id
        // TODO make it so we don't need to do this
        queries.id = self.id;
        // update the base index for this query bundle correctly
        queries.base_index = self.base_index;
        // archive our queries
        let archived = rkyv::to_bytes::<_>(&queries)?;
        // record what serializing this bundle cost
        stamps.mark_serialized();
        // build the header that goes ahead of this bundle
        //
        // this is attributed to serialization rather than to the pool wait, since it is the last
        // thing done to the bytes before a connection is asked for
        let preamble = protocol::request_preamble(
            archived.len(),
            self.peer_max_frame_bytes.load(Ordering::Relaxed),
        )?;
        // get a connection from our connection pool and send our query
        let mut conn = self.pool.get().await.map_err(|e| {
            Errors::ConnectionPool(format!("failed to get connection from pool: {e}"))
        })?;
        // record what waiting on the connection pool cost
        //
        // this is where client side backpressure shows up once enough queries are in flight
        stamps.mark_pooled();
        // build our vectored byte slices to send
        let mut bufs = &mut [IoSlice::new(&preamble), IoSlice::new(&archived)][..];
        // keep sending our data until all of this archive has been sent
        while !bufs.is_empty() {
            // send this data back to our client
            match conn.write_vectored(bufs).await? {
                // if n is zero then no bytes were written
                n if n == 0 => {
                    return Err(Errors::IO(std::io::Error::new(
                        ErrorKind::WriteZero,
                        "no bytes were written",
                    )));
                }
                // consume the data thats already been sent
                n => IoSlice::advance_slices(&mut bufs, n),
            }
        }
        // record that this bundle is now the sockets problem
        stamps.mark_written();
        // record which connection this bundle is owed an answer on
        //
        // a query stream takes whatever connection the pool hands out per bundle, so this can
        // move between bundles. it names the most recent one, which is the one the answers we
        // are still waiting for are coming back over
        self.channel_map.pin().insert(
            self.id,
            Waiter {
                conn: Some(conn.id),
                tx: self.response_tx.clone(),
            },
        );
        // increment the number of queries sent and our base index
        self.queries_sent += 1;
        self.base_index += queries.queries.len();
        Ok(stamps)
    }

    /// Close this query stream
    pub async fn close(self) -> Result<(), Errors> {
        self.response_tx
            .send(ClientMsg::End(self.base_index))
            .await
            .map_err(send_failed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{error, protocol, ClientMsg, ErrorCode, Frame, TcpProxy, Waiter};
    use papaya::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use uuid::Uuid;

    /// Build the whole of an error frame, preamble and message together
    ///
    /// # Arguments
    ///
    /// * `query_id` - The query this failure belongs to, or nil for the connection itself
    /// * `code` - What class of failure this is
    /// * `msg` - What to say about it
    fn error_frame(query_id: &Uuid, code: ErrorCode, msg: &str) -> Vec<u8> {
        // build the preamble that goes ahead of the message
        let preamble = error::error_preamble(
            query_id,
            code,
            msg.len(),
            protocol::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("failed to build an error preamble");
        // lay the message down behind it
        let mut frame = Vec::from(preamble);
        frame.extend_from_slice(msg.as_bytes());
        frame
    }

    /// The payload lengths a response frame is read at
    ///
    /// These are deliberately awkward. A single length that happened to be a multiple of sixteen
    /// would pass an alignment check by luck, so the list runs either side of the preamble length
    /// and either side of the alignment itself.
    const PAYLOAD_LENS: [usize; 7] = [0, 1, 17, 23, 25, 31, 4095];

    /// The response payload lands at the start of a sixteen byte aligned allocation
    ///
    /// This is the test that catches the two `read_exact` calls in `TcpProxy::read_frame` being
    /// merged into one. A single read of preamble plus payload puts the payload at offset 24 of
    /// the buffer, and offset 24 of a sixteen byte aligned allocation is never itself sixteen byte
    /// aligned — so the archive would stop being readable by a pointer cast and would either fail
    /// `bytecheck` or, worse, be read unaligned. Merging those reads looks like an obvious
    /// optimization, which is exactly why it needs a test standing in front of it.
    #[tokio::test]
    async fn the_response_payload_lands_on_a_sixteen_byte_boundary() {
        // walk a set of payload lengths so that no single one can pass by luck
        for len in PAYLOAD_LENS {
            // stand up a socket pair, letting the kernel pick the port
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("failed to bind a listener");
            let addr = listener.local_addr().expect("listener had no address");
            // build the frame a server would write for a response of this size
            let query_id = Uuid::new_v4();
            let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let preamble =
                protocol::response_preamble(&query_id, len, protocol::DEFAULT_MAX_FRAME_BYTES)
                    .expect("failed to build a response preamble");
            // write that frame from the server side of the pair
            let written = payload.clone();
            let server = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.expect("failed to accept");
                sock.write_all(&preamble).await.expect("failed to write");
                sock.write_all(&written).await.expect("failed to write");
                sock.flush().await.expect("failed to flush");
            });
            // read it back through the same path a real response takes
            let stream = TcpStream::connect(addr).await.expect("failed to connect");
            let (reader, _writer) = stream.into_split();
            let channel_map = Arc::new(HashMap::with_capacity(1));
            let dead_conns = Arc::new(HashMap::with_capacity(1));
            let is_shutting_down = Arc::new(AtomicBool::new(false));
            let mut proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
            let frame = proxy
                .read_frame()
                .await
                .expect("failed to read a frame")
                .expect("the connection closed instead of yielding a frame");
            server.await.expect("the writer task panicked");
            // a response frame has to read back as one and not as anything else
            let super::Frame::Response(read_id, buff) = frame else {
                panic!("a response frame read back as something else for len {len}");
            };
            // the routing field and the payload both survived
            assert_eq!(read_id, query_id);
            assert_eq!(buff.len(), len, "payload length changed for len {len}");
            assert_eq!(&buff[..], &payload[..], "payload changed for len {len}");
            // and the payload starts on a sixteen byte boundary, which is the point
            //
            // an empty payload is skipped because there is no allocation to align: an
            // `AlignedVec` that never allocated hands back a dangling pointer, and asserting
            // anything about it would be asserting something about `AlignedVec` rather than
            // about this read path
            if len > 0 {
                assert_eq!(
                    buff.as_ptr() as usize % 16,
                    0,
                    "a payload of {len} bytes did not land on a sixteen byte boundary"
                );
            }
        }
    }

    /// A frame larger than this client will accept is refused instead of allocated for
    #[tokio::test]
    async fn a_frame_over_our_bound_is_refused_before_it_is_allocated_for() {
        // stand up a socket pair
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind a listener");
        let addr = listener.local_addr().expect("listener had no address");
        // build a preamble claiming far more than the client will accept, and send nothing else
        let mut preamble =
            protocol::response_preamble(&Uuid::new_v4(), 0, protocol::DEFAULT_MAX_FRAME_BYTES)
                .expect("failed to build a response preamble");
        preamble[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("failed to accept");
            sock.write_all(&preamble).await.expect("failed to write");
            sock.flush().await.expect("failed to flush");
        });
        // the read has to fail on the header rather than block waiting for bytes that never come
        let stream = TcpStream::connect(addr).await.expect("failed to connect");
        let (reader, _writer) = stream.into_split();
        let channel_map = Arc::new(HashMap::with_capacity(1));
        let dead_conns = Arc::new(HashMap::with_capacity(1));
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let mut proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
        let error = proxy
            .read_frame()
            .await
            .expect_err("an oversize frame was accepted");
        server.await.expect("the writer task panicked");
        // and the error names the bound rather than being an out of memory abort
        assert!(
            matches!(
                error,
                super::Errors::Protocol(protocol::ProtocolError::FrameTooLarge { .. })
            ),
            "an oversize frame failed with the wrong error: {error:?}"
        );
    }

    /// An error frame ahead of a response leaves the response's payload aligned
    ///
    /// The dispatch between the two frame kinds sits between the preamble read and the body read,
    /// which is exactly where a change could collapse them into one. This walks an error frame
    /// through first so that the response behind it is read at a socket offset the response path
    /// never sees on its own, and then asserts the alignment invariant still holds.
    #[tokio::test]
    async fn an_error_frame_does_not_disturb_the_response_read() {
        // walk the same awkward payload lengths, since the offset the error frame leaves behind
        // interacts with each of them differently
        for len in PAYLOAD_LENS {
            // stand up a socket pair, letting the kernel pick the port
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("failed to bind a listener");
            let addr = listener.local_addr().expect("listener had no address");
            // build an error frame and a response frame for two different queries
            let failed_id = Uuid::new_v4();
            let ok_id = Uuid::new_v4();
            let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let failure = error_frame(&failed_id, ErrorCode::StorageRead, "partition 7 is gone");
            let preamble =
                protocol::response_preamble(&ok_id, len, protocol::DEFAULT_MAX_FRAME_BYTES)
                    .expect("failed to build a response preamble");
            // write the error frame first and the response behind it
            let written = payload.clone();
            let server = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.expect("failed to accept");
                sock.write_all(&failure).await.expect("failed to write");
                sock.write_all(&preamble).await.expect("failed to write");
                sock.write_all(&written).await.expect("failed to write");
                sock.flush().await.expect("failed to flush");
            });
            // read both back through the same path a real connection takes
            let stream = TcpStream::connect(addr).await.expect("failed to connect");
            let (reader, _writer) = stream.into_split();
            let channel_map = Arc::new(HashMap::with_capacity(1));
            let dead_conns = Arc::new(HashMap::with_capacity(1));
            let is_shutting_down = Arc::new(AtomicBool::new(false));
            let mut proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
            // the error frame reads back whole, naming its query and its code
            let first = proxy
                .read_frame()
                .await
                .expect("failed to read the error frame")
                .expect("the connection closed instead of yielding the error frame");
            let Frame::Error(read_id, code, msg) = first else {
                panic!("an error frame read back as a response for len {len}");
            };
            assert_eq!(read_id, failed_id);
            assert_eq!(code, ErrorCode::StorageRead);
            assert_eq!(msg, "partition 7 is gone");
            // and the response behind it is still exactly what was written
            let second = proxy
                .read_frame()
                .await
                .expect("failed to read the response frame")
                .expect("the connection closed instead of yielding the response frame");
            let Frame::Response(read_id, buff) = second else {
                panic!("a response frame read back as an error for len {len}");
            };
            server.await.expect("the writer task panicked");
            assert_eq!(read_id, ok_id);
            assert_eq!(&buff[..], &payload[..], "payload changed for len {len}");
            // and it still lands on a sixteen byte boundary, which is what the dispatch had to
            // not break - an empty payload never allocated, so there is nothing to align
            if len > 0 {
                assert_eq!(
                    buff.as_ptr() as usize % 16,
                    0,
                    "a payload of {len} bytes behind an error frame lost its alignment"
                );
            }
        }
    }

    /// An error frame is delivered to the query it names
    #[tokio::test]
    async fn an_error_frame_is_delivered_to_the_query_it_names() {
        // stand up a socket pair
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind a listener");
        let addr = listener.local_addr().expect("listener had no address");
        // write one error frame for a query we are about to register
        let query_id = Uuid::new_v4();
        let frame = error_frame(&query_id, ErrorCode::ArchiveMissing, "archive is gone");
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("failed to accept");
            sock.write_all(&frame).await.expect("failed to write");
            sock.flush().await.expect("failed to flush");
            // hold the socket open so the read loop ends on our frame rather than on EOF
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        // register a channel for that query, the way sending a bundle would
        let stream = TcpStream::connect(addr).await.expect("failed to connect");
        let (reader, _writer) = stream.into_split();
        let channel_map = Arc::new(HashMap::with_capacity(1));
        let (tx, rx) = kanal::unbounded_async();
        channel_map
            .pin()
            .insert(query_id, Waiter { conn: Some(1), tx });
        let dead_conns = Arc::new(HashMap::with_capacity(1));
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
        tokio::spawn(proxy.start());
        // the failure arrives on that query's channel, with the code and message it was sent with
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the failure never arrived")
            .expect("the channel closed instead of delivering the failure");
        server.await.expect("the writer task panicked");
        match msg {
            ClientMsg::ServerError(code, msg, _) => {
                assert_eq!(code, ErrorCode::ArchiveMissing);
                assert_eq!(msg, "archive is gone");
            }
            other => panic!("an error frame was delivered as something else: {other:?}"),
        }
    }

    /// A frame for a query nobody is waiting on does not end the read loop
    ///
    /// A result stream dropped before it was drained leaves its id in the channel map, and every
    /// response the server still sends for it arrives here with nowhere to go. Ending the loop
    /// over that would take every other query multiplexed on the same connection down with it.
    #[tokio::test]
    async fn an_error_frame_for_an_unknown_query_does_not_end_the_read_loop() {
        // stand up a socket pair
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind a listener");
        let addr = listener.local_addr().expect("listener had no address");
        // write a frame for a query nobody knows about, then one for a query somebody does
        let unknown_id = Uuid::new_v4();
        let known_id = Uuid::new_v4();
        let orphan = error_frame(&unknown_id, ErrorCode::StorageRead, "nobody is listening");
        let wanted = error_frame(&known_id, ErrorCode::Internal, "somebody is");
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("failed to accept");
            sock.write_all(&orphan).await.expect("failed to write");
            sock.write_all(&wanted).await.expect("failed to write");
            sock.flush().await.expect("failed to flush");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        // only register the second query
        let stream = TcpStream::connect(addr).await.expect("failed to connect");
        let (reader, _writer) = stream.into_split();
        let channel_map = Arc::new(HashMap::with_capacity(1));
        let (tx, rx) = kanal::unbounded_async();
        channel_map
            .pin()
            .insert(known_id, Waiter { conn: Some(1), tx });
        let dead_conns = Arc::new(HashMap::with_capacity(1));
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
        tokio::spawn(proxy.start());
        // the second frame still arrives, which it could not do if the first had ended the loop
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the read loop died on a frame nobody was waiting on")
            .expect("the channel closed instead of delivering the failure");
        server.await.expect("the writer task panicked");
        match msg {
            ClientMsg::ServerError(code, msg, _) => {
                assert_eq!(code, ErrorCode::Internal);
                assert_eq!(msg, "somebody is");
            }
            other => panic!("an error frame was delivered as something else: {other:?}"),
        }
    }

    /// A connection that dies fails the queries written to it, and only those
    ///
    /// A result stream holds a clone of its own sender, so a dropped connection never closes the
    /// channel a caller is parked on — before the sweep, a query whose connection died waited
    /// forever with the failure discarded along with the read task's join handle. The second
    /// query here is the other half of the test: the channel map is shared by the whole pool, so
    /// a sweep that failed everything would break every healthy connection alongside the dead one.
    #[tokio::test]
    async fn a_dead_connection_fails_the_queries_it_owed_and_no_others() {
        // stand up a socket pair whose server side hangs up without answering
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind a listener");
        let addr = listener.local_addr().expect("listener had no address");
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.expect("failed to accept");
            drop(sock);
        });
        // register one query on the connection that is about to die, and one on another
        let stream = TcpStream::connect(addr).await.expect("failed to connect");
        let (reader, _writer) = stream.into_split();
        let channel_map = Arc::new(HashMap::with_capacity(2));
        let (doomed_tx, doomed_rx) = kanal::unbounded_async();
        let (other_tx, other_rx) = kanal::unbounded_async();
        channel_map.pin().insert(
            Uuid::new_v4(),
            Waiter {
                conn: Some(1),
                tx: doomed_tx,
            },
        );
        channel_map.pin().insert(
            Uuid::new_v4(),
            Waiter {
                conn: Some(2),
                tx: other_tx,
            },
        );
        // read that connection until it ends
        let dead_conns = Arc::new(HashMap::with_capacity(1));
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let proxy = TcpProxy::new(reader, 1, &channel_map, &dead_conns, &is_shutting_down);
        let _ = proxy.start().await;
        server.await.expect("the listener task panicked");
        // the query on the dead connection was told, rather than left waiting
        let msg = doomed_rx
            .try_recv()
            .expect("the channel closed")
            .expect("a query on a dead connection was not told about it");
        match msg {
            ClientMsg::ServerError(code, _, _) => assert_eq!(code, ErrorCode::ConnectionLost),
            other => panic!("a dead connection delivered something else: {other:?}"),
        }
        // and the query on the other connection was left entirely alone
        assert!(
            other_rx.try_recv().expect("the channel closed").is_none(),
            "one dead connection failed a query belonging to another"
        );
    }
}
