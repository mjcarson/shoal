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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub mod errors;
pub mod messages;

use super::shared::queries::Queries;
use crate::shared::protocol::{self, handshake, ProtocolError};
use crate::shared::responses::ResponseActionNames;
use crate::shared::traits::{
    ExistsQuery, QuerySupport, RkyvSupport, ShoalQuerySupport, ShoalResponseSupport,
};
pub use errors::{ConnectError, Errors, ShqlParseError};
use messages::{BatchStamps, ClientMsg, ClientStamps};

// Connection manager for bb8
#[derive(Clone)]
struct ShoalConnectionManager {
    /// The shoal server to connect too
    server_addr: SocketAddr,
    /// The channel to send our read halves to our proxy over
    proxy_tx: AsyncSender<OwnedReadHalf>,
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
}

/// How long a server has to finish its half of the handshake
///
/// `bb8`'s connection timeout bounds its retry loop and `pool.get()`, not `connect` itself, so
/// without this a server that accepts a connection and then stalls would park `Shoal::new`
/// forever. Before the handshake existed `connect` could not block at all, since it neither read
/// nor wrote — this deadline is created by the handshake and belongs to it.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl ShoalConnectionManager {
    /// Create a new shoal connection manager
    ///
    /// # Arguments
    ///
    /// * `server_addr` - The address of the server to connect too
    /// * `proxy_tx` - The channel to hand read halves to the proxy over
    /// * `peer_max_frame_bytes` - Where to record the largest frame the server will accept
    /// * `schema_fingerprint` - The fingerprint of the schema this client was built from
    pub fn new(
        server_addr: SocketAddr,
        proxy_tx: AsyncSender<OwnedReadHalf>,
        peer_max_frame_bytes: &Arc<AtomicU32>,
        schema_fingerprint: u64,
    ) -> Self {
        ShoalConnectionManager {
            server_addr,
            proxy_tx,
            peer_max_frame_bytes: peer_max_frame_bytes.clone(),
            schema_fingerprint,
        }
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
        // say who we are, and how large a frame we are willing to be sent
        let hello = handshake::Hello {
            schema_fingerprint: self.schema_fingerprint,
            max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
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
        Ok(ack)
    }
}

#[async_trait::async_trait]
impl ManageConnection for ShoalConnectionManager {
    type Connection = OwnedWriteHalf;
    type Error = ConnectError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut stream = TcpStream::connect(&self.server_addr).await?;
        // Disable Nagle's algorithm
        stream.set_nodelay(true)?;
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
        // split our stream into read and write halves
        let (tcp_rx, tcp_tx) = stream.into_split();
        // send the read half to our tcp proxy
        self.proxy_tx.send(tcp_rx).await.map_err(|e| {
            ConnectError::Io(std::io::Error::new(
                ErrorKind::Other,
                format!("failed to send to proxy: {e}"),
            ))
        })?;
        Ok(tcp_tx)
    }

    /// Check if a connection is still valid
    ///
    /// # Arguments
    ///
    /// * `conn` - The conn to check
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // TODO implement a ping/pong type request?
        conn.peer_addr()?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        // Check if connection is broken without async context
        conn.peer_addr().is_err()
    }
}

pub struct Shoal<S: QuerySupport> {
    // A pool of tcp connections to send messages over
    pool: bb8::Pool<ShoalConnectionManager>,
    /// A concurrent map of what channel to send streaming results too
    pub channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// A channel of channels to send streaming results over
    channel_queue_rx: AsyncReceiver<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    /// Whether this client is shutting down or not
    is_shutting_down: Arc<AtomicBool>,
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
    /// # Arguments
    ///
    /// * `socket` - The socket to bind too
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
        // Create a new shoal connection manager
        let manager = ShoalConnectionManager::new(
            addr,
            proxy_tx,
            &peer_max_frame_bytes,
            S::SCHEMA_FINGERPRINT,
        );
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
        let (tx, rx) = match self.channel_queue_rx.try_recv()? {
            Some((tx, rx)) => (tx, rx),
            None => kanal::unbounded_async(),
        };
        // keep trying new query ids until we don't hit a collision
        loop {
            // check if this id already exists in our channel map
            if self.channel_map.pin().get(&*query_id).is_none() {
                // insert this id
                self.channel_map.pin().insert(*query_id, tx.clone());
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

struct TcpProxy {
    /// The reader to read messages from the shoal server from
    reader: OwnedReadHalf,
    /// A map of channels to send messages to stream readers on
    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
    /// Whether shoal or the client is shutting down
    is_shutting_down: Arc<AtomicBool>,
    /// The largest frame this client will read before it refuses the connection
    max_frame_bytes: u32,
}

impl TcpProxy {
    /// Create a new tcp proxy
    pub fn new(
        reader: OwnedReadHalf,
        channel_map: &Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
        is_shutting_down: &Arc<AtomicBool>,
    ) -> Self {
        // Create a new tcp proxy
        TcpProxy {
            reader,
            channel_map: channel_map.clone(),
            is_shutting_down: is_shutting_down.clone(),
            max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        }
    }

    /// Read a single response frame off of this connection
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
    /// **The length is checked against our own frame bound before the allocation happens**, not
    /// after. A decoder that returned the length and left the check to the caller would be one
    /// forgotten call site away from letting a peer name its own allocation size.
    async fn read_frame(&mut self) -> Result<Option<(Uuid, AlignedVec<16>)>, Errors> {
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
        // check the header and pull the routing fields out of the preamble
        let frame = protocol::decode_response(&preamble, self.max_frame_bytes)?;
        // Create an aligned vec to act as a pool of bytes
        let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.payload_len);
        // resize our aligned vec
        aligned_buff.resize(frame.payload_len, 0);
        self.reader.read_exact(&mut aligned_buff).await?;
        Ok(Some((frame.query_id, aligned_buff)))
    }

    /// Start relaying messages from this tcp stream
    pub async fn start(mut self) -> Result<(), Errors> {
        // keep reading from our tcp socket
        loop {
            // read the next response frame, or stop if this connection is shutting down
            //
            // a HelloAck can never reach here, because the handshake completes before this
            // connections read half is handed to the proxy. moving it after the split would
            // send the ack down this path, where it would decode as a response with a garbage
            // query id and fall into the missing channel arm below
            let (query_id, aligned_buff) = match self.read_frame().await? {
                Some(frame) => frame,
                None => return Ok(()),
            };
            // note when this responses last byte arrived
            //
            // the servers own record ends when it hands these bytes to its socket, so this
            // is what closes the loop on the wire time between the two
            let stamps = ClientStamps::arrived_now();
            // remember how big this payload was before we hand it off
            let len = aligned_buff.len();
            // wrap our response in a client message
            let wrapped = ClientMsg::Response(aligned_buff, stamps);
            // get the channel for this query
            match self.channel_map.pin_owned().get(&query_id) {
                // send our response to the right shoal stream
                Some(tx) => tx.send(wrapped).await?,
                None => {
                    return Err(Errors::ProtocolError(format!(
                        "missing stream channel for query {query_id} (len={len})"
                    )));
                }
            }
        }
    }
}

struct ShoalTcpProxy<S: ShoalQuerySupport, R: ShoalResponseSupport> {
    /// The channel to listen for new tcp readers on
    proxy_rx: AsyncReceiver<OwnedReadHalf>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
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
        proxy_rx: AsyncReceiver<OwnedReadHalf>,
        channel_map: &Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
        is_shutting_down: &Arc<AtomicBool>,
    ) -> Self {
        // create our proxy
        ShoalTcpProxy {
            proxy_rx,
            channel_map: channel_map.clone(),
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
            let reader = match self.proxy_rx.recv().await {
                Ok(reader) => reader,
                Err(_) => return,
            };
            // build a new tcp proxy
            let tcp_proxy = TcpProxy::new(reader, &self.channel_map, &self.is_shutting_down);
            // spawn a task to watch this tcp reader for results
            tokio::task::spawn(tcp_proxy.start());
        }
    }
}

/// Allow types to be retrieved from a [`ShoalStream`]
pub trait FromShoal<S: QuerySupport>: Sized + Archive {
    /// The response kinds to deserialize from
    type ResponseKinds: std::fmt::Debug;

    /// Retrieve a type from a [`ShoalStream`]
    ///
    /// # Arguments
    ///
    /// * `kind` - The response kind to try to cast
    fn retrieve(
        archived: &<S::ResponseKinds as Archive>::Archived,
    ) -> Result<&ArchivedOption<ArchivedVec<<Self as Archive>::Archived>>, Errors>;
}

/// The options for determining if a query suceeded or not
///
/// This will default to requiring every kind of query to succeed
#[derive(Debug, Archive, Clone, Copy)]
pub struct QuerySuceededOpts {
    /// Whether to check if inserts actually inserted data
    pub insert: bool,
    /// Whether to check if updates actually inserted data
    pub update: bool,
    /// Whether to check if gets actually goted data
    pub get: bool,
    /// Whether to check if deletes actually deleted data
    pub delete: bool,
    /// Whether to check if exists actually found data
    pub exists: bool,
}

impl Default for QuerySuceededOpts {
    /// Default to requiring all queries to have actaully inserted data
    fn default() -> Self {
        QuerySuceededOpts {
            insert: true,
            update: true,
            get: true,
            delete: true,
            exists: true,
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
    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
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
                            ClientMsg::End(_) => return Ok((true, None)),
                        };
                    }
                }
            }
            // get the next response from our query
            let msg = response_rx.recv().await?;
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
            let (end, resp) = self.wait_for_next_response().await?;
            // if this is the final response then return our channels
            if end {
                // remove this stream id from our channel map
                self.channel_map.pin().remove(&self.id);
                // take the ends of our channel
                if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
                    self.channel_queue_tx.send((tx, rx)).await?;
                }
            }
            // return our accessable response
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
    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
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
            match response_rx.recv().await? {
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
            let (end, resp) = self.wait_for_next_response().await?;
            // if this is the final response then return our channels
            if end {
                // remove this stream id from our channel map
                self.channel_map.pin().remove(&self.id);
                // take the ends of our channel
                if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
                    self.channel_queue_tx.send((tx, rx)).await?;
                }
            }
            // return our accessable response
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
        // increment the number of queries sent and our base index
        self.queries_sent += 1;
        self.base_index += queries.queries.len();
        Ok(stamps)
    }

    /// Close this query stream
    pub async fn close(self) -> Result<(), Errors> {
        self.response_tx
            .send(ClientMsg::End(self.base_index))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{protocol, TcpProxy};
    use papaya::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use uuid::Uuid;

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
            let is_shutting_down = Arc::new(AtomicBool::new(false));
            let mut proxy = TcpProxy::new(reader, &channel_map, &is_shutting_down);
            let (read_id, buff) = proxy
                .read_frame()
                .await
                .expect("failed to read a frame")
                .expect("the connection closed instead of yielding a frame");
            server.await.expect("the writer task panicked");
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
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let mut proxy = TcpProxy::new(reader, &channel_map, &is_shutting_down);
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
}
