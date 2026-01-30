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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub mod errors;
mod messages;

use super::shared::queries::Queries;
use crate::shared::responses::{ArchivedResponseAction, ResponseActionNames};
use crate::shared::traits::{
    ExistsQuery, QuerySupport, RkyvSupport, ShoalQuerySupport, ShoalResponseSupport,
};
pub use errors::{Errors, ShqlParseError};
use messages::ClientMsg;

// Connection manager for bb8
#[derive(Clone)]
struct ShoalConnectionManager {
    /// The shoal server to connect too
    server_addr: SocketAddr,
    /// The channel to send our read halves to our proxy over
    proxy_tx: AsyncSender<OwnedReadHalf>,
}

impl ShoalConnectionManager {
    /// Create a new shoal connection manager
    pub fn new(server_addr: SocketAddr, proxy_tx: AsyncSender<OwnedReadHalf>) -> Self {
        ShoalConnectionManager {
            server_addr,
            proxy_tx,
        }
    }
}

#[async_trait::async_trait]
impl ManageConnection for ShoalConnectionManager {
    type Connection = OwnedWriteHalf;
    type Error = std::io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = TcpStream::connect(&self.server_addr).await?;
        // Disable Nagle's algorithm
        stream.set_nodelay(true)?;
        // split our stream into read and write halves
        let (tcp_rx, tcp_tx) = stream.into_split();
        // send the read half to our tcp proxy
        self.proxy_tx.send(tcp_rx).await.unwrap();
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
        let addr = tokio::net::lookup_host(addr).await.unwrap().next().unwrap();
        // create a channel for our connection pool and our tcp proxy
        let (proxy_tx, proxy_rx) = kanal::unbounded_async();
        // Create a new shoal connection manager
        let manager = ShoalConnectionManager::new(addr, proxy_tx);
        // build our connection pool
        let pool = bb8::Pool::builder()
            .min_idle(10)
            .max_size(50)
            .connection_timeout(std::time::Duration::from_secs(5))
            .idle_timeout(Some(std::time::Duration::from_secs(300)))
            .max_lifetime(Some(std::time::Duration::from_secs(1800)))
            .build(manager)
            .await
            .unwrap();
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
        // get the size of the archive we are sending to the client
        let len = archived.len().to_le_bytes();
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut queries.id)?;
        // get a connection from our connection pool and send our query
        let mut conn = self.pool.get().await.unwrap();
        // build our vectored byte slices to send
        let mut bufs = &mut [IoSlice::new(&len), IoSlice::new(&archived)][..];
        // keep sending our data until all of this archive has been sent
        while !bufs.is_empty() {
            // send this data back to our client
            match conn.write_vectored(bufs).await {
                Ok(0) => panic!("No bytes were written?"),
                Ok(n) => IoSlice::advance_slices(&mut bufs, n),
                Err(error) => panic!("Ahhh error?: {error:#?}"),
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
        }
    }
    /// Start relaying messages from this tcp stream
    pub async fn start(mut self) {
        // keep reading from our tcp socket
        loop {
            // have a buffer for our query_id and for our length
            let mut preamble: [u8; 24] = [0; 24];
            // try to read from our tcp socket
            if let Err(error) = self.reader.read_exact(&mut preamble).await {
                // if we are shutting down then ignore any EOF errors
                if self.is_shutting_down.load(Ordering::Relaxed) {
                    // check if this was an EOF error
                    if error.kind() == ErrorKind::UnexpectedEof {
                        break;
                    }
                } else {
                    // we are not shutting down so raise this EOF error
                    panic!("Error: {error:#?}");
                }
            }
            // convert our uuid from bytes
            let query_id = Uuid::from_slice(&preamble[..16]).unwrap();
            // get a fixed size slice for our u64
            let u64_bytes = preamble[16..24].try_into().unwrap();
            // parse our message length from our length bytes
            let len = u64::from_le_bytes(u64_bytes) as usize;
            // Create an aligned vec to act as a pool of bytes
            let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
            // resize our aligned vec
            aligned_buff.resize(len, 0);
            self.reader.read_exact(&mut aligned_buff).await.unwrap();
            // wrap our response in a client message
            let wrapped = ClientMsg::Response(aligned_buff);
            // get the channel for this query
            match self.channel_map.pin_owned().get(&query_id) {
                // send our response to the right shoal stream
                Some(tx) => tx.send(wrapped).await.unwrap(),
                None => panic!("Missing stream channel! -> {query_id} {len}"),
            }
        }
    }
}

//pub struct TcpProxyHelper<R: ShoalResponseSupport> {
//    reader: OwnedReadHalf,
//    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
//    phantom: PhantomData<R>,
//}
//
//impl<R: ShoalResponseSupport> TcpProxyHelper<R> {
//async fn tcp_proxy_helper(
//    mut reader: OwnedReadHalf,
//    channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,
//)
////where
////    for<'a> <R as Archive>::Archived: rkyv::bytecheck::CheckBytes<
////        Strategy<
////            rkyv::validation::Validator<
////                rkyv::validation::archive::ArchiveValidator<'a>,
////                rkyv::validation::shared::SharedValidator,
////            >,
////            rkyv::rancor::Error,
////        >,
////    >,
//{
//    // keep reading from our tcp socket
//    loop {
//        // have a buffer for our query_id and for our length
//        let mut preamble: [u8; 24] = [0; 24];
//        // try to read from our tcp socket
//        if let Err(error) = reader.read_exact(&mut preamble).await {
//            // check if this was an EOF error
//            if error.kind() == ErrorKind::UnexpectedEof {
//                break;
//            }
//        }
//        // convert our uuid from bytes
//        let query_id = Uuid::from_slice(&preamble[..16]).unwrap();
//        // get a fixed size slice for our u64
//        let u64_bytes = preamble[16..24].try_into().unwrap();
//        // parse our message length from our length bytes
//        let len = u64::from_le_bytes(u64_bytes) as usize;
//        // Create an aligned vec to act as a pool of bytes
//        let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
//        // resize our aligned vec
//        aligned_buff.resize(len, 0);
//        reader.read_exact(&mut aligned_buff).await.unwrap();
//        // wrap our response in a client message
//        let wrapped = ClientMsg::Response(aligned_buff);
//        // get the channel for this query
//        match channel_map.pin_owned().get(&query_id) {
//            // send our response to the right shoal stream
//            Some(tx) => tx.send(wrapped).await.unwrap(),
//            None => panic!("Missing stream channel! -> {query_id} {len}"),
//        }
//    }
//}
//}

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
            let reader = self.proxy_rx.recv().await.unwrap();
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
    pub(super) fn new(buff: AlignedVec) -> Self
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // access our response
        let archived = S::ResponseKinds::access(&buff).unwrap();
        // This is safe as we own our backing aligned vec
        let const_archived = archived as *const _;
        ShoalResponse {
            _buff: buff,
            archived: const_archived,
            phantom: PhantomData,
        }
    }

    /// Get the inner aligned vec
    pub(super) fn inner(self) -> AlignedVec {
        self._buff
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
                            ClientMsg::Response(response) => {
                                // wrap our response so we don't have to keep repaying access costs
                                let response = ShoalResponse::<S>::new(response);
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
                ClientMsg::Response(response) => {
                    // wrap our response so we don't have to keep repaying access costs
                    let response = ShoalResponse::<S>::new(response);
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
                    let rewrapped = ClientMsg::Response(response.inner());
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
                ClientMsg::Response(archived) => {
                    // wrap our response so we don't have to keep repaying access costs
                    let response = ShoalResponse::<S>::new(archived);
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
    pub async fn send(&mut self, mut queries: Queries<Q>) -> Result<(), Errors> {
        // override our query id
        // TODO make it so we don't need to do this
        queries.id = self.id;
        // update the base index for this query bundle correctly
        queries.base_index = self.base_index;
        // archive our queries
        let archived = rkyv::to_bytes::<_>(&queries)?;
        // get the size of the archive we are sending to the client
        let len = archived.len().to_le_bytes();
        // get a connection from our connection pool and send our query
        let mut conn = self.pool.get().await.unwrap();
        // build our vectored byte slices to send
        let mut bufs = &mut [IoSlice::new(&len), IoSlice::new(&archived)][..];
        // keep sending our data until all of this archive has been sent
        while !bufs.is_empty() {
            // send this data back to our client
            match conn.write_vectored(bufs).await {
                Ok(0) => panic!("No bytes were written?"),
                Ok(n) => IoSlice::advance_slices(&mut bufs, n),
                Err(error) => panic!("Ahhh error?: {error:#?}"),
            }
        }
        // increment the number of queries sent and our base index
        self.queries_sent += 1;
        self.base_index += queries.queries.len();
        Ok(())
    }

    /// Close this query stream
    pub async fn close(self) -> Result<(), Errors> {
        self.response_tx
            .send(ClientMsg::End(self.base_index))
            .await?;
        Ok(())
    }
}
