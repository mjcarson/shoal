//! The client for a Shoal database

use bytes::BytesMut;
use dashmap::mapref::entry::Entry;
//use dashmap::DashMap;
use kanal::{AsyncReceiver, AsyncSender};
use papaya::HashMap;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::util::AlignedVec;
use rkyv::Archive;
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub mod errors;

use super::shared::queries::Queries;
use crate::shared::traits::{QuerySupport, RkyvSupport, ShoalQuery, ShoalResponse};
pub use errors::Errors;

pub struct Shoal<S: QuerySupport> {
    /// The udp socket to send and receive messages on
    socket: Arc<UdpSocket>,
    /// A concurrent map of what channel to send streaming results too
    pub channel_map: Arc<HashMap<Uuid, AsyncSender<AlignedVec>>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<AlignedVec>, AsyncReceiver<AlignedVec>)>,
    /// A channel of channels to send streaming results over
    channel_queue_rx: AsyncReceiver<(AsyncSender<AlignedVec>, AsyncReceiver<AlignedVec>)>,
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
        // bind to our addr twice
        let socket = Arc::new(UdpSocket::bind(&addr).await?);
        // build a channel for sending and recieving response streams on
        let (channel_queue_tx, channel_queue_rx) = kanal::bounded_async(8192);
        // create a map for storing what channels to send response streams on
        let channel_map = Arc::new(HashMap::with_capacity(1024));
        // create the response proxy for this client
        let proxy = ShoalUdpProxy::<S::QueryKinds, S::ResponseKinds>::new(
            socket.clone(),
            channel_map.clone(),
        );
        // start our proxy
        let proxy_handle = tokio::spawn(async move { proxy.start().await });
        // build our client
        let shoal = Shoal {
            socket,
            channel_map,
            channel_queue_tx,
            channel_queue_rx,
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
        queries: &mut Queries<S>,
    ) -> Result<(AsyncSender<AlignedVec>, AsyncReceiver<AlignedVec>), Errors> {
        // get the next available response channel or create a new one
        let (tx, rx) = match self.channel_queue_rx.try_recv()? {
            Some((tx, rx)) => (tx, rx),
            None => kanal::unbounded_async(),
        };
        // keep trying new query ids until we don't hit a collision
        loop {
            // check if this id already exists in our channel map
            if self.channel_map.pin().get(&queries.id).is_none() {
                // insert this id
                self.channel_map.pin().insert(queries.id, tx.clone());
                // we found a unique query id so stop trying to find a new id
                break;
            }
            // try to generate a unique query id
            queries.id = Uuid::new_v4();
        }
        // return our channels
        Ok((tx, rx))
    }

    /// Send a query to our server
    pub async fn send(&self, mut queries: Queries<S>) -> Result<ShoalStream<S>, Errors> {
        // archive our queries
        let archived = rkyv::to_bytes::<_>(&queries)?;
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut queries)?;
        // send our query
        self.socket
            .send_to(archived.as_slice(), "127.0.0.1:12000")
            .await?;
        // build a new shoal stream
        let stream = ShoalStream {
            id: queries.id,
            response_tx: Some(response_tx),
            response_rx: Some(response_rx),
            channel_map: self.channel_map.clone(),
            channel_queue_tx: self.channel_queue_tx.clone(),
            next_index: 0,
            pending: BTreeMap::default(),
            phantom: PhantomData,
        };
        Ok(stream)
    }
}

impl<S: QuerySupport> Drop for Shoal<S> {
    fn drop(&mut self) {
        // stop our proxy
        self.proxy_handle.abort();
    }
}

struct ShoalUdpProxy<S: ShoalQuery, R: ShoalResponse> {
    /// The udp socket to listen on
    socket: Arc<UdpSocket>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, AsyncSender<AlignedVec>>>,
    /// The database we are getting responses from
    phantom_query: PhantomData<S>,
    /// The database we are getting responses from
    phantom_response: PhantomData<R>,
}

impl<S: ShoalQuery, R: ShoalResponse> ShoalUdpProxy<S, R> {
    /// Create a new udp proxy
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket to listen on
    /// * `channel_map` - A distributed map of channels to relay messages with
    /// * `shutdown` - A flag used to tell the proxy to shutdown
    pub fn new(
        socket: Arc<UdpSocket>,
        channel_map: Arc<HashMap<Uuid, AsyncSender<AlignedVec>>>,
    ) -> Self {
        // create our proxy
        ShoalUdpProxy {
            socket,
            channel_map,
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
        // Wait for new messages from our server and proxy the respones to the right channel
        loop {
            // Create an aligned vec to act as a pool of bytes
            let mut aligned_buff = AlignedVec::with_capacity(4096);
            // resize our aligned vec
            aligned_buff.resize(4096, 0);
            // try to read a single datagram from our udp socket
            let (read, _) = self.socket.recv_from(&mut aligned_buff).await.unwrap();
            // shrink our aligned vec to just the data read
            aligned_buff.resize(read, 0);
            // get this responses id
            let id = match R::access(&aligned_buff) {
                Ok(archived) => R::get_query_id(archived),
                Err(error) => panic!("SPEC FAIL -> {error:#?}"),
            };
            // get the channel for this query
            match self.channel_map.pin_owned().get(&id) {
                // send our response to the right shoal stream
                Some(tx) => tx.send(aligned_buff).await.unwrap(),
                None => panic!("Missing stream channel! -> {id} {read}"),
            }
        }
    }
}

/// Allow types to be retrieved from a [`ShoalStream`]
pub trait FromShoal<S: QuerySupport>: Sized {
    /// The response kinds to deserialize from
    type ResponseKinds: std::fmt::Debug;

    /// Retrieve a type from a [`ShoalStream`]
    ///
    /// # Arguments
    ///
    /// * `kind` - The response kind to try to cast
    fn retrieve(kind: S::ResponseKinds) -> Result<Option<Vec<Self>>, Errors>;
}

/// The reponses from our queries in a stream
pub struct ShoalStream<S: QuerySupport> {
    /// This channels id
    id: Uuid,
    /// The transmission side of the response stream channel
    response_tx: Option<AsyncSender<AlignedVec>>,
    /// the receive side of the response stream channel
    response_rx: Option<AsyncReceiver<AlignedVec>>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<HashMap<Uuid, AsyncSender<AlignedVec>>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<AlignedVec>, AsyncReceiver<AlignedVec>)>,
    /// The next message to be returned
    next_index: usize,
    /// The messages that are to be returned later to ensure the correct order of receipt
    pending: BTreeMap<usize, S::ResponseKinds>,
    /// The database kind we are streaming response for
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> ShoalStream<S>
where
    <S::ResponseKinds as Archive>::Archived:
        rkyv::Deserialize<S::ResponseKinds, Strategy<Pool, rkyv::rancor::Error>>,
{
    async fn wait_for_next_response(&mut self) -> Result<(bool, S::ResponseKinds), Errors>
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
                    if let Some((_, resp)) = self.pending.pop_first() {
                        // increment our index
                        self.next_index += 1;
                        // check if this response is the last one in this stream
                        let end = resp.is_end_of_stream();
                        return Ok((end, resp));
                    }
                }
            }
            // get the next response from our query
            let buff = response_rx.recv().await?;
            // unarchive our response'
            let archived = S::ResponseKinds::access(&buff)?;
            // deserialize our response
            // TODO do we have to do this?
            let resp = S::ResponseKinds::deserialize(archived)?;
            // get this responses order index
            let index = resp.get_index();
            // if this is the next row then return it
            if self.next_index == index {
                // increment the index of our next response
                self.next_index += 1;
                // check if this response is the last one in this stream
                let end = resp.is_end_of_stream();
                // this is the next response so just return it
                return Ok((end, resp));
            }
            // push this into our pending responses and wait for the next response
            self.pending.insert(index, resp);
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
    pub async fn next(&mut self) -> Result<Option<S::ResponseKinds>, Errors>
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
            // return our response
            Ok(Some(resp))
        } else {
            // this stream has already ended
            Ok(None)
        }
    }

    /// Get the next response to our query and cast it to a specific type
    pub async fn next_typed<T: FromShoal<S>>(&mut self) -> Result<Option<Vec<T>>, Errors>
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
            // if this is the final response then take return our channels
            if end {
                // remove this stream id from our channel map
                self.channel_map.pin().remove(&self.id);
                // take the ends of our channel
                if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
                    self.channel_queue_tx.send((tx, rx)).await?;
                }
            }
            // try to cast to the correct type
            Ok(T::retrieve(resp)?)
        } else {
            // this stream has already ended
            Ok(None)
        }
    }

    /// Get the next response to our query and get the first row returned and cast it to our specific type
    ///
    /// This will ignore any remaining rows in the next response.
    pub async fn next_typed_first<T: FromShoal<S>>(&mut self) -> Result<Option<Option<T>>, Errors>
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
        // try to get the next response
        match self.next_typed().await? {
            Some(mut rows) => {
                // check how may rows we found
                if rows.len() == 1 {
                    // if we only have a single row then just remove it
                    Ok(Some(Some(rows.remove(0))))
                } else {
                    // we have more then 1 row so do a swap remove to avoid moving the items in the vec forward
                    Ok(Some(Some(rows.swap_remove(1))))
                }
            }
            //Some(None) => Ok(Some(None)),
            None => Ok(None),
        }
    }
}
