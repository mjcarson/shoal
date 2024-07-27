//! The client for a Shoal database

use bytes::BytesMut;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec,
};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub mod errors;

use super::shared::queries::Queries;
use crate::shared::traits::{ShoalDatabase, ShoalQuery, ShoalResponse};
pub use errors::Errors;

pub struct Shoal<S: ShoalDatabase> {
    /// The udp socket to send and receive messages on
    socket: Arc<UdpSocket>,
    /// A concurrent map of what channel to send streaming results too
    pub channel_map: Arc<DashMap<Uuid, AsyncSender<BytesMut>>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<BytesMut>, AsyncReceiver<BytesMut>)>,
    /// A channel of channels to send streaming results over
    channel_queue_rx: AsyncReceiver<(AsyncSender<BytesMut>, AsyncReceiver<BytesMut>)>,
    /// The handle to this clients proxy
    proxy_handle: JoinHandle<()>,
    /// The database kind we are querying
    phantom: PhantomData<S>,
}

impl<S: ShoalDatabase + Send> Shoal<S> {
    /// Create a new shoal client
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket to bind too
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        // bind to our addr twice
        let socket = Arc::new(UdpSocket::bind(&addr).await.unwrap());
        // build a channel for sending and recieving response streams on
        let (channel_queue_tx, channel_queue_rx) = kanal::bounded_async(8192);
        // create a map for storing what channels to send response streams on
        let channel_map = Arc::new(DashMap::with_capacity(100));
        // create the response proxy for this client
        let proxy = ShoalUdpProxy::<S>::new(socket.clone(), channel_map.clone());
        // start our proxy
        let proxy_handle = tokio::spawn(async move { proxy.start().await });
        // build our client
        Shoal {
            socket,
            channel_map,
            channel_queue_tx,
            channel_queue_rx,
            proxy_handle,
            phantom: PhantomData,
        }
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
    ) -> (AsyncSender<BytesMut>, AsyncReceiver<BytesMut>) {
        // get the next available response channel or create a new one
        let (tx, rx) = match self.channel_queue_rx.try_recv().unwrap() {
            Some((tx, rx)) => (tx, rx),
            None => kanal::unbounded_async(),
        };
        // keep trying new query ids until we don't hit a collision
        loop {
            // try to add this query to our reponse stream
            match self.channel_map.entry(queries.id) {
                Entry::Occupied(_) => queries.id = Uuid::new_v4(),
                Entry::Vacant(entry) => {
                    // add the tx side of our channel
                    entry.insert(tx.clone());
                    // stop generating new ids
                    break;
                }
            }
        }
        // return our channels
        (tx, rx)
    }

    /// Send a query to our server
    pub async fn send(&self, mut queries: Queries<S>) -> ShoalStream<S>
    where
        <S as ShoalDatabase>::QueryKinds: rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<256>, AllocScratch>,
                SharedSerializeMap,
            >,
        >,
    {
        // archive our queries
        let archived = rkyv::to_bytes::<_, 256>(&queries).unwrap();
        // start tracking this response
        let (response_tx, response_rx) = self.track_response(&mut queries);
        // send our query
        self.socket
            .send_to(archived.as_slice(), "127.0.0.1:12000")
            .await
            .unwrap();
        // build a new shoal stream
        ShoalStream {
            id: queries.id,
            response_tx: Some(response_tx),
            response_rx: Some(response_rx),
            channel_map: self.channel_map.clone(),
            channel_queue_tx: self.channel_queue_tx.clone(),
            next_index: 0,
            pending: BTreeMap::default(),
            phantom: PhantomData,
        }
    }
}

impl<S: ShoalDatabase> Drop for Shoal<S> {
    fn drop(&mut self) {
        // stop our proxy
        self.proxy_handle.abort();
    }
}

struct ShoalUdpProxy<S: ShoalDatabase> {
    /// A pool of memory to use
    pool: BytesMut,
    /// The udp socket to listen on
    socket: Arc<UdpSocket>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<DashMap<Uuid, AsyncSender<BytesMut>>>,
    /// The database we are getting responses from
    phantom: PhantomData<S>,
}

impl<S: ShoalDatabase> ShoalUdpProxy<S> {
    /// Create a new udp proxy
    ///
    /// # Arguments
    ///
    /// * `socket` - The socket to listen on
    /// * `channel_map` - A distributed map of channels to relay messages with
    /// * `shutdown` - A flag used to tell the proxy to shutdown
    pub fn new(
        socket: Arc<UdpSocket>,
        channel_map: Arc<DashMap<Uuid, AsyncSender<BytesMut>>>,
    ) -> Self {
        // Init our pool of bytes
        let pool = BytesMut::zeroed(8192);
        // create our proxy
        ShoalUdpProxy {
            pool,
            socket,
            channel_map,
            phantom: PhantomData,
        }
    }

    /// Continuously proxy responses from shoal to the correct client channel
    pub async fn start(mut self) {
        // Wait for new messages from our server and proxy the respones to the right channel
        loop {
            if self.pool.len() < 256 {
                // try to extend our pool to reclaim storage
                self.pool.reserve(4192);
                // zero out our pool
                self.pool.extend((0..4192).map(|_| 0));
            }
            // try to read a single datagram from our udp socket
            let (read, _) = self.socket.recv_from(&mut self.pool).await.unwrap();
            // split the bytes we read into from our bytes pool
            let data = self.pool.split_to(read);
            // try to deserialize our responses query id
            let id = S::QueryKinds::response_query_id(&data[..]);
            //let id = S::response_query_id(&data[..]);
            // get the channel for this query
            match self.channel_map.get(id) {
                // send our response to the right shoal stream
                Some(tx) => tx.send(data).await.unwrap(),
                None => panic!("Missing stream channel!"),
            }
        }
    }
}

/// Allow types to be retrieved from a [`ShoalStream`]
pub trait FromShoal<S: ShoalDatabase>: Sized {
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
pub struct ShoalStream<S: ShoalDatabase> {
    /// This channels id
    id: Uuid,
    /// The transmission side of the response stream channel
    response_tx: Option<AsyncSender<BytesMut>>,
    /// the receive side of the response stream channel
    response_rx: Option<AsyncReceiver<BytesMut>>,
    /// A concurrent map of what channel to send streaming results too
    channel_map: Arc<DashMap<Uuid, AsyncSender<BytesMut>>>,
    /// The channel to add unused response streams too
    channel_queue_tx: AsyncSender<(AsyncSender<BytesMut>, AsyncReceiver<BytesMut>)>,
    /// The next message to be returned
    next_index: usize,
    /// The messages that are to be returned later to ensure the correct order of receipt
    pending: BTreeMap<usize, S::ResponseKinds>,
    /// The database kind we are streaming response for
    phantom: PhantomData<S>,
}

impl<S: ShoalDatabase> ShoalStream<S> {
    async fn wait_for_next_response(&mut self) -> (bool, S::ResponseKinds) {
        // get our response channel if one exists or
        let response_rx = match self.response_rx.as_mut() {
            Some(response_rx) => response_rx,
            None => panic!("NO CALL STREAM NEXT AFTER END!"),
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
                        return (end, resp);
                    }
                }
            }
            // get the next response from our query
            let buff = response_rx.recv().await.unwrap();
            // unarchive our response
            let resp = S::unarchive_response(&buff);
            // get this responses order index
            let index = resp.get_index();
            // if this is the next row then return it
            if self.next_index == index {
                // increment the index of our next response
                self.next_index += 1;
                // check if this response is the last one in this stream
                let end = resp.is_end_of_stream();
                // this is the next response so just return it
                return (end, resp);
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
    pub async fn skip(&mut self, mut skip: usize) {
        // get the next message and throw it away
        while self.next().await.is_some() {
            // decrement our skip
            skip -= 1;
            // if skip is 0 then we can return
            if skip == 0 {
                break;
            }
        }
    }

    /// Get the next response to our query
    pub async fn next(&mut self) -> Option<S::ResponseKinds> {
        // try to get our receive channels
        if self.response_rx.is_some() {
            // wait for the next response
            let (end, resp) = self.wait_for_next_response().await;
            // if this is the final response then return our channels
            if end {
                // remove this stream id from our channel map
                self.channel_map.remove(&self.id);
                // take the ends of our channel
                if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
                    self.channel_queue_tx.send((tx, rx)).await.unwrap();
                }
            }
            // return our response
            Some(resp)
        } else {
            // this stream has already ended
            None
        }
    }

    /// Get the next response to our query and cast it to a specific type
    pub async fn next_typed<T: FromShoal<S>>(&mut self) -> Result<Option<Option<Vec<T>>>, Errors> {
        // try to get our receive channels
        if self.response_rx.is_some() {
            // wait for the next response
            let (end, resp) = self.wait_for_next_response().await;
            // if this is the final response then take return our channels
            if end {
                // remove this stream id from our channel map
                self.channel_map.remove(&self.id);
                // take the ends of our channel
                if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
                    self.channel_queue_tx.send((tx, rx)).await.unwrap();
                }
            }
            // try to cast to the correct type
            T::retrieve(resp).map(Some)
        } else {
            // this stream has already ended
            Ok(None)
        }
    }

    /// Get the next response to our query and get the first row returned and cast it to our specific type
    ///
    /// This will ignore any remaining rows in the next response.
    pub async fn next_typed_first<T: FromShoal<S>>(&mut self) -> Result<Option<Option<T>>, Errors> {
        // try to get the next response
        match self.next_typed().await? {
            Some(Some(mut rows)) => {
                // check how may rows we found
                if rows.len() == 1 {
                    // if we only have a single row then just remove it
                    Ok(Some(Some(rows.remove(0))))
                } else {
                    // we have more then 1 row so do a swap remove to avoid moving the items in the vec forward
                    Ok(Some(Some(rows.swap_remove(1))))
                }
            }
            Some(None) => Ok(Some(None)),
            None => Ok(None),
        }
    }
}

//pub fn unarchive<'a, S: ShoalDatabase>(buff: &'a [u8])
//where
//    <<S as ShoalDatabase>::ResponseKinds as Archive>::Archived:
//        rkyv::CheckBytes<DefaultValidator<'a>>,
//    <<S as ShoalDatabase>::ResponseKinds as Archive>::Archived: std::fmt::Debug,
//{
//    // try to cast this query
//    let raw = rkyv::check_archived_root::<S::ResponseKinds>(buff).unwrap();
//    // deserialize it
//    println!("raw response -> {:#?}", raw);
//}
//
//pub struct ShoalStream<S: ShoalDatabase> {
//    ///// The buffer to deserialize results with
//    //buffer: BytesMut,
//    /// The database we are getting responses from
//    db_kind: PhantomData<S>,
//}
//
//impl<S: ShoalDatabase> ShoalStream<S> {
//    /// Create a new response object
//    pub fn new() -> Self {
//        Self {
//            //buffer,
//            db_kind: PhantomData,
//        }
//    }
//
//    /// cast our response
//    pub fn read<'a>(&self, read: usize, data: BytesMut)
//    where
//        <<S as ShoalDatabase>::ResponseKinds as Archive>::Archived:
//            rkyv::CheckBytes<DefaultValidator<'a>>,
//        <<S as ShoalDatabase>::ResponseKinds as Archive>::Archived: std::fmt::Debug,
//    {
//        // get a ref to our readable data
//        let readable = &data[..read];
//        // unarchive our data
//        //unarchive::<S>(readable);
//        //// try to cast this query
//        //let raw = rkyv::check_archived_root::<S::ResponseKinds>(&self.buffer).unwrap();
//        //// deserialize it
//        //println!("raw response -> {:#?}", raw);
//        //let response: S::ResponseKinds = raw.deserialize(&mut rkyv::Infallible).unwrap();
//    }
//}
//
