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
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use std::{cell::RefCell, hash::BuildHasherDefault};
use std::{collections::HashMap, io::IoSlice};
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use super::messages::{QueryMetadata, ServerMsg};
use super::ring::Ring;
use super::{Comms, Conf, ServerError};
use crate::{
    shared::{
        queries::Queries,
        traits::{QuerySupport, RkyvSupport, ShoalDatabase, ShoalQuerySupport},
    },
    storage::{FullArchiveMap, LoaderMsg, Loaders},
};

async fn client_rx_relay<S: ShoalDatabase>(
    peer: Uuid,
    mut tcp_rx: ReadHalf<TcpStream>,
    kanal_tx: AsyncSender<ServerMsg<S>>,
) {
    // keep waiting for messages until  our tcp socket closes
    loop {
        // have a buffer for our query_id and for our length
        let mut len_bytes: [u8; 8] = [0; 8];
        // try to read the size of the next message from our tcp socket
        if let Err(error) = tcp_rx.read_exact(&mut len_bytes).await {
            // if this was an unexpected EOF error then assume the client died
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            // TODO do something with this error
            panic!("client_rx_relay: {error:#?}")
        }
        // parse the upcoming messages size
        let len = u64::from_le_bytes(len_bytes) as usize;
        // allocate a buffer that is exactly the right size
        let mut data = BytesMut::zeroed(len);
        // wait for messages from our client
        tcp_rx.read_exact(&mut data).await.unwrap();
        // forward our clients message
        kanal_tx
            .send(ServerMsg::Client { peer, data })
            .await
            .unwrap();
    }
}

async fn client_tx_relay<S: ShoalDatabase>(
    client_rx: AsyncReceiver<(Uuid, Span, AlignedVec)>,
    mut tcp_tx: WriteHalf<TcpStream>,
) {
    // loop over messages to send back to our client
    loop {
        // try to get a message from our channel
        let (query_id, span, archived) = match client_rx.recv().await {
            Ok((query_id, span, archived)) => (query_id, span, archived),
            // if this channel was closed then stop our task
            // this should only happen exit/shutdown or when our client shutsdown
            Err(_) => break,
        };
        // enter our span
        let span_guard = span.enter();
        // get the size of the archive we are sending to the client
        let len = archived.len().to_le_bytes();
        // build our vectored byte slices to send
        let mut bufs = &mut [
            IoSlice::new(query_id.as_bytes()),
            IoSlice::new(&len),
            IoSlice::new(&archived),
        ][..];
        // keep sending our data until all of this archive has been sent
        while !bufs.is_empty() {
            // send this data back to our client
            match tcp_tx.write_vectored(bufs).await {
                Ok(0) => panic!("No bytes were written?"),
                Ok(n) => IoSlice::advance_slices(&mut bufs, n),
                Err(error) => panic!("Ahhh error?: {error:#?}"),
            }
        }
        // drop our span since we are done writting
        drop(span_guard);
    }
}

#[allow(clippy::future_not_send)]
async fn client_acceptor<S: ShoalDatabase>(
    tcp_sock: TcpListener,
    comms: Comms<S>,
    node_local_tx: AsyncSender<ServerMsg<S>>,
) -> Result<(), ServerError> {
    loop {
        // try to read a single datagram from our udp socket
        let stream = tcp_sock.accept().await?;
        // disable nagles algorithm on this socket
        stream.set_nodelay(true)?;
        // generate an id for this peer
        // TODO: detect collisions?
        let client = Uuid::new_v4();
        // break this stream up into a writer and a reader
        let (tcp_rx, tcp_tx) = stream.split();
        // create a channel for all of our shards to give data to send back to clients
        let (client_tx, client_rx) = kanal::unbounded_async();
        // TODO: do this with a task queue?
        glommio::spawn_local(client_rx_relay(client, tcp_rx, node_local_tx.clone())).detach();
        glommio::spawn_local(client_tx_relay::<S>(client_rx, tcp_tx)).detach();
        // build the new client message to broadcast
        let msg = ServerMsg::NewClient { client, client_tx };
        // broadcast this client to all shards on this node
        comms.broadcast(&msg).await?;
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
        // our ids are not 0 indexed so convert them to be 0 indexed
        let fixed = id - 1;
        // build our shard info
        Self {
            name: format!("Shard-{fixed}"),
            contact: ShardContact::Local(fixed),
        }
    }

    /// Get this shards mesh id
    pub fn mesh_id(&self) -> usize {
        match self.contact {
            ShardContact::Local(mesh_id) => mesh_id,
        }
    }
}

pub(super) struct Shard<S: ShoalDatabase> {
    /// This shards info
    info: ShardInfo,
    /// The config for shoal
    conf: Conf,
    /// The token ring info for shoal
    ring: Ring,
    /// Handles communications across shoal shards/nodes
    comms: Comms<S>,
    /// The tables we are responsible for on this shard
    pub tables: S,
    /// The full archive map for all tables
    table_map: FullArchiveMap<S::TableNames>,
    /// A map of channels to send responses to our client relays over
    client_map: HashMap<Uuid, AsyncSender<(Uuid, Span, AlignedVec)>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ServerMsg<S>>,
    /// The channel to Receive shard local messages on
    shard_local_rx: AsyncReceiver<ServerMsg<S>>,
    /// A map of storage systems and their loader channel
    loader_channels: HashMap<
        Loaders,
        (
            AsyncSender<LoaderMsg<S::TableNames>>,
            AsyncReceiver<LoaderMsg<S::TableNames>>,
        ),
    >,
    /// The responses whose queries have been flushed to disk
    flushed: Vec<(
        Uuid,
        Uuid,
        Span,
        <S::ClientType as QuerySupport>::ResponseKinds,
    )>,
    /// The latency sensitive task queue
    high_priority: TaskQueueHandle,
    /// The medium priority task queue
    _medium_priority: TaskQueueHandle,
    /// The tasks we have spawned
    tasks: Vec<Task<Result<(), ServerError>>>,
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(S::TableNames, u64), usize, BuildHasherDefault<GxHasher>>>>,
}

impl<S: ShoalDatabase> Shard<S>
where
    [<<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived]:
        DeserializeUnsized<
            [<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds],
            Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Create a new shard
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind our udp socket too
    #[instrument(name = "Shard::new", skip_all, err(Debug))]
    pub async fn new(conf: &Conf, comms: Comms<S>) -> Result<Self, ServerError> {
        // get a handle to our current executor
        let executor = glommio::executor();
        // build our shard info
        let info = ShardInfo::new(executor.id());
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
        let tables = S::new(
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
            ring: Ring::default(),
            comms,
            tables,
            table_map,
            client_map: HashMap::with_capacity(500),
            shard_local_tx,
            shard_local_rx,
            loader_channels,
            flushed: Vec::with_capacity(1000),
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
            client_acceptor(tcp_sock, self.comms.clone(), node_local_tx),
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
        Ok(())
    }

    /// Forward our queries to the correct shards
    #[instrument(name = "Coordinator::send_to_shard", skip_all)]
    async fn send_to_shard(
        &mut self,
        client: Uuid,
        queries: Queries<S::ClientType>,
    ) -> Result<(), ServerError> {
        // initialize a vec to store the shards we find
        let mut found = Vec::with_capacity(3);
        // get the index for the last query in this bundle
        let end_index = queries.queries.len() - 1;
        // crawl over our queries
        for (mut index, kind) in queries.queries.into_iter().enumerate() {
            // get our target shards info
            kind.find_shard(&self.ring, &mut found);
            // send this query to the right shards
            for shard_info in found.drain(..) {
                // add our base index to this messages index
                index += queries.base_index;
                // check if this is the last query or not
                let end = index == end_index;
                // build the metadata for this query
                let meta = QueryMetadata::new(client, queries.id, index, end);
                // clone our query
                let query = kind.clone();
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
    async fn handle_client<'a>(&mut self, peer: Uuid, data: BytesMut) -> Result<(), ServerError>
    where
        for<'b> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // load our arhived query from buffer
        let archived = Queries::access(&data)?;
        // deserialize our queries
        let queries = <Queries<S::ClientType> as RkyvSupport>::deserialize(archived)?;
        // send each query to the correct shard
        self.send_to_shard(peer, queries).await
    }

    /// Send a respones back to the client
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to send this reply too
    /// * `response` - The response to send
    #[instrument(name = "Shard::reply", parent = &span, skip_all, err(Debug))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn reply(
        &mut self,
        client: Uuid,
        query_id: Uuid,
        span: Span,
        response: <S::ClientType as QuerySupport>::ResponseKinds,
    ) -> Result<(), ServerError> {
        // archive our response
        let archived = rkyv::to_bytes::<_>(&response)?;
        // get this clients channel to send replies over
        match self.client_map.get(&client) {
            Some(client_tx) => client_tx.send((query_id, span, archived)).await?,
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
        meta: QueryMetadata,
        query: <S::ClientType as QuerySupport>::QueryKinds,
    ) -> Result<(), ServerError>
    where
        for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // copy our span for it we reply
        let span = meta.span.clone();
        // try to handle this query
        if let Some((addr, query_id, response)) = self.tables.handle(meta, query).await {
            // send this response back to the client
            self.reply(addr, query_id, span, response).await?;
        }
        Ok(())
    }

    /// Get all flushed messages and send their response back
    #[instrument(name = "Shard::handle_flushed", skip(self))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn handle_flushed(&mut self) -> Result<(), ServerError> {
        // get all flushed query responses
        self.tables.handle_flushed(&mut self.flushed).await?;
        // pop all of our flushed responses
        while let Some((client, query_id, span, response)) = self.flushed.pop() {
            // send our responses
            self.reply(client, query_id, span, response).await?;
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

    #[instrument(name = "Shard::shutdown_tasks", skip(self))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn shutdown_tasks(&mut self) -> Result<(), ServerError> {
        // cancel all of our tasks
        for task in self.tasks.drain(..) {
            // cancel this task
            if let Some(Err(error)) = task.cancel().await {
                // log this tasks error if it had one
                event!(Level::ERROR, error = format!("{error:#?}"));
            }
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
        for<'b> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
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
                ServerMsg::Client { peer, data } => self.handle_client(peer, data).await?,
                // handle this query from the user
                ServerMsg::Query { meta, query } => self.handle_query(meta, query).await?,
                // load this partition from disk
                ServerMsg::Partition(loaded) => {
                    self.tables
                        .load_partition(loaded, &self.shard_local_tx)
                        .await?
                }
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
                    break;
                }
            }
            // if we have no more messages then flush our current queries to disk
            if self.shard_local_rx.is_empty() {
                self.tables.flush().await?;
            }
            // check for any flushed response to handle
            self.handle_flushed().await?;
            // check if we need to evict any data
            if *self.memory_usage.borrow() > self.conf.resources.memory {
                // try to evict our least recently used data
                self.evict_data().await?;
            }
        }
        // check for any flushed response to handle
        self.handle_flushed().await?;
        // shudown
        self.tables.shutdown().await?;
        // shutdown all of our tasks
        self.shutdown_tasks().await?;
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
    // build our comms object for this nodes shards
    // we will have one shard per core
    let comms = Comms::<S>::with_capacity(16);
    // An atomic bool used to signal that shards should exit
    let should_shutdown = Arc::new(AtomicBool::new(false));
    // setup our executor
    let executor_builder =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(cpus.len(), Some(cpus)));
    // build and spawn our shards on all of remaining available cores
    let shards = executor_builder.on_all_shards(enclose!((comms, should_shutdown) move || {
        async move {
            // build an empty shard
            let shard: Shard<S> = Shard::new(&conf, comms).await?;
            // start this shard
            shard.start(should_shutdown.clone()).await
        }
    }))?;
    Ok((shards, should_shutdown))
}
