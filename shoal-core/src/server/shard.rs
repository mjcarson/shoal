//! A single shard in Shoal

use bytes::Bytes;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder, Receivers, Senders},
    enclose,
    net::UdpSocket,
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, PoolThreadHandles, Shares, Task,
    TaskQueueHandle,
};
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    util::AlignedVec,
    validation::{archive::ArchiveValidator, shared::SharedValidator, Validator},
    Archive,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::{cell::RefCell, hash::BuildHasherDefault};
use std::{net::SocketAddr, time::Duration};
use tokio::time::Instant;
use tracing::instrument;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use super::{messages::QueryMetadata, ServerError};
use super::{
    messages::{MeshMsg, ShardMsg},
    Conf,
};
use crate::{
    shared::{
        responses::ArchivedResponses,
        traits::{QuerySupport, RkyvSupport, ShoalDatabase},
    },
    storage::{FullArchiveMap, LoaderMsg, Loaders},
};

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
    /// * `mesh_id` - This shards mesh peer id
    #[must_use]
    pub fn new(mesh_id: usize) -> Self {
        // build our shard info
        Self {
            name: format!("Shard-{mesh_id}"),
            contact: ShardContact::Local(mesh_id),
        }
    }
}

pub struct MeshRelay<S: ShoalDatabase> {
    /// The glommio channel to receive messages on
    mesh_rx: Receivers<MeshMsg<S>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ShardMsg<S>>,
}

impl<S: ShoalDatabase> MeshRelay<S> {
    /// Create a new mesh relay
    ///
    /// # Arguments
    ///
    /// * `mesh_rx` - The glommio channel to receive mesh messages on
    /// * `node_local_tx` - The kanal channel to send shard local messages on
    pub fn new(mesh_rx: Receivers<MeshMsg<S>>, shard_local_tx: &AsyncSender<ShardMsg<S>>) -> Self {
        MeshRelay {
            mesh_rx,
            shard_local_tx: shard_local_tx.clone(),
        }
    }

    /// Start relaying messages from this nodes mesh to our shard
    pub async fn start(self) -> Result<(), ServerError> {
        // wait for a message on our mesh
        while let Some(msg) = self.mesh_rx.recv_from(0).await? {
            // handle this message
            match msg {
                MeshMsg::Join(_) => panic!("Join on shard?"),
                MeshMsg::Query { meta, query } => self
                    .shard_local_tx
                    .send(ShardMsg::Query { meta, query })
                    .await
                    .unwrap(),
                MeshMsg::NewClient { client, client_tx } => {
                    // forward this new client command
                    self.shard_local_tx
                        .send(ShardMsg::NewClient { client, client_tx })
                        .await
                        .unwrap();
                }
                MeshMsg::Shutdown => {
                    // forward this shutdown command
                    self.shard_local_tx.send(ShardMsg::Shutdown).await.unwrap();
                    // stop relaying messages
                    break;
                }
            }
        }
        Ok(())
    }
}

pub struct Shard<S: ShoalDatabase> {
    /// This shards info
    info: ShardInfo,
    /// The config for shoal
    conf: Conf,
    /// The tables we are responsible for on this shard
    pub tables: S,
    /// The full archive map for all tables
    table_map: FullArchiveMap<S::TableNames>,
    /// A map of channels to send responses to our client relays over
    client_map: HashMap<Uuid, AsyncSender<(Uuid, AlignedVec)>>,
    /// The glommio channel to send messages on
    local_tx: Senders<MeshMsg<S>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ShardMsg<S>>,
    /// The channel to Receive shard local messages on
    shard_local_rx: AsyncReceiver<ShardMsg<S>>,
    /// A map of storage systems and their loader channel
    loader_channels: HashMap<
        Loaders,
        (
            AsyncSender<LoaderMsg<S::TableNames>>,
            AsyncReceiver<LoaderMsg<S::TableNames>>,
        ),
    >,
    /// The responses whose queries have been flushed to disk
    flushed: Vec<(Uuid, Uuid, <S::ClientType as QuerySupport>::ResponseKinds)>,
    /// The latency sensitive task queue
    high_priority: TaskQueueHandle,
    /// The medium priority task queue
    _medium_priority: TaskQueueHandle,
    /// The tasks we have spawned
    tasks: Vec<Task<Result<(), ServerError>>>,
    /// The total size of all data on this shard
    memory_usage: Arc<RefCell<usize>>,
    /// The most recently used tables/partitions on this shard
    lru: Arc<RefCell<LruCache<(S::TableNames, u64), usize, BuildHasherDefault<Xxh3>>>>,
    timer_map: HashMap<usize, Instant>,
}

impl<S: ShoalDatabase> Shard<S> {
    /// Create a new shard
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind our udp socket too
    #[instrument(name = "Shard::new", skip_all, err(Debug))]
    pub async fn new(conf: &Conf, local_tx: Senders<MeshMsg<S>>) -> Result<Self, ServerError> {
        // build our shard info
        let info = ShardInfo::new(local_tx.peer_id());
        // create names for our high and low priority task queues
        let high_name = format!("HighPriority:{}", info.name);
        let medium_name = format!("MediumPriority:{}", info.name);
        // get a handle to our current executor
        let executor = glommio::executor();
        // create a high priority queue for this task queue
        let high_priority = executor.create_task_queue(
            Shares::Static(1000),
            Latency::Matters(Duration::from_millis(1)),
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
        let lru_hasher = BuildHasherDefault::<Xxh3>::default();
        // build our lru cache
        let lru = Arc::new(RefCell::new(LruCache::unbounded_with_hasher(lru_hasher)));
        // create our node local channel for this shard
        let (shard_local_tx, shard_local_rx) = kanal::unbounded_async();
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
            tables,
            table_map,
            client_map: HashMap::with_capacity(500),
            local_tx,
            shard_local_tx,
            shard_local_rx,
            loader_channels,
            flushed: Vec::with_capacity(1000),
            high_priority,
            _medium_priority: medium_priority,
            tasks: Vec::with_capacity(100),
            memory_usage,
            lru,
            timer_map: HashMap::with_capacity(100000),
        };
        Ok(shard)
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
    async fn init(&mut self, mesh_rx: Receivers<MeshMsg<S>>) -> Result<(), ServerError> {
        // build our join message
        let join_msg = MeshMsg::Join(self.info.clone());
        // send a message to our coordinator that we are ready to join
        self.local_tx.send_to(0, join_msg).await?;
        // build our mesh relay
        let relay = MeshRelay::new(mesh_rx, &self.shard_local_tx);
        // start relaying messages
        let relay =
            glommio::spawn_local_into(async move { relay.start().await }, self.high_priority)
                .unwrap();
        // start our loaders
        self.tables
            .init_storage_loaders(
                &self.table_map,
                &mut self.loader_channels,
                &self.shard_local_tx,
            )
            .await?;
        // track this task
        // TODO: allow tracked tasks to be differentiated
        self.tasks.push(relay);
        Ok(())
    }

    /// Send a respones back to the client
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to send this reply too
    /// * `response` - The response to send
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::reply", skip_all, err(Debug))]
    async fn reply(
        &mut self,
        client: Uuid,
        query_id: Uuid,
        response: <S::ClientType as QuerySupport>::ResponseKinds,
    ) -> Result<(), ServerError> {
        // archive our response
        let archived = rkyv::to_bytes::<_>(&response)?;
        // get this clients channel to send replies over
        match self.client_map.get(&client) {
            Some(client_tx) => client_tx.send((query_id, archived)).await.unwrap(),
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
    #[instrument(name = "Shard::handle_query", skip(self, query))]
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
        // get a timer for this query
        let index = meta.index;
        let timer = *self.timer_map.entry(meta.index).or_insert(Instant::now());
        println!(
            "S3.1: Adding timer for {} at {:?}",
            meta.index,
            timer.elapsed()
        );
        // try to handle this query
        if let Some((addr, query_id, response)) = self.tables.handle(meta, query, timer).await {
            println!(
                "S4:{}: PRE REPLY -> {index} $ {:?}",
                self.info.name,
                timer.elapsed()
            );
            // send this response back to the client
            self.reply(addr, query_id, response).await?;
            println!("S4: POST REPLY -> {index} $ {:?}", timer.elapsed());
        }
        Ok(())
    }

    /// Get all flushed messages and send their response back
    async fn handle_flushed(&mut self) -> Result<(), ServerError> {
        // get all flushed query responses
        self.tables.handle_flushed(&mut self.flushed).await?;
        // pop all of our flushed responses
        while let Some((client, query_id, response)) = self.flushed.pop() {
            // send our responses
            self.reply(client, query_id, response).await?;
        }
        Ok(())
    }

    /// Find partitions to evict
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
    pub async fn start<'a>(mut self, mesh_rx: Receivers<MeshMsg<S>>) -> Result<(), ServerError>
    where
        for<'b> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
            CheckBytes<
                Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
            >,
    {
        // initalize this shard
        self.init(mesh_rx).await?;
        // keep handling messages until we get a shutdown command
        loop {
            // wait for a message on our mesh
            let msg = self.shard_local_rx.recv().await.unwrap();
            // handle this message
            match msg {
                // Add this new client to our client map
                ShardMsg::NewClient { client, client_tx } => {
                    // add this client to our client map
                    if self.client_map.insert(client, client_tx).is_some() {
                        // panic if we had a client id collision
                        panic!("Client ID collision?");
                    }
                }
                // handle this query from the user
                ShardMsg::Query { meta, query } => self.handle_query(meta, query).await?,
                // load this partition from disk
                ShardMsg::Partition(loaded) => {
                    self.tables
                        .load_partition(loaded, &self.shard_local_tx)
                        .await?
                }
                // Mark some partitions as evictable
                ShardMsg::MarkEvictable {
                    generation,
                    table,
                    partitions,
                } => self.tables.mark_evictable(table, generation, partitions),
                // shutdown this shard
                ShardMsg::Shutdown => {
                    // signal all of our loaders to shutdown
                    for (_, (loader_tx, _)) in &self.loader_channels {
                        // signal this loader to shutdown
                        loader_tx.send(LoaderMsg::Shutdown).await.unwrap();
                    }
                    break;
                }
            }
            // if we have no more messages then flush our current queries to disk
            if self.shard_local_rx.is_empty() {
                println!("{}: flush now?", self.info.name);
                self.tables.flush().await?;
            }
            // check for any flushed response to handle
            self.handle_flushed().await?;
            // check if we need to evict any data
            if *self.memory_usage.borrow() as u64 > self.conf.resources.memory {
                println!("EVICT?: {}", self.info.name);
                // try to evict our least recently used data
                self.evict_data().await?;
            }
        }
        // check for any flushed response to handle
        self.handle_flushed().await?;
        // shudown
        self.tables.shutdown().await?;
        Ok(())
    }
}

pub fn start<S: ShoalDatabase>(
    conf: Conf,
    cpus: CpuSet,
    mesh: MeshBuilder<MeshMsg<S>, Full>,
) -> Result<PoolThreadHandles<Result<(), ServerError>>, ServerError>
where
    for<'a> <<<S as ShoalDatabase>::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // setup our executor
    let executor_builder =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(cpus.len(), Some(cpus)));
    // build and spawn our shards on all of remaining available cores
    let shards = executor_builder.on_all_shards(enclose!((mesh) move || {
        async move {
            // join this nodes mesh
            let (sender, receiver) = mesh.join().await?;
            // build an empty shard
            let shard: Shard<S> = Shard::new(&conf, sender).await?;
            // start this shard
            shard.start(receiver).await
        }
    }))?;
    Ok(shards)
}
