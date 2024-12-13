//! A single shard in Shoal

use glommio::{
    channels::channel_mesh::{Full, MeshBuilder, Receivers, Senders},
    enclose,
    net::UdpSocket,
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, PoolThreadHandles, Shares, Task,
    TaskQueueHandle,
};
use kanal::{AsyncReceiver, AsyncSender};
use std::{net::SocketAddr, time::Duration};
use tracing::instrument;

use super::{messages::QueryMetadata, ServerError};
use super::{
    messages::{MeshMsg, ShardMsg},
    Conf,
};
use crate::shared::traits::ShoalDatabase;

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
    node_local_tx: AsyncSender<ShardMsg<S>>,
}

impl<S: ShoalDatabase> MeshRelay<S> {
    /// Create a new mesh relay
    ///
    /// # Arguments
    ///
    /// * `mesh_rx` - The glommio channel to receive mesh messages on
    /// * `node_local_tx` - The kanal channel to send shard local messages on
    pub fn new(mesh_rx: Receivers<MeshMsg<S>>, node_local_tx: &AsyncSender<ShardMsg<S>>) -> Self {
        MeshRelay {
            mesh_rx,
            node_local_tx: node_local_tx.clone(),
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
                    .node_local_tx
                    .send(ShardMsg::Query { meta, query })
                    .await
                    .unwrap(),
                MeshMsg::Shutdown => {
                    // forward this shutdown command
                    self.node_local_tx.send(ShardMsg::Shutdown).await.unwrap();
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
    /// The tables we are responsible for on this shard
    pub tables: S,
    /// The glommio channel to send messages on
    local_tx: Senders<MeshMsg<S>>,
    /// The channel to send shard local messages on
    node_local_tx: AsyncSender<ShardMsg<S>>,
    /// The channel to Receive shard local messages on
    node_local_rx: AsyncReceiver<ShardMsg<S>>,
    /// The UDP socket to send responses on
    socket: UdpSocket,
    /// The responses whose queries have been flushed to disk
    flushed: Vec<(SocketAddr, S::ResponseKinds)>,
    /// The latency sensitive task queue
    high_priority: TaskQueueHandle,
    /// The low priority task queue
    low_priority: TaskQueueHandle,
    /// The tasks we have spawned
    tasks: Vec<Task<Result<(), ServerError>>>,
}

impl<S: ShoalDatabase> Shard<S> {
    /// Create a new shard
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind our udp socket too
    #[instrument(name = "Shard::new", skip_all, err(Debug))]
    pub async fn new(
        conf: &Conf,
        local_tx: Senders<MeshMsg<S>>,
        //local_rx: Receivers<MeshMsg<S>>,
    ) -> Result<Self, ServerError> {
        // build our shard info
        let info = ShardInfo::new(local_tx.peer_id());
        // build the addr to bind too
        // bind to port 0 so we get the next available port
        let addr = format!("{}:0", conf.networking.interface);
        // bind a udp socket to send responses out on
        let socket = UdpSocket::bind(addr)?;
        // build our shards tables
        let tables = S::new(&info.name, conf).await?;
        // create our node local channel for this shard
        let (node_local_tx, node_local_rx) = kanal::unbounded_async();
        // create names for our high and low priority task queues
        let high_name = format!("HighPriority:{}", info.name);
        let low_name = format!("LowPriority:{}", info.name);
        // get a handle to our current executor
        let executor = glommio::executor();
        // create a high priority queue for this task queue
        let high_priority = executor.create_task_queue(
            Shares::Static(2),
            Latency::Matters(Duration::from_millis(10)),
            &high_name,
        );
        // create a low priority queue for this task queue
        let low_priority =
            executor.create_task_queue(Shares::Static(1), Latency::NotImportant, &low_name);
        // build our shard
        let shard = Shard {
            info,
            tables,
            local_tx,
            node_local_tx,
            node_local_rx,
            socket,
            flushed: Vec::with_capacity(1000),
            high_priority,
            low_priority,
            tasks: Vec::with_capacity(100),
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
        let relay = MeshRelay::new(mesh_rx, &self.node_local_tx);
        // start relaying messages
        let relay =
            glommio::spawn_local_into(async move { relay.start().await }, self.high_priority)
                .unwrap();
        // track this task
        self.tasks.push(relay);
        Ok(())
    }

    /// Send a respones back to the client
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::reply", skip_all, err(Debug))]
    async fn reply(
        &mut self,
        addr: SocketAddr,
        response: S::ResponseKinds,
    ) -> Result<(), ServerError>
//where
    //    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
    //        CompositeSerializer<
    //            AlignedSerializer<AlignedVec>,
    //            FallbackScratch<HeapScratch<256>, AllocScratch>,
    //            SharedSerializeMap,
    //        >,
    //    >,
    {
        // archive our response
        let archived = rkyv::to_bytes::<_>(&response)?;
        // send our archived response back to the client
        self.socket.send_to(archived.as_slice(), addr).await?;
        Ok(())
    }

    /// Handle a query on this shard
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::handle_query", skip(self, query))]
    async fn handle_query(
        &mut self,
        meta: QueryMetadata,
        query: S::QueryKinds,
    ) -> Result<(), ServerError>
//where
    //    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
    //        CompositeSerializer<
    //            AlignedSerializer<AlignedVec>,
    //            FallbackScratch<HeapScratch<256>, AllocScratch>,
    //            SharedSerializeMap,
    //        >,
    //    >,
    {
        // try to handle this query
        if let Some((addr, response)) = self.tables.handle(meta, query).await {
            // send this response back to the client
            self.reply(addr, response).await?;
        }
        Ok(())
    }

    /// Get all flushed messages and send their response back
    async fn handle_flushed(&mut self) -> Result<(), ServerError>
//where
    //    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
    //        CompositeSerializer<
    //            AlignedSerializer<AlignedVec>,
    //            FallbackScratch<HeapScratch<256>, AllocScratch>,
    //            SharedSerializeMap,
    //        >,
    //    >,
    {
        // get all flushed query responses
        self.tables.handle_flushed(&mut self.flushed);
        // pop all of our flushed responses
        while let Some((addr, response)) = self.flushed.pop() {
            // send our responses
            self.reply(addr, response).await?;
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
//where
    //    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
    //        CompositeSerializer<
    //            AlignedSerializer<AlignedVec>,
    //            FallbackScratch<HeapScratch<256>, AllocScratch>,
    //            SharedSerializeMap,
    //        >,
    //    >,
    {
        // initalize this shard
        self.init(mesh_rx).await?;
        // keep handling messages until we get a shutdown command
        loop {
            // wait for a message on our mesh
            let msg = self.node_local_rx.recv().await.unwrap();
            // handle this message
            match msg {
                ShardMsg::Query { meta, query } => self.handle_query(meta, query).await?,
                ShardMsg::Shutdown => {
                    break;
                }
            }
            // if we have no more messages then flush our current queries to disk
            if self.node_local_rx.is_empty() {
                self.tables.flush().await?;
            }
            // check for any flushed response to handle
            self.handle_flushed().await?;
        }
        Ok(())
    }
}

///// Periodically send a flush message to our shard
//async fn periodic_send_flush<S: ShoalDatabase>(local_tx: Senders<MeshMsg<S>>) -> {
//}

pub fn start<S: ShoalDatabase>(
    conf: Conf,
    cpus: CpuSet,
    mesh: MeshBuilder<MeshMsg<S>, Full>,
) -> Result<PoolThreadHandles<Result<(), ServerError>>, ServerError>
//where
//    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
//        CompositeSerializer<
//            AlignedSerializer<AlignedVec>,
//            FallbackScratch<HeapScratch<256>, AllocScratch>,
//            SharedSerializeMap,
//        >,
//    >,
{
    // setup our executor
    let executor_builder =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(cpus.len(), Some(cpus)));
    // build and spawn our shards on all of our cores
    let shards = executor_builder.on_all_shards(enclose!((mesh) move || {
        async move {
            // join this nodes mesh
            let (sender, receiver) = mesh.join().await?;
            // build an empty shard
            let shard: Shard<S> = Shard::new(&conf, sender).await?;
            // start this shard
            shard.start(receiver).await
            //// start this shard
            //let query_handler = glommio::spawn_local_into( async move { shard.start().await }, high_priority).unwrap();
            //// wait for our query handler to exit
            //query_handler.await
        }
    }))?;
    Ok(shards)
}
