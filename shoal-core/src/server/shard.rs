//! A single shard in Shoal

use glommio::{
    channels::channel_mesh::{Full, MeshBuilder, Receivers, Senders},
    enclose,
    net::UdpSocket,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement, PoolThreadHandles,
};
use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec,
};
use std::net::SocketAddr;
use tracing::instrument;
use uuid::Uuid;

use super::ServerError;
use super::{messages::MeshMsg, Conf};
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

pub struct Shard<S: ShoalDatabase> {
    /// This shards info
    info: ShardInfo,
    /// The tables we are responsible for on this shard
    pub tables: S,
    /// The glommio channel to send messages on
    local_tx: Senders<MeshMsg<S>>,
    /// The glommio channel to receive messages on
    local_rx: Receivers<MeshMsg<S>>,
    /// The UDP socket to send responses on
    socket: UdpSocket,
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
        local_rx: Receivers<MeshMsg<S>>,
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
        // build our shard
        let shard = Shard {
            info,
            tables,
            local_tx,
            local_rx,
            socket,
        };
        Ok(shard)
    }

    /// Initialize this shard
    ///
    /// # Errors
    ///
    /// This will only fail if the coordinator has not joined the local mesh.
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::init", skip_all, err(Debug))]
    async fn init(&mut self) -> Result<(), ServerError> {
        // build our join message
        let join_msg = MeshMsg::Join(self.info.clone());
        // send a message to our coordinator that we are ready to join
        self.local_tx.send_to(0, join_msg).await?;
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
    where
        <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<256>, AllocScratch>,
                SharedSerializeMap,
            >,
        >,
    {
        // archive our response
        let archived = rkyv::to_bytes::<_, 256>(&response)?;
        // send our archived response back to the client
        self.socket.send_to(archived.as_slice(), addr).await?;
        Ok(())
    }

    /// Handle a query on this shard
    #[allow(clippy::future_not_send)]
    #[instrument(name = "Shard::handle_query", skip(self, query))]
    async fn handle_query(
        &mut self,
        addr: SocketAddr,
        id: Uuid,
        index: usize,
        query: S::QueryKinds,
        end: bool,
    ) -> Result<(), ServerError>
    where
        <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<256>, AllocScratch>,
                SharedSerializeMap,
            >,
        >,
    {
        // try to handle this query
        let response = self.tables.handle(id, index, query, end).await;
        // send this response back to the client
        self.reply(addr, response).await?;
        Ok(())
    }

    /// Start handling queries from users
    ///
    /// # Errors
    ///
    /// This wil return an error if a message cannot be sent to a coordinator or if a query fails
    #[allow(clippy::future_not_send)]
    pub async fn start<'a>(mut self) -> Result<(), ServerError>
    where
        <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<256>, AllocScratch>,
                SharedSerializeMap,
            >,
        >,
    {
        // initalize this shard
        self.init().await?;
        // wait for a message on our mesh
        while let Some(msg) = self.local_rx.recv_from(0).await? {
            match msg {
                MeshMsg::Join(_) => panic!("Join on shard?"),
                MeshMsg::Query {
                    addr,
                    id,
                    index,
                    query,
                    end,
                } => self.handle_query(addr, id, index, query, end).await?,
                MeshMsg::Shutdown => {
                    break;
                }
            }
        }
        Ok(())
    }
}

pub fn start<S: ShoalDatabase>(
    conf: Conf,
    cpus: CpuSet,
    mesh: MeshBuilder<MeshMsg<S>, Full>,
) -> Result<PoolThreadHandles<Result<(), ServerError>>, ServerError>
where
    <S as ShoalDatabase>::ResponseKinds: rkyv::Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<256>, AllocScratch>,
            SharedSerializeMap,
        >,
    >,
{
    // setup our executor
    let executor = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(cpus.len(), Some(cpus)));
    // build and spawn our shards on all of our cores
    let shards = executor.on_all_shards(enclose!((mesh) move || {
        async move {
            // join this nodes mesh
            let (sender, receiver) = mesh.join().await?;
            // build an empty shard
            let shard: Shard<S> = Shard::new(&conf, sender, receiver).await?;
            // start this shard
            shard.start().await
        }
    }))?;
    Ok(shards)
}
