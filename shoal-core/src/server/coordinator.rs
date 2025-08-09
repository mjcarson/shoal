//! Coordinates traffic between this node and others

use bytes::BytesMut;
use glommio::{
    channels::{
        channel_mesh::{Full, MeshBuilder, Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    net::UdpSocket,
    Task,
};
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::Archive;
use std::{marker::PhantomData, net::SocketAddr};
use tracing::{event, instrument, Level};

use super::messages::QueryMetadata;
use super::ring::Ring;
use super::{
    messages::{MeshMsg, Msg},
    shard::ShardContact,
};
use super::{Conf, ServerError};
use crate::shared::{
    queries::ArchivedQueries,
    traits::{RkyvSupport, ShoalDatabase, ShoalQuery},
};
use crate::shared::{queries::Queries, traits::QuerySupport};

/// Coordinates traffic between this node and others
pub struct Coordinator<S: ShoalDatabase> {
    /// The Shoal config
    conf: Conf,
    /// The glommio channel to send messages on
    mesh_tx: Senders<MeshMsg<S>>,
    /// The glommio channel to receive messages on
    mesh_rx: Receivers<MeshMsg<S>>,
    /// The kanal channel to listen for coordinator local messages on
    kanal_rx: AsyncReceiver<Msg<S>>,
    /// The tasks we have spawned
    tasks: Vec<Task<()>>,
    /// The token ring info for shoal
    ring: Ring,
    /// The tables we are coordinating
    table_kind: PhantomData<S>,
}

impl<S: ShoalDatabase> Coordinator<S>
where
    <<S::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <S::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<S::ClientType> as Archive>::Archived: rkyv::bytecheck::CheckBytes<
        rkyv::rancor::Strategy<
            rkyv::validation::Validator<
                rkyv::validation::archive::ArchiveValidator<'a>,
                rkyv::validation::shared::SharedValidator,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    /// Start a new coordinator thread
    #[allow(clippy::future_not_send)]
    pub async fn start<'a>(
        conf: Conf,
        mesh: MeshBuilder<MeshMsg<S>, Full>,
        kanal_tx: AsyncSender<Msg<S>>,
        kanal_rx: AsyncReceiver<Msg<S>>,
    ) -> Result<(), ServerError> {
        // join our mesh
        let (mesh_tx, mesh_rx) = mesh.join().await?;
        // build our coordinator
        let mut coordinator: Coordinator<S> = Coordinator {
            conf,
            mesh_tx,
            mesh_rx,
            kanal_rx,
            tasks: Vec::new(),
            ring: Ring::default(),
            table_kind: PhantomData,
        };
        // spawn our mesh listeners
        coordinator.spawn_mesh_listeners(&kanal_tx);
        // spawmn our client litener
        coordinator.spawn_client_listener(&kanal_tx)?;
        // start handling messages
        coordinator.start_handling().await;
        Ok(())
    }

    /// Spawn our mesh listeners
    fn spawn_mesh_listeners(&mut self, kanal_tx: &AsyncSender<Msg<S>>) {
        // connect to all of our channels
        for (id, local_rx) in self.mesh_rx.streams() {
            // clone our kanal transmitter
            let kanal_tx = kanal_tx.clone();
            // spawn a future waiting for a message on this channel
            let handle = glommio::spawn_local(mesh_listener(id, local_rx, kanal_tx));
            // add this task to our task list
            self.tasks.push(handle);
        }
    }

    /// Spawn our client network listener
    fn spawn_client_listener(&mut self, kanal_tx: &AsyncSender<Msg<S>>) -> Result<(), ServerError> {
        // bind our udp socket
        let udp_sock = UdpSocket::bind(self.conf.networking.to_addr())?;
        // clone our kanal transmitter
        let kanal_tx = kanal_tx.clone();
        // spawn our client listener
        let handle = glommio::spawn_local(client_listener(udp_sock, kanal_tx));
        // add this task to our task list
        self.tasks.push(handle);
        Ok(())
    }

    /// Handle a mesh messages
    #[instrument(name = "Coordinator::handle_mesh", skip(self, msg))]
    fn handle_mesh(&mut self, _shard: usize, msg: MeshMsg<S>) {
        match msg {
            MeshMsg::Join(info) => self.ring.add(info),
            MeshMsg::Query { meta, query, .. } => {
                println!("QUERY -> {:#?} from {:#?}", query, meta.addr)
            }
            MeshMsg::Shutdown => panic!("SHUTDOWN MESH MSG?"),
        }
    }

    /// Forward our queries to the correct shards
    #[instrument(name = "Coordinator::send_to_shard", skip_all)]
    async fn send_to_shard(
        &mut self,
        addr: SocketAddr,
        queries: Queries<S::ClientType>,
    ) -> Result<(), ServerError> {
        // initialize a vec to store the shards we find
        let mut found = Vec::with_capacity(3);
        // get the index for the last query in this bundle
        let end_index = queries.queries.len() - 1;
        // crawl over our queries
        for (index, kind) in queries.queries.into_iter().enumerate() {
            // get our target shards info
            kind.find_shard(&self.ring, &mut found);
            // send this query to the right shards
            for shard_info in found.drain(..) {
                match &shard_info.contact {
                    ShardContact::Local(id) => {
                        // check if this is the last query or not
                        let end = index == end_index;
                        // build the metadata for this query
                        let meta = QueryMetadata::new(addr, queries.id, index, end);
                        // build our mesh message
                        let msg = MeshMsg::Query {
                            meta,
                            query: kind.clone(),
                        };
                        // send our query mesh message to the right shard
                        self.mesh_tx.send_to(*id, msg).await.unwrap();
                    }
                };
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
    #[instrument(name = "Coordinator::handle_client", skip(self, addr, data))]
    async fn handle_client<'a>(&mut self, addr: SocketAddr, read: usize, data: BytesMut) {
        // get the slice to deserialize
        let readable = &data[..read];
        // load our arhived query from buffer
        //let archived = <Queries<S::ClientType> as RkyvSupport>::load(readable);
        let archived = Queries::access(readable).unwrap();
        // deserialize our queries
        let queries = <Queries<S::ClientType> as RkyvSupport>::deserialize(archived).unwrap();
        // send each query to the correct shard
        self.send_to_shard(addr, queries).await.unwrap();
    }

    /// Handle a shutdown command
    #[instrument(name = "Coordinator::shutdown", skip_all, err(Debug))]
    async fn handle_shutdown(&mut self) -> Result<(), ServerError> {
        // get all shards
        let shards = &self.ring.shards;
        // forward this shutdown comman dot all shards
        for shard in shards {
            // if this is a local shard then send this message over the local mesh
            match &shard.contact {
                ShardContact::Local(id) => {
                    // forward this shutdown command to this shard
                    self.mesh_tx.send_to(*id, MeshMsg::Shutdown).await?;
                    // log that we told this shard to shutdown
                    event!(Level::INFO, shard = shard.name, id);
                }
            }
        }
        Ok(())
    }

    /// Start handling messages
    #[allow(clippy::future_not_send)]
    async fn start_handling<'a>(&mut self) {
        // handle messages across any of our channels
        loop {
            // try to get a message from our channel
            let msg = match self.kanal_rx.recv().await {
                Ok(msg) => msg,
                Err(error) => panic!("AHHHH: {:#?}", error),
            };
            // handle this message based on its type
            match msg {
                Msg::Mesh { shard, msg } => self.handle_mesh(shard, msg),
                Msg::Client { addr, read, data } => self.handle_client(addr, read, data).await,
                Msg::Shutdown => {
                    // forward this shutdown command to others
                    self.handle_shutdown().await.unwrap();
                    // exit the coordinator
                    break;
                }
            }
        }
        // log that this coordinator is shutting down
        event!(Level::INFO, msg = "Shutting Down");
    }
}

#[allow(clippy::future_not_send)]
async fn mesh_listener<S: ShoalDatabase>(
    shard: usize,
    local_rx: ConnectedReceiver<MeshMsg<S>>,
    kanal_rx: AsyncSender<Msg<S>>,
) {
    // loop forever waiting for mesh messages
    loop {
        if let Some(msg) = local_rx.recv().await {
            // build our wrapped mesh msg
            let wrapped = Msg::Mesh { shard, msg };
            // forward this message to the coordinator
            kanal_rx.send(wrapped).await.unwrap();
        }
    }
}

#[allow(clippy::future_not_send)]
async fn client_listener<S: ShoalDatabase>(udp_sock: UdpSocket, kanal_tx: AsyncSender<Msg<S>>) {
    loop {
        // TODO reuse buffers instead of making new ones
        let mut data = BytesMut::zeroed(16384);
        // try to read a single datagram from our udp socket
        let (read, addr) = udp_sock.recv_from(&mut data).await.unwrap();
        // forward our clients message
        kanal_tx
            .send(Msg::Client { addr, read, data })
            .await
            .unwrap();
    }
}
