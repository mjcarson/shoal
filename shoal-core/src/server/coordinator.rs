//! Coordinates traffic between this node and others

use bytes::{Bytes, BytesMut};
use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{TcpListener, TcpStream};
use glommio::{
    channels::{
        channel_mesh::{Full, MeshBuilder, Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    Task,
};
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::util::AlignedVec;
use rkyv::Archive;
use std::io::IoSlice;
use std::{marker::PhantomData, time::Duration};
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use super::messages::QueryMetadata;
use super::ring::Ring;
use super::{
    messages::{MeshMsg, Msg},
    shard::ShardContact,
};
use super::{Conf, ServerError};
use crate::shared::traits::{RkyvSupport, ShoalDatabase, ShoalQuerySupport};
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
        // join our mesh for the coordinator and the client acceptor
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
        coordinator.spawn_mesh_listeners(&kanal_tx).await;
        // spawn our client litener
        coordinator.spawn_client_listener(&kanal_tx)?;
        // start handling messages
        coordinator.start_handling().await;
        Ok(())
    }

    /// Spawn our mesh listeners
    async fn spawn_mesh_listeners(&mut self, kanal_tx: &AsyncSender<Msg<S>>) {
        // wait for the full number of listeners to have connected
        loop {
            // check how many of our shards have connected
            if self.mesh_rx.nr_producers() < self.conf.resources.cpus().unwrap().len() - 1 {
                // not all of our shards are ready so sleep
                glommio::timer::sleep(Duration::from_millis(100)).await;
                // restart our loop
                continue;
            }
            // connect to all of our channels
            for (id, local_rx) in self.mesh_rx.streams() {
                // clone our kanal transmitter
                let kanal_tx = kanal_tx.clone();
                // spawn a future waiting for a message on this channel
                let handle = glommio::spawn_local(mesh_listener(id, local_rx, kanal_tx));
                // add this task to our task list
                self.tasks.push(handle);
            }
            break;
        }
    }

    /// Spawn our client network listener
    fn spawn_client_listener(&mut self, kanal_tx: &AsyncSender<Msg<S>>) -> Result<(), ServerError> {
        // bind our udp socket
        let tcp_sock = TcpListener::bind(self.conf.networking.to_addr())?;
        // clone our kanal transmitter
        let kanal_tx = kanal_tx.clone();
        // spawn our client listener
        let handle = glommio::spawn_local(client_acceptor(tcp_sock, kanal_tx));
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
                println!("QUERY -> {:#?} from {:#?}", query, meta.client)
            }
            MeshMsg::NewClient { client, .. } => {
                println!("New Client -> {client}?");
            }
            MeshMsg::Shutdown => panic!("SHUTDOWN MESH MSG?"),
        }
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
                match &shard_info.contact {
                    ShardContact::Local(id) => {
                        // add our base index to this messages index
                        index += queries.base_index;
                        // check if this is the last query or not
                        let end = index == end_index;
                        // build the metadata for this query
                        let meta = QueryMetadata::new(client, queries.id, index, end);
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
    #[instrument(name = "Coordinator::handle_client", skip(self, peer, data))]
    async fn handle_client<'a>(&mut self, peer: Uuid, data: BytesMut) {
        // load our arhived query from buffer
        let archived = Queries::access(&data).unwrap();
        // deserialize our queries
        let queries = <Queries<S::ClientType> as RkyvSupport>::deserialize(archived).unwrap();
        // send each query to the correct shard
        self.send_to_shard(peer, queries).await.unwrap();
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
        // get mesh id so we don't send a message to ourselves
        let our_id = self.mesh_tx.peer_id();
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
                Msg::NewClient { id, client_tx } => {
                    // tell every shard about our new client
                    for mesh_peer in 1..self.mesh_tx.nr_consumers() {
                        // skip ourselves
                        if mesh_peer == our_id {
                            continue;
                        }
                        //  clone our client sender
                        let client_tx = client_tx.clone();
                        // tell this shard about this new client
                        self.mesh_tx
                            .send_to(
                                mesh_peer,
                                MeshMsg::NewClient {
                                    client: id,
                                    client_tx,
                                },
                            )
                            .await
                            .unwrap();
                    }
                }
                Msg::Client { peer, data } => self.handle_client(peer, data).await,
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

async fn client_rx_relay<S: ShoalDatabase>(
    peer: Uuid,
    mut tcp_rx: ReadHalf<TcpStream>,
    kanal_tx: AsyncSender<Msg<S>>,
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
        kanal_tx.send(Msg::Client { peer, data }).await.unwrap();
    }
}

async fn client_tx_relay<S: ShoalDatabase>(
    client_rx: AsyncReceiver<(Uuid, Span, AlignedVec)>,
    mut tcp_tx: WriteHalf<TcpStream>,
) {
    // loop over messages to send back to our client
    loop {
        // try to get a message from our channel
        let (query_id, span, archived) = client_rx.recv().await.unwrap();
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
async fn client_acceptor<S: ShoalDatabase>(tcp_sock: TcpListener, kanal_tx: AsyncSender<Msg<S>>) {
    loop {
        // try to read a single datagram from our udp socket
        let stream = tcp_sock.accept().await.unwrap();
        // disable nagles algorithm on this socket
        stream.set_nodelay(true).unwrap();
        // generate an id for this peer
        // TODO: detect collisions?
        let id = Uuid::new_v4();
        // break this stream up into a writer and a reader
        let (tcp_rx, tcp_tx) = stream.split();
        // create a channel for all of our shards to give data to send back to clients
        let (client_tx, client_rx) = kanal::unbounded_async();
        // TODO: do this with a task queue?
        glommio::spawn_local(client_rx_relay(id, tcp_rx, kanal_tx.clone())).detach();
        glommio::spawn_local(client_tx_relay::<S>(client_rx, tcp_tx)).detach();
        // tell our coordinator we have a new client so it can let our shards know
        kanal_tx
            .send(Msg::NewClient { id, client_tx })
            .await
            .unwrap();
    }
}
