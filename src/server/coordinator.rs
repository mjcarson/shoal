//! Coordinates traffic between this node and others

use std::{marker::PhantomData, net::SocketAddr};

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

use super::messages::{MeshMsg, Msg};
use super::ring::Ring;
use super::{Conf, ServerError};
use crate::shared::traits::ShoalDatabase;

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

impl<S: ShoalDatabase> Coordinator<S> {
    /// Start a new coordinator thread
    #[allow(clippy::future_not_send)]
    pub async fn start<'a>(
        conf: Conf,
        mesh: MeshBuilder<MeshMsg<S>, Full>,
    ) -> Result<(), ServerError> {
        // join our mesh
        let (mesh_tx, mesh_rx) = mesh.join().await?;
        // create our kanal channels
        let (kanal_tx, kanal_rx) = kanal::bounded_async(8192);
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
    fn handle_mesh(&mut self, _shard: usize, msg: MeshMsg<S>) {
        match msg {
            MeshMsg::Join(info) => self.ring.add(info),
            MeshMsg::Query { addr, query, .. } => {
                println!("QUERY -> {:#?} from {:#?}", query, addr)
            }
        }
    }

    /// Handle a client messages
    ///
    /// # Arguments
    ///
    /// * `addr` - The address
    #[allow(clippy::future_not_send)]
    async fn handle_client<'a>(&mut self, addr: SocketAddr, read: usize, data: BytesMut) {
        // get the slice to deserialize
        let readable = &data[..read];
        // deserialize our queries
        let queries = S::unarchive(&readable);
        // send our query to correct shard
        S::send_to_shard(&self.ring, &mut self.mesh_tx, addr, queries)
            .await
            .unwrap();
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
            }
        }
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
        let mut data = BytesMut::zeroed(8192);
        // try to read a single datagram from our udp socket
        let (read, addr) = udp_sock.recv_from(&mut data).await.unwrap();
        //println!("coord - read {} from {}", read, addr);
        // forward our clients message
        kanal_tx
            .send(Msg::Client { addr, read, data })
            .await
            .unwrap();
    }
}
