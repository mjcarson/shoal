//! The different messages that can be sent in shoal

use bytes::BytesMut;
use std::net::SocketAddr;
use uuid::Uuid;

use super::shard::ShardInfo;
use crate::shared::traits::ShoalDatabase;

/// The messages that can be sent in shoal
#[derive(Debug)]
pub enum Msg<S: ShoalDatabase> {
    /// A message from our node local mesh
    Mesh { shard: usize, msg: MeshMsg<S> },
    /// A message from a client
    Client {
        /// The address this request came from
        addr: SocketAddr,
        /// The number of bytes to read
        read: usize,
        /// The raw data for our request
        data: BytesMut,
    },
}

/// The messages that can be sent over of node local mesh
#[derive(Debug)]
pub enum MeshMsg<S: ShoalDatabase> {
    /// Join this nodes token ring
    Join(ShardInfo),
    /// A query to execute
    Query {
        /// The address of the client this query came from
        addr: SocketAddr,
        /// The id for this query
        id: Uuid,
        /// This queries index in the queries vec
        index: usize,
        /// The query to execute
        query: S::QueryKinds,
        /// Whether this is the last query in a query bundle
        end: bool,
    },
}
