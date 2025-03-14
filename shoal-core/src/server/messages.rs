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
    /// Tell Shoal to shutdown
    Shutdown,
}

/// The metadata about a query from a client
#[derive(Debug)]
pub struct QueryMetadata {
    /// The address of the client this query came from
    pub addr: SocketAddr,
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// Whether this is the last query in a query bundle
    pub end: bool,
}

impl QueryMetadata {
    /// Create a new query metadata object
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to respond to this query at
    /// * `id` - The id of this query
    /// * `index` - The index for this query in a bundle of queries
    /// * `end` - Whether this is the last query in a bundle or not
    pub fn new(addr: SocketAddr, id: Uuid, index: usize, end: bool) -> Self {
        QueryMetadata {
            addr,
            id,
            index,
            end,
        }
    }
}

/// The messages that can be sent over of node local mesh
#[derive(Debug)]
pub enum MeshMsg<S: ShoalDatabase> {
    /// Join this nodes token ring
    Join(ShardInfo),
    /// A query to execute
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The query to execute
        query: S::QueryKinds,
    },
    /// Tell this shard to shutdown
    Shutdown,
}

/// The messages that can be sent between workers in a shard
pub enum ShardMsg<D: ShoalDatabase> {
    /// A query to execute
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The query to execute
        query: D::QueryKinds,
    },
    /// Tell this shard to shutdown
    Shutdown,
}
