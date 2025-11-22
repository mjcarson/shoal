//! The different messages that can be sent in shoal

use bytes::BytesMut;
use glommio::io::ReadResult;
use kanal::AsyncSender;
use rkyv::util::AlignedVec;
use tracing::Span;
use uuid::Uuid;

use super::shard::ShardInfo;
use crate::shared::traits::{QuerySupport, ShoalDatabase};

/// The messages that can be sent in shoal
pub enum Msg<S: ShoalDatabase> {
    /// A message from our node local mesh
    Mesh { shard: usize, msg: MeshMsg<S> },
    /// A new client has connected
    NewClient {
        id: Uuid,
        client_tx: AsyncSender<(Uuid, Span, AlignedVec)>,
    },
    /// A message from a client
    Client {
        /// This peers id
        peer: Uuid,
        /// The raw data for our request
        data: BytesMut,
    },
    /// Tell Shoal to shutdown
    Shutdown,
}

/// The metadata about a query from a client
#[derive(Debug)]
pub struct QueryMetadata {
    /// The id of the client this query came from
    pub client: Uuid,
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// Whether this is the last query in a query bundle
    pub end: bool,
    /// The span context for this query
    pub span: Span,
}

impl QueryMetadata {
    /// Create a new query metadata object
    ///
    /// # Arguments
    ///
    /// * `client` - The id for this client
    /// * `id` - The id of this query
    /// * `index` - The index for this query in a bundle of queries
    /// * `end` - Whether this is the last query in a bundle or not
    pub fn new(client: Uuid, id: Uuid, index: usize, end: bool) -> Self {
        QueryMetadata {
            client,
            id,
            index,
            end,
            span: Span::current(),
        }
    }
}

/// The messages that can be sent over of node local mesh
pub enum MeshMsg<D: ShoalDatabase> {
    /// Join this nodes token ring
    Join(ShardInfo),
    /// A query to execute
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The query to execute
        query: <D::ClientType as QuerySupport>::QueryKinds,
    },
    /// Tell this shard about a new client
    NewClient {
        /// This clients id
        client: Uuid,
        /// The channel to send responses for this client on
        client_tx: AsyncSender<(Uuid, Span, AlignedVec)>,
    },
    /// Tell this shard to shutdown
    Shutdown,
}

pub struct LoadedPartition {
    /// The partition that is being read from disk
    pub partition_id: u64,
    /// The result for this read
    pub data: ReadResult,
}

pub struct LoadedPartitionKinds<D: ShoalDatabase> {
    /// The table this partition is for
    pub table: D::TableNames,
    /// The partition that was loaded from storage
    pub loaded: LoadedPartition,
}

/// The messages that can be sent between workers in a shard
pub enum ShardMsg<D: ShoalDatabase> {
    /// A New client connected to shoal
    NewClient {
        client: Uuid,
        client_tx: AsyncSender<(Uuid, Span, AlignedVec)>,
    },
    /// A still archived query to execute
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The query to execute
        query: <D::ClientType as QuerySupport>::QueryKinds,
    },
    /// A partition loaded from disk
    Partition(LoadedPartitionKinds<D>),
    /// Mark some partitions as evictable
    MarkEvictable {
        generation: u64,
        table: D::TableNames,
        partitions: Vec<u64>,
    },
    /// Tell this shard to shutdown
    Shutdown,
}
