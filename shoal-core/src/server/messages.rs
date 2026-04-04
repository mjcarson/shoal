//! The different messages that can be sent in shoal

use bytes::BytesMut;
use glommio::io::ReadResult;
use kanal::AsyncSender;
use rkyv::util::AlignedVec;
use tracing::Span;
use uuid::Uuid;

use super::shard::ShardInfo;
use crate::shared::traits::{QuerySupport, ShoalDatabase};

/// The metadata about a query from a client
#[derive(Debug, Clone)]
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
///
/// # Safety
///
/// The Partition variant must never be sent across threads. In order to
/// prevent that only loaders should ever create Partition variant. Loaders
/// should also refrain from have any channel other then to their local shard.
pub enum ServerMsg<D: ShoalDatabase>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Join this nodes token ring
    Join(ShardInfo),
    /// Tell this shard about a new client
    NewClient {
        /// This clients id
        client: Uuid,
        /// The channel to send responses for this client on
        client_tx: AsyncSender<(Uuid, Span, AlignedVec)>,
    },
    /// A message from a client
    Client {
        /// This peers id
        peer: Uuid,
        /// The raw data for our request
        data: BytesMut,
    },
    /// A query to execute
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The query to execute
        query: <D::ClientType as QuerySupport>::QueryKinds,
    },
    /// A partition loaded from disk. This can never be sent across threads!
    Partition(LoadedPartitionKinds<D>),
    /// Some data has been flushed to storage
    DataFlushed { table: D::TableNames, flushed: u64 },
    /// Mark some partitions as evictable
    MarkEvictable {
        generation: u64,
        table: D::TableNames,
        partitions: Vec<u64>,
    },
    /// Tell this shard to shutdown
    Shutdown,
}

impl<D: ShoalDatabase> Clone for ServerMsg<D> {
    fn clone(&self) -> Self {
        match self {
            ServerMsg::Join(info) => ServerMsg::Join(info.clone()),
            ServerMsg::Client { peer, data } => ServerMsg::Client {
                peer: peer.clone(),
                data: data.clone(),
            },
            ServerMsg::NewClient { client, client_tx } => ServerMsg::NewClient {
                client: client.clone(),
                client_tx: client_tx.clone(),
            },
            ServerMsg::Query { meta, query } => ServerMsg::Query {
                meta: meta.clone(),
                query: query.clone(),
            },
            ServerMsg::Partition(loaded) => ServerMsg::Partition(loaded.clone()),
            ServerMsg::DataFlushed { table, flushed } => ServerMsg::DataFlushed {
                table: *table,
                flushed: *flushed,
            },
            ServerMsg::MarkEvictable {
                generation,
                table,
                partitions,
            } => ServerMsg::MarkEvictable {
                generation: *generation,
                table: *table,
                partitions: partitions.clone(),
            },
            ServerMsg::Shutdown => ServerMsg::Shutdown,
        }
    }
}

/// # Safety
///
/// The Partition variant should not be sent across threads ever.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D>
where
    D: Send,
    D::TableNames: Send,
    <D::ClientType as QuerySupport>::QueryKinds: Send,
{
}

#[derive(Clone)]
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

impl<D: ShoalDatabase> Clone for LoadedPartitionKinds<D> {
    fn clone(&self) -> Self {
        LoadedPartitionKinds {
            table: self.table.clone(),
            loaded: self.loaded.clone(),
        }
    }
}
