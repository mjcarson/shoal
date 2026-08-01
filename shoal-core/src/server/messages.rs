//! The different messages that can be sent in shoal

use bytes::BytesMut;
use glommio::io::ReadResult;
use kanal::AsyncSender;
use rkyv::util::AlignedVec;
use tracing::Span;
use uuid::Uuid;

use super::shard::{ShardContact, ShardInfo};
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
    /// The shard collecting this queries responses if it was split across shards
    ///
    /// A query naming partitions on several shards is answered in pieces, and the
    /// client is owed exactly one response for it, so those pieces go back to the
    /// shard that split it instead of straight to the client. `None` means this query
    /// is answered by one shard alone and needs no collecting.
    pub gather: Option<ShardContact>,
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
    /// * `gather` - The shard collecting this queries responses if it was split
    pub fn new(
        client: Uuid,
        id: Uuid,
        index: usize,
        end: bool,
        gather: Option<ShardContact>,
    ) -> Self {
        QueryMetadata {
            client,
            id,
            index,
            end,
            gather,
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
    /// One shards share of a query that was split across several shards
    ///
    /// This travels from the shard that executed a piece of a query back to the shard
    /// that split it, which merges the pieces and answers the client once.
    Gathered {
        /// The metadata for the query this is part of the answer to
        meta: QueryMetadata,
        /// This shards share of the answer
        response: <D::ClientType as QuerySupport>::ResponseKinds,
    },
    /// A partition loaded from disk. This can never be sent across threads!
    Partition(LoadedPartitionKinds<D>),
    /// Some data has been flushed to storage
    ///
    /// This carries no position. The writer tracks its own durable watermark in
    /// state shared with its detached IO tasks, which is correct the instant an IO
    /// completes rather than whenever the shard happens to drain its channel. This
    /// message exists purely to wake the shard up so it runs `handle_flushed`.
    DataFlushed,
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
            // a gathered response travels to exactly one shard and is never broadcast,
            // so there is nothing that would ever ask us to duplicate one
            ServerMsg::Gathered { .. } => {
                panic!("A gathered response is only ever sent to one shard")
            }
            ServerMsg::Partition(loaded) => ServerMsg::Partition(loaded.clone()),
            ServerMsg::DataFlushed => ServerMsg::DataFlushed,
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
