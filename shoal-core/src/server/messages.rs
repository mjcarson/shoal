//! The different messages that can be sent in shoal

use bytes::BytesMut;
use glommio::io::ReadResult;
use kanal::AsyncSender;
use rkyv::util::AlignedVec;
use tracing::Span;
use uuid::Uuid;

use super::shard::{ShardContact, ShardInfo};
use super::stage_profile::{StageStamps, Stamp};
use crate::shared::responses::ResponseError;
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
    /// When this query reached each stage of its journey through shoal
    ///
    /// A zero sized type unless the `stage-profile` feature is on, which is what lets this
    /// ride along in the response tuples without a `#[cfg]` at every site that builds one.
    pub stamps: StageStamps,
    /// A partition this execution must answer without reading from disk
    ///
    /// Set only when a load for that partition failed, so the replay that failure releases
    /// answers from what is resident instead of asking for the same read that just failed.
    /// It has to travel with the query rather than sit on the table, because a failed load
    /// releases several queries at once and each of them consumes the exemption separately.
    ///
    /// This is server side state on a server side struct - it never reaches a client.
    pub skip_disk: Option<u64>,
    /// The failure this query must answer with instead of the rows it could not read
    ///
    /// Set when a read this query was parked on gave up. It travels with the query rather than
    /// being answered on the spot because the query may still be parked on *another* partition:
    /// answering here would put a second response at an index that already has one. It is
    /// applied where this query finally produces a response, which happens exactly once.
    pub failed: Option<ResponseError>,
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
    /// * `stamps` - The stage timings for the bundle this query arrived in
    pub fn new(
        client: Uuid,
        id: Uuid,
        index: usize,
        end: bool,
        gather: Option<ShardContact>,
        stamps: StageStamps,
    ) -> Self {
        QueryMetadata {
            client,
            id,
            index,
            end,
            gather,
            span: Span::current(),
            stamps,
            // a query starts out with no reason to skip a read, since only a load that has
            // already failed can give it one
            skip_disk: None,
            // and with nothing to report, since nothing has failed yet
            failed: None,
        }
    }

    /// Create query metadata that is not being profiled
    ///
    /// Every query the server routes carries the stamps of the bundle it arrived in, so this
    /// is only for callers that build metadata outside that path — tests, and the replay of a
    /// query that was parked before the profile existed. The stamps it makes are based at
    /// now, so a record built from them measures from here rather than from the socket read.
    ///
    /// # Arguments
    ///
    /// * `client` - The id for this client
    /// * `id` - The id of this query
    /// * `index` - The index for this query in a bundle of queries
    /// * `end` - Whether this is the last query in a bundle or not
    /// * `gather` - The shard collecting this queries responses if it was split
    pub fn untimed(
        client: Uuid,
        id: Uuid,
        index: usize,
        end: bool,
        gather: Option<ShardContact>,
    ) -> Self {
        QueryMetadata::new(
            client,
            id,
            index,
            end,
            gather,
            StageStamps::new(Stamp::now()),
        )
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
        client_tx: AsyncSender<(Uuid, Span, StageStamps, AlignedVec)>,
    },
    /// A message from a client
    Client {
        /// This peers id
        peer: Uuid,
        /// The raw data for our request
        data: BytesMut,
        /// When the last byte of this request came off the socket
        ///
        /// Every stage offset a query in this bundle records is measured from here, since
        /// this is the first moment the server knows the bundle exists.
        base: Stamp,
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
    /// A partition that could not be loaded from disk
    ///
    /// A load completing is the only thing that drains a tables `blocked` map, so a load that
    /// fails has to say so rather than simply not arriving - otherwise every query parked on
    /// that partition waits for a message that will never be sent, and so does its client.
    ///
    /// The failure it carries is what the *client* is told, which is deliberately less than
    /// what is logged: the loader is where the context is richest and it logs the path and the
    /// errno, while this carries only a class and a line naming the table and partition. A
    /// pruned partition carries no failure at all, because a partition that really is gone is
    /// answered correctly by finding nothing.
    PartitionLoadFailed {
        /// The table the partition that could not be read belongs to
        table: D::TableNames,
        /// The partition that could not be read
        partition_id: u64,
        /// What the queries parked on it should answer with, if this was a failure at all
        error: Option<ResponseError>,
    },
    /// Some data has been flushed to storage
    ///
    /// This carries no position. The writer tracks its own durable watermark in
    /// state shared with its detached IO tasks, which is correct the instant an IO
    /// completes rather than whenever the shard happens to drain its channel. This
    /// message exists purely to wake the shard up so it runs `handle_flushed`.
    ///
    /// Its *arrival* is load-bearing: the shard only sweeps its tables for newly
    /// durable responses when one of these has landed, so every advance of a durable
    /// watermark has to be followed by one of these. See the invariants on
    /// `docs/src/features/flushed-sweep-gate.md` before adding a path that moves a
    /// watermark.
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
            ServerMsg::Client { peer, data, base } => ServerMsg::Client {
                peer: *peer,
                data: data.clone(),
                base: *base,
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
            ServerMsg::PartitionLoadFailed {
                table,
                partition_id,
                error,
            } => ServerMsg::PartitionLoadFailed {
                error: error.clone(),
                table: *table,
                partition_id: *partition_id,
            },
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
