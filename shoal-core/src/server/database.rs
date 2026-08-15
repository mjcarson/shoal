//! The trait a Shoal database implements to be served
//!
//! This is the server half of a schema. Every one of its methods names something only an engine
//! has - a glommio task queue, the shard's channels, the storage loaders, the server config.
//!
//! It used to live in `shared/traits.rs`, where nine of its methods carried
//! `#[cfg(feature = "server")]` and four did not, and where the gating could not have worked
//! either way: the imports those signatures needed sat at module scope with no `cfg` on them at
//! all. Turning the feature off produced a build failure rather than a client. It moved here in
//! F15, and the nine attributes went with it - a trait that only exists in the engine has no use
//! for a flag saying so.
//!
//! A `#[shoal::db(client)]` schema does not implement this, and that is the whole difference
//! between a client build and a server one.

use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use uuid::Uuid;

use crate::server::messages::{LoadedPartitionKinds, QueryMetadata, ServerMsg};
use crate::server::routing::ShardRouting;
use crate::server::{Conf, ServerError};
use crate::shared::queries::{ArchivedQueries, Queries};
use crate::shared::responses::ResponseError;
use crate::shared::traits::{QuerySupport, TableNameSupport};
use crate::storage::{FullArchiveMap, LoaderMsg, Loaders, RecoveryStats};

/// The core trait that all databases in shoal must support
pub trait ShoalDatabase: 'static + Sized {
    /// This databases external client type
    ///
    /// Its queries have to be routable, because only something that owns a ring implements
    /// this trait at all. A `#[shoal::db(client)]` schema implements neither, which is what
    /// keeps [`ShardRouting`] - and therefore the ring and the shard - out of a client build.
    type ClientType: QuerySupport<QueryKinds: ShardRouting> + Sized;

    /// The different tables in this database
    type TableNames: TableNameSupport;

    /// Create a new shoal db instance
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard that owns this table
    /// * `conf` - A shoal config
    #[allow(async_fn_in_trait)]
    async fn new(
        shard_name: &str,
        shard_archive_map: &FullArchiveMap<Self::TableNames>,
        loader_channels: &mut HashMap<
            Loaders,
            (
                AsyncSender<LoaderMsg<Self::TableNames>>,
                AsyncReceiver<LoaderMsg<Self::TableNames>>,
            ),
        >,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        memory_usage: &Arc<RefCell<usize>>,
        lru: &Arc<RefCell<LruCache<(Self::TableNames, u64), usize, BuildHasherDefault<GxHasher>>>>,
        shard_local_tx: &AsyncSender<ServerMsg<Self>>,
    ) -> Result<Self, ServerError>;

    /// Initialize the different loaders for our storage kinds
    #[allow(async_fn_in_trait)]
    async fn init_storage_loaders(
        &self,
        table_map: &FullArchiveMap<Self::TableNames>,
        loader_channels: &mut HashMap<
            Loaders,
            (
                AsyncSender<LoaderMsg<Self::TableNames>>,
                AsyncReceiver<LoaderMsg<Self::TableNames>>,
            ),
        >,
        shard_local_tx: &AsyncSender<ServerMsg<Self>>,
    ) -> Result<(), ServerError>;

    /// Get what replaying every tables intent logs had to discard
    ///
    /// Tables are built before a shard finishes starting, so this is the whole of
    /// what this shards recovery lost by the time its startup summary is emitted.
    fn recovery_stats(&self) -> RecoveryStats;

    /// Build a default queries bundle
    #[must_use]
    fn queries() -> Queries<Self::ClientType> {
        Queries::default()
    }

    /// Deserialize our query types
    fn unarchive_queries(buff: &[u8]) -> &ArchivedQueries<Self::ClientType> {
        // load an archived type from a slice
        unsafe { rkyv::access_unchecked::<ArchivedQueries<Self::ClientType>>(&buff) }
    }

    /// Handle messages for different table types
    #[allow(async_fn_in_trait)]
    async fn handle(
        &mut self,
        meta: QueryMetadata,
        typed_query: <Self::ClientType as QuerySupport>::QueryKinds,
    ) -> Option<(
        Uuid,
        Uuid,
        crate::server::stage_profile::StageStamps,
        <Self::ClientType as QuerySupport>::ResponseKinds,
    )>;

    /// Mark partitions as evictable if they are no longer in the intent log
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table with the partition to mark as evictable
    /// * `generation` - The generation of data to mark as evictable
    /// * `partitions` - The partitions to mark as evictable
    fn mark_evictable(
        &mut self,
        table_name: Self::TableNames,
        generation: u64,
        partitions: Vec<u64>,
    );

    /// Evict specific partitions from a table
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to evict data from
    /// * `victims` - The partitions to evict
    #[allow(async_fn_in_trait)]
    fn evict(&mut self, table_name: Self::TableNames, victims: Vec<u64>);

    /// Flush any in flight writes to disk
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Check if any of our tables intent logs are due to be rotated
    ///
    /// The shard asks this on every message it handles, so it is deliberately
    /// synchronous — it only reads position counters and never touches storage.
    fn compaction_due(&self) -> bool;

    /// Get all flushed messages and send their response back
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed response to send back
    #[allow(async_fn_in_trait)]
    async fn handle_flushed(
        &mut self,
        flushed: &mut Vec<(
            Uuid,
            Uuid,
            tracing::Span,
            crate::server::stage_profile::StageStamps,
            <Self::ClientType as QuerySupport>::ResponseKinds,
        )>,
    ) -> Result<(), ServerError>;

    /// Load a partition and execute any pending queries
    #[allow(async_fn_in_trait)]
    async fn load_partition(
        &mut self,
        loaded: LoadedPartitionKinds<Self>,
        shard_local_tx: &AsyncSender<ServerMsg<Self>>,
    ) -> Result<(), ServerError>;

    /// Release the queries waiting on a partition that could not be read
    ///
    /// This is the other half of [`ShoalDatabase::load_partition`]: between them they cover
    /// every outcome a partition read can have, and a read whose outcome reaches neither
    /// leaves its queries parked for the life of the process.
    ///
    /// # Arguments
    ///
    /// * `table` - The table the partition that could not be read belongs to
    /// * `partition_id` - The partition that could not be read
    /// * `error` - What the released queries should answer with, if this was a failure at all
    /// * `shard_local_tx` - The channel to replay the released queries on
    #[allow(async_fn_in_trait)]
    async fn fail_partition(
        &mut self,
        table: Self::TableNames,
        partition_id: u64,
        error: Option<ResponseError>,
        shard_local_tx: &AsyncSender<ServerMsg<Self>>,
    ) -> Result<(), ServerError>;

    /// Shutdown this table and flush any data to disk if needed
    #[allow(async_fn_in_trait)]
    async fn shutdown(self) -> Result<(), ServerError>;
}
