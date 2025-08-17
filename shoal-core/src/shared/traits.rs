//! The root traits that shoal is built upon that are shared between the client and server

use glommio::TaskQueueHandle;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::instrument;
use uuid::Uuid;

mod storable;
mod unsorted;

use super::queries::ArchivedQueries;
use super::queries::{Queries, SortedUpdate};
use crate::server::messages::{LoadedPartitionKinds, QueryMetadata, ShardMsg};
use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::server::{Conf, ServerError};
use crate::storage::{FullArchiveMap, LoaderMsg, Loaders};

pub use unsorted::ShoalUnsortedTable;

impl RkyvSupport for String {}

pub trait RkyvSupport: Archive
    + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
    + Sized
{
    /// Archive this type to an aligned vec
    fn serialize(&self) -> AlignedVec {
        rkyv::to_bytes::<Error>(self).unwrap()
    }

    /// Load our archived type from a slice
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw bytes to load an archive from
    #[instrument(name = "RkyvSupport::access", skip_all, err(Debug))]
    fn access(raw: &[u8]) -> Result<&<Self as Archive>::Archived, rkyv::rancor::Error>
    where
        for<'a> <Self as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        // load an archived type from a slice
        rkyv::access::<<Self as Archive>::Archived, rkyv::rancor::Error>(raw)
    }

    /// Deserialize our archived type
    fn deserialize(archived: &<Self as Archive>::Archived) -> Result<Self, rkyv::rancor::Error>
    where
        <Self as Archive>::Archived: rkyv::Deserialize<Self, Strategy<Pool, rkyv::rancor::Error>>,
    {
        rkyv::deserialize::<Self, rkyv::rancor::Error>(archived)
    }
}

/// The traits for queries in shoal
pub trait ShoalQuery: std::fmt::Debug + RkyvSupport + Sized + Send + Clone {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> Result<&Uuid, rkyv::rancor::Error>;

    /// Find the right shards for this query
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The shards we found for this query
    fn find_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<&'a ShardInfo>);
}

/// The traits ror responses from shoal
pub trait ShoalResponse: std::fmt::Debug + RkyvSupport + Sized + Send {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index(&self) -> usize;

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool;

    /// Get the query id from the response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived type to get our query id from
    fn get_query_id(archived: &<Self as Archive>::Archived) -> Uuid;
}

pub trait QuerySupport: 'static {
    /// The different tables or types of queries we will handle
    type QueryKinds: ShoalQuery;

    /// The different tables we can get responses from
    type ResponseKinds: ShoalResponse;
}

pub trait TableNameSupport:
    std::fmt::Display + std::fmt::Debug + PartialEq + Eq + Ord + std::hash::Hash + Clone + Copy
{
}

/// The core trait that all databases in shoal must support
pub trait ShoalDatabase: 'static + Sized {
    /// This databases external client type
    type ClientType: QuerySupport;

    /// The different tables in this database
    type TableNames: TableNameSupport;

    ///// The different tables or types of queries we will handle
    //type QueryKinds: ShoalQuery;

    ///// The different tables we can get responses from
    //type ResponseKinds: ShoalResponse;

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
        shard_local_tx: &AsyncSender<ShardMsg<Self>>,
    ) -> Result<(), ServerError>;

    /// Build a default queries bundle
    #[must_use]
    fn queries() -> Queries<Self::ClientType> {
        Queries::default()
    }

    /// Deserialize our query types
    #[cfg(feature = "server")]
    fn unarchive_queries(buff: &[u8]) -> &ArchivedQueries<Self::ClientType> {
        // load an archived type from a slice
        unsafe { rkyv::access_unchecked::<ArchivedQueries<Self::ClientType>>(&buff) }
    }

    /// Handle messages for different table types
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn handle(
        &mut self,
        meta: QueryMetadata,
        typed_query: <Self::ClientType as QuerySupport>::QueryKinds,
    ) -> Option<(
        SocketAddr,
        <Self::ClientType as QuerySupport>::ResponseKinds,
    )>;

    /// Flush any in flight writes to disk
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Get all flushed messages and send their response back
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed response to send back
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn handle_flushed(
        &mut self,
        flushed: &mut Vec<(
            SocketAddr,
            <Self::ClientType as QuerySupport>::ResponseKinds,
        )>,
    ) -> Result<(), ServerError>;

    /// Load a partition and execute any pending queries
    #[allow(async_fn_in_trait)]
    async fn load_partition(
        &mut self,
        loaded: LoadedPartitionKinds<Self>,
        shard_local_tx: &AsyncSender<ShardMsg<Self>>,
    ) -> Result<(), ServerError>;

    /// Shutdown this table and flush any data to disk if needed
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn shutdown(&mut self) -> Result<(), ServerError>;
}

pub trait PartitionKeySupport: std::fmt::Debug + Clone + RkyvSupport + Sized {
    /// The partition key type for this data
    type PartitionKey;

    /// The name of this table
    fn name() -> &'static str;

    /// Get this rows partition key
    fn get_partition_key(&self) -> u64;

    /// Calculate the partition key for this row for this rows sort key
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key to build our partition key from
    fn get_partition_key_from_values(sort: &Self::PartitionKey) -> u64;

    /// Get the partition key for this row from an archived value
    fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64;

    ///// Get the table name for this data type
    //fn get_table_name<S: ShoalDatabase>() -> S::TableNames;
}

pub trait ShoalSortedTable:
    std::fmt::Debug + Clone + RkyvSupport + PartitionKeySupport + Sized
{
    /// The updates that can be applied to this table
    type Update: RkyvSupport + std::fmt::Debug + Clone;

    /// The sort type for this data
    type Sort: Ord + RkyvSupport + std::fmt::Debug + From<Self::Sort> + Clone;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort;

    /// Any filters to apply when listing/crawling rows
    type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;

    /// Apply an update to a single row
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a specific row
    fn update(&mut self, update: &SortedUpdate<Self>);
}
