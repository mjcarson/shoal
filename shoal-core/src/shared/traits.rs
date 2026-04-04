//! The root traits that shoal is built upon that are shared between the client and server

use deepsize2::DeepSizeOf;
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use lru::LruCache;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

mod sorted;
mod storable;
mod unsorted;

use super::queries::ArchivedQueries;
use super::queries::Queries;
use crate::client::{Errors, QuerySuceededOpts, ShqlParseError};
use crate::server::messages::{LoadedPartitionKinds, QueryMetadata, ServerMsg};
use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::server::{Conf, ServerError};
use crate::shared::queries::parser::{FieldRole, TypeValidator};
use crate::shared::responses::ResponseActionNames;
use crate::storage::{FullArchiveMap, LoaderMsg, Loaders};

pub use sorted::ShoalSortedTable;
pub use unsorted::ShoalUnsortedTable;

/// Marker trait for Exists queries
///
/// This trait is used to constrain the `exists` method on the Shoal client
/// to only accept Exists queries at compile time, preventing incorrect usage.
pub trait ExistsQuery {}

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
pub trait ShoalQuerySupport: std::fmt::Debug + RkyvSupport + Sized + Send + Clone {
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
pub trait ShoalResponseSupport: std::fmt::Debug + RkyvSupport + Sized + Send {
    /// Get the index of a single [`<Self::ResponseKinds as Archive>::Archived`]
    fn get_index_archived(archived: &<Self as Archive>::Archived) -> usize;

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(archived: &<Self as Archive>::Archived) -> bool;

    /// Get the query id from the response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived type to get our query id from
    fn get_query_id(archived: &<Self as Archive>::Archived) -> Uuid;
}

pub trait QuerySupport: 'static + Sized {
    /// The different tables or types of queries we will handle
    type QueryKinds: ShoalQuerySupport;

    /// The different tables we can get responses from
    type ResponseKinds: ShoalResponseSupport;

    /// The different tables in this database
    type TableNames: TableNameSupport;

    /// Make sure queries have succeeded based on some critiera
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived query to check
    /// * `opts` - The options to use when validating query responses
    fn succeeded(
        archived: &<Self::ResponseKinds as Archive>::Archived,
        opts: QuerySuceededOpts,
    ) -> Result<(), Errors>;

    /// Get the kind of query this is a response to
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived query to get the query kind for
    fn kind(archived: &<Self::ResponseKinds as Archive>::Archived) -> ResponseActionNames;

    /// Get the exists result from an Exists response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived response to get the exists result from
    fn get_exists(archived: &<Self::ResponseKinds as Archive>::Archived) -> Option<bool>;

    /// Parse a SHQL (Shoal Query Language) string into a query
    ///
    /// SHQL supports SQL-like SELECT queries for reading data from tables.
    /// Currently only GET queries are supported.
    ///
    /// # Syntax
    ///
    /// ```text
    /// SELECT * FROM <table_name> WHERE <partition_key> = '<value>';
    /// ```
    ///
    /// # Arguments
    ///
    /// * `query` - The SHQL query string to parse
    fn parse(query: &str) -> Result<Self::QueryKinds, ShqlParseError>;

    /// Get the table name from a query
    ///
    /// # Arguments
    ///
    /// * `query` - The query to get the table name for
    fn query_table_name(query: &Self::QueryKinds) -> Self::TableNames;

    /// Get the table name from an archived response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived response to get the table name for
    fn response_table_name(
        archived: &<Self::ResponseKinds as Archive>::Archived,
    ) -> Self::TableNames;

    /// Format an archived response into column headers and row values
    ///
    /// Returns `Some((headers, rows))` for Get responses with data,
    /// `None` for non-Get responses or empty results.
    fn format_response(
        archived: &<Self::ResponseKinds as Archive>::Archived,
    ) -> Option<(Vec<&'static str>, Vec<Vec<String>>)>;
}

pub trait TableNameSupport:
    std::fmt::Display + std::fmt::Debug + PartialEq + Eq + Ord + std::hash::Hash + Clone + Copy + Send
{
}

/// The core trait that all databases in shoal must support
pub trait ShoalDatabase: 'static + Sized {
    /// This databases external client type
    type ClientType: QuerySupport + Sized;

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
        Uuid,
        Uuid,
        <Self::ClientType as QuerySupport>::ResponseKinds,
    )>;

    /// Mark partitions as evictable if they are no longer in the intent log
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table with the partition to mark as evictable
    /// * `generation` - The generation of data to mark as evictable
    /// * `partitions` - The partitions to mark as evictable
    #[cfg(feature = "server")]
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
    #[cfg(feature = "server")]
    fn evict(&mut self, table_name: Self::TableNames, victims: Vec<u64>);

    /// Flush any in flight writes to disk
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Inform a table that some of its data has been flushed to storage
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table that we are marking a new flushed offset watermark
    /// * `flushed_pos` - The new offset for flushed data
    #[cfg(feature = "server")]
    fn mark_flushed(&mut self, table: Self::TableNames, flushed_pos: u64);

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
            Uuid,
            Uuid,
            tracing::Span,
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

    /// Shutdown this table and flush any data to disk if needed
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn shutdown(self) -> Result<(), ServerError>;
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
}

/// Schema information for a table
pub trait TableSchemaSupport {
    /// Get the type validator for a given field name
    fn get_field_validator(field_name: &str) -> Option<TypeValidator>;

    /// Get the role of a field (partition, sort, or filter)
    fn get_field_role(field_name: &str) -> Option<FieldRole>;

    /// Get all valid field names
    fn field_names() -> Vec<&'static str>;
}

/// Support for formatting table rows as displayable strings
///
/// This trait is implemented for archived table types to enable
/// formatting query results as ASCII tables.
pub trait TableRowFormat {
    /// Get the column headers for this table
    fn headers() -> Vec<&'static str>;

    /// Convert this row's field values to strings for display
    fn row_values(&self) -> Vec<String>;
}

/// The core traits that all tables require
pub trait ShoalTableSupport:
    std::fmt::Debug
    + Clone
    + RkyvSupport
    + PartitionKeySupport
    + Sized
    + DeepSizeOf
    + TableSchemaSupport
{
    /// The updates that can be applied to this table
    type Update: RkyvSupport + std::fmt::Debug + Clone;

    /// The server facing updates that can be applied to this table (just the updates no keys)
    type UpdateData: RkyvSupport + std::fmt::Debug + Clone;

    /// Any filters to apply when listing/crawling rows
    type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered_archived(
        filter: &Self::Filters,
        row: &<Self as rkyv::Archive>::Archived,
    ) -> bool;
}
