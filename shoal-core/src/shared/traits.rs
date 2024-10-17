//! The root traits that shoal is built upon that are shared between the client and server

use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec, Archive,
};
use uuid::Uuid;

use super::queries::{Queries, Update};
use crate::server::ring::Ring;
use crate::server::shard::ShardInfo;
use crate::server::{Conf, ServerError};

/// The traits for queries in shoal
pub trait ShoalQuery:
    std::fmt::Debug
    + rkyv::Archive
    + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
    + Sized
    + Send
    + Clone
{
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> &Uuid;

    /// Find the right shards for this query
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The shards we found for this query
    fn find_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<&'a ShardInfo>);
}

/// The traits ror responses from shoal
pub trait ShoalResponse:
    std::fmt::Debug
    + rkyv::Archive
    + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
    + Sized
    + Send
{
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index(&self) -> usize;

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool;
}

/// The core trait that all databases in shoal must support
pub trait ShoalDatabase: 'static + Sized {
    /// The different tables or types of queries we will handle
    type QueryKinds: ShoalQuery;

    /// The different tables we can get responses from
    type ResponseKinds: ShoalResponse;

    /// Create a new shoal db instance
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard that owns this table
    /// * `conf` - A shoal config
    #[allow(async_fn_in_trait)]
    async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError>;

    /// Build a default queries bundle
    #[must_use]
    fn queries() -> Queries<Self> {
        Queries::default()
    }

    /// Deserialize our query types
    #[cfg(feature = "server")]
    fn unarchive(buff: &[u8]) -> Queries<Self>;

    // Deserialize our response types
    fn unarchive_response(buff: &[u8]) -> Self::ResponseKinds;

    /// Handle messages for different table types
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn handle(
        &mut self,
        id: Uuid,
        index: usize,
        typed_query: Self::QueryKinds,
        end: bool,
    ) -> Self::ResponseKinds;

    /// Shutdown this table and flush any data to disk if needed
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn shutdown(&mut self) -> Result<(), ServerError>;
}

pub trait ShoalTable:
    std::fmt::Debug
    + Clone
    + rkyv::Archive
    + rkyv::Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<1024>, AllocScratch>,
            SharedSerializeMap,
        >,
    > + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
    + Sized
{
    /// The partition key type for this data
    type PartitionKey;

    /// The updates that can be applied to this table
    type Update: rkyv::Archive
        + rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<1024>, AllocScratch>,
                SharedSerializeMap,
            >,
        > + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
        + std::fmt::Debug
        + Clone;

    /// The sort type for this data
    type Sort: Ord
        + rkyv::Archive
        + rkyv::Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<1024>, AllocScratch>,
                SharedSerializeMap,
            >,
        > + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
        + std::fmt::Debug
        + From<Self::Sort>
        + Clone;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort;

    /// Get this rows partition key
    fn get_partition_key(&self) -> u64;

    /// Calculate the partition key for this row for this rows sort key
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key to build our partition key from
    fn get_partition_key_from_values(sort: &Self::PartitionKey) -> u64;

    /// Deserialize a row from its archived format
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived data to deserialize
    fn deserialize(archived: &<Self as Archive>::Archived) -> Self;

    /// Deserialize a sort key from its archived format
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived data to deserialize
    fn deserialize_sort(archived: &<Self::Sort as Archive>::Archived) -> Self::Sort;

    /// Deserialize an update from its archived format
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived data to deserialize
    fn deserialize_update(archived: &<Update<Self> as Archive>::Archived) -> Update<Self>;

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
    fn update(&mut self, update: &Update<Self>);
}
