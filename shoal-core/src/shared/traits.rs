//! The root traits that shoal is built upon that are shared between the client and server

use uuid::Uuid;

use crate::server::{ring::Ring, shard::ShardInfo};

use super::queries::Queries;

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
pub trait ShoalDatabase: Default + 'static + std::fmt::Debug + Sized {
    /// The different tables or types of queries we will handle
    type QueryKinds: ShoalQuery;

    /// The different tables we can get responses from
    type ResponseKinds: ShoalResponse;

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
}

pub trait ShoalTable:
    std::fmt::Debug
    + Clone
    + rkyv::Archive
    + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
    + Sized
{
    /// The sort type for this data
    type Sort: Ord + rkyv::Archive + std::fmt::Debug + From<Self::Sort> + Clone;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort;

    /// Calculate the partition key for this row
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key to build our partition key from
    fn partition_key(sort: &Self::Sort) -> u64;

    /// Any filters to apply when listing/crawling rows
    type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;
}
