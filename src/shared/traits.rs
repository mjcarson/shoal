//! The root traits that shoal is built upon that are shared between the client and server

use std::net::SocketAddr;
use uuid::Uuid;

use super::queries::Queries;

/// The core trait that all databases in shoal must support
pub trait ShoalDatabase: Default + 'static + std::fmt::Debug + Sized {
    /// The different tables or types of queries we will handle
    type QueryKinds: std::fmt::Debug
        + rkyv::Archive
        + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
        + Sized
        + Send
        + Clone;

    /// The different tables we can get responses from
    type ResponseKinds: std::fmt::Debug
        + rkyv::Archive
        + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
        + Sized
        + Send;

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

    // Deserialize our response types
    fn response_query_id(buff: &[u8]) -> &Uuid;

    /// Get the index of a single [`Self::ResponseKinds`]
    ///
    /// # Arguments
    ///
    /// * `resp` - The response to get the order index for
    fn response_index(resp: &Self::ResponseKinds) -> usize;

    /// Get whether this is the last response in a response stream
    ///
    /// # Arguments
    ///
    /// * `resp` - The response to check
    fn is_end_of_stream(resp: &Self::ResponseKinds) -> bool;

    /// Forward our queries to the correct shards
    #[allow(async_fn_in_trait)]
    #[cfg(feature = "server")]
    async fn send_to_shard(
        ring: &crate::server::ring::Ring,
        mesh_tx: &mut glommio::channels::channel_mesh::Senders<
            crate::server::messages::MeshMsg<Self>,
        >,
        addr: SocketAddr,
        queries: Queries<Self>,
    ) -> Result<(), crate::server::ServerError>;

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
