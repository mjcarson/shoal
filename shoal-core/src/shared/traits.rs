//! The root traits that shoal is built upon that are shared between the client and server

use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec, Archive, Deserialize, Serialize,
};
use std::path::PathBuf;

use uuid::Uuid;

use crate::server::{ring::Ring, shard::ShardInfo, Conf};

use super::queries::Queries;

/// The trait required for a type to be archivable
pub trait Archivable:
    std::fmt::Debug
    + Sized
    + rkyv::Archive
    + rkyv::Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<1024>, AllocScratch>,
            SharedSerializeMap,
        >,
    >
{
    fn deserialize(archived: &<Self as Archive>::Archived) -> Self;
    //    archived.deserialize(&mut rkyv::Infallible).unwrap()
    //}
    //fn archive(&self) -> AlignedVec {
    //    // archive this row
    //    rkyv::to_bytes::<_, 1024>(&self).unwrap()
    //}
}

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
    async fn new(shard_name: &str, conf: &Conf) -> Self;

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

///// The methods required to read/write data from shoal storage
//pub trait ShoalStorable {
//    /// Serialize a new row of data into an insert entry
//    async fn insert(&self) ->
//}
//
//pub trait ShoalStorage {
//    /// The type we are storing
//    type Data: ShoalStorable;
//    /// Create a new instance of this storage engine
//    ///
//    /// # Arguments
//    ///
//    /// * `shard_name` - The id of the shard that owns this table
//    /// * `conf` - The Shoal config
//    async fn new(shard_name: &str, conf: &Conf) -> Self;
//
//    /// Write this row to our storage
//    ///
//    /// # Arguments
//    ///
//    /// * `data` - The data to write
//    async fn write(&mut self, data: &[u8]);
//
//    /// Add a new row to storage
//    async fn add(&mut self, )
//
//    /// Flush all currently pending writes to storage
//    async fn flush(&mut self);
//
//    /// Read an intent log from storage
//    ///
//    /// # Arguments
//    ///
//    /// * `path` - The path to the intent log to read in
//    async fn read_intents(path: &PathBuf);
//}
