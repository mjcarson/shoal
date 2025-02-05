//! The different storage backends supported by shoal

use glommio::TaskQueueHandle;
use kanal::AsyncReceiver;
use rkyv::{de::Pool, rancor::Strategy, Archive, Deserialize, Portable, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use crate::{
    server::{messages::QueryMetadata, Conf, ServerError},
    shared::{
        queries::Update,
        responses::{Response, ResponseAction},
        traits::ShoalTable,
    },
};

mod fs;

pub use fs::FileSystem;

use super::partitions::Partition;

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(u8)]
pub enum Intents<T: ShoalTable> {
    Insert(T),
    Delete {
        partition_key: u64,
        sort_key: T::Sort,
    },
    Update(Update<T>),
}

/// A compaction job
pub enum CompactionJob {
    /// A path to an intent log to compact
    IntentLog(PathBuf),
    /// Compact this shards archive data
    Archives,
    /// Shutdown this compactor
    Shutdown,
}

pub trait ShoalStorage<T: ShoalTable>: Sized {
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    #[allow(async_fn_in_trait)]
    async fn new(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError>;

    /// Write this new row to storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to insert
    #[allow(async_fn_in_trait)]
    async fn insert(&mut self, insert: &Intents<T>) -> Result<u64, ServerError>;

    /// Delete a row from storage
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key to the partition we are deleting data from
    /// * `sort_key` - The sort key to use to delete data from with in a partition
    #[allow(async_fn_in_trait)]
    async fn delete(&mut self, partition_key: u64, sort_key: T::Sort) -> Result<u64, ServerError>;

    /// Write a row update to storage
    ///
    /// # Arguments
    ///
    /// * `update` - The update that was applied to our row
    #[allow(async_fn_in_trait)]
    async fn update(&mut self, update: Update<T>) -> Result<u64, ServerError>;

    /// Add a pending response action thats data is still being flushed
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `pos` - The position at which this entry will have been flushed to disk
    /// * `response` - The pending response action
    fn add_pending(&mut self, meta: QueryMetadata, pos: u64, response: ResponseAction<T>);

    /// Get all flushed response actions
    ///
    /// # Arguments
    ///
    /// * `flushed` - The response to return
    #[allow(async_fn_in_trait)]
    async fn get_flushed(
        &mut self,
        flushed: &mut Vec<(SocketAddr, Response<T>)>,
    ) -> Result<(), ServerError>;

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    #[allow(async_fn_in_trait)]
    async fn read_intents(
        path: &PathBuf,
        partitions: &mut HashMap<u64, Partition<T>>,
    ) -> Result<(), ServerError>;

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(&mut self) -> Result<(), ServerError>;
}
