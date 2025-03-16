//! The different storage backends supported by shoal

use glommio::{io::ReadResult, TaskQueueHandle};
use rkyv::{de::Pool, rancor::Strategy, Archive};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::{collections::HashMap, path::PathBuf};

pub mod fs;

pub use fs::FileSystem;

use crate::server::messages::QueryMetadata;
use crate::server::{Conf, ServerError};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport};

#[derive(Debug)]
pub struct PendingResponse<T> {
    /// The still pending writes
    pending: VecDeque<(u64, QueryMetadata, ResponseAction<T>)>,
}

impl<T> PendingResponse<T> {
    /// Create a pending response queue with a capacity
    ///
    /// # Arguments
    ///
    /// * `capacity` - The capacity to set
    pub fn with_capacity(capacity: usize) -> Self {
        PendingResponse {
            pending: VecDeque::with_capacity(capacity),
        }
    }

    /// Add a pending response action thats data is still being flushed
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `pos` - The position at which this entry will have been flushed to disk
    /// * `response` - The pending response action
    pub fn add(&mut self, meta: QueryMetadata, pos: u64, response: ResponseAction<T>) {
        // add this pending action to our pending queue
        self.pending.push_back((pos, meta, response));
    }

    /// Get all responses that have had their data committed to disk
    pub fn get(&mut self, flushed_pos: u64, flushed: &mut Vec<(SocketAddr, Response<T>)>) {
        // keep popping response actions until we find one that isn't yet flushed
        // or we have no more response actions to check
        while !self.pending.is_empty() {
            // check if the first item has been flushed
            let is_flushed = match self.pending.front() {
                Some((pending_pos, _, _)) => flushed_pos >= *pending_pos,
                None => break,
            };
            // if this action has been flushed to disk then pop it
            if is_flushed {
                // pop this flushed action
                if let Some((_, meta, data)) = self.pending.pop_front() {
                    // build the response for this query
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data,
                        end: meta.end,
                    };
                    // add this action to our flushed vec
                    flushed.push((meta.addr, response));
                }
            } else {
                // we don't have any flushed data yet
                break;
            }
        }
    }
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

pub trait IntentReadSupport: Sized + RkyvSupport {
    /// The intent type to use
    type Intent: RkyvSupport;

    ///// Create a new partition
    /////
    ///// # Arguments
    /////
    ///// * `key` - The key for this new partition
    //fn new(key: u64) -> Self;

    /// Load a partition from a read and insert it into our map
    fn load(read: &ReadResult, partitions: &mut HashMap<u64, Self>) -> Result<(), ServerError>;

    /// Apply an intent to this partition
    ///
    /// This will also return true or false if this partition should be pruned.
    fn apply_intents(loaded: &mut HashMap<u64, Self>, key: u64, intents: Vec<Self::Intent>)
        -> bool;

    /// Get the partition key for a specific intent
    fn partition_key_and_intent(read: &ReadResult) -> Result<(u64, Self::Intent), ServerError>;
}

pub trait StorageSupport: Sized {
    /// The settings for this storage engine
    type Settings;

    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    #[allow(async_fn_in_trait)]
    async fn new<P: IntentReadSupport + 'static, R: PartitionKeySupport + 'static>(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError>
    where
        <P as Archive>::Archived: rkyv::Deserialize<P, Strategy<Pool, rkyv::rancor::Error>>,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>;

    /// Get a tables config or use default settings
    ///
    /// # Arguments
    ///
    /// * `conf` - The shoal config to get settings from
    fn get_settings<T: PartitionKeySupport>(conf: &Conf) -> Result<Self::Settings, ServerError>;

    /// Commit an operation to this storages intent log
    ///
    /// # Arguments
    ///
    /// * `data` - The data to commit
    #[allow(async_fn_in_trait)]
    async fn commit<D: RkyvSupport>(&mut self, data: &D) -> Result<u64, ServerError>;

    /// Set our intent log to be compact if its needed
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<T: PartitionKeySupport>(&mut self) -> Result<u64, ServerError>;

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard to read intents for
    /// * `conf` - A Shoal config
    /// * `path` - The path to the intent log to read in
    #[allow(async_fn_in_trait)]
    async fn read_intents<T: IntentReadSupport, R: PartitionKeySupport>(
        shard_name: &str,
        conf: &Conf,
        partitions: &mut HashMap<u64, T>,
    ) -> Result<(), ServerError>;

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(&mut self) -> Result<(), ServerError>;
}
