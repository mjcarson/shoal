//! The different storage backends supported by shoal

use glommio::{io::ReadResult, TaskQueueHandle};
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::bytecheck::CheckBytes;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{de::Pool, rancor::Strategy, Archive};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod fs;

pub use fs::FileSystem;

use crate::server::messages::{QueryMetadata, ShardMsg};
use crate::server::{Conf, ServerError};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, ShoalDatabase, TableNameSupport};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};

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
#[derive(Debug, Clone)]
pub enum CompactionJob {
    /// A path to an intent log to compact
    IntentLog(PathBuf),
    /// Compact this shards archive data
    Archives,
    /// Shutdown this compactor
    Shutdown,
}

/// Whether a partition should be pruned or not
pub enum ShouldPrune {
    /// Do not prune a partition
    No,
    /// Prune a partition
    Yes,
}

pub trait IntentReadSupport<T: RkyvSupport>: Sized + RkyvSupport + PartitionSupport {
    /// The intent type to use
    type Intent: RkyvSupport;

    ///// Create a new partition
    /////
    ///// # Arguments
    /////
    ///// * `key` - The key for this new partition
    //fn new(key: u64) -> Self;

    /// Load a partition from a read and insert it into our map
    fn load(
        read: &ReadResult,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
    ) -> Result<(), ServerError>;

    /// Apply an intent to this partition
    ///
    /// This will also return whether a partition should be pruned or not.
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
    ) -> ShouldPrune;

    /// Get the partition key for a specific intent
    fn partition_key_and_intent(read: &ReadResult) -> Result<(u64, Self::Intent), ServerError>
    where
        for<'a> <Self::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >;
}

/// A map of of archives across all tables
#[derive(Debug)]
pub struct FullArchiveMap<N: TableNameSupport> {
    /// The map of archive for each table
    map: RefCell<HashMap<N, ArchiveMapKinds>>,
}

impl<N: TableNameSupport> Default for FullArchiveMap<N> {
    fn default() -> Self {
        FullArchiveMap {
            map: RefCell::new(HashMap::default()),
        }
    }
}

impl<N: TableNameSupport> FullArchiveMap<N> {
    /// Insert a new archive map into our full archive map
    pub fn insert(&self, table_name: N, map: ArchiveMapKinds) {
        self.map.borrow_mut().insert(table_name, map);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Loaders {
    /// A Filesystem loader
    FileSystem,
}

impl std::fmt::Display for Loaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Loaders::FileSystem => write!(f, "FileSystem"),
        }
    }
}

/// The different messages to and from loaders
#[derive(Debug, Clone, Copy)]
pub enum LoaderMsg<N: TableNameSupport> {
    /// A request to read a partition from disk
    Request { table_name: N, partition_id: u64 },
    /// Shutdown this loader
    Shutdown,
}

/// The different storage engine archive maps
#[derive(Debug)]
pub enum ArchiveMapKinds {
    /// The archive map for filesystem based storage engines
    FileSystem(Arc<fs::ArchiveMap>),
}

/// An storage archive map that has been filtered down a single type
pub struct FilteredFullArchiveMap<N: TableNameSupport, M> {
    /// The map of archive for each table
    map: RefCell<HashMap<N, Arc<M>>>,
}

pub trait StorageSupport: Sized {
    /// The settings for this storage engine
    type Settings;

    /// The archive map this storage engine uses
    type ArchiveMap;

    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    #[allow(async_fn_in_trait)]
    async fn new<
        P: IntentReadSupport<R> + 'static,
        R: PartitionKeySupport + 'static,
        N: TableNameSupport,
    >(
        shard_name: &str,
        table_name: N,
        shard_archive_map: &FullArchiveMap<N>,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError>
    where
        <P as Archive>::Archived: rkyv::Deserialize<P, Strategy<Pool, rkyv::rancor::Error>>,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <P as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <P::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >;

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
    ///
    /// # Arguments
    ///
    /// * `force` - Whether to force a compaction of the intent logs
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<T: PartitionKeySupport>(
        &mut self,
        force: bool,
    ) -> Result<u64, ServerError>;

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
    async fn read_intents<T: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        shard_name: &str,
        conf: &Conf,
        partitions: &mut HashMap<u64, MaybeLoaded<T>>,
    ) -> Result<(), ServerError>;

    /// Get the type of loader this storage kind requires
    fn loader_kind() -> Loaders;

    /// Spawn a loader for this storage type if not yet spawned
    #[allow(async_fn_in_trait)]
    async fn spawn_loader<D: ShoalDatabase>(
        &self,
        table_map: &FullArchiveMap<D::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ShardMsg<D>>,
    ) -> Result<(), ServerError>;

    /// Load a partition from disk if it exists
    ///
    /// Returns true if a partition exists and will be loaded from disk and
    /// false if it does not and wont.
    #[allow(async_fn_in_trait)]
    async fn load_partition<N: TableNameSupport>(
        &self,
        table_name: N,
        partition_id: u64,
        loader_tx: &AsyncSender<LoaderMsg<N>>,
    ) -> Result<bool, ServerError>;

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(&mut self) -> Result<(), ServerError>;
}
