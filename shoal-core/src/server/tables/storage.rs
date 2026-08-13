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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::Span;
use uuid::Uuid;

pub mod fs;
pub mod none;

pub use fs::FileSystem;
pub use none::NoStorage;

use crate::server::messages::{QueryMetadata, ServerMsg};
use crate::server::stage_profile::{StageDurability, StageStamps};
use crate::server::{Conf, ServerError};
use crate::shared::responses::{Response, ResponseAction};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, ShoalDatabase, TableNameSupport};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};

/// An unblocked response
#[derive(Debug)]
pub struct UnblockedResponse<T> {
    /// The id for the client this response is for
    pub client_id: Uuid,
    /// The id of the query this response is for
    pub query_id: Uuid,
    /// The span this response is from
    pub span: Span,
    /// The response to return
    pub response: Response<T>,
}

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

    /// Release every pending response regardless of position
    ///
    /// This is only correct at an intent log rotation, where the writer has already
    /// fdatasynced everything it ever wrote to the old file. Positions restart at 0
    /// in the new file, so comparing an old position against a new watermark is
    /// meaningless and the queue has to be drained instead of tested.
    ///
    /// # Arguments
    ///
    /// * `flushed` - The vec to write our released responses too
    pub fn drain_all(&mut self, flushed: &mut Vec<(Uuid, Uuid, Span, StageStamps, Response<T>)>) {
        // every pending response is durable so release all of them
        for (_, mut meta, data) in self.pending.drain(..) {
            // note that this response came out on a rotation rather than on a watermark
            //
            // a rotation fdatasynced the old log and threw its durability windows away with
            // it, so these records carry no durability stages at all. Flagging them keeps
            // them a separate population in the profile instead of an interpolated one.
            meta.stamps.set_rotated(true);
            // record when this response was released back to the shard
            meta.stamps.mark_released();
            // build the response for this query
            let response = Response {
                id: meta.id,
                index: meta.index,
                data,
                end: meta.end,
            };
            // add this action to our flushed vec
            flushed.push((meta.client, meta.id, meta.span, meta.stamps, response));
        }
    }

    /// Get all responses that have had their data committed to disk
    ///
    /// # Arguments
    ///
    /// * `flushed_pos` - The position that all data below is durable at
    /// * `flushed` - The vec to write our released responses too
    pub fn get(
        &mut self,
        flushed_pos: u64,
        flushed: &mut Vec<(Uuid, Uuid, Span, StageStamps, Response<T>)>,
    ) {
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
                if let Some((pos, mut meta, data)) = self.pending.pop_front() {
                    // remember the offset this query became durable at
                    //
                    // the durability phases are intervals of the log rather than properties
                    // of a query, so this offset is the only thing that can match a released
                    // response back to the write that carried it
                    meta.stamps.set_commit_pos(pos);
                    // record when this response was released back to the shard
                    meta.stamps.mark_released();
                    // build the response for this query
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data,
                        end: meta.end,
                    };
                    // add this action to our flushed vec
                    flushed.push((meta.client, meta.id, meta.span, meta.stamps, response));
                }
            } else {
                // we don't have any flushed data yet
                break;
            }
        }
    }
}

/// How far a tables intent log has been made durable
#[derive(Debug, Clone, Copy)]
pub struct FlushProgress {
    /// The position that all data below is durably on disk at
    pub durable_pos: u64,
    /// The current generation of this tables intent log
    pub generation: u64,
    /// Whether this check rotated the intent log
    ///
    /// A rotation fdatasyncs everything in the old log and then restarts positions
    /// at 0, so callers have to release their pending responses rather than compare
    /// their old positions against a new files watermark.
    pub rotated: bool,
}

/// A compaction job
#[derive(Debug, Clone)]
pub enum CompactionJob {
    /// A path to an intent log to compact
    IntentLog { path: PathBuf, generation: u64 },
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

/// Counts of everything recovery discarded for a table
///
/// Three of these mean data was lost and one does not, which is the whole point of
/// keeping them apart: an update that lands on a row a delete already tombstoned is
/// correctly dropped, and counting it as loss would make the numbers that do mean
/// loss useless. Only [`RecoveryStats::is_clean`] decides which is which.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Update intents whose base partition was in no archive and in no earlier log
    pub orphaned_updates: u64,
    /// Update intents that targeted a row a delete had already tombstoned
    ///
    /// This is not data loss - the row was meant to be gone.
    pub updates_after_delete: u64,
    /// Intent log entries that could not be replayed at all
    pub unreplayable_entries: u64,
    /// Intent logs whose reader stopped early on a truncated or corrupt tail
    pub truncated_logs: u64,
}

impl RecoveryStats {
    /// Check whether recovery discarded anything that counted as data
    #[must_use]
    pub fn is_clean(&self) -> bool {
        // an update dropped onto a deleted row is not loss, so it does not count here
        self.orphaned_updates == 0 && self.unreplayable_entries == 0 && self.truncated_logs == 0
    }

    /// Add another set of counts to this one
    ///
    /// # Arguments
    ///
    /// * `other` - The counts to add to ours
    pub fn merge(&mut self, other: Self) {
        // saturate rather than wrap, since a wrapped counter reads as a clean startup
        self.orphaned_updates = self.orphaned_updates.saturating_add(other.orphaned_updates);
        self.updates_after_delete = self
            .updates_after_delete
            .saturating_add(other.updates_after_delete);
        self.unreplayable_entries = self
            .unreplayable_entries
            .saturating_add(other.unreplayable_entries);
        self.truncated_logs = self.truncated_logs.saturating_add(other.truncated_logs);
    }
}

pub trait IntentReadSupport<T: RkyvSupport>: Sized + RkyvSupport + PartitionSupport {
    /// The intent type to use
    type Intent: RkyvSupport;

    /// Name the partitions an intent needs loaded before it can be replayed
    ///
    /// This only collects keys, it does not load them. The load is done once for
    /// every log at a time when no replay has happened yet, since a load that runs
    /// after a replay overwrites what that replay built.
    ///
    /// # Arguments
    ///
    /// * `read` - The intent to name the partitions of
    /// * `to_load` - The set of partition keys to add to
    fn scan_keys(read: &ReadResult, to_load: &mut HashSet<u64>) -> Result<(), ServerError>;

    /// Load a intent from a read and insert it into our map
    ///
    /// # Arguments
    ///
    /// * `read` - The archived intent to load
    /// * `generation` - The generation to load these intents as
    /// * `partitions` - The map to load our intents into
    /// * `memory_usage` - The total memory usage of of this shard
    /// * `stats` - The counts of what this recovery has discarded
    fn replay(
        read: &ReadResult,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<Self>>,
        memory_usage: &mut Arc<RefCell<usize>>,
        stats: &mut RecoveryStats,
    ) -> Result<(), ServerError>;

    /// Apply an intent to this partition
    ///
    /// This will also return whether a partition should be pruned or not.
    ///
    /// # Arguments
    ///
    /// * `loaded` - The partitions loaded from their current archives
    /// * `key` - The key of the partition to apply intents to
    /// * `intents` - The intents to apply to this partition
    /// * `stats` - The counts of what this compaction has discarded
    fn apply_intents(
        loaded: &mut HashMap<u64, Self>,
        key: u64,
        intents: Vec<Self::Intent>,
        stats: &mut RecoveryStats,
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
    /// No loader at all
    ///
    /// A storage engine that never writes has nothing to read back, so it has no loader to
    /// spawn. This is a variant rather than an `Option` because the shard keys the set of
    /// loaders it has already spawned on this type, and a table reporting a kind it does not
    /// actually need would claim that kinds slot without filling it.
    None,
}

impl std::fmt::Display for Loaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Loaders::FileSystem => write!(f, "FileSystem"),
            Loaders::None => write!(f, "None"),
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

    /// The database type this storage engine is associated with
    type Database: ShoalDatabase;

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
        shard_table_name: <Self::Database as ShoalDatabase>::TableNames,
        shard_archive_map: &FullArchiveMap<N>,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        shard_local_tx: &AsyncSender<ServerMsg<Self::Database>>,
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

    /// Get how this storage engine makes a committed intent durable
    ///
    /// A stage record carries this rather than letting a report assume the default, since
    /// an engine acknowledging on a landed write has no fdatasync stage at all and showing
    /// one for it would be fiction.
    fn durability(&self) -> StageDurability;

    /// Fill in the durability stages for a response that has just been released
    ///
    /// The phases between a commit returning and its response coming back are intervals of
    /// the intent log rather than properties of a query, so they are looked up here by the
    /// offset the response parked at instead of being stamped as they happen.
    ///
    /// # Arguments
    ///
    /// * `stamps` - The stamps to fill in, already carrying their commit offset
    #[cfg(feature = "stage-profile")]
    fn fill_durability(&self, stamps: &mut StageStamps);

    /// Check if this intent log has grown past the size it rotates at
    ///
    /// This is the synchronous half of [`StorageSupport::compact_if_needed`], split out
    /// so a caller can ask whether a rotation is due without paying for the async sweep
    /// that performs one. It has to stay cheap enough to call on every message.
    fn compaction_due(&self) -> bool;

    /// Set our intent log to be compact if its needed
    ///
    /// Returns how far this tables intent log has been made durable
    ///
    /// # Arguments
    ///
    /// * `force` - Whether to force a compaction of the intent logs
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<T: PartitionKeySupport>(
        &mut self,
        force: bool,
    ) -> Result<FlushProgress, ServerError>;

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Read an intent log from storage
    ///
    /// Returns the counts of everything this recovery had to discard.
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard to read intents for
    /// * `conf` - A Shoal config
    /// * `partitions` - The map of partitions to load read our intent data into
    /// * `memory_usage` - The memory usage for this node
    #[allow(async_fn_in_trait)]
    async fn read_intents<T: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        &self,
        conf: &Conf,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<T>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<RecoveryStats, ServerError>
    where
        // a partition read during recovery is validated as it is wrapped, so this is where
        // the bound that used to sit on each individual read now lives
        for<'a> <T as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >;

    /// Get the type of loader this storage kind requires
    fn loader_kind() -> Loaders;

    /// Spawn a loader for this storage type if not yet spawned
    #[allow(async_fn_in_trait)]
    async fn spawn_loader(
        &self,
        table_map: &FullArchiveMap<<Self::Database as ShoalDatabase>::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<<Self::Database as ShoalDatabase>::TableNames>>,
        shard_local_tx: &AsyncSender<ServerMsg<Self::Database>>,
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

    /// Load a partition from disk if it exists directly
    ///
    /// This doesn't use the loader channel and instead returns the Partition data.
    #[allow(async_fn_in_trait)]
    async fn load_partition_direct(
        &self,
        partition_id: u64,
    ) -> Result<Option<ReadResult>, ServerError>;

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(self) -> Result<(), ServerError>;
}

#[cfg(test)]
mod tests {
    use super::{PendingResponse, QueryMetadata, RecoveryStats};
    use crate::server::stage_profile::{StageStamps, Stamp};
    use crate::shared::responses::ResponseAction;
    use tracing::Span;
    use uuid::Uuid;

    /// Build a pending response queue holding one entry per position
    ///
    /// # Arguments
    ///
    /// * `positions` - The durable positions to park entries at
    fn queue_at(positions: &[u64]) -> PendingResponse<()> {
        // build a queue big enough to hold every entry
        let mut pending = PendingResponse::with_capacity(positions.len());
        // park one insert response at each position
        for (index, pos) in positions.iter().enumerate() {
            let meta = QueryMetadata {
                client: Uuid::new_v4(),
                id: Uuid::new_v4(),
                index,
                end: false,
                // only gets and exists are ever split across shards, and this queue
                // only ever holds the responses to writes
                gather: None,
                span: Span::none(),
                // these entries are built by hand rather than routed, so they carry stamps
                // based at now rather than at a socket read
                stamps: StageStamps::new(Stamp::now()),
                // nothing here replays a query, so no read is ever exempted
                skip_disk: None,
            };
            pending.add(meta, *pos, ResponseAction::Insert(true));
        }
        pending
    }

    #[test]
    /// A response is released exactly when the watermark reaches its position
    fn releases_at_its_own_position() {
        let mut pending = queue_at(&[512]);
        let mut flushed = Vec::new();
        // one byte short of this entry is not enough to release it
        pending.get(511, &mut flushed);
        assert!(flushed.is_empty());
        // reaching its position exactly releases it
        pending.get(512, &mut flushed);
        assert_eq!(flushed.len(), 1);
    }

    #[test]
    /// A watermark part way through the queue releases only what it covers
    fn releases_only_what_is_durable() {
        let mut pending = queue_at(&[512, 1024, 1536]);
        let mut flushed = Vec::new();
        // this watermark covers the first two entries but not the third
        pending.get(1024, &mut flushed);
        assert_eq!(flushed.len(), 2);
        // the third comes out once our watermark reaches it
        pending.get(1536, &mut flushed);
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    /// Staging more responses never makes anything releasable on its own
    ///
    /// This is what lets the shard skip its flushed sweep on a write message. A write
    /// query parks its response above the current watermark and nothing else, so the
    /// only event that can turn a pending response into a releasable one is the
    /// watermark moving — which only ever happens behind a completed IO, and every
    /// completed IO sends a `DataFlushed`. If staging could release, gating the sweep
    /// on that message would strand responses until the next unrelated wakeup.
    fn staging_a_response_releases_nothing() {
        let mut pending = queue_at(&[512]);
        let mut flushed = Vec::new();
        // our first entry is not covered by the watermark it was staged against
        pending.get(0, &mut flushed);
        assert!(flushed.is_empty());
        // parking three more writes behind it changes nothing about that
        for (index, pos) in [1024, 1536, 2048].into_iter().enumerate() {
            let meta = QueryMetadata {
                client: Uuid::new_v4(),
                id: Uuid::new_v4(),
                index,
                end: false,
                gather: None,
                span: Span::none(),
                // these entries are built by hand rather than routed, so they carry stamps
                // based at now rather than at a socket read
                stamps: StageStamps::new(Stamp::now()),
                // nothing here replays a query, so no read is ever exempted
                skip_disk: None,
            };
            pending.add(meta, pos, ResponseAction::Insert(true));
            // the watermark has not moved, so neither has what is releasable
            pending.get(0, &mut flushed);
            assert!(flushed.is_empty());
        }
        // only advancing the watermark releases anything
        pending.get(1536, &mut flushed);
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    /// Nothing is released while the watermark is still at zero
    fn nothing_is_released_before_any_io_lands() {
        let mut pending = queue_at(&[512, 1024]);
        let mut flushed = Vec::new();
        // this is the case the old hardcoded `Ok(0)` broke: with every entry parked
        // at position 0 the watermark test was trivially true and acknowledged
        // everything before any IO had completed
        pending.get(0, &mut flushed);
        assert!(flushed.is_empty());
    }

    #[test]
    /// Draining releases every entry regardless of position
    fn drain_all_releases_everything() {
        let mut pending = queue_at(&[512, 1024, 1536]);
        let mut flushed = Vec::new();
        // rotation makes all of these durable at once
        pending.drain_all(&mut flushed);
        assert_eq!(flushed.len(), 3);
        // draining again yields nothing since the queue is now empty
        pending.drain_all(&mut flushed);
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    /// Merging accumulates every count
    fn recovery_stats_merge_accumulates() {
        // start from one tables counts
        let mut stats = RecoveryStats {
            orphaned_updates: 1,
            updates_after_delete: 2,
            unreplayable_entries: 3,
            truncated_logs: 4,
        };
        // fold in a second tables counts
        stats.merge(RecoveryStats {
            orphaned_updates: 10,
            updates_after_delete: 20,
            unreplayable_entries: 30,
            truncated_logs: 40,
        });
        assert_eq!(stats.orphaned_updates, 11);
        assert_eq!(stats.updates_after_delete, 22);
        assert_eq!(stats.unreplayable_entries, 33);
        assert_eq!(stats.truncated_logs, 44);
        // a merge that would overflow saturates, since a wrapped counter would
        // read as a shard that recovered cleanly
        stats.merge(RecoveryStats {
            orphaned_updates: u64::MAX,
            updates_after_delete: u64::MAX,
            unreplayable_entries: u64::MAX,
            truncated_logs: u64::MAX,
        });
        assert_eq!(stats.orphaned_updates, u64::MAX);
        assert_eq!(stats.truncated_logs, u64::MAX);
    }

    #[test]
    /// Only the counts that mean data was lost make a recovery unclean
    fn recovery_stats_is_clean_ignores_deleted_rows() {
        // a recovery that discarded nothing is clean
        assert!(RecoveryStats::default().is_clean());
        // an update dropped onto a row a delete already took is the delete working,
        // so it must not make a healthy startup look like it lost something
        let after_delete = RecoveryStats {
            updates_after_delete: 5,
            ..RecoveryStats::default()
        };
        assert!(after_delete.is_clean());
        // each of the other three does mean data we no longer have
        for stats in [
            RecoveryStats {
                orphaned_updates: 1,
                ..RecoveryStats::default()
            },
            RecoveryStats {
                unreplayable_entries: 1,
                ..RecoveryStats::default()
            },
            RecoveryStats {
                truncated_logs: 1,
                ..RecoveryStats::default()
            },
        ] {
            assert!(!stats.is_clean(), "{stats:?} should not be clean");
        }
    }
}
