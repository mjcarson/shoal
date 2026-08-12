//! The storage engine that stores nothing
//!
//! This is what makes a table ephemeral. A table is generic over its storage engine, so an
//! in-memory table is not a different table at all — it is the same table with an engine that
//! never opens a file. Every layer above this one is byte identical to the persistent table it
//! is compared against, which is exactly what makes an ephemeral benchmark a control for a
//! persistent one: the only difference between the two is what happens below this line.
//!
//! # What it still costs
//!
//! Removing the disk does not remove the machinery built around having one. A row inserted into
//! an ephemeral table is still wrapped in an intent, still parked in a `PendingResponse` and
//! still released on a shard sweep rather than inline, and its partition is still held behind a
//! `MaybeLoaded`. An ephemeral table is a persistent table with the disk taken out, not a bare
//! `BTreeMap`.
//!
//! # The two behaviours that are load bearing
//!
//! Both are documented on the methods that implement them, and both are invariants rather than
//! choices:
//!
//! - [`NoStorage::compaction_due`] is the only thing that ever wakes the shard up to release a
//!   parked response. Get it wrong and an insert hangs forever.
//! - `NoStorage` never sends a
//!   [`MarkEvictable`](crate::server::messages::ServerMsg::MarkEvictable), which is the only
//!   reason ephemeral data is safe. Get it wrong and eviction is silent data loss.

use glommio::io::ReadResult;
use glommio::TaskQueueHandle;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use super::{FlushProgress, IntentReadSupport, RecoveryStats, StorageSupport};
use crate::server::messages::ServerMsg;
use crate::server::stage_profile::StageDurability;
#[cfg(feature = "stage-profile")]
use crate::server::stage_profile::StageStamps;
use crate::server::{Conf, ServerError};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, ShoalDatabase, TableNameSupport};
use crate::storage::{FullArchiveMap, LoaderMsg, Loaders};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};

/// The bookkeeping that stands in for an intent logs write and durable positions
///
/// Held apart from [`NoStorage`] because it is the only part of this engine with any behaviour,
/// and because it carries no database generic it can be tested without one.
#[derive(Debug, Default, PartialEq, Eq)]
struct Watermark {
    /// The position the last commit was handed
    ///
    /// There is no log for this to be an offset into. It exists because the table parks each
    /// pending response at the position its commit returned and releases it once the durable
    /// watermark passes that position, so the positions still have to be distinct and rising.
    pos: u64,
    /// Whether a commit has landed that the table has not yet been given the chance to release
    pending: bool,
}

impl Watermark {
    /// Take the next position, and note that something is now waiting on a sweep
    fn commit(&mut self) -> u64 {
        // hand out a position past every one we have handed out before
        self.pos += 1;
        // there is now a response parked that only a sweep can release, so ask for one
        self.pending = true;
        self.pos
    }

    /// Whether anything is parked that a sweep would release
    fn due(&self) -> bool {
        self.pending
    }

    /// Report the durable position and clear the request for a sweep
    ///
    /// Everything ever committed to this engine is as durable as it is ever going to be, so this
    /// is always the newest position handed out.
    fn release(&mut self) -> u64 {
        // the caller is about to release everything parked below our position, so the next
        // sweep has nothing to do until another commit arrives
        self.pending = false;
        self.pos
    }
}

/// A storage engine that keeps nothing, so the table above it keeps everything in memory
pub struct NoStorage<D: ShoalDatabase> {
    /// The positions this engine hands out in place of an intent logs offsets
    watermark: Watermark,
    /// The database this storage engine belongs to
    phantom: PhantomData<D>,
}

impl<D: ShoalDatabase> StorageSupport for NoStorage<D> {
    /// The settings for this storage engine
    ///
    /// There is nothing to configure about not storing anything.
    type Settings = ();

    /// The archive map this storage engine uses
    ///
    /// There are no archives, so there is no map of them.
    type ArchiveMap = ();

    /// The database type this storage engine is associated with
    type Database = D;

    /// Create a new instance of this storage engine
    ///
    /// Every argument is ignored. Notably the archive map is not registered in the shards full
    /// map, so a loader walking that map never sees this table at all.
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `table_name` - The name of this table
    /// * `shard_table_name` - The name of this table in the shards own naming
    /// * `shard_archive_map` - The shards shared map of archives
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    /// * `shard_local_tx` - The channel back to the shard that owns this table
    #[allow(async_fn_in_trait)]
    async fn new<
        P: IntentReadSupport<R> + 'static,
        R: PartitionKeySupport + 'static,
        N: TableNameSupport,
    >(
        _shard_name: &str,
        _table_name: N,
        _shard_table_name: D::TableNames,
        _shard_archive_map: &FullArchiveMap<N>,
        _conf: &Conf,
        _medium_priority: TaskQueueHandle,
        _shard_local_tx: &AsyncSender<ServerMsg<D>>,
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
        >,
    {
        // there is no directory to set up, no log to open and no compactor to spawn
        Ok(NoStorage {
            watermark: Watermark::default(),
            phantom: PhantomData,
        })
    }

    /// Get a tables config or use default settings
    ///
    /// # Arguments
    ///
    /// * `conf` - The shoal config to get settings from
    fn get_settings<R: PartitionKeySupport>(_conf: &Conf) -> Result<Self::Settings, ServerError> {
        // nothing about this engine is configurable
        Ok(())
    }

    /// Commit an operation to this storages intent log
    ///
    /// The data is dropped on the floor. The row itself is already being written into the
    /// partition by the caller; this is only the copy that would have survived a restart.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to commit
    #[allow(async_fn_in_trait)]
    async fn commit<I: RkyvSupport>(&mut self, _data: &I) -> Result<u64, ServerError> {
        // take the next position, which is also what asks for the sweep that releases it
        Ok(self.watermark.commit())
    }

    /// Fill in the durability stages for responses that have just been released
    ///
    /// # Arguments
    ///
    /// * `stamps` - The stamps to fill in, each already carrying its commit offset
    #[cfg(feature = "stage-profile")]
    fn fill_durability(&self, _stamps: &mut StageStamps) {
        // a query that never touched a log has no durability phases to report, and inventing
        // any here would put a stage in the report that never happened
    }

    /// Get how this storage engine makes a committed intent durable
    fn durability(&self) -> StageDurability {
        // it does not, and a report that showed an fdatasync for this table would be fiction
        StageDurability::None
    }

    /// Check if this intent log has grown past the size it rotates at
    ///
    /// **This is the only thing that ever releases an insert.** The shard sweeps its tables when
    /// `data_flushed || compaction_due()`, and `data_flushed` is set by a `DataFlushed` message
    /// that only the filesystem compactor sends. Without this returning true, a response parked
    /// by [`NoStorage::commit`] would sit in the tables pending queue forever and the query would
    /// never be answered.
    ///
    /// It tracks whether anything is actually parked rather than returning a constant `true`,
    /// because the shards rule is that it only sweeps when a sweep could do something. A database
    /// mixing ephemeral and persistent tables would otherwise walk every table on every message.
    fn compaction_due(&self) -> bool {
        self.watermark.due()
    }

    /// Set our intent log to be compact if its needed
    ///
    /// Returns how far this tables intent log has been made durable
    ///
    /// # Arguments
    ///
    /// * `force` - Whether to force a compaction of the intent logs
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<R: PartitionKeySupport>(
        &mut self,
        _force: bool,
    ) -> Result<FlushProgress, ServerError> {
        Ok(FlushProgress {
            // everything ever committed to this engine is as durable as it will ever be, which
            // is what releases every parked response on this sweep
            durable_pos: self.watermark.release(),
            // the generation never moves, because nothing is ever sealed into an archive. That
            // is also what keeps `MaybeLoaded::is_evictable` false for every partition we hold
            generation: 1,
            // a rotation restarts positions at zero, and ours only ever climb
            rotated: false,
        })
    }

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError> {
        // there is nothing in flight to wait on
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `conf` - A Shoal config
    /// * `generation` - The generation to replay these intents as
    /// * `partitions` - The map of partitions to load intents into
    /// * `memory_usage` - The memory usage for this shard
    #[allow(async_fn_in_trait)]
    async fn read_intents<P: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        &self,
        _conf: &Conf,
        _generation: u64,
        _partitions: &mut HashMap<u64, MaybeLoaded<P>>,
        _memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<RecoveryStats, ServerError>
    where
        for<'a> <P as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // a table that starts empty every time discarded nothing getting there
        Ok(RecoveryStats::default())
    }

    /// Get the type of loader this storage kind requires
    fn loader_kind() -> Loaders {
        // none, and saying so is what stops this table claiming the filesystem loaders slot in
        // the shards spawned set and starving a persistent table declared after it
        Loaders::None
    }

    /// Spawn a loader for this storage type if not yet spawned
    ///
    /// # Arguments
    ///
    /// * `table_map` - The shards shared map of archives
    /// * `loader_rx` - The channel loader requests arrive on
    /// * `shard_local_tx` - The channel back to the shard
    async fn spawn_loader(
        &self,
        _table_map: &FullArchiveMap<D::TableNames>,
        _loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        _shard_local_tx: &AsyncSender<ServerMsg<D>>,
    ) -> Result<(), ServerError> {
        // nothing is ever read back, so there is nothing to read it
        Ok(())
    }

    /// Load a partition from disk if it exists
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to load a partition for
    /// * `partition_id` - The partition to load
    /// * `loader_tx` - The channel to send load requests on
    async fn load_partition<N: TableNameSupport>(
        &self,
        _table_name: N,
        _partition_id: u64,
        _loader_tx: &AsyncSender<LoaderMsg<N>>,
    ) -> Result<bool, ServerError> {
        // no partition is ever on disk, so a get that misses in memory has genuinely missed
        Ok(false)
    }

    /// Load a partition from disk if it exists directly
    ///
    /// # Arguments
    ///
    /// * `partition_id` - The partition to load
    async fn load_partition_direct(
        &self,
        _partition_id: u64,
    ) -> Result<Option<ReadResult>, ServerError> {
        // there is nothing on disk to hand back
        Ok(None)
    }

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(self) -> Result<(), ServerError> {
        // no files to close, no tasks to join, and deliberately no final flush - the data this
        // table held is meant to be gone
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Watermark;
    use crate::storage::Loaders;

    /// A fresh engine has nothing parked, so it asks for no sweep
    ///
    /// If this were true from the start, every shard holding an ephemeral table would sweep all
    /// of its tables on every message it handled without ever releasing anything.
    #[test]
    fn nothing_is_due_until_something_is_committed() {
        let watermark = Watermark::default();
        assert!(!watermark.due());
    }

    /// Every commit is handed a position past every one before it
    ///
    /// The table parks each pending response under the position its commit returned, so two
    /// commits sharing one would have the second overwrite the first and lose a response.
    #[test]
    fn a_commit_hands_out_a_position_past_every_one_before_it() {
        let mut watermark = Watermark::default();
        let mut last = 0;
        for _ in 0..100 {
            let pos = watermark.commit();
            assert!(pos > last, "{pos} did not come after {last}");
            last = pos;
        }
    }

    /// A commit asks for the sweep that releases it
    #[test]
    fn a_commit_asks_for_a_sweep() {
        let mut watermark = Watermark::default();
        watermark.commit();
        assert!(watermark.due());
    }

    /// A release covers every position handed out, and stops asking for sweeps
    ///
    /// Covering every position is what releases a parked response on the very next sweep rather
    /// than one sweep later, and clearing the request is what keeps a quiet shard quiet.
    #[test]
    fn releasing_covers_every_position_and_clears_the_request() {
        let mut watermark = Watermark::default();
        let last = (0..10).map(|_| watermark.commit()).last().unwrap();
        assert_eq!(watermark.release(), last);
        assert!(!watermark.due());
        // and a release with nothing parked still reports the same position rather than moving
        assert_eq!(watermark.release(), last);
    }

    /// A commit after a release asks for another sweep
    #[test]
    fn a_commit_after_a_release_asks_again() {
        let mut watermark = Watermark::default();
        watermark.commit();
        watermark.release();
        watermark.commit();
        assert!(watermark.due());
    }

    /// The loader kinds are distinct, both as values and in a log line
    ///
    /// The shard keys the set of loaders it has already spawned on this type, so a kind meaning
    /// "no loader" that compared equal to a real one would mark that real one spawned.
    #[test]
    fn no_loader_is_its_own_kind() {
        assert_ne!(Loaders::None, Loaders::FileSystem);
        assert_eq!(Loaders::None.to_string(), "None");
        assert_eq!(Loaders::FileSystem.to_string(), "FileSystem");
    }
}
