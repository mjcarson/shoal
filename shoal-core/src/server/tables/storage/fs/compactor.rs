//! The file system compaction utilties for intent logs/archives

use futures::{select, AsyncWriteExt, FutureExt, StreamExt};
use glommio::io::{BufferedFile, DmaFile, DmaStreamWriter, OpenOptions};
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::conf::FileSystemTableConf;
use super::map::{
    write_record, ArchiveEntry, ArchiveFormat, ArchiveMap, MapIntent, MapIntentKinds,
    ARCHIVE_HEADER_LEN,
};
use super::IntentLogReader;
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::replication::snapshot::{
    self, SnapshotManifest, SnapshotProvenance, SnapshotReader, SnapshotWriter,
};
use crate::server::ring::Ring;
use crate::server::wal::WalLogId;
use crate::server::errors::ShoalError;
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, GroupId, NodeId};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, TableNameSupport as _};
use crate::storage::ArchiveFault;
use crate::storage::{CompactionJob, IntentReadSupport, RecoveryStats, ShouldPrune};

/// The minimum size an active archive must be in order to be considered for compaction
/// This is 100 Mebibytes
const MIN_ARCHIVE_COMPACTABLE: u64 = 10 << 20;

/// What reading an intent log for compaction cost us
///
/// A compaction deletes the log it just read on every path out, so whatever the reader
/// gave up on is gone the moment that delete lands. How much was lost is not the same
/// question as whether anything was: a log that yielded records before it stopped had a
/// tail dropped, and a log that yielded none was dropped whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TailLoss {
    /// This log was read to its end and nothing was discarded
    None,
    /// This log stopped on damage before a single record could be read from it
    Whole,
    /// This log gave us records and then stopped on damage, dropping the rest
    Tail,
}

/// Work out what a compaction is about to throw away with the log it read
///
/// This is split out from `compact_intent` because it is the whole of the decision and
/// the rest of that function cannot be reached without a live shard behind it. The
/// distinction it draws is the point: an empty log is the ordinary result of rotating a
/// table nobody wrote to, so `truncated` alone cannot say whether anything was lost.
///
/// # Arguments
///
/// * `truncated` - Whether the reader gave up on a damaged entry
/// * `read_any` - Whether the reader yielded any records before it stopped
pub(crate) fn classify_tail(truncated: bool, read_any: bool) -> TailLoss {
    // a reader that ran to the end of its log discarded nothing, however little it found
    if !truncated {
        return TailLoss::None;
    }
    // a log we read part of lost only what came after the damage
    if read_any {
        TailLoss::Tail
    } else {
        // and one we read nothing from was discarded in its entirety
        TailLoss::Whole
    }
}

/// Frame a map intent onto the end of a job's staged intents: its size, its checksum, its bytes
///
/// A job's intents are staged rather than written to the map's intent log as they are made,
/// because the log's writer flushes each buffer as it fills: an intent on disk ahead of the
/// record it names is an entry past the end of its archive after a crash
/// ([Resolved #159](../../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md)).
///
/// # Arguments
///
/// * `staged` - The job's staged intents
/// * `intent` - The intent to stage
fn stage_intent(staged: &mut Vec<u8>, intent: &MapIntent) -> Result<(), ServerError> {
    // archive this intent log entry
    let archived_intent = rkyv::to_bytes::<Error>(intent)?;
    // get the size of the data to write
    let size = archived_intent.len();
    // compute a checksum over our serialized data
    let mut hasher = GxHasher::default();
    hasher.write(archived_intent.as_slice());
    let checksum = hasher.finish();
    // the size, the checksum and the intent, the framing the intent log reader expects
    staged.extend_from_slice(&size.to_le_bytes());
    staged.extend_from_slice(&checksum.to_le_bytes());
    staged.extend_from_slice(archived_intent.as_slice());
    Ok(())
}

/// Stage a new intent for our maps intent log, and hand back the entry it carries
///
/// # Arguments
///
/// * `staged` - The job's staged intents
/// * `intent` - The entry intent to stage
macro_rules! stage_map_intent {
    ($staged:expr, $intent:expr, $variant:ident) => {{
        // frame this intent onto the job's staged intents
        stage_intent(&mut $staged, &$intent)?;
        // ensure that during testing/development we always have an entry intent
        debug_assert!($intent.is_kind(MapIntentKinds::$variant));
        // we should only have this variant for the intent since we just wrapped it
        match $intent {
            // save this entry to be added to our map after these writes sync
            MapIntent::$variant(entry) => entry,
            // we cannot have any other type here then an entry
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }};
}

/// How many bytes of staged map intents a job holds before it writes them early
///
/// About sixty thousand partitions' intents. A job that rewrites more than that syncs its
/// archive and writes what it staged, so its memory is bounded by this and not by the job.
const STAGED_MAP_LIMIT: usize = 4 << 20;

/// Write a job's staged map intents to the intent log, once the records they name are durable
///
/// The archive is synced first, so nothing in the intent log ever names a record the archive
/// could lose ([Resolved #159](../../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md)).
/// The intent log itself is not synced here; the job syncs it before it repoints the map.
///
/// # Arguments
///
/// * `writer` - The archive writer the staged intents' records went to, if one was opened
/// * `map_writer` - The map's intent log writer
/// * `staged` - The job's staged intents, emptied
async fn write_staged(
    writer: &mut Option<DmaStreamWriter>,
    map_writer: &mut DmaStreamWriter,
    staged: &mut Vec<u8>,
) -> Result<(), ServerError> {
    // nothing staged is nothing to order
    if staged.is_empty() {
        return Ok(());
    }
    // the records first, if any were written
    if let Some(writer) = writer.as_mut() {
        writer.sync().await?;
    }
    // then the intents that name them
    map_writer.write_all(staged).await?;
    staged.clear();
    Ok(())
}

/// Write a job's staged intents early if they have grown past their bound
///
/// # Arguments
///
/// * `writer` - The archive writer the staged intents' records went to, if one was opened
/// * `map_writer` - The map's intent log writer
/// * `staged` - The job's staged intents
async fn bound_staged(
    writer: &mut Option<DmaStreamWriter>,
    map_writer: &mut DmaStreamWriter,
    staged: &mut Vec<u8>,
) -> Result<(), ServerError> {
    // under the bound, the intents wait for the job's end
    if staged.len() < STAGED_MAP_LIMIT {
        return Ok(());
    }
    write_staged(writer, map_writer, staged).await
}

/// The active archive's writer, creating the archive if nothing has been written to it yet
///
/// # Arguments
///
/// * `writer` - The compactor's writer, if the active archive has one
/// * `map` - The archive map naming the active archive
async fn active_writer<'a>(
    writer: &'a mut Option<DmaStreamWriter>,
    map: &ArchiveMap,
) -> Result<&'a mut DmaStreamWriter, ServerError> {
    // the first record written to the active archive creates it
    if writer.is_none() {
        *writer = Some(map.get_active_writer().await?);
    }
    // the writer is always set by now
    writer
        .as_mut()
        .ok_or_else(|| ServerError::GlommioGeneric("the active archive has no writer".into()))
}

/// Where the active archive's writer stands, or zero before anything was written to it
///
/// # Arguments
///
/// * `writer` - The compactor's writer, if the active archive has one
fn written_to(writer: Option<&DmaStreamWriter>) -> u64 {
    // an archive not yet created has had nothing written to it
    writer.map_or(0, DmaStreamWriter::current_pos)
}

/// How many archived records a snapshot cut reads at once
///
/// Enough to keep a device queue busy under a node's own load; the records are still written
/// in key order ([O70](../../../../../../docs/src/appendix/optimizations.md#o70-a-snapshot-cut-reads-its-records-one-at-a-time)).
const CUT_READS_IN_FLIGHT: usize = 32;

/// The first wait before a compaction job that failed before writing is tried again
const COMPACTION_RETRY_MIN: Duration = Duration::from_millis(100);

/// The longest wait between two tries of the same job
const COMPACTION_RETRY_MAX: Duration = Duration::from_secs(5);

/// Why a compaction job did not finish, and whether trying it again could
///
/// A job that failed before it wrote anything - an archive it could not open, a log it could
/// not read - has changed nothing on disk or in the map, so it is tried again after a backoff
/// rather than ending the compactor with every job after it
/// ([Resolved #91](../../../../../../docs/src/appendix/resolved/compaction-retry.md)). A job
/// that failed after writing cannot be redone blind, and ends the compactor as it always did.
#[derive(Debug)]
enum JobFailure {
    /// Nothing was written; the job is tried again after a backoff
    Retry(ServerError),
    /// Something was written that cannot be redone; the compactor ends with the error
    Fatal(ServerError),
}

impl JobFailure {
    /// The error, whichever way it failed
    ///
    /// For the startup fold, which runs before any shard serves and has nothing to try
    /// again: a log it cannot read stops the start, as it always did.
    fn into_error(self) -> ServerError {
        match self {
            JobFailure::Retry(error) | JobFailure::Fatal(error) => error,
        }
    }
}

/// A job that failed before writing, waiting to be tried again
struct RetryJob {
    /// When to try it next
    at: Instant,
    /// The job
    job: CompactionJob,
    /// How many times it has failed
    attempts: u32,
}

/// The backoff before a job's next try
///
/// # Arguments
///
/// * `attempts` - How many times the job has failed
fn retry_backoff(attempts: u32) -> Duration {
    // double from the floor, capped at the ceiling
    COMPACTION_RETRY_MIN
        .saturating_mul(1u32 << attempts.min(16))
        .min(COMPACTION_RETRY_MAX)
}

/// What the loop does after a job
enum AfterJob {
    /// Take the next job
    Continue,
    /// The compactor was told to stop
    Stop,
}

pub struct FileSystemCompactor<T: IntentReadSupport<R>, R: PartitionKeySupport, S: ShoalDatabase> {
    /// The name of this table
    table_name: S::TableNames,
    /// The shard local shared map of archive/partition data
    map: Arc<ArchiveMap>,
    /// The file to write compacted partition data too, once a record has been written to it
    ///
    /// The active archive's file is created by the first record written to it, never when
    /// the compactor starts, so a start that ends without a shutdown leaves no empty archive
    /// the map never named ([Resolved #161](../../../../../../docs/src/appendix/resolved/failed-start-empty-archive.md)).
    writer: Option<DmaStreamWriter>,
    /// The writer for updates to our archive maps partition data
    map_writer: DmaStreamWriter,
    /// The partitions whose records failed their checksum at an archive compaction, reported
    reported_corrupt: HashSet<u64>,
    /// The changes to apply to the already compacted partitions on disk
    changes: HashMap<u64, Vec<T::Intent>>,
    /// The partitions in the intent log currently being compacted
    ///
    /// This is cleared once those partitions have been written, since anything left
    /// in it would be rewritten by every later job for no reason.
    loaded: HashMap<u64, T>,
    /// The entries to add to our archive map after syncing writes
    entries: Vec<(u64, ArchiveEntry)>,
    /// The partitions that were pruned and so must be dropped from our archive map
    removals: Vec<u64>,
    /// The map intents of the job in progress, framed and not yet written to the intent log
    ///
    /// Written only once the archive holding the records they name is synced
    /// ([Resolved #159](../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md)).
    staged: Vec<u8>,
    /// The channel to listen for paths to intent logs to compact
    jobs_rx: AsyncReceiver<CompactionJob>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ServerMsg<S>>,
    /// The path to this tables archive folder
    archive_path: PathBuf,
    /// The last entry of each tablet group merged into the archives since this compactor started
    ///
    /// What a snapshot cut between two jobs takes as its boundary
    /// ([F43](../../../../../docs/src/features/node-recovery.md)); empty after a restart, when
    /// the loop's checkpoint stands in for it.
    merged: HashMap<GroupId, WalLogId>,
    /// The row type this table contains
    row_kind: PhantomData<R>,
}

#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<T: IntentReadSupport<R>, R: PartitionKeySupport, S: ShoalDatabase>
    FileSystemCompactor<T, R, S>
{
    /// Create a new filesystem compactor
    #[instrument(name = "FileSystemCompactor::with_capacity", skip_all, err(Debug))]
    pub async fn with_capacity(
        table_name: S::TableNames,
        conf: &FileSystemTableConf,
        jobs_rx: AsyncReceiver<CompactionJob>,
        shard_local_tx: &AsyncSender<ServerMsg<S>>,
        map: &Arc<ArchiveMap>,
        capacity: usize,
    ) -> Result<Self, ServerError> {
        // compact any existing map data
        let map_writer = map.compact_map().await?;
        // the active archive is created by the first record written to it, not here
        let writer = None;
        // build a file system compactor
        let compactor = FileSystemCompactor {
            table_name,
            map: map.clone(),
            writer,
            map_writer,
            reported_corrupt: HashSet::new(),
            changes: HashMap::with_capacity(capacity),
            loaded: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            removals: Vec::with_capacity(capacity),
            staged: Vec::new(),
            jobs_rx,
            shard_local_tx: shard_local_tx.clone(),
            archive_path: conf.get_archive_path(R::name()),
            merged: HashMap::new(),
            row_kind: PhantomData,
        };
        Ok(compactor)
    }

    /// End a job's writes: its records durable, then the map intents naming them, then those
    ///
    /// Everything a job writes to the map's intent log goes through here, so an intent is never
    /// on disk before its record ([Resolved #159](../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md)).
    async fn sync_job(&mut self) -> Result<(), ServerError> {
        // the records, then the intents that name them
        if let Some(writer) = self.writer.as_mut() {
            writer.sync().await?;
        }
        write_staged(&mut self.writer, &mut self.map_writer, &mut self.staged).await?;
        // and the intents durable before the map is repointed
        self.map_writer.sync().await?;
        Ok(())
    }

    /// Read and sort this intent log by partition
    ///
    /// Returns whether this log ended on a damaged entry instead of a clean end of log,
    /// since a log we read nothing from is deleted either way and the two reasons it
    /// can be empty are worth telling apart.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read
    #[instrument(name = "FileSystemCompactor::sort_intent_log", skip_all, err(Debug))]
    async fn sort_intent_log(&mut self, path: &PathBuf) -> Result<bool, ServerError>
    where
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // create a reader for this intent log
        let mut reader = IntentLogReader::new(&path).await?;
        // read all of the intent from this intent log
        while let Some(read) = reader.next_buff().await? {
            // get the partition key for this intent
            let (partition_key, intent) = T::partition_key_and_intent(&read)?;
            // add this change to our changes vec
            let entry = self.changes.entry(partition_key).or_default();
            // add our change
            entry.push(intent);
        }
        // remember whether this reader stopped on damage before we drop it
        let truncated = reader.truncated;
        // close our reader
        reader.close().await?;
        Ok(truncated)
    }

    /// Load all of our partitions from disk
    #[instrument(
        name = "FileSystemCompactor::load_partitions_for_intents",
        skip_all,
        err(Debug)
    )]
    async fn load_partitions_for_intents(&mut self) -> Result<(), ServerError>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        // crawl over all partitions with intents
        for partition in self.changes.keys() {
            // get this partiitons current archive if it exists
            // the entry is copied out so no borrow of the map is held across the read
            let entry = self.map.to_archive.borrow().get(partition).copied();
            if let Some(entry) = entry {
                // read this partitions record, verified against its checksum
                let read = self.map.read_record(&entry).await?;
                // load this partitions data
                let archived = <T as RkyvSupport>::access(&read)?;
                // deserialize this partition
                let deserialized = <T as RkyvSupport>::deserialize(archived)?;
                // add this deserialized partition to our loaded partition map
                self.loaded.insert(*partition, deserialized);
            }
        }
        Ok(())
    }

    /// Apply our intents to our loaded partitions
    ///
    /// A compaction runs for the life of a shard rather than only at startup, so what
    /// it discards is reported here instead of in the summary a shard emits once it
    /// has finished starting.
    ///
    /// # Arguments
    ///
    /// * `loss` - What reading the log these intents came from already cost us
    #[instrument(name = "FileSystemCompactor::apply_intents", skip_all, err(Debug))]
    async fn apply_intents(&mut self, loss: TailLoss) -> Result<(), ServerError> {
        // start this job with whatever reading its log already discarded, so the
        // summary below counts the records we never saw alongside the ones we could
        // not apply rather than reporting only half of what went missing
        let mut stats = RecoveryStats {
            truncated_logs: u64::from(loss != TailLoss::None),
            ..RecoveryStats::default()
        };
        // replay all intents over our partitions
        for (partition, intents) in self.changes.drain() {
            // apply these intents to the correct partition
            if let ShouldPrune::Yes =
                T::apply_intents(&mut self.loaded, partition, intents, &mut stats)
            {
                // this partition should be pruned as it is empty
                self.loaded.remove(&partition);
                // this partition is not going to be rewritten, so its old archive entry
                // has to go too or the map keeps pointing at its pre-delete copy
                self.removals.push(partition);
            }
        }
        // say so loudly if this job could not apply everything it was given
        if !stats.is_clean() {
            event!(
                Level::WARN,
                msg = "Compaction discarded intents",
                orphaned_updates = stats.orphaned_updates,
                unreplayable_entries = stats.unreplayable_entries,
                truncated_logs = stats.truncated_logs,
                updates_after_delete = stats.updates_after_delete,
            );
        }
        Ok(())
    }

    /// Write parititons to disk
    #[instrument(name = "FileSystemCompactor::write_partition", skip_all, err(Debug))]
    async fn write_partition(&mut self) -> Result<Vec<u64>, ServerError> {
        // preallocate a vec to store the partition ids to mark as evictable
        let mut to_mark = Vec::with_capacity(1000);
        // get a copy of our active archive id
        let active_id = *self.map.active.borrow();
        // where the map stood when this pass began, for the fixture's crash point
        let (map_written_at_start, map_flushed_at_start) = (
            self.map_writer.current_pos(),
            self.map_writer.current_flushed_pos(),
        );
        // write all of our compacted partitions to disk
        for (key, partition) in &self.loaded {
            // serialize this partitions data
            let archived = rkyv::to_bytes::<_>(partition)?;
            // write this archived partition as a record: its size, its checksum, its bytes
            let offset = write_record(
                active_writer(&mut self.writer, &self.map).await?,
                archived.as_slice(),
            )
            .await?;
            // build the archive entry for this partitions data
            let intent = MapIntent::entry(*key, active_id, offset, archived.len());
            // stage this map intent, for the intent log once the record is durable
            let entry = stage_map_intent!(self.staged, intent, Entry);
            // a long job writes what it staged early, its records first, to bound its memory
            bound_staged(&mut self.writer, &mut self.map_writer, &mut self.staged).await?;
            // add this entry to our entries list
            self.entries.push((*key, entry));
            // add this partition to the list of partitions we want to mark
            // as potentially evictable
            to_mark.push(*key);
            // the fixture dies here once a whole buffer of this pass's map is on disk
            if crate::server::replication::install::crash_point::armed()
                == crate::server::replication::install::CrashPoint::MidCompaction
                && self.map_writer.current_pos() - map_written_at_start >= 128 << 10
            {
                // the buffer was handed to a write in the background; let it land first
                while self.map_writer.current_flushed_pos() <= map_flushed_at_start {
                    glommio::timer::sleep(Duration::from_millis(1)).await;
                }
                crate::server::replication::install::crash_point::hit(
                    crate::server::replication::install::CrashPoint::MidCompaction,
                );
            }
        }
        // or here, every record written and nothing synced, if it never did
        crate::server::replication::install::crash_point::hit(
            crate::server::replication::install::CrashPoint::MidCompaction,
        );
        // log the removal of every partition we pruned
        for key in &self.removals {
            // write this removal to our map intent log
            stage_map_intent!(self.staged, MapIntent::Remove(*key), Remove);
            // a pruned partitions tombstone can be evicted once this removal lands
            to_mark.push(*key);
        }
        // drop the partitions we just wrote so the next job starts from their archive
        // copies instead of rewriting every partition this compactor has ever loaded
        self.loaded.clear();
        // flush our current writers
        self.sync_job().await?;
        // add the archive entries for the data we just synced
        for (id, entry) in self.entries.drain(..) {
            // add this entry to our shared map
            self.map.set_partition(id, entry);
        }
        // drop the archive entries for the partitions we pruned
        for id in self.removals.drain(..) {
            // this partition no longer has data in any archive
            self.map.remove_partition(id);
        }
        // check how large our map intent log is and if needed compact it
        if self
            .map
            .compaction_due(self.map_writer.current_flushed_pos())
        {
            // close our current map writer
            self.map_writer.close().await?;
            // compact our map data and get a new intent writer
            self.map_writer = self.map.compact_map().await?;
        }
        Ok(to_mark)
    }

    /// Tell our shard to mark these partitions as evictable
    async fn send_mark_evictables(
        &mut self,
        generation: u64,
        partitions: Vec<u64>,
    ) -> Result<(), ServerError> {
        // build our message to mark these these nodes as evictable
        let msg = ServerMsg::MarkEvictable {
            generation,
            table: self.table_name,
            partitions,
        };
        // send our mark evictable shard message
        self.shard_local_tx.send(msg).await?;
        Ok(())
    }

    /// Fold intent logs into the archives and stop, for a rehome
    ///
    /// Each log is compacted exactly as a job would compact it - read, applied over the
    /// archived partitions, written as records, its map entries synced, the log deleted - and
    /// then the compactor shuts down, leaving the shard's data all archives and map. The
    /// evictable marks it sends go to a channel nothing reads, since no table is resident
    /// ([F47](../../../../../docs/src/features/local-rehome.md)). Returns how many partitions
    /// were written.
    ///
    /// # Arguments
    ///
    /// * `logs` - The intent logs, oldest generation first, the active log last
    #[instrument(name = "FileSystemCompactor::fold", skip_all, err(Debug))]
    pub(crate) async fn fold(mut self, logs: Vec<PathBuf>) -> Result<u64, ServerError>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // every log in the order it was written, each folded whole before the next
        let mut folded = 0u64;
        for (generation, path) in logs.into_iter().enumerate() {
            // the generation is only what the evictable mark carries, and nothing reads it here
            folded += self
                .compact_intent(path, generation as u64 + 1)
                .await
                .map_err(JobFailure::into_error)?;
        }
        // leave the map and the archives consistent on disk
        self.shutdown().await?;
        Ok(folded)
    }

    /// Compact an intent log down
    ///
    /// Returns how many partitions were written.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to compact
    #[instrument(name = "FileSystemCompactor::compact_intent", skip_all, err(Debug))]
    async fn compact_intent(&mut self, path: PathBuf, generation: u64) -> Result<u64, JobFailure>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // read and sort this intent log, which writes nothing and so can be tried again
        let truncated = self
            .sort_intent_log(&path)
            .await
            .map_err(JobFailure::Retry)?;
        // work out what deleting this log is about to cost us before we touch it
        let loss = classify_tail(truncated, !self.changes.is_empty());
        // check if we have any compacted partitions to write
        let partitions = if self.changes.is_empty() {
            // this log had nothing to compact so there is nothing to write
            Vec::default()
        } else {
            // load any existing partitions from disk, still writing nothing
            self.load_partitions_for_intents()
                .await
                .map_err(JobFailure::Retry)?;
            // the active archive is created here if nothing has been written to it yet, so an
            // archive that cannot be created fails the job before it wrote anything
            active_writer(&mut self.writer, &self.map)
                .await
                .map_err(JobFailure::Retry)?;
            // apply the new intents to our loaded partitions
            self.apply_intents(loss).await.map_err(JobFailure::Fatal)?;
            // write our compacted partitions to disk, past which nothing can be tried again
            self.write_partition().await.map_err(JobFailure::Fatal)?
        };
        // say what this log cost us, on both paths - an empty log is the ordinary
        // result of rotating a table nobody wrote to, and a damaged one is not
        match loss {
            // this log was read to its end, so there is nothing to report
            TailLoss::None => (),
            // we could not read a single record out of this log before deleting it
            TailLoss::Whole => event!(
                Level::WARN,
                msg = "Discarding an intent log we could read no entries from",
                path = path.to_str(),
            ),
            // we compacted what we could read and the rest goes with the file
            TailLoss::Tail => event!(
                Level::WARN,
                msg = "Discarding the unreadable tail of an intent log we compacted",
                path = path.to_str(),
            ),
        }
        // delete our no longer needed inactive intent log, which is safe for both arms
        // above: write_partition syncs everything it wrote before returning, and a log
        // we compacted nothing from has nothing left to make durable
        glommio::io::remove(path)
            .await
            .map_err(|error| JobFailure::Fatal(error.into()))?;
        // how many partitions this log wrote, for a fold's count
        let written = u64::try_from(partitions.len()).unwrap_or(u64::MAX);
        // tell our shard this generation is now durable even if it was empty, since
        // that is what tells our table how far its data has been compacted
        self.send_mark_evictables(generation, partitions)
            .await
            .map_err(JobFailure::Fatal)?;
        Ok(written)
    }

    /// Merge this table's frames of a sealed WAL segment into its archives
    ///
    /// The cluster node's compaction ([F40](../../../../../docs/src/features/replication.md)):
    /// the shard names exactly the frames that are this table's and still live, in log order per
    /// group, and they are read at those offsets and nowhere else. The merge is the intent log's
    /// unchanged. The file is left where it is - the shard deletes a segment once every group in
    /// it has purged past it - and the shard is told the segment is compacted for this table, so
    /// its groups' checkpoints can move.
    ///
    /// # Arguments
    ///
    /// * `path` - The segment
    /// * `generation` - Its generation
    /// * `frames` - This table's frames in it, as (offset, length)
    /// * `positions` - The last entry of each group with frames in the job
    #[instrument(name = "FileSystemCompactor::compact_segment", skip_all, err(Debug))]
    async fn compact_segment(
        &mut self,
        path: PathBuf,
        generation: u64,
        frames: Vec<(u64, u32)>,
        positions: Vec<(GroupId, WalLogId)>,
    ) -> Result<(), JobFailure>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // read every frame named, and sort its command's intent under its partition
        if !frames.is_empty() {
            // reading the segment writes nothing, so a failure here is tried again
            let file = BufferedFile::open(&path)
                .await
                .map_err(|error| JobFailure::Retry(error.into()))?;
            for (offset, len) in &frames {
                let read = file
                    .read_at(*offset, *len as usize)
                    .await
                    .map_err(|error| JobFailure::Retry(error.into()))?;
                // a frame that is not a whole command is a shard bug, not a torn log: the shard
                // named it from an index it built from whole frames
                let Some(command) = crate::server::wal::frame::command_of(&read) else {
                    return Err(JobFailure::Fatal(ServerError::GlommioGeneric(format!(
                        "the frame at {}:{offset} named for compaction is not a command",
                        path.display()
                    ))));
                };
                // a scrub entry carries no intent; the WAL's index never names one for
                // compaction, and one that reached here is skipped rather than decoded
                if command.scrub_op().is_some() {
                    continue;
                }
                let (partition_key, intent) = T::partition_key_and_intent_checked(&command.payload)
                    .map_err(JobFailure::Fatal)?;
                self.changes.entry(partition_key).or_default().push(intent);
            }
            file.close()
                .await
                .map_err(|error| JobFailure::Retry(error.into()))?;
        }
        // merge what was read the way an intent log is merged; a resolved segment is whole by
        // construction, so nothing was lost reading it
        let partitions = if self.changes.is_empty() {
            Vec::default()
        } else {
            // still writing nothing, so this too can be tried again
            self.load_partitions_for_intents()
                .await
                .map_err(JobFailure::Retry)?;
            // the active archive is created here if nothing has been written to it yet, so an
            // archive that cannot be created fails the job before it wrote anything
            active_writer(&mut self.writer, &self.map)
                .await
                .map_err(JobFailure::Retry)?;
            // past here something is written, and a failure ends the compactor
            self.apply_intents(TailLoss::None)
                .await
                .map_err(JobFailure::Fatal)?;
            self.write_partition().await.map_err(JobFailure::Fatal)?
        };
        // the archives now hold the effect of every frame through these positions
        for (group, last) in positions {
            let entry = self.merged.entry(group).or_insert_with(|| last.clone());
            if last.index >= entry.index {
                *entry = last;
            }
        }
        // the table hears the generation is compacted, then the shard hears this table is done
        self.send_mark_evictables(generation, partitions)
            .await
            .map_err(JobFailure::Fatal)?;
        self.shard_local_tx
            .send(ServerMsg::SegmentCompacted {
                table: self.table_name,
                generation,
            })
            .await
            .map_err(|error| JobFailure::Fatal(error.into()))?;
        Ok(())
    }

    /// Cut a snapshot of one group's tablets from the archives as they stand now
    ///
    /// Between two jobs the archive map is a consistent state: every frame through the merged
    /// positions and nothing after, since this task is the only writer of archives. The
    /// boundary is the highest position merged for the group, or the loop's checkpoint when
    /// that is higher, which is only the case for a compactor that merged nothing since the
    /// process started. Every partition of the group's tablets the map names is read at its
    /// archive offset and written as a record; the trailer is the group's remembered requests
    /// applied at or below the boundary ([F43](../../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `schema_id` - The schema's fingerprint, for the manifest
    /// * `tablets` - The tablets the group serves
    /// * `at_least` - The loop's checkpoint for the group
    /// * `memberships` - Every membership the cut could be as of, oldest first
    /// * `retries` - Every remembered request of the group
    /// * `expired_before` - The newest time-ordered identity the group has forgotten
    /// * `provenance` - Where the cut is made and which file format it is written in
    /// * `dir` - The directory the file goes in
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "FileSystemCompactor::cut_snapshot", skip_all, err(Debug))]
    async fn cut_snapshot(
        &mut self,
        group: GroupId,
        schema_id: u64,
        tablets: Vec<u16>,
        at_least: Option<WalLogId>,
        memberships: Vec<
            openraft::type_config::alias::StoredMembershipOf<
                crate::server::replication::DataConfig,
            >,
        >,
        retries: Vec<(
            crate::shared::protocol::peer::RequestId,
            crate::server::replication::Remembered,
        )>,
        expired_before: u64,
        provenance: SnapshotProvenance,
        dir: PathBuf,
    ) -> Result<(), ServerError> {
        let outcome = self
            .cut_snapshot_file(
                group,
                schema_id,
                tablets,
                at_least,
                memberships,
                retries,
                expired_before,
                provenance,
                dir,
            )
            .await
            .map_err(|error| format!("{error:?}"));
        // the shard hears what was built, or why nothing was
        self.shard_local_tx
            .send(ServerMsg::SnapshotBuilt { group, outcome })
            .await?;
        Ok(())
    }

    /// The cut itself, apart from telling the shard about it
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `schema_id` - The schema's fingerprint
    /// * `tablets` - The tablets the group serves
    /// * `at_least` - The loop's checkpoint for the group
    /// * `memberships` - Every membership the cut could be as of, oldest first
    /// * `retries` - Every remembered request of the group
    /// * `expired_before` - The newest time-ordered identity the group has forgotten
    /// * `provenance` - Where the cut is made and which file format it is written in
    /// * `dir` - The directory the file goes in
    #[allow(clippy::too_many_arguments)]
    async fn cut_snapshot_file(
        &mut self,
        group: GroupId,
        schema_id: u64,
        tablets: Vec<u16>,
        at_least: Option<WalLogId>,
        memberships: Vec<
            openraft::type_config::alias::StoredMembershipOf<
                crate::server::replication::DataConfig,
            >,
        >,
        retries: Vec<(
            crate::shared::protocol::peer::RequestId,
            crate::server::replication::Remembered,
        )>,
        expired_before: u64,
        provenance: SnapshotProvenance,
        dir: PathBuf,
    ) -> Result<(PathBuf, SnapshotManifest), ServerError> {
        // the boundary: what was merged here, or the loop's checkpoint if that is further
        let boundary = match (self.merged.get(&group).cloned(), at_least) {
            (Some(merged), Some(point)) if point.index > merged.index => point,
            (Some(merged), _) => merged,
            (None, Some(point)) => point,
            (None, None) => {
                return Err(ServerError::GlommioGeneric(format!(
                    "group {group} has no boundary to cut a snapshot at: nothing merged and no checkpoint"
                )));
            }
        };
        // every partition of the group's tablets the map names, in key order so two cuts of
        // one state are one file
        let mut entries: Vec<ArchiveEntry> = self
            .map
            .to_archive
            .borrow()
            .iter()
            .filter(|(key, _)| {
                // truncation cannot happen: a tablet id is twelve bits
                #[allow(clippy::cast_possible_truncation)]
                let tablet = Ring::tablet_of(**key) as u16;
                tablets.contains(&tablet)
            })
            .map(|(_, entry)| *entry)
            .collect();
        entries.sort_by_key(|entry| entry.key);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(snapshot::snapshot_name(group, boundary.index));
        let table = self.table_name.table_id();
        // the header at the file format the cluster has activated
        // ([F48](../../../../../docs/src/features/rolling-compatibility.md))
        let header = provenance.header(
            table,
            group,
            boundary.index,
            entries.len() as u64,
            schema_id,
        );
        let mut writer = SnapshotWriter::create(&path, header).await?;
        event!(Level::INFO, msg = "cutting a snapshot", group = %group, boundary = boundary.index, records = entries.len());
        // the records read a few at a time and written in key order: one read at a time, each a
        // direct read at a random offset, took minutes for a group of 146,000 partitions on a
        // loaded lab node, and a repair or a new replica waited on it
        // ([O70](../../../../../../docs/src/appendix/optimizations.md#o70-a-snapshot-cut-reads-its-records-one-at-a-time))
        let map = self.map.clone();
        let mut reads = futures::stream::iter(entries.iter().copied())
            .map(|entry| {
                let map = map.clone();
                async move { map.read_record(&entry).await.map(|read| (entry.key, read)) }
            })
            .buffered(CUT_READS_IN_FLIGHT);
        while let Some(read) = reads.next().await {
            // every record verified against its checksum as it is read, in the order asked
            let (key, read) = read?;
            writer.record(key, &read).await?;
        }
        // the trailer: what was remembered at or below the boundary, oldest first
        let remembered: Vec<_> = retries
            .into_iter()
            .filter(|(_, remembered)| remembered.applied <= boundary.index)
            .collect();
        let (total, checksum) = writer.finish(&remembered).await?;
        snapshot::sync_dir(&dir).await?;
        // the membership as of the boundary, never one applied since it
        let membership = crate::server::shard::membership_as_of(&memberships, boundary.index);
        let manifest = SnapshotManifest {
            group,
            table,
            schema_id,
            boundary,
            membership,
            tablets,
            records: entries.len() as u64,
            total,
            checksum,
            retries: u32::try_from(remembered.len()).unwrap_or(u32::MAX),
            expired_before,
            cluster: ClusterId::default(),
            origin: NodeId::default(),
            created_ms: 0,
        };
        // stamped with where and when it was cut
        let manifest = provenance.stamp(manifest, &header);
        event!(Level::INFO, msg = "cut a snapshot", group = %group, boundary = manifest.boundary.index, records = manifest.records, bytes = total);
        Ok((path, manifest))
    }

    /// Install a received snapshot's records into the archives, and remove what it does not name
    ///
    /// The records are the sender's archived partitions as they were, validated here as the
    /// row type this table holds - a foreign file is refused before a byte of it reaches an
    /// archive - and written into the active archive with the same size prefix and map intent
    /// a compaction writes. Every partition of the file's tablets the map names and the file
    /// does not is removed, since a snapshot's absence is total. The data and the map intent
    /// log are synced before the map is repointed, so a crash before the sync leaves the old
    /// generation whole and one after it leaves the new; the loop hears the trailer once the
    /// map is durable ([F43](../../../../../docs/src/features/node-recovery.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `tablets` - The tablets the file covers
    /// * `path` - The verified file
    #[instrument(name = "FileSystemCompactor::install_snapshot", skip_all, err(Debug))]
    async fn install_snapshot(
        &mut self,
        group: GroupId,
        tablets: Vec<u16>,
        path: PathBuf,
    ) -> Result<(), ServerError>
    where
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        let outcome = self
            .install_snapshot_records(&tablets, &path)
            .await
            .map_err(|error| format!("{error:?}"));
        // an install that failed part way leaves nothing for the next job to publish: its
        // entries, removals and staged intents name a file the loop is about to refuse
        if outcome.is_err() {
            self.reset_job();
        }
        // the loop hears the trailer, or why the archives were not touched
        self.shard_local_tx
            .send(ServerMsg::SnapshotInstalled {
                table: self.table_name,
                group,
                outcome,
            })
            .await?;
        Ok(())
    }

    /// The install itself, apart from telling the shard about it
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets the file covers
    /// * `path` - The verified file
    async fn install_snapshot_records(
        &mut self,
        tablets: &[u16],
        path: &std::path::Path,
    ) -> Result<
        Vec<(
            crate::shared::protocol::peer::RequestId,
            crate::server::replication::Remembered,
        )>,
        ServerError,
    >
    where
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        use crate::server::replication::install::{crash_point, CrashPoint};
        let mut reader = SnapshotReader::open(path).await?;
        // the file has to be this table's
        if reader.header().table != self.table_name.table_id() {
            return Err(ServerError::GlommioGeneric(format!(
                "the snapshot is of table {} and this compactor is {}'s",
                reader.header().table,
                self.table_name
            )));
        }
        let active_id = *self.map.active.borrow();
        let mut written: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut first = true;
        while let Some((key, bytes)) = reader.next_record().await? {
            // a record has to be a whole partition of this table's row type: validated as the
            // archive it will be read back as, at its alignment
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(&bytes);
            <T as RkyvSupport>::access(&aligned)?;
            // and its key has to be one of the tablets the file claims
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = Ring::tablet_of(key) as u16;
            if !tablets.contains(&tablet) {
                return Err(ServerError::GlommioGeneric(format!(
                    "the snapshot holds partition {key:016x} of tablet {tablet}, which it does not claim to cover"
                )));
            }
            // written as a compaction writes a partition: the record, then the map intent staged
            // behind it, so an installed record carries a checksum whatever the sender's archive
            // did and its intent never reaches the log before it
            let offset =
                write_record(active_writer(&mut self.writer, &self.map).await?, &bytes).await?;
            let intent = MapIntent::entry(key, active_id, offset, bytes.len());
            let entry = stage_map_intent!(self.staged, intent, Entry);
            bound_staged(&mut self.writer, &mut self.map_writer, &mut self.staged).await?;
            self.entries.push((key, entry));
            written.insert(key);
            if first {
                first = false;
                crash_point::hit(CrashPoint::MidInstall);
                crash_point::held().await;
            }
        }
        let trailer = reader.trailer().await?;
        reader.close().await?;
        // every partition of the covered tablets the map names and the file does not is gone
        let absent: Vec<u64> = self
            .map
            .to_archive
            .borrow()
            .keys()
            .filter(|key| {
                #[allow(clippy::cast_possible_truncation)]
                let tablet = Ring::tablet_of(**key) as u16;
                tablets.contains(&tablet) && !written.contains(key)
            })
            .copied()
            .collect();
        for key in &absent {
            stage_map_intent!(self.staged, MapIntent::Remove(*key), Remove);
            self.removals.push(*key);
        }
        // the data, then the map intent log, durable before the map is repointed
        self.sync_job().await?;
        for (id, entry) in self.entries.drain(..) {
            self.map.set_partition(id, entry);
        }
        for id in self.removals.drain(..) {
            self.map.remove_partition(id);
        }
        crash_point::hit(CrashPoint::MapSaved);
        event!(
            Level::INFO,
            msg = "installed a snapshot into the archives",
            records = written.len(),
            removed = absent.len(),
        );
        // a map intent log that grew past its bound is compacted, as after any job
        if self
            .map
            .compaction_due(self.map_writer.current_flushed_pos())
        {
            self.map_writer.close().await?;
            self.map_writer = self.map.compact_map().await?;
        }
        Ok(trailer)
    }

    /// Remove every archived partition of some tablets, and tell the shard
    ///
    /// # Arguments
    ///
    /// * `group` - The group whose copy retired
    /// * `tablets` - The tablets
    async fn drop_tablets(&mut self, group: GroupId, tablets: Vec<u16>) -> Result<(), ServerError> {
        let outcome = self
            .drop_tablet_records(&tablets)
            .await
            .map_err(|error| format!("{error:?}"));
        self.shard_local_tx
            .send(ServerMsg::TabletsDropped {
                table: self.table_name,
                group,
                outcome,
            })
            .await?;
        Ok(())
    }

    /// The removal itself: every partition of the tablets the map names, through the intent log
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    async fn drop_tablet_records(&mut self, tablets: &[u16]) -> Result<u64, ServerError> {
        // every partition of the tablets the map names is gone
        let absent: Vec<u64> = self
            .map
            .to_archive
            .borrow()
            .keys()
            .filter(|key| {
                #[allow(clippy::cast_possible_truncation)]
                let tablet = Ring::tablet_of(**key) as u16;
                tablets.contains(&tablet)
            })
            .copied()
            .collect();
        for key in &absent {
            stage_map_intent!(self.staged, MapIntent::Remove(*key), Remove);
            self.removals.push(*key);
        }
        // the map intent log durable before the map is repointed
        self.sync_job().await?;
        for id in self.removals.drain(..) {
            self.map.remove_partition(id);
        }
        event!(
            Level::INFO,
            msg = "dropped a retired copy's partitions from the archives",
            removed = absent.len()
        );
        // a map intent log that grew past its bound is compacted, as after any job
        if self
            .map
            .compaction_due(self.map_writer.current_flushed_pos())
        {
            self.map_writer.close().await?;
            self.map_writer = self.map.compact_map().await?;
        }
        Ok(absent.len() as u64)
    }

    /// Inject a fault into one partition's archived copy, for the fixture
    ///
    /// The compactor owns the archives, so the fault is done here, between two jobs, where
    /// the map is consistent ([F44](../../../../../docs/src/features/repair.md)). A corruption
    /// is written through a buffered handle and synced; Linux flushes the range before the next
    /// direct read, so the loader meets the flipped byte. A forget logs the removal the way a
    /// prune would; an erase writes a record with no live row under a valid checksum.
    ///
    /// # Arguments
    ///
    /// * `fault` - The fault
    /// * `key` - The partition
    async fn inject_fault(
        &mut self,
        fault: ArchiveFault,
        key: u64,
    ) -> Result<serde_json::Value, String> {
        let outcome = self
            .inject_fault_inner(fault, key)
            .await
            .map_err(|error| format!("{error:?}"));
        // a fault that failed part way leaves nothing staged for the next job to write
        if outcome.is_err() {
            self.reset_job();
        }
        outcome
    }

    /// The fault itself, with the engine's own errors
    ///
    /// # Arguments
    ///
    /// * `fault` - The fault
    /// * `key` - The partition
    async fn inject_fault_inner(
        &mut self,
        fault: ArchiveFault,
        key: u64,
    ) -> Result<serde_json::Value, ServerError> {
        // only a compacted partition has an archived copy to fault
        let Some(entry) = self.map.find_partition(key) else {
            return Err(ServerError::GlommioGeneric(format!(
                "no archive of {} holds partition {key:016x}",
                self.table_name
            )));
        };
        match fault {
            ArchiveFault::Corrupt => {
                // flip one byte in the middle of the record's payload, in place
                let path = self.archive_path.join(entry.archive.to_string());
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .buffered_open(&path)
                    .await?;
                let at = entry.offset + (entry.size as u64) / 2;
                let read = file.read_at(at, 1).await?;
                let Some(byte) = read.first().copied() else {
                    return Err(ServerError::GlommioGeneric(format!(
                        "archive {} is shorter than the record it should hold",
                        entry.archive
                    )));
                };
                file.write_at(vec![byte ^ 0x40], at).await?;
                file.fdatasync().await?;
                file.close().await?;
                event!(Level::WARN, msg = "corrupted a record, as the fixture asked", table = %self.table_name, partition = format!("{key:016x}"), archive = %entry.archive, at);
                Ok(
                    serde_json::json!({ "fault": "corrupt", "archive": entry.archive.to_string(), "offset": at, "was": byte }),
                )
            }
            ArchiveFault::Forget => {
                // log the removal the way a prune does, then drop the entry
                stage_map_intent!(self.staged, MapIntent::Remove(key), Remove);
                self.sync_job().await?;
                self.map.remove_partition(key);
                event!(Level::WARN, msg = "forgot a partition, as the fixture asked", table = %self.table_name, partition = format!("{key:016x}"));
                Ok(serde_json::json!({ "fault": "forget", "archive": entry.archive.to_string() }))
            }
            ArchiveFault::Erase => {
                // a record with no live row, written and pointed at like any compaction's
                let erased = T::erased(key);
                let archived = rkyv::to_bytes::<Error>(&erased)?;
                let active_id = *self.map.active.borrow();
                let offset = write_record(
                    active_writer(&mut self.writer, &self.map).await?,
                    archived.as_slice(),
                )
                .await?;
                let intent = MapIntent::entry(key, active_id, offset, archived.len());
                let entry = stage_map_intent!(self.staged, intent, Entry);
                self.sync_job().await?;
                self.map.set_partition(key, entry);
                event!(Level::WARN, msg = "erased a partition, as the fixture asked", table = %self.table_name, partition = format!("{key:016x}"));
                Ok(
                    serde_json::json!({ "fault": "erase", "archive": active_id.to_string(), "offset": offset }),
                )
            }
        }
    }

    /// Compact archives with the least amount of active data
    #[instrument(name = "FileSystemCompactor::compact_archives", skip_all, err(Debug))]
    async fn compact_archives(&mut self) -> Result<(), ServerError> {
        // find the archives with the least amount of active data
        let sorted = self.map.sort_by_load();
        // keep a list of old archive paths to delete
        let mut old_paths = Vec::with_capacity(self.map.all_archives.borrow().len());
        // track the stats for this compaction attempt
        let start_pos = written_to(self.writer.as_ref());
        let mut precompaction = 0;
        // start compacting from the lowest utilization to the highest
        for (used, archive_ids) in &sorted.sorted {
            // compact this group of archives
            for old_id in archive_ids {
                // get this archives valid data entries, gathered now rather than for every
                // archive up front: the index is not changed until this pass ends
                // ([O68](../../../../../../docs/src/appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index))
                let entries = self.map.entries_of(old_id);
                if !entries.is_empty() {
                    // build the path to this archive file
                    let path = self.archive_path.join(old_id.to_string());
                    // get a handle to this archive
                    let archive = DmaFile::open(&path).await?;
                    // get the size of this file
                    let size = archive.file_size().await?;
                    // an archive from before checksums is rewritten whatever its utilization,
                    // since that is how its records come to carry one
                    // (F44); the active archive is this build's and never needs it
                    let head = archive.read_at(0, ARCHIVE_HEADER_LEN).await?;
                    let unverified = ArchiveFormat::detect(&head) == ArchiveFormat::Unverified
                        && *old_id != *self.map.active.borrow();
                    // skip this file if its more then 50% utilized
                    if !unverified && *used as f64 > size as f64 * 0.50 {
                        // this file is largely valid so don't compact it
                        event!(Level::DEBUG, archive = old_id.to_string(), skip = true);
                        // close this archive
                        archive.close().await?;
                        continue;
                    }
                    // TODO make size configurable
                    // if this is our active file and its under 100MiB then skip it
                    if *old_id == *self.map.active.borrow() {
                        // if this file is under 100 MiB then skip it
                        if size < MIN_ARCHIVE_COMPACTABLE {
                            // close this archive since its under our minumum active archive
                            // compaction size
                            archive.close().await?;
                            continue;
                        }
                        // set a new active archive id
                        *self.map.active.borrow_mut() = Uuid::new_v4();
                        // the new active archive is created by the first record written to it,
                        // and the old one's writer is closed
                        if let Some(mut old_writer) = self.writer.take() {
                            old_writer.close().await?;
                        }
                        // don't compact/delete our old active archive this loop as that can lead to
                        // dangling partitions if we have already compacted data to
                        // the prior active archive in this compaction
                        continue;
                    }
                    // get a copy of our active archive id
                    let active_id = *self.map.active.borrow();
                    // whether a record here failed its checksum, which keeps the archive
                    let mut kept_corrupt = false;
                    // read all of the still valid data from this archive
                    for mut entry in entries {
                        // read this entry from our archive file, verified against its
                        // checksum: a corrupt record is never rewritten under a fresh one. It
                        // is left where it is, with its archive, and the copy holding it is
                        // quarantined; failing the pass instead retried it every five seconds,
                        // each try rewriting the records before it into the active archive
                        // for nothing, 38 GB on the lab ([Resolved #165](../../../../../../docs/src/appendix/resolved/corrupt-record-compaction-loop.md))
                        let read = match self.map.read_record(&entry).await {
                            Ok(read) => read,
                            Err(ServerError::Shoal(ShoalError::CorruptArchive { .. })) => {
                                kept_corrupt = true;
                                self.report_corrupt(entry.key).await;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        // write this entry to our new archive as a checksummed record
                        let start = write_record(
                            active_writer(&mut self.writer, &self.map).await?,
                            &read[..],
                        )
                        .await?;
                        // update our entries info
                        entry.archive = active_id;
                        entry.offset = start;
                        // wrap our entry in a map intent
                        let intent = MapIntent::Entry(entry);
                        // stage this map intent, for the intent log once the record is durable
                        let entry = stage_map_intent!(self.staged, intent, Entry);
                        // a long rewrite writes what it staged early, its records first
                        bound_staged(&mut self.writer, &mut self.map_writer, &mut self.staged)
                            .await?;
                        // add this entry to our entries list
                        self.entries.push((entry.key, entry))
                    }
                    // close our archive
                    archive.close().await?;
                    // an archive still holding a corrupt record stays, since the map names it
                    if kept_corrupt {
                        continue;
                    }
                    // stage an intent that we are deleting this archive, written after the
                    // records copied out of it are durable
                    stage_intent(&mut self.staged, &MapIntent::DeleteArchive(*old_id))?;
                    // save this archive path for deletion
                    old_paths.push((old_id, path));
                    // add this to our total compacted size
                    precompaction += used;
                } else {
                    // skip this unused/empty archive if its our active archive
                    if *old_id == *self.map.active.borrow() {
                        // we shouldn't delete our active archive if its empty
                        // since we just might not have written to it yet
                        continue;
                    }
                    // stage an intent that we are deleting this archive, written after the
                    // records copied out of it are durable
                    stage_intent(&mut self.staged, &MapIntent::DeleteArchive(*old_id))?;
                    // build the path to this now unused archive file
                    let path = self.archive_path.join(old_id.to_string());
                    // add this unused archive to the list of archives to remove
                    old_paths.push((old_id, path));
                }
            }
        }
        // remove any archives that are no longer in used from our map
        // short circut and stop compacting early if we didn't compact any archives
        if self.entries.is_empty() && old_paths.is_empty() {
            return Ok(());
        }
        // flush our current writers
        self.sync_job().await?;
        // calculate the stats for this round of compaction
        let post_compaction = written_to(self.writer.as_ref()).saturating_sub(start_pos);
        // log out total amount of compacted data
        event!(Level::INFO, post_compaction, precompaction);
        // add the archive entries for the data we just synced
        for (id, entry) in self.entries.drain(..) {
            // add this entry to our shared map
            self.map.set_partition(id, entry);
        }
        // remove our old archive files from our shared map
        for (old_id, _) in &old_paths {
            // remove this archive from our map
            self.map.remove_archive(old_id).await?;
        }
        // check how large our map intent log is and if needed compact it
        if self
            .map
            .compaction_due(self.map_writer.current_flushed_pos())
        {
            // close our current map writer
            self.map_writer.close().await?;
            // compact our map data and get a new intent writer
            self.map_writer = self.map.compact_map().await?;
        }
        // delete our old archive files
        for (_, old_archive) in old_paths {
            // delete this old archive
            glommio::io::remove(&old_archive).await?;
            // log that we are removing an old archive file
            event!(
                Level::INFO,
                msg = "Removing old archive file",
                path = old_archive.to_str()
            );
        }
        Ok(())
    }

    /// Shutdown this compactor
    async fn shutdown(&mut self) -> Result<(), ServerError> {
        // an active archive nothing was written to was never created, so there is nothing to
        // close or remove, only the map's intents to make durable
        let Some(mut writer) = self.writer.take() else {
            self.map_writer.write_all(&self.staged).await?;
            self.staged.clear();
            self.map_writer.sync().await?;
            self.map_writer.close().await?;
            return Ok(());
        };
        // flush and close our writer
        writer.sync().await?;
        writer.close().await?;
        // get our currently active id
        let active_id = *self.map.active.borrow();
        // build the path to our active archive
        let active_path = self.archive_path.join(active_id.to_string());
        // open the archive file and get its size
        // we do this rather then checking the writer position to ensure we
        // don't delete any archives with data
        let file = OpenOptions::new().read(true).dma_open(&active_path).await?;
        // check if our archive file is empty
        if file.file_size().await? == 0 {
            // stage an intent that we are deleting this archive
            stage_intent(&mut self.staged, &MapIntent::DeleteArchive(active_id))?;
            // remove this empty active archive from our map
            self.map
                .all_archives
                .borrow_mut()
                .remove(&self.map.active.borrow());
            // delete this empty archive file
            glommio::io::remove(active_path).await?;
        }
        // close our file handle
        file.close().await?;
        // the archive was synced before it closed, so nothing staged names a record it could lose
        self.map_writer.write_all(&self.staged).await?;
        self.staged.clear();
        // flush and close our map writer
        self.map_writer.sync().await?;
        self.map_writer.close().await?;
        Ok(())
    }

    /// Forget what a job that failed before writing had read, so its next try starts clean
    ///
    /// The sort and the load fill `changes` and `loaded` as they go; a try that stopped
    /// part way leaves them half full, and a second sort on top would count every intent
    /// twice.
    fn reset_job(&mut self) {
        // everything a job accumulates before it writes
        self.changes.clear();
        self.loaded.clear();
        self.entries.clear();
        self.removals.clear();
        // and what it staged for the map, which names records the job never finished
        self.staged.clear();
    }

    /// Tell the shard a record failed its checksum, once for each partition
    ///
    /// The shard quarantines the copy holding it, as a read that met it would. An archive
    /// holding one stays a candidate for every later pass, so a partition already reported is
    /// not reported again.
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition whose record failed
    async fn report_corrupt(&mut self, partition: u64) {
        // once a partition
        if !self.reported_corrupt.insert(partition) {
            return;
        }
        event!(Level::ERROR, msg = "an archive compaction left a record that failed its checksum where it was", table = R::name(), partition = format!("{partition:016x}"));
        let _ = self
            .shard_local_tx
            .send(ServerMsg::CorruptRecord {
                table: self.table_name,
                partition,
            })
            .await;
    }

    /// Run one compaction job
    ///
    /// # Arguments
    ///
    /// * `job` - The job
    ///
    /// # Errors
    ///
    /// Says whether the job can be tried again - it wrote nothing - or ended the compactor.
    async fn run_job(&mut self, job: CompactionJob) -> Result<AfterJob, JobFailure>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // handle this job
        match job {
            CompactionJob::IntentLog { path, generation } => {
                let _ = self.compact_intent(path, generation).await?;
            }
            CompactionJob::Segment {
                path,
                generation,
                frames,
                positions,
            } => {
                self.compact_segment(path, generation, frames, positions)
                    .await?;
            }
            // a cut and an install answer the shard with their own outcome, so only the
            // channel to it can fail here
            CompactionJob::Snapshot {
                group,
                schema_id,
                tablets,
                at_least,
                memberships,
                retries,
                expired_before,
                provenance,
                dir,
            } => {
                self.cut_snapshot(
                    group,
                    schema_id,
                    tablets,
                    at_least,
                    memberships,
                    retries,
                    expired_before,
                    provenance,
                    dir,
                )
                .await
                .map_err(JobFailure::Fatal)?;
            }
            CompactionJob::Install {
                group,
                tablets,
                path,
            } => self
                .install_snapshot(group, tablets, path)
                .await
                .map_err(JobFailure::Fatal)?,
            CompactionJob::Drop { group, tablets } => self
                .drop_tablets(group, tablets)
                .await
                .map_err(JobFailure::Fatal)?,
            CompactionJob::Fault { fault, key, reply } => {
                // a fault the fixture asked for, answered with what was done
                let outcome = self.inject_fault(fault, key).await;
                let _ = reply.send(outcome);
            }
            // rewriting the archives copies live records forward and repoints the map after
            // each archive, so a failure part way leaves dead bytes and nothing lost: tried again
            CompactionJob::Archives => self.compact_archives().await.map_err(JobFailure::Retry)?,
            CompactionJob::Shutdown => {
                // shutdown this compactor
                self.shutdown().await.map_err(JobFailure::Fatal)?;
                // stop handling compactor jobs
                return Ok(AfterJob::Stop);
            }
        }
        Ok(AfterJob::Continue)
    }

    /// Handle compaction jobs until told to stop
    ///
    /// A job that fails before it wrote anything is tried again after a backoff that doubles
    /// from a tenth of a second to five, for as long as it keeps failing, with a warning each
    /// time; new jobs are taken meanwhile, so one unreadable archive holds up its own log
    /// and nothing else. A job that fails after writing ends the compactor with its error,
    /// which the shard's exit reports
    /// ([Resolved #91](../../../../../../docs/src/appendix/resolved/compaction-retry.md)).
    ///
    /// # Errors
    ///
    /// The channel closing, or a job that failed past the point it could be tried again.
    pub async fn start(mut self) -> Result<(), ServerError>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // the jobs that failed before writing, waiting for their next try
        let mut retries: Vec<RetryJob> = Vec::new();
        loop {
            // a retry that is due comes before the channel
            let due = retries
                .iter()
                .position(|retry| retry.at <= Instant::now())
                .map(|index| retries.remove(index));
            let (job, attempts) = match due {
                Some(retry) => (retry.job, retry.attempts),
                None => {
                    // wait for a job, but no longer than the earliest retry
                    let earliest = retries
                        .iter()
                        .map(|retry| retry.at)
                        .min()
                        .map(|at| at.saturating_duration_since(Instant::now()));
                    match earliest {
                        Some(wait) => {
                            let mut recv = Box::pin(self.jobs_rx.recv()).fuse();
                            let mut timer = Box::pin(glommio::timer::sleep(wait)).fuse();
                            select! {
                                job = recv => (job?, 0),
                                () = timer => continue,
                            }
                        }
                        None => (self.jobs_rx.recv().await?, 0),
                    }
                }
            };
            // run it, keeping a copy in case it has to be tried again
            match self.run_job(job.clone()).await {
                Ok(AfterJob::Continue) => (),
                Ok(AfterJob::Stop) => break,
                Err(JobFailure::Retry(error)) => {
                    // say so, and try it again after the backoff
                    let attempts = attempts + 1;
                    let delay = retry_backoff(attempts);
                    event!(
                        Level::WARN,
                        msg = "A compaction failed before writing and will be tried again",
                        table = R::name(),
                        attempts,
                        retry_in_ms = delay.as_millis() as u64,
                        error = ?error,
                    );
                    self.reset_job();
                    retries.push(RetryJob {
                        at: Instant::now() + delay,
                        job,
                        attempts,
                    });
                }
                Err(JobFailure::Fatal(error)) => return Err(error),
            }
        }
        Ok(())
    }
}

