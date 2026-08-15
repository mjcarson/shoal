//! The file system compaction utilties for intent logs/archives

use byte_unit::Byte;
use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, OpenOptions};
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use std::hash::Hasher;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::conf::FileSystemTableConf;
use super::map::{ArchiveEntry, ArchiveMap, MapIntent, MapIntentKinds};
use super::IntentLogReader;
use crate::server::messages::ServerMsg;
use crate::server::ServerError;
use crate::server::database::ShoalDatabase;
use crate::shared::traits::{PartitionKeySupport, RkyvSupport};
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

/// Write a new intent to our maps intent log
///
/// # Arguments
///
/// * `map_writer` - The writer for our maps intent log
/// * `intent` - The entry intent to write
macro_rules! write_map_intent {
    ($map_writer:expr, $intent:expr, $variant:ident) => {{
        // archive this intent log entry
        let archived_intent = rkyv::to_bytes::<Error>(&$intent)?;
        // get the size of the data to write
        let size = archived_intent.len();
        // compute a checksum over our serialized data
        let mut hasher = GxHasher::default();
        hasher.write(archived_intent.as_slice());
        let checksum = hasher.finish();
        // write the size of our archived entry data
        $map_writer.write_all(&size.to_le_bytes()).await?;
        // write our checksum
        $map_writer.write_all(&checksum.to_le_bytes()).await?;
        // write our archived partition map data
        $map_writer.write_all(archived_intent.as_slice()).await?;
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

pub struct FileSystemCompactor<T: IntentReadSupport<R>, R: PartitionKeySupport, S: ShoalDatabase> {
    /// The name of this table
    table_name: S::TableNames,
    /// The shard local shared map of archive/partition data
    map: Arc<ArchiveMap>,
    /// The file to write compacted partition data too
    writer: DmaStreamWriter,
    /// The writer for updates to our archive maps partition data
    map_writer: DmaStreamWriter,
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
    /// The channel to listen for paths to intent logs to compact
    jobs_rx: AsyncReceiver<CompactionJob>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ServerMsg<S>>,
    /// The path to this tables archive folder
    archive_path: PathBuf,
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
        // get our currently active archive writer
        let writer = map.get_active_writer().await?;
        // build a file system compactor
        let compactor = FileSystemCompactor {
            table_name,
            map: map.clone(),
            writer,
            map_writer,
            changes: HashMap::with_capacity(capacity),
            loaded: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            removals: Vec::with_capacity(capacity),
            jobs_rx,
            shard_local_tx: shard_local_tx.clone(),
            archive_path: conf.get_archive_path(R::name()),
            row_kind: PhantomData,
        };
        Ok(compactor)
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
            if let Some(entry) = self.map.to_archive.borrow().get(partition) {
                // get this archive or insert it into our map
                let handle = self.map.get_archive(&entry.archive).await?;
                // set options for reading from this file
                let read = handle.read_at(entry.offset, entry.size).await?;
                // load this partitions data
                let archived = <T as RkyvSupport>::access(&read)?;
                // close this handle now that we are done reading
                handle.close().await?;
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
        // write all of our compacted partitions to disk
        for (key, partition) in &self.loaded {
            // serialize this partitions data
            let archived = rkyv::to_bytes::<_>(partition)?;
            // get the size of the data to write
            // this size is only used in recovery operations of archive files
            let size = archived.len();
            // write our size
            self.writer.write_all(&size.to_le_bytes()).await?;
            // get the current positions of the writer
            let offset = self.writer.current_pos();
            // write this archived partition
            self.writer.write_all(archived.as_slice()).await?;
            // build the archive entry for this partitions data
            let intent = MapIntent::entry(*key, active_id, offset, size);
            // write this map intent to our map intent log
            let entry = write_map_intent!(self.map_writer, intent, Entry);
            // add this entry to our entries list
            self.entries.push((*key, entry));
            // add this partition to the list of partitions we want to mark
            // as potentially evictable
            to_mark.push(*key);
        }
        // log the removal of every partition we pruned
        for key in &self.removals {
            // write this removal to our map intent log
            write_map_intent!(self.map_writer, MapIntent::Remove(*key), Remove);
            // a pruned partitions tombstone can be evicted once this removal lands
            to_mark.push(*key);
        }
        // drop the partitions we just wrote so the next job starts from their archive
        // copies instead of rewriting every partition this compactor has ever loaded
        self.loaded.clear();
        // flush our current writers
        self.writer.sync().await?;
        self.map_writer.sync().await?;
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
        if self.map_writer.current_flushed_pos() > Byte::MEBIBYTE {
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

    /// Compact an intent log down
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to compact
    #[instrument(name = "FileSystemCompactor::compact_intent", skip_all, err(Debug))]
    async fn compact_intent(&mut self, path: PathBuf, generation: u64) -> Result<(), ServerError>
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
        // read and sort this intent log
        let truncated = self.sort_intent_log(&path).await?;
        // work out what deleting this log is about to cost us before we touch it
        let loss = classify_tail(truncated, !self.changes.is_empty());
        // check if we have any compacted partitions to write
        let partitions = if self.changes.is_empty() {
            // this log had nothing to compact so there is nothing to write
            Vec::default()
        } else {
            // load any existing partitions from disk
            self.load_partitions_for_intents().await?;
            // apply the new intents to our loaded partitions
            self.apply_intents(loss).await?;
            // write our compacted partitions to disk
            self.write_partition().await?
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
        glommio::io::remove(path).await?;
        // tell our shard this generation is now durable even if it was empty, since
        // that is what tells our table how far its data has been compacted
        self.send_mark_evictables(generation, partitions).await?;
        Ok(())
    }

    /// Compact archives with the least amount of active data
    #[instrument(name = "FileSystemCompactor::compact_archives", skip_all, err(Debug))]
    async fn compact_archives(&mut self) -> Result<(), ServerError> {
        // find the archives with the least amount of active data
        let mut sorted = self.map.sort_by_load();
        // keep a list of old archive paths to delete
        let mut old_paths = Vec::with_capacity(sorted.entries.len());
        // track the stats for this compaction attempt
        let start_pos = self.writer.current_pos();
        let mut precompaction = 0;
        // start compacting from the lowest utilization to the highest
        for (used, archive_ids) in &sorted.sorted {
            // compact this group of archives
            for old_id in archive_ids {
                // get this archives valid data entries
                if let Some(entries) = sorted.entries.remove(&old_id) {
                    // build the path to this archive file
                    let path = self.archive_path.join(old_id.to_string());
                    // get a handle to this archive
                    let archive = DmaFile::open(&path).await?;
                    // get the size of this file
                    let size = archive.file_size().await?;
                    // skip this file if its more then 50% utilized
                    if *used as f64 > size as f64 * 0.50 {
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
                        // get a new active archive writer
                        let new_writer = self.map.get_active_writer().await?;
                        // swap our writers and close our old one
                        let mut old_writer = std::mem::replace(&mut self.writer, new_writer);
                        // close our old writer
                        old_writer.close().await?;
                        // don't compact/delete our old active archive this loop as that can lead to
                        // dangling partitions if we have already compacted data to
                        // the prior active archive in this compaction
                        continue;
                    }
                    // get a copy of our active archive id
                    let active_id = *self.map.active.borrow();
                    // read all of the still valid data from this archive
                    for mut entry in entries {
                        // read this entry from our archive file
                        let read = archive.read_at(entry.offset, entry.size).await?;
                        // write our size
                        self.writer.write_all(&entry.size.to_le_bytes()).await?;
                        // get the current positions of the writer
                        let start = self.writer.current_pos();
                        // write this entry to our new archive
                        self.writer.write_all(&read[..]).await?;
                        // update our entries info
                        entry.archive = active_id;
                        entry.offset = start;
                        // wrap our entry in a map intent
                        let intent = MapIntent::Entry(entry);
                        // write this map intent to our map intent log
                        let entry = write_map_intent!(self.map_writer, intent, Entry);
                        // add this entry to our entries list
                        self.entries.push((entry.key, entry))
                    }
                    // close our archive
                    archive.close().await?;
                    // build an intent that we are deleting this archive
                    let intent = MapIntent::DeleteArchive(*old_id);
                    // archive this intent log entry
                    let archived_intent = rkyv::to_bytes::<Error>(&intent)?;
                    // get the size of the data to write
                    let size = archived_intent.len();
                    // compute checksum
                    let mut hasher = GxHasher::default();
                    hasher.write(archived_intent.as_slice());
                    let checksum = hasher.finish();
                    // write the size of our archived entry data
                    self.map_writer.write_all(&size.to_le_bytes()).await?;
                    // write our checksum
                    self.map_writer.write_all(&checksum.to_le_bytes()).await?;
                    // write our archived partition map data
                    self.map_writer
                        .write_all(archived_intent.as_slice())
                        .await?;
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
                    // build an intent that we are deleting this archive
                    let intent = MapIntent::DeleteArchive(*old_id);
                    // archive this intent log entry
                    let archived_intent = rkyv::to_bytes::<Error>(&intent)?;
                    // get the size of the data to write
                    let size = archived_intent.len();
                    // compute checksum
                    let mut hasher = GxHasher::default();
                    hasher.write(archived_intent.as_slice());
                    let checksum = hasher.finish();
                    // write the size of our archived entry data
                    self.map_writer.write_all(&size.to_le_bytes()).await?;
                    // write our checksum
                    self.map_writer.write_all(&checksum.to_le_bytes()).await?;
                    // write our archived partition map data
                    self.map_writer
                        .write_all(archived_intent.as_slice())
                        .await?;
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
        self.writer.sync().await?;
        self.map_writer.sync().await?;
        // calculate the stats for this round of compaction
        let post_compaction = self.writer.current_pos() - start_pos;
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
        if self.map_writer.current_flushed_pos() > Byte::MEBIBYTE {
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
        // flush and close our writer
        self.writer.sync().await?;
        self.writer.close().await?;
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
            // build an intent that we are deleting this archive
            let intent = MapIntent::DeleteArchive(active_id);
            // archive this intent log entry
            let archived_intent = rkyv::to_bytes::<Error>(&intent)?;
            // get the size of the data to write
            let size = archived_intent.len();
            // compute checksum
            let mut hasher = GxHasher::default();
            hasher.write(archived_intent.as_slice());
            let checksum = hasher.finish();
            // write the size of our archived entry data
            self.map_writer.write_all(&size.to_le_bytes()).await?;
            // write our checksum
            self.map_writer.write_all(&checksum.to_le_bytes()).await?;
            // write our archived partition map data
            self.map_writer
                .write_all(archived_intent.as_slice())
                .await?;
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
        // flush and close our map writer
        self.map_writer.sync().await?;
        self.map_writer.close().await?;
        Ok(())
    }

    /// Start this compactor
    ///
    /// This is skipped by the profiler. It is a task that runs for as long as the shard does,
    /// so measuring it reports the process lifetime rather than any work, and at roughly 24x
    /// the run length it swamps every real entry in the report. The compaction work itself is
    /// measured through `compact_intent`, `compact_archives` and their callees.
    #[cfg_attr(feature = "hotpath", hotpath::skip)]
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
        loop {
            // wait for a intent log compaction job
            let job = self.jobs_rx.recv().await?;
            // handle this job;
            match job.clone() {
                CompactionJob::IntentLog { path, generation } => {
                    self.compact_intent(path, generation).await?
                }
                CompactionJob::Archives => self.compact_archives().await?,
                CompactionJob::Shutdown => {
                    // shutdown this compactor
                    self.shutdown().await?;
                    // stop handling compactor jobs
                    break;
                }
            }
        }
        Ok(())
    }
}
