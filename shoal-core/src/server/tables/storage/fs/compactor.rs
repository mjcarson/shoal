//! The file system compaction utilties for intent logs/archives

use byte_unit::Byte;
use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, OpenOptions};
use kanal::AsyncReceiver;
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
use crate::server::ServerError;
use crate::shared::traits::{PartitionKeySupport, RkyvSupport};
use crate::storage::{CompactionJob, IntentReadSupport, ShouldPrune};

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
        // write the size of our archived entry data
        $map_writer.write_all(&size.to_le_bytes()).await?;
        // write our archived partition map data
        $map_writer.write_all(archived_intent.as_slice()).await?;
        // ensure that during testing/development we always have an entry intent
        assert!($intent.is_kind(MapIntentKinds::$variant));
        // unwrap our intent to add the entry to our entry list
        match $intent {
            // save this entry to be added to our map after these writes sync
            MapIntent::$variant(entry) => entry,
            // we cannot have any other type here then an entry
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }};
}

pub struct FileSystemCompactor<T: IntentReadSupport<R>, R: PartitionKeySupport> {
    /// The shard local shared map of archive/partition data
    map: Arc<ArchiveMap>,
    /// The file to write compacted partition data too
    writer: DmaStreamWriter,
    /// The writer for updates to our archive maps partition data
    map_writer: DmaStreamWriter,
    /// The changes to apply to the already compacted partitions on disk
    changes: HashMap<u64, Vec<T::Intent>>,
    /// The partitions in this intent log
    loaded: HashMap<u64, T>,
    /// The entries to add to our archive map after syncing writes
    entries: Vec<(u64, ArchiveEntry)>,
    /// The channel to listen for paths to intent logs to compact
    jobs_rx: AsyncReceiver<CompactionJob>,
    /// The path to this tables archive folder
    archive_path: PathBuf,
    /// The row type this table contains
    row_kind: PhantomData<R>,
}

impl<T: IntentReadSupport<R>, R: PartitionKeySupport> FileSystemCompactor<T, R> {
    /// Create a new filesystem compactor
    #[instrument(name = "FileSystemCompactor::with_capacity", skip_all, err(Debug))]
    pub async fn with_capacity(
        conf: &FileSystemTableConf,
        jobs_rx: AsyncReceiver<CompactionJob>,
        map: &Arc<ArchiveMap>,
        capacity: usize,
    ) -> Result<Self, ServerError> {
        // compact any existing map data
        let map_writer = map.compact_map().await?;
        // get our currently active archive writer
        let writer = map.get_active_writer().await?;
        // build a file system compactor
        let compactor = FileSystemCompactor {
            map: map.clone(),
            writer,
            map_writer,
            changes: HashMap::with_capacity(capacity),
            loaded: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            jobs_rx,
            archive_path: conf.get_archive_path(R::name()),
            row_kind: PhantomData,
        };
        Ok(compactor)
    }

    /// Read and sort this intent log by partition
    #[instrument(name = "FileSystemCompactor::sort_intent_log", skip_all, err(Debug))]
    async fn sort_intent_log(&mut self, path: &PathBuf) -> Result<(), ServerError>
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
        // close our reader
        reader.close().await?;
        Ok(())
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
                // pint our archive map
                let mut map = self.map.loaded_archives.borrow_mut();
                // get the handle to this archive
                let handle = match map.get(&entry.archive) {
                    Some(archive) => archive,
                    None => {
                        // add this archive id onto our path
                        let path = self.archive_path.join(&entry.archive.to_string());
                        // get a handle to this archive
                        let handle = DmaFile::open(&path).await?;
                        // insert this handle
                        map.insert(entry.archive, handle);
                        // get a handle to this archive again
                        map.get(&entry.archive).unwrap()
                    }
                };
                // set options for reading from this file
                let read = handle.read_at(entry.offset, entry.size).await?;
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
    #[instrument(name = "FileSystemCompactor::apply_intents", skip_all, err(Debug))]
    async fn apply_intents(&mut self) -> Result<(), ServerError> {
        // replay all intents over our partitions
        for (partition, intents) in self.changes.drain() {
            // apply these intents to the correct partition
            if let ShouldPrune::Yes = T::apply_intents(&mut self.loaded, partition, intents) {
                // this partition should be pruned as it is empty
                self.loaded.remove(&partition);
                // TODO: does anything else need to be done to remove this partition
                // from archive maps?
            }
        }
        Ok(())
    }

    /// Write parititons to disk
    #[instrument(name = "FileSystemCompactor::write_partition", skip_all, err(Debug))]
    async fn write_partition(&mut self) -> Result<(), ServerError> {
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
            self.entries.push((*key, entry))
        }
        // flush our current writers
        self.writer.sync().await?;
        self.map_writer.sync().await?;
        // add the archive entries for the data we just synced
        for (id, entry) in self.entries.drain(..) {
            // add this entry to our shared map
            self.map.set_partition(id, entry);
        }
        // check how large our map intent log is and if needed compact it
        if self.map_writer.current_flushed_pos() > Byte::MEBIBYTE {
            // close our current map writer
            self.map_writer.close().await?;
            // compact our map data and get a new intent writer
            self.map_writer = self.map.compact_map().await?;
        }
        Ok(())
    }

    /// Compact an intent log down
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to compact
    #[instrument(name = "FileSystemCompactor::compact_intent", skip_all, err(Debug))]
    async fn compact_intent(&mut self, path: PathBuf) -> Result<(), ServerError>
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
        self.sort_intent_log(&path).await?;
        // check if we have any compacted partitions to write
        if !self.changes.is_empty() {
            // load any existing partitions from disk
            self.load_partitions_for_intents().await?;
            // apply the new intents to our loaded partitions
            self.apply_intents().await?;
            // write our compacted partitions to disk
            self.write_partition().await?;
            // delete our no longer needed inactive intent log
            glommio::io::remove(path).await?;
        }
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
                        if size < Byte::MEBIBYTE.multiply(100).unwrap() {
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
                    // write the size of our archived entry data
                    self.map_writer.write_all(&size.to_le_bytes()).await?;
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
                    // write the size of our archived entry data
                    self.map_writer.write_all(&size.to_le_bytes()).await?;
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
            // write the size of our archived entry data
            self.map_writer.write_all(&size.to_le_bytes()).await?;
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
                CompactionJob::IntentLog(path) => self.compact_intent(path).await?,
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
