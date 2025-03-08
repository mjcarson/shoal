//! The file system compaction utilties for intent logs/archives

use byte_unit::Byte;
use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter};
use kanal::AsyncReceiver;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::Archive;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{event, instrument, Level};

use super::conf::FileSystemTableConf;
use super::map::{ArchiveEntry, ArchiveMap};
use super::IntentLogReader;
use crate::server::ServerError;
use crate::shared::traits::{RkyvSupport, ShoalTable};
use crate::storage::{ArchivedIntents, CompactionJob, Intents};
use crate::tables::partitions::Partition;

pub struct FileSystemCompactor<T: ShoalTable> {
    /// The shard local shared map of archive/partition data
    map: Arc<ArchiveMap>,
    /// The file to write compacted partition data too
    writer: DmaStreamWriter,
    /// The writer for updates to our archive maps partition data
    map_writer: DmaStreamWriter,
    /// The changes to apply to the already compacted partitions on disk
    changes: HashMap<u64, Vec<Intents<T>>>,
    /// The partitions in this intent log
    loaded: HashMap<u64, Partition<T>>,
    /// The entries to add to our archive map after syncing writes
    entries: Vec<(u64, ArchiveEntry)>,
    /// The config for this table
    table_conf: FileSystemTableConf,
    /// The channel to listen for paths to intent logs to compact
    jobs_rx: AsyncReceiver<CompactionJob>,
    /// The path to this tables archive folder
    archive_path: PathBuf,
}

impl<T: ShoalTable> FileSystemCompactor<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Sort as Archive>::Archived: rkyv::Deserialize<T::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
    <<T as ShoalTable>::Sort as Archive>::Archived: Ord,
{
    /// Create a new filesystem compactor
    #[instrument(name = "FileSystemCompactor::with_capacity", skip_all, err(Debug))]
    pub async fn with_capacity(
        conf: &FileSystemTableConf,
        jobs_rx: AsyncReceiver<CompactionJob>,
        map: &Arc<ArchiveMap>,
        capacity: usize,
    ) -> Result<Self, ServerError> {
        // compact any existing map data
        map.compact_map().await?;
        // get a writer for this shards archive map
        let map_writer = map.new_writer().await?;
        // get our currently active archive writer
        let writer = map.get_active_writer(conf).await?;
        // build a file system compactor
        let compactor = FileSystemCompactor {
            map: map.clone(),
            writer,
            map_writer,
            changes: HashMap::with_capacity(capacity),
            loaded: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            table_conf: conf.clone(),
            jobs_rx,
            archive_path: conf.get_archive_path(T::name()),
        };
        Ok(compactor)
    }

    /// Read and sort this intent log by partition
    #[instrument(name = "FileSystemCompactor::sort_intent_log", skip_all, err(Debug))]
    async fn sort_intent_log(&mut self, path: &PathBuf) -> Result<(), ServerError> {
        // create a reader for this intent log
        let mut reader = IntentLogReader::new(&path).await?;
        // read all of the intent from this intent log
        while let Some(read) = reader.next_buff().await? {
            // try to deserialize this row from our intent log
            let archived = unsafe { rkyv::access_unchecked::<ArchivedIntents<T>>(&read[..]) };
            // deserialize this intent
            let intent = rkyv::deserialize::<Intents<T>, rkyv::rancor::Error>(archived)?;
            // get this intent entries partition key
            let partition_key = match &intent {
                Intents::Insert(row) => row.get_partition_key(),
                Intents::Delete { partition_key, .. } => *partition_key,
                Intents::Update(update) => update.partition_key,
            };
            // add this change to our changes vec
            let entry = self.changes.entry(partition_key).or_default();
            // add our change
            entry.push(intent);
        }
        Ok(())
    }

    /// Load all of our partitions from disk
    #[instrument(
        name = "FileSystemCompactor::load_partitions_for_intents",
        skip_all,
        err(Debug)
    )]
    async fn load_partitions_for_intents(&mut self) -> Result<(), ServerError> {
        // crawl over all partitions with intents
        for partition in self.changes.keys() {
            // get this partiitons current archive it it exists
            if let Some(entry) = self.map.to_archive.pin().get(partition) {
                // pint our archive map
                let pinned = self.map.archives.pin();
                // get the handle to this archive
                let handle = match pinned.get(&entry.archive) {
                    Some(archive) => archive,
                    None => {
                        // add this archive id onto our path
                        let path = self.archive_path.join(&entry.archive.to_string());
                        // get a handle to this archive
                        let handle = DmaFile::open(&path).await?;
                        // insert this handle
                        pinned.insert(entry.archive, handle);
                        // get a handle to this archive again
                        pinned.get(&entry.archive).unwrap()
                    }
                };
                // set options for reading from this file
                let read = handle.read_at(entry.start, entry.size).await?;
                // load this partitions data
                let archived = Partition::<T>::load(&read[..]);
                // deserialize this partition
                let deserialized = Partition::<T>::deserialize(archived)?;
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
            // get this partitions data
            let entry = self
                .loaded
                .entry(partition)
                .or_insert_with(|| Partition::new(partition));
            // apply all of this partitions intents
            for intent in intents {
                match intent {
                    Intents::Insert(row) => {
                        entry.insert(row);
                    }
                    Intents::Delete { sort_key, .. } => {
                        entry.remove(&sort_key);
                    }
                    Intents::Update(update) => {
                        entry.update(&update);
                    }
                }
            }
        }
        Ok(())
    }

    /// Write parititons to disk
    #[instrument(name = "FileSystemCompactor::write_partition", skip_all, err(Debug))]
    async fn write_partition(&mut self) -> Result<(), ServerError> {
        // write all of our compacted partitions to disk
        for (id, partition) in &self.loaded {
            // serialize this partitions data
            let archived = rkyv::to_bytes::<_>(partition)?;
            // get the size of the data to write
            // this size is only used in recovery operations of archive files
            let size = archived.len();
            // write our size
            self.writer.write_all(&size.to_le_bytes()).await?;
            // get the current positions of the writer
            let start = self.writer.current_pos();
            // write this archived partition
            self.writer.write_all(archived.as_slice()).await?;
            // build the archive entry for this partitions data
            let entry = ArchiveEntry::new(*id, self.map.active, start, size);
            // archive this intent log entry
            let archived_entry = rkyv::to_bytes::<Error>(&entry)?;
            // get the size of the data to write
            let size = archived_entry.len();
            // write the size of our archived entry data
            self.map_writer.write_all(&size.to_le_bytes()).await?;
            // write our archived partition map data
            self.map_writer.write_all(archived_entry.as_slice()).await?;
            // save this entry to be added to our map after these writes sync
            self.entries.push((*id, entry));
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
    async fn compact_intent(&mut self, path: PathBuf) -> Result<(), ServerError> {
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
        let mut sorted = self.map.sort_by_load(self.map.active);
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
                    // skip this file if its more then 50% utilized
                    if *used as f64 > archive.file_size().await? as f64 * 0.60 {
                        // this file is largely valid so don't compact it
                        continue;
                    }
                    // read all of the still valid data from this archive
                    for mut entry in entries {
                        // read this entry from our archive file
                        let read = archive.read_at(entry.start, entry.size).await?;
                        // write our size
                        self.writer.write_all(&entry.size.to_le_bytes()).await?;
                        // get the current positions of the writer
                        let start = self.writer.current_pos();
                        // write this entry to our new archive
                        self.writer.write_all(&read[..]).await?;
                        // update our entries info
                        entry.archive = self.map.active;
                        entry.start = start;
                        // archive this intent log entry
                        let archived_entry = rkyv::to_bytes::<Error>(&entry)?;
                        // get the size of the data to write
                        let size = archived_entry.len();
                        // write the size of our archived entry data
                        self.map_writer.write_all(&size.to_le_bytes()).await?;
                        // write our archived partition map data
                        self.map_writer.write_all(archived_entry.as_slice()).await?;
                        // save this entry to be added to our map after these writes sync
                        self.entries.push((entry.key, entry));
                    }
                    // save this archive path for deletion
                    old_paths.push((old_id, path));
                    // add this to our total compacted size
                    precompaction += used;
                }
            }
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
        // check how large our map intent log is and if needed compact it
        if self.map_writer.current_flushed_pos() > Byte::MEBIBYTE {
            // compact our map data
            self.map_writer = self.map.compact_map().await?;
        }
        // delete our old archive files
        for (old_id, old_archive) in old_paths {
            // remove this archive from our map
            self.map.remove_archive(old_id);
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

    /// Start this compactor
    pub async fn start(mut self) -> Result<(), ServerError> {
        loop {
            // wait for a intent log compaction job
            let job = self.jobs_rx.recv().await?;
            // handle this job
            match job {
                CompactionJob::IntentLog(path) => self.compact_intent(path).await?,
                CompactionJob::Archives => self.compact_archives().await?,
                CompactionJob::Shutdown => break,
            }
        }
        Ok(())
    }
}
