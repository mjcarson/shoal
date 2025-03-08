//! A map of archives for the file system storage engine

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use glommio::GlommioError;
use papaya::HashMap;
use rkyv::collections::swiss_table::ArchivedHashMap;
use rkyv::primitive::ArchivedU64;
use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::instrument;
use uuid::Uuid;

use crate::server::ServerError;

use super::conf::FileSystemTableConf;
use super::reader::IntentLogReader;

/// An entry for a partitions data in an archive
#[derive(Debug, Archive, Deserialize, Serialize, Clone)]
pub struct ArchiveEntry {
    /// The key to this partition
    pub key: u64,
    /// The id of the archive this partition is on
    pub archive: Uuid,
    /// The start of this partitions data
    pub start: u64,
    /// The length of this partitions data
    pub size: usize,
}

impl ArchiveEntry {
    /// Create a new archive entry
    ///
    /// # Arguments
    ///
    /// * `key` - The key for this partition
    /// * `archive` - The id for the archive that contains this partition
    /// * `start` - The start byte for this archive
    /// * `size` - The length of this partitions data in bytes
    pub fn new(key: u64, archive: Uuid, start: u64, size: usize) -> Self {
        ArchiveEntry {
            key,
            archive,
            start,
            size,
        }
    }
}

/// A serialized archive map
#[derive(Debug, Archive, Deserialize, Serialize, Clone)]
pub struct SerializedMap {
    /// The currently active archive for this shard
    active: Uuid,
    /// The map of partitions keys to archive entries
    to_archive: std::collections::HashMap<u64, ArchiveEntry>,
}

impl SerializedMap {
    #[instrument(name = "SerializableMap::load_intent_log", skip(self), err(Debug))]
    async fn load_intent_log(&mut self, intent_path: &PathBuf) -> Result<(), ServerError> {
        // get a reader for this intent file
        let mut reader = IntentLogReader::new(intent_path).await?;
        // read all of the intent from this intent log
        while let Some(read) = reader.next_buff().await? {
            // try to deserialize this row from our intent log
            let archived = unsafe { rkyv::access_unchecked::<ArchivedArchiveEntry>(&read[..]) };
            // deserialize this entry
            let entry = rkyv::deserialize::<ArchiveEntry, rkyv::rancor::Error>(archived)?;
            self.to_archive.insert(entry.key, entry);
        }
        Ok(())
    }

    /// Load a map from disk
    ///
    /// This will read from an existing serialized map and its intent log.
    ///
    /// # Arguments
    ///
    /// * `map_path` - The path to an existing serialized archive map path
    #[instrument(name = "SerializableMap::new", err(Debug))]
    pub async fn new(map_path: &PathBuf, intent_path: &PathBuf) -> Result<Self, ServerError> {
        // open this shards map file
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(map_path)
            .await?;
        // get the size of this file
        let size = file.file_size().await?;
        // if this file contains data then load it otherwise use an empty map
        if size > 0 {
            // read this entire file at once
            let read = file.read_at(0, size as usize).await?;
            // try to deserialize this archive map
            let archived = unsafe { rkyv::access_unchecked::<ArchivedSerializedMap>(&read[..]) };
            // deserialize this map
            let mut map = rkyv::deserialize::<SerializedMap, rkyv::rancor::Error>(archived)?;
            // load our intent log
            map.load_intent_log(intent_path).await?;
            Ok(map)
        } else {
            // build a default serializable map
            let map = SerializedMap {
                active: Uuid::new_v4(),
                to_archive: std::collections::HashMap::with_capacity(1000),
            };
            Ok(map)
        }
    }

    #[instrument(name = "SerializableMap::save", skip_all, err(Debug))]
    pub async fn save(map: &ArchiveMap) -> Result<(), ServerError> {
        // load our current committed map data from disk
        let serializable = Self::new(&map.map_path, &map.intent_path).await?;
        // serialized this data
        let archived = rkyv::to_bytes::<Error>(&serializable)?;
        // open a file to store our new map at temporarily
        let temp_map = OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(true)
            .dma_open(&map.temp_map_path)
            .await?;
        // wrap our map in a stream writer
        let mut writer = DmaStreamWriterBuilder::new(temp_map).build();
        // write this map to disk and sync it
        writer.write_all(&archived).await?;
        // sync and close our writer
        writer.sync().await?;
        writer.close().await?;
        // rename our temp path to our current one
        glommio::io::rename(&map.temp_map_path, &map.map_path).await?;
        Ok(())
    }
}

/// load a map from disk
///
/// Wow the types are ugly in this function :(
#[instrument(name = "fs::map::load_map", err(Debug))]
async fn load_map(
    map_path: &PathBuf,
) -> Result<std::collections::HashMap<u64, ArchiveEntry>, ServerError> {
    // open this shards map file
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .dma_open(map_path)
        .await?;
    // get the size of this file
    let size = file.file_size().await?;
    // if this file contains data then load it otherwise use an empty map
    if size > 0 {
        // read this entire file at once
        let read = file.read_at(0, size as usize).await?;
        // try to deserialize this archive map
        let archived = unsafe {
            rkyv::access_unchecked::<ArchivedHashMap<ArchivedU64, ArchivedArchiveEntry>>(&read[..])
        };
        // deserialize this map
        let map = rkyv::deserialize::<
            std::collections::HashMap<u64, ArchiveEntry>,
            rkyv::rancor::Error,
        >(archived)?;
        Ok(map)
    } else {
        Ok(std::collections::HashMap::with_capacity(1000))
    }
}

/// All archives sorted by how much of it is used
pub struct SortedUsageMap {
    /// This shards archive ids sorted by total used bytes
    pub sorted: BTreeMap<usize, Vec<Uuid>>,
    /// The entries across all archive maps by archive
    pub entries: std::collections::HashMap<Uuid, Vec<ArchiveEntry>>,
}

impl SortedUsageMap {
    /// Create a new sorted usage map
    ///
    /// # Arguments
    ///
    /// * `capacity` - The capacity to set
    pub fn with_capacity(capacity: usize) -> Self {
        SortedUsageMap {
            sorted: BTreeMap::default(),
            entries: std::collections::HashMap::with_capacity(capacity),
        }
    }
}

/// A map of archives for the file system storage engine
#[derive(Debug)]
pub struct ArchiveMap {
    /// The name of the table we are an archive map for
    table_name: String,
    /// The currently active archive id
    pub active: Uuid,
    /// A shard local map of what archives contain what data
    pub to_archive: HashMap<u64, ArchiveEntry>,
    /// A map of archives
    pub archives: HashMap<Uuid, DmaFile>,
    /// The path to this shards compacted and comitted archive map data
    pub map_path: PathBuf,
    /// The path for this shards temporary archive map
    pub temp_map_path: PathBuf,
    /// The path to this shards archive map intent log
    pub intent_path: PathBuf,
}

impl ArchiveMap {
    /// Load an archive map for this shard from disk if one exists
    #[instrument(name = "ArchiveMap::new", skip(conf), err(Debug))]
    pub async fn new(
        shard_name: &str,
        table_name: &str,
        conf: &FileSystemTableConf,
    ) -> Result<Self, ServerError> {
        // get the path to this shards map and its intent log
        let mut map_path = conf.get_archive_map_path(table_name);
        let mut temp_map_path = conf.get_archive_map_temp_path(table_name);
        let mut intent_path = conf.get_archive_intent_path(table_name);
        // add our shard name to our paths
        map_path.push(shard_name);
        temp_map_path.push(shard_name);
        intent_path.push(shard_name);
        // load our serializable map from disk so we can load it into our papaya map
        let serializable = SerializedMap::new(&map_path, &intent_path).await?;
        // start out with an empty papaya map with room for 1k partitions
        let to_archive = HashMap::with_capacity(1000);
        // load all of this shards keys into this map
        for (key, entry) in serializable.to_archive {
            // add this entry from our map
            to_archive.pin().insert(key, entry);
        }
        // just use empty maps for now
        let map = ArchiveMap {
            table_name: table_name.to_owned(),
            active: serializable.active,
            to_archive,
            archives: HashMap::with_capacity(1000),
            map_path,
            temp_map_path,
            intent_path,
        };
        Ok(map)
    }

    /// Build a writer for this maps data
    #[instrument(name = "ArchiveMap::new_writer", skip_all, err(Debug))]
    pub async fn new_writer(&self) -> Result<DmaStreamWriter, ServerError> {
        // open a file to our intent log
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .dma_open(&self.intent_path)
            .await?;
        // wrap our file in a stream writer
        let writer = DmaStreamWriterBuilder::new(file).build();
        Ok(writer)
    }

    /// Build a writer for the currently active archive writer
    #[instrument(name = "ArchiveMap::get_active_writer", skip_all, err(Debug))]
    pub async fn get_active_writer(
        &self,
        conf: &FileSystemTableConf,
    ) -> Result<DmaStreamWriter, ServerError> {
        // if we already have the current active file open then just make a writer for it
        match self.archives.pin().get(&self.active) {
            Some(file) => Ok(DmaStreamWriterBuilder::new(file.dup()?).build()),
            None => {
                // build the path to this archive
                let mut path = conf.get_archive_path(&self.table_name);
                // add our active id
                path.push(self.active.to_string());
                // open this file
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .dma_open(&path)
                    .await?;
                // clone this file handle and place it in our archive map
                self.add_archive(self.active, file.dup()?);
                // build a stream writer for this file
                Ok(DmaStreamWriterBuilder::new(file).build())
            }
        }
    }

    /// Update the location for a partition
    pub fn set_partition(&self, id: u64, entry: ArchiveEntry) {
        // insert or update this partitions entry
        self.to_archive.pin().insert(id, entry);
    }

    /// Add a new archive to our map
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the archive we are adding a file handle for
    /// * `file` - The handle to this archive
    pub fn add_archive(&self, id: Uuid, file: DmaFile) {
        // insert or update this partitions entry to disk
        self.archives.pin().insert(id, file);
    }

    /// Remove an archive from our map
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the archive we are removing
    pub fn remove_archive(&self, id: &Uuid) {
        // insert or update this partitions entry to disk
        self.archives.pin().remove(id);
    }

    /// Sort our archives by how much data they have
    ///
    /// They will be sorted from least used to most used.
    #[instrument(name = "ArchiveMap::sort_by_load", skip_all)]
    pub fn sort_by_load(&self, active_id: Uuid) -> SortedUsageMap {
        // count how many times each archive is used
        let mut used_by: std::collections::HashMap<Uuid, usize> =
            std::collections::HashMap::with_capacity(self.archives.len());
        // build a map to sort our archive maps by number of entries in
        let mut sorted = SortedUsageMap::with_capacity(self.archives.len());
        // step over our to archive map
        for (_, archive_entry) in self.to_archive.pin().iter() {
            // skip any entries for our active archive
            if archive_entry.archive != active_id {
                // get an entry to this archives count
                let entry: &mut usize = used_by.entry(archive_entry.archive).or_default();
                // increment this archived used by count
                *entry += archive_entry.size;
                // get an entry to this archives archive entries
                let entries_entry = sorted.entries.entry(archive_entry.archive).or_default();
                // add this archive entry to our sorted map
                entries_entry.push(archive_entry.clone());
            }
        }
        // add each used by count and sort them
        for (uuid, size) in used_by {
            // get an enty to this counts archive list
            let archive_entry: &mut Vec<Uuid> = sorted.sorted.entry(size).or_default();
            // add this archives uuid
            archive_entry.push(uuid);
        }
        sorted
    }

    /// Serialize and save an archive map to disk
    #[instrument(name = "ArchiveMap::compact_map", skip_all)]
    pub async fn compact_map(&self) -> Result<DmaStreamWriter, ServerError> {
        // compact and save our current committed map data
        SerializedMap::save(self).await?;
        // delete our current intent log if it exists
        // this is kind of ugly not sure if theres a cleaner way to do this
        if let Err(error) = glommio::io::remove(&self.intent_path).await {
            // check if this error was an io error
            if let GlommioError::IoError(io_error) = &error {
                // check if this io error was a file not found error
                if std::io::ErrorKind::NotFound != io_error.kind() {
                    // this is not a file not found error
                    return Err(ServerError::from(error));
                }
            } else {
                // this is not a file not found error
                return Err(ServerError::from(error));
            }
        }
        // get a new map intent writer
        let writer = self.new_writer().await?;
        Ok(writer)
    }
}
