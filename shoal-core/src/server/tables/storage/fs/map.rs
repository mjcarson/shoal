//! A map of archives for the file system storage engine

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use glommio::GlommioError;
use gxhash::GxHasher;
use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use crate::server::errors::ShoalError;
use crate::server::ServerError;
use crate::shared::traits::TableNameSupport;
use crate::storage::{ArchiveMapKinds, FilteredFullArchiveMap, FullArchiveMap};

use super::conf::FileSystemTableConf;
use super::reader::IntentLogReader;

/// An entry for a partitions data in an archive
#[derive(Debug, Archive, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveEntry {
    /// The key to this partition
    pub key: u64,
    /// The id of the archive this partition is on
    pub archive: Uuid,
    /// The start of this partitions data
    pub offset: u64,
    /// The length of this partitions data
    pub size: usize,
}

/// The different kinds of map intents
#[derive(Debug, PartialEq, Eq)]
pub enum MapIntentKinds {
    DeleteArchive,
    Entry,
    Remove,
}

/// An intent line for our map intent log
///
/// New variants must be appended, since the discriminants of the existing ones
/// are what already written intent logs are read back with.
#[derive(Debug, Archive, Deserialize, Serialize)]
pub enum MapIntent {
    /// An archive has been deleted and is no longer in use
    DeleteArchive(Uuid),
    /// A new entry for partition in an archive
    Entry(ArchiveEntry),
    /// A partition has been pruned and no longer has data in any archive
    Remove(u64),
}

impl MapIntent {
    /// Create a new archive entry MapIntent variant
    ///
    /// # Arguments
    ///
    /// * `key` - The key for this partition
    /// * `archive` - The id for the archive that contains this partition
    /// * `offset` - The start byte for this archive
    /// * `size` - The length of this partitions data in bytes
    pub fn entry(key: u64, archive: Uuid, offset: u64, size: usize) -> Self {
        // create a new archive entry
        let entry = ArchiveEntry {
            key,
            archive,
            offset,
            size,
        };
        // wrap our entry in our map intent enum
        MapIntent::Entry(entry)
    }

    /// Ensure that an intent is of a certain kind
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind to compare against
    pub fn is_kind(&self, kind: MapIntentKinds) -> bool {
        match self {
            MapIntent::DeleteArchive(_) => kind == MapIntentKinds::DeleteArchive,
            MapIntent::Entry(_) => kind == MapIntentKinds::Entry,
            MapIntent::Remove(_) => kind == MapIntentKinds::Remove,
        }
    }
}

/// A serialized archive map
#[derive(Debug, Archive, Deserialize, Serialize, Clone)]
pub struct SerializedMap {
    /// All archives this shard knows about
    all_archives: HashSet<Uuid>,
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
            // try to deserialize this archive entry from our intent log
            let archived = rkyv::access::<ArchivedMapIntent, rkyv::rancor::Error>(&read[..])?;
            // deserialize this entry
            let intent = rkyv::deserialize::<MapIntent, rkyv::rancor::Error>(archived)?;
            // add this map intent to our map
            match intent {
                MapIntent::DeleteArchive(id) => {
                    self.all_archives.remove(&id);
                }
                // add this entry to our map
                MapIntent::Entry(entry) => {
                    self.to_archive.insert(entry.key, entry);
                }
                // this partition was pruned so it no longer has an archive entry
                MapIntent::Remove(key) => {
                    self.to_archive.remove(&key);
                }
            }
        }
        // close our reader
        reader.close().await?;
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
    pub async fn new(
        map_path: &PathBuf,
        intent_path: &PathBuf,
        from: &str,
    ) -> Result<Self, ServerError> {
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
            // close our file
            file.close().await?;
            // get our maps xxh3 hash
            if read.len() < 8 {
                return Err(ServerError::Shoal(ShoalError::TruncatedIntentLog));
            }
            let expected = u64::from_le_bytes(read[..8].try_into()?);
            // build a hasher to verify this map
            let mut hasher = GxHasher::default();
            // hash our map
            hasher.write(&read[8..]);
            // get theh hash for our
            let found = hasher.finish();
            // if our hashes don't match then panic
            if expected != found {
                // build a shoal map corruption error
                let shoal_err = ShoalError::MapCorruption { found, expected };
                // return our map corruption error
                return Err(ServerError::Shoal(shoal_err));
            }
            // try to deserialize this archive map
            let archived = rkyv::access::<ArchivedSerializedMap, rkyv::rancor::Error>(&read[8..])?;
            // deserialize this map
            let mut map = rkyv::deserialize::<SerializedMap, rkyv::rancor::Error>(archived)?;
            // load our intent log
            map.load_intent_log(intent_path).await?;
            Ok(map)
        } else {
            // close our file
            file.close().await?;
            // build a default serializable map
            let map = SerializedMap {
                all_archives: HashSet::with_capacity(1000),
                to_archive: std::collections::HashMap::with_capacity(1000),
            };
            Ok(map)
        }
    }

    #[instrument(name = "SerializableMap::save", skip_all, err(Debug))]
    pub async fn save(map: &ArchiveMap) -> Result<(), ServerError> {
        // load our current committed map data from disk
        let serializable = SerializedMap {
            all_archives: map.all_archives.borrow().clone(),
            to_archive: map.to_archive.borrow().clone(),
        };
        // serialized this data
        let archived = rkyv::to_bytes::<Error>(&serializable)?;
        // hash our map
        let mut hasher = GxHasher::default();
        // hash our archive map
        hasher.write(&archived);
        // get our maps archive
        let map_hash = hasher.finish();
        // open a file to store our new map at temporarily
        let temp_map = OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(true)
            .dma_open(&map.temp_map_path)
            .await?;
        // wrap our map in a stream writer
        let mut writer = DmaStreamWriterBuilder::new(temp_map).build();
        // write our map hash to disk
        writer.write_all(&map_hash.to_le_bytes()).await?;
        // write this map to disk and sync it
        writer.write_all(&archived).await?;
        // sync and close our writer
        writer.sync().await?;
        writer.close().await?;
        // rename our temp path to our current one
        glommio::io::rename(&map.temp_map_path, &map.map_path).await?;
        // fsync the parent directory to ensure the rename is durable
        if let Some(parent) = map.map_path.parent() {
            let dir = glommio::io::Directory::open(parent).await?;
            dir.sync().await?;
            dir.close().await?;
        }
        Ok(())
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

impl<N: TableNameSupport> From<&FullArchiveMap<N>> for FilteredFullArchiveMap<N, ArchiveMap> {
    /// Filter a full archive map down to just a specfic storage engines maps
    ///
    /// # Arguments
    ///
    /// * `full` - The full archive map to filter
    fn from(full: &FullArchiveMap<N>) -> Self {
        // create a map to store our filesystem maps in
        let mut filtered: HashMap<N, Arc<ArchiveMap>> = HashMap::default();
        // step over all of our maps
        for (name, map) in full.map.borrow().iter() {
            // only add filesystem maps
            let ArchiveMapKinds::FileSystem(fs_map) = &*map;
            // add this filesystem map to our filtered map
            filtered.insert(*name, fs_map.clone());
        }
        // return only the filesystem maps
        FilteredFullArchiveMap {
            map: RefCell::new(filtered),
        }
    }
}

impl<N: TableNameSupport> FilteredFullArchiveMap<N, ArchiveMap> {
    /// Get the archive map for a single table
    ///
    /// The handle is cloned out rather than borrowed, so a caller that goes on to await
    /// against it is not holding a `RefCell` borrow of this map while it does.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to get the archive map of
    pub fn get_table_map(&self, table_name: N) -> Result<Arc<ArchiveMap>, ServerError> {
        // get this tables archive map, cloning the handle so the borrow ends here
        match self.map.borrow().get(&table_name) {
            Some(table_map) => Ok(table_map.clone()),
            None => Err(ServerError::Shoal(ShoalError::TableMapMissing)),
        }
    }

}

/// A map of archives for the file system storage engine
#[derive(Debug)]
pub struct ArchiveMap {
    /// The name of the table we are an archive map for
    table_name: String,
    /// The currently active archive id
    pub active: RefCell<Uuid>,
    /// A shard local map of what archives contain what data
    pub to_archive: RefCell<HashMap<u64, ArchiveEntry>>,
    /// A map of loaded archives
    pub loaded_archives: RefCell<HashMap<Uuid, DmaFile>>,
    /// All archives this shard knows about
    pub all_archives: RefCell<HashSet<Uuid>>,
    /// The path to this shards compacted and comitted archive map data
    pub map_path: PathBuf,
    /// The path for this shards temporary archive map
    pub temp_map_path: PathBuf,
    /// The path to this shards archive map intent log
    pub intent_path: PathBuf,
    /// The config for this table
    conf: FileSystemTableConf,
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
        // TODO make issue about SerializedMap not needing to track active
        let serializable = SerializedMap::new(&map_path, &intent_path, "new").await?;
        // start out with an empty hash map with room for 1k partitions
        let to_archive = RefCell::new(HashMap::with_capacity(1000));
        // load all of this shards keys into this map
        for (key, entry) in serializable.to_archive {
            // add this entry from our map
            to_archive.borrow_mut().insert(key, entry);
        }
        // just use empty maps for now
        let map = ArchiveMap {
            table_name: table_name.to_owned(),
            active: RefCell::new(Uuid::new_v4()),
            to_archive,
            loaded_archives: RefCell::new(HashMap::with_capacity(1000)),
            all_archives: RefCell::new(serializable.all_archives),
            map_path,
            temp_map_path,
            intent_path,
            conf: conf.clone(),
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
        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(self.conf.throughput_sensitive.buffer_size)
            .with_write_behind(self.conf.throughput_sensitive.write_behind)
            .build();
        Ok(writer)
    }

    /// Build a writer for the currently active archive writer
    #[instrument(name = "ArchiveMap::get_active_writer", skip_all, err(Debug))]
    pub async fn get_active_writer(&self) -> Result<DmaStreamWriter, ServerError> {
        // if we already have the current active file open then just make a writer for it
        if let Some(file) = self.loaded_archives.borrow_mut().get(&self.active.borrow()) {
            return Ok(DmaStreamWriterBuilder::new(file.dup()?).build());
        }
        // add this archive to our active archive set
        self.all_archives.borrow_mut().insert(*self.active.borrow());
        // build the path to this archive
        let mut path = self.conf.get_archive_path(&self.table_name);
        // add our active id
        path.push(self.active.borrow().to_string());
        // open this file
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // clone this file handle and place it in our archive map
        self.add_archive(*self.active.borrow(), file.dup()?);
        // build a stream writer for this file
        Ok(DmaStreamWriterBuilder::new(file).build())
    }

    /// Update the location for a partition
    ///
    /// # Arguments
    ///
    /// * `id` - The key of the partition to set the location for
    /// * `entry` - The archive entry for this partitions data
    pub fn set_partition(&self, id: u64, entry: ArchiveEntry) {
        // insert or update this partitions entry
        self.to_archive.borrow_mut().insert(id, entry);
    }

    /// Drop the location for a partition that no longer has any data
    ///
    /// Without this a pruned partition keeps pointing at its pre-delete copy in
    /// an old archive, and the next read resurrects the deleted data.
    ///
    /// # Arguments
    ///
    /// * `id` - The key of the partition to forget
    pub fn remove_partition(&self, id: u64) {
        // drop this partitions entry
        self.to_archive.borrow_mut().remove(&id);
    }

    /// Get a handle to an archive if it exists
    pub async fn get_archive(&self, archive_id: &Uuid) -> Result<DmaFile, ServerError> {
        // check if this archive is in our archive map
        if let Some(archive) = self.loaded_archives.borrow().get(archive_id) {
            return Ok(archive.dup()?);
        }
        // we don't have a handle to this archive so get one
        // build the path to this archive
        let mut path = self.conf.get_archive_path(&self.table_name);
        // add our active id
        path.push(archive_id.to_string());
        // open this file
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // clone this file handle and place it in our archive map
        self.add_archive(*archive_id, file.dup()?);
        Ok(file)
    }

    /// Find the location for a partition
    pub fn find_partition(&self, id: u64) -> Option<ArchiveEntry> {
        // get the entry for this partition if it exists
        match self.to_archive.borrow().get(&id) {
            Some(entry) => Some(*entry),
            None => None,
        }
    }

    /// Add a new archive to our map
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the archive we are adding a file handle for
    /// * `file` - The handle to this archive
    pub fn add_archive(&self, id: Uuid, file: DmaFile) {
        // insert or update this partitions entry to disk
        self.loaded_archives.borrow_mut().insert(id, file);
    }

    /// Remove an archive from our map
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the archive we are removing
    pub async fn remove_archive(&self, id: &Uuid) -> Result<(), ServerError> {
        // remove this archive from our loaded archive file handle map
        if let Some(removed) = self.loaded_archives.borrow_mut().remove(id) {
            removed.close().await?;
        }
        // remove this archive from our map of all archives
        self.all_archives.borrow_mut().remove(id);
        Ok(())
    }

    /// Sort our archives by how much data they have
    ///
    /// They will be sorted from least used to most used.
    #[instrument(name = "ArchiveMap::sort_by_load", skip_all)]
    pub fn sort_by_load(&self) -> SortedUsageMap {
        // get the length of our archive map
        let archive_len = self.loaded_archives.borrow().len();
        // count how many times each archive is used
        let mut used_by: std::collections::HashMap<Uuid, usize> =
            std::collections::HashMap::with_capacity(archive_len);
        // prepopulate our used by map with 0 for each archive
        for archive in self.all_archives.borrow().iter() {
            // default our used by count to 0 for all archives
            used_by.insert(*archive, 0);
        }
        // build a map to sort our archive maps by number of entries in
        let mut sorted = SortedUsageMap::with_capacity(archive_len);
        // step over our to archive map
        for (_, archive_entry) in self.to_archive.borrow().iter() {
            // get an entry to this archives count
            let entry: &mut usize = used_by.entry(archive_entry.archive).or_default();
            // increment this archived used by count
            *entry += archive_entry.size;
            // get an entry to this archives archive entries
            let entries_entry = sorted.entries.entry(archive_entry.archive).or_default();
            // add this archive entry to our sorted map
            entries_entry.push(*archive_entry);
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

    ///  Close all of the archives in our map
    pub async fn close_all(&self) -> Result<(), ServerError> {
        // get all of the keys in our archive map
        // step over each archive and close it
        for (_, archive) in self.loaded_archives.take() {
            // close this archive
            archive.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use glommio::LocalExecutor;
    use std::hash::Hasher;
    use std::path::PathBuf;
    use tempfile::TempDir;

    use super::{ArchiveEntry, GxHasher, HashSet, MapIntent, SerializedMap, Uuid};

    /// Create a temp dir on a filesystem that supports direct IO
    ///
    /// `TempDir::new` uses `/tmp`, which is usually tmpfs, and glommio silently
    /// disables O_DIRECT there.
    fn test_dir() -> TempDir {
        // build a path under cargo's target dir, which is on a real filesystem
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
        // make sure our base dir exists
        std::fs::create_dir_all(&base).expect("Failed to create test tmp dir");
        TempDir::new_in(&base).expect("Failed to create temp dir")
    }

    /// Frame a map intent as `[8-byte size][8-byte checksum][data]`
    ///
    /// # Arguments
    ///
    /// * `intent` - The map intent to serialize and frame
    fn framed(intent: &MapIntent) -> Vec<u8> {
        // serialize this intent
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(intent).unwrap();
        // compute a checksum over our payload
        let mut hasher = GxHasher::default();
        hasher.write(archived.as_slice());
        let checksum = hasher.finish();
        // build our framed record
        let mut record = Vec::with_capacity(16 + archived.len());
        record.extend_from_slice(&archived.len().to_le_bytes());
        record.extend_from_slice(&checksum.to_le_bytes());
        record.extend_from_slice(archived.as_slice());
        record
    }

    /// Serialize a map snapshot the way `SerializedMap::save` lays it out
    ///
    /// # Arguments
    ///
    /// * `map` - The map to serialize
    fn snapshot(map: &SerializedMap) -> Vec<u8> {
        // serialize this map
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(map).unwrap();
        // hash our map
        let mut hasher = GxHasher::default();
        hasher.write(&archived);
        // write the hash ahead of the payload
        let mut out = hasher.finish().to_le_bytes().to_vec();
        out.extend_from_slice(&archived);
        out
    }

    #[test]
    /// A remove intent drops a pruned partitions entry when the map is loaded back
    fn remove_intent_drops_an_entry() {
        LocalExecutor::default().run(async {
            // get a temp dir to build our fixtures in
            let temp_dir = test_dir();
            let map_path = temp_dir.path().join("test-map");
            let intent_path = temp_dir.path().join("test-map-intent");
            // build the archive entry our snapshot starts with
            let stale = ArchiveEntry {
                key: 42,
                archive: Uuid::new_v4(),
                offset: 0,
                size: 128,
            };
            // build a snapshot that already knows about that partition
            let mut snapshot_map = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
            };
            snapshot_map.to_archive.insert(stale.key, stale);
            // write our snapshot to disk
            std::fs::write(&map_path, snapshot(&snapshot_map)).unwrap();
            // log an entry for a second partition and the removal of the first
            let mut intents = framed(&MapIntent::entry(7, stale.archive, 256, 64));
            intents.extend_from_slice(&framed(&MapIntent::Remove(stale.key)));
            std::fs::write(&intent_path, &intents).unwrap();
            // load this map back with its intent log applied
            let loaded =
                SerializedMap::new(&map_path.to_path_buf(), &intent_path.to_path_buf(), "test")
                    .await
                    .expect("Failed to load map");
            // our pruned partition should be gone
            assert!(!loaded.to_archive.contains_key(&stale.key));
            // and our logged entry should still be there
            assert!(loaded.to_archive.contains_key(&7));
        });
    }
}
