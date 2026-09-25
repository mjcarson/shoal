//! A map of archives for the file system storage engine

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult};
use glommio::GlommioError;
use gxhash::GxHasher;
use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{event, instrument, Level};
use uuid::Uuid;

/// The share of the saved map the intent log may reach before it is folded into a new map
///
/// Four: the log is folded at a quarter of the map, so a fold writes at most four bytes of map
/// per byte of intent, and a restart replays at most a quarter of the map
/// ([O62](../../../../../../docs/src/appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)).
const MAP_FOLD_RATIO: u64 = 4;

/// The smallest intent log that is folded into a new map, whatever the map's size
///
/// The bound every fold used before the ratio, which is what a small map still gets.
const MAP_FOLD_FLOOR: u64 = 1024 * 1024;

use crate::server::errors::ShoalError;
use crate::server::ServerError;
use crate::shared::traits::TableNameSupport;
use crate::storage::{ArchiveMapKinds, FilteredFullArchiveMap, FullArchiveMap};

use super::conf::FileSystemTableConf;
use super::reader::IntentLogReader;

/// The magic a checksummed archive begins with
///
/// A format 1 archive begins with the size of its first record, and no partition is
/// `0x4352414c414f4853` bytes long, so the first eight bytes of a file say which format it is.
pub const ARCHIVE_MAGIC: &[u8; 8] = b"SHOALARC";

/// The archive format this build writes
pub const ARCHIVE_FORMAT: u32 = 2;

/// The length of a format 2 archive's header: the magic, the version and a reserved word
pub const ARCHIVE_HEADER_LEN: usize = 16;

/// The length of a format 2 record's prefix: the size and the checksum
pub const RECORD_PREFIX_LEN: u64 = 16;

/// What the records of an archive look like, and whether a read of one can be verified
///
/// An archive's format is fixed when it is created and never changes: a format 1 archive is
/// never written to again, since a restart mints a new active archive, and it is retired by
/// ordinary archive compaction, which rewrites what is still live in it into a format 2 one
/// ([F44](../../../../../../docs/src/features/repair.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    /// Format 1: `[size][payload]` records with nothing to verify a read against
    ///
    /// Written before F44. Corruption in one is caught only if rkyv's validation happens to
    /// reject it, and every read of one is counted as unverified.
    Unverified,
    /// Format 2: a header, then `[size][checksum][payload]` records
    ///
    /// The checksum is gxhash64 over the payload, seeded like the intent log's, and a read
    /// that does not hash to it is refused as [`ShoalError::CorruptArchive`].
    Checksummed,
}

impl ArchiveFormat {
    /// Decide an archive's format from its first bytes
    ///
    /// A file too short to hold the header is a format 1 archive that nothing was ever
    /// written to, or the active archive before its first sync: either way nothing points
    /// into it yet, and reading it as unverified is harmless.
    ///
    /// # Arguments
    ///
    /// * `head` - The first bytes of the archive, at least the header's length if it has one
    #[must_use]
    pub fn detect(head: &[u8]) -> Self {
        // a header is the magic, then the version this build knows
        if head.len() >= ARCHIVE_HEADER_LEN && &head[..8] == ARCHIVE_MAGIC {
            // the version follows the magic
            let version = u32::from_le_bytes([head[8], head[9], head[10], head[11]]);
            // this build writes format 2 and reads nothing newer
            if version == ARCHIVE_FORMAT {
                return ArchiveFormat::Checksummed;
            }
        }
        // no header, so this archive's records carry no checksum
        ArchiveFormat::Unverified
    }

    /// The header a new archive of this build begins with
    #[must_use]
    pub fn header() -> [u8; ARCHIVE_HEADER_LEN] {
        // the magic, the version and a reserved word
        let mut header = [0u8; ARCHIVE_HEADER_LEN];
        header[..8].copy_from_slice(ARCHIVE_MAGIC);
        header[8..12].copy_from_slice(&ARCHIVE_FORMAT.to_le_bytes());
        header
    }
}

/// The checksum a format 2 record carries for its payload
///
/// Seeded like the intent log's, so one hasher describes every checksummed record on disk.
///
/// # Arguments
///
/// * `payload` - The archived partition
#[must_use]
pub fn record_checksum(payload: &[u8]) -> u64 {
    // hash the payload with the same hasher the intent log uses
    let mut hasher = GxHasher::default();
    hasher.write(payload);
    hasher.finish()
}

/// Write one record into an archive: the size, the checksum, then the payload
///
/// This is the one place a record is written, whether by a compaction, an archive
/// compaction or a snapshot install, so every record a format 2 archive holds carries a
/// checksum. Returns the offset of the payload, which is what the map entry points at.
///
/// # Arguments
///
/// * `writer` - The active archive's writer
/// * `payload` - The archived partition
pub async fn write_record(
    writer: &mut DmaStreamWriter,
    payload: &[u8],
) -> Result<u64, ServerError> {
    // write the size of this record's payload
    writer.write_all(&payload.len().to_le_bytes()).await?;
    // then the checksum a read verifies it against
    writer
        .write_all(&record_checksum(payload).to_le_bytes())
        .await?;
    // the map entry points at the payload, not at the prefix
    let offset = writer.current_pos();
    // then the payload itself
    writer.write_all(payload).await?;
    Ok(offset)
}

/// What the archives of one table have seen of their own integrity
///
/// Counted on the map because it is the one thing every reader of a table's archives on a
/// shard shares - the loader, the compactor and the direct reads - and reported through the
/// shard's replication report ([F44](../../../../../../docs/src/features/repair.md)).
#[derive(Debug, Default)]
pub struct IntegrityCounters {
    /// Reads of format 1 records, which nothing could verify
    pub unverified_reads: Cell<u64>,
    /// Reads whose payload did not hash to its checksum
    pub checksum_failures: Cell<u64>,
}

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
            // try to deserialize this archive entry from our intent log. A whole frame that is
            // no intent can only be what a partial flush left past the log's end - recycled
            // buffer memory, whose archive records share an intent's framing and checksum -
            // so it ends the log the way a torn frame does, rather than failing the shard
            // ([Resolved #148](../../../../../../docs/src/appendix/resolved/stale-intent-log-tail.md))
            let decoded = rkyv::access::<ArchivedMapIntent, rkyv::rancor::Error>(&read[..])
                .and_then(rkyv::deserialize::<MapIntent, rkyv::rancor::Error>);
            let intent = match decoded {
                Ok(intent) => intent,
                Err(error) => {
                    tracing::warn!(
                        "A frame at position {} of {} is no map intent ({error}) - treating it as the end of the intent log",
                        reader.position,
                        intent_path.display()
                    );
                    break;
                }
            };
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
        // a temp map is only ever a save that never reached its rename, so one found here was
        // left by a process that died mid-save and holds nothing the committed map needs.
        // refusing to replace it failed the shard on every start after such a crash
        // ([Resolved #135](../../../../../../docs/src/appendix/resolved/leftover-temp-map.md))
        match std::fs::remove_file(&map.temp_map_path) {
            Ok(()) => event!(
                Level::WARN,
                msg = "removed a temp map a save that did not finish left behind",
                path = %map.temp_map_path.display(),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(ServerError::IO(error)),
        }
        // open a file to store our new map at temporarily; nobody else writes this shard's
        // map, so a file here now is a second writer and still refused
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
        // the size the next fold of the intent log is measured against: the hash and the map
        map.saved_bytes.set(archived.len() as u64 + 8);
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

/// Check whether a failed open means the file was not there
///
/// Glommio reports the same errno in two shapes depending on whether it had a path to attach
/// to it, and an open by path can come back as either, so both have to be unwrapped to the
/// `std::io::Error` underneath before its kind means anything.
///
/// # Arguments
///
/// * `error` - The error an open failed with
fn is_not_found(error: &GlommioError<()>) -> bool {
    // unwrap whichever shape this error came back in and ask what its errno was
    match error {
        GlommioError::IoError(source) | GlommioError::EnhancedIoError { source, .. } => {
            source.kind() == std::io::ErrorKind::NotFound
        }
        // every other glommio error is about something other than the file not being there
        _ => false,
    }
}

/// What a table's archives hold per tablet, indexed by tablet
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TabletUsage {
    /// The archived bytes of every partition of each tablet
    pub bytes: Vec<u64>,
    /// How many archived partitions each tablet holds
    pub partitions: Vec<u64>,
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
    /// The format of every loaded archive, decided once when its handle was opened
    formats: RefCell<HashMap<Uuid, ArchiveFormat>>,
    /// What this table's archives have seen of their own integrity
    pub integrity: IntegrityCounters,
    /// All archives this shard knows about
    pub all_archives: RefCell<HashSet<Uuid>>,
    /// The path to this shards compacted and comitted archive map data
    pub map_path: PathBuf,
    /// The path for this shards temporary archive map
    pub temp_map_path: PathBuf,
    /// The path to this shards archive map intent log
    pub intent_path: PathBuf,
    /// How large the map was when it was last saved, which the intent log is folded against
    ///
    /// A save rewrites the whole map, so folding the log each time it passes a fixed size makes
    /// the bytes written per intent grow with the map
    /// ([O62](../../../../../../docs/src/appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)).
    pub saved_bytes: Cell<u64>,
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
        // how large the map on disk is, which the intent log is folded against
        let saved_bytes = std::fs::metadata(&map_path).map_or(0, |meta| meta.len());
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
            formats: RefCell::new(HashMap::with_capacity(1000)),
            integrity: IntegrityCounters::default(),
            all_archives: RefCell::new(serializable.all_archives),
            map_path,
            temp_map_path,
            intent_path,
            saved_bytes: Cell::new(saved_bytes),
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
        // a new archive is this build's format, and begins with the header that says so
        self.formats
            .borrow_mut()
            .insert(*self.active.borrow(), ArchiveFormat::Checksummed);
        // build a stream writer for this file
        let mut writer = DmaStreamWriterBuilder::new(file).build();
        // write the header, so a reader can tell this archive's records carry checksums
        writer.write_all(&ArchiveFormat::header()).await?;
        Ok(writer)
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

    /// The bytes the archives hold per tablet, indexed by tablet
    ///
    /// One pass over the map, so the figure can never drift from what the map names: a
    /// partition replaced, removed or reloaded is counted as the map has it now
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md)).
    #[must_use]
    pub fn tablet_bytes(&self) -> Vec<u64> {
        self.tablet_usage().bytes
    }

    /// The bytes and partitions the archives hold per tablet, indexed by tablet
    ///
    /// The same one pass as [`ArchiveMap::tablet_bytes`], counting each partition once onto
    /// its tablet as well as its size, so a shard's report learns both for the price of the
    /// walk it already paid for the bytes ([F52](../../../docs/src/features/cluster-stats.md)).
    #[must_use]
    pub fn tablet_usage(&self) -> TabletUsage {
        // one slot per tablet for each figure
        let mut usage = TabletUsage {
            bytes: vec![0u64; crate::server::ring::TABLET_COUNT],
            partitions: vec![0u64; crate::server::ring::TABLET_COUNT],
        };
        // every partition the map names lands on the tablet its key hashes into
        for (key, entry) in self.to_archive.borrow().iter() {
            let tablet = crate::server::ring::Ring::tablet_of(*key);
            usage.bytes[tablet] += u64::try_from(entry.size).unwrap_or(u64::MAX);
            usage.partitions[tablet] += 1;
        }
        usage
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

    /// Get a handle to an archive that already exists
    ///
    /// This never creates the archive it is asked for. Every caller of this is a read, and a
    /// read whose archive is not on disk has to hear so: creating an empty one instead makes
    /// the read come back short and the failure surface later as a validation error on bytes
    /// nobody wrote, which cannot name the file that went missing. Creating an archive is
    /// [`ArchiveMap::get_active_writer`]'s job and only ever happens for the active one.
    ///
    /// The handle is opened for writing as well as reading even though this is a read path,
    /// because [`ArchiveMap::get_active_writer`] serves the active archive out of the same
    /// `loaded_archives` cache this one fills. A read only handle cached here for the active
    /// id would be duplicated into a stream writer that cannot write.
    ///
    /// # Arguments
    ///
    /// * `archive_id` - The id of the archive to get a handle to
    //
    // deliberately not instrumented: this runs on every partition read, including the ones
    // that hit the handle cache, and a span there costs a registry slab insert per read to
    // say what `loader::read_partition`'s span already covers
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
        // open this file, which must already be there
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .dma_open(&path)
            .await
        {
            Ok(file) => file,
            // this archive is not on disk, so say which one instead of making an empty one
            Err(error) if is_not_found(&error) => {
                return Err(ServerError::Shoal(ShoalError::ArchiveMissing {
                    archive: *archive_id,
                    path,
                }))
            }
            // any other failure to open is reported as the IO error it is
            Err(error) => return Err(error.into()),
        };
        // decide this archive's format from its first bytes, once for the life of the handle
        let head = file.read_at(0, ARCHIVE_HEADER_LEN).await?;
        let format = ArchiveFormat::detect(&head);
        // say so when an archive from before checksums is opened, since every read of it
        // is one nothing can verify
        if format == ArchiveFormat::Unverified {
            event!(Level::WARN, msg = "Opened an archive with no checksums", table = %self.table_name, archive = %archive_id);
        }
        self.formats.borrow_mut().insert(*archive_id, format);
        // clone this file handle and place it in our archive map
        self.add_archive(*archive_id, file.dup()?);
        Ok(file)
    }

    /// The format of an archive whose handle is open
    ///
    /// Every handle comes from [`ArchiveMap::get_archive`] or [`ArchiveMap::get_active_writer`],
    /// which both record the format before handing one out, so an archive with no recorded
    /// format is one that was never opened here. It is read as unverified rather than trusted,
    /// so the miss shows in the counters instead of being silent.
    ///
    /// # Arguments
    ///
    /// * `archive_id` - The archive
    #[must_use]
    pub fn format_of(&self, archive_id: &Uuid) -> ArchiveFormat {
        // an archive nobody opened through this map has no format we can vouch for
        self.formats
            .borrow()
            .get(archive_id)
            .copied()
            .unwrap_or(ArchiveFormat::Unverified)
    }

    /// Read one partition's record out of its archive and verify it
    ///
    /// This is the one place a partition's bytes leave an archive - the loader, the compactor's
    /// merge, an archive compaction, a snapshot cut and a direct read all come through here -
    /// so a format 2 record is verified against its checksum once per read, and a format 1
    /// record is counted as a read nothing could verify. A record that does not hash to its
    /// checksum is [`ShoalError::CorruptArchive`], which names the archive and the partition,
    /// and is counted ([F44](../../../../../../docs/src/features/repair.md)).
    ///
    /// The handle is closed whether or not the read succeeded.
    ///
    /// # Arguments
    ///
    /// * `entry` - Where the partition's record is
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn read_record(&self, entry: &ArchiveEntry) -> Result<ReadResult, ServerError> {
        // get a handle to the archive holding this record
        let archive = self.get_archive(&entry.archive).await?;
        // read the record, then close the handle whether or not that worked
        let read = self.read_record_from(&archive, entry).await;
        archive.close().await?;
        read
    }

    /// The read and the check, apart from the handle's lifetime
    ///
    /// Public for a canonical cut, which reads through handles it collected on the loop rather
    /// than through the cache, so an archive unlinked after the cut still reads
    /// ([F44](../../../../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `archive` - The open archive
    /// * `entry` - Where the partition's record is
    pub async fn read_record_from(
        &self,
        archive: &DmaFile,
        entry: &ArchiveEntry,
    ) -> Result<ReadResult, ServerError> {
        // a format 1 record has nothing to verify against, so read it as it is and count it
        if self.format_of(&entry.archive) == ArchiveFormat::Unverified {
            self.integrity
                .unverified_reads
                .set(self.integrity.unverified_reads.get() + 1);
            return Ok(archive.read_at(entry.offset, entry.size).await?);
        }
        // read the checksum ahead of the payload and the payload in one read
        let read = archive.read_at(entry.offset - 8, entry.size + 8).await?;
        // a short read is a torn record, which is corruption of a different shape
        if read.len() < entry.size + 8 {
            self.integrity
                .checksum_failures
                .set(self.integrity.checksum_failures.get() + 1);
            return Err(ServerError::Shoal(ShoalError::CorruptArchive {
                archive: entry.archive,
                partition_id: entry.key,
                expected: 0,
                found: 0,
            }));
        }
        // the payload has to hash to the checksum written beside it
        let expected = u64::from_le_bytes(read[..8].try_into()?);
        let found = record_checksum(&read[8..]);
        if expected != found {
            self.integrity
                .checksum_failures
                .set(self.integrity.checksum_failures.get() + 1);
            return Err(ServerError::Shoal(ShoalError::CorruptArchive {
                archive: entry.archive,
                partition_id: entry.key,
                expected,
                found,
            }));
        }
        // hand back the payload alone, which is what the map entry describes
        match ReadResult::slice(&read, 8, entry.size) {
            Some(payload) => Ok(payload),
            // the slice is inside a read we just measured, so this cannot happen
            None => Err(ServerError::GlommioGeneric(format!(
                "the record of partition {:016x} in archive {} could not be sliced",
                entry.key, entry.archive
            ))),
        }
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
        // take this archive's handle out of the cache first, so the borrow ends before the
        // close is awaited: a read landing on this executor meanwhile borrows the same cache,
        // and a borrow held across the await panicked it
        // ([Resolved #111](../../../../../../docs/src/appendix/resolved/archive-removal-borrow.md))
        let removed = self.loaded_archives.borrow_mut().remove(id);
        if let Some(removed) = removed {
            removed.close().await?;
        }
        // its format goes with its handle
        self.formats.borrow_mut().remove(id);
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

    /// Whether the intent log has grown enough to be folded into a new map
    ///
    /// A fold rewrites the whole map, so the log is folded once it passes a share of the map as
    /// last saved, and never below a floor: the bytes a fold writes per byte of intent stay
    /// bounded by the ratio, and a restart replays at most that share of the map
    /// ([O62](../../../../../../docs/src/appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)).
    ///
    /// # Arguments
    ///
    /// * `intent_bytes` - How many bytes the intent log holds
    #[must_use]
    pub fn compaction_due(&self, intent_bytes: u64) -> bool {
        // a quarter of the saved map, or the old fixed bound for a small one
        let bound = (self.saved_bytes.get() / MAP_FOLD_RATIO).max(MAP_FOLD_FLOOR);
        intent_bytes > bound
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
    use glommio::io::{DmaFile, DmaStreamWriterBuilder, OpenOptions};
    use glommio::LocalExecutor;
    use futures::AsyncWriteExt;
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

    /// The intent log is folded at a quarter of the saved map, and never below the floor (O62)
    ///
    /// A fold rewrites the whole map. At the old fixed bound of a mebibyte, a shard holding a
    /// million partitions rewrote a 125 MB map for every mebibyte of intents: on the lab that
    /// was 70% of everything a node wrote, in bursts that stalled its fsyncs.
    #[test]
    fn the_intent_log_is_folded_against_the_map_it_rewrites() {
        use super::super::conf::FileSystemTableConf;
        use super::{ArchiveMap, MAP_FOLD_FLOOR};
        LocalExecutor::default().run(async {
            // a table's map under a directory of its own
            let temp_dir = test_dir();
            let conf = FileSystemTableConf::builder()
                .latency_sensitive(
                    super::super::conf::FileSystemLatencyWriterConf::builder()
                        .path(temp_dir.path()),
                )
                .throughput_sensitive(
                    super::super::conf::FileSystemThroughputWriterConf::builder()
                        .path(temp_dir.path()),
                );
            conf.setup_paths("T").await.expect("paths");
            let map = ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map");
            // an empty map folds at the floor, as every map did before
            assert!(!map.compaction_due(MAP_FOLD_FLOOR));
            assert!(map.compaction_due(MAP_FOLD_FLOOR + 1));
            // a map of 125 MB folds at a quarter of it, not at the floor
            map.saved_bytes.set(125_000_000);
            assert!(!map.compaction_due(2 * MAP_FOLD_FLOOR));
            assert!(!map.compaction_due(31_250_000));
            assert!(map.compaction_due(31_250_001));
            // a save records the size of what it wrote, and an open reads it back
            map.saved_bytes.set(0);
            let mut writer = map.compact_map().await.expect("a save");
            futures::AsyncWriteExt::close(&mut writer).await.expect("a close");
            let saved = map.saved_bytes.get();
            assert!(saved > 8, "a save did not record its size");
            let reopened = ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map");
            assert_eq!(reopened.saved_bytes.get(), saved);
            map.close_all().await.expect("a close");
        });
    }

    /// A long intent log is read back in blocks, not with three device reads a record (item 140)
    ///
    /// Each record's size, checksum and payload were direct reads of their own, so a map
    /// intent log of half a million records took a shard minutes to replay and failed the
    /// node's readiness timeout on every restart ([Resolved #140](../../../../../../docs/src/appendix/resolved/intent-log-read-ahead.md)).
    #[test]
    fn a_long_intent_log_is_read_in_blocks() {
        use super::super::reader::IntentLogReader;
        LocalExecutor::default().run(async {
            // a hundred thousand framed intents in one log
            let temp_dir = test_dir();
            let path = temp_dir.path().join("intents");
            let mut log = Vec::new();
            for key in 0..100_000u64 {
                log.extend_from_slice(&framed(&MapIntent::Remove(key)));
            }
            std::fs::write(&path, &log).expect("a log");
            // every one comes back, in order
            let mut reader = IntentLogReader::new(&path).await.expect("a reader");
            let mut next = 0u64;
            while let Some(read) = reader.next_buff().await.expect("a record") {
                let archived =
                    rkyv::access::<super::ArchivedMapIntent, rkyv::rancor::Error>(&read[..])
                        .expect("an intent");
                let intent = rkyv::deserialize::<MapIntent, rkyv::rancor::Error>(archived)
                    .expect("an intent");
                assert!(matches!(intent, MapIntent::Remove(key) if key == next), "{intent:?}");
                next += 1;
            }
            assert_eq!(next, 100_000, "the log was not read to its end");
            assert!(!reader.truncated, "a whole log was read as a damaged one");
            // in a handful of device reads, not three hundred thousand
            let blocks = (log.len() as u64).div_ceil(4 * 1024 * 1024) + 1;
            assert!(
                reader.device_reads <= blocks,
                "{} records took {} device reads",
                next,
                reader.device_reads
            );
            reader.close().await.expect("a close");
        });
    }

    /// An archive entry for a key, somewhere in a made up archive
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    fn entry_for(key: u64) -> ArchiveEntry {
        ArchiveEntry {
            key,
            archive: Uuid::nil(),
            offset: key * 128,
            size: 128,
        }
    }

    /// Frame some bytes the way an archive record is framed, which is how an intent is too
    ///
    /// # Arguments
    ///
    /// * `payload` - What the record holds
    fn archive_framed(payload: &[u8]) -> Vec<u8> {
        // the same checksum an intent carries, over the payload alone
        let mut hasher = GxHasher::default();
        hasher.write(payload);
        let mut record = Vec::with_capacity(16 + payload.len());
        record.extend_from_slice(&payload.len().to_le_bytes());
        record.extend_from_slice(&hasher.finish().to_le_bytes());
        record.extend_from_slice(payload);
        record
    }

    /// A whole frame past an intent log's end that is no intent ends the log (item 148)
    ///
    /// A partial flush wrote a whole aligned buffer, and what lay past the log's end in it was
    /// recycled memory: on the lab, archive records of the same table, whose framing and
    /// checksum an intent's share. After a crash the reader took the first one for an intent,
    /// passed its checksum, failed to decode it, and failed the shard, so the node never started
    /// again ([Resolved #148](../../../../../../docs/src/appendix/resolved/stale-intent-log-tail.md)).
    #[test]
    fn a_foreign_frame_past_an_intent_logs_end_ends_it() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let path = temp_dir.path().join("intents");
            // three intents, then an archive record the way the lab's file had one, then zeros
            let mut log = Vec::new();
            for key in 0..3u64 {
                log.extend_from_slice(&framed(&MapIntent::Remove(key)));
            }
            log.extend_from_slice(&archive_framed(b"My Boss, My Hero2001-12-14/ovLJAwkA28f8lwLL5Pq"));
            log.resize(4096, 0);
            std::fs::write(&path, &log).expect("a log");
            // every intent before it is applied, and the shard opens
            let mut map = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
            };
            for key in 0..4u64 {
                map.to_archive.insert(key, entry_for(key));
            }
            map.load_intent_log(&path).await.expect("a log with a stale tail loads");
            assert_eq!(map.to_archive.len(), 1, "the three removes were not applied");
            assert!(map.to_archive.contains_key(&3));
        });
    }

    /// An intent log copied while it is written holds zeros past its end, never stale memory (item 148)
    ///
    /// What a crash leaves: the writer synced a partial buffer and was never closed, so nothing
    /// truncated the file to its end. glommio recycles DMA buffers without zeroing them, and a
    /// partial flush wrote the whole buffer
    /// ([Resolved #148](../../../../../../docs/src/appendix/resolved/stale-intent-log-tail.md)).
    #[test]
    fn an_intent_log_synced_but_not_closed_holds_zeros_past_its_end() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let path = temp_dir.path().join("intents");
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .dma_open(&path)
                .await
                .expect("a file");
            // recycled buffers full of archive records, which a fresh allocation may reuse
            let record = archive_framed(&[b'x'; 360]);
            let pollute = |file: &DmaFile| {
                for _ in 0..16 {
                    let mut stale = file.alloc_dma_buffer(128 * 1024);
                    for chunk in stale.as_bytes_mut().chunks_mut(record.len()) {
                        let len = chunk.len();
                        chunk.copy_from_slice(&record[..len]);
                    }
                    drop(stale);
                }
            };
            pollute(&file);
            let mut writer = DmaStreamWriterBuilder::new(file.dup().expect("a dup"))
                .with_buffer_size(128 * 1024)
                .build();
            // intents, synced the way the compactor syncs its map writer
            let mut written = 0usize;
            for key in 0..100u64 {
                let frame = framed(&MapIntent::Remove(key));
                written += frame.len();
                writer.write_all(&frame).await.expect("a write");
            }
            pollute(&file);
            writer.sync().await.expect("a sync");
            // the file as a crash leaves it
            let on_disk = std::fs::read(&path).expect("the file");
            let stale = on_disk[written..].iter().filter(|byte| **byte != 0).count();
            assert_eq!(stale, 0, "{stale} bytes past the intent log's end are not zero");
            writer.close().await.expect("a close");
            file.close().await.expect("a close");
        });
    }

    /// A save a crash left half done does not stop the map being saved again (item 135)
    ///
    /// A save writes the whole map to a temp file and renames it over the live one. A process
    /// killed between the two leaves the temp file behind, and the next start's first save
    /// used to refuse to create it with `AlreadyExists`, which failed the shard and the node
    /// on every restart after that ([Resolved #135](../../../../../../docs/src/appendix/resolved/leftover-temp-map.md)).
    #[test]
    fn a_leftover_temp_map_does_not_stop_a_save() {
        use super::super::conf::FileSystemTableConf;
        use super::ArchiveMap;
        use futures::AsyncWriteExt as _;
        LocalExecutor::default().run(async {
            // a table's map under a directory of its own
            let temp_dir = test_dir();
            let conf = FileSystemTableConf::builder()
                .latency_sensitive(
                    super::super::conf::FileSystemLatencyWriterConf::builder()
                        .path(temp_dir.path()),
                )
                .throughput_sensitive(
                    super::super::conf::FileSystemThroughputWriterConf::builder()
                        .path(temp_dir.path()),
                );
            conf.setup_paths("T").await.expect("paths");
            let map = ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map");
            // what a save killed before its rename leaves: a partly written temp map
            std::fs::write(&map.temp_map_path, vec![0xAB; 12_345]).expect("a leftover temp map");
            // the next save goes through, and replaces the leftover rather than keeping it
            let mut writer = map
                .compact_map()
                .await
                .expect("a leftover temp map stopped the save");
            writer.close().await.expect("a close");
            assert!(
                !map.temp_map_path.exists(),
                "the save left its temp map behind instead of renaming it"
            );
            // and the map it saved loads back
            let reopened = ArchiveMap::new("Shard-0", "T", &conf).await;
            assert!(reopened.is_ok(), "the saved map did not load: {:?}", reopened.err());
            map.close_all().await.expect("a close");
        });
    }

    /// Removing an archive does not hold the handle map across the close, so a read that lands
    /// while the handle is closing is served rather than panicking the shard
    ///
    /// Two archives are open. One task removes the first, whose close is an io_uring operation
    /// that suspends it; a second task reads the other through the handle cache meanwhile.
    /// Before [item 111](../../../../../../docs/src/appendix/resolved/archive-removal-borrow.md)
    /// the removal's `borrow_mut` lived across the await and the read's `borrow` panicked with
    /// "already mutably borrowed", taking the executor with it.
    #[test]
    fn removing_an_archive_does_not_hold_the_handle_map_across_the_close() {
        use super::super::conf::FileSystemTableConf;
        use super::ArchiveMap;
        use futures::AsyncWriteExt as _;
        use std::rc::Rc;
        LocalExecutor::default().run(async {
            // a table's map under a directory of its own
            let temp_dir = test_dir();
            let conf = FileSystemTableConf::builder()
                .latency_sensitive(
                    super::super::conf::FileSystemLatencyWriterConf::builder()
                        .path(temp_dir.path()),
                )
                .throughput_sensitive(
                    super::super::conf::FileSystemThroughputWriterConf::builder()
                        .path(temp_dir.path()),
                );
            conf.setup_paths("T").await.expect("paths");
            let map = Rc::new(ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map"));
            // two archives, both open in the handle cache
            let first = *map.active.borrow();
            let mut writer = map.get_active_writer().await.expect("a writer");
            writer.write_all(b"first").await.expect("a write");
            writer.close().await.expect("a close");
            let second = Uuid::new_v4();
            *map.active.borrow_mut() = second;
            let mut writer = map.get_active_writer().await.expect("a writer");
            writer.write_all(b"second").await.expect("a write");
            writer.close().await.expect("a close");
            assert!(map.loaded_archives.borrow().contains_key(&first));
            assert!(map.loaded_archives.borrow().contains_key(&second));
            // the removal suspends at the close; the read lands while it is suspended
            let remover = map.clone();
            let removing =
                glommio::spawn_local(async move { remover.remove_archive(&first).await });
            let reader = map.clone();
            let reading = glommio::spawn_local(async move { reader.get_archive(&second).await });
            removing.await.expect("the removal failed");
            let handle = reading
                .await
                .expect("the read failed while an archive was being removed");
            handle.close().await.expect("a close");
            // the removed archive is gone from the cache and the other is still there
            assert!(!map.loaded_archives.borrow().contains_key(&first));
            assert!(map.loaded_archives.borrow().contains_key(&second));
            assert!(!map.all_archives.borrow().contains(&first));
            map.close_all().await.expect("a close");
        });
    }
}
