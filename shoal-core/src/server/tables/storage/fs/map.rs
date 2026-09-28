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
use std::path::{Path, PathBuf};
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
use crate::server::tables::PartitionBytes;
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
    Chain,
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
    /// A partition's whole chain: its base record and the fragments written over it, oldest first
    ///
    /// The whole chain rather than the fragment appended, so replaying an intent twice - a
    /// log folded into a map whose deletion a crash stopped - lands on the same chain instead
    /// of applying a fragment twice, which would put an older fragment's rows back over a
    /// newer one's ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
    Chain(ChainEntry),
}

/// A partition written as a base record and the fragments merged over it since
#[derive(Debug, Archive, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct ChainEntry {
    /// The partition's base record, a whole partition
    pub base: ArchiveEntry,
    /// The fragments written over the base, oldest first
    pub fragments: Vec<ArchiveEntry>,
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
            MapIntent::Chain(_) => kind == MapIntentKinds::Chain,
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
    /// The fragments written over a partition's base record, oldest first, for the few that have any
    fragments: std::collections::HashMap<u64, Vec<ArchiveEntry>>,
}

/// A serialized archive map's fields borrowed from the live map, archived as a `SerializedMap`
///
/// A save used to clone the whole index into a `SerializedMap` before serializing it: a transient
/// copy of every entry, per shard, on every fold, which at ten times the lab's dataset was hundreds
/// of megabytes a shard held twice over
/// ([cluster testing](../../../../../../docs/src/cluster-testing/performance.md#memory-at-ten-times-the-dataset)).
///
/// Its fields are `SerializedMap`'s, in its order and archived as its are, so the bytes are a
/// `SerializedMap`'s and are read back as one; `a_chain_is_counted_gathered_and_saved` and the
/// fold tests read back what this wrote.
#[derive(Archive, Serialize)]
struct SerializedMapRef<'a> {
    /// All archives this shard knows about
    #[rkyv(with = rkyv::with::Inline)]
    all_archives: &'a HashSet<Uuid>,
    /// The map of partitions keys to archive entries
    #[rkyv(with = rkyv::with::Inline)]
    to_archive: &'a std::collections::HashMap<u64, ArchiveEntry>,
    /// The fragments written over a partition's base record, for the few that have any
    #[rkyv(with = rkyv::with::Inline)]
    fragments: &'a std::collections::HashMap<u64, Vec<ArchiveEntry>>,
}

/// Whether an entry's record lies inside its archive, as the archive is on disk
///
/// An archive that is not there is kept: that is another failure, reported loudly where the
/// record is read.
///
/// # Arguments
///
/// * `entry` - The entry
/// * `archive_dir` - The directory the table's archives are in
/// * `lengths` - The archives' lengths read so far, filled as they are needed
fn within_archive(
    entry: &ArchiveEntry,
    archive_dir: &Path,
    lengths: &mut HashMap<Uuid, Option<u64>>,
) -> bool {
    // each archive's length is read once
    let length = *lengths.entry(entry.archive).or_insert_with(|| {
        std::fs::metadata(archive_dir.join(entry.archive.to_string()))
            .ok()
            .map(|meta| meta.len())
    });
    match length {
        Some(length) => entry.offset + entry.size as u64 <= length,
        None => true,
    }
}

impl SerializedMap {
    /// Apply an intent log over this map
    ///
    /// An entry whose record lies past the end of its archive is skipped, so the entry before
    /// it stands. A build before [Resolved #159](../../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md)
    /// could write an intent to the log before the record it names reached its archive, and a
    /// crash between the two left the map naming bytes the disk never got. The job that wrote
    /// such an entry never finished, so the log it was compacting is still there and is
    /// compacted again over the entry before.
    ///
    /// # Arguments
    ///
    /// * `intent_path` - The intent log
    /// * `archive_dir` - The directory the table's archives are in, to check entries against
    #[instrument(name = "SerializableMap::load_intent_log", skip(self), err(Debug))]
    async fn load_intent_log(
        &mut self,
        intent_path: &PathBuf,
        archive_dir: Option<&Path>,
    ) -> Result<(), ServerError> {
        // the archives' lengths, read as entries name them, and the entries skipped
        let mut lengths: HashMap<Uuid, Option<u64>> = HashMap::new();
        let mut skipped = 0u64;
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
                // add this entry to our map, if its record is on disk
                MapIntent::Entry(entry) => {
                    if let Some(dir) = archive_dir {
                        if !within_archive(&entry, dir, &mut lengths) {
                            skipped += 1;
                            continue;
                        }
                    }
                    self.to_archive.insert(entry.key, entry);
                    // a whole record replaces whatever chain the partition had
                    self.fragments.remove(&entry.key);
                }
                // this partition was pruned so it no longer has an archive entry
                MapIntent::Remove(key) => {
                    self.to_archive.remove(&key);
                    self.fragments.remove(&key);
                }
                // a chain stands only if every record of it reached its archive
                MapIntent::Chain(chain) => {
                    if let Some(dir) = archive_dir {
                        let whole = within_archive(&chain.base, dir, &mut lengths)
                            && chain
                                .fragments
                                .iter()
                                .all(|fragment| within_archive(fragment, dir, &mut lengths));
                        if !whole {
                            skipped += 1;
                            continue;
                        }
                    }
                    self.to_archive.insert(chain.base.key, chain.base);
                    if chain.fragments.is_empty() {
                        self.fragments.remove(&chain.base.key);
                    } else {
                        self.fragments.insert(chain.base.key, chain.fragments);
                    }
                }
            }
        }
        // close our reader
        reader.close().await?;
        // say so if a crash had left the log naming records that never reached disk
        if skipped > 0 {
            event!(
                Level::WARN,
                msg = "skipped map entries whose records lie past the end of their archive",
                log = %intent_path.display(),
                skipped,
            );
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
    /// * `intent_path` - The path to the map's intent log
    /// * `archive_dir` - The directory the table's archives are in, to check logged entries against
    /// * `from` - Who is loading it, for the trace
    #[instrument(name = "SerializableMap::new", err(Debug))]
    pub async fn new(
        map_path: &PathBuf,
        intent_path: &PathBuf,
        archive_dir: Option<&Path>,
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
            map.load_intent_log(intent_path, archive_dir).await?;
            Ok(map)
        } else {
            // close our file
            file.close().await?;
            // build a default serializable map
            let map = SerializedMap {
                all_archives: HashSet::with_capacity(1000),
                to_archive: std::collections::HashMap::with_capacity(1000),
                fragments: std::collections::HashMap::new(),
            };
            Ok(map)
        }
    }

    #[instrument(name = "SerializableMap::save", skip_all, err(Debug))]
    pub async fn save(map: &ArchiveMap) -> Result<(), ServerError> {
        // serialize the map straight from its live index, the borrows ending before any await
        let archived = {
            let (all_archives, to_archive, fragments) = (
                map.all_archives.borrow(),
                map.to_archive.borrow(),
                map.fragments.borrow(),
            );
            let borrowed = SerializedMapRef {
                all_archives: &all_archives,
                to_archive: &to_archive,
                fragments: &fragments,
            };
            // with an arena of its own, dropped when the save ends: `rkyv::to_bytes` keeps a thread's
            // arena at the largest thing it ever serialized, and a map is the largest by far, so
            // every shard kept its map's scratch for good, about 128 MB a shard at ten times the
            // lab's dataset ([cluster testing](../../../../../../docs/src/cluster-testing/performance.md#memory-at-ten-times-the-dataset))
            let mut arena = rkyv::ser::allocator::Arena::new();
            rkyv::api::high::to_bytes_in_with_alloc::<_, _, Error>(
                &borrowed,
                rkyv::util::AlignedVec::<16>::new(),
                arena.acquire(),
            )?
        };
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
///
/// Only the bytes each archive holds: the entries of an archive are gathered when it is
/// compacted, never for every archive at once
/// ([O68](../../../../../../docs/src/appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index)).
pub struct SortedUsageMap {
    /// This shards archive ids sorted by total used bytes
    pub sorted: BTreeMap<usize, Vec<Uuid>>,
}

impl SortedUsageMap {
    /// Create a new sorted usage map
    pub fn new() -> Self {
        SortedUsageMap {
            sorted: BTreeMap::default(),
        }
    }
}

impl Default for SortedUsageMap {
    /// An empty map
    fn default() -> Self {
        Self::new()
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
    /// How many of them are chains, a base with fragments over it
    /// ([F61](../../../../../../docs/src/features/fragmented-partitions.md))
    pub chained: Vec<u64>,
}

impl TabletUsage {
    /// Nothing held on any tablet
    #[must_use]
    pub fn empty() -> Self {
        TabletUsage {
            bytes: vec![0u64; crate::server::ring::TABLET_COUNT],
            partitions: vec![0u64; crate::server::ring::TABLET_COUNT],
            chained: vec![0u64; crate::server::ring::TABLET_COUNT],
        }
    }

    /// Count a partition's entry onto its tablet
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `entry` - Its entry
    fn add(&mut self, key: u64, entry: &ArchiveEntry) {
        let tablet = crate::server::ring::Ring::tablet_of(key);
        self.bytes[tablet] += u64::try_from(entry.size).unwrap_or(u64::MAX);
        self.partitions[tablet] += 1;
    }

    /// Take a partition's entry off its tablet
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `entry` - The entry it was counted with
    fn remove(&mut self, key: u64, entry: &ArchiveEntry) {
        let tablet = crate::server::ring::Ring::tablet_of(key);
        self.bytes[tablet] =
            self.bytes[tablet].saturating_sub(u64::try_from(entry.size).unwrap_or(u64::MAX));
        self.partitions[tablet] = self.partitions[tablet].saturating_sub(1);
    }

    /// Count a partition's fragments onto its tablet, as bytes and not as partitions
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `fragments` - Its fragments
    fn add_fragments(&mut self, key: u64, fragments: &[ArchiveEntry]) {
        let tablet = crate::server::ring::Ring::tablet_of(key);
        for fragment in fragments {
            self.bytes[tablet] += u64::try_from(fragment.size).unwrap_or(u64::MAX);
        }
        // a chain is counted once, however long
        if !fragments.is_empty() {
            self.chained[tablet] += 1;
        }
    }

    /// Take a partition's fragments off its tablet
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `fragments` - The fragments it was counted with
    fn remove_fragments(&mut self, key: u64, fragments: &[ArchiveEntry]) {
        let tablet = crate::server::ring::Ring::tablet_of(key);
        for fragment in fragments {
            self.bytes[tablet] = self.bytes[tablet]
                .saturating_sub(u64::try_from(fragment.size).unwrap_or(u64::MAX));
        }
        if !fragments.is_empty() {
            self.chained[tablet] = self.chained[tablet].saturating_sub(1);
        }
    }
}

/// Fold a partition's base record and its fragments into one partition's archived bytes
///
/// The map holds no partition type, so the table's storage hands it this when the map is
/// opened for a table whose partitions can be written as fragments
/// ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
pub type FoldFn = fn(&[u8], &[&[u8]]) -> Result<rkyv::util::AlignedVec, ServerError>;

/// A map of archives for the file system storage engine
#[derive(Debug)]
pub struct ArchiveMap {
    /// The name of the table we are an archive map for
    table_name: String,
    /// The currently active archive id
    pub active: RefCell<Uuid>,
    /// A shard local map of what archives contain what data
    ///
    /// Changed only through `set_partition` and `remove_partition`, which keep `usage` in step.
    pub to_archive: RefCell<HashMap<u64, ArchiveEntry>>,
    /// The fragments merged over a partition's record in `to_archive` since it was last written
    /// whole, oldest first, for the partitions that have any
    ///
    /// Changed only through `set_partition`, `set_chain` and `remove_partition`: a whole record
    /// always ends a partition's chain ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
    fragments: RefCell<HashMap<u64, Vec<ArchiveEntry>>>,
    /// How this table's partitions are folded from their chains, if they can be written as fragments
    folder: Cell<Option<FoldFn>>,
    /// The bytes and partitions `to_archive` holds per tablet, kept as it changes
    ///
    /// A shard's replication report asks for it on every tick, and a pass over a map of millions
    /// of partitions a few times a second was a shard core's work for nothing
    /// ([O57](../../../../../../docs/src/appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)).
    usage: RefCell<TabletUsage>,
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
        let archive_dir = conf.get_archive_path(table_name);
        let serializable =
            SerializedMap::new(&map_path, &intent_path, Some(&archive_dir), "new").await?;
        // how large the map on disk is, which the intent log is folded against
        let saved_bytes = std::fs::metadata(&map_path).map_or(0, |meta| meta.len());
        // start out with an empty hash map with room for 1k partitions
        let to_archive = RefCell::new(HashMap::with_capacity(1000));
        // and what they hold per tablet, counted as they are loaded
        let mut usage = TabletUsage::empty();
        // load all of this shards keys into this map
        for (key, entry) in serializable.to_archive {
            // add this entry from our map
            usage.add(key, &entry);
            to_archive.borrow_mut().insert(key, entry);
        }
        // and the fragments of the partitions that have a chain, counted as their bytes
        for (key, fragments) in &serializable.fragments {
            usage.add_fragments(*key, fragments);
        }
        // just use empty maps for now
        let map = ArchiveMap {
            table_name: table_name.to_owned(),
            active: RefCell::new(Uuid::new_v4()),
            to_archive,
            fragments: RefCell::new(serializable.fragments),
            folder: Cell::new(None),
            usage: RefCell::new(usage),
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
        // add this archive to our active archive set, only once it exists: one named here
        // that the open failed to create would be opened by every later archive compaction
        self.all_archives.borrow_mut().insert(*self.active.borrow());
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
        // insert or update this partitions entry, and move its tablet's figures by the change
        let mut usage = self.usage.borrow_mut();
        if let Some(old) = self.to_archive.borrow_mut().insert(id, entry) {
            usage.remove(id, &old);
        }
        usage.add(id, &entry);
        // a whole record ends whatever chain this partition had
        if let Some(old) = self.fragments.borrow_mut().remove(&id) {
            usage.remove_fragments(id, &old);
        }
    }

    /// Set a partition's whole chain: its base record and the fragments written over it
    ///
    /// # Arguments
    ///
    /// * `chain` - The chain, whose base names the partition
    pub fn set_chain(&self, chain: ChainEntry) {
        // the base is set as a whole record is, ending the old chain
        let id = chain.base.key;
        self.set_partition(id, chain.base);
        // then the fragments over it, if there are any
        if !chain.fragments.is_empty() {
            self.usage
                .borrow_mut()
                .add_fragments(id, &chain.fragments);
            self.fragments.borrow_mut().insert(id, chain.fragments);
        }
    }

    /// A partition's chain, or its one record with no fragments
    ///
    /// # Arguments
    ///
    /// * `id` - The partition's key
    #[must_use]
    pub fn chain_of(&self, id: u64) -> Option<ChainEntry> {
        // no base, no partition
        let base = *self.to_archive.borrow().get(&id)?;
        // and whatever was written over it
        let fragments = self
            .fragments
            .borrow()
            .get(&id)
            .cloned()
            .unwrap_or_default();
        Some(ChainEntry { base, fragments })
    }

    /// Whether a partition has fragments over its base record
    ///
    /// # Arguments
    ///
    /// * `id` - The partition's key
    #[must_use]
    pub fn is_chained(&self, id: u64) -> bool {
        self.fragments.borrow().contains_key(&id)
    }

    /// The bytes this map's index holds, estimated from its capacity: the entries and the chains
    ///
    /// Held for every partition the shard has ever archived, whether or not it is resident
    /// ([cluster testing](../../../../../../docs/src/cluster-testing/performance.md#memory-at-ten-times-the-dataset)).
    #[must_use]
    pub fn index_bytes(&self) -> usize {
        // a bucket is its key, its entry and a control byte, at hashbrown's load factor of 7/8
        let bucket = std::mem::size_of::<(u64, ArchiveEntry)>() + 1;
        let entries = self.to_archive.borrow().capacity().saturating_mul(bucket) / 7 * 8;
        // and each chain's vector of fragments
        let fragments = self.fragments.borrow();
        let chain_bucket = std::mem::size_of::<(u64, Vec<ArchiveEntry>)>() + 1;
        let chains = fragments.capacity().saturating_mul(chain_bucket) / 7 * 8
            + fragments
                .values()
                .map(|chain| chain.capacity() * std::mem::size_of::<ArchiveEntry>())
                .sum::<usize>();
        entries + chains
    }

    /// How many partitions have fragments over their base record
    #[must_use]
    pub fn chained_count(&self) -> usize {
        self.fragments.borrow().len()
    }

    /// The partitions with a chain any record of which lies in one archive
    ///
    /// # Arguments
    ///
    /// * `archive` - The archive
    #[must_use]
    pub fn chained_in(&self, archive: &Uuid) -> Vec<u64> {
        let to_archive = self.to_archive.borrow();
        self.fragments
            .borrow()
            .iter()
            .filter(|(key, fragments)| {
                // the base, or any fragment over it
                to_archive
                    .get(key)
                    .is_some_and(|base| base.archive == *archive)
                    || fragments.iter().any(|fragment| fragment.archive == *archive)
            })
            .map(|(key, _)| *key)
            .collect()
    }

    /// Give this map the fold for its table's partitions
    ///
    /// # Arguments
    ///
    /// * `folder` - How a chain is folded into one partition
    pub fn set_folder(&self, folder: FoldFn) {
        self.folder.set(Some(folder));
    }

    /// Read a whole partition: its one record, or its chain folded into one partition
    ///
    /// Every record of a chain is verified against its checksum as it is read, as
    /// `read_record` does for one.
    ///
    /// # Arguments
    ///
    /// * `chain` - The partition's chain
    pub async fn read_chain(&self, chain: &ChainEntry) -> Result<PartitionBytes, ServerError> {
        // the base, as every partition has
        let base = self.read_record(&chain.base).await?;
        // a partition of one record is that record
        if chain.fragments.is_empty() {
            return Ok(PartitionBytes::Record(base));
        }
        // otherwise every fragment, oldest first
        let mut fragments = Vec::with_capacity(chain.fragments.len());
        for fragment in &chain.fragments {
            fragments.push(self.read_record(fragment).await?);
        }
        self.fold_records(chain.base.key, &base, &fragments)
    }

    /// Fold a base record and its fragments, read already, into one partition
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key, for the error
    /// * `base` - The base record's payload
    /// * `fragments` - Each fragment's payload, oldest first
    pub fn fold_records(
        &self,
        key: u64,
        base: &[u8],
        fragments: &[ReadResult],
    ) -> Result<PartitionBytes, ServerError> {
        // a chain this map cannot fold is one written by a table it was not opened for
        let Some(fold) = self.folder.get() else {
            return Err(ServerError::GlommioGeneric(format!(
                "partition {key} of {} is written as fragments and this map has no fold for it",
                self.table_name
            )));
        };
        let fragments: Vec<&[u8]> = fragments.iter().map(|read| &read[..]).collect();
        Ok(PartitionBytes::Folded(fold(base, &fragments)?))
    }

    /// Read a whole partition by its key, which has to be in the map
    ///
    /// # Arguments
    ///
    /// * `id` - The partition's key
    pub async fn read_partition(&self, id: u64) -> Result<PartitionBytes, ServerError> {
        // a partition the map does not name has been pruned
        let Some(chain) = self.chain_of(id) else {
            return Err(ServerError::Shoal(ShoalError::PartitionNotFound {
                partition_id: id,
            }));
        };
        self.read_chain(&chain).await
    }

    /// The bytes the archives hold per tablet, indexed by tablet
    ///
    /// Kept by every change to the map, so the figure is what the map names now: a partition
    /// replaced, removed or reloaded is counted as the map has it
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md)).
    #[must_use]
    pub fn tablet_bytes(&self) -> Vec<u64> {
        self.tablet_usage().bytes
    }

    /// The bytes and partitions the archives hold per tablet, indexed by tablet
    ///
    /// The counters `set_partition` and `remove_partition` keep, copied: no pass over the map
    /// ([O57](../../../../../../docs/src/appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)).
    #[must_use]
    pub fn tablet_usage(&self) -> TabletUsage {
        self.usage.borrow().clone()
    }

    /// The bytes and partitions the archives hold per tablet, counted by one pass over the map
    ///
    /// What `tablet_usage` has to equal, for a test that the counters never drift.
    #[must_use]
    pub fn tablet_usage_by_pass(&self) -> TabletUsage {
        let mut usage = TabletUsage::empty();
        // every partition the map names lands on the tablet its key hashes into
        for (key, entry) in self.to_archive.borrow().iter() {
            usage.add(*key, entry);
        }
        // and the bytes of every chain's fragments
        for (key, fragments) in self.fragments.borrow().iter() {
            usage.add_fragments(*key, fragments);
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
        // drop this partitions entry, and its figures from its tablet's
        if let Some(old) = self.to_archive.borrow_mut().remove(&id) {
            self.usage.borrow_mut().remove(id, &old);
        }
        // and its chain with it
        if let Some(old) = self.fragments.borrow_mut().remove(&id) {
            self.usage.borrow_mut().remove_fragments(id, &old);
        }
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
        self.verify_record(entry, &read, 0)
    }

    /// Read a run of records that sit near each other in one archive with one read
    ///
    /// The run is read from the first record's start to the last one's end, the bytes of other
    /// partitions between them included, and each record is verified against its checksum and
    /// sliced out of the one buffer. A snapshot cut reads a group's records this way: one
    /// direct read per record, at the lab's 700 bytes a record, took a Zen1 host 5 to 8 s a
    /// set on a device busy flushing its WAL
    /// ([O78](../../../../../../docs/src/appendix/optimizations.md#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order)).
    ///
    /// # Arguments
    ///
    /// * `archive` - The open archive
    /// * `run` - The records, in offset order, all in `archive`
    pub async fn read_run_from(
        &self,
        archive: &DmaFile,
        run: &[ArchiveEntry],
    ) -> Result<Vec<(u64, ReadResult)>, ServerError> {
        // nothing to read for no records
        let (Some(first), Some(last)) = (run.first(), run.last()) else {
            return Ok(Vec::new());
        };
        // a format 1 archive has no checksums ahead of its records, so its run starts at the
        // first record itself
        let verified = self.format_of(&first.archive) != ArchiveFormat::Unverified;
        let lead = if verified { 8 } else { 0 };
        let start = first.offset - lead;
        let end = last.offset + last.size as u64;
        // truncation cannot happen: a run is bounded far below usize by its caller
        #[allow(clippy::cast_possible_truncation)]
        let read = archive.read_at(start, (end - start) as usize).await?;
        let mut records = Vec::with_capacity(run.len());
        for entry in run {
            // where this record's checksum (or payload) sits inside the run's buffer
            #[allow(clippy::cast_possible_truncation)]
            let at = (entry.offset - lead - start) as usize;
            let payload = if verified {
                self.verify_record(entry, &read, at)?
            } else {
                // counted the way a single unverified read is
                self.integrity
                    .unverified_reads
                    .set(self.integrity.unverified_reads.get() + 1);
                self.short_record(entry, &read, at, entry.size)?
            };
            records.push((entry.key, payload));
        }
        Ok(records)
    }

    /// Verify one record's checksum inside a buffer and slice its payload out
    ///
    /// # Arguments
    ///
    /// * `entry` - Where the partition's record is
    /// * `read` - A buffer holding the record's checksum and payload
    /// * `at` - Where in `read` the record's checksum starts
    fn verify_record(
        &self,
        entry: &ArchiveEntry,
        read: &ReadResult,
        at: usize,
    ) -> Result<ReadResult, ServerError> {
        // the checksum and the payload together, or a torn record
        let record = self.short_record(entry, read, at, entry.size + 8)?;
        // the payload has to hash to the checksum written beside it
        let expected = u64::from_le_bytes(record[..8].try_into()?);
        let found = record_checksum(&record[8..]);
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
        self.short_record(entry, &record, 8, entry.size)
    }

    /// Slice a record's bytes out of a buffer, or name it torn when the buffer is short
    ///
    /// # Arguments
    ///
    /// * `entry` - Where the partition's record is
    /// * `read` - The buffer
    /// * `at` - Where in `read` the bytes start
    /// * `len` - How many bytes
    fn short_record(
        &self,
        entry: &ArchiveEntry,
        read: &ReadResult,
        at: usize,
        len: usize,
    ) -> Result<ReadResult, ServerError> {
        // a short read is a torn record, which is corruption of a different shape
        match ReadResult::slice(read, at, len) {
            Some(slice) if slice.len() == len => Ok(slice),
            _ => {
                self.integrity
                    .checksum_failures
                    .set(self.integrity.checksum_failures.get() + 1);
                Err(ServerError::Shoal(ShoalError::CorruptArchive {
                    archive: entry.archive,
                    partition_id: entry.key,
                    expected: 0,
                    found: 0,
                }))
            }
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

    /// The entries of the partitions whose latest data lies in one archive
    ///
    /// A pass over the index, gathering only that archive's: what a compaction of it needs
    /// ([O68](../../../../../../docs/src/appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index)).
    ///
    /// # Arguments
    ///
    /// * `archive` - The archive
    #[must_use]
    pub fn entries_of(&self, archive: &Uuid) -> Vec<ArchiveEntry> {
        // every partition of one record whose entry names this archive: a chained partition's
        // base is never moved alone, since a whole record in its place would end its chain
        let fragments = self.fragments.borrow();
        self.to_archive
            .borrow()
            .values()
            .filter(|entry| entry.archive == *archive && !fragments.contains_key(&entry.key))
            .copied()
            .collect()
    }

    /// Sort our archives by how much data they have
    ///
    /// They will be sorted from least used to most used.
    #[instrument(name = "ArchiveMap::sort_by_load", skip_all)]
    pub fn sort_by_load(&self) -> SortedUsageMap {
        // count how many bytes each archive holds, starting every known archive at none
        let mut used_by: std::collections::HashMap<Uuid, usize> =
            std::collections::HashMap::with_capacity(self.all_archives.borrow().len());
        for archive in self.all_archives.borrow().iter() {
            used_by.insert(*archive, 0);
        }
        // one pass over the index, counting and copying nothing else: copying every entry here
        // was a copy of the whole index on every compaction
        // ([O68](../../../../../../docs/src/appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index))
        for (_, archive_entry) in self.to_archive.borrow().iter() {
            // a record's footprint in its archive is its prefix and its payload: counted as the
            // payload alone, a fully live archive of short records read as under half live and
            // every pass copied it ([Resolved #179](../../../../../../docs/src/appendix/resolved/archive-usage-prefix.md))
            *used_by.entry(archive_entry.archive).or_default() +=
                archive_entry.size + RECORD_PREFIX_LEN as usize;
        }
        // and every fragment, where it lies
        for fragments in self.fragments.borrow().values() {
            for fragment in fragments {
                *used_by.entry(fragment.archive).or_default() +=
                    fragment.size + RECORD_PREFIX_LEN as usize;
            }
        }
        // the archives by how much they hold, least first
        let mut sorted = SortedUsageMap::new();
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
    use futures::AsyncWriteExt;
    use glommio::io::{DmaFile, DmaStreamWriterBuilder, OpenOptions};
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

    /// A logged entry past the end of its archive is skipped, and the entry before it stands
    ///
    /// The shape a crash left under a build before [Resolved #159](../../../../../../docs/src/appendix/resolved/map-ahead-of-archive.md):
    /// the map's intent log naming a record its archive never got.
    #[test]
    fn a_logged_entry_past_its_archive_keeps_the_one_before() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let map_path = temp_dir.path().join("test-map");
            let intent_path = temp_dir.path().join("test-map-intent");
            let archive_dir = temp_dir.path().join("archives");
            std::fs::create_dir_all(&archive_dir).unwrap();
            // two archives on disk: an old one that is whole, and the active one, cut short
            let (old, active, gone) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
            std::fs::write(archive_dir.join(old.to_string()), vec![1u8; 4096]).unwrap();
            std::fs::write(archive_dir.join(active.to_string()), vec![1u8; 1024]).unwrap();
            // the snapshot knows partition 1 in the old archive
            let before = ArchiveEntry {
                key: 1,
                archive: old,
                offset: 512,
                size: 256,
            };
            let mut snapshot_map = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
                fragments: std::collections::HashMap::default(),
            };
            snapshot_map.to_archive.insert(before.key, before);
            std::fs::write(&map_path, snapshot(&snapshot_map)).unwrap();
            // the log repoints 1 past the active archive's end, logs 2 there too, logs 3
            // inside it, and logs 4 in an archive that is not there at all
            let mut intents = framed(&MapIntent::entry(1, active, 2048, 256));
            intents.extend_from_slice(&framed(&MapIntent::entry(2, active, 900, 256)));
            intents.extend_from_slice(&framed(&MapIntent::entry(3, active, 512, 512)));
            intents.extend_from_slice(&framed(&MapIntent::entry(4, gone, 0, 64)));
            std::fs::write(&intent_path, &intents).unwrap();
            let loaded = SerializedMap::new(
                &map_path.to_path_buf(),
                &intent_path.to_path_buf(),
                Some(&archive_dir),
                "test",
            )
            .await
            .expect("the map loads");
            // the entry before the torn one stands
            assert_eq!(loaded.to_archive.get(&1), Some(&before));
            // a torn entry with nothing before it names nothing
            assert_eq!(loaded.to_archive.get(&2), None);
            // one inside its archive, up to its last byte, is kept
            assert_eq!(
                loaded.to_archive.get(&3).map(|entry| entry.archive),
                Some(active)
            );
            // and one in a missing archive is kept, for the read to report by name
            assert_eq!(
                loaded.to_archive.get(&4).map(|entry| entry.archive),
                Some(gone)
            );
        });
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
                fragments: std::collections::HashMap::default(),
            };
            snapshot_map.to_archive.insert(stale.key, stale);
            // write our snapshot to disk
            std::fs::write(&map_path, snapshot(&snapshot_map)).unwrap();
            // log an entry for a second partition and the removal of the first
            let mut intents = framed(&MapIntent::entry(7, stale.archive, 256, 64));
            intents.extend_from_slice(&framed(&MapIntent::Remove(stale.key)));
            std::fs::write(&intent_path, &intents).unwrap();
            // load this map back with its intent log applied
            let loaded = SerializedMap::new(
                &map_path.to_path_buf(),
                &intent_path.to_path_buf(),
                None,
                "test",
            )
            .await
            .expect("Failed to load map");
            // our pruned partition should be gone
            assert!(!loaded.to_archive.contains_key(&stale.key));
            // and our logged entry should still be there
            assert!(loaded.to_archive.contains_key(&7));
        });
    }

    /// A chain's intent replayed twice lands on the same chain, and a whole record ends it
    ///
    /// A map intent names the whole chain, not the fragment appended, so a log replayed over a
    /// map that already holds it - a fold whose log deletion a crash stopped - puts no fragment
    /// in twice ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
    #[test]
    fn a_chain_replayed_twice_is_the_same_chain() {
        use super::ChainEntry;
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let map_path = temp_dir.path().join("test-map");
            let intent_path = temp_dir.path().join("test-map-intent");
            let archive_dir = temp_dir.path().join("archives");
            std::fs::create_dir_all(&archive_dir).unwrap();
            // a map is saved before its first intent is logged, so one is on disk, empty
            let empty = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
                fragments: std::collections::HashMap::default(),
            };
            std::fs::write(&map_path, snapshot(&empty)).unwrap();
            let archive = Uuid::new_v4();
            std::fs::write(archive_dir.join(archive.to_string()), vec![1u8; 8192]).unwrap();
            let at = |key: u64, offset: u64| ArchiveEntry {
                key,
                archive,
                offset,
                size: 100,
            };
            // partition 1 gains two fragments; partition 2 gains one and is then written whole;
            // partition 3 gains one and is then pruned
            let mut log = Vec::new();
            for intent in [
                MapIntent::entry(1, archive, 16, 100),
                MapIntent::Chain(ChainEntry { base: at(1, 16), fragments: vec![at(1, 200)] }),
                MapIntent::Chain(ChainEntry { base: at(1, 16), fragments: vec![at(1, 200), at(1, 400)] }),
                MapIntent::Chain(ChainEntry { base: at(2, 600), fragments: vec![at(2, 800)] }),
                MapIntent::entry(2, archive, 1000, 100),
                MapIntent::Chain(ChainEntry { base: at(3, 1200), fragments: vec![at(3, 1400)] }),
                MapIntent::Remove(3),
            ] {
                log.extend_from_slice(&framed(&intent));
            }
            // the whole log twice, as a replay over a map that already folded it would read
            let twice = [log.clone(), log].concat();
            std::fs::write(&intent_path, &twice).unwrap();
            let loaded = SerializedMap::new(&map_path, &intent_path, Some(&archive_dir), "test")
                .await
                .expect("the map loads");
            assert_eq!(loaded.to_archive.get(&1), Some(&at(1, 16)));
            assert_eq!(loaded.fragments.get(&1), Some(&vec![at(1, 200), at(1, 400)]));
            // a whole record ended partition 2's chain
            assert_eq!(loaded.to_archive.get(&2), Some(&at(2, 1000)));
            assert_eq!(loaded.fragments.get(&2), None);
            // and a removal took partition 3 and its chain
            assert_eq!(loaded.to_archive.get(&3), None);
            assert_eq!(loaded.fragments.get(&3), None);
        });
    }

    /// A chain whose newest fragment never reached its archive is skipped, and the chain before stands
    #[test]
    fn a_chain_past_its_archive_keeps_the_chain_before() {
        use super::ChainEntry;
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let map_path = temp_dir.path().join("test-map");
            let intent_path = temp_dir.path().join("test-map-intent");
            let archive_dir = temp_dir.path().join("archives");
            std::fs::create_dir_all(&archive_dir).unwrap();
            // a map is saved before its first intent is logged, so one is on disk, empty
            let empty = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
                fragments: std::collections::HashMap::default(),
            };
            std::fs::write(&map_path, snapshot(&empty)).unwrap();
            let archive = Uuid::new_v4();
            std::fs::write(archive_dir.join(archive.to_string()), vec![1u8; 1024]).unwrap();
            let at = |offset: u64| ArchiveEntry {
                key: 7,
                archive,
                offset,
                size: 100,
            };
            let mut log = framed(&MapIntent::Chain(ChainEntry {
                base: at(16),
                fragments: vec![at(200)],
            }));
            // the next fragment lies past the archive's end: a crash between its write and its sync
            log.extend_from_slice(&framed(&MapIntent::Chain(ChainEntry {
                base: at(16),
                fragments: vec![at(200), at(2048)],
            })));
            std::fs::write(&intent_path, &log).unwrap();
            let loaded = SerializedMap::new(&map_path, &intent_path, Some(&archive_dir), "test")
                .await
                .expect("the map loads");
            assert_eq!(loaded.fragments.get(&7), Some(&vec![at(200)]));
        });
    }

    /// A chain is counted, gathered and saved as the map's other entries are
    ///
    /// Its fragments are bytes on its tablet and in their archives; an archive's entries leave a
    /// chained base out, since a pass copying it alone would end the chain; the chains touching
    /// an archive are found by any of their records; and a save and an open keep them
    /// ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
    #[test]
    fn a_chain_is_counted_gathered_and_saved() {
        use super::super::conf::FileSystemTableConf;
        use super::{ArchiveMap, ChainEntry, RECORD_PREFIX_LEN};
        LocalExecutor::default().run(async {
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
            let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
            map.all_archives.borrow_mut().extend([a, b]);
            let at = |key: u64, archive: Uuid, offset: u64, size: usize| ArchiveEntry {
                key,
                archive,
                offset,
                size,
            };
            // partition 1 is one record in a; partition 2 has its base in a and a fragment in b
            map.set_partition(1, at(1, a, 16, 1000));
            map.set_chain(ChainEntry {
                base: at(2, a, 1100, 2000),
                fragments: vec![at(2, b, 16, 300)],
            });
            assert!(map.is_chained(2) && !map.is_chained(1));
            assert_eq!(map.chained_count(), 1);
            // the counters are what a pass counts, fragments as bytes and a chain as one partition
            let usage = map.tablet_usage();
            assert_eq!(usage, map.tablet_usage_by_pass());
            assert_eq!(usage.bytes.iter().sum::<u64>(), 3300);
            assert_eq!(usage.partitions.iter().sum::<u64>(), 2);
            assert_eq!(usage.chained.iter().sum::<u64>(), 1);
            // archive a's entries leave the chained base out, and both archives find the chain
            assert_eq!(map.entries_of(&a), vec![at(1, a, 16, 1000)]);
            assert!(map.entries_of(&b).is_empty());
            assert_eq!(map.chained_in(&a), vec![2]);
            assert_eq!(map.chained_in(&b), vec![2]);
            // and b holds the fragment's bytes
            let load = map.sort_by_load();
            let b_used = load
                .sorted
                .iter()
                .find(|(_, ids)| ids.contains(&b))
                .map(|(used, _)| *used);
            assert_eq!(b_used, Some(300 + RECORD_PREFIX_LEN as usize));
            // a save and an open keep the chain
            let mut writer = map.compact_map().await.expect("a save");
            futures::AsyncWriteExt::close(&mut writer).await.expect("a close");
            let reopened = ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map");
            assert_eq!(reopened.chain_of(2), map.chain_of(2));
            assert_eq!(reopened.tablet_usage(), usage);
            // a whole record ends it, and its fragments' bytes go with it
            reopened.set_partition(2, at(2, b, 400, 2100));
            assert!(!reopened.is_chained(2));
            assert_eq!(reopened.tablet_usage(), reopened.tablet_usage_by_pass());
            assert_eq!(reopened.tablet_usage().chained.iter().sum::<u64>(), 0);
            map.close_all().await.expect("a close");
            reopened.close_all().await.expect("a close");
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
            futures::AsyncWriteExt::close(&mut writer)
                .await
                .expect("a close");
            let saved = map.saved_bytes.get();
            assert!(saved > 8, "a save did not record its size");
            let reopened = ArchiveMap::new("Shard-0", "T", &conf).await.expect("a map");
            assert_eq!(reopened.saved_bytes.get(), saved);
            map.close_all().await.expect("a close");
        });
    }

    /// Archives are ordered by what they hold, and one archive's entries are gathered alone (O68)
    ///
    /// Every archive compaction copied the shard's whole partition index into vectors per
    /// archive, five million entries a shard on the lab, to compact the few archives below half
    /// used. It now counts bytes per archive and gathers an archive's entries when it compacts it
    /// ([O68](../../../../../../docs/src/appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index)).
    #[test]
    fn archives_are_ordered_by_load_and_gathered_one_at_a_time() {
        use super::super::conf::FileSystemTableConf;
        use super::ArchiveMap;
        LocalExecutor::default().run(async {
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
            // three archives holding one, two and three partitions of 128 bytes
            let archives = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
            let mut key = 0u64;
            for (at, archive) in archives.iter().enumerate() {
                map.all_archives.borrow_mut().insert(*archive);
                for _ in 0..=at {
                    map.set_partition(
                        key,
                        ArchiveEntry {
                            key,
                            archive: *archive,
                            offset: key * 128,
                            size: 128,
                        },
                    );
                    key += 1;
                }
            }
            // ordered least used first, by the bytes each holds: every record's payload and its
            // sixteen byte prefix (item 179)
            let sorted = map.sort_by_load();
            let order: Vec<(usize, Vec<Uuid>)> = sorted.sorted.into_iter().collect();
            let expected: Vec<(usize, Vec<Uuid>)> = archives
                .iter()
                .enumerate()
                .map(|(at, archive)| ((at + 1) * (128 + 16), vec![*archive]))
                .collect();
            assert_eq!(order, expected);
            // and each archive's entries are its own, all of them
            for (at, archive) in archives.iter().enumerate() {
                let entries = map.entries_of(archive);
                assert_eq!(entries.len(), at + 1);
                assert!(entries.iter().all(|entry| entry.archive == *archive));
            }
            // an archive nothing lives in has none, which is what marks it for deletion
            assert!(map.entries_of(&Uuid::new_v4()).is_empty());
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
                assert!(
                    matches!(intent, MapIntent::Remove(key) if key == next),
                    "{intent:?}"
                );
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
            log.extend_from_slice(&archive_framed(
                b"My Boss, My Hero2001-12-14/ovLJAwkA28f8lwLL5Pq",
            ));
            log.resize(4096, 0);
            std::fs::write(&path, &log).expect("a log");
            // every intent before it is applied, and the shard opens
            let mut map = SerializedMap {
                all_archives: HashSet::default(),
                to_archive: std::collections::HashMap::default(),
                fragments: std::collections::HashMap::default(),
            };
            for key in 0..4u64 {
                map.to_archive.insert(key, entry_for(key));
            }
            map.load_intent_log(&path, None)
                .await
                .expect("a log with a stale tail loads");
            assert_eq!(
                map.to_archive.len(),
                1,
                "the three removes were not applied"
            );
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
            assert_eq!(
                stale, 0,
                "{stale} bytes past the intent log's end are not zero"
            );
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
            assert!(
                reopened.is_ok(),
                "the saved map did not load: {:?}",
                reopened.err()
            );
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
