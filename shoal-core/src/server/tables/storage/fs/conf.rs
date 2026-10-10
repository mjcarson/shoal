//! The config for FileSystem based storage

use byte_unit::Byte;
use glommio::io::Directory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::instrument;

use crate::server::conf::cluster::DurationSpec;
use crate::server::ServerError;
use crate::utils;

/// Set default path for latency files
fn default_path() -> PathBuf {
    PathBuf::from("/opt/shoal")
}

/// Set default buffer_size for latency files to 512 bytes
fn default_latency_buffer_size() -> usize {
    512
}

/// Set the default ceiling the latency staging buffer sizes itself up to, 256 Kibibytes
///
/// The staging buffer grows to hold several records so they share one aligned write, and this
/// is where that growth stops. 256 Kibibytes holds thirty two of the kilobyte rows the
/// benchmarks use as a reference and four of a sixty four kilobyte row, which is the widest
/// point [O34]'s capture measured a gain at.
///
/// [O34]: ../../../../../../docs/src/appendix/optimizations.md
fn default_latency_max_buffer_size() -> usize {
    256 << 10
}

/// Set default write behind for latency files
fn default_latency_write_behind() -> usize {
    128
}

/// Set default intent log size to 10 Mebibytes
fn default_intent_log_size() -> u64 {
    10 << 20
}

/// Set the default durability level for latency files
fn default_durability() -> Durability {
    Durability::Fsync
}

/// How durable a write has to be before its response is released to a client
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Acknowledge a write only once it has been fdatasynced
    ///
    /// O_DIRECT skips the page cache but not the drives own volatile write cache,
    /// so an fdatasync is the only thing that makes a write survive power loss.
    /// At most one fdatasync is in flight at a time, so concurrent writes group
    /// commit behind the one already running and the cost per write falls as load
    /// rises.
    Fsync,
    /// Acknowledge a write once the kernel has accepted it
    ///
    /// Faster, but a write can be acknowledged and then lost to power loss, since
    /// it may still be sitting in the drives write cache. Useful for benchmarking
    /// against [`Durability::Fsync`].
    Async,
}

/// The settings to use for a specific writer
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileSystemLatencyWriterConf {
    /// The path to write too (table name will be added before the final filename)
    #[serde(default = "default_path")]
    pub path: PathBuf,
    /// The smallest buffer size to use when writting data
    ///
    /// A floor rather than the size itself: the writer sizes each staging buffer to hold
    /// several of the widest record the last one held, so that records share an aligned write.
    /// This is where that sizing starts and [`FileSystemLatencyWriterConf::max_buffer_size`] is
    /// where it stops.
    #[serde(default = "default_latency_buffer_size")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub buffer_size: usize,
    /// The largest buffer size the writer will size itself up to
    ///
    /// A writer may hold up to `write_behind + 1` buffers of this size at once, per table, per
    /// shard, so this is the memory bound on the staging half of the write path. Setting it
    /// equal to `buffer_size` turns the sizing off entirely: every buffer is then the floor, or
    /// one record if a record is wider than the floor, which is what the writer did before it
    /// sized itself.
    #[serde(default = "default_latency_max_buffer_size")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub max_buffer_size: usize,
    /// The number of write behind buffers to use
    #[serde(default = "default_latency_write_behind")]
    pub write_behind: usize,
    /// The size of the intent log for this table
    #[serde(default = "default_intent_log_size")]
    #[serde(deserialize_with = "utils::deserialize_byte_size_u64")]
    pub intent_log_size: u64,
    /// How durable a write must be before its response is released to a client
    #[serde(default = "default_durability")]
    pub durability: Durability,
}

impl Default for FileSystemLatencyWriterConf {
    /// Create a default `FileSystemlatencyWriterConf`
    fn default() -> Self {
        FileSystemLatencyWriterConf {
            path: default_path(),
            buffer_size: default_latency_buffer_size(),
            max_buffer_size: default_latency_max_buffer_size(),
            write_behind: default_latency_write_behind(),
            intent_log_size: default_intent_log_size(),
            durability: default_durability(),
        }
    }
}

impl FileSystemLatencyWriterConf {
    /// Create a new FileSystemLatencyWriterConf with default values
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set the path to write to
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Set the smallest buffer size to use when writing data
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the largest buffer size the writer may size itself up to
    ///
    /// # Arguments
    ///
    /// * `max_buffer_size` - The ceiling to set in bytes
    pub fn max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    /// Set the number of write behind buffers
    pub fn write_behind(mut self, write_behind: usize) -> Self {
        self.write_behind = write_behind;
        self
    }

    /// Set the intent log size
    pub fn intent_log_size(mut self, intent_log_size: u64) -> Self {
        self.intent_log_size = intent_log_size;
        self
    }

    /// Set how durable a write must be before its response is released to a client
    ///
    /// # Arguments
    ///
    /// * `durability` - The durability level to set
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }
}

/// Set default buffer_size for throughput files to 128 Kibibytes
fn default_throughput_buffer_size() -> usize {
    128 << 10
}

/// Set default write behind for throughput files
fn default_throughput_write_behind() -> usize {
    4
}

/// Set how many bytes one archive pass copies before it ends and is queued again, 16 Mebibytes
///
/// A pass repoints the archive index only once it ends, so a snapshot cut queued behind one waits
/// for all of it ([O74](../../../../../../docs/src/appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)).
fn default_archive_pass_bytes() -> usize {
    16 << 20
}

/// Set the least time between the starts of two archive passes, a minute
///
/// A pass is queued behind every compaction; run after each one it copies an archive as soon
/// as it falls under half live, where a later pass finds it deader and copies less for the same
/// space ([O74](../../../../../../docs/src/appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)).
fn default_archive_pass_interval() -> DurationSpec {
    DurationSpec(std::time::Duration::from_secs(60))
}

/// Set the share of an archive that has to be live for a pass to leave it alone, 50 percent
///
/// An archive under it is copied: its live records into the active archive, and the file deleted.
/// Higher reclaims space sooner and copies more for it; the lab's rewrite-heavy bench paid about 6%
/// of its throughput at 50 ([O74](../../../../../../docs/src/appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)).
fn default_archive_pass_live_percent() -> u8 {
    50
}

/// Set the smallest base record a merge writes fragments over rather than rewriting, 4 KiB
///
/// A merge rewrites a partition whole unless its record is at least this large; below it a
/// rewrite costs about what a fragment does and leaves nothing to fold on a read. On the lab 4 KiB
/// and chains of 16 cut the keyword table's archive writes by four fifths where 16 KiB and chains
/// of 8 cut them by half, and cold reads of the table did not slow
/// ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
fn default_fragment_min_bytes() -> usize {
    4 << 10
}

/// Set how many fragments a partition's chain holds before a merge writes it whole again, 16
///
/// Zero writes every partition whole, as before F61. A read of a chained partition reads each
/// record of its chain, so this bounds the reads a get pays for what a merge saves
/// ([F61](../../../../../../docs/src/features/fragmented-partitions.md)).
fn default_fragment_max_chain() -> usize {
    16
}

/// The settings to use for a specific writer
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileSystemThroughputWriterConf {
    /// The path to write too (table name will be added before the final filename)
    #[serde(default = "default_path")]
    pub path: PathBuf,
    /// The buffer size to use when writting data
    #[serde(default = "default_throughput_buffer_size")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub buffer_size: usize,
    /// The number of write behind buffers to use
    #[serde(default = "default_throughput_write_behind")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub write_behind: usize,
    /// How many bytes one archive pass copies before it ends and is queued again
    #[serde(default = "default_archive_pass_bytes")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub archive_pass_bytes: usize,
    /// The least time between the starts of two archive passes
    #[serde(default = "default_archive_pass_interval")]
    pub archive_pass_interval: DurationSpec,
    /// The percent of an archive that has to be live for a pass to leave it alone, 1 to 99
    #[serde(default = "default_archive_pass_live_percent")]
    pub archive_pass_live_percent: u8,
    /// The smallest base record of a sorted partition a merge writes fragments over
    #[serde(default = "default_fragment_min_bytes")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub fragment_min_bytes: usize,
    /// How many fragments a chain holds before a merge writes the partition whole, zero for none
    #[serde(default = "default_fragment_max_chain")]
    pub fragment_max_chain: usize,
}

impl Default for FileSystemThroughputWriterConf {
    /// Create a default `FileSystemArchiveWriterConf`
    fn default() -> Self {
        FileSystemThroughputWriterConf {
            path: default_path(),
            buffer_size: default_throughput_buffer_size(),
            write_behind: default_throughput_write_behind(),
            archive_pass_bytes: default_archive_pass_bytes(),
            archive_pass_interval: default_archive_pass_interval(),
            archive_pass_live_percent: default_archive_pass_live_percent(),
            fragment_min_bytes: default_fragment_min_bytes(),
            fragment_max_chain: default_fragment_max_chain(),
        }
    }
}

impl FileSystemThroughputWriterConf {
    /// Create a new FileSystemThroughputWriterConf with default values
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set the path to write to
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Set the buffer size for writing data
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the number of write behind buffers
    pub fn write_behind(mut self, write_behind: usize) -> Self {
        self.write_behind = write_behind;
        self
    }

    /// Set how many bytes one archive pass copies before it ends and is queued again
    ///
    /// # Arguments
    ///
    /// * `archive_pass_bytes` - The budget in bytes
    pub fn archive_pass_bytes(mut self, archive_pass_bytes: usize) -> Self {
        self.archive_pass_bytes = archive_pass_bytes;
        self
    }

    /// Set the least time between the starts of two archive passes
    ///
    /// # Arguments
    ///
    /// * `archive_pass_interval` - The interval
    pub fn archive_pass_interval(mut self, archive_pass_interval: std::time::Duration) -> Self {
        self.archive_pass_interval = DurationSpec(archive_pass_interval);
        self
    }

    /// Set the percent of an archive that has to be live for a pass to leave it alone
    ///
    /// # Arguments
    ///
    /// * `percent` - The share, from 1 to 99
    pub fn archive_pass_live_percent(mut self, percent: u8) -> Self {
        self.archive_pass_live_percent = percent;
        self
    }

    /// Set the smallest base record a merge writes fragments over
    ///
    /// # Arguments
    ///
    /// * `bytes` - The size of the base record
    pub fn fragment_min_bytes(mut self, bytes: usize) -> Self {
        self.fragment_min_bytes = bytes;
        self
    }

    /// Set how many fragments a chain holds before a merge writes the partition whole
    ///
    /// # Arguments
    ///
    /// * `fragments` - The longest chain, zero to write every partition whole
    pub fn fragment_max_chain(mut self, fragments: usize) -> Self {
        self.fragment_max_chain = fragments;
        self
    }
}

/// Set how many changed partitions a map holds in memory before it writes them as a run, 16,384
///
/// The delta is what the map's intent log holds since the last flush, kept in memory so a lookup
/// of a partition just compacted costs no read. About ninety bytes a partition, so a map's delta
/// stays under about a mebibyte and a half ([F76](../../../../../../docs/src/features/paged-archive-map.md)).
fn default_map_delta_entries() -> usize {
    16_384
}

/// Set how many bytes of decoded index pages a map keeps cached, 2 Mebibytes
///
/// A page is 4 KiB and holds about 145 partitions, so this holds about 74,000 partitions' entries
/// a table a shard. Every cold point read whose page is not here costs one more device read
/// ([F76](../../../../../../docs/src/features/paged-archive-map.md)).
fn default_map_page_cache_bytes() -> usize {
    2 << 20
}

/// Set how many bits a key each run's filter keeps in memory, 10
///
/// About one percent of lookups for a key a run does not hold read a page anyway. Zero keeps no
/// filter, which bounds the map's memory by the cache and the delta alone, at the price of a read
/// for every key whose page is not cached ([F76](../../../../../../docs/src/features/paged-archive-map.md)).
fn default_map_filter_bits() -> u32 {
    10
}

/// Set how many times larger a run may be than the run above it before the two are merged, 4
///
/// The runs' sizes grow by at least this factor from newest to oldest, so a map of N partitions
/// holds about log4(N / delta) runs and rewrites a partition's entry about that many times
/// ([F76](../../../../../../docs/src/features/paged-archive-map.md)).
fn default_map_merge_ratio() -> u64 {
    4
}

/// The settings for a table's archive map, the index of where each partition's record is
///
/// The map is paged ([F76](../../../../../../docs/src/features/paged-archive-map.md)): what it
/// holds in memory is the delta since its last flush, each run's directory and filter, and a
/// cache of pages, and these are what bound it.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArchiveMapConf {
    /// How many changed partitions the map holds in memory before it writes them as a run
    #[serde(default = "default_map_delta_entries")]
    pub delta_entries: usize,
    /// How many bytes of index pages the map keeps cached
    #[serde(default = "default_map_page_cache_bytes")]
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub page_cache_bytes: usize,
    /// How many bits a key each run's filter keeps in memory, zero for no filter
    #[serde(default = "default_map_filter_bits")]
    pub filter_bits: u32,
    /// How many times larger a run may be than the one above it before they are merged
    #[serde(default = "default_map_merge_ratio")]
    pub merge_ratio: u64,
}

impl Default for ArchiveMapConf {
    /// Create a default `ArchiveMapConf`
    fn default() -> Self {
        ArchiveMapConf {
            delta_entries: default_map_delta_entries(),
            page_cache_bytes: default_map_page_cache_bytes(),
            filter_bits: default_map_filter_bits(),
            merge_ratio: default_map_merge_ratio(),
        }
    }
}

impl ArchiveMapConf {
    /// Create a new ArchiveMapConf with default values
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set how many changed partitions the map holds in memory before it writes them as a run
    ///
    /// # Arguments
    ///
    /// * `entries` - The most partitions the delta holds, at least one
    pub fn delta_entries(mut self, entries: usize) -> Self {
        self.delta_entries = entries;
        self
    }

    /// Set how many bytes of index pages the map keeps cached
    ///
    /// # Arguments
    ///
    /// * `bytes` - The cache's budget, zero for no cache
    pub fn page_cache_bytes(mut self, bytes: usize) -> Self {
        self.page_cache_bytes = bytes;
        self
    }

    /// Set how many bits a key each run's filter keeps in memory
    ///
    /// # Arguments
    ///
    /// * `bits` - The bits a key, zero for no filter
    pub fn filter_bits(mut self, bits: u32) -> Self {
        self.filter_bits = bits;
        self
    }

    /// Set how many times larger a run may be than the one above it before they are merged
    ///
    /// # Arguments
    ///
    /// * `ratio` - The ratio, at least two
    pub fn merge_ratio(mut self, ratio: u64) -> Self {
        self.merge_ratio = ratio;
        self
    }
}

/// Create all directories in this path
async fn mkdir_all(path: &PathBuf) -> Result<(), ServerError> {
    // build our path slowly
    let mut built = PathBuf::new();
    // step over each part of this path
    for component in path.iter() {
        // add this component to our path
        built.push(component);
        // create this component of our path
        let dir = Directory::create(&built).await?;
        // close this directory
        dir.close().await?;
    }
    Ok(())
}

/// The settings for a specific tables file system based storage
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct FileSystemTableConf {
    /// The settings for the highly latency sensistive io
    #[serde(default)]
    pub latency_sensitive: FileSystemLatencyWriterConf,
    /// The settings for the lower latency but high throughput sensistive io
    #[serde(default)]
    pub throughput_sensitive: FileSystemThroughputWriterConf,
    /// The settings for the table's archive map
    #[serde(default)]
    pub map: ArchiveMapConf,
}

impl FileSystemTableConf {
    /// Create a new FileSystemTableConf with default values
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set the archive map's configuration
    ///
    /// # Arguments
    ///
    /// * `map` - The archive map's settings
    pub fn map(mut self, map: ArchiveMapConf) -> Self {
        self.map = map;
        self
    }

    /// Set the latency sensitive writer configuration
    pub fn latency_sensitive(mut self, latency_sensitive: FileSystemLatencyWriterConf) -> Self {
        self.latency_sensitive = latency_sensitive;
        self
    }

    /// Set the throughput sensitive writer configuration
    pub fn throughput_sensitive(
        mut self,
        throughput_sensitive: FileSystemThroughputWriterConf,
    ) -> Self {
        self.throughput_sensitive = throughput_sensitive;
        self
    }

    /// Get the path to this shards intent log
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to build an intent path for
    pub fn get_intent_path(&self, name: &str) -> PathBuf {
        // add our table name to this path
        let mut path = self.latency_sensitive.path.join(name);
        // add our intents folder
        path.push("intents");
        path
    }

    /// Get the path to this shards archive directory
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to build an archive path for
    pub fn get_archive_path(&self, name: &str) -> PathBuf {
        // add our table name to this path
        let mut path = self.throughput_sensitive.path.join(name);
        // add our archives folder
        path.push("archives");
        path
    }

    /// Get the path to this shards archive map directory
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to build an archive path for
    pub fn get_archive_map_path(&self, name: &str) -> PathBuf {
        // add our table name to this path
        let mut path = self.latency_sensitive.path.join(name);
        // add our maps folder
        path.push("maps");
        path
    }

    /// Get the path to this shards archive map directory
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to build an archive path for
    pub fn get_archive_map_temp_path(&self, name: &str) -> PathBuf {
        // get our archive map path
        let mut path = self.get_archive_map_path(name);
        // add our maps folder
        path.push("temp");
        path
    }

    /// Get the path to this shards archive map directory
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to build an archive path for
    pub fn get_archive_intent_path(&self, name: &str) -> PathBuf {
        // get that path to this shards archives
        let mut path = self.get_archive_path(name);
        // add our maps folder
        path.push("intents");
        path
    }

    /// Setup all of our paths
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to setup directories for
    #[instrument(name = "FileSystemTableConf::setup_paths", skip(self), err(Debug))]
    pub async fn setup_paths(&self, name: &str) -> Result<(), ServerError> {
        // Create all of our directories
        mkdir_all(&self.get_intent_path(name)).await?;
        mkdir_all(&self.get_archive_path(name)).await?;
        mkdir_all(&self.get_archive_map_path(name)).await?;
        mkdir_all(&self.get_archive_map_temp_path(name)).await?;
        mkdir_all(&self.get_archive_intent_path(name)).await?;
        Ok(())
    }
}

pub struct FileSystemConf {
    /// The default settings to apply to all tables
    pub default: FileSystemTableConf,
    /// The table specific settings to set
    pub specific: HashMap<String, FileSystemTableConf>,
}
