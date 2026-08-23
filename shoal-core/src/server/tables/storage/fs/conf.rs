//! The config for FileSystem based storage

use byte_unit::Byte;
use glommio::io::Directory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::instrument;

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
}

impl Default for FileSystemThroughputWriterConf {
    /// Create a default `FileSystemArchiveWriterConf`
    fn default() -> Self {
        FileSystemThroughputWriterConf {
            path: default_path(),
            buffer_size: default_throughput_buffer_size(),
            write_behind: default_throughput_write_behind(),
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
}

impl FileSystemTableConf {
    /// Create a new FileSystemTableConf with default values
    pub fn builder() -> Self {
        Self::default()
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
