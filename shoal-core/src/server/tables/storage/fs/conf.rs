//! The config for FileSystem based storage

use glommio::io::Directory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::instrument;

use crate::server::ServerError;

/// Set default path for latency files
fn default_path() -> PathBuf {
    PathBuf::from("/opt/shoal")
}

/// Set default buffer_size for latency files
fn default_latency_buffer_size() -> usize {
    1024
}

/// Set default write behind for latency files
fn default_latency_write_behind() -> usize {
    128
}

/// The settings to use for a specific writer
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileSystemLatencyWriterConf {
    /// The path to write too (table name will be added before the final filename)
    #[serde(default = "default_path")]
    pub path: PathBuf,
    /// The buffer size to use when writting data
    #[serde(default = "default_latency_buffer_size")]
    pub buffer_size: usize,
    /// The number of write behind buffers to use
    #[serde(default = "default_latency_write_behind")]
    pub write_behind: usize,
}

impl Default for FileSystemLatencyWriterConf {
    /// Create a default `FileSystemlatencyWriterConf`
    fn default() -> Self {
        FileSystemLatencyWriterConf {
            path: default_path(),
            buffer_size: default_latency_buffer_size(),
            write_behind: default_latency_write_behind(),
        }
    }
}

/// Set default buffer_size for throughput files
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
    pub buffer_size: usize,
    /// The number of write behind buffers to use
    #[serde(default = "default_throughput_write_behind")]
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
