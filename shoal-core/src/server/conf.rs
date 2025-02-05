//! The config for a Shoal database

use std::{path::PathBuf, str::FromStr};

use config::{Config, ConfigError};
use glommio::{
    io::{DmaFile, DmaStreamReaderBuilder, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions},
    CpuSet,
};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;
use uuid::Uuid;

use super::ServerError;

/// The compute settings to use
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Compute {
    /// Configure the number of cores to use, default to all
    cores: Option<usize>,
    /// The max amount of memory to use
    memory: Option<usize>,
}

impl Compute {
    /// Get the cpuset to run shoal on
    pub fn cpus(&self) -> Result<CpuSet, ServerError> {
        // get all online cpus
        let online = CpuSet::online()?;
        // if the user specified a limted number of cores then force that
        if let Some(cores) = self.cores {
            // limit our cores to just number specfied
            let cores = online.into_iter().take(cores).collect::<CpuSet>();
            Ok(cores)
        } else {
            Ok(online)
        }
    }
}

/// Help serde default interface we should bind to
fn default_interface() -> String {
    "127.0.0.1".to_owned()
}

/// Help serde default interface we should bind to
fn default_port() -> usize {
    12000
}

/// The networking settings for Shoal
#[derive(Serialize, Deserialize, Clone)]
pub struct Networking {
    /// The interface to bind too
    #[serde(default = "default_interface")]
    pub interface: String,
    /// The port to bind too
    #[serde(default = "default_port")]
    pub port: usize,
}

impl Default for Networking {
    /// Builds a default networking struct
    fn default() -> Self {
        Networking {
            interface: default_interface(),
            port: default_port(),
        }
    }
}

impl Networking {
    /// Build the address to bind too
    pub fn to_addr(&self) -> String {
        println!("listening on {}:{}", self.interface, self.port);
        format!("{}:{}", self.interface, self.port)
    }
}

/// Help serde set a default file system storage path
fn default_fs_intent_path() -> PathBuf {
    PathBuf::from_str("/opt/shoal/intents")
        .expect("Failed to build default filesystem storage path")
}

/// Help serde set a default file system storage path
fn default_fs_archives_path() -> PathBuf {
    PathBuf::from_str("/opt/shoal/archives")
        .expect("Failed to build default filesystem storage path")
}

/// Help serde set a default file system storage path
fn default_fs_map_path() -> PathBuf {
    PathBuf::from_str("/opt/shoal/maps").expect("Failed to build default filesystem storage path")
}

/// The settings for file system based storage
#[derive(Serialize, Deserialize, Clone)]
pub struct FileSystemStorage {
    /// Where to store intent logs
    #[serde(default = "default_fs_intent_path")]
    pub intent: PathBuf,
    /// Where to store compacted partitions
    #[serde(default = "default_fs_archives_path")]
    pub archives: PathBuf,
    /// Where to store map data
    #[serde(default = "default_fs_map_path")]
    pub maps: PathBuf,
}

impl Default for FileSystemStorage {
    fn default() -> Self {
        FileSystemStorage {
            intent: default_fs_intent_path(),
            archives: default_fs_archives_path(),
            maps: default_fs_map_path(),
        }
    }
}

impl FileSystemStorage {
    /// Get a handle to a new empty archive stream writer
    pub async fn get_new_archive_path(
        &self,
    ) -> Result<(Uuid, DmaFile, DmaStreamWriter), ServerError> {
        // build the path to this new file
        let mut path = self.archives.clone();
        // get a random uuid for our file name
        let id = Uuid::new_v4();
        // append this random id
        path.push(id.to_string());
        // set the options to ensure we create a new writable file
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // wrap our file in a stream writer
        let writer = DmaStreamWriterBuilder::new(file.dup()?).build();
        Ok((id, file, writer))
    }

    ///  Get a reader to an archive file
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the archive to build a path too
    pub fn get_archive_path(&self, id: &Uuid) -> PathBuf {
        // clone our base path
        let mut path = self.archives.clone();
        // build the path to this archive file
        path.push(id.to_string());
        path
    }

    /// Get the path to the archive map file for this shard
    pub fn get_archive_intent_path(&self, shard_name: &str) -> PathBuf {
        // get the base path for our map data
        let mut path = self.maps.clone();
        // build the path to this shards map intent log
        path.push(format!("{}-intents", &shard_name));
        path
    }

    /// Get the path to the temp archive map file for this shard
    pub fn get_temp_archive_intent_path(&self, shard_name: &str) -> PathBuf {
        // get the base path for our map data
        let mut path = self.maps.clone();
        // build the path to this shards map intent log
        path.push(format!("{}-intents-temp", &shard_name));
        path
    }

    /// Get the path to the temp archive map file for this shard
    pub fn get_temp_archive_map_path(&self, shard_name: &str) -> PathBuf {
        // clone our archive map base path
        let mut path = self.maps.clone();
        // add our shard name to this path
        path.push(format!("{shard_name}-temp"));
        path
    }

    /// Get the path to the archive map file
    pub fn get_archive_map_path(&self, shard_name: &str) -> PathBuf {
        // clone our archive map base path
        let mut path = self.maps.clone();
        // add our shard name to this path
        path.push(format!("{shard_name}"));
        path
    }
}

/// The storage settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Storage {
    /// The settings for file system based storage
    #[serde(default)]
    pub fs: FileSystemStorage,
}

/// The different levels to log tracing info at
#[derive(Serialize, Deserialize, Clone, Default)]
pub enum TraceLevel {
    /// Log everything include high verbosity low priority info
    Trace,
    /// Log low priority debug infomation and up
    Debug,
    /// Log standard priority information and up
    #[default]
    Info,
    /// Log only warning and Errors
    Warn,
    /// Log only errors
    Error,
    /// Do not log anything
    Off,
}

impl TraceLevel {
    /// Convert this [`TraceLevels`] to a [`LevelFilter`]
    pub fn to_filter(&self) -> LevelFilter {
        match self {
            TraceLevel::Trace => LevelFilter::TRACE,
            TraceLevel::Debug => LevelFilter::DEBUG,
            TraceLevel::Info => LevelFilter::INFO,
            TraceLevel::Warn => LevelFilter::WARN,
            TraceLevel::Error => LevelFilter::ERROR,
            TraceLevel::Off => LevelFilter::OFF,
        }
    }
}

/// The tracing settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Tracing {
    // The level to log traces at
    #[serde(default)]
    pub level: TraceLevel,
}

/// The config for running Shoal
#[derive(Serialize, Deserialize, Clone)]
pub struct Conf {
    /// The compute settings to use
    #[serde(default)]
    pub compute: Compute,
    /// The networking settings to use
    #[serde(default)]
    pub networking: Networking,
    /// The storage settings to use
    #[serde(default)]
    pub storage: Storage,
    /// The tracing settings to use
    #[serde(default)]
    pub tracing: Tracing,
}

impl Conf {
    /// Build a config from our environment and a config file
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        // build our config sources
        let conf = Config::builder()
            // start with the settings in our config file
            .add_source(config::File::with_name(path).required(false))
            // overlay our env vars on top
            .add_source(config::Environment::with_prefix("shoal"))
            .build()?;
        conf.try_deserialize()
    }
}
