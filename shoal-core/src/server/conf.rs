//! The config for a Shoal database

use byte_unit::Byte;
use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::level_filters::LevelFilter;

use super::tables::storage::fs::conf::FileSystemTableConf;
use super::ServerError;
use crate::utils::{self, IntoStorageSize};

/// The resource settings to use
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Resources {
    /// Configure the number of cores to use, default to all
    pub cores: Option<usize>,
    /// Any cores to exlude from use
    #[serde(default)]
    pub exclude_cores: Vec<usize>,
    /// The max amount of memory to use in bytes
    #[serde(deserialize_with = "utils::deserialize_byte_size")]
    pub memory: usize,
}

impl Resources {
    /// Set the number of cores to use
    pub fn cores(mut self, cores: usize) -> Self {
        self.cores = Some(cores);
        self
    }

    /// Set the cores to exclude from use
    pub fn exclude_cores(mut self, exclude_cores: Vec<usize>) -> Self {
        self.exclude_cores = exclude_cores;
        self
    }

    /// Set the maximum amount of memory to use
    pub fn memory<M: IntoStorageSize>(mut self, memory: M) -> Result<Self, ServerError> {
        self.memory = memory.into_bytes()?;
        Ok(self)
    }

    /// Get the cpuset to run shoal on
    pub fn cpus(&self) -> Result<CpuSet, ServerError> {
        // get all online cpus
        let online = CpuSet::online()?
            // never run on cpu 0 as that is the coordinator cpu
            .filter(|location| location.cpu != 0)
            // don't run on any excluded cores
            .filter(|location| !self.exclude_cores.contains(&location.core));
        // if the user specified a limted number of cores then force that
        if let Some(cores) = self.cores {
            // limit our cores to just number specfied
            let cpus = online.into_iter().take(cores).collect::<CpuSet>();
            Ok(cpus)
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
fn default_port() -> u16 {
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
    pub port: u16,
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
    /// Set the interface to bind to
    pub fn interface(mut self, interface: impl Into<String>) -> Self {
        self.interface = interface.into();
        self
    }

    /// Set the port to bind to
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Build the address to bind too
    pub fn to_addr(&self) -> String {
        println!("listening on {}:{}", self.interface, self.port);
        format!("{}:{}", self.interface, self.port)
    }
}

/// The settings to apply to each storage engine kinds if no specific table settings set
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DefaultStorageSettings {
    /// The settings for the filesystem storage engine
    #[serde(default)]
    pub filesystem: FileSystemTableConf,
}

impl DefaultStorageSettings {
    /// Set the filesystem storage settings
    pub fn filesystem(mut self, filesystem: FileSystemTableConf) -> Self {
        self.filesystem = filesystem;
        self
    }
}

/// The different storage engines in Shoal
#[derive(Serialize, Deserialize, Clone)]
pub enum TableSettings {
    /// The filesystem based storage engine config
    FS(FileSystemTableConf),
}

/// The storage settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Storage {
    /// The default settings to apply to different storage engines
    #[serde(default)]
    pub default: DefaultStorageSettings,
    /// The table specific settings to use
    #[serde(default)]
    pub tables: HashMap<String, TableSettings>,
}

impl Storage {
    /// Set the default storage settings
    pub fn default_settings(mut self, default: DefaultStorageSettings) -> Self {
        self.default = default;
        self
    }

    /// Add table-specific settings
    pub fn table(mut self, name: impl Into<String>, settings: TableSettings) -> Self {
        self.tables.insert(name.into(), settings);
        self
    }

    /// Set all table-specific settings
    pub fn tables(mut self, tables: HashMap<String, TableSettings>) -> Self {
        self.tables = tables;
        self
    }
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

/// The settings for different remote tracing sinks (not stdout)
#[derive(Serialize, Deserialize, Clone)]
pub enum RemoteTracing {
    /// The settings for a GRPC based tracing sink
    Grpc(String),
}

/// The tracing settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Tracing {
    /// The level to log traces at
    #[serde(default)]
    pub level: TraceLevel,
    /// The settings for sending traces to a grpc sink
    pub remote: Option<RemoteTracing>,
}

impl Tracing {
    /// Set the trace level
    pub fn level(mut self, level: TraceLevel) -> Self {
        self.level = level;
        self
    }

    /// Set the remote tracing sink
    pub fn remote(mut self, remote: RemoteTracing) -> Self {
        self.remote = Some(remote);
        self
    }

    /// Set a GRPC remote tracing sink
    pub fn grpc(mut self, endpoint: impl Into<String>) -> Self {
        self.remote = Some(RemoteTracing::Grpc(endpoint.into()));
        self
    }
}

/// The config for running Shoal
#[derive(Serialize, Deserialize, Clone)]
pub struct Conf {
    /// The compute settings to use
    #[serde(default)]
    pub resources: Resources,
    /// The networking settings to use
    #[serde(default)]
    pub networking: Networking,
    /// The tracing settings to use
    #[serde(default)]
    pub tracing: Tracing,
    /// The storage settings to use
    #[serde(default)]
    pub storage: Storage,
}

impl Default for Conf {
    fn default() -> Self {
        Conf {
            resources: Resources::default(),
            networking: Networking::default(),
            tracing: Tracing::default(),
            storage: Storage::default(),
        }
    }
}

impl Conf {
    /// Build a config from our environment and a config file
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        // build our config sources
        let conf = Config::builder()
            // start with the settings in our config file
            .add_source(config::File::with_name(path).required(false))
            // overlay our env vars on top
            .add_source(config::Environment::with_prefix("shoal"))
            .build()?;
        conf.try_deserialize()
    }

    /// Build a config from our environment and a config file
    #[deprecated(since = "0.2.0", note = "Use `Conf::from_file` instead")]
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        Self::from_file(path)
    }

    /// Set the resource settings
    pub fn resources(mut self, resources: Resources) -> Self {
        self.resources = resources;
        self
    }

    /// Set the networking settings
    pub fn networking(mut self, networking: Networking) -> Self {
        self.networking = networking;
        self
    }

    /// Set the tracing settings
    pub fn tracing(mut self, tracing: Tracing) -> Self {
        self.tracing = tracing;
        self
    }

    /// Set the storage settings
    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }
}
