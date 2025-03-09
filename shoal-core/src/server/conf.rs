//! The config for a Shoal database

use std::collections::HashMap;

use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;

use super::ServerError;
// The table specific configs
pub use crate::server::tables::storage::fs::conf::FileSystemTableConf;

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

/// The settings to apply to each storage engine kinds if no specific table settings set
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DefaultStorageSettings {
    /// The settings for the filesystem storage engine
    #[serde(default)]
    pub filesystem: FileSystemTableConf,
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
    /// The tracing settings to use
    #[serde(default)]
    pub tracing: Tracing,
    /// The storage settings to use
    #[serde(default)]
    pub storage: Storage,
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
