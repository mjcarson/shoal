//! The config for a Shoal database

use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};

use super::ServerError;

/// The compute settings to use
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Compute {
    /// Configure the number of cores to use, default to all
    cores: Option<usize>,
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

/// The config for running Shoal
#[derive(Serialize, Deserialize, Clone)]
pub struct Conf {
    /// The compute settings to use
    #[serde(default)]
    pub compute: Compute,
    /// The networking settings for shoal
    #[serde(default)]
    pub networking: Networking,
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
