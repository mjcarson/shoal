//! The config for a Shoal database

use byte_unit::Byte;
use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::level_filters::LevelFilter;

use super::tables::storage::fs::conf::FileSystemTableConf;
use super::ServerError;
use crate::utils::{self, IntoStorageSize};

/// The resource settings to use
///
/// Unknown keys are rejected rather than ignored. A misspelled resource setting is not a
/// harmless no-op: it silently gives back a server with a different shape than the one the
/// config describes, and the config file is the only record of what a benchmark measured.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// Configure the number of cores to use, default to all
    pub cores: Option<usize>,
    /// Any cores to exclude from use
    ///
    /// This filters on the physical core id, so excluding a core removes both of its SMT
    /// threads.
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
    ///
    /// When a core count is set this spreads the shards over distinct physical cores before
    /// it puts two of them on the threads of one core. Both parts of that matter:
    ///
    /// [`CpuSet`] is backed by a [`HashSet`](std::collections::HashSet), whose iteration order
    /// depends on a per process hash seed, so taking cpus straight off it gave a different
    /// core assignment on every start. Runs of the same build were not comparable to each
    /// other, and neither ordering nor pairing was stable enough to reason about.
    ///
    /// Taking them in sorted order alone would fix the churn and keep the worse half of the
    /// problem: cpu `n` and cpu `n + physical_cores` are two threads of one core, so a plain
    /// ascending scan fills both threads of the low cores while the high cores stay idle.
    pub fn cpus(&self) -> Result<CpuSet, ServerError> {
        // get all online cpus
        let online = CpuSet::online()?
            // never run on cpu 0 as that is the coordinator cpu
            .filter(|location| location.cpu != 0)
            // don't run on any excluded cores
            .filter(|location| !self.exclude_cores.contains(&location.core));
        // hand back everything we may use if no core count was set
        let Some(cores) = self.cores else {
            return Ok(online);
        };
        // sort our candidates into a stable order so the same config picks the same cpus
        let mut candidates = online.into_iter().collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|location| {
            (
                location.numa_node,
                location.package,
                location.core,
                location.cpu,
            )
        });
        // take one cpu from each physical core first
        let mut selected = Vec::with_capacity(cores);
        let mut claimed_cores = HashSet::with_capacity(cores);
        for location in &candidates {
            // stop once we have all the cpus we were asked for
            if selected.len() == cores {
                break;
            }
            // claim this cpu only if nothing else is on its physical core yet
            if claimed_cores.insert(location.core) {
                selected.push(location.clone());
            }
        }
        // only now start doubling up on the sibling threads of cores we already took
        let claimed_cpus = selected
            .iter()
            .map(|location| location.cpu)
            .collect::<HashSet<_>>();
        for location in candidates {
            // stop once we have all the cpus we were asked for
            if selected.len() == cores {
                break;
            }
            // skip any cpu we already claimed in the first pass
            if !claimed_cpus.contains(&location.cpu) {
                selected.push(location);
            }
        }
        Ok(selected.into_iter().collect::<CpuSet>())
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
#[derive(Serialize, Deserialize, Clone, Debug)]
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
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
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
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TableSettings {
    /// The filesystem based storage engine config
    FS(FileSystemTableConf),
}

/// The storage settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
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
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
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
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RemoteTracing {
    /// The settings for a GRPC based tracing sink
    Grpc(String),
}

/// The tracing settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
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
#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[cfg(test)]
mod tests {
    use super::{Conf, Resources};

    /// Write a config file into a temp dir and load it
    ///
    /// # Arguments
    ///
    /// * `body` - The yaml body to write
    fn load(body: &str) -> (tempfile::TempDir, Result<Conf, config::ConfigError>) {
        // build a temp dir to hold this config
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // write our config body into it
        let path = dir.path().join("shoal.yml");
        std::fs::write(&path, body).expect("failed to write a temp config");
        // load the config we just wrote
        let conf = Conf::from_file(path.to_str().expect("temp path was not utf8"));
        // hand back the temp dir too so it outlives this load
        (dir, conf)
    }

    #[test]
    /// A misspelled resource setting is rejected instead of being silently dropped
    ///
    /// `exluded_cores` was set in the shipped config for a long time while the field is
    /// `exclude_cores`. Serde ignored the unknown key, so core exclusion never happened and
    /// every benchmark that believed it had isolated cores had not.
    fn misspelled_resource_key_is_rejected() {
        // load a config with the historical typo in it
        let (_dir, conf) = load("resources:\n  memory: \"4Gi\"\n  exluded_cores: [12, 13, 14, 15]\n");
        // that config must not load at all
        let error = conf.expect_err("a misspelled resource key was accepted");
        // and the error must name the key that was wrong
        let rendered = error.to_string();
        assert!(
            rendered.contains("exluded_cores"),
            "error did not name the offending key: {rendered}"
        );
    }

    #[test]
    /// The correctly spelled key still loads and still excludes those cores
    fn exclude_cores_is_honored() {
        // load a config that spells the key correctly
        let (_dir, conf) = load("resources:\n  memory: \"4Gi\"\n  exclude_cores: [12, 13, 14, 15]\n");
        let conf = conf.expect("a correctly spelled config failed to load");
        // the cores we asked to exclude should have been parsed
        assert_eq!(conf.resources.exclude_cores, vec![12, 13, 14, 15]);
    }

    /// Build a sorted list of the (cpu, core) pairs a set of resources selects
    ///
    /// # Arguments
    ///
    /// * `resources` - The resources to build a cpuset from
    fn layout(resources: &Resources) -> Vec<(usize, usize)> {
        // build the cpuset these resources describe
        let cpus = resources.cpus().expect("failed to build a cpuset");
        // reduce it to the pairs we care about, sorted so two runs can be compared
        let mut layout = cpus
            .into_iter()
            .map(|location| (location.cpu, location.core))
            .collect::<Vec<_>>();
        layout.sort_unstable();
        layout
    }

    /// The number of physical cores this machine will let a shard run on
    fn available_cores() -> usize {
        Resources::default()
            .cpus()
            .expect("failed to build a cpuset")
            .into_iter()
            .map(|location| location.core)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    #[test]
    /// The same config selects the same cpus every time it is asked
    ///
    /// `CpuSet` is a `HashSet`, so it iterates in an order that depends on a per process hash
    /// seed. Taking cpus straight off it gave a different core assignment on every start,
    /// which meant two runs of the same build were not comparable.
    fn cpu_selection_is_deterministic() {
        // ask for the same layout several times over
        let resources = Resources::default().cores(4);
        let first = layout(&resources);
        // every later answer has to match the first one exactly
        for attempt in 0..16 {
            assert_eq!(
                layout(&resources),
                first,
                "cpu selection changed on attempt {attempt}"
            );
        }
    }

    #[test]
    /// Shards land on distinct physical cores before any core gets two of them
    ///
    /// A plain ascending scan of cpu ids fills both SMT threads of the low cores while the
    /// high cores sit idle, which costs far more than pairing saves.
    fn cpu_selection_fills_physical_cores_first() {
        // ask for fewer cpus than we have physical cores to spread over
        let wanted = std::cmp::min(4, available_cores());
        let selected = layout(&Resources::default().cores(wanted));
        // we should have been given exactly what we asked for
        assert_eq!(selected.len(), wanted);
        // and no two of them may share a physical core
        let cores = selected
            .iter()
            .map(|(_, core)| *core)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            cores.len(),
            wanted,
            "two shards shared a physical core while another sat idle: {selected:?}"
        );
    }

    #[test]
    /// Excluding a core removes it from the cpuset we build
    ///
    /// `exclude_cores` filters on the physical core id, so excluding one drops both of its
    /// SMT threads. That is what makes a clean client/server split possible on an SMT part.
    fn excluded_cores_leave_the_cpuset() {
        // build resources that exclude a couple of physical cores
        let resources = Resources::default().exclude_cores(vec![1, 2]);
        // build the cpuset those resources describe
        let cpus = resources.cpus().expect("failed to build a cpuset");
        // none of the cpus in it may live on an excluded core
        assert!(
            cpus.into_iter().all(|location| location.core != 1 && location.core != 2),
            "an excluded core survived into the cpuset"
        );
    }
}
