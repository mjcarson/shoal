//! The config for a Shoal database

use byte_unit::Byte;
use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::level_filters::LevelFilter;

use super::tables::storage::fs::conf::FileSystemTableConf;
use super::{ServerError, ShoalError};
use crate::shared::auth::{CredentialStore, StoredCredential, DEFAULT_ITERATIONS};
use crate::shared::protocol::auth::AuthMechanism;
use crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES;
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

/// Help serde default the largest frame this server will accept
fn default_max_frame_bytes() -> u32 {
    DEFAULT_MAX_FRAME_BYTES
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
    /// The largest frame this server will read before it closes the connection that sent it
    ///
    /// A frame's length is used as an allocation size before a byte of its body has arrived, so
    /// without a bound a peer can ask this server to allocate whatever a `u32` can spell. This is
    /// what turns that into a refused connection.
    ///
    /// The client is told this number when it connects, so it can refuse to write a bundle that
    /// would be rejected rather than discovering it as a closed socket.
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: u32,
}

impl Default for Networking {
    /// Builds a default networking struct
    fn default() -> Self {
        Networking {
            interface: default_interface(),
            port: default_port(),
            max_frame_bytes: default_max_frame_bytes(),
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

    /// Set the largest frame this server will accept
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The largest frame to accept
    pub fn max_frame_bytes(mut self, max_frame_bytes: u32) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Build the address to bind too
    pub fn to_addr(&self) -> String {
        println!("listening on {}:{}", self.interface, self.port);
        format!("{}:{}", self.interface, self.port)
    }
}

/// Help serde default the mechanisms this server accepts
///
/// SCRAM is the only mechanism that can be selected today, so listing it is the only useful
/// default. A deployment that lists nothing and requires authentication refuses every client,
/// which is a legible failure rather than a silently open port.
fn default_mechanisms() -> Vec<AuthMechanism> {
    vec![AuthMechanism::ScramSha256]
}

/// What a config file says about one user
///
/// Two spellings, and the difference between them matters. `password` is derived into a
/// [`StoredCredential`] when the config is read and the password is dropped, which is convenient
/// and puts a password in a file. `scram_sha_256` is the derivation itself, which is what a
/// deployment that does not want a password on disk writes — generate one with
/// `cargo run --example scram_credential`.
///
/// This is two optional fields rather than an enum with two variants, which is what it wants to
/// be. The `config` crate cannot deserialize an externally tagged enum whose variant carries a
/// struct — it reports "does not have variant constructor" for the spelling every YAML example
/// would use — so the shape that reads correctly wins over the shape that models correctly, and
/// [`UserCredential::to_stored`] carries the check the type would otherwise have made impossible.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct UserCredential {
    /// A password to derive a credential from when this config is read
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// A credential that has already been derived, so no password is on disk
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_sha_256: Option<StoredCredential>,
}

impl UserCredential {
    /// Turn this into the credential a server actually checks against
    ///
    /// A derived credential wins over a password when a config names both, since it is the one
    /// that says what the deployment meant to store.
    ///
    /// # Arguments
    ///
    /// * `username` - The name this credential belongs to, for the error if it names nothing
    /// * `iterations` - The number of PBKDF2 rounds to derive a password with
    pub fn to_stored(
        &self,
        username: &str,
        iterations: u32,
    ) -> Result<StoredCredential, ServerError> {
        // a derivation is what the deployment meant to store, so it is preferred over a password
        if let Some(stored) = &self.scram_sha_256 {
            return Ok(stored.clone());
        }
        // otherwise derive the password now, so that nothing past this point holds one
        if let Some(password) = &self.password {
            return Ok(StoredCredential::from_password(password, iterations));
        }
        // a user that named neither is a config that cannot do what it says it does
        Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "the user {username} has neither a password nor a derived credential"
        ))))
    }
}

/// The authentication settings for Shoal
///
/// # Invariants
///
/// **The default is off.** A config with no `auth` section produces a server that requires
/// nothing, which is every deployment this database has had. Turning it on refuses every client
/// that has no credentials — including the benchmark harness, which is why the
/// [frozen baseline](../operations/performance-baseline.md) is only comparable against a server
/// with this left alone.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    /// Whether a client has to prove who it is before it can send a query
    #[serde(default)]
    pub required: bool,
    /// The mechanisms this server accepts, strongest first
    #[serde(
        default = "default_mechanisms",
        with = "crate::shared::auth::mechanism_names"
    )]
    pub mechanisms: Vec<AuthMechanism>,
    /// The number of PBKDF2 rounds a password in this file is derived with
    #[serde(default = "default_iterations")]
    pub iterations: u32,
    /// The users this server will accept, by name
    #[serde(default)]
    pub users: HashMap<String, UserCredential>,
}

/// Help serde default the number of rounds a password is derived with
fn default_iterations() -> u32 {
    DEFAULT_ITERATIONS
}

impl Default for Auth {
    /// Builds a default auth struct
    ///
    /// This is written out rather than derived because `serde`'s field defaults only fire while a
    /// file is being read. A derived `Default` would hand a builder an empty mechanism list, which
    /// is a server that requires authentication and accepts no way of providing it — the one
    /// configuration that cannot be talked to and does not look wrong.
    fn default() -> Self {
        Auth {
            required: false,
            mechanisms: default_mechanisms(),
            iterations: default_iterations(),
            users: HashMap::default(),
        }
    }
}

impl Auth {
    /// Require every client to prove who it is
    ///
    /// # Arguments
    ///
    /// * `required` - Whether authentication is required
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Set the mechanisms this server accepts
    ///
    /// # Arguments
    ///
    /// * `mechanisms` - The mechanisms to accept, strongest first
    pub fn mechanisms(mut self, mechanisms: Vec<AuthMechanism>) -> Self {
        self.mechanisms = mechanisms;
        self
    }

    /// Set the number of PBKDF2 rounds a password in this config is derived with
    ///
    /// # Arguments
    ///
    /// * `iterations` - The number of rounds to derive with
    pub fn iterations(mut self, iterations: u32) -> Self {
        self.iterations = iterations;
        self
    }

    /// Add a user with a password, which is derived when this config is turned into a store
    ///
    /// # Arguments
    ///
    /// * `username` - The name to add
    /// * `password` - The password to derive a credential from
    pub fn user<U: Into<String>, P: Into<String>>(mut self, username: U, password: P) -> Self {
        self.users.insert(
            username.into(),
            UserCredential {
                password: Some(password.into()),
                scram_sha_256: None,
            },
        );
        self
    }

    /// Build the store the server checks connections against
    ///
    /// Every password in this config is derived here and is not held past this call, which is the
    /// only reason a `password:` key is an acceptable thing to support at all. That derivation is
    /// the expensive one PBKDF2 exists to be, so this runs once per shard at startup rather than
    /// once per connection.
    ///
    /// # Errors
    ///
    /// Fails if a user in this config named neither a password nor a derived credential.
    pub fn store(&self) -> Result<CredentialStore, ServerError> {
        // derive every user this config named, dropping whatever password it named them with
        let users = self
            .users
            .iter()
            .map(|(name, credential)| {
                Ok((name.clone(), credential.to_stored(name, self.iterations)?))
            })
            .collect::<Result<_, ServerError>>()?;
        Ok(CredentialStore::new(
            users,
            self.mechanisms.clone(),
            self.required,
        ))
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
    /// The authentication settings to use
    #[serde(default)]
    pub auth: Auth,
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
            auth: Auth::default(),
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

    /// Set the authentication settings
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
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
    use super::{AuthMechanism, Conf, Resources, DEFAULT_ITERATIONS, DEFAULT_MAX_FRAME_BYTES};

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

    #[test]
    /// A config that never mentions the frame bound still gets one
    ///
    /// This is what let the frame bound be added without touching `shoal.yml`, which is committed
    /// and is the config every frozen benchmark was captured against. A required key would have
    /// invalidated that baseline for a setting nobody has ever needed to change.
    fn a_config_without_a_frame_bound_gets_the_default() {
        // load a config that says nothing about networking at all
        let (_dir, conf) = load("resources:\n  memory: \"4Gi\"\n");
        let conf = conf.expect("a config with no networking section failed to load");
        assert_eq!(conf.networking.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        // and one that sets a port but not a bound gets the same default
        let (_dir, conf) = load("networking:\n  port: 13000\n");
        let conf = conf.expect("a config with a partial networking section failed to load");
        assert_eq!(conf.networking.port, 13000);
        assert_eq!(conf.networking.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    }

    #[test]
    /// A config that names the frame bound overrides the default
    fn a_config_can_set_its_own_frame_bound() {
        // load a config that asks for a much smaller bound than the default
        let (_dir, conf) = load("networking:\n  max_frame_bytes: 4096\n");
        let conf = conf.expect("a config naming a frame bound failed to load");
        assert_eq!(conf.networking.max_frame_bytes, 4096);
    }

    #[test]
    /// A config with no auth section produces a server that requires nothing
    ///
    /// This is the case every deployment before authentication was in, and the one the benchmark
    /// harness and the integration suite are in. A default that required anything would refuse
    /// every one of them.
    fn auth_defaults_to_off() {
        let (_dir, conf) = load("resources:\n  memory: \"4Gi\"\n");
        let conf = conf.expect("a config with no auth section failed to load");
        assert!(!conf.auth.required);
        assert!(conf.auth.users.is_empty());
        // the store it builds selects nothing, which is what an open server does
        assert!(!conf
            .auth
            .store()
            .expect("an empty auth section failed to build a store")
            .is_required());
        // and the mechanism list still defaults to the one mechanism that works
        assert_eq!(conf.auth.mechanisms, vec![AuthMechanism::ScramSha256]);
        assert_eq!(conf.auth.iterations, DEFAULT_ITERATIONS);
    }

    #[test]
    /// A user named with a password is derived into a credential when the config is read
    ///
    /// The password is not kept anywhere past this point, which is the only reason supporting a
    /// `password:` key at all is defensible.
    fn a_password_in_the_config_is_derived() {
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\nauth:\n  required: true\n  users:\n    reader:\n      password: hunter2\n",
        );
        let conf = conf.expect("a config naming a password failed to load");
        assert!(conf.auth.required);
        // the store holds a derivation rather than the password
        let store = conf.auth.store().expect("failed to build a store");
        assert!(store.is_required());
        let (credential, known) = store.lookup("reader");
        assert!(known, "the user this config named was not in its store");
        assert_eq!(credential.iterations, DEFAULT_ITERATIONS);
        assert_eq!(credential.stored_key.len(), 32);
        // and nothing it holds is the password
        assert!(!credential.stored_key.windows(7).any(|w| w == b"hunter2"));
    }

    #[test]
    /// A derived credential can be written in the config instead of a password
    fn a_derived_credential_in_the_config_loads() {
        // derive one, spell it the way the file does, and read it back
        let derived = crate::shared::auth::StoredCredential::from_password("hunter2", 4096);
        let yaml = serde_yaml::to_string(&derived).expect("failed to write a credential");
        let indented = yaml
            .lines()
            .map(|line| format!("        {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let body = format!(
            "resources:\n  memory: \"4Gi\"\nauth:\n  required: true\n  users:\n    reader:\n      scram_sha_256:\n{indented}\n"
        );
        let (_dir, conf) = load(&body);
        let conf = conf.expect("a config naming a derived credential failed to load");
        let (credential, known) = conf
            .auth
            .store()
            .expect("failed to build a store")
            .lookup("reader");
        assert!(known);
        assert_eq!(credential, derived);
    }

    #[test]
    /// A mechanism nothing knows is rejected rather than silently dropped
    ///
    /// A deployment that misspelled the only mechanism it accepts would otherwise get a server
    /// that requires authentication and can never grant it, which looks like a broken client.
    fn an_unknown_mechanism_is_rejected() {
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\nauth:\n  required: true\n  mechanisms: [\"SCRAM-SHA-1\"]\n",
        );
        let error = conf.expect_err("an unknown mechanism was accepted");
        assert!(
            error.to_string().contains("SCRAM-SHA-1"),
            "the error did not name the offending mechanism: {error}"
        );
    }
}
