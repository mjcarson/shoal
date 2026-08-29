//! The config for a Shoal database

use byte_unit::Byte;
use config::{Config, ConfigError};
use glommio::CpuSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tracing::level_filters::LevelFilter;

use super::tables::storage::fs::conf::FileSystemTableConf;
use super::{ServerError, ShoalError};
use crate::shared::auth::{CredentialStore, StoredCredential, DEFAULT_ITERATIONS};
use crate::shared::protocol::auth::AuthMechanism;
use crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES;
use crate::shared::tls::TlsServerOptions;
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
///
/// # Invariants
///
/// **Unknown fields are refused.** Every other section of this config already refuses them, and
/// this one is where it matters most: a misspelled `tls:` key under a section that ignored it
/// would produce a server that starts, listens, and serves every query in clear, with nothing
/// anywhere saying so. A typo has to be a startup failure rather than a silent downgrade.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Networking {
    /// The interface to bind too
    #[serde(default = "default_interface")]
    pub interface: String,
    /// The port to bind too
    #[serde(default = "default_port")]
    pub port: u16,
    /// The certificate and key to encrypt client connections with, if this listener should
    ///
    /// Absent means plaintext, which is what every deployment before
    /// [F14](../../../docs/src/features/encryption-in-transit.md) had and what keeps the benchmark
    /// harness comparable against the frozen baseline. The same shape the `auth` section uses:
    /// a server asks for nothing unless a config says otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsServerOptions>,
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
            tls: None,
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

    /// Encrypt client connections to this listener
    ///
    /// # Arguments
    ///
    /// * `cert` - The PEM file holding this server's certificate chain, leaf first
    /// * `key` - The PEM file holding the private key for that chain
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::conf::Networking;
    ///
    /// let networking = Networking::default().tls("/etc/shoal/server.pem", "/etc/shoal/server.key");
    /// ```
    pub fn tls<C: Into<PathBuf>, K: Into<PathBuf>>(mut self, cert: C, key: K) -> Self {
        self.tls = Some(TlsServerOptions {
            cert: cert.into(),
            key: key.into(),
        });
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

/// The path an OTLP over HTTP collector accepts spans on
///
/// Named so that [`Tracing::metrics_sink`] can recognize a trace endpoint and rewrite it, rather
/// than asking a config to repeat the same host and port twice.
const TRACES_PATH: &str = "/v1/traces";

/// The path an OTLP over HTTP collector accepts metrics on
const METRICS_PATH: &str = "/v1/metrics";

/// The settings for an OTLP trace sink
///
/// Spans are exported as protobuf over HTTP, which is what port 4318 on a collector speaks.
/// There is no gRPC exporter, and never has been, despite what [`RemoteTracing::Grpc`] is called.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct OtlpTracing {
    /// The full URL to POST spans to, including the `/v1/traces` path
    pub endpoint: String,
    /// Any extra headers to send with every export
    ///
    /// A multi tenant collector reads the tenant out of a header rather than out of the payload,
    /// so this is where `X-Scope-OrgID` belongs. A tenant compiled into the binary is how spans
    /// end up somewhere nobody queries.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// How long a single export may take before it is abandoned, in seconds
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// How often the batch processor ships whatever it has, in milliseconds
    ///
    /// The SDK default is five seconds, which is longer than a short run lives. Anything not
    /// shipped by then leaves only on shutdown.
    #[serde(default)]
    pub batch_delay_ms: Option<u64>,
    /// How many spans may be queued before new ones are dropped
    #[serde(default)]
    pub max_queue_size: Option<usize>,
    /// What fraction of traces to export, between 0.0 and 1.0
    ///
    /// `None` exports every trace, which is what a short run wants. A workload driving millions of
    /// queries wants a small fraction of them instead, because the queue above is finite and a
    /// sink that cannot keep up drops spans rather than choosing which ones to keep.
    ///
    /// **This is the collector's knob, not the cost knob.** A sampler decides after `tracing` has
    /// already built the span, so it bounds what is exported and not what is spent building it.
    /// [`Tracing::level`] is what bounds the latter.
    #[serde(default)]
    pub sample_ratio: Option<f64>,
}

impl OtlpTracing {
    /// Creates a new OTLP trace sink pointed at an endpoint
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The full URL to POST spans to, including the `/v1/traces` path
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::conf::OtlpTracing;
    ///
    /// OtlpTracing::new("http://127.0.0.1:4318/v1/traces")
    ///     .header("X-Scope-OrgID", "Shoal");
    /// ```
    pub fn new<E: Into<String>>(endpoint: E) -> Self {
        OtlpTracing {
            endpoint: endpoint.into(),
            headers: HashMap::default(),
            timeout_secs: None,
            batch_delay_ms: None,
            max_queue_size: None,
            sample_ratio: None,
        }
    }

    /// Add a header to send with every export
    ///
    /// # Arguments
    ///
    /// * `key` - The header name to set
    /// * `value` - The value to set it to
    pub fn header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        // add this header to the ones we already have
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set how long a single export may take before it is abandoned
    ///
    /// # Arguments
    ///
    /// * `secs` - The timeout in seconds
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }

    /// Set how often the batch processor ships whatever it has
    ///
    /// # Arguments
    ///
    /// * `millis` - The delay between exports in milliseconds
    pub fn batch_delay_ms(mut self, millis: u64) -> Self {
        self.batch_delay_ms = Some(millis);
        self
    }

    /// Set how many spans may be queued before new ones are dropped
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum number of queued spans
    pub fn max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = Some(size);
        self
    }

    /// Set what fraction of traces to export
    ///
    /// # Arguments
    ///
    /// * `ratio` - The fraction to export, between 0.0 and 1.0
    pub fn sample_ratio(mut self, ratio: f64) -> Self {
        self.sample_ratio = Some(ratio);
        self
    }
}

/// The settings for an OTLP metrics sink
///
/// Metrics are exported as protobuf over HTTP, to the same collector port spans go to and a
/// different path. Nothing in `shoal-core` builds this pipeline — the type lives here because the
/// configuration does, and the process that has metrics worth shipping owns the exporter.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct OtlpMetrics {
    /// The full URL to POST metrics to, including the `/v1/metrics` path
    pub endpoint: String,
    /// Any extra headers to send with every export
    ///
    /// The same tenancy rule [`OtlpTracing::headers`] describes applies here.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// How often to ship whatever has been recorded, in seconds
    #[serde(default)]
    pub interval_secs: Option<u64>,
    /// How long a single export may take before it is abandoned, in seconds
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl OtlpMetrics {
    /// Creates a new OTLP metrics sink pointed at an endpoint
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The full URL to POST metrics to, including the `/v1/metrics` path
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::conf::OtlpMetrics;
    ///
    /// OtlpMetrics::new("http://127.0.0.1:4318/v1/metrics").interval_secs(10);
    /// ```
    pub fn new<E: Into<String>>(endpoint: E) -> Self {
        OtlpMetrics {
            endpoint: endpoint.into(),
            headers: HashMap::default(),
            interval_secs: None,
            timeout_secs: None,
        }
    }

    /// Add a header to send with every export
    ///
    /// # Arguments
    ///
    /// * `key` - The header name to set
    /// * `value` - The value to set it to
    pub fn header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        // add this header to the ones we already have
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set how often to ship whatever has been recorded
    ///
    /// # Arguments
    ///
    /// * `secs` - The interval between exports in seconds
    pub fn interval_secs(mut self, secs: u64) -> Self {
        self.interval_secs = Some(secs);
        self
    }

    /// Set how long a single export may take before it is abandoned
    ///
    /// # Arguments
    ///
    /// * `secs` - The timeout in seconds
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }
}

/// The settings for different remote tracing sinks (not stdout)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RemoteTracing {
    /// The settings for an OTLP over HTTP tracing sink
    Otlp(OtlpTracing),
    /// The old name for an OTLP over HTTP sink, carrying just an endpoint
    ///
    /// This never spoke gRPC. It is kept so a config written against the old name still parses,
    /// and it resolves to the same exporter [`RemoteTracing::Otlp`] does.
    Grpc(String),
}

impl RemoteTracing {
    /// Get the OTLP settings this sink resolves to
    ///
    /// The deprecated `Grpc` spelling carries an endpoint and nothing else, so it widens into a
    /// default [`OtlpTracing`] rather than being handled separately everywhere.
    pub fn otlp(&self) -> OtlpTracing {
        match self {
            // already the right shape
            RemoteTracing::Otlp(otlp) => otlp.clone(),
            // widen the endpoint only spelling into the full settings
            RemoteTracing::Grpc(endpoint) => OtlpTracing::new(endpoint),
        }
    }
}

/// The tracing settings for Shoal
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Tracing {
    /// The level to log traces at
    #[serde(default)]
    pub level: TraceLevel,
    /// The settings for sending traces to a remote sink
    pub remote: Option<RemoteTracing>,
    /// The settings for sending metrics to a remote sink
    ///
    /// Left unset when [`Tracing::remote`] names a sink, [`Tracing::metrics_sink`] derives one
    /// from it, because a collector almost always takes both on the same host and port.
    #[serde(default)]
    pub metrics: Option<OtlpMetrics>,
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

    /// Set an OTLP over HTTP remote tracing sink
    ///
    /// # Arguments
    ///
    /// * `otlp` - The OTLP settings to export with
    pub fn otlp(mut self, otlp: OtlpTracing) -> Self {
        self.remote = Some(RemoteTracing::Otlp(otlp));
        self
    }

    /// Set an OTLP over HTTP remote tracing sink from an endpoint alone
    ///
    /// This is the old spelling and is kept for callers that already use it. It exports over
    /// HTTP, not gRPC.
    pub fn grpc(mut self, endpoint: impl Into<String>) -> Self {
        self.remote = Some(RemoteTracing::Otlp(OtlpTracing::new(endpoint)));
        self
    }

    /// Set an OTLP over HTTP metrics sink
    ///
    /// # Arguments
    ///
    /// * `metrics` - The OTLP metrics settings to export with
    pub fn metrics(mut self, metrics: OtlpMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get the metrics sink these settings resolve to
    ///
    /// An explicit [`Tracing::metrics`] wins. Failing that, a trace sink implies one on the same
    /// collector: the two paths differ only in their last segment, and a config naming one endpoint
    /// twice is a config with two places to forget to change. Returns `None` when neither is set,
    /// which is what leaves the pipeline uninstalled.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::conf::Tracing;
    ///
    /// let derived = Tracing::default().grpc("http://127.0.0.1:4318/v1/traces");
    /// assert_eq!(
    ///     derived.metrics_sink().unwrap().endpoint,
    ///     "http://127.0.0.1:4318/v1/metrics",
    /// );
    /// // and nothing configured stays nothing configured
    /// assert!(Tracing::default().metrics_sink().is_none());
    /// ```
    pub fn metrics_sink(&self) -> Option<OtlpMetrics> {
        // an explicit sink is the one that was asked for
        if let Some(metrics) = &self.metrics {
            return Some(metrics.clone());
        }
        // otherwise derive one from the trace sink, keeping its headers so a tenant carries over
        let otlp = self.remote.as_ref()?.otlp();
        let endpoint = match otlp.endpoint.strip_suffix(TRACES_PATH) {
            // the usual shape, so swap the path a collector serves spans on for the metrics one
            Some(base) => format!("{base}{METRICS_PATH}"),
            // an endpoint that does not end in the path we expect is not one to rewrite blindly
            None => return None,
        };
        Some(OtlpMetrics {
            endpoint,
            headers: otlp.headers.clone(),
            interval_secs: None,
            timeout_secs: otlp.timeout_secs,
        })
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
    use super::{
        AuthMechanism, Conf, OtlpTracing, PathBuf, RemoteTracing, Resources, TraceLevel,
        DEFAULT_ITERATIONS, DEFAULT_MAX_FRAME_BYTES,
    };

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
    /// A config with no tls section produces a plaintext listener
    ///
    /// The same case, and the same reason, as `auth_defaults_to_off` below: this is what every
    /// deployment before encryption was in, and what the benchmark harness and the integration
    /// suite are in. A default that encrypted would make every capture incomparable against the
    /// frozen baseline without anything saying so.
    fn tls_defaults_to_off() {
        // a config that says nothing about networking at all
        let (_dir, conf) = load("resources:\n  memory: \"4Gi\"\n");
        let conf = conf.expect("a config with no networking section failed to load");
        assert!(conf.networking.tls.is_none());
        // and one that configures networking without mentioning tls
        let (_dir, conf) = load("networking:\n  port: 13000\n");
        let conf = conf.expect("a config with a partial networking section failed to load");
        assert!(conf.networking.tls.is_none());
    }

    #[test]
    /// A config that names a certificate and key produces a TLS listener
    fn a_tls_section_is_read() {
        // both paths are required, and neither is resolved until the server starts
        let (_dir, conf) = load(
            "networking:\n  tls:\n    cert: \"/etc/shoal/server.pem\"\n    key: \"/etc/shoal/server.key\"\n",
        );
        let conf = conf.expect("a config naming a certificate failed to load");
        let tls = conf.networking.tls.expect("the tls section was dropped");
        assert_eq!(tls.cert, PathBuf::from("/etc/shoal/server.pem"));
        assert_eq!(tls.key, PathBuf::from("/etc/shoal/server.key"));
    }

    #[test]
    /// A tls section missing half of its pair is refused rather than half applied
    fn a_tls_section_needs_both_a_certificate_and_a_key() {
        // a certificate with no key cannot make a server, so this has to fail while parsing
        let (_dir, conf) = load("networking:\n  tls:\n    cert: \"/etc/shoal/server.pem\"\n");
        assert!(
            conf.is_err(),
            "a tls section with no key should not have parsed"
        );
    }

    #[test]
    /// A misspelled networking key is refused rather than ignored
    ///
    /// This is the test that makes `deny_unknown_fields` on `Networking` load bearing. Without it
    /// a config that meant to say `tls:` and said something else produces a server that starts,
    /// listens, and serves every query in clear. A typo has to be a startup failure.
    fn a_misspelled_networking_key_is_refused() {
        // the shape of the mistake that matters - close enough to be plausible
        let (_dir, conf) = load(
            "networking:\n  tsl:\n    cert: \"/etc/shoal/server.pem\"\n    key: \"/etc/shoal/server.key\"\n",
        );
        assert!(
            conf.is_err(),
            "a misspelled tls section should not have parsed"
        );
    }

    #[test]
    /// A misspelled key inside the tls section is refused too
    fn a_misspelled_tls_key_is_refused() {
        // `deny_unknown_fields` on TlsServerOptions is what catches this one
        let (_dir, conf) = load(
            "networking:\n  tls:\n    cert: \"/etc/shoal/server.pem\"\n    key: \"/k\"\n    ca: \"/ca.pem\"\n",
        );
        assert!(
            conf.is_err(),
            "a tls section with an unknown key should not have parsed"
        );
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

    #[test]
    /// The old `Grpc` spelling still loads, and still means an OTLP over HTTP sink
    ///
    /// Every config written before the variant was renamed uses this spelling, including the one
    /// checked into this repository. Dropping it would turn a working deployment's config into a
    /// parse error on upgrade.
    fn the_deprecated_grpc_spelling_still_loads() {
        // load a config written the old way
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  level: Info\n  remote:\n    Grpc: \"http://127.0.0.1:4318/v1/traces\"\n",
        );
        let conf = conf.expect("a config using the old Grpc spelling failed to load");
        // it has to still be a remote sink
        let remote = conf.tracing.remote.expect("the remote sink was dropped");
        assert!(matches!(remote, RemoteTracing::Grpc(_)));
        // and it has to widen into the same OTLP settings the new spelling produces
        let otlp = remote.otlp();
        assert_eq!(otlp.endpoint, "http://127.0.0.1:4318/v1/traces");
        assert!(otlp.headers.is_empty());
    }

    #[test]
    /// The `Otlp` spelling carries the headers a multi tenant collector needs
    ///
    /// The tenant used to be compiled into `trace.rs` as `X-Scope-OrgID: Shoal`, which is how
    /// spans reach a tenant nobody queries. It belongs in the config.
    fn an_otlp_sink_carries_its_headers() {
        // load a config naming a tenant
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  level: Info\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n      headers:\n        X-Scope-OrgID: Shoal\n      batch_delay_ms: 500\n",
        );
        let conf = conf.expect("a config naming an OTLP sink failed to load");
        // the tenant has to survive the round trip
        let otlp = conf
            .tracing
            .remote
            .expect("the remote sink was dropped")
            .otlp();
        assert_eq!(otlp.headers.get("X-Scope-OrgID").map(String::as_str), Some("Shoal"));
        assert_eq!(otlp.batch_delay_ms, Some(500));
    }

    #[test]
    /// A misspelled OTLP key is rejected rather than silently dropped
    ///
    /// The same reasoning as `misspelled_resource_key_is_rejected`: a typo in a tenant header or
    /// a batch delay would otherwise export to the wrong place and say nothing about it.
    fn a_misspelled_otlp_key_is_rejected() {
        // `endpoints` is not a field
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n      endpoints: 3\n",
        );
        let error = conf.expect_err("a misspelled OTLP key was accepted");
        assert!(
            error.to_string().contains("endpoints"),
            "the error did not name the offending key: {error}"
        );
    }

    #[test]
    /// `Tracing::grpc` builds an OTLP sink, because it never spoke gRPC
    ///
    /// The builder kept its old name so callers do not break, but it must not keep the old
    /// variant, or the config a program builds in memory would disagree with the one it writes.
    fn the_grpc_builder_builds_an_otlp_sink() {
        // build a config the way a program embedding shoal does
        let tracing = super::Tracing::default()
            .level(TraceLevel::Debug)
            .grpc("http://127.0.0.1:4318/v1/traces");
        // it has to be the OTLP variant
        let remote = tracing.remote.expect("the builder set no remote sink");
        assert!(matches!(remote, RemoteTracing::Otlp(_)));
        assert_eq!(remote.otlp().endpoint, "http://127.0.0.1:4318/v1/traces");
    }

    #[test]
    /// An OTLP sink built in memory carries what it was given
    fn an_otlp_sink_builds_from_its_setters() {
        // build one with every knob set
        let otlp = OtlpTracing::new("http://127.0.0.1:4318/v1/traces")
            .header("X-Scope-OrgID", "Shoal")
            .timeout_secs(3)
            .batch_delay_ms(250)
            .max_queue_size(64);
        assert_eq!(otlp.headers.get("X-Scope-OrgID").map(String::as_str), Some("Shoal"));
        assert_eq!(otlp.timeout_secs, Some(3));
        assert_eq!(otlp.batch_delay_ms, Some(250));
        assert_eq!(otlp.max_queue_size, Some(64));
    }

    #[test]
    /// A sample ratio parses, and its absence means every trace
    ///
    /// `OtlpTracing` denies unknown fields, so this name is part of the config contract: a
    /// deployment that writes `sample_ratio` and gets a parse error would be told its whole
    /// tracing section is wrong rather than that one key is.
    fn a_sample_ratio_parses_and_defaults_to_none() {
        // a config that names one
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n      sample_ratio: 0.001\n",
        );
        let conf = conf.expect("a config naming a sample ratio failed to load");
        let otlp = conf
            .tracing
            .remote
            .expect("the remote sink was dropped")
            .otlp();
        assert_eq!(otlp.sample_ratio, Some(0.001));
        // and one that does not, which has to mean every trace rather than none of them
        let (_dir, bare) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n",
        );
        let bare = bare.expect("a config without a sample ratio failed to load");
        assert_eq!(
            bare.tracing
                .remote
                .expect("the remote sink was dropped")
                .otlp()
                .sample_ratio,
            None
        );
    }

    #[test]
    /// A metrics sink parses, with its own endpoint and interval
    fn a_metrics_sink_parses() {
        // a config naming both sinks explicitly
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n  metrics:\n    endpoint: \"http://collector:4318/v1/metrics\"\n    interval_secs: 5\n",
        );
        let conf = conf.expect("a config naming a metrics sink failed to load");
        // the explicit sink is the one that wins, on a different host to prove it was not derived
        let metrics = conf.tracing.metrics_sink().expect("the metrics sink was dropped");
        assert_eq!(metrics.endpoint, "http://collector:4318/v1/metrics");
        assert_eq!(metrics.interval_secs, Some(5));
    }

    #[test]
    /// A metrics endpoint is derived from the trace one when the config names only that
    ///
    /// A collector takes both on the same host and port, so requiring the endpoint twice is
    /// requiring two places to forget to change. The tenant header carries over with it, because
    /// metrics landing in a tenant nobody queries is the same failure spans used to have.
    fn a_metrics_endpoint_defaults_from_the_trace_one() {
        // a config naming a trace sink and nothing else
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  remote:\n    Otlp:\n      endpoint: \"http://127.0.0.1:4318/v1/traces\"\n      headers:\n        X-Scope-OrgID: Shoal\n",
        );
        let conf = conf.expect("a config naming only a trace sink failed to load");
        let metrics = conf.tracing.metrics_sink().expect("no metrics sink was derived");
        assert_eq!(metrics.endpoint, "http://127.0.0.1:4318/v1/metrics");
        assert_eq!(metrics.headers.get("X-Scope-OrgID").map(String::as_str), Some("Shoal"));
        // and a config naming no sink at all derives nothing, which is what leaves it uninstalled
        assert!(super::Tracing::default().metrics_sink().is_none());
    }

    #[test]
    /// An endpoint that is not a trace path is not rewritten into a metrics one
    ///
    /// Guessing at a URL whose shape we do not recognize would export to a path the collector
    /// does not serve, and the exporter reports that as a failure per batch rather than once.
    fn an_unrecognized_endpoint_derives_no_metrics_sink() {
        // a sink whose path is not the one a rewrite knows how to move
        let tracing = super::Tracing::default().grpc("http://127.0.0.1:4318/ingest");
        assert!(tracing.metrics_sink().is_none());
    }

    #[test]
    /// A misspelled metrics key is rejected rather than silently dropped
    fn a_misspelled_metrics_key_is_rejected() {
        // `interval` is not a field, `interval_secs` is
        let (_dir, conf) = load(
            "resources:\n  memory: \"4Gi\"\ntracing:\n  metrics:\n    endpoint: \"http://127.0.0.1:4318/v1/metrics\"\n    interval: 5\n",
        );
        let error = conf.expect_err("a misspelled metrics key was accepted");
        assert!(
            error.to_string().contains("interval"),
            "the error did not name the offending key: {error}"
        );
    }
}
