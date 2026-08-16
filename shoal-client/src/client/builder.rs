//! How a client is described before it is built
//!
//! Everything a caller can say about a client that is not a query. Before this, each of those
//! things arrived as another constructor: [`Shoal::new`] took an address,
//! [`Shoal::with_credentials`] took an address and credentials, and
//! [`Shoal::with_options`] took an address and a [`ClientOptions`] holding the two things that
//! had accumulated by then. `addr × credentials × tls` is already three combinations, and
//! deadlines, pool sizing and a health check are three more axes that would multiply it again.
//!
//! [`ClientOptions`] is kept and still works. It is what this absorbs rather than replaces: a
//! caller holding one hands it to [`ShoalBuilder::options`] and adds whatever else it wants.
//!
//! [`Shoal::new`]: super::Shoal::new
//! [`Shoal::with_credentials`]: super::Shoal::with_credentials
//! [`Shoal::with_options`]: super::Shoal::with_options

use std::collections::HashSet;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use rkyv::rancor::Strategy;
use rkyv::Archive;
use shoal_proto::shared::auth::Credentials;
use shoal_proto::shared::tls::TlsClientOptions;
use shoal_proto::shared::traits::QuerySupport;
use tracing::{event, Level};

use super::{ClientOptions, Errors, Shoal};

/// How the pool of connections underneath a client is sized and aged
///
/// These are `bb8`'s own settings and are named after them, so that a reader who knows the pool
/// crate does not have to learn a second vocabulary for the same five numbers. The defaults are
/// exactly the literals every client was built with before this was configurable, which is what
/// makes adding a builder a change no existing caller can observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// How many connections to keep open when nothing is being sent
    ///
    /// Each of these shakes hands when the client is built, and — against a server that asks for
    /// it — authenticates. A cold client therefore pays this many handshakes concurrently.
    pub min_idle: u32,
    /// The most connections this client will open at once
    pub max_size: u32,
    /// How long a caller waits for a connection before giving up
    ///
    /// This bounds `bb8`'s retry loop and its checkout, and **not** the handshake inside one
    /// connection — that is [`Deadlines::handshake`]. A permanent refusal such as a schema
    /// mismatch is retried until this elapses, which is why such an error takes seconds to
    /// arrive rather than milliseconds.
    pub connection_timeout: Duration,
    /// How long an unused connection is kept before it is closed
    pub idle_timeout: Option<Duration>,
    /// How long any connection is kept, used or not
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    /// The pool every client had before it could be configured
    fn default() -> Self {
        PoolConfig {
            min_idle: 10,
            max_size: 50,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Some(Duration::from_secs(300)),
            max_lifetime: Some(Duration::from_secs(1800)),
        }
    }
}

impl PoolConfig {
    /// Check that this pool describes something that can be built
    ///
    /// A minimum above a maximum is a pool `bb8` would never satisfy, and it is worth refusing
    /// here rather than letting a client come up that can never reach its own floor.
    fn validate(&self) -> Result<(), Errors> {
        // a pool that can never reach its own floor is a pool nobody asked for
        if self.min_idle > self.max_size {
            return Err(Errors::Config(format!(
                "a pool cannot keep {} connections idle when it may only open {}",
                self.min_idle, self.max_size
            )));
        }
        // a pool with no connections cannot answer anything
        if self.max_size == 0 {
            return Err(Errors::Config(
                "a pool that may open no connections cannot send a query".to_owned(),
            ));
        }
        Ok(())
    }
}

/// How long each part of a client's work is given before it is given up on
///
/// This holds one deadline today and is a struct rather than an argument because it is the seam
/// the rest of them land on — a per query deadline and a read idle deadline both belong here, and
/// both change what a caller must handle rather than only what it may configure.
///
/// Note this deliberately does **not** hold the pool's `connection_timeout`. That is a `bb8`
/// setting bounding a checkout, it lives on [`PoolConfig`] beside the other four, and calling
/// both of them "connect" is how a reader ends up believing one bounds the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    /// How long a server has to finish its half of opening a connection
    ///
    /// This covers **all three** handshakes a connection can have: the TLS one, the Shoal one,
    /// and the authentication exchange. A peer that stalls between any two of them holds exactly
    /// as much of this client as one that stalls before all three.
    ///
    /// `bb8`'s [`PoolConfig::connection_timeout`] bounds its retry loop and its checkout, not the
    /// connect itself, so without this a server that accepts a connection and then says nothing
    /// would park a client being built forever.
    pub handshake: Duration,
}

impl Default for Deadlines {
    /// The deadlines every client had before they could be configured
    fn default() -> Self {
        Deadlines {
            // generous compared to a handshake that is one 24 byte write and one 24 byte read,
            // and deliberately so - this is here to bound a stalled peer, not to police a slow one
            handshake: Duration::from_secs(10),
        }
    }
}

/// Resolve a list of endpoints, in the order they were given
///
/// A name is expanded to **every** address it resolves to rather than to the first one, which is
/// the difference between a client that has a second server to try and one that does not. Before
/// this, a client resolved one name and took `lookup_host`'s first answer, so a name with three A
/// records behind it produced a client that only ever knew one of them.
///
/// Duplicates are dropped while order is kept, since two names resolving to one address should not
/// make that address twice as likely to be picked.
///
/// This is a free function rather than a method so that what it does can be tested without
/// standing up a schema to parameterise a builder with.
///
/// # Arguments
///
/// * `endpoints` - The addresses or names to resolve
async fn resolve_endpoints(endpoints: &[String]) -> Result<Vec<SocketAddr>, Errors> {
    // a client with nowhere to go is a caller's mistake, and is worth saying so before a
    // resolver is asked about it
    if endpoints.is_empty() {
        return Err(Errors::Config(
            "a client needs at least one endpoint to connect to".to_owned(),
        ));
    }
    // collect every address every endpoint resolves to, in the order they were given
    let mut resolved = Vec::with_capacity(endpoints.len());
    let mut seen = HashSet::with_capacity(endpoints.len());
    for endpoint in endpoints {
        // ask the resolver what this endpoint is
        let addrs = tokio::net::lookup_host(endpoint).await.map_err(|error| {
            Errors::DnsResolution(format!("failed to resolve {endpoint}: {error}"))
        })?;
        // keep every answer this endpoint gave that we have not already got
        for addr in addrs {
            if seen.insert(addr) {
                resolved.push(addr);
            }
        }
    }
    // an endpoint that resolved to nothing leaves us with nowhere to go
    if resolved.is_empty() {
        return Err(Errors::DnsResolution(format!(
            "none of {} endpoints resolved to an address",
            endpoints.len()
        )));
    }
    Ok(resolved)
}

/// Everything a client can be told before it opens a socket
///
/// # Examples
///
/// ```no_run
/// # async fn example<S: shoal_proto::shared::traits::QuerySupport>() -> Result<(), shoal_client::client::Errors>
/// # where for<'a> <<S as shoal_proto::shared::traits::QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
/// #     rkyv::bytecheck::CheckBytes<rkyv::rancor::Strategy<rkyv::validation::Validator<
/// #         rkyv::validation::archive::ArchiveValidator<'a>,
/// #         rkyv::validation::shared::SharedValidator>, rkyv::rancor::Error>> {
/// use shoal_client::client::{PoolConfig, Shoal};
/// use shoal_proto::shared::auth::Credentials;
///
/// let client = Shoal::<S>::builder()
///     .endpoints(["10.0.0.1:12000", "10.0.0.2:12000"])
///     .pool(PoolConfig {
///         max_size: 200,
///         ..PoolConfig::default()
///     })
///     .credentials(Credentials::scram("reader", "hunter2"))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ShoalBuilder<S: QuerySupport> {
    /// The servers to try, as written, before any of them have been resolved
    ///
    /// These are kept as text until [`ShoalBuilder::build`] so that a name is resolved once the
    /// client is actually being made, rather than at whatever moment the caller happened to name
    /// it.
    endpoints: Vec<String>,
    /// How to size and age the pool underneath this client
    pool: PoolConfig,
    /// How long each part of this client's work is given
    deadlines: Deadlines,
    /// What this client proves itself with and encrypts with
    options: ClientOptions,
    /// The database kind this client will query
    phantom: PhantomData<S>,
}

impl<S: QuerySupport> Default for ShoalBuilder<S> {
    /// A builder with no endpoint and every default
    fn default() -> Self {
        ShoalBuilder {
            endpoints: Vec::new(),
            pool: PoolConfig::default(),
            deadlines: Deadlines::default(),
            options: ClientOptions::new(),
            phantom: PhantomData,
        }
    }
}

impl<S: QuerySupport> ShoalBuilder<S> {
    /// Start describing a client
    pub fn new() -> Self {
        ShoalBuilder::default()
    }

    /// Add one server to try
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The address or name of a server to try
    #[must_use]
    pub fn endpoint<E: ToString>(mut self, endpoint: E) -> Self {
        // keep this as written until the client is built
        self.endpoints.push(endpoint.to_string());
        self
    }

    /// Add several servers to try
    ///
    /// These are tried in the order they are given, one connection at a time, so the first is the
    /// one a healthy client will mostly be talking to.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - The addresses or names of the servers to try
    #[must_use]
    pub fn endpoints<E: ToString, I: IntoIterator<Item = E>>(mut self, endpoints: I) -> Self {
        // keep these as written until the client is built
        self.endpoints
            .extend(endpoints.into_iter().map(|endpoint| endpoint.to_string()));
        self
    }

    /// Set how the pool underneath this client is sized and aged
    ///
    /// # Arguments
    ///
    /// * `pool` - How to size and age the pool
    #[must_use]
    pub fn pool(mut self, pool: PoolConfig) -> Self {
        self.pool = pool;
        self
    }

    /// Set how long each part of this client's work is given
    ///
    /// # Arguments
    ///
    /// * `deadlines` - How long to give each part of this client's work
    #[must_use]
    pub fn deadlines(mut self, deadlines: Deadlines) -> Self {
        self.deadlines = deadlines;
        self
    }

    /// Prove this client's identity with a username and password
    ///
    /// # Arguments
    ///
    /// * `credentials` - What to prove this client's identity with
    #[must_use]
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.options.credentials = credentials;
        self
    }

    /// Encrypt this client's connections
    ///
    /// # Arguments
    ///
    /// * `tls` - Which authority to trust, and what name to ask the server for
    #[must_use]
    pub fn tls(mut self, tls: TlsClientOptions) -> Self {
        self.options.tls = Some(tls);
        self
    }

    /// Take the credentials and encryption from an existing set of options
    ///
    /// This is how a caller that already holds a [`ClientOptions`] moves to the builder without
    /// taking its two fields apart. Anything already set by [`ShoalBuilder::credentials`] or
    /// [`ShoalBuilder::tls`] is replaced.
    ///
    /// # Arguments
    ///
    /// * `options` - What to prove this client's identity with and what to encrypt with
    #[must_use]
    pub fn options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self
    }

    /// Build the client this describes
    ///
    /// # Errors
    ///
    /// Fails before a socket is opened if this builder has no endpoint, if the pool cannot be
    /// satisfied, or if no endpoint resolves. After that it fails the way every constructor does:
    /// with whatever refused the first connection.
    pub async fn build(self) -> Result<Shoal<S>, Errors>
    where
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // refuse a pool that cannot be satisfied before anything opens a socket for it
        self.pool.validate()?;
        // work out every address this client may talk to
        let endpoints = resolve_endpoints(&self.endpoints).await?;
        // say so when encryption and several endpoints are going to disagree about names
        //
        // the tls handshake falls back to the address it dialled as the name it asks the server
        // for, so several endpoints with no explicit name means each server is asked for a
        // different one and all but the first will fail their certificate check. this is a
        // warning rather than an error because a caller may be running one certificate per host
        if endpoints.len() > 1 {
            if let Some(tls) = &self.options.tls {
                if tls.server_name.is_none() {
                    event!(
                        Level::WARN,
                        msg = "an encrypted client with several endpoints and no server name will \
                               ask each server for a different name",
                        endpoints = endpoints.len(),
                    );
                }
            }
        }
        Shoal::connect(endpoints, self.options, self.pool, self.deadlines).await
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_endpoints, Deadlines, Errors, PoolConfig, SocketAddr};
    use std::time::Duration;

    /// Turn a list of string slices into what the resolver takes
    ///
    /// # Arguments
    ///
    /// * `endpoints` - The endpoints to hand over
    fn owned(endpoints: &[&str]) -> Vec<String> {
        endpoints.iter().map(|e| (*e).to_owned()).collect()
    }

    /// Read an address, saying which type it is
    ///
    /// `rkyv` gives `SocketAddr` a second `PartialEq` against its archived form, so a bare
    /// `.parse()` compared against one has two candidate impls and infers neither.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to read
    fn addr(addr: &str) -> SocketAddr {
        addr.parse().expect("failed to read an address")
    }

    /// The defaults have to be the numbers every client was built with before they were settings
    ///
    /// This is the whole safety argument for adding a builder: if these five drift, then making
    /// the pool configurable silently reconfigured every caller that never asked for anything.
    #[test]
    fn the_pool_defaults_are_the_numbers_that_were_hardcoded() {
        let pool = PoolConfig::default();
        assert_eq!(pool.min_idle, 10);
        assert_eq!(pool.max_size, 50);
        assert_eq!(pool.connection_timeout, Duration::from_secs(5));
        assert_eq!(pool.idle_timeout, Some(Duration::from_secs(300)));
        assert_eq!(pool.max_lifetime, Some(Duration::from_secs(1800)));
        // and the handshake deadline that used to be a constant beside them
        assert_eq!(Deadlines::default().handshake, Duration::from_secs(10));
    }

    /// A pool that can never reach its own floor is refused rather than built
    #[test]
    fn a_pool_that_cannot_be_satisfied_is_refused() {
        let pool = PoolConfig {
            min_idle: 60,
            max_size: 50,
            ..PoolConfig::default()
        };
        assert!(matches!(pool.validate(), Err(Errors::Config(_))));
    }

    /// A pool that may open no connections cannot answer anything
    #[test]
    fn a_pool_of_no_connections_is_refused() {
        let pool = PoolConfig {
            min_idle: 0,
            max_size: 0,
            ..PoolConfig::default()
        };
        assert!(matches!(pool.validate(), Err(Errors::Config(_))));
    }

    /// The default pool is one that can actually be built
    #[test]
    fn the_default_pool_is_valid() {
        assert!(PoolConfig::default().validate().is_ok());
    }

    /// A builder with nowhere to go says so before it asks a resolver anything
    #[tokio::test]
    async fn a_client_with_no_endpoint_is_refused() {
        let resolved = resolve_endpoints(&[]).await;
        assert!(matches!(resolved, Err(Errors::Config(_))));
    }

    /// Every endpoint is kept, in the order it was given
    #[tokio::test]
    async fn endpoints_are_resolved_in_the_order_they_were_given() {
        let resolved = resolve_endpoints(&owned(&["127.0.0.2:12001", "127.0.0.1:12000"]))
            .await
            .expect("failed to resolve two addresses");
        assert_eq!(
            resolved,
            vec![addr("127.0.0.2:12001"), addr("127.0.0.1:12000")]
        );
    }

    /// Two endpoints naming one address do not make that address twice as likely to be picked
    ///
    /// The round robin in the connection manager steps through this list, so a duplicate would
    /// send two out of every three connections to the same server.
    #[tokio::test]
    async fn an_address_given_twice_is_only_kept_once() {
        let resolved = resolve_endpoints(&owned(&[
            "127.0.0.1:12000",
            "127.0.0.2:12001",
            "127.0.0.1:12000",
        ]))
        .await
        .expect("failed to resolve three addresses");
        assert_eq!(
            resolved,
            vec![addr("127.0.0.1:12000"), addr("127.0.0.2:12001")]
        );
    }
}
