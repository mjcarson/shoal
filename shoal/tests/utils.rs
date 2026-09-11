//! The shared utilities for tests in Shoal
//!
//! Each integration test binary pulls this in with `mod utils;` and uses a different
//! subset of it, and cargo also builds this file as a test target of its own, so
//! anything here looks dead from somewhere.
#![allow(dead_code)]

use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{Archive, Deserialize};
use shoal::client::{Errors, Shoal};
use shoal::server::conf::{
    Auth, Conf, DefaultStorageSettings, Networking, Resources, Storage,
};
use shoal::server::ServerError;
use shoal::shared::queries::Queries;
use shoal::shared::tls::TlsClientOptions;
use shoal::ShoalDatabase;
use shoal::shared::traits::QuerySupport;
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::ShoalPool;
use std::time::Duration;
use tempfile::TempDir;

/// Error type for tests
#[derive(Debug)]
pub enum TestError {
    Server(ServerError),
    Client(Errors),
    /// A raw socket a test opened alongside a client failed
    ///
    /// The framing tests talk to the server without going through a client, so they hit
    /// `std::io::Error` directly rather than through either of the two above.
    Io(std::io::Error),
}

impl From<ServerError> for TestError {
    fn from(e: ServerError) -> Self {
        TestError::Server(e)
    }
}

impl From<std::io::Error> for TestError {
    fn from(e: std::io::Error) -> Self {
        TestError::Io(e)
    }
}

impl From<Errors> for TestError {
    fn from(e: Errors) -> Self {
        TestError::Client(e)
    }
}

/// Create a temp dir for a test on a filesystem that supports direct IO
///
/// `TempDir::new` uses `/tmp`, which is usually tmpfs. Glommio silently disables
/// O_DIRECT on tmpfs, so any test using it exercises a buffered write path where
/// alignment is not enforced and `fdatasync` is meaningless. `CARGO_TARGET_TMPDIR`
/// lives under `target/`, which is on the same real filesystem as the repo.
pub fn test_dir() -> TempDir {
    // build our temp dir under cargo's target dir so we get a real filesystem
    TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).expect("Failed to create temp dir")
}

/// Create a default config for tests
pub fn build_config(temp_dir: &TempDir) -> Conf {
    // get a random port to bind to
    // any port: `ShoalPool::start` resolves zero to a real one before its shards bind, and
    // `ready` reports it, so two test binaries can never be handed the same number (item 38)
    let port = 0;
    // build a default test conf
    Conf::default()
        .resources(
            Resources::default()
                .cores(2)
                .memory("100MiB")
                .expect("Failed to set memory to 100MiB"),
        )
        .networking(Networking::default().port(port))
        .storage(
            Storage::default().default_settings(
                DefaultStorageSettings::default().filesystem(
                    FileSystemTableConf::default()
                        .latency_sensitive(
                            FileSystemLatencyWriterConf::default().path(temp_dir.path()),
                        )
                        .throughput_sensitive(
                            FileSystemThroughputWriterConf::default().path(temp_dir.path()),
                        ),
                ),
            ),
        )
}

/// Create a config that keeps a shard under constant memory pressure
///
/// Eviction is checked once per shard loop iteration against `resources.memory`, so a
/// one byte limit makes every iteration evict everything the LRU is holding. That is
/// the only way to test that a partition is not marked evictable before its changes
/// have been compacted, since nothing else forces an eviction.
///
/// The intent log is shrunk at the same time so generations advance every few writes
/// instead of every 10 MiB; a partition can then be mutated in one generation and
/// marked by the compaction of an earlier one.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir to store this servers data in
pub fn build_pressured_config(temp_dir: &TempDir) -> Conf {
    // start from the default test config
    let mut conf = build_config(temp_dir);
    // evict on every shard loop iteration
    conf.resources.memory = 1;
    // rotate the intent log every 4 KiB so generations advance quickly
    conf.storage
        .default
        .filesystem
        .latency_sensitive
        .intent_log_size = 4 << 10;
    conf
}

/// Create a config that runs every partition on a single shard
///
/// A get naming several partition keys is fanned out to the shards that own those
/// keys, so with more than one shard a multi partition test depends on how the keys
/// happen to hash. Running a single shard makes that deterministic.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir to store this servers data in
pub fn build_single_shard_config(temp_dir: &TempDir) -> Conf {
    // start from the default test config
    let mut conf = build_config(temp_dir);
    // run a single shard so every partition key lands on it
    conf.resources.cores = Some(1);
    conf
}

/// A certificate and key on disk, and the client options that trust them
///
/// The files live inside the caller's temp dir, so they go away with it and nothing expires. That
/// is the reason these are generated rather than committed: a fixture with a hard expiry date
/// fails the suite years from now for a reason nobody will connect to this file.
pub struct TestCertificate {
    /// The PEM file holding the certificate
    pub cert: std::path::PathBuf,
    /// The PEM file holding its key
    pub key: std::path::PathBuf,
}

impl TestCertificate {
    /// Generate a self signed certificate for `localhost` inside a temp dir
    ///
    /// # Arguments
    ///
    /// * `temp_dir` - The temp dir to write the certificate and key into
    pub fn new(temp_dir: &TempDir) -> Self {
        use std::io::Write;

        // one throwaway certificate, valid for the name and the address a test connects to
        let issued = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
        ])
        .expect("failed to generate a certificate");
        let cert = temp_dir.path().join("cert.pem");
        let key = temp_dir.path().join("key.pem");
        std::fs::File::create(&cert)
            .expect("failed to create a certificate file")
            .write_all(issued.cert.pem().as_bytes())
            .expect("failed to write a certificate");
        std::fs::File::create(&key)
            .expect("failed to create a key file")
            .write_all(issued.key_pair.serialize_pem().as_bytes())
            .expect("failed to write a key");
        TestCertificate { cert, key }
    }

    /// The client options that trust this certificate
    ///
    /// The certificate is its own authority, since it is self signed, and the name asked for is
    /// the one it carries rather than the loopback address a test connects to.
    pub fn client_options(&self) -> TlsClientOptions {
        TlsClientOptions::new(&self.cert).server_name("localhost")
    }
}

/// Whether this machine can do kTLS at all
///
/// `setsockopt` does not autoload the kernel's `tls` module, so a machine that has never used it
/// answers `ENOENT`. Tests that need it say so and skip loudly rather than failing, which is the
/// same treatment the `stage-profile` tests get for being outside a default run.
pub fn ktls_available() -> bool {
    shoal::shared::tls::ktls::is_available()
}

/// Skip a test with a message naming what would make it run
///
/// # Arguments
///
/// * `test` - The name of the test being skipped
#[macro_export]
macro_rules! skip_without_ktls {
    ($test:literal) => {
        if !utils::ktls_available() {
            eprintln!(
                "SKIPPING {}: the 'tls' kernel module is not loaded. run 'sudo modprobe tls'",
                $test
            );
            return Ok(());
        }
    };
}

/// Create a config for a server that encrypts every connection
///
/// # Arguments
///
/// * `temp_dir` - The temp dir to store this servers data in
/// * `cert` - The certificate this server proves itself with
pub fn build_tls_config(temp_dir: &TempDir, cert: &TestCertificate) -> Conf {
    // start from the default test config and turn encryption on
    let conf = build_config(temp_dir);
    let port = conf.networking.port;
    conf.networking(Networking::default().port(port).tls(&cert.cert, &cert.key))
}

/// Create a config for a server that encrypts and requires one user to authenticate
///
/// This is the pair the two features are meant to be deployed as, and the one D3 argues for: SCRAM
/// over a plaintext link shows an observer the username and the whole exchange.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir to store this servers data in
/// * `cert` - The certificate this server proves itself with
/// * `username` - The one user this server will accept
/// * `password` - The password that user authenticates with
pub fn build_tls_auth_config(
    temp_dir: &TempDir,
    cert: &TestCertificate,
    username: &str,
    password: &str,
) -> Conf {
    // both sections at once, since neither implies the other
    build_tls_config(temp_dir, cert).auth(Auth::default().required(true).user(username, password))
}

/// Create a config for a server that requires one user to authenticate
///
/// The password is named rather than a derived credential, so the config derives it at startup —
/// which is also the path a test wants exercised, since it is the one an operator will use first.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir to store this servers data in
/// * `username` - The one user this server will accept
/// * `password` - The password that user authenticates with
pub fn build_auth_config(temp_dir: &TempDir, username: &str, password: &str) -> Conf {
    // start from the default test config and turn authentication on
    build_config(temp_dir).auth(Auth::default().required(true).user(username, password))
}

/// Setup and start a default shoal server/config
pub async fn start<T: ShoalDatabase>(
    temp_dir: &TempDir,
) -> Result<(Shoal<T::ClientType>, ShoalPool<T>), TestError>
where
    // Bounds for ShoalPool impl block
    <<T::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: Deserialize<
        <T::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<T::ClientType> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    // Bounds for ShoalPool::start
    for<'a> <<T::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    // Bounds for Shoal::new
    for<'a> <<T::ClientType as QuerySupport>::ResponseKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // get a config for this test
    let conf = build_config(&temp_dir);
    // start a server with it
    start_with_conf::<T>(conf).await
}

/// Setup and start a shoal server from an existing config
///
/// # Arguments
///
/// * `conf` - The config to start this server with
pub async fn start_with_conf<T: ShoalDatabase>(
    conf: Conf,
) -> Result<(Shoal<T::ClientType>, ShoalPool<T>), TestError>
where
    // Bounds for ShoalPool impl block
    <<T::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: Deserialize<
        <T::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<T::ClientType> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    // Bounds for ShoalPool::start
    for<'a> <<T::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    // Bounds for Shoal::new
    for<'a> <<T::ClientType as QuerySupport>::ResponseKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // Start the server
    let mut pool = ShoalPool::<T>::start(conf)?;
    // wait until every shard is answering, rather than for a fixed number of seconds, and let
    // a shard that failed say so here rather than as a refused connection below (item 58)
    let addr = pool.ready(READY_TIMEOUT)?;
    // setup a client
    let client = Shoal::<T::ClientType>::new(&addr.to_string()).await?;
    Ok((client, pool))
}

/// How long a test waits for a server's shards to bind before it gives up
///
/// Generous, because a test binary runs many servers at once and a shard that is merely slow is
/// not the failure this is meant to catch. A shard that failed is reported at once regardless.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The env var naming the temp dir a crash test child should use
pub const CRASH_DIR_VAR: &str = "SHOAL_CRASH_TEST_DIR";

/// The line a crash test child prints once its writes have been acknowledged
pub const CRASH_READY_LINE: &str = "SHOAL_CRASH_TEST_READY";

/// Create a config for a crash test child, reusing a fixed dir and port
///
/// # Arguments
///
/// * `path` - The storage path to use
/// * `port` - The port to bind
pub fn build_crash_config(path: &std::path::Path, port: u16) -> Conf {
    // build a default test conf pinned to our callers dir and port
    Conf::default()
        .resources(
            Resources::default()
                .cores(2)
                .memory("100MiB")
                .expect("Failed to set memory to 100MiB"),
        )
        .networking(Networking::default().port(port))
        .storage(
            Storage::default().default_settings(
                DefaultStorageSettings::default().filesystem(
                    FileSystemTableConf::default()
                        .latency_sensitive(FileSystemLatencyWriterConf::default().path(path))
                        .throughput_sensitive(FileSystemThroughputWriterConf::default().path(path)),
                ),
            ),
        )
}

/// A guard that makes a tables archive directory unreadable while it is alive
///
/// This is how a partition read is made to fail on demand. The archives live in their own
/// directory, apart from the intent logs and the archive map, so taking the permissions off
/// it fails the open of an archive without touching the map that says which archive a
/// partition is in - which is exactly the shape of a real IO failure, and the only shape a
/// test can produce without a fault injection hook in the storage engine.
///
/// The permissions are restored when this is dropped, so the temp dir can still be cleaned
/// up if the test panics part way through.
pub struct UnreadableArchives {
    /// The archive directory whose permissions were taken away
    path: std::path::PathBuf,
    /// The permissions to put back
    original: std::fs::Permissions,
}

impl UnreadableArchives {
    /// Take the permissions off a tables archive directory
    ///
    /// Returns `None` when the archives cannot be made unreadable, which is the case when
    /// the tests are running as root - root traverses a directory whatever its mode says,
    /// so there would be no failure to observe.
    ///
    /// # Arguments
    ///
    /// * `temp_dir` - The temp dir this servers data lives in
    /// * `table_name` - The name of the table whose archives to hide
    pub fn new(temp_dir: &TempDir, table_name: &str) -> Option<Self> {
        use std::os::unix::fs::PermissionsExt;
        // build the path to this tables archives
        let path = temp_dir.path().join(table_name).join("archives");
        // remember the permissions we are about to take away
        let original = std::fs::metadata(&path).ok()?.permissions();
        // take every permission off this directory so opening an archive in it fails
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).ok()?;
        // check that this actually made the archives unreadable, which it does not for root
        if std::fs::read_dir(&path).is_ok() {
            // put the permissions back, since they are doing nothing
            std::fs::set_permissions(&path, original).ok()?;
            return None;
        }
        Some(UnreadableArchives { path, original })
    }
}

impl Drop for UnreadableArchives {
    /// Put the archive directorys permissions back
    fn drop(&mut self) {
        // restore the permissions so the temp dir can be cleaned up
        let _ = std::fs::set_permissions(&self.path, self.original.clone());
    }
}

/// A guard that moves a tables archive files out of the way while it is alive
///
/// This is a different failure to [`UnreadableArchives`] and has to be made a different way.
/// That guard leaves the archives where they are and refuses the open, which is an IO error.
/// This one leaves the archive directory perfectly readable and takes the files out of it, so
/// the open of a named archive finds nothing - which is what a read holding an archive entry
/// from before a compaction re-pointed it sees, since the compactor deletes an archive once it
/// has rewritten what was still live in it.
///
/// Unlike [`UnreadableArchives`] this works as root, because it changes what is on disk rather
/// than who may look at it.
///
/// The files are moved back when this is dropped, so a test that panics part way through still
/// leaves a temp dir that can be cleaned up.
pub struct MissingArchives {
    /// The archive directory the files were taken out of
    archives: std::path::PathBuf,
    /// The directory the files were moved into
    hidden: std::path::PathBuf,
    /// The file names that were moved, so they can be moved back
    moved: Vec<std::ffi::OsString>,
}

impl MissingArchives {
    /// Move every archive file of a table out of its archive directory
    ///
    /// Only regular files are moved - the archive directory also holds an `intents`
    /// subdirectory, and moving that would break the map rather than the archives.
    ///
    /// # Arguments
    ///
    /// * `temp_dir` - The temp dir this servers data lives in
    /// * `table_name` - The name of the table whose archives to take away
    pub fn new(temp_dir: &TempDir, table_name: &str) -> Self {
        // build the path to this tables archives
        let archives = temp_dir.path().join(table_name).join("archives");
        // build a directory beside it to move the archives into
        let hidden = temp_dir.path().join(table_name).join("archives-hidden");
        // make the directory we are about to move the archives into
        std::fs::create_dir_all(&hidden).expect("Failed to create the hidden archive dir");
        // track every file we move so it can be moved back
        let mut moved = Vec::new();
        // crawl over everything in this tables archive directory
        for entry in std::fs::read_dir(&archives).expect("Failed to read the archive dir") {
            // get this entry
            let entry = entry.expect("Failed to read an archive dir entry");
            // skip anything that is not a regular file, since intents live in here too
            if !entry
                .file_type()
                .expect("Failed to stat an archive")
                .is_file()
            {
                continue;
            }
            // get this archives name
            let name = entry.file_name();
            // move this archive out of the directory a read will look in
            std::fs::rename(entry.path(), hidden.join(&name)).expect("Failed to hide an archive");
            // remember it so it can be moved back
            moved.push(name);
        }
        MissingArchives {
            archives,
            hidden,
            moved,
        }
    }

    /// Check whether any of the archives that were moved away has come back
    ///
    /// A read that creates the archive it could not find leaves an empty file behind at the
    /// name it looked for, so a name reappearing here is that stray archive.
    pub fn recreated(&self) -> Vec<std::path::PathBuf> {
        // collect every name we moved away that now exists again
        self.moved
            .iter()
            .map(|name| self.archives.join(name))
            .filter(|path| path.exists())
            .collect()
    }
}

impl Drop for MissingArchives {
    /// Put the archive files back
    fn drop(&mut self) {
        // move every archive we took away back to where a read looks for it
        for name in &self.moved {
            // a stray archive created in its place has to go, or the rename fails
            let _ = std::fs::remove_file(self.archives.join(name));
            // move this archive back
            let _ = std::fs::rename(self.hidden.join(name), self.archives.join(name));
        }
        // drop the directory we borrowed, which is empty again now
        let _ = std::fs::remove_dir(&self.hidden);
    }
}
