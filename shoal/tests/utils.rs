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
use shoal_core::client::{Errors, Shoal};
use shoal_core::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal_core::server::ServerError;
use shoal_core::shared::queries::Queries;
use shoal_core::shared::traits::{QuerySupport, ShoalDatabase};
use shoal_core::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal_core::ShoalPool;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tempfile::TempDir;

/// Error type for tests
#[derive(Debug)]
pub enum TestError {
    Server(ServerError),
    Client(Errors),
}

impl From<ServerError> for TestError {
    fn from(e: ServerError) -> Self {
        TestError::Server(e)
    }
}

impl From<Errors> for TestError {
    fn from(e: Errors) -> Self {
        TestError::Client(e)
    }
}

/// Global port counter to ensure each test gets a unique port
static PORT_COUNTER: AtomicU16 = AtomicU16::new(13000);

/// Helper to get a unique port for each test
fn get_unique_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
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
    let port = get_unique_port();
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
    // build the address our client should connect too
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    println!("TALK TO {addr}");
    // Start the server
    let pool = ShoalPool::<T>::start(conf)?;
    // wait for our server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    // setup a client
    let client = Shoal::<T::ClientType>::new(&addr).await?;
    Ok((client, pool))
}

/// The env var naming the temp dir a crash test child should use
pub const CRASH_DIR_VAR: &str = "SHOAL_CRASH_TEST_DIR";

/// The env var naming the port a crash test child should bind
pub const CRASH_PORT_VAR: &str = "SHOAL_CRASH_TEST_PORT";

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
