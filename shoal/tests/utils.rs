//! The shared utilities for tests in Shoal

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

/// Create a default config for tests
pub fn build_config(temp_dir: &TempDir) -> Conf {
    //// get a temp dir for this config
    //let temp_dir = TempDir::new().expect("Failed to create temp dir");
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
