//! The shared utilities for tests in Shoal

use shoal_core::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal_core::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;

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
