//! A preload waits out a shed, however long the cluster sheds a write
//! ([item 357](../../../docs/src/appendix/resolved/preload-sheds-waited-out.md))
//!
//! A shed write was turned away before anything was applied, so it is always safe to send again,
//! and it is what a cluster answers a client that outruns it, which a preload does by design. The
//! bench's preload sent one at most nine times over about two seconds, and a lab run's preload of
//! four million rows died on twenty-one inserts a cluster shed for longer. This holds a node's
//! writes of one table shed for three seconds, longer than those nine sends, and preloads through
//! it.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bench_dataset::{Catalog, CatalogClient};
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::server::faults;
use shoal::shared::identity::TableId;
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::{Shoal, ShoalPool};
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::{send_options, Driver};
use shoal_loadgen::feed::{prepare, ScanOptions};
use shoal_loadgen::progress::Progress;
use shoal_loadgen::spec::ReadLevel;
use shoal_loadgen::window::Window;

/// How long the node sheds every write of the items: longer than nine sends' backoffs together
const SHED_FOR: Duration = Duration::from_secs(3);

/// A two shard server's config, storing under cargo's target directory
///
/// # Arguments
///
/// * `dir` - Where the server stores its tables
fn conf(dir: &tempfile::TempDir) -> Conf {
    // any port, two shards, and a real filesystem for direct I/O
    Conf::default()
        .resources(
            Resources::default()
                .cores(2)
                .memory("256MiB")
                .expect("256MiB is a size"),
        )
        .networking(Networking::default().port(0))
        .storage(
            Storage::default()
                .pool("local", bench_dataset::images_pool())
                .bind("Images", "local")
                .default_settings(
                    DefaultStorageSettings::default().filesystem(
                        FileSystemTableConf::default()
                            .latency_sensitive(FileSystemLatencyWriterConf::default().path(dir.path()))
                            .throughput_sensitive(
                                FileSystemThroughputWriterConf::default().path(dir.path()),
                            ),
                    ),
                ),
        )
}

/// A preload through a node that sheds every write of a table for longer than nine sends last
/// loads every row once the node takes them again
///
/// The node sheds the items' writes for three seconds from before the preload starts, through
/// the same path a table past its pending bound takes. Against the tree before item 357's fix
/// every item the preload sent was shed on all nine of its sends, and the preload failed all
/// thousand `Shedding` in 3.0 s. Now each is sent again until the node takes it, the preload
/// says what sent them again by code, and the node counts every shed under its cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_preload_waits_out_a_shed_longer_than_its_retries() {
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // a node serving the catalog, in this process
    let mut pool = ShoalPool::<Catalog>::start(conf(&dir)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready");
    let client = Arc::new(
        Shoal::<CatalogClient>::new(&addr.to_string())
            .await
            .expect("the client connects"),
    );
    // the committed dataset, every file scanned
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset");
    let dataset = Dataset::open::<CatalogClient>(&root).expect("the dataset is usable");
    let tables: Vec<_> = dataset
        .files
        .iter()
        .map(|file| prepare::<CatalogClient>(file, &ScanOptions::default()).expect("the file scans"))
        .collect();
    let driver = Driver::new(vec![client], tables, send_options(ReadLevel::Default), 4);
    // every write of the items shed for longer than the preload's nine sends of one last
    let items = TableId::of("Item").0;
    faults::shed_writes(items, Some(SHED_FOR));
    let (seconds, took) = driver.preload(64, 256, &Progress::none()).await;
    faults::shed_writes(items, None);
    let window = Window::sum(&seconds);
    let summary = window.summary(took);
    // every row the preload sent is in, however long its first sends were shed
    assert_eq!(
        summary.insert.failed(),
        0,
        "the preload failed {} inserts: {:?} ({:?}), after {} retries in {took:?}",
        summary.insert.failed(),
        summary.insert.errors,
        summary.insert.samples,
        summary.insert.retried,
    );
    assert!(
        took >= SHED_FOR,
        "the preload ended in {took:?}, inside the {SHED_FOR:?} the node shed the items"
    );
    // what sent them again is said by code: the sheds, sent again past the nine sends a retriable
    // failure of any other code is given
    let shed_again = summary.insert.retried_by.get("Shedding").copied().unwrap_or(0);
    assert!(
        shed_again > 9 * 100,
        "the preload sent {shed_again} shed inserts again: {:?}",
        summary.insert.retried_by
    );
    // and the node says what shed them: the items' pending bound, on its own figures
    let shed: u64 = pool
        .read_verb(None, shoal::server::replication::ReadVerb::WriteSheds)
        .expect("the shards answered")
        .into_iter()
        .map(|answer| answer.expect("a shard's sheds")["pending_writes"].as_u64().unwrap_or(0))
        .sum();
    eprintln!(
        "the preload loaded {} rows in {took:?}, sent {shed_again} shed inserts again ({:?}), and the node counted {shed} shed",
        summary.insert.ok, summary.insert.retried_by
    );
    assert_eq!(
        shed, shed_again,
        "the node counted {shed} writes shed by the pending bound, and the preload sent {shed_again} again"
    );
}
