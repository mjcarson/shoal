//! The whole generic benchmark path against a node started in process ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! Nothing in this file names a row of the catalog: the dataset is found by its file names,
//! each table's rows by the type its derive hands back, and every query is built by the
//! derive's code. That is the property a schema's benchmark depends on - if this needed a line
//! of catalog code, every schema would.

use bench_dataset::{Catalog, CatalogClient};
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::shared::dataset::DatasetSupport;
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::{Shoal, ShoalPool};
use shoal_loadgen::dataset::{Dataset, Refusal};
use shoal_loadgen::driver::{send_options, ArmClock, ArmSettings, Driver};
use shoal_loadgen::feed::{prepare, ScanOptions};
use shoal_loadgen::pick::Picker;
use shoal_loadgen::progress::Progress;
use shoal_loadgen::spec::{OnExhaust, ReadLevel, Workload};
use shoal_loadgen::window::Window;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// The committed dataset
fn dataset_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(name)
}

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
            Storage::default().default_settings(
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

/// The committed dataset preloads, every workload runs at two bundle sizes, every read finds its
/// row, and every acknowledged insert is read back
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_workload_runs_against_a_node_and_loses_nothing() {
    // a node serving the catalog, in this process
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&dir)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready");
    let client = Arc::new(
        Shoal::<CatalogClient>::new(&addr.to_string())
            .await
            .expect("the client connects"),
    );
    // the dataset, judged and scanned with nothing but the schema's client type
    let dataset = Dataset::open::<CatalogClient>(&dataset_dir("dataset")).expect("the dataset is usable");
    let tables: Vec<_> = dataset
        .files
        .iter()
        .map(|file| prepare::<CatalogClient>(file, &ScanOptions::default()).expect("the file scans"))
        .collect();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].scan().rows, 2000);
    assert_eq!(tables[1].scan().distinct_keys, 2000);
    assert!(tables[1].scan().sorted);
    let driver = Driver::new(vec![client], tables, send_options(ReadLevel::Default), 4);
    let progress = Progress::none();
    // the preload is half of each file
    let (seconds, _) = driver.preload(64, 256, &progress).await;
    let preload = Window::sum(&seconds);
    assert_eq!(preload.insert.latency.len(), 2000, "{:?}", preload.insert.errors);
    assert_eq!(preload.insert.failed(), 0);
    // every workload at two bundle sizes
    let scans: Vec<_> = driver.tables().iter().map(|table| table.scan().clone()).collect();
    let scan_refs: Vec<_> = scans.iter().collect();
    for workload in ["read100", "insert100", "rw50"] {
        for bundle in [1usize, 16] {
            let workload: Workload = workload.parse().unwrap();
            let picker = Picker::new(
                &workload,
                &scan_refs,
                &Default::default(),
                shoal_loadgen::keys::KeyDistribution::Zipfian,
                1,
                7,
                &format!("{}/b{bundle}", workload.name),
            )
            .unwrap();
            let settings = ArmSettings {
                bundle,
                in_flight: bundle * 4,
                warmup: Duration::from_secs(0),
                duration: Duration::from_secs(2),
                on_exhaust: OnExhaust::Wrap,
                retries: 0,
                picker,
                inserts: workload.writes(),
                pace: None,
            };
            let clock = ArmClock::start(settings.warmup + settings.duration);
            let outcome = driver.run_arm(&settings, clock, &progress).await;
            let total = Window::sum(&outcome.seconds);
            let what = format!("{} at {bundle}", workload.name);
            // something was done, and nothing failed or missed
            assert!(total.read.latency.len() + total.insert.latency.len() > 0, "{what} did nothing");
            assert_eq!(total.read.failed(), 0, "{what}: {:?}", total.read.errors);
            assert_eq!(total.insert.failed(), 0, "{what}: {:?}", total.insert.errors);
            assert_eq!(workload.reads(), total.read.latency.len() > 0, "{what}");
            assert_eq!(workload.writes(), total.insert.latency.len() > 0, "{what}");
            // a bundle is counted once, when its last answer is in
            assert!(total.bundles.len() > 0, "{what}");
            assert!(total.bundles.len() <= total.read.latency.len() + total.insert.latency.len());
            // and every insert it was acknowledged for reads back
            if workload.writes() {
                let verify = driver.verify(64, 256, &progress).await;
                assert!(verify.checked > 0, "{what}");
                assert_eq!(verify.lost, 0, "{what} lost acknowledged writes");
                assert!(verify.errors.is_empty(), "{what}: {:?}", verify.errors);
            }
        }
    }
    drop(pool);
}

/// A folder that names a table which did not opt in, one that is no table, and a table twice
/// is refused for all of it at once
#[test]
fn a_bad_dataset_is_refused_by_name() {
    let refusals = Dataset::open::<CatalogClient>(&dataset_dir("dataset-bad")).unwrap_err().0;
    assert!(refusals
        .iter()
        .any(|refusal| matches!(refusal, Refusal::NotOptedIn { table: "Audit", .. })));
    assert!(refusals
        .iter()
        .any(|refusal| matches!(refusal, Refusal::UnknownTable { table, .. } if table == "Nope")));
    assert!(refusals
        .iter()
        .any(|refusal| matches!(refusal, Refusal::Duplicate { table: "Item", .. })));
    // and the schema itself says the audit log never opted in
    assert!(CatalogClient::dataset_tables().contains(&("Audit", false)));
}
