//! A table scanned whole through a node's walk, as the bench's scan kind drives it
//! ([F115](../../../docs/src/features/bench-scans.md))
//!
//! W12's M22 row `a_scan_kind_times_a_whole_scan_page_by_page`: against a node in process, a whole
//! scan of a preloaded table walks every tablet, hands out every partition the preload wrote once,
//! and its windows count the pages, partitions, rows and bytes it read; an arm of scans back to
//! back counts each whole scan that ended. And O139's: a scan whose pages are shed moves its
//! streams on to other tablets, and still hands out every partition once.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bench_dataset::{Catalog, CatalogClient};
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::server::faults;
use shoal::shared::identity::TableId;
use shoal::shared::placement::TABLET_COUNT;
use shoal::shared::protocol::admin::WalkClass;
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::{Shoal, ShoalPool};
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::{send_options, ArmClock, Driver};
use shoal_loadgen::feed::{prepare, ScanOptions};
use shoal_loadgen::progress::Progress;
use shoal_loadgen::scans::{ScanArm, ScanDriver, ScanMember, SCAN_KIND};
use shoal_loadgen::spec::ReadLevel;
use shoal_loadgen::window::Window;

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

/// Held by every test of this binary for its whole length: a held walk is a process-wide static,
/// and every test scans the items, so a hold one test sets would hold another's pages
static SERIAL: Mutex<()> = Mutex::new(());

/// Take this binary's turn, whichever test before it panicked
fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A node serving the catalog, in this process, its preload written through the driver
///
/// # Arguments
///
/// * `dir` - Where the node stores its tables
async fn preloaded(dir: &tempfile::TempDir) -> (ShoalPool<Catalog>, Arc<Shoal<CatalogClient>>, Driver<CatalogClient>) {
    let mut pool = ShoalPool::<Catalog>::start(conf(dir)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready");
    let client = Arc::new(
        Shoal::<CatalogClient>::new(&addr.to_string())
            .await
            .expect("the client connects"),
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset");
    let dataset = Dataset::open::<CatalogClient>(&root).expect("the dataset is usable");
    let tables: Vec<_> = dataset
        .files
        .iter()
        .map(|file| prepare::<CatalogClient>(file, &ScanOptions::default()).expect("the file scans"))
        .collect();
    let driver = Driver::new(vec![client.clone()], tables, send_options(ReadLevel::Default), 4);
    let (preloaded, _) = driver.preload(64, 256, &Progress::none()).await;
    assert_eq!(Window::sum(&preloaded).insert.failed(), 0);
    (pool, client, driver)
}

/// A scan whose shards shed its pages moves its streams on to other tablets, and still hands out
/// every partition once, each shed tablet taken up where it stood
///
/// O139's: with every scan read of the items held, each shard's two pages stay in flight and the
/// streams past them are shed, put their tablets back and take others, again and again; once the
/// hold lifts, every tablet is walked to its end and nothing is handed out twice or lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shed_scan_moves_on_and_hands_out_every_partition_once() {
    let _turn = serial();
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (pool, client, _driver) = preloaded(&dir).await;
    // every scan read of the items held, so a shard's pages stay in flight and the rest are shed
    let table = TableId::of("Item").0;
    faults::hold_walk_reads(table, WalkClass::Scan, true);
    let scans = ScanDriver::new(vec![ScanMember {
        name: "alone".to_string(),
        node: None,
        client: client.clone(),
    }]);
    // eight streams against two shards that read two pages each at once
    let seconds = Arc::new(Mutex::new(Vec::new()));
    let scan = {
        let seconds = seconds.clone();
        tokio::spawn(async move {
            let clock = ArmClock::start(Duration::from_secs(120));
            scans.scan_once("Item", 8, 7, &clock, &seconds).await
        })
    };
    // both shards' pages held and the streams past them shed again and again, then the hold lifted
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let shed: u64 = seconds
            .lock()
            .expect("the seconds")
            .iter()
            .filter_map(|second| second.scan.as_ref().map(|scan| scan.shed))
            .sum();
        if faults::walk_reads_waiting() >= 4 && shed >= 16 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scan's pages were not held and shed: {} held, {shed} shed",
            faults::walk_reads_waiting()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    faults::hold_walk_reads(table, WalkClass::Scan, false);
    let end = scan.await.expect("the scan's task");
    assert!(end.finished, "{end:?}");
    let total = Window::sum(seconds.lock().expect("the seconds").iter());
    let summary = total.summary(end.took);
    let walked = summary.scan.as_ref().expect("the windows saw the scan's pages");
    // every partition handed out once, though tablets were shed and taken up by other streams
    assert_eq!(walked.partitions, 1000, "{walked:?}");
    assert_eq!(end.partitions, 1000);
    assert!(walked.pages >= TABLET_COUNT as u64, "{} pages", walked.pages);
    assert!(walked.shed >= 16, "{walked:?}");
    // a shed is a page asked again, never a page answered or a failure
    assert_eq!(summary.kinds[SCAN_KIND].ok, walked.pages);
    assert_eq!(summary.kinds[SCAN_KIND].failed(), 0);
    drop(pool);
}

/// A whole scan of a preloaded table hands out every partition once and its windows count what it
/// read, page by page; an arm of scans back to back counts each that ended
///
/// W12's `a_scan_kind_times_a_whole_scan_page_by_page`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_kind_times_a_whole_scan_page_by_page() {
    let _turn = serial();
    // a node serving the catalog, in this process, its preload written through the driver
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (pool, client, driver) = preloaded(&dir).await;
    // the items the preload wrote: the first half of the file, each its own partition
    let items = driver.tables()[0].scan().clone();
    assert_eq!(items.table, "Item");
    // a node alone, asked as the connection that is not authenticated, which is its operator
    let scans = ScanDriver::new(vec![ScanMember {
        name: "alone".to_string(),
        node: None,
        client: client.clone(),
    }]);
    // one whole scan, on a clock of its own, every page in the second it was answered
    let clock = ArmClock::start(Duration::from_secs(120));
    let seconds = Arc::new(Mutex::new(Vec::new()));
    let end = scans.scan_once("Item", 4, 7, &clock, &seconds).await;
    assert!(end.finished, "{end:?}");
    assert!(end.failure.is_none(), "{end:?}");
    let total = Window::sum(seconds.lock().expect("the seconds").iter());
    let summary = total.summary(end.took);
    let scan = summary.scan.as_ref().expect("the windows saw the scan's pages");
    // every tablet answered at least one page, and every partition the preload wrote came once
    assert!(scan.pages >= TABLET_COUNT as u64, "{} pages", scan.pages);
    assert_eq!(end.pages, scan.pages);
    assert_eq!(scan.partitions, 1000, "{scan:?}");
    assert_eq!(end.partitions, 1000);
    assert_eq!(scan.rows, 1000);
    assert!(scan.bytes > 0 || scan.resident == 1000, "{scan:?}");
    assert_eq!(scan.resident + scan.archived, 1000);
    // a page's latency is the scan kind's, one a page, and the member's figures are its pages'
    assert_eq!(summary.kinds[SCAN_KIND].ok, scan.pages);
    assert_eq!(summary.kinds[SCAN_KIND].failed(), 0);
    assert_eq!(scan.members["alone"].pages, scan.pages);
    assert_eq!(scan.members["alone"].partitions, 1000);
    assert_eq!(scan.held.count, scan.pages);
    assert_eq!(scan.first_page.count, 1);
    // an arm of whole scans back to back, long enough for two, counts each that ended
    let arm = ScanArm {
        table: "Item".to_string(),
        streams: Some(4),
    };
    let duration = end.took * 2 + Duration::from_secs(1);
    let clock = ArmClock::start(duration);
    let outcome = scans
        .run_arm(&arm, Duration::ZERO, duration, 7, "scan/Item/c4/none/0", clock, &Progress::none())
        .await;
    assert!(outcome.ended_early.is_none(), "{:?}", outcome.ended_early);
    let arm_total = Window::sum(&outcome.seconds).summary(duration);
    let arm_scan = arm_total.scan.as_ref().expect("the arm's windows saw its pages");
    assert!(arm_scan.scans >= 1, "{arm_scan:?}");
    assert_eq!(arm_scan.whole.count, arm_scan.scans);
    // and every second's partitions add to the whole: nothing counted twice across seconds
    let by_second: u64 = outcome
        .seconds
        .iter()
        .filter_map(|second| second.scan.as_ref().map(|scan| scan.partitions))
        .sum();
    assert_eq!(by_second, arm_scan.partitions);
    // a sorted table's partitions hold several rows each, every one the preload wrote handed out
    let reviews = driver.tables()[1].scan().clone();
    assert_eq!(reviews.table, "Review");
    let again = Arc::new(Mutex::new(Vec::new()));
    let sorted = scans.scan_once("Review", 2, 11, &ArmClock::start(Duration::from_secs(120)), &again).await;
    assert!(sorted.finished, "{sorted:?}");
    let sorted_total = Window::sum(again.lock().expect("the seconds").iter()).summary(sorted.took);
    let sorted_scan = sorted_total.scan.as_ref().expect("the sorted scan's pages were seen");
    assert_eq!(sorted_scan.rows, 1000, "{sorted_scan:?}");
    assert!(sorted_scan.partitions > 0 && sorted_scan.partitions <= 1000, "{sorted_scan:?}");
    drop(pool);
}
