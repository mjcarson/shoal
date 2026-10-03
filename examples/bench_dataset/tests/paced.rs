//! A paced stream beside the main load, against a node started in process ([F72](../../../docs/src/features/bench-paced-stream.md))
//!
//! The main load is a closed loop over `Item` and sends as fast as answers come back; the paced
//! stream drives `Review` alone, at an offered rate, with windows of its own. These tests hold
//! the stream to its rate, its windows to its own table's operations, and `shoaladm bench run`
//! to a capture that carries both.

use bench_dataset::{Catalog, CatalogClient};
use clap::Parser;
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::{Shoal, ShoalPool};
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::{send_options, ArmClock, ArmSettings, Driver};
use shoal_loadgen::feed::{prepare, ScanOptions};
use shoal_loadgen::keys::KeyDistribution;
use shoal_loadgen::pick::Picker;
use shoal_loadgen::progress::Progress;
use shoal_loadgen::results::Capture;
use shoal_loadgen::spec::{OnExhaust, ReadLevel, Workload};
use shoal_loadgen::window::Window;
use shoaladm::cli::{Cli, Command};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// A two shard server's config, storing under cargo's target directory
///
/// # Arguments
///
/// * `dir` - Where the server stores its tables
fn conf(dir: &tempfile::TempDir) -> Conf {
    // any port, two shards, and a real filesystem for direct I/O
    Conf::default()
        .resources(Resources::default().cores(2).memory("256MiB").expect("a size"))
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

/// One arm's settings for a driver over some tables
///
/// # Arguments
///
/// * `driver` - The driver, whose tables the picker chooses among
/// * `workload` - What it sends
/// * `stream` - The name its choices are drawn under
/// * `bundle` - How many queries a bundle holds
/// * `in_flight` - How many a worker keeps outstanding
/// * `pace` - The rate it is offered at, for a paced arm
fn settings(
    driver: &Driver<CatalogClient>,
    workload: &str,
    stream: &str,
    bundle: usize,
    in_flight: usize,
    pace: Option<f64>,
) -> ArmSettings {
    // a picker over the driver's own tables, so its indices are theirs
    let workload: Workload = workload.parse().unwrap();
    let scans: Vec<_> = driver.tables().iter().map(|table| table.scan().clone()).collect();
    let picker = Picker::new(
        &workload,
        &scans.iter().collect::<Vec<_>>(),
        &Default::default(),
        KeyDistribution::Uniform,
        1,
        7,
        stream,
    )
    .unwrap();
    ArmSettings {
        bundle,
        in_flight,
        warmup: Duration::from_secs(1),
        duration: Duration::from_secs(3),
        on_exhaust: OnExhaust::Wrap,
        retries: 0,
        picker,
        inserts: workload.inserts(),
        pace,
    }
}

/// A paced stream sends at its rate whatever the closed loop beside it does, keeps its windows
/// apart, and loses none of its inserts
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paced_stream_keeps_its_rate_and_its_own_windows() {
    // a node serving the catalog, in this process, with the dataset's preload in it
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&dir)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready");
    let client = Arc::new(Shoal::<CatalogClient>::new(&addr.to_string()).await.expect("the client connects"));
    let dataset = Dataset::open::<CatalogClient>(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset")).unwrap();
    let tables: Vec<_> = dataset
        .files
        .iter()
        .map(|file| prepare::<CatalogClient>(file, &ScanOptions::default()).unwrap())
        .collect();
    let all = Driver::new(vec![client], tables, send_options(ReadLevel::Default), 4);
    let (seconds, _) = all.preload(64, 256, &Progress::none()).await;
    assert_eq!(Window::sum(&seconds).insert.failed(), 0);
    // the main load over Item, and the paced stream over Review, as the bench splits them
    let main = all.narrowed(|scan| scan.table != "Review", 4);
    let paced = all.narrowed(|scan| scan.table == "Review", 2);
    let names = |driver: &Driver<CatalogClient>| {
        driver.tables().iter().map(|table| table.scan().table.clone()).collect::<Vec<_>>()
    };
    assert_eq!((names(&main), names(&paced)), (vec!["Item".to_string()], vec!["Review".to_string()]));
    // a closed loop of reads at a depth, and the paced stream at forty a second over two streams,
    // reads and inserts half each
    for (paced_workload, rate) in [("read100", 40.0), ("rw50", 40.0)] {
        let main_settings = settings(&main, "read100", "main", 8, 32, None);
        let paced_settings = settings(&paced, paced_workload, "paced", 1, 20, Some(rate));
        let clock = ArmClock::start(main_settings.warmup + main_settings.duration);
        let quiet = Progress::none();
        let (main_outcome, paced_outcome) = tokio::join!(
            main.run_arm(&main_settings, clock.clone(), &quiet),
            paced.run_arm(&paced_settings, clock.clone(), &quiet)
        );
        let what = format!("{paced_workload} at {rate}/s");
        // the closed loop sent far more than the paced stream, which is what makes it the load
        let main_total = Window::sum(&main_outcome.seconds);
        let paced_total = Window::sum(&paced_outcome.seconds);
        let sent = paced_total.read.latency.len() + paced_total.insert.latency.len();
        assert!(main_total.read.latency.len() > sent * 10, "{what}: main {} paced {sent}", main_total.read.latency.len());
        // the paced stream sent its rate for the arm's four seconds, within a fifth
        let expected = rate * 4.0;
        assert!(
            (sent as f64 - expected).abs() <= expected / 5.0,
            "{what}: sent {sent}, offered {expected}"
        );
        // second by second too: no measured second is far off the rate, so it was paced and not
        // sent in a burst
        for (at, second) in paced_outcome.seconds.iter().enumerate().skip(1).take(3) {
            let ops = (second.read.latency.len() + second.insert.latency.len()) as f64;
            assert!((ops - rate).abs() <= rate / 4.0, "{what}: second {at} sent {ops}");
        }
        // its windows hold its own operations and nothing of the main load's: the main load only
        // read, so the paced stream's inserts are its own, and its reads are its own count
        assert_eq!(paced_total.read.failed() + paced_total.insert.failed(), 0, "{what}");
        assert_eq!(main_total.insert.latency.len(), 0, "{what}");
        if paced_workload == "rw50" {
            assert!(paced_total.insert.latency.len() > 0, "{what}");
            // and every insert it was acknowledged for reads back from its own table
            let verify = paced.verify(64, 256, &quiet).await;
            assert!(verify.checked > 0, "{what}");
            assert_eq!(verify.lost, 0, "{what}");
        } else {
            assert_eq!(paced_total.insert.latency.len(), 0, "{what}");
        }
        // only the paced stream's own table had a feed opened
        let fed: Vec<&str> = paced_outcome.feeds.iter().map(|(table, _)| table.as_str()).collect();
        assert_eq!(fed, vec!["Review"]);
    }
    drop(pool);
}

/// `shoaladm bench run --paced` writes a capture whose every run carries the paced stream's
/// windows, beside a main load that left its table alone, and the capture compares
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_with_a_paced_stream_writes_it_into_the_capture() {
    // a node serving the catalog, in this process
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready").to_string();
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    let dir = out.path().join("paced").display().to_string();
    let cli = Cli::try_parse_from([
        "shoaladm", "--project", env!("CARGO_MANIFEST_DIR"), "bench", "run", "--addr", &addr,
        "--dataset", &dataset, "--workloads", "read100,insert100", "--bundles", "8", "--duration", "2",
        "--warmup", "1", "--runs", "2", "--workers", "2", "--on-exhaust", "wrap", "--paced", "Review",
        "--paced-rate", "30", "--yes-write", "--allow-dirty", "--basic", "--out", &dir,
    ])
    .unwrap();
    let Command::Bench(command) = cli.command else {
        panic!("not a bench command");
    };
    shoaladm::bench::run::<CatalogClient>(&cli.project, command).await.expect("the run finishes");
    let capture = Capture::read(&out.path().join("paced")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    assert_eq!(capture.spec.paced.as_ref().map(|paced| paced.table.as_str()), Some("Review"));
    for arm in &capture.arms {
        for run in &arm.runs {
            let paced = run.paced.as_ref().unwrap_or_else(|| panic!("{} has no paced stream", arm.id));
            assert_eq!((paced.table.as_str(), paced.workload.as_str(), paced.per_sec), ("Review", "read100", 30.0));
            // it read at its rate over the measured time, and every read found its row
            let measured = &paced.measured;
            assert!((measured.read.per_sec - 30.0).abs() <= 6.0, "{}: {measured:?}", arm.id);
            assert_eq!(measured.read.failed(), 0, "{}: {measured:?}", arm.id);
            assert!(paced.worst_second_p99_ms().is_some(), "{}", arm.id);
            // and the main load never fed the paced table
            assert!(!run.feeds.contains_key("Review"), "{}: {:?}", arm.id, run.feeds);
        }
    }
    // the insert arm's main load inserted into Item alone, and lost nothing
    let insert = capture.arms.iter().find(|arm| arm.workload == "insert100").unwrap();
    assert!(insert.runs.iter().all(|run| run.feeds.contains_key("Item")));
    assert!(insert.runs.iter().all(|run| run.verify.as_ref().is_some_and(|verify| verify.lost == 0)));
    // compare reads the paced stream's tail beside the main load's numbers
    let comparison = shoal_loadgen::compare::compare(&capture, &capture, &[]).expect("a capture compares with itself");
    assert!(comparison.arms.iter().all(|arm| arm.metrics.iter().any(|metric| metric.metric == "paced read p99 ms")));
    // and show prints it
    let shown = shoaladm::bench::store::show_lines(&capture).join("\n");
    assert!(shown.contains("paced Review read100 at 30/s"), "{shown}");
    drop(pool);
}
