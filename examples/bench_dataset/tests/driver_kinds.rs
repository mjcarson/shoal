//! A kind of operation the driver is handed, and the bytes it counts both ways
//! ([F69](../../../docs/src/features/driver-operation-kinds.md))
//!
//! The driver is generic over a schema and knows two kinds of its own, read and insert. These
//! tests hand it a third that it knows nothing else about, and check it is weighed, picked,
//! timed and reported as its own two are; and they put a proxy between it and a node that counts
//! the bytes of every query and answer frame, which the driver's own windows have to equal.

use bench_dataset::{Catalog, CatalogClient, ItemGet};
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::shared::dataset::OperationKind;
use shoal::shared::traits::QuerySupport;
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
use shoal_loadgen::spec::{OnExhaust, ReadLevel, Workload};
use shoal_loadgen::window::Window;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The type byte of a frame of queries, client to server
const QUERIES: u8 = 5;

/// The type byte of a frame answering one query, server to client
const RESPONSE: u8 = 6;

/// How many items the preload holds: the first half of the committed `Item.csv`
const PRELOADED_ITEMS: u64 = 1000;

/// A kind a schema could supply: a get of one preloaded item, chosen by the operation's seed
struct Lookup;

impl OperationKind<CatalogClient> for Lookup {
    /// The name a workload weighs it by
    fn name(&self) -> &str {
        "lookup"
    }

    /// A get writes nothing
    fn writes(&self) -> bool {
        false
    }

    /// A get of one item the preload holds
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's own seed
    fn build(&self, seed: u64) -> <CatalogClient as QuerySupport>::QueryKinds {
        ItemGet::new(vec![1 + seed % PRELOADED_ITEMS]).into()
    }
}

/// The committed dataset
fn dataset_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset")
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

/// A driver over one client of a node, its dataset scanned and preloaded
///
/// # Arguments
///
/// * `addr` - Where the client connects: the node, or a proxy in front of it
async fn preloaded_driver(addr: &str) -> Driver<CatalogClient> {
    // one client, the committed dataset, and the preload loaded through them
    let client = Arc::new(
        Shoal::<CatalogClient>::new(addr)
            .await
            .expect("the client connects"),
    );
    let dataset = Dataset::open::<CatalogClient>(&dataset_dir()).expect("the dataset is usable");
    let tables: Vec<_> = dataset
        .files
        .iter()
        .map(|file| {
            prepare::<CatalogClient>(file, &ScanOptions::default()).expect("the file scans")
        })
        .collect();
    let driver =
        Driver::new(vec![client], tables, send_options(ReadLevel::Default), 2).with_kinds(vec![
            Arc::new(Lookup) as Arc<dyn OperationKind<CatalogClient>>,
        ]);
    let (seconds, _) = driver.preload(64, 256, &Progress::none()).await;
    assert_eq!(
        Window::sum(&seconds).insert.failed(),
        0,
        "the preload failed"
    );
    driver
}

/// Run one arm of a workload for two seconds, and add up its seconds
///
/// # Arguments
///
/// * `driver` - The driver
/// * `workload` - The workload, as a spec writes it
async fn run(driver: &Driver<CatalogClient>, workload: &str) -> Window {
    // a picker over the driver's tables and the kinds it was handed
    let workload: Workload = workload.parse().expect("the workload parses");
    let scans: Vec<_> = driver
        .tables()
        .iter()
        .map(|table| table.scan().clone())
        .collect();
    let scan_refs: Vec<_> = scans.iter().collect();
    let picker = Picker::new_with_kinds(
        &workload,
        &scan_refs,
        &Default::default(),
        KeyDistribution::Uniform,
        1,
        7,
        &workload.name,
        &driver.kind_names(),
    )
    .expect("the picker is built");
    let settings = ArmSettings {
        bundle: 8,
        in_flight: 32,
        warmup: Duration::from_secs(0),
        duration: Duration::from_secs(2),
        on_exhaust: OnExhaust::Wrap,
        retries: 0,
        picker,
        inserts: workload.inserts(),
    };
    let clock = ArmClock::start(settings.warmup + settings.duration);
    let outcome = driver.run_arm(&settings, clock, &Progress::none()).await;
    Window::sum(&outcome.seconds)
}

/// A kind the driver was handed and knows nothing else about is weighted, picked, timed and
/// reported as read and insert are; one it was not handed is refused by name
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_runs_kinds_a_schema_supplies() {
    // a node serving the catalog, preloaded through a driver handed one supplied kind
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&dir)).expect("the node starts");
    let addr = pool
        .ready(Duration::from_secs(30))
        .expect("the node is ready");
    let driver = preloaded_driver(&addr.to_string()).await;
    // half reads, half lookups
    let total = run(&driver, "read:50,lookup:50").await;
    let lookup = total
        .kinds
        .get("lookup")
        .expect("the supplied kind was recorded under its name");
    assert!(lookup.latency.len() > 0, "no lookup was timed");
    assert_eq!(lookup.failed(), 0, "lookups failed: {:?}", lookup.errors);
    assert!(total.read.latency.len() > 0, "no read was timed");
    assert_eq!(total.insert.latency.len(), 0, "an insert was sent");
    // the two kinds share the operations about evenly
    let share =
        lookup.latency.len() as f64 / (lookup.latency.len() + total.read.latency.len()) as f64;
    assert!(
        (0.4..0.6).contains(&share),
        "lookups were {share} of the operations"
    );
    // and it reads as a kind of its own in what a capture keeps
    let summary = total.summary(Duration::from_secs(2));
    assert!(summary.kinds["lookup"].per_sec > 0.0);
    assert!(summary.line().contains("lookup"), "{}", summary.line());
    // a kind the driver was not handed is refused before anything is sent
    let scans: Vec<_> = driver
        .tables()
        .iter()
        .map(|table| table.scan().clone())
        .collect();
    let refused = Picker::new_with_kinds(
        &"read:1,scan:1".parse().unwrap(),
        &scans.iter().collect::<Vec<_>>(),
        &Default::default(),
        KeyDistribution::Uniform,
        1,
        7,
        "x",
        &driver.kind_names(),
    )
    .unwrap_err();
    assert!(
        refused.contains("\"scan\"") && refused.contains("lookup"),
        "{refused}"
    );
    drop(pool);
}

/// The bytes of one kind of frame a proxy saw go past, one direction of every connection
#[derive(Default)]
struct Counted {
    /// Bytes of query frames, client to server, header included
    up: AtomicU64,
    /// Bytes of answer frames, server to client, header included
    down: AtomicU64,
}

/// Copy frames from one side of a connection to the other, counting those of one type
///
/// # Arguments
///
/// * `from` - Where frames are read
/// * `to` - Where they are written
/// * `kind` - The frame type to count
/// * `count` - Where its bytes are added
/// * `up` - Whether this is the client to server half
async fn relay(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    kind: u8,
    count: Arc<Counted>,
    up: bool,
) {
    // frame by frame: the eight byte header, then as many bytes as it says follow it
    let mut header = [0u8; 8];
    while from.read_exact(&mut header).await.is_ok() {
        let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let mut body = vec![0u8; len];
        if from.read_exact(&mut body).await.is_err() {
            return;
        }
        // counted before it is forwarded, so an answer the client has read is always counted
        if header[1] == kind {
            let counter = if up { &count.up } else { &count.down };
            counter.fetch_add((header.len() + len) as u64, Ordering::SeqCst);
        }
        if to.write_all(&header).await.is_err() || to.write_all(&body).await.is_err() {
            return;
        }
    }
}

/// Start a proxy in front of a node that counts query and answer frames, returning its address
///
/// # Arguments
///
/// * `node` - The node's client address
/// * `count` - Where the bytes are added
async fn counting_proxy(node: std::net::SocketAddr, count: Arc<Counted>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the proxy binds");
    let addr = listener.local_addr().expect("the proxy has an address");
    tokio::spawn(async move {
        // every connection the client opens is relayed to a connection of its own to the node
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(node).await else {
                continue;
            };
            let (client_rx, client_tx) = client.into_split();
            let (server_rx, server_tx) = server.into_split();
            tokio::spawn(relay(client_rx, server_tx, QUERIES, count.clone(), true));
            tokio::spawn(relay(server_rx, client_tx, RESPONSE, count.clone(), false));
        }
    });
    addr
}

/// The bytes a window counts both ways equal what went over the wire, for rows and for a kind a
/// test supplies
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_counts_bytes_both_ways() {
    // a node, and a proxy in front of it that counts every query and answer frame
    let dir = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&dir)).expect("the node starts");
    let addr = pool
        .ready(Duration::from_secs(30))
        .expect("the node is ready");
    let count = Arc::new(Counted::default());
    let proxy = counting_proxy(addr, count.clone()).await;
    let driver = preloaded_driver(&proxy.to_string()).await;
    // what the preload sent is not the arm's
    let (up_before, down_before) = (
        count.up.load(Ordering::SeqCst),
        count.down.load(Ordering::SeqCst),
    );
    // reads, inserts and lookups, every one of them through the proxy
    let total = run(&driver, "read:40,insert:30,lookup:30").await;
    let up = count.up.load(Ordering::SeqCst) - up_before;
    let down = count.down.load(Ordering::SeqCst) - down_before;
    assert!(up > 0 && down > 0, "the proxy saw nothing");
    assert_eq!(
        total.bytes_sent, up,
        "the driver's sent bytes are not the wire's"
    );
    assert_eq!(
        total.bytes_received, down,
        "the driver's received bytes are not the wire's"
    );
    // and each kind's received bytes add up to no more than the whole
    let by_kind = total.read.bytes_received
        + total.insert.bytes_received
        + total
            .kinds
            .values()
            .map(|stats| stats.bytes_received)
            .sum::<u64>();
    assert!(
        by_kind > 0 && by_kind <= total.bytes_received,
        "{by_kind} of {}",
        total.bytes_received
    );
    assert!(
        total.kinds["lookup"].bytes_received > 0,
        "the lookups' answers were not counted"
    );
    drop(pool);
}
