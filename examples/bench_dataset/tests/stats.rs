//! Every figure a bench run and the stats view should show, read off a real node
//!
//! A cluster of one node serving the catalog is started in this process and initialized the way
//! a deployment initializes one. A bench run against it by `--addr` has to record the node's own
//! answers beside the driver's, and a short read and insert workload has to move every metric of
//! the stats view's catalog that says it moves under one.

use bench_dataset::{Catalog, CatalogClient};
use clap::Parser;
use shoal::server::conf::{
    Cluster as ClusterConf, Conf, DefaultStorageSettings, Networking, Resources, Storage,
};
use shoal::server::{AdminKind, AdminRequest};
use shoal::shared::protocol::admin::AdminOutcome;
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::{Shoal, ShoalPool};
use shoal_loadgen::results::Capture;
use shoaladm::cli::{Cli, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The data and control ports of the one node's cluster block: fixed, and under 32768 so no
/// client's ephemeral port can hold them
const PEER_PORTS: (u16, u16) = (23_711, 23_712);

/// A two shard cluster node's config, bootstrapping a cluster of itself, storing under cargo's
/// target directory
///
/// # Arguments
///
/// * `dir` - Where the node stores its tables and logs
/// * `ports` - The cluster block's data and control ports
fn conf(dir: &tempfile::TempDir, ports: (u16, u16)) -> Conf {
    // the cluster of one: its control thread shares cpu 0 rather than taking a core of its own
    let cluster = ClusterConf::default()
        .bootstrap(true)
        .control_core(0)
        .control_core_shared(true)
        .replication_factor(1)
        .port(ports.0)
        .control_port(ports.1);
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
        .cluster(cluster)
}

/// Wait for the node to be a voting member, place the tablets on it, and wait for writes
///
/// The placement is asked for as the process, the way the cluster fixture asks: a connection
/// with no credentials may read the cluster but never change it.
///
/// # Arguments
///
/// * `pool` - The node
/// * `addr` - The node's client address
async fn initialize(pool: &ShoalPool<Catalog>, addr: &str) {
    let node = pool.identity().node;
    let shoal = Arc::new(Shoal::<CatalogClient>::new(addr).await.expect("a connection"));
    // the member up and voting, as a deployment waits for it
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let model = shoaladm::cluster::poll(&shoal).await.expect("a poll");
        if shoaladm::deploy::ops::members_ready(&model, &[node], 1) {
            break;
        }
        assert!(Instant::now() < deadline, "the node never came up: {model:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // the tablets placed on the one node, against the version the node holds now
    let response = tokio::task::block_in_place(|| {
        let version = pool.topology().expect("a topology").version;
        pool.admin(AdminRequest {
            op: shoal::uuid::Uuid::new_v4(),
            expected_version: version,
            kind: AdminKind::Initialize { nodes: vec![node] },
        })
    })
    .expect("an admin answer");
    assert!(
        matches!(response.outcome, Ok(AdminOutcome::Applied { .. })),
        "{:?}",
        response.outcome
    );
    // and writes admitted afterwards
    loop {
        let model = shoaladm::cluster::poll(&shoal).await.expect("a poll");
        if model.default_writes == "admitted" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "writes were never admitted: {}",
            model.default_writes
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Run `shoaladm bench` with these arguments, as the schema's admin program would
///
/// # Arguments
///
/// * `args` - The arguments after `bench`
async fn bench(args: &[&str]) -> color_eyre::Result<()> {
    let project = env!("CARGO_MANIFEST_DIR");
    let line = ["shoaladm", "--project", project, "bench"]
        .into_iter()
        .chain(args.iter().copied());
    let cli = Cli::try_parse_from(line)?;
    let Command::Bench(command) = cli.command else {
        panic!("not a bench command");
    };
    shoaladm::bench::run::<CatalogClient>(&cli.project, command).await
}

/// A bench run against a cluster node by `--addr` records the node's answers beside its own
///
/// The node's figures are what the stats view's ops/s by kind chart draws; a run that never
/// reads them leaves that chart and the capture's server series empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_by_addr_records_the_nodes_answers() {
    // a cluster of one node serving the catalog, in this process
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage, PEER_PORTS)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(60)).expect("the node is ready").to_string();
    initialize(&pool, &addr).await;
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    let dir = out.path().join("figures").display().to_string();
    // an arm that reads and inserts, long enough for the node's figures to be sampled
    bench(&[
        "run", "--addr", &addr, "--dataset", &dataset, "--workloads", "rw50", "--bundles", "4",
        "--duration", "6", "--warmup", "0", "--runs", "1", "--workers", "2", "--on-exhaust",
        "wrap", "--yes-write", "--allow-dirty", "--basic", "--out", &dir,
    ])
    .await
    .expect("the run finishes");
    let capture = Capture::read(&out.path().join("figures")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    let run = &capture.arms[0].runs[0];
    // the node answered gets and inserts, and said how long its slowest took
    let answered = |kind: &str| {
        run.server_series
            .iter()
            .map(|sample| sample.answers_per_sec.get(kind).copied().unwrap_or_default())
            .fold(0.0, f64::max)
    };
    assert!(
        answered("get") > 0.0 && answered("insert") > 0.0,
        "the node's answers were never recorded: {:?}",
        run.server_series
    );
    assert!(
        run.server_series.iter().any(|sample| sample.p99_ms.is_some()),
        "the node's p99 was never recorded: {:?}",
        run.server_series
    );
    // and the node is on a build with figures, so nothing was left out or unread
    assert!(run.unfigured.is_empty(), "{:?}", run.unfigured);
    assert_eq!(run.figures_unread, None);
    // and its memory was sampled with them: a resident set, and an index for the rows it holds
    // (F71)
    let peaks = run.peak_resident();
    assert_eq!(peaks.len(), 1, "one member's memory, by its name: {:?}", run.server_series);
    assert!(peaks.values().all(|resident| *resident > 0), "{peaks:?}");
    let last = run.last_memory().expect("a sample with memory in it");
    assert!(last.values().all(|memory| memory.index_bytes() > 0), "{last:?}");
    drop(pool);
}

/// Every metric the stats view says a run of reads and inserts moves is moved by one
///
/// The figures are read the way the view reads them while a bench run drives the node, every
/// answer is sampled into the view's history, and each metric is judged on the largest value
/// it read: a metric whose figure never reaches the view reads zero throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_metric_that_should_move_does() {
    use shoaladm::cluster::stats::history::History;
    use shoaladm::cluster::stats::metrics::{values, Expect, Reader, METRICS};
    use shoaladm::cluster::stats::{read_stats, StatsModel};
    use std::collections::BTreeMap;
    // a cluster of one node serving the catalog, in this process, on ports of its own
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let ports = (PEER_PORTS.0 + 10, PEER_PORTS.1 + 10);
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage, ports)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(60)).expect("the node is ready").to_string();
    initialize(&pool, &addr).await;
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    let dir = out.path().join("moves").display().to_string();
    // a run of reads and inserts, read from beside it as the stats view reads it
    let run = {
        let addr = addr.clone();
        tokio::spawn(async move {
            bench(&[
                "run", "--addr", &addr, "--dataset", &dataset, "--workloads", "rw50", "--bundles",
                "4", "--duration", "12", "--warmup", "0", "--runs", "1", "--workers", "2",
                "--on-exhaust", "wrap", "--yes-write", "--allow-dirty", "--basic", "--out", &dir,
            ])
            .await
        })
    };
    let shoal = Arc::new(Shoal::<CatalogClient>::new(addr.as_str()).await.expect("a connection"));
    let mut history = History::default();
    let mut largest: BTreeMap<(&str, String), f64> = BTreeMap::new();
    while !run.is_finished() {
        if let Ok(view) = read_stats(&shoal, None).await {
            for metric in METRICS {
                for (series, value) in values(metric, &view) {
                    let entry = largest.entry((metric.key, series)).or_default();
                    *entry = entry.max(value);
                }
            }
            history.record(&StatsModel::new(view, None), Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    run.await.unwrap().expect("the run finishes");
    // every metric that should move read above zero at least once: for a per kind metric,
    // each kind the run sends
    let mut flat = Vec::new();
    for (index, metric) in METRICS.iter().enumerate() {
        if metric.under_load != Expect::Moves {
            continue;
        }
        let wanted: Vec<String> = match metric.read {
            Reader::Kinds(_) => vec!["get".to_string(), "insert".to_string()],
            Reader::Member(_) | Reader::Cluster(_) => largest
                .keys()
                .filter(|(key, _)| *key == metric.key)
                .map(|(_, series)| series.clone())
                .collect(),
        };
        if wanted.is_empty() {
            flat.push(format!("{}: never read", metric.key));
        }
        for series in wanted {
            let seen = largest.get(&(metric.key, series.clone())).copied().unwrap_or_default();
            if !(seen > 0.0) {
                flat.push(format!("{} ({series}): {seen}", metric.key));
            }
        }
        // and the view's history holds a line of it to draw
        if history.series(index, Duration::from_secs(300), Instant::now()).is_empty() {
            flat.push(format!("{}: no line in the history", metric.key));
        }
    }
    // a metric let off that moved anyway is said, so its reason can be revisited
    let moved: Vec<&str> = METRICS
        .iter()
        .filter(|metric| matches!(metric.under_load, Expect::Quiet(_)))
        .filter(|metric| largest.iter().any(|((key, _), value)| key == &metric.key && *value > 0.0))
        .map(|metric| metric.key)
        .collect();
    eprintln!("let off and moved anyway: {moved:?}");
    assert!(flat.is_empty(), "metrics that should move read zero throughout: {flat:#?}");
    drop(pool);
}
