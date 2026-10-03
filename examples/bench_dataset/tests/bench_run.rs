//! `shoaladm bench run --addr` whole, against a node started in process ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! The same entry a schema's admin program calls, with nothing but the schema's client type:
//! the dataset is judged, scanned and preloaded, every arm runs and is read back, and the
//! capture it writes reads back as a capture that `show` and `compare` accept.

use bench_dataset::{Catalog, CatalogClient};
use clap::Parser;
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::ShoalPool;
use shoal_loadgen::results::Capture;
use shoaladm::cli::{Cli, Command};
use std::time::Duration;

/// A two shard server's config, storing under cargo's target directory
///
/// # Arguments
///
/// * `dir` - Where the server stores its tables
fn conf(dir: &tempfile::TempDir) -> Conf {
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

/// A run writes a capture of every arm and run with nothing lost, and two runs compare
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_against_one_node_writes_a_capture_that_compares() {
    // a node serving the catalog, in this process
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready").to_string();
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    // two captures of the same spec: an insert arm first, so the reads after it find rows
    for label in ["first", "second"] {
        let dir = out.path().join(label).display().to_string();
        bench(&[
            "run", "--addr", &addr, "--dataset", &dataset, "--workloads", "insert100,read100,rw50",
            "--bundles", "1,8", "--duration", "1", "--warmup", "0", "--runs", "2", "--workers", "2",
            "--on-exhaust", "wrap", "--yes-write", "--allow-dirty", "--basic", "--out", &dir,
        ])
        .await
        .expect("the run finishes");
    }
    // every arm ran twice, read every row it asked for, and lost no acknowledged insert
    let capture = Capture::read(&out.path().join("first")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    assert_eq!(capture.arms.len(), 6);
    assert_eq!(capture.dataset.tables.len(), 2);
    assert!(capture.preload.as_ref().is_some_and(|preload| preload.insert.ok == 2000));
    for arm in &capture.arms {
        assert_eq!(arm.runs.len(), 2, "{}", arm.id);
        for run in &arm.runs {
            let measured = &run.measured;
            assert!(measured.read.ok + measured.insert.ok > 0, "{} did nothing", arm.id);
            assert_eq!(measured.read.failed() + measured.insert.failed(), 0, "{}: {measured:?}", arm.id);
            if let Some(verify) = &run.verify {
                assert_eq!(verify.lost, 0, "{} lost acknowledged inserts", arm.id);
            }
            // a node that is no cluster member keeps no figures, which the run records
            assert!(run.server_series.is_empty(), "{}: {:?}", arm.id, run.server_series);
            assert!(run.figures_unread.is_some(), "{} does not say why it read no figures", arm.id);
        }
    }
    assert_eq!(capture.provenance.schema.db, "Catalog");
    // the two compare, arm by arm; a capture of another spec is refused
    let first = out.path().join("first").display().to_string();
    let second = out.path().join("second").display().to_string();
    bench(&["compare", &first, &second]).await.expect("the two compare");
    let mut other = Capture::read(&out.path().join("second")).unwrap();
    other.spec_digest = "another".to_string();
    other.write(&out.path().join("other")).unwrap();
    let third = out.path().join("other").display().to_string();
    let refused = bench(&["compare", &first, &third]).await.unwrap_err().to_string();
    assert!(refused.contains("spec"), "{refused}");
    drop(pool);
}

/// A run told to stop mid arm stops there, says why, and still writes what it measured
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aborted_run_stops_and_keeps_what_it_measured() {
    use shoal_loadgen::dataset::Dataset;
    use shoal_loadgen::progress::{Control, Progress};
    use shoaladm::bench::orchestrate::{orchestrate, Context};
    // a node serving the catalog, in this process
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready").to_string();
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    // one long arm, so the abort lands inside it
    let cli = Cli::try_parse_from([
        "shoaladm", "--project", env!("CARGO_MANIFEST_DIR"), "bench", "run", "--addr", &addr,
        "--dataset", &dataset, "--workloads", "read100", "--bundles", "8", "--duration", "60",
        "--warmup", "0", "--runs", "1", "--basic", "--allow-dirty",
    ])
    .unwrap();
    let Command::Bench(shoaladm::bench::BenchCommand::Run(args)) = cli.command else {
        panic!("not a run");
    };
    let spec = args.spec().unwrap();
    let ctx = Context {
        project: cli.project.clone(),
        dataset: Dataset::open::<CatalogClient>(&spec.dataset).unwrap(),
        spec,
        args: *args,
        dir: out.path().to_path_buf(),
        label: "aborted".to_string(),
        inventory: None,
        project_facts: Default::default(),
        shoal_facts: Default::default(),
    };
    let (control_tx, control_rx) = tokio::sync::watch::channel(Control::Run);
    let started = std::time::Instant::now();
    // the run on a thread and runtime of its own, as `shoaladm bench` runs it: it holds a
    // deployment, which is not Send
    let run = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        runtime.block_on(orchestrate::<CatalogClient>(ctx, Progress::none(), control_rx, None))
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    control_tx.send(Control::Abort).unwrap();
    let result = tokio::task::spawn_blocking(move || run.join().unwrap()).await.unwrap();
    // it stopped long before its minute, and says it was stopped
    assert!(started.elapsed() < Duration::from_secs(40), "the abort was not heeded");
    let capture = Capture::read(out.path()).expect("a capture");
    let run = &capture.arms[0].runs[0];
    assert_eq!(run.ended_early.as_ref().map(|ended| ended.reason.as_str()), Some("aborted"));
    assert!(run.measured.read.ok > 0);
    assert!(result.is_ok() || capture.error.is_some());
    drop(pool);
}


/// A run that names no workload and has no terminal runs the four defaults, and says so
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_naming_no_workload_runs_the_defaults() {
    // a node serving the catalog, in this process
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage)).expect("the node starts");
    let addr = pool.ready(Duration::from_secs(30)).expect("the node is ready").to_string();
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset", env!("CARGO_MANIFEST_DIR"));
    let dir = out.path().join("defaults").display().to_string();
    // a test's stdin is no terminal, so no wizard opens; the defaults read before they insert,
    // which an attached node allows once writes are, since the run loads the preload first
    bench(&[
        "run", "--addr", &addr, "--dataset", &dataset, "--bundles", "4", "--duration", "1",
        "--warmup", "0", "--runs", "1", "--workers", "2", "--on-exhaust", "wrap", "--yes-write",
        "--allow-dirty", "--out", &dir,
    ])
    .await
    .expect("the run finishes");
    let capture = Capture::read(&out.path().join("defaults")).expect("a capture");
    let names: Vec<&str> = capture.spec.workloads.iter().map(|workload| workload.name.as_str()).collect();
    assert_eq!(names, ["read100", "insert100", "rw50", "read90"]);
    assert_eq!(capture.arms.len(), 4);
    // and the first arm, the one that only reads, found every row it asked for
    let first = &capture.arms[0];
    assert_eq!(first.workload, "read100");
    assert!(first.runs[0].measured.read.ok > 0 && first.runs[0].measured.read.misses == 0, "{:?}", first.runs[0].measured.read);
    let log = std::fs::read_to_string(out.path().join("defaults").join("log.txt")).unwrap();
    assert!(
        log.contains("no --workloads given: running the defaults read100, insert100, rw50, read90"),
        "{log}"
    );
    drop(pool);
}

/// Write a dataset of items whose descriptions are each a little over a mebibyte
///
/// # Arguments
///
/// * `dir` - The folder to write `Item.jsonl` into
/// * `rows` - How many items to write
fn wide_items(dir: &std::path::Path, rows: u64) {
    use std::io::Write;
    // one description shared by every row: 1.1 MiB of one letter, which json needs no escape for
    let description = "w".repeat(1_153_434);
    let mut file = std::io::BufWriter::new(std::fs::File::create(dir.join("Item.jsonl")).unwrap());
    // one item a line, each with its own id so none replaces another
    for id in 0..rows {
        writeln!(
            file,
            "{{\"id\": {id}, \"name\": \"item-{id}\", \"price\": {id}, \"description\": \"{description}\"}}"
        )
        .unwrap();
    }
    file.flush().unwrap();
}

/// Copy bytes from a client to a node, stopping once for a while after the first mebibyte
///
/// # Arguments
///
/// * `from` - The client's half of the connection
/// * `to` - The node's half
/// * `stall` - How long the link stops for
/// * `stalled` - Whether the link has stopped already, on this connection or another
async fn stalling(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    stall: Duration,
    stalled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // a chunk at a time, so the stop lands in the middle of the first bundle of rows
    let mut chunk = vec![0u8; 64 << 10];
    let mut carried = 0;
    while let Ok(read) = from.read(&mut chunk).await {
        if read == 0 || to.write_all(&chunk[..read]).await.is_err() {
            return;
        }
        carried += read;
        // past the handshake and into the rows, the link stops once, as a congested one would
        if carried > 1 << 20 && !stalled.swap(true, Ordering::SeqCst) {
            tokio::time::sleep(stall).await;
        }
    }
}

/// Start a proxy in front of a node whose queries stop once on their way, returning its address
///
/// # Arguments
///
/// * `node` - The node's client address
/// * `stall` - How long the queries stop for
async fn stalling_link(node: std::net::SocketAddr, stall: Duration) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("the proxy binds");
    let addr = listener.local_addr().expect("the proxy has an address");
    // one stop for the whole link, whichever connection carries the rows first
    let stalled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    tokio::spawn(async move {
        // every connection the client opens is relayed to a connection of its own to the node
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect(node).await else {
                continue;
            };
            let (client_rx, mut client_tx) = client.into_split();
            let (mut server_rx, server_tx) = server.into_split();
            tokio::spawn(stalling(client_rx, server_tx, stall, stalled.clone()));
            // answers go back as they come
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut server_rx, &mut client_tx).await;
            });
        }
    });
    addr
}

/// Rows of a mebibyte and more preload in bundles a frame can carry (item 210)
///
/// The frame check judges the spec's bundles and passes a bundle of one, but the preload used to
/// load in bundles of up to sixty four, which for these rows is past the 64 MiB a node accepts.
/// A bundle only fills when the file is read faster than the cluster takes rows, as it is on the
/// lab's 1 GbE. A debug build parses these rows slower than a node on loopback takes them, so
/// the link here stops for five seconds once the first rows are on it, and the file gets ahead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wide_rows_preload_in_bundles_a_frame_carries() {
    // a node serving the catalog, in this process, behind a link that stops once
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(&storage)).expect("the node starts");
    let node = pool.ready(Duration::from_secs(30)).expect("the node is ready");
    let addr = stalling_link(node, Duration::from_secs(5)).await.to_string();
    // a hundred items, all of them preloaded
    let dataset = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    wide_items(dataset.path(), 100);
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = out.path().join("wide").display().to_string();
    // a bundle of one is well inside the frame, so the run is accepted and preloads
    bench(&[
        "run", "--addr", &addr, "--dataset", &dataset.path().display().to_string(), "--workloads",
        "read100", "--bundles", "1", "--preload", "100%", "--duration", "1", "--warmup", "0",
        "--runs", "1", "--workers", "1", "--yes-write", "--allow-dirty", "--basic", "--out", &dir,
    ])
    .await
    .expect("the run finishes");
    // every row went in, and the arm read them back
    let capture = Capture::read(&out.path().join("wide")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    assert!(capture.preload.as_ref().is_some_and(|preload| preload.insert.ok == 100), "{:?}", capture.preload);
    let run = &capture.arms[0].runs[0];
    assert!(run.measured.read.ok > 0 && run.measured.read.misses == 0, "{:?}", run.measured.read);
    drop(pool);
}
