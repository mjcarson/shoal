//! A bucket's object arms against a node in process ([F80](../../../docs/src/features/object-wire.md))
//!
//! S15's two M13 rows ([S15](../../../docs/src/object-storage/performance.md#acceptance-tests)):
//! the driver over a described dataset round trips every object it acknowledged, and a byte of an
//! acknowledged object damaged on disk fails the read back. And the whole of `shoaladm bench run`
//! over the described dataset, which writes object arms into a capture.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bench_dataset::{Catalog, CatalogClient};
use clap::Parser;
use shoal::client::Shoal;
use shoal::server::conf::{Conf, DefaultStorageSettings, Networking, Resources, Storage};
use shoal::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::ShoalPool;
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::ArmClock;
use shoal_loadgen::objects::{make, ObjectArm, ObjectDriver, ObjectMix};
use shoal_loadgen::progress::Progress;
use shoal_loadgen::results::Capture;
use shoal_loadgen::window::{OpKind, Window};
use shoaladm::cli::{Cli, Command};

/// A two shard server's config with the catalog's images bound, storing under cargo's target
///
/// # Arguments
///
/// * `dir` - Where the server stores its tables
fn conf(dir: &Path) -> Conf {
    Conf::default()
        .resources(
            Resources::default()
                .cores(2)
                .memory("256MiB")
                .expect("a size"),
        )
        .networking(Networking::default().port(0))
        .storage(
            Storage::default()
                .pool("local", bench_dataset::images_pool())
                .bind("Images", "local")
                .default_settings(
                    DefaultStorageSettings::default().filesystem(
                        FileSystemTableConf::default()
                            .latency_sensitive(FileSystemLatencyWriterConf::default().path(dir))
                            .throughput_sensitive(
                                FileSystemThroughputWriterConf::default().path(dir),
                            ),
                    ),
                ),
        )
}

/// The catalog's described objects, judged as a run judges them
fn described() -> Dataset {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("dataset-objects");
    Dataset::open::<CatalogClient>(&root).expect("the described dataset opens")
}

/// Start a node of the catalog and an object driver of its images through one client
///
/// # Arguments
///
/// * `dir` - Where the node stores its tables
async fn node(dir: &Path) -> (ShoalPool<Catalog>, Arc<Shoal<CatalogClient>>) {
    let mut pool = ShoalPool::<Catalog>::start(conf(dir)).expect("the node starts");
    let addr = pool
        .ready(Duration::from_secs(30))
        .expect("the node is ready")
        .to_string();
    let client = Arc::new(Shoal::<CatalogClient>::new(&addr).await.expect("a client"));
    (pool, client)
}

/// Run one object arm for two seconds, and its seconds added up
///
/// # Arguments
///
/// * `driver` - The driver
/// * `mix` - The arm's mix, by name
async fn run(driver: &ObjectDriver<CatalogClient>, mix: &str) -> Window {
    let arm = ObjectArm {
        mix: mix.parse::<ObjectMix>().expect("a mix"),
        size: None,
        concurrency: 8,
    };
    let clock = ArmClock::start(Duration::from_secs(2));
    let outcome = driver
        .run_arm(
            &arm,
            Duration::ZERO,
            Duration::from_secs(2),
            7,
            &format!("{mix}/sd/c8/none/0"),
            clock,
            &Progress::none(),
        )
        .await;
    Window::sum(&outcome.seconds)
}

/// The driver over a described dataset against a node in process: preload, three mixes, nothing
/// lost, nothing misread
///
/// S15's `object_arm_round_trips_against_one_node`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_arm_round_trips_against_one_node() {
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (pool, client) = node(storage.path()).await;
    let dataset = described();
    assert!(
        dataset.files.is_empty(),
        "a folder of descriptions alone holds no table"
    );
    assert_eq!(dataset.objects.len(), 1);
    assert_eq!(dataset.objects[0].bucket, "Images");
    let driver = ObjectDriver::new(vec![client], dataset.objects.clone(), 50.0);
    // the preload: half the description's objects, each acknowledged
    let (seconds, _) = driver
        .preload(None, 8, &Progress::none())
        .await
        .expect("a first preload puts objects");
    let preload = Window::sum(&seconds);
    let puts = preload
        .kind(&OpKind::Supplied(Arc::from("put")))
        .expect("puts were recorded");
    assert_eq!(puts.total(), 200, "half of 400 objects are preloaded");
    assert_eq!(puts.failed(), 0, "{:?}", puts.errors);
    assert!(
        driver.preload(None, 8, &Progress::none()).await.is_none(),
        "a size is preloaded once"
    );
    // three mixes, every operation answered and every get's bytes the object's
    for mix in ["put100", "get100", "rw50"] {
        let window = run(&driver, mix).await;
        let mut ops = 0;
        for kind in ["put", "get"] {
            if let Some(record) = window.kind(&OpKind::Supplied(Arc::from(kind))) {
                assert_eq!(record.failed(), 0, "{mix} {kind}: {:?}", record.errors);
                assert_eq!(record.misses, 0, "{mix} {kind} missed a preloaded object");
                ops += record.total();
            }
        }
        assert!(ops > 0, "{mix} did nothing");
        // bytes counted where they were sent and where they were received
        if mix != "get100" {
            assert!(window.bytes_sent > 0, "{mix} counted no bytes sent");
        }
        if mix != "put100" {
            assert!(window.bytes_received > 0, "{mix} counted no bytes received");
        }
    }
    // every acknowledged object read back, byte for byte
    let verify = driver.verify(8, &Progress::none()).await;
    assert_eq!(verify.checked, driver.acknowledged());
    assert!(verify.checked >= 200, "{verify:?}");
    assert_eq!(verify.lost, 0, "{verify:?}");
    assert_eq!(verify.misread, 0, "{verify:?}");
    assert!(verify.errors.is_empty(), "{verify:?}");
    drop(pool);
}

/// Flip one byte of an object's bytes wherever they are in a node's files
///
/// Returns how many places were changed.
///
/// # Arguments
///
/// * `dir` - The node's storage
/// * `bytes` - The object's bytes
fn damage(dir: &Path, bytes: &[u8]) -> usize {
    // a window from the middle of the object, long enough to be found nowhere else
    let probe = &bytes[bytes.len() / 2..bytes.len() / 2 + 64];
    let mut changed = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).expect("a storage directory reads") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let mut content = std::fs::read(&path).expect("a storage file reads");
            let mut found = false;
            let mut from = 0;
            while let Some(offset) = content[from..]
                .windows(probe.len())
                .position(|window| window == probe)
            {
                let flip = from + offset + 32;
                content[flip] ^= 0x01;
                found = true;
                from += offset + probe.len();
            }
            if found {
                std::fs::write(&path, &content).expect("a damaged file writes");
                changed += 1;
            }
        }
    }
    changed
}

/// A write acknowledged and then damaged on disk fails the run
///
/// S15's `every_acknowledged_byte_is_read_back`. Objects are put and acknowledged, the node is
/// stopped, one byte of one acknowledged object is flipped in every file it is stored in, and the
/// node is started again on the same directory: reading every acknowledged object back finds it,
/// as a byte that differs, a read that fails or an object that is gone, any of which fails a run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acknowledged_byte_is_read_back() {
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (pool, client) = node(storage.path()).await;
    let dataset = described();
    let mut driver = ObjectDriver::new(vec![client], dataset.objects.clone(), 25.0);
    driver.preload(None, 8, &Progress::none()).await;
    // read back clean first: nothing is wrong yet
    let clean = driver.verify(8, &Progress::none()).await;
    assert_eq!(clean.checked, 100);
    assert_eq!((clean.lost, clean.misread), (0, 0), "{clean:?}");
    assert!(clean.errors.is_empty(), "{clean:?}");
    // the node stopped, so its files are what it left
    pool.exit().expect("the node stops");
    // one acknowledged object's bytes, damaged where they lie
    let object = &dataset.objects[0];
    let bytes = make(object, None, 0);
    let changed = damage(storage.path(), &bytes);
    assert!(changed > 0, "the object's bytes were found in no file");
    // the node again, on the same files, and the same driver through a new client
    let (pool, client) = node(storage.path()).await;
    driver.reconnect(vec![client]);
    let damaged = driver.verify(8, &Progress::none()).await;
    assert_eq!(damaged.checked, 100);
    let failed = damaged.lost + damaged.misread + damaged.errors.values().sum::<u64>();
    assert!(
        failed > 0,
        "a damaged acknowledged object read back as it was put: {damaged:?}"
    );
    drop(pool);
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

/// `shoaladm bench run` over a described dataset runs object arms and records them
///
/// The arms are named `{mix}/s{size}/c{concurrency}/none`; their windows count each kind and the
/// bytes both ways; every acknowledged object is read back; and the capture keeps the description
/// and folds its digest into the dataset's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_run_writes_a_capture() {
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(storage.path())).expect("the node starts");
    let addr = pool
        .ready(Duration::from_secs(30))
        .expect("the node is ready")
        .to_string();
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dataset = format!("{}/dataset-objects", env!("CARGO_MANIFEST_DIR"));
    let dir = out.path().join("objects").display().to_string();
    bench(&[
        "run",
        "--addr",
        &addr,
        "--dataset",
        &dataset,
        "--objects",
        "put100,get100",
        "--object-sizes",
        "4KiB,32KiB",
        "--concurrency",
        "4",
        "--duration",
        "1",
        "--warmup",
        "0",
        "--runs",
        "1",
        "--yes-write",
        "--allow-dirty",
        "--basic",
        "--out",
        &dir,
    ])
    .await
    .expect("the run finishes");
    let capture = Capture::read(&out.path().join("objects")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    let mut ids: Vec<&str> = capture.arms.iter().map(|arm| arm.id.0.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [
            "get100/s32k/c4/none",
            "get100/s4k/c4/none",
            "put100/s32k/c4/none",
            "put100/s4k/c4/none"
        ]
    );
    for arm in &capture.arms {
        let run = &arm.runs[0];
        let kind = if arm.id.0.starts_with("put") {
            "put"
        } else {
            "get"
        };
        let summary = run
            .measured
            .kind(&OpKind::Supplied(Arc::from(kind)))
            .expect("its kind ran");
        assert!(summary.ok > 0, "{} did nothing", arm.id);
        assert_eq!(summary.failed(), 0, "{}: {:?}", arm.id, summary.errors);
        if kind == "put" {
            assert!(
                run.measured.bytes_sent > 0,
                "{} counted no bytes sent",
                arm.id
            );
            let verify = run.verify.as_ref().expect("a put arm is read back");
            assert_eq!(
                (verify.lost, verify.misread),
                (0, 0),
                "{}: {verify:?}",
                arm.id
            );
        } else {
            assert!(
                run.measured.bytes_received > 0,
                "{} counted no bytes received",
                arm.id
            );
        }
    }
    // the description is kept whole, and its digest is the dataset's
    assert!(capture.dataset.tables.is_empty());
    assert_eq!(capture.dataset.objects.len(), 1);
    assert_eq!(capture.dataset.objects[0].bucket, "Images");
    drop(pool);
}

/// A paced stream reads a table beside an object arm, the folder's only table, and no table arm
/// runs, since object mixes named alone choose a run of objects
///
/// The small query's tail beside a stream is half of M13's exit: the object arm is the main load,
/// so the paced stream's table may be the dataset's only one, which F72 refused when the main load
/// was always tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paced_stream_runs_beside_an_object_arm() {
    let storage = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut pool = ShoalPool::<Catalog>::start(conf(storage.path())).expect("the node starts");
    let addr = pool
        .ready(Duration::from_secs(30))
        .expect("the node is ready")
        .to_string();
    // a folder of the bucket's description and one table
    let folder = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let manifest = env!("CARGO_MANIFEST_DIR");
    std::fs::copy(
        format!("{manifest}/dataset-objects/Images.yaml"),
        folder.path().join("Images.yaml"),
    )
    .expect("the description copies");
    std::fs::copy(
        format!("{manifest}/dataset/Review.jsonl"),
        folder.path().join("Review.jsonl"),
    )
    .expect("the table copies");
    let out = tempfile::TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = out.path().join("paced").display().to_string();
    let dataset = folder.path().display().to_string();
    bench(&[
        "run",
        "--addr",
        &addr,
        "--dataset",
        &dataset,
        "--objects",
        "put100",
        "--object-sizes",
        "4KiB",
        "--concurrency",
        "2",
        "--duration",
        "2",
        "--warmup",
        "0",
        "--runs",
        "1",
        "--paced",
        "Review",
        "--paced-rate",
        "20",
        "--yes-write",
        "--allow-dirty",
        "--basic",
        "--out",
        &dir,
    ])
    .await
    .expect("the run finishes");
    let capture = Capture::read(&out.path().join("paced")).expect("a capture");
    assert!(capture.complete, "{:?}", capture.error);
    // the object arm alone, and beside it the paced stream, reading its table at its rate
    let ids: Vec<&str> = capture.arms.iter().map(|arm| arm.id.0.as_str()).collect();
    assert_eq!(ids, ["put100/s4k/c2/none"]);
    let run = &capture.arms[0].runs[0];
    let paced = run.paced.as_ref().expect("the run has its paced stream");
    assert_eq!(paced.table, "Review");
    assert!(paced.measured.read.ok > 0, "{:?}", paced.measured);
    assert_eq!(paced.measured.read.failed(), 0, "{:?}", paced.measured);
    // and the object arm read back everything it put
    let verify = run.verify.as_ref().expect("a put arm is read back");
    assert_eq!((verify.lost, verify.misread), (0, 0), "{verify:?}");
    drop(pool);
}
