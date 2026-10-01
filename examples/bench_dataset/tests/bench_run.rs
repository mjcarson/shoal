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
            "run", "--addr", &addr, "--dataset", &dataset, "--mixes", "insert100,read100,rw50",
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
