//! The device store's load beside a live server, for what the module's unit tests cannot see
//!
//! The unit tests in `harness::object_load` hold the plan and the percentiles to their arithmetic.
//! None of them starts a pool, so none would notice a device never claimed, an object executor
//! never started, a slice served on another cpu than the one asked for, or a report whose measured
//! window is empty because the harness's marks were not reached. This runs the reference cell at a
//! hundredth of the data beside the load and reads the report it wrote.
//!
//! Behind `object-load` in `shoal-bench/Cargo.toml`, since a default build has no load to run;
//! run it with `cargo test -p shoal-bench --features object-load --test object_load_report`. Its
//! server runs against a scratch copy of the committed `shoal.yml` under a directory it owns, as
//! `stage_join` does.

use shoal_bench::cli::Scale;
use shoal_bench::workloads::harness::object_load::{
    CPU_ENV, DEVICE_ENV, FILESYSTEM_ENV, RATE_ENV, REPORT_ENV, RING_ENV,
};
use shoal_bench::workloads::harness::{self, RunRequest};

/// The arm the load runs beside, X9's
const ARM: &str = "macro/grid/unsorted/r50/1024";

/// A port no capture hands out, and not `stage_join`'s; the one X9's report test held
const PORT: u16 = 13872;

/// Write a scratch copy of the committed `shoal.yml` with its storage under `dir`
///
/// # Arguments
///
/// * `dir` - The directory this test owns, which the storage goes under
fn scratch_conf(dir: &std::path::Path) -> std::path::PathBuf {
    // read the committed config, which is the base every capture starts from
    let committed = std::fs::read_to_string("../shoal.yml").expect("the committed shoal.yml");
    let mut conf: serde_yaml::Value =
        serde_yaml::from_str(&committed).expect("the committed shoal.yml parses");
    // point both writer paths at the scratch storage
    let storage = dir.join("storage");
    let filesystem = &mut conf["storage"]["default"]["filesystem"];
    for writer in ["latency_sensitive", "throughput_sensitive"] {
        filesystem[writer]["path"] = serde_yaml::Value::String(
            storage
                .to_str()
                .expect("the scratch path is utf8")
                .to_string(),
        );
    }
    // write the copy beside the storage it names
    let path = dir.join("shoal.yml");
    std::fs::write(
        &path,
        serde_yaml::to_string(&conf).expect("the scratch config serializes"),
    )
    .expect("the scratch config is written");
    path
}

/// The cpu the object executor is asked for: the highest this process may run on, which the
/// committed configuration's shards leave alone on a host with more cpus than it asks for
fn object_cpu() -> usize {
    // the cpus this test may run on, the highest last
    let online = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
    online - 1
}

/// The reference cell runs beside the store's load, which serves its slice on the cpu asked for
/// and reports what it staged and applied inside the measured phase
#[test]
fn the_load_runs_beside_the_cell_and_reports_its_measured_window() {
    // under `target/` rather than `/tmp`, which is usually tmpfs, where glommio gives up direct I/O
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temporary directory");
    let device = dir.path().join("device");
    std::fs::create_dir_all(&device).expect("the device's directory");
    let report = dir.path().join("object-load.json");
    let cpu = object_cpu();
    // SAFETY: this binary's one test sets the environment before its server and client runtime
    // exist, so no other thread reads it while it is written
    unsafe {
        std::env::set_var(DEVICE_ENV, &device);
        std::env::set_var(CPU_ENV, cpu.to_string());
        std::env::set_var(RATE_ENV, "32");
        std::env::set_var(RING_ENV, "16");
        std::env::set_var(REPORT_ENV, &report);
        // the repository's directories are on btrfs, which the store refuses
        std::env::set_var(FILESYSTEM_ENV, "xfs");
    }
    // a hundredth of the data, with its own storage
    let workload = shoal_bench::workloads::find(ARM).expect("the arm is registered");
    let conf = scratch_conf(dir.path());
    harness::run(
        workload.as_ref(),
        &RunRequest {
            conf,
            seed: 1,
            scale: Scale::Smoke,
            port: PORT,
            label: Some("object-load-report-test".to_string()),
            stage_json: None,
            stage_sample: 1,
            server: shoal_bench::workloads::harness::ServerSource::InProcess,
            cluster: None,
            remotes: Vec::new(),
            driver_address: None,
        },
    )
    .expect("the arm runs beside the object load");
    // the report the load wrote as its server stopped
    let bytes = std::fs::read(&report).expect("the load wrote its report");
    let report: serde_json::Value = serde_json::from_slice(&bytes).expect("the report is json");
    // the slice was served on the cpu asked for, and nowhere else
    assert_eq!(report["served_on"], serde_json::json!([cpu]), "{report}");
    assert_eq!(report["apply_sync"], "shared");
    // it staged and applied whole chunks, and refused none
    let whole = &report["whole"];
    assert!(whole["chunks"].as_u64() > Some(0), "no chunk was written: {whole}");
    assert_eq!(whole["refused"], serde_json::json!({}), "{whole}");
    assert_eq!(whole["apply_us"]["n"], whole["chunks"], "{whole}");
    // and the measured phase was marked around the arm, inside the whole run: a smoke run's is a
    // few tens of milliseconds, so it may hold no chunk of a debug build's
    let measured = &report["measured"];
    assert!(measured["secs"].as_f64() > Some(0.0), "no measured window: {report}");
    assert!(measured["secs"].as_f64() < whole["secs"].as_f64(), "{report}");
    assert!(measured["chunks"].as_u64() <= whole["chunks"].as_u64(), "{report}");
    // the chunks are under the device's slice, a ring of sixteen at most
    let chunks = walk(&device.join("object-load"))
        .into_iter()
        .filter(|path| path.components().any(|part| part.as_os_str() == "chunks"))
        .count();
    assert!((1..=16).contains(&chunks), "{chunks} chunk files");
}

/// Every file under a directory
///
/// # Arguments
///
/// * `dir` - The directory
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}
