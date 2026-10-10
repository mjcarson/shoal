//! X9's object work beside a live server, for what its unit tests cannot see
//!
//! The unit tests in `shoal_core::server::x9` hold the plan, the schedule, the steps and the
//! windows to their arithmetic. None of them starts a pool, so none would notice a runner that
//! was never spawned, a third queue nobody created, an own core that never started, or a report
//! whose measured window is empty because the harness's marks were not reached. This runs the
//! reference cell at a hundredth of the data twice, once with the work on the table shards and
//! once on a core of its own, and reads the report each wrote.
//!
//! Behind `x9` in `shoal-bench/Cargo.toml`, since a default build has no object work to run; run
//! it with `cargo test -p shoal-bench --features x9 --test x9_report`. Its server runs against a
//! scratch copy of the committed `shoal.yml` under a directory it owns, as `stage_join` does.

use shoal_bench::cli::Scale;
use shoal_bench::workloads::harness::{self, RunRequest};

/// The arm X9 runs beside
const ARM: &str = "macro/grid/unsorted/r50/1024";

/// A port no capture hands out, and not `stage_join`'s
const PORT: u16 = 13872;

/// The unit every phase runs at
const UNIT: u64 = 64 << 10;

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

/// Run the arm once with the object work placed at `place`, and read the report it wrote
///
/// # Arguments
///
/// * `dir` - The directory this test owns
/// * `place` - What `SHOAL_X9_PLACE` asks for
fn run_placed(dir: &std::path::Path, place: &str) -> serde_json::Value {
    // the pool's directory and the report, one of each a phase
    let tag = place.replace(':', "-");
    let pool = dir.join(format!("pool-{tag}"));
    std::fs::create_dir_all(&pool).expect("the pool directory");
    let report = dir.join(format!("x9-{tag}.json"));
    // SAFETY: this binary's one test runs its phases in turn, and the previous phase's server
    // and client runtime are gone before this one starts, so no other thread reads the
    // environment while it is written
    unsafe {
        std::env::set_var(shoal::server::x9::PLACE_ENV, place);
        std::env::set_var(shoal::server::x9::RATE_ENV, "16");
        std::env::set_var(shoal::server::x9::UNIT_ENV, UNIT.to_string());
        std::env::set_var(shoal::server::x9::DIR_ENV, &pool);
        std::env::set_var(shoal::server::x9::REPORT_ENV, &report);
        std::env::set_var(shoal::server::x9::RING_ENV, "8");
    }
    // a hundredth of the data, with its own storage
    let workload = shoal_bench::workloads::find(ARM).expect("the arm is registered");
    let conf = scratch_conf(dir);
    harness::run(
        workload.as_ref(),
        &RunRequest {
            conf,
            seed: 1,
            scale: Scale::Smoke,
            port: PORT,
            label: Some(format!("x9-report-test-{tag}")),
            stage_json: None,
            stage_sample: 1,
            server: shoal_bench::workloads::harness::ServerSource::InProcess,
            cluster: None,
            remotes: Vec::new(),
            driver_address: None,
        },
    )
    .expect("the arm runs beside the object work");
    // the report the pool wrote as it exited
    let bytes = std::fs::read(&report).expect("the pool wrote its x9 report");
    serde_json::from_slice(&bytes).expect("the report is json")
}

/// Assert what every runner of a report did
///
/// # Arguments
///
/// * `report` - The report
/// * `label` - What every runner's label starts with
fn assert_runners(report: &serde_json::Value, label: &str) {
    let runners = report["runners"].as_array().expect("runners");
    assert!(!runners.is_empty(), "no runner registered: {report}");
    for runner in runners {
        // named for where it ran
        let name = runner["label"].as_str().expect("a label");
        assert!(name.starts_with(label), "{name} is not a {label} runner");
        // it took stripes in and wrote every unit of each
        let whole = &runner["whole"];
        let stripes = whole["stripes"].as_u64().expect("stripes");
        assert!(stripes > 0, "{name} finished no stripe: {whole}");
        assert_eq!(
            whole["written_bytes"].as_u64(),
            Some(stripes * 6 * UNIT),
            "{name} wrote other than six units a stripe"
        );
        assert_eq!(whole["data_bytes"].as_u64(), Some(stripes * 4 * UNIT));
        // fourteen steps a stripe, each offered back
        assert_eq!(whole["yields_offered"].as_u64(), Some(stripes * 14));
        assert!(whole["hold"]["n"].as_u64() >= Some(stripes));
        // and the measured phase was marked around it
        let measured = &runner["measured"];
        assert!(
            measured["secs"].as_f64() > Some(0.0),
            "{name} has no measured window: {runner}"
        );
    }
}

/// The reference cell runs beside object work on the table shards and on a core of its own, and
/// each run reports what its runners did inside the measured phase
#[test]
fn object_work_runs_beside_the_cell_and_reports_its_measured_window() {
    // under `target/` rather than `/tmp`, which is usually tmpfs, where glommio gives up direct
    // I/O and the x9 run refuses its pool
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temporary directory");
    // on the table shards: one runner a shard, each at its share, each with its own ring file
    let shards = run_placed(dir.path(), "shards");
    assert_eq!(shards["placement"], "TableShards");
    assert_runners(&shards, "shard-");
    let runners = shards["runners"].as_array().expect("runners").len();
    assert_eq!(
        Some(runners),
        shards["shard_cpus"].as_array().map(Vec::len),
        "a runner a shard"
    );
    for runner in shards["runners"].as_array().expect("runners") {
        let label = runner["label"].as_str().expect("a label");
        assert!(
            dir.path()
                .join("pool-shards")
                .join(format!("x9-{label}.ring"))
                .is_file(),
            "{label} wrote no ring"
        );
    }
    // on a core of its own: cpu 0, which no shard ever runs on, at the whole rate
    let own = run_placed(dir.path(), "core:0");
    assert_eq!(own["placement"]["OwnCore"], 0);
    assert_runners(&own, "core-0");
    assert_eq!(own["runners"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        own["runners"][0]["cpu"], 0,
        "the executor was not on its own core"
    );
}
