//! The stage layer against a live server, for the one thing its unit tests cannot see
//!
//! The ten tests in `workloads::stages` are pure functions over `build_report`: they fabricate a
//! matched pair of halves and check the summary that comes out. Every one of them passed while
//! three of the four workloads the layer profiled produced reports with nothing in them, because
//! nothing anywhere ran a workload and looked at what it wrote
//! ([Resolved #76](../../docs/src/appendix/resolved/stage-join.md)).
//!
//! This is that missing test. It starts a real server, runs a grid arm at a hundredth of the
//! data, and asserts the report has a join in it. It is behind `stage-profile` in
//! `shoal-bench/Cargo.toml`, so a default `cargo test --workspace` never pays for it; run it with
//! `cargo test -p shoal-bench --features stage-profile --test stage_join`.
//!
//! The server it starts runs against a scratch copy of the committed `shoal.yml` whose storage
//! sits under a directory this test owns, so it runs wherever the suite does rather than only on
//! a host with `/opt/shoal` ([Resolved #97](../../docs/src/appendix/resolved/stage-join-storage.md)).
//!
//! It also holds each persistent table's durability phases to the report, one arm a table kind.
//! Those phases are filled by a loop in each table's `get_flushed` that only a `stage-profile`
//! build compiles, and the report builder's own tests stamp them by hand, so this is the one
//! place a loop that stopped compiling or stopped running shows
//! ([Resolved #227](../../docs/src/appendix/resolved/stage-profile-build.md)).

use std::sync::Mutex;

use shoal_bench::cli::Scale;
use shoal_bench::model::stages::StageReport;
use shoal_bench::workloads::harness::{self, RunRequest};

/// The arm to profile, which is the narrowest of the three the stage layer sweeps
///
/// A grid arm rather than `macro/insert_unsorted`: the write path workload drives `drive_with`,
/// which is the one driver that always gathered its client half, so it would have passed against
/// the broken tree.
const ARM: &str = "macro/grid/unsorted/r50/1024";

/// A port no capture hands out
///
/// A workload's position in `workload_ids::IDS` decides the port a capture gives it, counting up
/// from 12000. This sits well above the top of that range so a test and a capture can run at once.
const PORT: u16 = 13871;

/// The sorted table's arm, which holds `PersistentSortedTable::get_flushed`'s durability loop
///
/// The same mixture, width and depth as [`ARM`] on the other persistent table, so the two reports
/// differ in the table that served them and in nothing the stage layer looks at.
const SORTED_ARM: &str = "macro/grid/sorted/r50/1024";

/// The sorted arm's port, beside [`PORT`] and clear of `x9_report`'s `13872`
const SORTED_PORT: u16 = 13873;

/// Held for the whole of an arm, so the arms in this binary run one at a time
///
/// The profiler collects every shard's records into one buffer a process, which the harness
/// resets before an arm's measured phase and drains after it. Two arms at once would each reset
/// and drain the other's records, so the tests serialize on this rather than on the test threads.
static ONE_ARM_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Write a scratch copy of the committed `shoal.yml` with its storage under `dir`
///
/// Every setting the benchmarks run under is kept - the cores, the memory, the writer knobs -
/// and only the two storage paths move, so the arm is measured against the configuration a
/// capture uses while writing nowhere a test may not.
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

/// Run one arm against an in-process server and read back the stage report it wrote
///
/// Every assertion that holds for any arm is made here: the server wrote under the scratch
/// storage, the halves joined, and the report broke both operations of the mixture down.
///
/// # Arguments
///
/// * `arm` - The workload to run
/// * `port` - The port its server binds, which no other test in the suite uses
fn run_arm(arm: &str, port: u16) -> StageReport {
    // one arm at a time, since the profiler's buffer is the process's; a test that failed while
    // holding the lock poisoned it and nothing else, so the next arm runs anyway
    let _arm = ONE_ARM_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let workload = shoal_bench::workloads::find(arm).expect("the arm is registered");
    // under `target/` rather than `/tmp`, which is usually tmpfs, where glommio silently gives up
    // direct I/O and the server under test is a different server
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temporary directory");
    let stages = dir.path().join("stages.json");
    // the committed configuration with its storage moved under this test's directory
    let conf = scratch_conf(dir.path());
    // a hundredth of the data, which is enough to join on and cheap enough to run in a test
    harness::run(
        workload.as_ref(),
        &RunRequest {
            conf,
            seed: 1,
            scale: Scale::Smoke,
            port,
            label: Some("stage-join-test".to_string()),
            stage_json: Some(stages.clone()),
            // keep every query, so a smoke run has records to join rather than a handful
            stage_sample: 1,
            // the arm starts its own server, as every capture does
            server: shoal_bench::workloads::harness::ServerSource::InProcess,
            cluster: None,
            remotes: Vec::new(),
            driver_address: None,
        },
    )
    .expect("the arm runs");
    // the server wrote under the scratch storage and nowhere else: the arm's own subdirectory is
    // there, which is what says a later edit did not point this test back at `/opt/shoal`
    let arm_storage = dir
        .path()
        .join("storage")
        .join(shoal_bench::workloads::harness::conf::slug(arm));
    assert!(
        arm_storage.is_dir(),
        "the server did not write under the scratch storage at {}",
        arm_storage.display()
    );
    let report = shoal_bench::workloads::stages::read_report(&stages).expect("it wrote a report");
    // the defect of Resolved #76, in one line
    assert!(
        report.join.joined > 0,
        "the stage layer joined nothing for {arm}: {:?}",
        report.join
    );
    // and the halves matched, rather than a token few of them lining up. the seed phase's server
    // records are thrown away before the measured phase starts, so nothing should be one sided
    assert_eq!(
        report.join.client_only, 0,
        "the server has no record of a query the client sent"
    );
    assert!(
        report.join.server_only < report.join.joined,
        "most of the server's records found no client half: {:?}",
        report.join
    );
    // a mixture drives both paths, and the report keys its operations the way the server names
    // them rather than the way the workload does
    assert!(
        report.ops.contains_key("get"),
        "the read half of the mixture produced no breakdown: {:?}",
        report.ops.keys().collect::<Vec<_>>()
    );
    assert!(
        report.ops.contains_key("insert"),
        "the write half of the mixture produced no breakdown: {:?}",
        report.ops.keys().collect::<Vec<_>>()
    );
    // and a breakdown with no stages in it is the same empty middle in a different place
    let insert = report.bucket("insert", "all").expect("an `all` bucket");
    assert!(
        !insert.stages.is_empty(),
        "a bucket with no stages is not a breakdown"
    );
    report
}

/// Assert an arm's writes were broken down by the intent log's phases
///
/// A table fills a released write's durability stamps in `get_flushed`. Without that, every
/// write has no `write_submitted`, so the report names its whole wait `durable_rotated` - the
/// stage for a write a rotation released - and none of the phases appear. That is what a loop
/// that does not run looks like, and the report still has stages in it, which is why the join
/// assertions alone never saw it.
///
/// # Arguments
///
/// * `arm` - The workload the report came from, for the messages
/// * `report` - The report the arm wrote
fn assert_durability_phases(arm: &str, report: &StageReport) {
    let insert = report.bucket("insert", "all").expect("an `all` bucket");
    // the committed configuration fsyncs its intent log, so a write that was looked up has all
    // four phases, the write and the sync among them
    for phase in ["durable_write", "durable_sync"] {
        let samples = insert.stages.get(phase).map_or(0, |cost| cost.samples);
        assert!(
            samples > 0,
            "{arm}'s writes have no `{phase}`, so nothing filled their durability phases: {:?}",
            insert.stages.keys().collect::<Vec<_>>()
        );
    }
    // a write is reported whole only when a rotation released it or its window had aged out of
    // the log's timeline, and the report counts both, so anything past them was never looked up
    let rotated = insert
        .stages
        .get("durable_rotated")
        .map_or(0, |cost| cost.samples);
    let explained = report.ops["insert"].rotated + report.join.window_missing;
    assert!(
        rotated <= explained,
        "{arm} reported {rotated} writes whole and only {explained} were rotated or missing \
         their window: {:?}",
        report.join
    );
}

/// A grid arm's stage report has a join in it, and its writes have their durability phases
///
/// The defect this reproduces: a grid arm's measured phase ran through a driver with no stage
/// wiring at all, so every server side record was discarded for want of a client half and the
/// report came out with the right schema, the right workload name, a plausible join block and an
/// empty `ops` map. Against the tree before the fix this fails on the first assertion with
/// `joined: 0`. The durability assertions are item 227's on the unsorted table: with its
/// `get_flushed` loop removed this fails naming `durable_write`.
#[test]
fn a_grid_arm_joins_its_stage_records() {
    // run the unsorted arm and check what any arm's report must hold
    let report = run_arm(ARM, PORT);
    // and that the unsorted table looked each released write up in its log's timeline
    assert_durability_phases(ARM, &report);
}

/// The sorted table's writes have their durability phases too
///
/// `PersistentSortedTable::get_flushed` fills them with a loop of its own, the twin of the
/// unsorted table's, which only a `stage-profile` build compiles. Item 227 was both loops naming
/// five fields of a six field tuple; this is the one test that runs the sorted one.
#[test]
fn a_sorted_grid_arm_reports_its_durability_stages() {
    // run the sorted arm and check what any arm's report must hold
    let report = run_arm(SORTED_ARM, SORTED_PORT);
    // and that the sorted table looked each released write up in its log's timeline
    assert_durability_phases(SORTED_ARM, &report);
}
