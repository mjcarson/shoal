//! The stage layer against a live server, for the one thing its unit tests cannot see
//!
//! The eight tests in `workloads::stages` are pure functions over `build_report`: they fabricate a
//! matched pair of halves and check the summary that comes out. Every one of them passed while
//! three of the four workloads the layer profiled produced reports with nothing in them, because
//! nothing anywhere ran a workload and looked at what it wrote
//! ([Resolved #76](../../docs/src/appendix/resolved/stage-join.md)).
//!
//! This is that missing test. It starts a real server, runs one grid arm at a hundredth of the
//! data, and asserts the report has a join in it. It is behind `stage-profile` in
//! `shoal-bench/Cargo.toml`, so a default `cargo test --workspace` never pays for it; run it with
//! `cargo test -p shoal-bench --features stage-profile`.

use shoal_bench::cli::Scale;
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

/// A grid arm's stage report has a join in it
///
/// The defect this reproduces: a grid arm's measured phase ran through a driver with no stage
/// wiring at all, so every server side record was discarded for want of a client half and the
/// report came out with the right schema, the right workload name, a plausible join block and an
/// empty `ops` map. Against the tree before the fix this fails on the first assertion with
/// `joined: 0`.
#[test]
fn a_grid_arm_joins_its_stage_records() {
    let workload = shoal_bench::workloads::find(ARM).expect("the arm is registered");
    let dir = tempfile::tempdir().expect("a temporary directory");
    let stages = dir.path().join("stages.json");
    // a hundredth of the data, which is enough to join on and cheap enough to run in a test
    harness::run(
        workload.as_ref(),
        &RunRequest {
            conf: std::path::PathBuf::from("../shoal.yml"),
            seed: 1,
            scale: Scale::Smoke,
            port: PORT,
            label: Some("stage-join-test".to_string()),
            stage_json: Some(stages.clone()),
            // keep every query, so a smoke run has records to join rather than a handful
            stage_sample: 1,
        },
    )
    .expect("the arm runs");
    let report = shoal_bench::workloads::stages::read_report(&stages).expect("it wrote a report");
    // the defect, in one line
    assert!(
        report.join.joined > 0,
        "the stage layer joined nothing for {ARM}: {:?}",
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
}
