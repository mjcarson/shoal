//! The store comparison's fragments, page and mirror of a capture, held to what the tree commits
//! and what loadgen writes ([F94](../../docs/src/features/bench-object-endpoints.md))

use std::path::{Path, PathBuf};

use shoal_bench::render::stores::{self, capture::Capture, fragment_names, included, Results};

/// The repository's root
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// The comparison's page includes every fragment of the set and nothing else, since mdbook renders
/// an include of a missing file as nothing
#[test]
fn the_page_includes_exactly_the_fragments() {
    let page = std::fs::read_to_string(root().join(stores::PAGE)).expect("the comparison's page");
    let mut found = included(&page);
    let mut wanted = fragment_names();
    found.sort();
    wanted.sort();
    assert_eq!(found, wanted);
}

/// Every committed fragment is what the committed captures draw, byte for byte
#[test]
fn the_committed_fragments_are_current() {
    let results = Results::read(&root().join(stores::RESULTS_DIR)).expect("the committed captures read");
    for (name, text) in stores::render(&results).expect("the fragments render") {
        let committed = std::fs::read_to_string(root().join(stores::FRAGMENT_DIR).join(&name))
            .unwrap_or_else(|_| panic!("{name} is not committed; run `shoal-bench render-stores`"));
        assert!(committed == text, "{name} is out of date; run `shoal-bench render-stores`");
    }
}

/// The page's verdict is loadgen's `judge` with the other store as the baseline and Shoal as the
/// candidate, so `shoaladm bench compare` and the page cannot disagree about one pair of captures
#[test]
fn the_verdict_is_loadgens_judge() {
    use shoal_bench::render::stores::verdict::{judge, Interval, Verdict};
    use shoal_loadgen::compare;
    // the other store's interval, then Shoal's: apart either way, touching, inside, and far apart
    let cases = [
        ((100.0, 110.0), (121.0, 130.0)),
        ((100.0, 110.0), (40.0, 50.0)),
        ((100.0, 110.0), (110.0, 120.0)),
        ((100.0, 110.0), (102.0, 104.0)),
        ((18.8, 20.0), (97.3, 107.0)),
    ];
    for ((other_low, other_high), (shoal_low, shoal_high)) in cases {
        for higher_is_better in [true, false] {
            // the page's answer, whose median plays no part in a verdict
            let other = Interval { low: other_low, median: other_low, high: other_high };
            let shoal = Interval { low: shoal_low, median: shoal_low, high: shoal_high };
            let ours = judge(other, shoal, higher_is_better);
            // and loadgen's, in the page's terms
            let theirs = match compare::judge(
                compare::Interval { low: other_low, high: other_high },
                compare::Interval { low: shoal_low, high: shoal_high },
                higher_is_better,
            ) {
                compare::Verdict::Better { gap_pct } => Verdict::Ahead(gap_pct),
                compare::Verdict::Worse { gap_pct } => Verdict::Behind(gap_pct),
                compare::Verdict::NoDifference => Verdict::Within,
                compare::Verdict::Absent => panic!("both sides ran"),
            };
            assert_eq!(ours, theirs, "{other_low}-{other_high} against {shoal_low}-{shoal_high}, higher better {higher_is_better}");
        }
    }
}

/// The mirror reads what loadgen writes: a run's windows, totals and every host counter
#[test]
fn the_mirror_reads_what_loadgen_writes() {
    use shoal_loadgen::results::{
        DeviceCounters, HostCpu, HostDevices, InterfaceCounters, ProcessCounters, RunResult, RunTotals, StoreFacts,
    };
    use shoal_loadgen::window::WindowSummary;
    // a run as loadgen writes one, from a run of before F94 with every F94 field filled in
    let mut measured = WindowSummary::default();
    let put = measured.kinds.entry("put".to_string()).or_default();
    put.ok = 300;
    put.per_sec = 10.0;
    put.latency.p99_ms = 42.0;
    measured.sent_per_sec = 10.0 * 1048576.0;
    let mut run: RunResult = serde_json::from_value(serde_json::json!({
        "run": 1, "order": 0, "started_at": "", "measured": measured, "warmup": WindowSummary::default(),
        "series": [], "ended_early": null, "feeds": {}, "wrapped": false, "verify": null, "event": null,
        "server_series": [], "driver_cpu_peak_pct": 1.0, "progress_dropped": 0
    }))
    .unwrap();
    run.totals = Some(RunTotals { sent: 300 << 20, received: 0 });
    run.devices = vec![HostDevices {
        host: "titan".to_string(),
        devices: vec![DeviceCounters { device: "dm-3".to_string(), written_bytes: 900 << 20, secs: 35.0, ..DeviceCounters::default() }],
        cpu: Some(HostCpu { busy_secs: 12.0, total_secs: 280.0 }),
        interfaces: vec![InterfaceCounters { name: "enp3s0".to_string(), received_bytes: 0, sent_bytes: 600 << 20 }],
        processes: vec![ProcessCounters { name: "minio".to_string(), cpu_secs: 3.0, peak_resident_bytes: 512 << 20, ..ProcessCounters::default() }],
        ..HostDevices::default()
    }];
    let mut capture = serde_json::json!({
        "format": 1, "label": "x", "complete": true,
        "provenance": {"store": StoreFacts { system: "minio".to_string(), ..StoreFacts::default() }},
        "spec": shoal_loadgen::spec::BenchSpec::default(),
        "dataset": {"digest": "d"},
        "arms": [{"id": "put100/s1m/c16/none", "runs": [run]}]
    });
    // the capture's other fields are loadgen's own defaults
    capture["preload"] = serde_json::Value::Null;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("minio-ec21.json");
    std::fs::write(&path, capture.to_string()).unwrap();
    let read = Capture::read(&path).expect("the mirror reads it");
    assert_eq!(read.system(), "minio");
    let run = &read.arms[0].runs[0];
    assert_eq!(run.kind("put").unwrap().latency.p99_ms, 42.0);
    assert_eq!(run.bytes_sent(), 300 << 20);
    assert_eq!(run.device_bytes_per_sent_byte(), Some(3.0));
    assert_eq!(run.wire_bytes_per_moved_byte(), Some(2.0));
    assert_eq!(run.peak_resident(), Some(512 << 20));
    // three cpu-seconds over a run of 35 s at ten puts a second
    assert!((run.cpu_secs_per_kop().unwrap() - 3.0 * 1000.0 / 350.0).abs() < 1e-9);
}
