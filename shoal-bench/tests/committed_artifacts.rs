//! Every artifact committed under `docs/perf/` parses, and nothing in one has gone unnoticed
//!
//! These tests read the real files rather than a copy of them. A copy would let the committed
//! artifacts rot without anything noticing, which is the failure this whole crate exists to stop
//! happening to the numbers themselves.
//!
//! The corpus is thirty two files: twenty one captured runs, three baselines and eight repeats.

use std::path::{Path, PathBuf};

use shoal_bench::model::macro_layer::{
    MACRO_VERSION_V1, MacroCaptureV1, TMDB_WORKLOAD, Timing,
};
use shoal_bench::registry::Layer;
use shoal_bench::store::Store;

/// Opens a store on the real repository this test is compiled inside
fn repo() -> Store {
    // the manifest directory is `shoal-bench/`, so the repository is one level above it
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench is a workspace member, so it has a parent")
        .to_path_buf();
    Store::new(root)
}

/// Lists every file in a directory whose name ends with a suffix, sorted
///
/// # Arguments
///
/// * `dir` - The directory to list
/// * `suffix` - The suffix a file must end with to be included
fn files_ending_with(dir: &Path, suffix: &str) -> Vec<PathBuf> {
    // an absent directory contributes nothing rather than failing, so a partial checkout still
    // runs the tests that do apply to it
    if !dir.is_dir() {
        return Vec::new();
    }
    // collect the matching entries
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect();
    // sorted so a failure names the same file every time
    found.sort();
    found
}

/// Every committed baseline and repeat parses as a micro capture
#[test]
fn every_committed_micro_capture_parses() {
    let store = repo();
    // baselines, repeats and the micro half of every run all share one schema
    let mut paths = files_ending_with(&store.baselines_dir(), ".json");
    paths.extend(files_ending_with(&store.repeats_dir(), ".micro.json"));
    paths.extend(files_ending_with(&store.runs_dir(), ".micro.json"));
    assert!(
        !paths.is_empty(),
        "no micro captures found under {}",
        store.perf_dir().display()
    );
    // each one must parse and must be a version this tool understands
    for path in &paths {
        let capture = store
            .read_micro(path)
            .unwrap_or_else(|err| panic!("{}: {err:#}", path.display()));
        assert!(
            !capture.benchmarks.is_empty(),
            "{} holds no benchmarks",
            path.display()
        );
    }
}

/// Every committed macro capture parses
#[test]
fn every_committed_macro_capture_parses() {
    let store = repo();
    let paths = files_ending_with(&store.runs_dir(), ".macro.json");
    assert!(
        !paths.is_empty(),
        "no macro captures found under {}",
        store.runs_dir().display()
    );
    // each one must parse, and every workload in it must describe a run that moved rows
    for path in &paths {
        let capture = store
            .read_macro(path)
            .unwrap_or_else(|err| panic!("{}: {err:#}", path.display()));
        assert!(
            !capture.workloads.is_empty(),
            "{} holds no workloads",
            path.display()
        );
        for (id, workload) in &capture.workloads {
            let moved: u64 = workload.counters.values().sum();
            assert!(
                moved > 0,
                "{} says {id} moved no rows",
                path.display()
            );
        }
    }
}

/// Every committed version 1 capture lifts to the one workload it was
///
/// The seven captures taken before purpose built workloads existed are the historical record and
/// are never rewritten. They are read through [`MacroCaptureV1::upgrade`] instead, and this pins
/// what that lift produces: one workload, named `macro/tmdb`, carrying the row counts and both
/// operations the original file recorded. If the lift stopped doing that, those captures would
/// quietly stop joining with each other and their history would be gone.
#[test]
fn every_committed_v1_capture_lifts_to_one_workload() {
    let store = repo();
    let mut lifted = 0;
    for path in files_ending_with(&store.runs_dir(), ".macro.json") {
        // read the raw file to find out which version it actually is
        let body = std::fs::read_to_string(&path).expect("a committed artifact is readable");
        let value: serde_json::Value = serde_json::from_str(&body).expect("it is json");
        if value["version"].as_u64() != Some(u64::from(MACRO_VERSION_V1)) {
            continue;
        }
        lifted += 1;
        let capture = store
            .read_macro(&path)
            .unwrap_or_else(|err| panic!("{}: {err:#}", path.display()));
        assert!(
            capture.is_lifted_v1(),
            "{} did not lift to the single {TMDB_WORKLOAD} workload, it lifted to {:?}",
            path.display(),
            capture.workload_ids()
        );
        let workload = &capture.workloads[TMDB_WORKLOAD];
        // both operations the original file named survive the lift
        assert!(workload.ops.contains_key("insert"), "{}", path.display());
        assert!(workload.ops.contains_key("get"), "{}", path.display());
        // as do both counters, which is what keeps the throughput figure identical
        assert!(
            workload.counters["inserted"] > 0 && workload.counters["retrieved"] > 0,
            "{} lost its row counts in the lift",
            path.display()
        );
        // and it is a saturated, per batch workload, which is what `tmdb` was
        assert_eq!(workload.timing, Timing::PerBatch);
    }
    assert!(
        lifted > 0,
        "no committed version 1 capture was found, so the lift is untested against the record"
    );
}

/// No committed version 1 capture carries a key `MacroCaptureV1` does not name
///
/// This is the drift alarm, and it applies only to version 1. That struct mirrors
/// `shoal::bencher::BenchResult` in another crate, so a field added there would otherwise be
/// silently dropped; instead it lands in the catch-all and fails this test, naming itself.
///
/// Version 2 needs no such alarm: `crate::workloads::harness` builds these very structs, so the
/// writer and the reader cannot disagree.
#[test]
fn no_v1_capture_has_an_unmirrored_field() {
    let store = repo();
    // check every capture rather than stopping at the first, so one run does not hide another
    for path in files_ending_with(&store.runs_dir(), ".macro.json") {
        let body = std::fs::read_to_string(&path).expect("a committed artifact is readable");
        let value: serde_json::Value = serde_json::from_str(&body).expect("it is json");
        if value["version"].as_u64() != Some(u64::from(MACRO_VERSION_V1)) {
            continue;
        }
        let capture: MacroCaptureV1 =
            serde_json::from_value(value).expect("a version 1 capture parses as version 1");
        assert!(
            capture.extra.is_empty(),
            "{} carries {:?}, which shoal_bench::model::macro_layer::MacroCaptureV1 does not \
             mirror - add the field there and to the lift before this artifact is trusted",
            path.display(),
            capture.extra.keys().collect::<Vec<_>>()
        );
    }
}

/// Every committed hotpath profile parses
#[test]
fn every_committed_hotpath_profile_parses() {
    let store = repo();
    let paths = files_ending_with(&store.runs_dir(), ".hotpath.json");
    assert!(
        !paths.is_empty(),
        "no hotpath profiles found under {}",
        store.runs_dir().display()
    );
    // each one must parse and must have attributed time to at least one scope
    for path in &paths {
        let profile = store
            .read_hotpath(path)
            .unwrap_or_else(|err| panic!("{}: {err:#}", path.display()));
        assert!(
            !profile.output.is_empty(),
            "{} attributed time to no scopes at all - the feature was probably not forwarded",
            path.display()
        );
    }
}

/// Every committed stage report parses
///
/// There are none yet: `scripts/bench.sh` produced them and none was ever committed. This test
/// passes vacuously until the first capture lands, and starts checking on the day it does.
#[test]
fn every_committed_stage_report_parses() {
    let store = repo();
    // each one must parse and must have joined the client and server halves of some queries
    for path in files_ending_with(&store.runs_dir(), ".stages.json") {
        let report = store
            .read_stages(&path)
            .unwrap_or_else(|err| panic!("{}: {err:#}", path.display()));
        assert!(
            report.join.joined > 0,
            "{} joined no queries, so it is not a report about that run",
            path.display()
        );
    }
}

/// Labels are discovered from the artifacts that exist, one per capture
#[test]
fn labels_are_discovered_from_the_artifacts() {
    let store = repo();
    let labels = store.labels().expect("the runs directory is readable");
    // the seven captures taken before this tool existed must all still be discoverable
    for expected in [
        "B0-powersave",
        "B1-performance",
        "o3-before",
        "o3-after",
        "o3-after-repeat",
        "o17-after",
        "o17-after-repeat",
    ] {
        assert!(
            labels.iter().any(|label| label == expected),
            "label {expected} was not discovered, found {labels:?}"
        );
    }
}

/// A label resolves to the artifacts it captured, and a baseline resolves ahead of a run
#[test]
fn a_name_resolves_to_a_capture() {
    let store = repo();
    // the frozen baseline exists in both places, and the baseline is the one that should win
    let (path, capture) = store
        .resolve_micro("B1-performance")
        .expect("the frozen baseline resolves");
    assert!(
        path.starts_with(store.baselines_dir()),
        "B1-performance resolved to {} rather than to the baseline",
        path.display()
    );
    // it is the frozen capture, which was taken before the maybe_loaded group was written
    assert_eq!(
        capture.benchmarks.len(),
        35,
        "the frozen baseline is supposed to hold 35 benchmarks"
    );
    // a run only label resolves to the run
    let (path, _) = store
        .resolve_micro("o17-after")
        .expect("a captured run resolves");
    assert_eq!(path, store.run_artifact("o17-after", Layer::Micro));
    // and a name that is neither is an error rather than an empty comparison
    assert!(store.resolve_micro("not-a-capture").is_err());
}

/// The frozen baseline and the trailing one are the pair a comparison defaults to
///
/// They disagree on purpose: the trailing baseline has the `maybe_loaded` group and the frozen one
/// predates it. That difference is what the set-difference reporting exists to surface, so it is
/// pinned here rather than left to be rediscovered.
#[test]
fn the_two_baselines_disagree_by_the_maybe_loaded_group() {
    let store = repo();
    let (_, frozen) = store
        .resolve_micro(shoal_bench::store::FROZEN_BASELINE)
        .expect("the frozen baseline resolves");
    let (_, trailing) = store
        .resolve_micro(shoal_bench::store::TRAILING_BASELINE)
        .expect("the trailing baseline resolves");
    // everything the frozen baseline measures is still measured today
    let gone: Vec<&String> = frozen
        .benchmarks
        .keys()
        .filter(|name| !trailing.benchmarks.contains_key(*name))
        .collect();
    assert!(
        gone.is_empty(),
        "the trailing baseline no longer measures {gone:?}"
    );
    // and the trailing baseline measures twenty four things the frozen one never did
    let added: Vec<&String> = trailing
        .benchmarks
        .keys()
        .filter(|name| !frozen.benchmarks.contains_key(*name))
        .collect();
    assert_eq!(
        added.len(),
        24,
        "expected the maybe_loaded group to be the whole difference, found {added:?}"
    );
}
