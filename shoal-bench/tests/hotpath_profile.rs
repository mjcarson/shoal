//! The hotpath layer against a live server, which nothing else built or ran
//!
//! The `hotpath` build of the engine did not compile from F76 to item 229's fix: laying out the
//! compactor's `fold` future went past the compiler's query depth, because `hotpath` wraps every
//! measured method in a future of its own and the chain down through the archive map's commit
//! was already within one future of the limit without it
//! ([Resolved #229](../../docs/src/appendix/resolved/hotpath-build.md)). Nothing noticed until
//! F80, since the runbook line was the only thing that built the feature and no test ran under
//! it.
//!
//! This is that test. It runs a grid arm at a hundredth of the data through the `shoal-workload`
//! binary built with the feature - the binary a capture's hotpath phase runs - and holds the
//! profile it prints to the scopes of the compactor whose futures overflowed, beside a table's
//! and the storage engine's. It holds the compactor's job loop out of the profile too: a loop
//! that runs for a shard's life reports that life as work, and its skip had been lost for a
//! month before anything ran the feature again
//! ([Resolved #231](../../docs/src/appendix/resolved/compactor-loop-profiled.md)). It is behind
//! `hotpath` in `shoal-bench/Cargo.toml`, so a default `cargo test --workspace` never pays for
//! it; run it with `cargo test -p shoal-bench --features hotpath --test hotpath_profile`.
//!
//! The server runs against a scratch copy of the committed `shoal.yml` whose storage sits under
//! a directory this test owns ([Resolved #97](../../docs/src/appendix/resolved/stage-join-storage.md)).
//! Two settings move as well, so a run this small reaches the compactor's whole path: the intent
//! log rotates at a few KiB, and the archive map commits after every job.

mod common;

use shoal_bench::model::hotpath::HotpathProfile;

/// The arm to profile, the persistent unsorted table at the reference mixture
///
/// Half of its queries are inserts, so its intent logs fill and rotate and the compactor runs.
const ARM: &str = "macro/grid/unsorted/r50/1024";

/// A port no capture hands out
///
/// Beside `stage_join`'s `13871` and `13873` and `x9_report`'s `13872`, and well above the range
/// a capture numbers its workloads' ports from.
const PORT: u16 = 13874;

/// The module every scope of the compactor is named under
///
/// `hotpath` names a measured function by its module path and its name, so every method of
/// `FileSystemCompactor` reports under this prefix.
const COMPACTOR: &str = "shoal_core::server::tables::storage::fs::compactor::";

/// The compactor's scopes a run that compacts has to report
///
/// The chain whose layout overflowed, from a job to the archive map's commit: a job folds an
/// intent log, writes its partitions, then settles the map, which commits it.
const COMPACTOR_SCOPES: [&str; 4] = ["run_job", "compact_intent", "write_partition", "settle_map"];

/// The intent log size the scratch configuration rotates at
///
/// Small enough that a smoke run's inserts rotate every shard's log several times. The committed
/// configuration rotates at 10 MiB, which a smoke run never reaches.
const INTENT_LOG_SIZE: &str = "16KiB";

/// Run the arm through the hotpath build of `shoal-workload` and read back its profile
///
/// The profile is the last line the binary prints to stdout, which is where a capture's hotpath
/// phase takes it from; it is written to a file and checked the way a capture checks one.
///
/// # Arguments
///
/// * `dir` - The directory this test owns, which the storage and the profile go under
fn profile_arm(dir: &std::path::Path) -> HotpathProfile {
    // the committed configuration with its storage moved, and two settings moved so a run this
    // small reaches the compactor's whole path
    let conf = common::scratch_conf(dir, |conf| {
        let filesystem = &mut conf["storage"]["default"]["filesystem"];
        // rotate each shard's intent log after a few KiB of inserts, so the compactor runs
        filesystem["latency_sensitive"]["intent_log_size"] =
            serde_yaml::Value::String(INTENT_LOG_SIZE.to_string());
        // and commit the archive map after every job, so each one reaches the commit
        filesystem["map"]["delta_entries"] = serde_yaml::Value::Number(1.into());
    });
    // the result file the binary insists on, which this test does not read
    let result = dir.join("result.json");
    // the binary a capture's hotpath phase runs, built with the feature this test is behind
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_shoal-workload"))
        .args([
            "run",
            "--id",
            ARM,
            "--conf",
            conf.to_str().expect("the scratch path is utf8"),
            "--seed",
            "1",
            "--scale",
            "smoke",
            "--port",
            &PORT.to_string(),
            "--json",
            result.to_str().expect("the scratch path is utf8"),
        ])
        .current_dir(dir)
        .output()
        .expect("shoal-workload starts");
    // a run that failed has nothing to profile, and its stderr says why
    assert!(
        output.status.success(),
        "{ARM} failed under the hotpath build: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // the profile is the last line on stdout, as a capture takes it
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let last = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .expect("the run printed something");
    // checked the way a capture checks its artifact, which refuses an empty profile by name
    let artifact = dir.join("profile.json");
    std::fs::write(&artifact, last).expect("the profile is written");
    let described = shoal_bench::collect::hotpath::check(&artifact)
        .unwrap_or_else(|error| panic!("the last line is not a profile: {error:#}: {last}"));
    println!("{ARM}: {described}");
    shoal_bench::store::read_json(&artifact).expect("the profile parses")
}

/// A grid arm's hotpath profile names the compactor's scopes, a table's and the storage engine's
///
/// Against the tree before item 229's fix this does not compile: it is the first test built with
/// the feature, so the build is what fails. A fix that compiled by skipping the compactor's
/// methods (`#[hotpath::skip]`) fails here naming the scopes it dropped, and so does a feature
/// that stops being forwarded to `shoal-core`. With the compactor's `start` measured again, it
/// fails naming the loop (item 231).
#[test]
fn a_grid_arm_profiles_the_compactor() {
    // under `target/` rather than `/tmp`, which is usually tmpfs, where glommio silently gives up
    // direct I/O and the server under test is a different server
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temporary directory");
    let profile = profile_arm(dir.path());
    // the server wrote under the scratch storage and nowhere else
    let arm_storage = dir
        .path()
        .join("storage")
        .join(shoal_bench::workloads::harness::conf::slug(ARM));
    assert!(
        arm_storage.is_dir(),
        "the server did not write under the scratch storage at {}",
        arm_storage.display()
    );
    // every scope of the compactor's chain down to the map's commit was entered and reported
    let missing: Vec<&str> = COMPACTOR_SCOPES
        .iter()
        .copied()
        .filter(|scope| {
            profile
                .output
                .get(&format!("{COMPACTOR}{scope}"))
                .is_none_or(|measured| measured.calls == 0)
        })
        .collect();
    assert!(
        missing.is_empty(),
        "the profile has no calls of the compactor's {missing:?}; it reported {:?}",
        profile.output.keys().collect::<Vec<_>>()
    );
    // and not the loop the jobs run in, which lives as long as its shard and would report that
    // lifetime as the costliest scope of the run
    assert!(
        !profile.output.contains_key(&format!("{COMPACTOR}start")),
        "the compactor's job loop is measured, so the profile reports its lifetime as work: {:?}",
        profile.output.get(&format!("{COMPACTOR}start"))
    );
    // and the layers above it, which `measure_all` instruments the same way
    for layer in [
        "shoal_core::server::tables::persistent::unsorted::",
        "shoal_core::server::tables::storage::fs::",
    ] {
        assert!(
            profile
                .output
                .iter()
                .any(|(name, measured)| name.starts_with(layer)
                    && !name.starts_with(COMPACTOR)
                    && measured.calls > 0),
            "the profile has no scope under {layer}; it reported {:?}",
            profile.output.keys().collect::<Vec<_>>()
        );
    }
}
