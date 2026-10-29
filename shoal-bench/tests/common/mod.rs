//! What the tests that start a server against the committed configuration share
//!
//! Each such test writes a scratch copy of `shoal.yml` whose storage sits under a directory the
//! test owns, so it runs wherever the suite does rather than only on a host with `/opt/shoal`
//! ([Resolved #97](../../../docs/src/appendix/resolved/stage-join-storage.md)). It began in
//! `stage_join.rs` and moved here when `hotpath_profile.rs` needed the same copy
//! ([Resolved #229](../../../docs/src/appendix/resolved/hotpath-build.md)).

/// Write a scratch copy of the committed `shoal.yml` with its storage under `dir`
///
/// Every setting the benchmarks run under is kept - the cores, the memory, the writer knobs -
/// and only the two storage paths move, so the arm runs against the configuration a capture
/// uses while writing nowhere a test may not. A test that needs a setting moved for its own
/// sake moves it in `edit`, which sees the copy after its paths have moved.
///
/// # Arguments
///
/// * `dir` - The directory this test owns, which the storage goes under
/// * `edit` - Any further change this test makes to the copy
pub fn scratch_conf(
    dir: &std::path::Path,
    edit: impl FnOnce(&mut serde_yaml::Value),
) -> std::path::PathBuf {
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
    // and whatever else this test needs moved
    edit(&mut conf);
    // write the copy beside the storage it names
    let path = dir.join("shoal.yml");
    std::fs::write(
        &path,
        serde_yaml::to_string(&conf).expect("the scratch config serializes"),
    )
    .expect("the scratch config is written");
    path
}
