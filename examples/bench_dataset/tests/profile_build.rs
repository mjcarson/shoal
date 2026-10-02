//! A profile build of the catalog's node, built for real ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! Ignored by default: it is a whole release build of the engine, minutes long. Run it with
//! `cargo test -p bench-dataset --test profile_build -- --ignored` after changing what a
//! profile wrapper generates.

use shoaladm::build::{program_with, Flavor, Role, Target};
use shoaladm::config::Config;
use shoaladm::project::Project;

/// The profile wrapper builds, and the node it builds is jemalloc with its settings exported
#[test]
#[ignore = "a release build of the engine"]
fn the_catalogs_profile_node_builds() {
    let project = Project::locate(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))).expect("the project");
    let schema = project.scan(None).expect("the catalog schema");
    let bin = tempfile::tempdir().unwrap();
    let config = Config {
        bin_dir: Some(bin.path().to_path_buf()),
        ..Config::default()
    };
    let program = program_with(&project, &schema, Role::Node, &Target::Native, &config, None, Flavor::Profile)
        .expect("the profile node builds");
    assert!(program.ends_with("bench-dataset-Catalog-node-native-profile"), "{}", program.display());
    // the settings jemalloc reads are in the program
    let bytes = std::fs::read(&program).unwrap();
    let conf = shoaladm::build::PROFILE_MALLOC_CONF.as_bytes();
    assert!(bytes.windows(conf.len()).any(|window| window == conf), "the profile settings are not in the program");
}
