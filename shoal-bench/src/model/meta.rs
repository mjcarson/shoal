//! What a capture was taken on: the commit, the tree, the machine and the toolchain
//!
//! This is the file `scripts/bench.sh` never wrote, and its absence is why no committed number
//! can say whether it still describes the current code.
//!
//! It lives beside the layer artifacts rather than inside them, at `runs/<label>.meta.json`. That
//! is deliberate: it describes the *capture*, not the micro numbers, and keeping it out means
//! `<label>.micro.json` stays byte shape compatible at version 1. The frozen baseline
//! `docs/perf/baselines/B1-performance.json` therefore needs no version bump and no migration to
//! stay comparable, which it would have needed if this block were embedded.
//!
//! The seven labels captured before this tool existed simply have no meta file. They are reported
//! as having no provenance, which is deliberately neither fresh nor stale - both would assert
//! something that is not known.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::registry::Layer;

/// The schema version this tool writes and is willing to read
pub const META_VERSION: u32 = 1;

/// What one measurement layer produced during a capture
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LayerRecord {
    /// How many benchmark ids this layer captured, for the micro layer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<usize>,
    /// How many times the workload was run, for the macro layer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<u32>,
    /// Whether this layer captured everything the registry holds for it
    pub complete: bool,
    /// The artifact file this layer wrote, relative to the run directory
    pub artifact: String,
    /// How the client and server halves lined up, for the stage layer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<super::stages::JoinStats>,
}

/// What the code looked like when a capture was taken
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeFacts {
    /// The full commit hash `HEAD` pointed at
    pub head: String,
    /// The abbreviated form of the same commit, for display
    pub head_short: String,
    /// Whether the working tree had uncommitted changes
    pub dirty: bool,
    /// How many paths were modified, if it was dirty
    #[serde(default)]
    pub dirty_paths: usize,
    /// Which layers had an uncommitted source among the ones they measure
    ///
    /// A capture of a layer in this list cannot be located in history at all: the bytes it
    /// measured exist in no commit. That is a stronger warning than being several commits behind,
    /// and it is tracked per layer because a change to the client does not make a micro capture
    /// unlocatable.
    #[serde(default)]
    pub dirty_layers: Vec<Layer>,
    /// Whether the capture was taken with `--allow-dirty`
    #[serde(default)]
    pub allow_dirty: bool,
    /// A hash of `docs/perf/sources.json` itself
    ///
    /// Recorded so that a change to *what each layer is considered to measure* is visible, and
    /// not just a change to the measured code.
    pub sources_manifest_sha: String,
    /// Every source file each layer measures, and its content hash
    #[serde(default)]
    pub sources: BTreeMap<Layer, BTreeMap<String, String>>,
    /// One hash per layer over that layer's sorted `(path, hash)` pairs
    ///
    /// This is what lets a capture taken several commits ago still be reported as describing the
    /// current code. It can only ever *narrow* a stale verdict to an unaffected one - never widen
    /// an unaffected one to fresh - because the source list it is built from is hand maintained
    /// and can be incomplete. See `crate::stale`.
    #[serde(default)]
    pub layer_digest: BTreeMap<Layer, String>,
}

/// What machine and toolchain a capture was taken on
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvFacts {
    /// The host the capture ran on
    pub hostname: String,
    /// The CPU model string out of `/proc/cpuinfo`
    pub cpu_model: String,
    /// How many logical CPUs were online
    pub cpu_online: usize,
    /// The scaling governor cpu0 was set to
    ///
    /// The benchmarks assume `performance`. A capture taken under `powersave` is a different
    /// measurement, which is the whole point of the `B0-powersave` and `B1-performance` pair.
    pub governor: String,
    /// The kernel release
    pub kernel: String,
    /// The full `rustc -vV` version line
    pub rustc: String,
    /// The commit hash of that rustc, which distinguishes two nightlies of the same version
    #[serde(default)]
    pub rustc_commit_hash: String,
    /// The rustflags read out of `.cargo/config.toml`, for display
    ///
    /// Best effort and display only. The comparable signal is [`EnvFacts::cargo_config_sha`],
    /// which does not depend on parsing the file.
    #[serde(default)]
    pub rustflags: String,
    /// The `RUSTFLAGS` environment variable, if it was set
    ///
    /// Recorded apart from the config file because it *overrides* it: a capture taken with it set
    /// is a different build even though `.cargo/config.toml` did not change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rustflags_env: Option<String>,
    /// A hash of `.cargo/config.toml`
    pub cargo_config_sha: String,
    /// A hash of `shoal.yml`
    ///
    /// Changing the benchmark config invalidates every number captured under the old one, so this
    /// is recorded even though it is not itself a staleness signal.
    pub shoal_yml_sha: String,
    /// A hash over the fields two captures must agree on to be comparable
    pub digest: String,
}

/// Everything known about how one capture was taken
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureMeta {
    /// The schema version this file was written with
    pub version: u32,
    /// The name the capture was taken under
    pub label: String,
    /// When it was taken, as an RFC 3339 timestamp in UTC
    pub captured: String,
    /// Which version of this tool took it
    pub tool_version: String,
    /// Whether a filter narrowed the capture to part of the registry
    ///
    /// A partial capture is not a lesser capture, it is a different one. It cannot be promoted to
    /// a baseline, and a comparison against one reports the set difference before the table
    /// rather than after it.
    #[serde(default)]
    pub partial: bool,
    /// The filters the capture was taken with
    #[serde(default)]
    pub filter: Vec<String>,
    /// Whether those filters were matched exactly rather than as substrings
    #[serde(default)]
    pub exact: bool,
    /// How many registry entries the filters selected
    pub selected: usize,
    /// How many entries the registry held at the time
    pub registry_total: usize,
    /// What each layer produced
    pub layers: BTreeMap<Layer, LayerRecord>,
    /// What the code looked like
    pub code: CodeFacts,
    /// What the machine and toolchain were
    pub env: EnvFacts,
}

impl CaptureMeta {
    /// Checks that this metadata's schema version is one this tool understands
    ///
    /// # Arguments
    ///
    /// * `path` - The path this metadata was read from, for the error message
    pub fn check_version(&self, path: &std::path::Path) -> Result<(), String> {
        // refuse anything this tool was not written against
        if self.version != META_VERSION {
            return Err(format!(
                "{}: capture metadata is version {}, this tool reads version {META_VERSION}",
                path.display(),
                self.version
            ));
        }
        Ok(())
    }
}
