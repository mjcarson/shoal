//! The command line surface
//!
//! Filtering is modelled on `cargo test`: positional arguments are substrings, matched against a
//! benchmark's identifier, and any of them matching selects it. `--exact` switches to equality.
//! A filter that matches nothing is an error rather than a silent no-op, because a capture that
//! quietly measured zero benchmarks looks exactly like one that measured all of them.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::registry::Layer;

/// Runs Shoal's benchmarks, stores their results, compares them and renders the book page
#[derive(Parser, Debug)]
#[command(name = "shoal-bench", author, version, about)]
pub struct Cli {
    /// The repository to work in, defaulting to the workspace above the current directory
    #[clap(long, global = true)]
    pub repo: Option<PathBuf>,
    /// What to do
    #[command(subcommand)]
    pub command: Command,
}

/// Everything this tool can be asked to do
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Print the benchmarks a filter selects, without running anything
    List(ListArgs),
    /// Capture a run and store it with its provenance
    Run(RunArgs),
    /// Compare a capture against one or more baselines
    Compare(CompareArgs),
    /// Report what has been captured and whether it still describes the current code
    Status(StatusArgs),
    /// Regenerate the book's performance pages
    Render(RenderArgs),
    /// Advance a baseline to a captured run
    Promote(PromoteArgs),
}

/// How many times a capture runs each workload before taking its median
///
/// Named rather than written twice, because `list --groups` estimates what a capture would cost and
/// an estimate taken at a different run count than the capture uses is not an estimate of it.
pub const DEFAULT_RUNS: u32 = 5;

/// How a filter selects benchmarks, shared by `list` and `run`
#[derive(clap::Args, Debug, Clone, Default)]
pub struct Selection {
    /// Substrings to match against a benchmark's identifier, any of which selects it
    #[clap(value_name = "FILTER")]
    pub filters: Vec<String>,
    /// Match the filters exactly rather than as substrings
    #[clap(long)]
    pub exact: bool,
    /// Restrict the selection to these layers
    #[clap(long = "layer", value_name = "LAYER")]
    pub layers: Vec<Layer>,
    /// Restrict the selection to these named groups, repeatable
    ///
    /// A group is a set of benchmarks that answers one question, declared in `crate::groups` and
    /// listed by `list --groups`. Several groups are combined with or; the result intersects with
    /// `--layer` and with the positional filters, the same way those two intersect with each other.
    /// Selecting a group narrows what a capture measures and changes nothing about how it runs -
    /// one workload at a time, exactly as a full capture does.
    ///
    /// The explicit `id` is load bearing: clap keys a flattened argument by its field name, and
    /// `list` carries its own `--groups` flag in a field of the same name. Without this the two
    /// collide at runtime rather than at compile time.
    #[clap(long = "group", value_name = "GROUP", id = "group")]
    pub groups: Vec<String>,
}

/// How a command that prints a report should print it
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[clap(rename_all = "lowercase")]
pub enum Format {
    /// Aligned columns for a terminal
    Text,
    /// Machine readable
    Json,
    /// A markdown table, which is what the generated page embeds
    Markdown,
}

/// Arguments to `shoal-bench list`
#[derive(clap::Args, Debug)]
pub struct ListArgs {
    /// Which benchmarks to print
    #[command(flatten)]
    pub selection: Selection,
    /// How to print them
    #[clap(long, value_enum, default_value = "text")]
    pub format: Format,
    /// Rediscover the micro benchmarks instead of using the cached list
    #[clap(long)]
    pub refresh: bool,
    /// Print the declared groups and what each one answers, instead of the benchmarks
    ///
    /// Reads the committed artifacts to estimate what a capture of each would cost, which is the
    /// number worth having before choosing one.
    #[clap(long)]
    pub groups: bool,
}

/// How much data a workload builds, and how hard it drives it
///
/// A capture runs at [`Scale::Full`]; someone iterating on a workload runs at [`Scale::Smoke`] so a
/// mistake costs seconds rather than minutes. The two are never compared - the scale is recorded in
/// the artifact and a comparison joins on it, because a p99 over 2,000 rows is not a smaller
/// version of a p99 over 200,000, it is a different measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum Scale {
    /// Enough to prove a workload runs, and not enough to measure anything
    Smoke,
    /// What a capture uses
    Full,
}

impl Scale {
    /// The lowercase name of this scale, which is also the key it is recorded under
    pub fn as_str(&self) -> &'static str {
        // the stored name is the displayed name, so there is only one spelling to remember
        match self {
            Scale::Smoke => "smoke",
            Scale::Full => "full",
        }
    }

    /// How many rows a workload should build at this scale
    ///
    /// # Arguments
    ///
    /// * `full` - How many rows the workload wants at [`Scale::Full`]
    pub fn rows(&self, full: u64) -> u64 {
        // a smoke run is two orders of magnitude smaller, floored so it is never empty
        match self {
            Scale::Smoke => (full / 100).max(100),
            Scale::Full => full,
        }
    }
}

/// Arguments to `shoal-bench run`
#[derive(clap::Args, Debug)]
pub struct RunArgs {
    /// Which benchmarks to run
    #[command(flatten)]
    pub selection: Selection,
    /// The name to store this capture under
    #[clap(long)]
    pub label: String,
    /// How many times to run each workload before taking its median
    #[clap(long, default_value_t = DEFAULT_RUNS)]
    pub runs: u32,
    /// The server configuration to run against
    #[clap(long, default_value = "shoal.yml")]
    pub conf: PathBuf,
    /// The seed every workload derives its rows from
    ///
    /// The same seed produces byte identical data on any machine. Changing it changes what the
    /// numbers mean, the same way changing `shoal.yml` does, so two captures taken under different
    /// seeds are not comparable and the seed is recorded in every artifact.
    #[clap(long, default_value_t = 42)]
    pub seed: u64,
    /// How large a run to take
    ///
    /// `full` is what a capture uses. `smoke` is two orders of magnitude smaller and exists so
    /// that iterating on a workload costs seconds - it proves a workload runs and measures
    /// nothing, and the scale is recorded so the two can never be compared.
    #[clap(long, value_enum, default_value_t = Scale::Full)]
    pub scale: Scale,
    /// Where to write the artifacts
    #[clap(long)]
    pub out: Option<PathBuf>,
    /// Print every command that would run and write nothing
    #[clap(long)]
    pub dry_run: bool,
    /// Capture even though the working tree has uncommitted changes
    ///
    /// Recorded in the capture's provenance when used. A measurement taken on a dirty tree cannot
    /// be located in history, so it is reported as uncommitted rather than as fresh.
    #[clap(long)]
    pub allow_dirty: bool,
    /// Keep criterion's previous output instead of clearing it first
    ///
    /// Criterion keeps a directory per benchmark id forever, so a benchmark that was renamed or
    /// removed keeps reporting its last result into every capture taken afterwards. Clearing is
    /// the default for that reason.
    #[clap(long)]
    pub keep_criterion: bool,
    /// Wipe the storage directory even though it does not look like a Shoal store
    #[clap(long)]
    pub force_wipe: bool,
    /// Leave the last instrumented build in place instead of rebuilding uninstrumented
    ///
    /// Only for debugging the instrumented builds. Without the restore, the next manual run of
    /// the example silently measures a profiling build.
    #[clap(long)]
    pub no_restore: bool,
}

/// Arguments to `shoal-bench compare`
#[derive(clap::Args, Debug)]
pub struct CompareArgs {
    /// The capture to judge, as a label or a path
    pub run: String,
    /// What to judge it against, as a label or a path, repeatable
    ///
    /// Defaults to the frozen baseline and the trailing one. One without the other misleads:
    /// against the frozen baseline alone a regression hides inside an earlier win, and against
    /// the trailing baseline alone a series of neutral looking changes can drift a long way.
    #[clap(long = "against", value_name = "BASELINE")]
    pub against: Vec<String>,
    /// Override the tiered noise band with one flat percentage
    #[clap(long)]
    pub noise_pct: Option<f64>,
    /// Restrict the comparison to these layers
    #[clap(long = "layer", value_name = "LAYER")]
    pub layers: Vec<Layer>,
    /// How to print the comparison
    #[clap(long, value_enum, default_value = "text")]
    pub format: Format,
    /// Exit non-zero if anything moved outside the noise band in the slow direction
    #[clap(long)]
    pub fail_on_regression: bool,
}

/// Arguments to `shoal-bench status`
#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Only report these captures, repeatable
    #[clap(long = "label", value_name = "LABEL")]
    pub labels: Vec<String>,
    /// How to print the report
    #[clap(long, value_enum, default_value = "text")]
    pub format: Format,
}

/// Arguments to `shoal-bench render`
#[derive(clap::Args, Debug)]
pub struct RenderArgs {
    /// The directory to write the pages into, defaulting to `docs/src/performance`
    #[clap(long)]
    pub out: Option<PathBuf>,
    /// Regenerate into memory and fail if any page differs from what is committed, writing nothing
    #[clap(long)]
    pub check: bool,
    /// The frozen baseline to draw against
    #[clap(long, default_value = crate::store::FROZEN_BASELINE)]
    pub baseline: String,
    /// The trailing baseline to draw against
    #[clap(long, default_value = crate::store::TRAILING_BASELINE)]
    pub trailing: String,
    /// The capture to render the current numbers from, defaulting to the most recent
    #[clap(long)]
    pub current: Option<String>,
}

/// Arguments to `shoal-bench promote`
#[derive(clap::Args, Debug)]
pub struct PromoteArgs {
    /// The capture to promote
    pub label: String,
    /// The baseline to advance
    #[clap(long, default_value = crate::store::TRAILING_BASELINE)]
    pub to: String,
    /// Promote a capture that only covers part of the registry
    ///
    /// Refused by default: a baseline missing benchmarks silently narrows every comparison taken
    /// against it afterwards, and a benchmark that vanished from a comparison is how a regression
    /// gets missed.
    #[clap(long)]
    pub force_partial: bool,
    /// Promote a capture that no longer describes the current code
    #[clap(long)]
    pub allow_stale: bool,
}
