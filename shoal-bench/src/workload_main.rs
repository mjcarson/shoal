//! The measured process: starts a server, runs one workload against it, writes what it measured
//!
//! This is a separate binary from the runner, and the reason is the two instrumented layers. Both
//! are built with a feature that changes the code being measured, and `hotpath` has to annotate a
//! `main`. If the workloads lived in the runner, taking a profile would mean rebuilding the tool
//! that is currently executing, and the profile would report that tool's own argument parsing and
//! chart rendering as scopes alongside the server's. Building `--bin shoal-workload --features
//! hotpath` leaves the runner's binary untouched on disk.
//!
//! It runs exactly one workload and exits. See [`shoal_bench::workloads::harness`] for why.

use std::path::PathBuf;

use anyhow::{bail, Context as _, Result};
use clap::{Parser, Subcommand};
use mimalloc::MiMalloc;

use shoal_bench::cli::Format;
use shoal_bench::workloads::harness::seed::Scale;
use shoal_bench::workloads::harness::{self, RunRequest};

/// The same allocator the server uses, so the client half is not measured against a different one
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Runs one of Shoal's purpose built workloads
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// What to do
    #[clap(subcommand)]
    command: Command,
}

/// The things this binary can be asked to do
#[derive(Subcommand, Debug)]
enum Command {
    /// Run one workload and write what it measured
    Run(RunArgs),
    /// Print every workload this build carries
    List(ListArgs),
}

/// What a run was asked for
#[derive(Parser, Debug)]
struct RunArgs {
    /// The workload to run
    #[clap(long)]
    id: String,
    /// The base server configuration to start from
    #[clap(long, default_value = "shoal.yml")]
    conf: PathBuf,
    /// Where to write what this run measured
    #[clap(long)]
    json: PathBuf,
    /// The seed every row of this run derives from
    ///
    /// The same seed produces byte identical data on any machine, which is what makes two runs
    /// differ by machine noise rather than by what they loaded.
    #[clap(long, default_value_t = 42)]
    seed: u64,
    /// How large a run to take
    #[clap(long, value_enum, default_value_t = Scale::Full)]
    scale: Scale,
    /// The port for this workload's server to bind
    #[clap(long, default_value_t = 12000)]
    port: u16,
    /// The name to record this run under
    #[clap(long)]
    label: Option<String>,
    /// Write a per query stage breakdown as json at this path
    ///
    /// Only a binary built with `--features stage-profile` records the stages, so asking this of
    /// one that was not is an error rather than an empty file. A report that is silently missing
    /// its entries reads as "this code was never called".
    #[clap(long)]
    stage_json: Option<PathBuf>,
    /// Keep a stage record for one in every this many queries
    ///
    /// The sample is taken on the query index so that the client and server halves keep the same
    /// queries. Sampling each side independently would leave nothing to join.
    #[clap(long, default_value_t = 1)]
    stage_sample: usize,
}

/// What a listing was asked for
#[derive(Parser, Debug)]
struct ListArgs {
    /// How to print the list
    #[clap(long, value_enum, default_value_t = Format::Text)]
    format: Format,
}

/// Prints every workload this build carries
///
/// This is what the runner would consume if it had to discover the list. It does not - the
/// identifiers are compiled into it through `shoal_bench::workload_ids` - so this exists for a
/// person checking what a build actually contains.
///
/// # Arguments
///
/// * `args` - How to print the list
fn list(args: &ListArgs) -> Result<()> {
    let workloads = shoal_bench::workloads::all();
    match args.format {
        Format::Json => {
            // machine readable, for anything driving this rather than reading it
            let rendered: Vec<serde_json::Value> = workloads
                .iter()
                .map(|workload| {
                    serde_json::json!({
                        "id": workload.id(),
                        "timing": workload.timing().as_str(),
                        "profiles": workload.profiles(),
                        "summary": workload.summary(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&rendered)?);
        }
        Format::Text | Format::Markdown => {
            // one workload per line, with what it is for, which is how a list is usually read
            for workload in &workloads {
                println!(
                    "{:<28} {:<10} {}",
                    workload.id(),
                    workload.timing().as_str(),
                    workload.summary()
                );
            }
        }
    }
    Ok(())
}

/// Runs one workload
///
/// # Arguments
///
/// * `args` - What this run was asked for
fn run(args: &RunArgs) -> Result<()> {
    // find the workload, and say what does exist when it is not one of them
    let Some(workload) = shoal_bench::workloads::find(&args.id) else {
        bail!(
            "unknown workload '{}'\n  this build carries: {}",
            args.id,
            shoal_bench::workload_ids::IDS.join(", ")
        );
    };
    // run it, which starts and stops its own server around it
    let capture = harness::run(
        workload.as_ref(),
        &RunRequest {
            conf: args.conf.clone(),
            seed: args.seed,
            scale: args.scale,
            port: args.port,
            label: args.label.clone(),
            stage_json: args.stage_json.clone(),
            stage_sample: args.stage_sample,
        },
    )?;
    // and write it where the runner will collect it from
    harness::write(&capture, &args.json)
        .with_context(|| format!("failed to write {}", args.json.display()))?;
    println!("wrote {} to {}", args.id, args.json.display());
    Ok(())
}

/// Runs one workload and exits
///
/// `limit = 0` means report every scope. The default is 15, which silently truncates the profile
/// to the fifteen costliest scopes - a profile missing entries without saying so is worse than no
/// profile, because the absence reads as "this code was never called".
#[cfg_attr(
    feature = "hotpath",
    hotpath::main(percentiles = [50, 90, 95, 99], format = "json", limit = 0)
)]
fn main() {
    let cli = Cli::parse();
    // dispatch, and report a failure as a message and an exit code rather than a panic
    let result = match &cli.command {
        Command::Run(args) => run(args),
        Command::List(args) => list(args),
    };
    if let Err(error) = result {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
