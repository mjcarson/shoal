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

use shoal::Conf;
use shoal_core::server::trace::{self, TraceOptions};

use shoal_bench::cli::Format;
use shoal_bench::workloads::harness::metrics::Meters;
use shoal_bench::workloads::harness::seed::Scale;
use shoal_bench::workloads::harness::{self, RunRequest};

/// The name this process reports itself as to a collector
///
/// Not `Shoal`. One collector serves the servers a person runs and the workloads a capture runs,
/// and a chart that cannot separate the two is a chart of neither.
const WORKLOAD_SERVICE_NAME: &str = "shoal-workload";

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

/// How this run wants to report itself to a collector
///
/// Split out from [`run`] so that the one rule this has to get right is testable without starting
/// a server: **the console layer writes to stderr**. The runner harvests the hotpath profile from
/// the last line a workload writes to stdout, so a log line on that stream corrupts the artifact
/// it collects — and does it silently, since a truncated profile reads as code that was never
/// called.
///
/// # Arguments
///
/// * `args` - What this run was asked for
fn trace_options(args: &RunArgs) -> TraceOptions {
    // one collector serves every workload of a capture, so each has to say which one it is
    TraceOptions::new(WORKLOAD_SERVICE_NAME)
        .attribute("shoal.workload", args.id.clone())
        // the capture this run belongs to, which is what joins spans to a committed artifact
        .attribute("shoal.label", args.label.clone().unwrap_or_default())
        .attribute("shoal.scale", format!("{:?}", args.scale).to_lowercase())
        .attribute("shoal.seed", args.seed.to_string())
        .attribute("shoal.port", args.port.to_string())
        // stdout belongs to the artifact, so log lines go to the stream the runner inherits
        .stderr(true)
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
    // read the config for its tracing section alone, before anything is started
    //
    // deliberately not the per workload configuration the harness resolves inside `run`: nothing
    // overrides the tracing section per workload, and the ephemeral workloads have no server
    // configuration at all - but they still have a client half worth tracing, since F16 put spans
    // on it
    let base = Conf::from_file(
        args.conf
            .to_str()
            .with_context(|| format!("config path {} is not valid utf8", args.conf.display()))?,
    )
    .with_context(|| format!("failed to load {}", args.conf.display()))?;
    // install the subscriber the config asks for, holding its guard across the whole run
    //
    // the guard is what flushes: dropping it early stops the export, so it has to outlive both the
    // shards and the client runtime the harness builds around them
    let guard = trace::setup_with(&base, &trace_options(args));
    // ship this run's measurements as they are taken, when the config names a metrics sink
    let meters = Meters::install(&base, args.label.as_deref());
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
    // report what this run measured, now that the server has stopped and nothing here can appear
    // in a number. the flush matters: the reader's interval outlives what is left of this process
    if let Some(meters) = &meters {
        meters.record(&capture);
        meters.flush();
    }
    // flush the spans before the process exits, and before the line below
    //
    // the runner harvests the hotpath profile from the last line on stdout, so anything the
    // subscriber has left to say has to be said first - and on stderr, which `trace_options` is
    // what guarantees
    trace::shutdown(guard);
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{trace_options, RunArgs, WORKLOAD_SERVICE_NAME};
    use shoal_bench::workloads::harness::seed::Scale;

    /// Builds the arguments one run would have been given
    ///
    /// # Arguments
    ///
    /// * `id` - The workload being run
    /// * `label` - The capture it belongs to, if any
    fn args(id: &str, label: Option<&str>) -> RunArgs {
        RunArgs {
            id: id.to_string(),
            conf: PathBuf::from("shoal.yml"),
            json: PathBuf::from("run.json"),
            seed: 42,
            scale: Scale::Full,
            port: 12000,
            label: label.map(str::to_string),
            stage_json: None,
            stage_sample: 1,
        }
    }

    #[test]
    /// The console layer writes to stderr, and this is not a preference
    ///
    /// The runner harvests the hotpath profile from the last line this binary writes to stdout
    /// (`Stdout::LastLine` in `run/exec.rs`) while inheriting stderr unconditionally. A log line
    /// on stdout therefore lands *in* `<label>.hotpath.json`, and a profile that will not parse is
    /// discovered a capture later. Flipping this back is the regression this test exists for.
    fn a_workload_logs_to_stderr() {
        assert!(
            trace_options(&args("macro/insert_unsorted", Some("cap"))).stderr,
            "a workload's log lines would land in the hotpath artifact"
        );
    }

    /// Every span carries which run produced it
    ///
    /// One collector serves every workload of a capture and every server a person runs. Without
    /// these a query can ask about Shoal and nothing narrower, which is the whole reason the
    /// options form exists.
    #[test]
    fn a_workload_names_itself_on_every_span() {
        let options = trace_options(&args("macro/grid/unsorted/r50/1024", Some("trace-check")));
        // not `Shoal`, or a capture's spans and a deployment's spans are one series
        assert_eq!(options.service_name, WORKLOAD_SERVICE_NAME);
        // the attributes a dashboard slices on, checked by name rather than by position
        let found = |key: &str| {
            options
                .attributes
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        assert_eq!(found("shoal.workload").as_deref(), Some("macro/grid/unsorted/r50/1024"));
        assert_eq!(found("shoal.label").as_deref(), Some("trace-check"));
        assert_eq!(found("shoal.scale").as_deref(), Some("full"));
        assert_eq!(found("shoal.seed").as_deref(), Some("42"));
    }

    /// A run taken without a label still names its workload
    ///
    /// The hotpath phase runs a workload with no `--label`, so an unwrap here would panic exactly
    /// once per capture, in the phase whose output is hardest to read.
    #[test]
    fn an_unlabelled_run_still_reports_itself() {
        let options = trace_options(&args("macro/insert_unsorted", None));
        assert_eq!(
            options
                .attributes
                .iter()
                .find(|(name, _)| name == "shoal.workload")
                .map(|(_, value)| value.as_str()),
            Some("macro/insert_unsorted")
        );
    }
}
