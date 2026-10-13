//! The X4 spike: erasure coding crates fed the same buffers, checked, and timed on one core
//!
//! [X4](../../docs/src/object-storage/spikes.md) asks which family of code, which crate and what
//! geometry an erasure coded pool is built on, and [S8](../../docs/src/object-storage/erasure-coding.md)
//! says what the code has to be: systematic, decodable from any k chunks, and with an update form.
//! This program answers the half of that a measurement can. Every candidate S18 pinned is adapted
//! to one trait and fed the same buffers, and three things are recorded, in this order:
//!
//! - **facts**: the threads a candidate starts and what one call of each operation allocates
//! - **check**: every loss pattern of every layout up to 6+3 and the two wide ones, decoded and
//!   compared byte for byte; for random coefficients, the fraction of k-sets that fail, counted;
//!   partial updates held to a fresh encode; digests of fixed inputs, which `report` compares
//!   across hosts and builds; and which candidates write the same parity bytes
//! - **speed**: GiB a second for one pinned core, for encode, decode with 1 to m chunks lost,
//!   a rebuild of one chunk and an update of one, at 2+1 to 10+4 and units of 4 KiB to 1 MiB,
//!   each cell three times with the candidates interleaved
//!
//! `kernels`, asked for by name, forces `rusty_erasure` through every kernel set the cpu has
//! (scalar, SSSE3, AVX2, GFNI), so that what an instruction set buys is read on one host.
//!
//! Nothing here is a capture. It is built for `znver1` and run on titan and europa, built natively
//! and run on europa, and its tables are pasted into
//! `docs/src/object-storage/erasure-coding-crates.md` with the host, cpu, build and governor they
//! came from. `shoal-bench` measures what is compared across commits; this measures what decides a
//! design.
//!
//! What it does not measure: a code inside a node, where the bytes arrive off a socket and leave
//! for a device, and the executor shares its core with tables; recoding as a repair protocol across
//! nodes; and any crate S18 did not pin.

mod alloc_count;
mod buffers;
mod check;
mod codes;
mod facts;
mod record;
mod report;
mod speed;

use std::path::PathBuf;

use buffers::Arenas;
use record::RunRecord;

#[global_allocator]
static ALLOC: alloc_count::CountingAlloc = alloc_count::CountingAlloc;

/// What the command line asked for
struct Args {
    /// The subcommand: all, facts, check, speed or report
    command: String,
    /// The core to pin to
    core: usize,
    /// Where the run's JSON goes
    out: Option<PathBuf>,
    /// Whether to run the quick pass
    quick: bool,
    /// For report: the runs to read
    inputs: Vec<PathBuf>,
    /// For report: write one run's every cell instead of the summary
    full: bool,
}

/// Read the command line
fn args() -> Result<Args, String> {
    let mut args = Args {
        command: "all".to_string(),
        core: 2,
        out: None,
        quick: false,
        inputs: Vec::new(),
        full: false,
    };
    let mut words = std::env::args().skip(1);
    while let Some(word) = words.next() {
        match word.as_str() {
            "all" | "facts" | "check" | "speed" | "kernels" | "report" => args.command = word,
            "--core" => {
                args.core = words
                    .next()
                    .and_then(|core| core.parse().ok())
                    .ok_or("--core takes a core number")?;
            }
            "--out" => args.out = Some(words.next().ok_or("--out takes a path")?.into()),
            "--quick" => args.quick = true,
            "--full" => args.full = true,
            "-h" | "--help" => {
                return Err("usage: shoal-spike-erasure [all|facts|check|speed|kernels] [--core N] [--out run.json] [--quick]\n       shoal-spike-erasure report [--full] run.json...".to_string());
            }
            path if args.command == "report" => args.inputs.push(path.into()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

/// Read every run named and print the summary, or one run's every cell
///
/// # Arguments
///
/// * `args` - The command line
fn report(args: &Args) -> Result<(), String> {
    // every run, in the order named
    let mut runs = Vec::new();
    for path in &args.inputs {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let run: RunRecord =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        runs.push(run);
    }
    if runs.is_empty() {
        return Err("report takes one or more run files".to_string());
    }
    if args.full {
        for run in &runs {
            print!("{}", report::full(run));
        }
    } else {
        print!("{}", report::summary(&runs));
    }
    Ok(())
}

/// Run the spike, or report on runs
fn main() {
    let args = match args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    // a report reads runs and measures nothing
    if args.command == "report" {
        if let Err(message) = report(&args) {
            eprintln!("{message}");
            std::process::exit(1);
        }
        return;
    }
    // everything else runs on one pinned core
    if let Err(e) = facts::pin(args.core) {
        eprintln!("could not pin to core {}: {e}", args.core);
        std::process::exit(1);
    }
    let plan = speed::Plan::new(args.quick);
    let arena_mib = if args.quick { 32 } else { 128 };
    let mut record = RunRecord {
        labels: facts::labels(
            args.core,
            plan.runs,
            plan.budget.as_millis() as u64,
            arena_mib,
            args.quick,
            &args.command,
        ),
        ..RunRecord::default()
    };
    eprintln!(
        "{} | {} | target-cpu={} | rse -march={} | governor {} | core {} | {} | {}",
        record.labels.host,
        record.labels.cpu,
        record.labels.target_cpu,
        record.labels.rse_arch,
        record.labels.governor,
        record.labels.core,
        record.labels.features.join(","),
        record.labels.rustc
    );
    // facts, then correctness, then speed, as asked
    if matches!(args.command.as_str(), "all" | "facts") {
        record.facts = facts::run();
    }
    if matches!(args.command.as_str(), "all" | "check") {
        record.check = Some(check::run(args.quick));
    }
    if matches!(args.command.as_str(), "all" | "speed") {
        // the arenas are allocated and touched once, before anything is timed
        let mut arenas = Arenas::new(arena_mib << 20, 2 * (arena_mib << 20), 1 << 20);
        record.speed = speed::run(&plan, &mut arenas);
    }
    // the kernels pass is asked for by name and is not part of `all`
    if args.command == "kernels" {
        let mut arenas = Arenas::new(arena_mib << 20, 2 * (arena_mib << 20), 1 << 20);
        record.speed = speed::kernels(&plan, &mut arenas);
    }
    // the threads after every candidate ran, which is the whole run's answer to "does it spawn"
    eprintln!("threads at the end: {}", facts::threads());
    // the run's JSON, then its tables
    if let Some(path) = &args.out {
        let json = serde_json::to_string_pretty(&record).expect("a record serializes");
        if let Err(e) = std::fs::write(path, json) {
            eprintln!("{}: {e}", path.display());
            std::process::exit(1);
        }
    }
    print!("{}", report::summary(std::slice::from_ref(&record)));
}
