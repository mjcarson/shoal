//! The X5 spike: checksum candidates fed the same buffers, checked for stability, and timed on one
//! core
//!
//! [X5](../../docs/src/object-storage/spikes.md) asks which checksum guards a chunk unit, and
//! whether its definition can ever move ([Q21](../../docs/src/object-storage/contract.md)). The
//! trigger it names is gxhash, the one hash the workspace has, giving different output for the
//! same bytes across cpu features, builds or ways of feeding it. Every candidate S18 pinned, and
//! two references, are adapted to one trait and three things are recorded, in this order:
//!
//! - **facts**: the kernel a candidate says it chose, the threads it starts and what a call
//!   allocates
//! - **check**: published check values; digests of seeded inputs at every short length, every
//!   unit and an odd length, which `report` compares across hosts and builds; the same bytes at
//!   every start offset in a cache line; the incremental interface fed in pieces against one call;
//!   a chunk's checksum combined from its units', and a unit's carried to its place without the
//!   bytes
//! - **speed**: GiB a second for one pinned core over units of 4 KiB to 1 MiB, cold and hot, one
//!   call and fed in 4 KiB pieces, each cell three times with the candidates interleaved; and the
//!   nanoseconds of one combine
//!
//! Nothing here is a capture. It is built for `x86-64-v3` (the AVX2 baseline a node may be held
//! to), for `znver1`, and on europa for `x86-64-v4` and natively, and its tables are pasted into
//! `docs/src/object-storage/checksums.md` with the host, cpu, build and governor they came from.
//!
//! What it does not measure: a checksum inside a node, where the bytes arrive off a socket and
//! leave for a device; error detection itself, which is the definitions' published property and
//! not something a run on good hardware can show; and any crate S18 did not pin.

mod alloc_count;
mod buffers;
mod check;
mod facts;
mod record;
mod report;
mod speed;
mod sums;

use std::path::PathBuf;

use buffers::AlignedBuf;
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
}

/// Read the command line
fn args() -> Result<Args, String> {
    let mut args = Args {
        command: "all".to_string(),
        core: 2,
        out: None,
        quick: false,
        inputs: Vec::new(),
    };
    let mut words = std::env::args().skip(1);
    while let Some(word) = words.next() {
        match word.as_str() {
            "all" | "facts" | "check" | "speed" | "report" => args.command = word,
            "--core" => {
                args.core = words
                    .next()
                    .and_then(|core| core.parse().ok())
                    .ok_or("--core takes a core number")?;
            }
            "--out" => args.out = Some(words.next().ok_or("--out takes a path")?.into()),
            "--quick" => args.quick = true,
            "-h" | "--help" => {
                return Err("usage: shoal-spike-checksum [all|facts|check|speed] [--core N] [--out run.json] [--quick]\n       shoal-spike-checksum report run.json...".to_string());
            }
            path if args.command == "report" => args.inputs.push(path.into()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

/// Read every run named and print their summary
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
    print!("{}", report::summary(&runs));
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
    let arena_mib = if args.quick { 32 } else { 256 };
    let mut record = RunRecord {
        labels: facts::labels(
            args.core,
            plan.runs,
            plan.budget.as_millis() as u64,
            arena_mib,
            args.quick,
        ),
        ..RunRecord::default()
    };
    eprintln!(
        "{} | {} | {} | compiled {} | detected {} | governor {} | core {} | {}",
        record.labels.host,
        record.labels.cpu,
        record.labels.build(),
        record.labels.compiled.join(","),
        record.labels.detected.join(","),
        record.labels.governor,
        record.labels.core,
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
        // the arena is allocated and touched once, before anything is timed
        let arena = AlignedBuf::seeded(arena_mib << 20, 0xda7a);
        record.speed = speed::run(&plan, &arena);
        record.combine = speed::combine(&plan);
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
