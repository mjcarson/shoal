//! The stripe model's search: generated runs at volume, and the records X1 reports
//!
//! The tests run a few seeds of every configuration; this runs as many as it is given, on as many
//! threads, and prints what S16 asks X1 to record: for each unsafe setting and layout, the runs
//! that violate a clause and which; for the safe policy, every configuration's runs survived and
//! what they exercised; the steps readers and stagers took once the faults stopped, with and
//! without each progress setting; and a digest of every outcome, so a run on another host can be
//! shown to have found the same.
//!
//! Seeds run in blocks of [`BLOCK`], each with a digest of its own, so two hosts that ran
//! overlapping ranges can be compared block by block and their disjoint blocks added to one count.
//!
//! ```text
//! cargo run -p shoal-model --release --example stripe_search -- safe --seeds 1000
//! cargo run -p shoal-model --release --example stripe_search -- unsafe --seeds 200
//! cargo run -p shoal-model --release --example stripe_search -- documented --seeds 1000
//! cargo run -p shoal-model --release --example stripe_search -- small --seeds 1000
//! cargo run -p shoal-model --release --example stripe_search -- all --seeds 2000 --from 0 --threads 32 --out x1.json
//! cargo run -p shoal-model --release --example stripe_search -- report x1-*.json
//! cargo run -p shoal-model --release --example stripe_search -- show --layout 4+2 --seed 17 [--setting <name>] [--variant <untouched/previous/reservation>]
//! cargo run -p shoal-model --release --example stripe_search -- failures --layout r3 --seeds 1000 [--variant <...>]
//! cargo run -p shoal-model --release --example stripe_search -- scenarios [--only <setting>]
//! cargo run -p shoal-model --release --example stripe_search -- inspect --layout r3 --seed 17
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use shoal_model::stripe::minimize::minimize;
use shoal_model::stripe::policy::{PreviousState, Reservation, UntouchedRule};
use shoal_model::stripe::scenarios::{findings, policy_named, s7};
use shoal_model::stripe::{
    generate, Layout, StripeCoverage, StripeParams, StripePolicy, StripeWorld,
};

/// How many seeds a block holds: the unit work is split into and digests are compared by
const BLOCK: u64 = 250;

/// One configuration the search runs: a layout and a policy, named
#[derive(Debug, Clone)]
struct Config {
    /// Which set it belongs to: `safe`, `unsafe`, `documented` or `small`
    group: &'static str,
    /// What the tables call it
    name: String,
    /// The layout
    layout: Layout,
    /// The policy
    policy: StripePolicy,
}

/// What one block of seeds found, kept apart so hosts can be compared and counts added
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Block {
    /// The block's first seed
    start: u64,
    /// How many seeds it ran
    len: u64,
    /// Runs that broke a clause, by its number
    violations: BTreeMap<String, u64>,
    /// Runs that broke a progress bound, by the bound
    stalls: BTreeMap<String, u64>,
    /// Calm reads that failed by name
    failed_reads: u64,
    /// Calm writes refused
    refused_writes: u64,
    /// A digest of every run's outcome, folded in seed order
    digest: u64,
}

/// What one configuration's seeds found
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Tally {
    /// Which set the configuration belongs to
    group: String,
    /// The configuration's name
    name: String,
    /// Runs
    runs: u64,
    /// Runs that broke a clause, by its number
    violations: BTreeMap<String, u64>,
    /// The first seed that broke each clause, and how
    first: BTreeMap<String, (u64, String)>,
    /// Runs that broke a progress bound, by the bound
    stalls: BTreeMap<String, u64>,
    /// The first seed that broke each bound, and how
    first_stall: BTreeMap<String, (u64, String)>,
    /// What the runs exercised
    coverage: StripeCoverage,
    /// How many calm reads took each number of steps
    reader_steps: BTreeMap<u64, u64>,
    /// How many calm writes took each number of steps to be acknowledged
    writer_steps: BTreeMap<u64, u64>,
    /// Calm reads that failed by name
    failed_reads: u64,
    /// Calm writes refused
    refused_writes: u64,
    /// Each block's findings, in seed order
    blocks: Vec<Block>,
    /// A digest of every block's digest, in seed order
    digest: u64,
}

/// Every configuration's blocks, by their first seed and length, with the records that ran each
type BlocksByConfig = BTreeMap<(String, String), BTreeMap<(u64, u64), Vec<(usize, Block)>>>;

/// One host's run of the search, as written by `all --out`
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    /// The host it ran on
    host: String,
    /// What the run was called, `--label`: the build, say
    label: String,
    /// The first seed
    from: u64,
    /// How many seeds each configuration ran
    seeds: u64,
    /// The threads it ran on
    threads: usize,
    /// The wall time it took, for information only
    secs: f64,
    /// Every configuration's findings
    tallies: Vec<Tally>,
}

/// SplitMix64's mixer, to fold outcomes into a digest that is the same on every host
///
/// # Arguments
///
/// * `z` - The value to mix
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Fold a string into a digest
///
/// # Arguments
///
/// * `digest` - The digest so far
/// * `text` - What to fold into it
fn fold(digest: u64, text: &str) -> u64 {
    text.bytes()
        .fold(digest, |acc, byte| mix(acc ^ u64::from(byte)))
}

/// The safe policy's configurations: every layout, both answers to the two progress questions,
/// and at r3 both again with small writes riding in their commits (Q27)
fn safe_configs() -> Vec<Config> {
    let mut configs = Vec::new();
    for layout in Layout::ALL {
        for previous in [PreviousState::Dropped, PreviousState::Kept] {
            for reservation in [Reservation::None, Reservation::Granted] {
                // Q16 is settled, so only the two progress settings vary
                let policy =
                    StripePolicy::safe_with(UntouchedRule::Confirmed, previous, reservation);
                configs.push(Config {
                    group: "safe",
                    name: format!("{} {}", layout.short(), policy.variant()),
                    layout,
                    policy,
                });
            }
        }
    }
    // the small write in its commit is a replicated stripe's alone, so it runs at r3; appended,
    // so every configuration from before it keeps its place
    let layout = Layout::Replicated3;
    for previous in [PreviousState::Dropped, PreviousState::Kept] {
        for reservation in [Reservation::None, Reservation::Granted] {
            let policy = StripePolicy::safe_with(UntouchedRule::Confirmed, previous, reservation)
                .in_commit();
            configs.push(Config {
                group: "safe",
                name: format!("{} {}", layout.short(), policy.variant()),
                layout,
                policy,
            });
        }
    }
    configs
}

/// The rules the small write in its commit depends on, each as one might write it, and Q16's two
/// answers that count on the row's word, which the small write makes reachable at r3: each the
/// safe policy with the path taken at r3 and that one rule moved
fn small_configs() -> Vec<Config> {
    let layout = Layout::Replicated3;
    let mut configs: Vec<Config> = StripePolicy::small_write_settings()
        .into_iter()
        .map(|(name, policy, _)| Config {
            group: "small",
            name: format!("{} {name}", layout.short()),
            layout,
            policy,
        })
        .collect();
    for (name, policy) in StripePolicy::documented_rules() {
        if !name.starts_with("untouched") {
            continue;
        }
        configs.push(Config {
            group: "small",
            name: format!("{} in-commit {name}", layout.short()),
            layout,
            policy: policy.in_commit(),
        });
    }
    configs
}

/// The rules the pages stated that the model found do not hold, and Q16's two answers that count
/// on the row's word: each the safe policy with that one rule as written, at every layout
fn documented_configs() -> Vec<Config> {
    let mut configs = Vec::new();
    for layout in Layout::ALL {
        for (name, policy) in StripePolicy::documented_rules() {
            configs.push(Config {
                group: "documented",
                name: format!("{} {name}", layout.short()),
                layout,
                policy,
            });
        }
    }
    configs
}

/// The unsafe settings' configurations: every setting at every layout, the progress one included
fn unsafe_configs() -> Vec<Config> {
    let mut configs = Vec::new();
    for layout in Layout::ALL {
        for (name, policy, _, _) in StripePolicy::unsafe_settings() {
            configs.push(Config {
                group: "unsafe",
                name: format!("{} {name}", layout.short()),
                layout,
                policy,
            });
        }
        // the setting S16 holds to the progress check alone
        let (name, policy, _) = StripePolicy::progress_setting();
        configs.push(Config {
            group: "unsafe",
            name: format!("{} {name}", layout.short()),
            layout,
            policy,
        });
    }
    configs
}

/// The parameters a run uses, with any overrides the command line gave
///
/// # Arguments
///
/// * `layout` - The layout the run is of
fn params_for(layout: Layout) -> StripeParams {
    let mut params = StripeParams::default_small(layout);
    let args: Vec<String> = std::env::args().collect();
    // every bound and size the generator takes can be moved for one search
    params.steps = arg(&args, "--steps", params.steps);
    params.calm_from = arg(&args, "--calm", params.calm_from);
    params.read_bound = arg(&args, "--read-bound", params.read_bound);
    params.write_bound = arg(&args, "--write-bound", params.write_bound);
    params.calm_writers = arg(&args, "--calm-writers", params.calm_writers);
    params.calm_readers = arg(&args, "--calm-readers", params.calm_readers);
    params
}

/// Run one block of a configuration's seeds
///
/// # Arguments
///
/// * `config` - The configuration
/// * `start` - The block's first seed
/// * `len` - How many seeds it runs
fn run(config: &Config, start: u64, len: u64) -> Tally {
    let params = params_for(config.layout);
    let mut tally = Tally::default();
    let mut block = Block {
        start,
        len,
        ..Block::default()
    };
    for seed in start..start + len {
        // generate the run and replay it from nothing
        let schedule = generate(&config.name, seed, &params, config.policy);
        let outcome = StripeWorld::replay(&schedule);
        tally.runs += 1;
        tally.coverage.add(&outcome.coverage);
        // what the calm phase's readers and writers took
        for steps in &outcome.progress.reader_steps {
            *tally.reader_steps.entry(*steps).or_default() += 1;
        }
        for steps in &outcome.progress.writer_steps {
            *tally.writer_steps.entry(*steps).or_default() += 1;
        }
        block.failed_reads += outcome.progress.failed_reads;
        block.refused_writes += outcome.progress.refused_writes;
        // the line the digest folds names the seed, its length, what it exercised, what every calm
        // reader and writer took, and what it broke: enough that two hosts agreeing on it ran the
        // same run, not merely runs that broke nothing
        let mut line = format!(
            "{seed}:{}:{:?}:{:?}:{:?}:{}:{}:",
            outcome.steps,
            outcome.coverage,
            outcome.progress.reader_steps,
            outcome.progress.writer_steps,
            outcome.progress.failed_reads,
            outcome.progress.refused_writes
        );
        if let Some(violation) = &outcome.violation {
            let key = violation.property.to_string();
            *block.violations.entry(key.clone()).or_default() += 1;
            tally
                .first
                .entry(key)
                .or_insert((seed, violation.detail.clone()));
            line.push_str(&violation.detail);
        } else if let Some(stalled) = &outcome.stalled {
            *block.stalls.entry(stalled.bound.clone()).or_default() += 1;
            tally
                .first_stall
                .entry(stalled.bound.clone())
                .or_insert((seed, stalled.detail.clone()));
            line.push_str(&stalled.detail);
        }
        block.digest = fold(block.digest, &line);
    }
    tally.blocks.push(block);
    tally
}

/// Add one block's tally into a configuration's, which must be merged in seed order
///
/// # Arguments
///
/// * `into` - The configuration's tally so far
/// * `tally` - The next block's
fn merge(into: &mut Tally, tally: Tally) {
    into.runs += tally.runs;
    // the first seed that broke each thing is the earliest block's
    for (key, first) in tally.first {
        into.first.entry(key).or_insert(first);
    }
    for (key, first) in tally.first_stall {
        into.first_stall.entry(key).or_insert(first);
    }
    into.coverage.add(&tally.coverage);
    for (steps, count) in tally.reader_steps {
        *into.reader_steps.entry(steps).or_default() += count;
    }
    for (steps, count) in tally.writer_steps {
        *into.writer_steps.entry(steps).or_default() += count;
    }
    // the counts are the blocks', and the digest folds the blocks' in order
    for block in tally.blocks {
        for (key, count) in &block.violations {
            *into.violations.entry(key.clone()).or_default() += count;
        }
        for (key, count) in &block.stalls {
            *into.stalls.entry(key.clone()).or_default() += count;
        }
        into.failed_reads += block.failed_reads;
        into.refused_writes += block.refused_writes;
        into.digest = mix(into.digest ^ block.digest);
        into.blocks.push(block);
    }
}

/// Run every configuration on a pool of threads
///
/// # Arguments
///
/// * `configs` - The configurations
/// * `from` - The first seed
/// * `seeds` - How many seeds each runs
/// * `threads` - How many threads to run them on
fn run_all(configs: &[Config], from: u64, seeds: u64, threads: usize) -> Vec<Tally> {
    // the work is split into blocks of seeds, so every thread stays busy and every host's blocks
    // start at the same seeds
    let mut jobs = Vec::new();
    for (index, _) in configs.iter().enumerate() {
        let mut start = from;
        while start < from + seeds {
            let len = (BLOCK - start % BLOCK).min(from + seeds - start);
            jobs.push((index, start, len));
            start += len;
        }
    }
    let next = AtomicU64::new(0);
    let results: Mutex<Vec<(usize, u64, Tally)>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                // take the next block nobody has taken
                let job = next.fetch_add(1, Ordering::Relaxed) as usize;
                let Some((index, start, len)) = jobs.get(job).copied() else {
                    return;
                };
                let tally = run(&configs[index], start, len);
                results
                    .lock()
                    .expect("no thread panics")
                    .push((index, start, tally));
            });
        }
    });
    // merge each configuration's blocks in seed order, so the digest is the same on every host
    let mut results = results.into_inner().expect("no thread panics");
    results.sort_by_key(|(index, start, _)| (*index, *start));
    let mut merged: Vec<Tally> = configs
        .iter()
        .map(|config| Tally {
            group: config.group.to_string(),
            name: config.name.clone(),
            ..Tally::default()
        })
        .collect();
    for (index, _, tally) in results {
        merge(&mut merged[index], tally);
    }
    merged
}

/// A percentile of a histogram of steps
///
/// # Arguments
///
/// * `steps` - How many took each number of steps
/// * `p` - The percentile, from zero to one
fn percentile(steps: &BTreeMap<u64, u64>, p: f64) -> u64 {
    let total: u64 = steps.values().sum();
    if total == 0 {
        return 0;
    }
    // the rank of the percentile among every sample, counted up the histogram
    let rank = ((total - 1) as f64 * p).round() as u64;
    let mut seen = 0;
    for (value, count) in steps {
        seen += count;
        if seen > rank {
            return *value;
        }
    }
    steps.keys().next_back().copied().unwrap_or(0)
}

/// A map of counts written `key count, ...`, or `none`
///
/// # Arguments
///
/// * `counts` - The counts
fn counts(counts: &BTreeMap<String, u64>) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|(key, count)| format!("{key} {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Print a table of what each configuration found
///
/// # Arguments
///
/// * `tallies` - Every configuration's findings
fn print(tallies: &[Tally]) {
    println!(
        "| Configuration | Runs | Violations | First | Stalls | Reader p50/p99/max | Failed reads | Writer p50/p99/max | Refused writes | Digest |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for tally in tallies {
        // the first thing it broke, a clause before a bound
        let first = tally
            .first
            .iter()
            .chain(tally.first_stall.iter())
            .next()
            .map(|(_, (seed, detail))| format!("seed {seed}: {detail}"))
            .unwrap_or_default();
        println!(
            "| {} | {} | {} | {} | {} | {}/{}/{} | {} | {}/{}/{} | {} | {:016x} |",
            tally.name,
            tally.runs,
            counts(&tally.violations),
            first,
            counts(&tally.stalls),
            percentile(&tally.reader_steps, 0.5),
            percentile(&tally.reader_steps, 0.99),
            tally.reader_steps.keys().next_back().copied().unwrap_or(0),
            tally.failed_reads,
            percentile(&tally.writer_steps, 0.5),
            percentile(&tally.writer_steps, 0.99),
            tally.writer_steps.keys().next_back().copied().unwrap_or(0),
            tally.refused_writes,
            tally.digest
        );
    }
}

/// Print what the safe policy's runs exercised, one row a configuration
///
/// # Arguments
///
/// * `tallies` - Every configuration's findings; only the safe ones are printed
fn print_coverage(tallies: &[Tally]) {
    println!(
        "| Configuration | Runs | Crashes | Torn applies | Replays | Disk failures | Replacements | Fills | Moves | Rebuilds | Truncates | Extensions | No-ops | Refusals | Unknown | Strong reads | Default reads | Lagging answers | Discards | Reclaims | Retries | Duplicates | Commits | Acks |"
    );
    println!("|{}", " --- |".repeat(24));
    for tally in tallies.iter().filter(|tally| tally.group == "safe") {
        let c = &tally.coverage;
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            tally.name,
            tally.runs,
            c.crashes,
            c.torn_applies,
            c.replays,
            c.disk_failures,
            c.disk_replacements,
            c.fills,
            c.moves,
            c.rebuilds,
            c.truncates,
            c.extensions,
            c.noops,
            c.refusals,
            c.unknown_outcomes,
            c.strong_reads,
            c.default_reads,
            c.lagging_answers,
            c.discards,
            c.reclaims,
            c.retries,
            c.duplicates,
            c.commits,
            c.acks
        );
    }
}

/// Print what the small write's path did, one row a configuration that took it
///
/// # Arguments
///
/// * `tallies` - Every configuration's findings; only the ones that took the path are printed
fn print_small_coverage(tallies: &[Tally]) {
    println!(
        "| Configuration | Runs | Small writes | Merges | Folds | Clears | Overlaid reads | Staged over pending | Pending at the end |"
    );
    println!("|{}", " --- |".repeat(9));
    for tally in tallies.iter().filter(|tally| tally.coverage.small_writes > 0) {
        let c = &tally.coverage;
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            tally.name,
            tally.runs,
            c.small_writes,
            c.merges,
            c.folds,
            c.clears,
            c.overlaid_reads,
            c.staged_over_pending,
            c.pending_at_end
        );
    }
}

/// The name of the host this runs on, for a record
fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|name| name.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Run every configuration and write a record of it
///
/// # Arguments
///
/// * `args` - The command line
/// * `from` - The first seed
/// * `seeds` - How many seeds each configuration runs
/// * `threads` - How many threads to run them on
fn all(args: &[String], from: u64, seeds: u64, threads: usize) {
    let out: String = arg(args, "--out", String::new());
    let label: String = arg(args, "--label", String::new());
    // every set of configurations, safe first
    let configs: Vec<Config> = safe_configs()
        .into_iter()
        .chain(unsafe_configs())
        .chain(documented_configs())
        .chain(small_configs())
        .collect();
    let started = Instant::now();
    let tallies = run_all(&configs, from, seeds, threads);
    let secs = started.elapsed().as_secs_f64();
    // the four tables, then what the safe runs exercised
    for group in ["safe", "unsafe", "documented", "small"] {
        println!("\n### {group}\n");
        let of_group: Vec<Tally> = tallies
            .iter()
            .filter(|tally| tally.group == group)
            .cloned()
            .collect();
        print(&of_group);
    }
    println!("\n### coverage\n");
    print_coverage(&tallies);
    println!("\n### the small write's coverage\n");
    print_small_coverage(&tallies);
    let record = Record {
        host: hostname(),
        label,
        from,
        seeds,
        threads,
        secs,
        tallies,
    };
    println!(
        "\n{} seeds from {} of {} configurations on {} threads of {} in {:.0}s",
        seeds,
        from,
        configs.len(),
        threads,
        record.host,
        secs
    );
    // and the record a report merges
    if !out.is_empty() {
        let text = serde_json::to_string(&record).expect("a record serializes");
        std::fs::write(&out, text).expect("the record is written");
    }
}

/// Merge records from several hosts: compare every block two of them ran, and add the rest up
///
/// # Arguments
///
/// * `files` - The records
fn report(files: &[String]) {
    // load every record
    let records: Vec<Record> = files
        .iter()
        .map(|file| {
            let text = std::fs::read_to_string(file).expect("a record");
            serde_json::from_str(&text).expect("a record parses")
        })
        .collect();
    println!("| Record | Host | Label | Seeds | Threads | Wall |");
    println!("| --- | --- | --- | --- | --- | --- |");
    for (file, record) in files.iter().zip(&records) {
        println!(
            "| {file} | {} | {} | {}..{} | {} | {:.0}s |",
            record.host,
            record.label,
            record.from,
            record.from + record.seeds,
            record.threads,
            record.secs
        );
    }
    // every configuration's blocks, by start, from whichever records ran them
    let mut by_config: BlocksByConfig = BTreeMap::new();
    let mut order: Vec<(String, String)> = Vec::new();
    for (index, record) in records.iter().enumerate() {
        for tally in &record.tallies {
            let key = (tally.group.clone(), tally.name.clone());
            if !by_config.contains_key(&key) {
                order.push(key.clone());
            }
            let blocks = by_config.entry(key).or_default();
            for block in &tally.blocks {
                blocks
                    .entry((block.start, block.len))
                    .or_default()
                    .push((index, block.clone()));
            }
        }
    }
    // the determinism check: a block more than one host ran must have come out the same
    let mut shared = 0;
    let mut differ = Vec::new();
    let mut pairs: BTreeSet<(String, String)> = BTreeSet::new();
    for (key, blocks) in &by_config {
        for ((start, len), runs) in blocks {
            if runs.len() < 2 {
                continue;
            }
            shared += 1;
            let (first, block) = &runs[0];
            for (other, theirs) in &runs[1..] {
                pairs.insert((records[*first].host.clone(), records[*other].host.clone()));
                if theirs != block {
                    differ.push(format!(
                        "{} seeds {start}..{}: {} and {} differ",
                        key.1,
                        start + len,
                        records[*first].host,
                        records[*other].host
                    ));
                }
            }
        }
    }
    println!(
        "\n{shared} blocks were run by more than one record, between {:?}; {} differ",
        pairs,
        differ.len()
    );
    for line in &differ {
        println!("  {line}");
    }
    // and every configuration's distinct blocks added up, each counted once
    println!(
        "\n| Set | Configuration | Runs | Violations | Stalls | Failed reads | Refused writes |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    for key in &order {
        let mut total = Block::default();
        for runs in by_config[key].values() {
            let (_, block) = &runs[0];
            total.len += block.len;
            for (clause, count) in &block.violations {
                *total.violations.entry(clause.clone()).or_default() += count;
            }
            for (bound, count) in &block.stalls {
                *total.stalls.entry(bound.clone()).or_default() += count;
            }
            total.failed_reads += block.failed_reads;
            total.refused_writes += block.refused_writes;
        }
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            key.0,
            key.1,
            total.len,
            counts(&total.violations),
            counts(&total.stalls),
            total.failed_reads,
            total.refused_writes
        );
    }
}

/// Parse `--name value` from the arguments
///
/// # Arguments
///
/// * `args` - The command line
/// * `name` - The flag
/// * `default` - What it is when absent or unparsable
fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Parse a layout's short name
///
/// # Arguments
///
/// * `name` - `r3`, `2+1` or `4+2`
fn layout_of(name: &str) -> Layout {
    Layout::ALL
        .into_iter()
        .find(|layout| layout.short() == name)
        .unwrap_or_else(|| panic!("no layout called {name}"))
}

/// A safe configuration of a layout by its variant, the default when none is named
///
/// # Arguments
///
/// * `layout` - The layout
/// * `variant` - The variant's name, as `StripePolicy::variant` writes it
fn safe_config(layout: Layout, variant: &str) -> Config {
    let variant = if variant.is_empty() {
        StripePolicy::safe().variant()
    } else {
        variant.to_string()
    };
    safe_configs()
        .into_iter()
        .find(|config| config.layout == layout && config.policy.variant() == variant)
        .unwrap_or_else(|| panic!("no variant {variant}"))
}

/// Generate one seed under a setting, print what it broke, and print it minimized
///
/// # Arguments
///
/// * `args` - The command line
fn show(args: &[String]) {
    let layout = layout_of(&arg(args, "--layout", "r3".to_string()));
    let seed: u64 = arg(args, "--seed", 0);
    let setting: String = arg(args, "--setting", String::new());
    let variant: String = arg(args, "--variant", String::new());
    // the setting by name, the safe policy when none is named
    let mut policy = if setting.is_empty() {
        StripePolicy::safe()
    } else {
        policy_named(&setting)
    };
    // and the progress settings of the variant named
    if !variant.is_empty() {
        let config = safe_config(layout, &variant);
        policy.untouched = config.policy.untouched;
        policy.previous = config.policy.previous;
        policy.reservation = config.policy.reservation;
        policy.small_writes.path = config.policy.small_writes.path;
    }
    let schedule = generate("show", seed, &params_for(layout), policy);
    println!(
        "{} events; violation {:?}; stalled {:?}",
        schedule.events.len(),
        schedule.expected,
        schedule.stalled
    );
    let small = minimize(&schedule);
    println!("minimized to {} events", small.events.len());
    println!("{}", small.to_json());
}

/// Print every seed of one safe configuration that broke anything, or failed a read by name
///
/// # Arguments
///
/// * `args` - The command line
/// * `from` - The first seed
/// * `seeds` - How many seeds to run
fn failures(args: &[String], from: u64, seeds: u64) {
    let layout = layout_of(&arg(args, "--layout", "r3".to_string()));
    let config = safe_config(layout, &arg(args, "--variant", String::new()));
    let params = params_for(layout);
    for seed in from..from + seeds {
        let outcome = StripeWorld::replay(&generate("f", seed, &params, config.policy));
        // a clause, then a bound, then a read that gave up
        if let Some(violation) = outcome.violation {
            println!("seed {seed}: {}", violation.detail);
        } else if let Some(stalled) = outcome.stalled {
            println!("seed {seed}: stalled {}", stalled.detail);
        } else if outcome.progress.failed_reads > 0 {
            println!(
                "seed {seed}: {} reads failed by name",
                outcome.progress.failed_reads
            );
        }
    }
}

/// Build every schedule written by hand, under its setting and under the safe policy
///
/// # Arguments
///
/// * `args` - The command line
fn scenarios(args: &[String]) {
    let only: String = arg(args, "--only", String::new());
    for (number, setting, build) in s7().into_iter().chain(findings()) {
        if !only.is_empty() && only != setting {
            continue;
        }
        // what it finds as written, and what the repair finds
        let unsafe_run = build(policy_named(setting));
        let safe_run = build(StripePolicy::safe());
        println!(
            "{:?} {setting}: {} events -> {:?} / {:?} | safe: {:?} / {:?}",
            number,
            unsafe_run.events.len(),
            unsafe_run.expected.as_ref().map(|v| v.detail.clone()),
            unsafe_run.stalled.as_ref().map(|s| s.detail.clone()),
            safe_run.expected.as_ref().map(|v| v.detail.clone()),
            safe_run.stalled.as_ref().map(|s| s.detail.clone()),
        );
    }
}

/// Print the world a safe seed ended in: nodes, disks, rows, drivers still running, messages
///
/// # Arguments
///
/// * `args` - The command line
fn inspect(args: &[String]) {
    let layout = layout_of(&arg(args, "--layout", "r3".to_string()));
    let seed: u64 = arg(args, "--seed", 0);
    let config = safe_config(layout, &arg(args, "--variant", String::new()));
    let schedule = generate("inspect", seed, &params_for(layout), config.policy);
    let world = StripeWorld::replay_world(&schedule);
    println!("step {} skipped {}", world.step, world.skipped);
    // the hosts and the disks under them
    for (id, node) in &world.nodes {
        println!(
            "node {id:?} up {} disk {:?} slice {:?} reported {}",
            node.up, node.disk, node.slice, node.reported
        );
    }
    for (id, disk) in &world.disks {
        println!(
            "disk {id:?} healthy {} full {} marker {:?}",
            disk.healthy, disk.full, disk.marker
        );
    }
    // what the groups committed
    for (stripe, group) in world.rows.iter().enumerate() {
        println!("stripe {stripe}: {:?}", group.latest());
    }
    println!("entry {:?}", world.entry.latest());
    // and what was still going on
    for (op, driver) in &world.drivers {
        println!("driver {op:?}: {driver:?}");
    }
    println!("{} in flight", world.net.in_flight.len());
    for msg in world.net.in_flight.iter().take(30) {
        println!("  {msg:?}");
    }
    for (op, record) in world.ledger.records.iter().rev().take(15) {
        println!("op {op:?}: {record:?}");
    }
}

/// Run the command the first argument names
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().cloned().unwrap_or_else(|| "safe".to_string());
    // the seed range and the threads every searching command takes
    let seeds: u64 = arg(&args, "--seeds", 100);
    let from: u64 = arg(&args, "--from", 0);
    let threads: usize = arg(
        &args,
        "--threads",
        std::thread::available_parallelism().map_or(4, |n| n.get()),
    );
    match command.as_str() {
        "safe" => print(&run_all(&safe_configs(), from, seeds, threads)),
        "unsafe" => print(&run_all(&unsafe_configs(), from, seeds, threads)),
        "documented" => print(&run_all(&documented_configs(), from, seeds, threads)),
        "small" => print(&run_all(&small_configs(), from, seeds, threads)),
        "all" => all(&args, from, seeds, threads),
        "report" => report(&args[1..]),
        "show" => show(&args),
        "failures" => failures(&args, from, seeds),
        "scenarios" => scenarios(&args),
        "inspect" => inspect(&args),
        other => panic!("no command {other}"),
    }
}
