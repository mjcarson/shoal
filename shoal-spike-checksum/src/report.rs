//! Markdown tables from one run or many: the labels, the checks and what they found across runs,
//! and the speed of every candidate

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::check::UNITS;
use crate::record::{RunRecord, SpeedCell, median};

/// A figure to three significant digits, or a dash for nothing
///
/// # Arguments
///
/// * `value` - The figure
fn fig(value: Option<f64>) -> String {
    match value {
        // three significant digits whatever the magnitude
        Some(v) if v >= 100.0 => format!("{v:.0}"),
        Some(v) if v >= 10.0 => format!("{v:.1}"),
        Some(v) if v >= 1.0 => format!("{v:.2}"),
        Some(v) => format!("{v:.3}"),
        None => "-".to_string(),
    }
}

/// A unit in KiB or MiB
///
/// # Arguments
///
/// * `unit` - The unit in bytes
fn unit_name(unit: usize) -> String {
    if unit >= 1 << 20 {
        format!("{} MiB", unit >> 20)
    } else {
        format!("{} KiB", unit >> 10)
    }
}

/// The candidates in the order the first run lists them
///
/// # Arguments
///
/// * `runs` - The runs
fn candidates(runs: &[RunRecord]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    // facts list every candidate; a run without facts falls back to its checks and speed cells
    for run in runs {
        let checked = run
            .check
            .iter()
            .flat_map(|check| check.digests.iter().map(|digest| digest.sum.clone()));
        let names = run
            .facts
            .iter()
            .map(|fact| fact.sum.clone())
            .chain(checked)
            .chain(run.speed.iter().map(|cell| cell.sum.clone()));
        for name in names {
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    out
}

/// One speed cell's median
///
/// # Arguments
///
/// * `run` - The run
/// * `sum` - The candidate
/// * `unit` - The unit
/// * `op` - The operation
/// * `hot` - Cold or hot
/// * `pick` - Which figure of the cell
fn cell(
    run: &RunRecord,
    sum: &str,
    unit: usize,
    op: &str,
    hot: bool,
    pick: fn(&SpeedCell) -> &Vec<f64>,
) -> Option<f64> {
    run.speed
        .iter()
        .find(|c| c.sum == sum && c.unit == unit && c.op == op && c.hot == hot)
        .and_then(|c| median(pick(c)))
}

/// The labels of every run
///
/// # Arguments
///
/// * `out` - Where the table goes
/// * `runs` - The runs
fn labels(out: &mut String, runs: &[RunRecord]) {
    let _ = writeln!(
        out,
        "| Run | CPU | Build | Compiled for | Detected | Governor | Core | Runs × budget | Date |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for run in runs {
        let l = &run.labels;
        let _ = writeln!(
            out,
            "| {} | {} | `{}` | {} | {} | {} | {} | {} × {} ms{} | {} |",
            l.short(),
            l.cpu,
            l.build(),
            l.compiled.join(", "),
            l.detected.join(", "),
            l.governor,
            l.core,
            l.runs,
            l.budget_ms,
            if l.quick { ", quick" } else { "" },
            l.date
        );
    }
    let _ = writeln!(out, "\n{}\n", runs[0].labels.rustc);
}

/// What each candidate is beyond its speed: width, kernel, threads and allocations
///
/// # Arguments
///
/// * `out` - Where the table goes
/// * `runs` - The runs
fn facts(out: &mut String, runs: &[RunRecord]) {
    let _ = writeln!(out, "## Facts\n");
    let _ = writeln!(
        out,
        "| Candidate | Bits | Kernel, by run | Threads started | Allocations, one call at 64 KiB | Allocations, fed in 4 KiB pieces |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- |");
    for sum in candidates(runs) {
        // every run's facts for this candidate
        let all: Vec<_> = runs
            .iter()
            .filter_map(|run| {
                run.facts
                    .iter()
                    .find(|f| f.sum == sum)
                    .map(|f| (run.labels.short(), f))
            })
            .collect();
        let Some((_, first)) = all.first() else {
            continue;
        };
        // the kernel each run chose, where the crate says
        let kernels: Vec<String> = all
            .iter()
            .filter_map(|(run, f)| f.kernel.as_ref().map(|k| format!("{run}: `{k}`")))
            .collect();
        let started = all
            .iter()
            .map(|(_, f)| f.threads_after.saturating_sub(f.threads_before))
            .max()
            .unwrap_or(0);
        let alloc = |op: &str| {
            first
                .allocations
                .iter()
                .find(|a| a.op == op)
                .map(|a| format!("{} / {} B", a.allocations, a.bytes))
                .unwrap_or_else(|| "-".to_string())
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            sum,
            first.bits,
            if kernels.is_empty() {
                "not said".to_string()
            } else {
                kernels.join("<br>")
            },
            started,
            alloc("one-shot"),
            alloc("stream-4k")
        );
    }
    let _ = writeln!(out);
}

/// The checks, and whether every run agreed
///
/// # Arguments
///
/// * `out` - Where the tables go
/// * `runs` - The runs
fn checks(out: &mut String, runs: &[RunRecord]) {
    let checked: Vec<&RunRecord> = runs.iter().filter(|run| run.check.is_some()).collect();
    if checked.is_empty() {
        return;
    }
    let _ = writeln!(out, "## Checks\n");
    // published values: one row a value, failures named by run
    let _ = writeln!(out, "### Published check values\n");
    let _ = writeln!(out, "| Candidate | Input | Expected | Source | Runs that gave it |");
    let _ = writeln!(out, "| --- | --- | --- | --- | --- |");
    let first = checked[0].check.as_ref().expect("filtered");
    for (i, vector) in first.vectors.iter().enumerate() {
        let wrong: Vec<String> = checked
            .iter()
            .filter(|run| !run.check.as_ref().expect("filtered").vectors[i].ok())
            .map(|run| {
                format!(
                    "{} gave `{}`",
                    run.labels.short(),
                    run.check.as_ref().expect("filtered").vectors[i].got
                )
            })
            .collect();
        let _ = writeln!(
            out,
            "| {} | {} | `{}` | {} | {} |",
            vector.sum,
            vector.input,
            vector.expected,
            vector.source,
            if wrong.is_empty() {
                format!("all {}", checked.len())
            } else {
                wrong.join("; ")
            }
        );
    }
    // digests across runs: a row a candidate, the sets that differ named
    let _ = writeln!(out, "\n### The same input across runs\n");
    let _ = writeln!(
        out,
        "Every set's output compared across all {} runs.\n",
        checked.len()
    );
    let _ = writeln!(out, "| Candidate | Sets | Identical in every run | Sets that differ |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for sum in candidates(runs) {
        // every set's outputs, by run
        let mut sets: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        for run in &checked {
            for digest in &run.check.as_ref().expect("filtered").digests {
                if digest.sum == sum {
                    sets.entry(digest.set.clone())
                        .or_default()
                        .push((run.labels.short(), digest.digest.clone()));
                }
            }
        }
        if sets.is_empty() {
            continue;
        }
        let differing: Vec<String> = sets
            .iter()
            .filter(|(_, outputs)| outputs.iter().any(|(_, d)| *d != outputs[0].1))
            .map(|(set, outputs)| {
                // group the runs by the output they gave
                let mut groups: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
                for (run, digest) in outputs {
                    groups.entry(digest).or_default().push(run);
                }
                let groups: Vec<String> = groups
                    .values()
                    .map(|runs| format!("[{}]", runs.join(", ")))
                    .collect();
                format!("{set}: {}", groups.join(" ≠ "))
            })
            .collect();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            sum,
            sets.len(),
            sets.len() - differing.len(),
            if differing.is_empty() {
                "none".to_string()
            } else {
                differing.join("<br>")
            }
        );
    }
    // the unit digests themselves, from the first run, for frozen vectors later
    let _ = writeln!(out, "\n### Outputs, from {}\n", checked[0].labels.short());
    let _ = writeln!(out, "| Candidate | Set | Output |");
    let _ = writeln!(out, "| --- | --- | --- |");
    for digest in &first.digests {
        let _ = writeln!(out, "| {} | {} | `{}` |", digest.sum, digest.set, digest.digest);
    }
    // alignment, streams, combines and extends: each row from every run, flagged if they differ
    let _ = writeln!(out, "\n### Every start offset in a cache line\n");
    let _ = writeln!(out, "| Candidate | Offsets equal to offset zero, every run |");
    let _ = writeln!(out, "| --- | --- |");
    for (i, row) in first.alignment.iter().enumerate() {
        let all: Vec<String> = checked
            .iter()
            .map(|run| {
                let r = &run.check.as_ref().expect("filtered").alignment[i];
                format!("{}/{}", r.equal, r.offsets)
            })
            .collect();
        let _ = writeln!(out, "| {} | {} |", row.sum, summarize(&all));
    }
    let _ = writeln!(out, "\n### Fed in pieces\n");
    let _ = writeln!(
        out,
        "| Candidate | Interface | Cut into | Equal to one call | Equal to the interface fed whole |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- |");
    for (i, row) in first.streams.iter().enumerate() {
        if row.splits == "none" {
            let _ = writeln!(out, "| {} | {} | - | - | - |", row.sum, row.api);
            continue;
        }
        let one: Vec<String> = checked
            .iter()
            .map(|run| {
                let r = &run.check.as_ref().expect("filtered").streams[i];
                format!("{}/{}", r.equal_one_shot, r.tried)
            })
            .collect();
        let whole: Vec<String> = checked
            .iter()
            .map(|run| {
                let r = &run.check.as_ref().expect("filtered").streams[i];
                format!("{}/{}", r.equal_whole, r.tried)
            })
            .collect();
        let _ = writeln!(
            out,
            "| {} | `{}` | {} | {} | {} |",
            row.sum,
            row.api,
            row.splits,
            summarize(&one),
            summarize(&whole)
        );
    }
    let _ = writeln!(out, "\n### A whole from its parts\n");
    let _ = writeln!(out, "| Candidate | How | Equal to one call over the whole |");
    let _ = writeln!(out, "| --- | --- | --- |");
    for (i, row) in first.combines.iter().enumerate() {
        let all: Vec<String> = checked
            .iter()
            .map(|run| {
                let r = &run.check.as_ref().expect("filtered").combines[i];
                format!("{}/{}", r.equal, r.tried)
            })
            .collect();
        let _ = writeln!(out, "| {} | {} | {} |", row.sum, row.how, summarize(&all));
    }
    let _ = writeln!(out, "\n### Carried to a place without the bytes\n");
    let _ = writeln!(
        out,
        "| Candidate | Through | Equal to one call over the bytes and a 32-byte identity |"
    );
    let _ = writeln!(out, "| --- | --- | --- |");
    for (i, row) in first.extends.iter().enumerate() {
        let all: Vec<String> = checked
            .iter()
            .map(|run| {
                let r = &run.check.as_ref().expect("filtered").extends[i];
                format!("{}/{}", r.equal, r.tried)
            })
            .collect();
        let _ = writeln!(out, "| {} | `{}` | {} |", row.sum, row.how, summarize(&all));
    }
    let _ = writeln!(out);
}

/// One figure when every run agrees, or each run's
///
/// # Arguments
///
/// * `all` - Each run's figure
fn summarize(all: &[String]) -> String {
    if all.iter().all(|figure| *figure == all[0]) {
        format!("{} in every run", all[0])
    } else {
        all.join(", ")
    }
}

/// One run's speed: every candidate at every unit, cold and hot
///
/// # Arguments
///
/// * `out` - Where the tables go
/// * `run` - The run
fn speed(out: &mut String, run: &RunRecord) {
    if run.speed.is_empty() {
        return;
    }
    let sums = candidates(std::slice::from_ref(run));
    let units: Vec<usize> = UNITS
        .into_iter()
        .filter(|unit| run.speed.iter().any(|c| c.unit == *unit))
        .collect();
    for (op, title) in [
        ("one-shot", "one call over a unit"),
        ("stream-4k", "the unit fed in pieces of 4 KiB"),
    ] {
        for hot in [false, true] {
            let _ = writeln!(
                out,
                "#### {}, {}, {}: GiB/s\n",
                run.labels.short(),
                title,
                if hot {
                    "hot (one unit, in cache)"
                } else {
                    "cold (units in turn, out of cache)"
                }
            );
            let header: Vec<String> = units.iter().map(|&u| unit_name(u)).collect();
            let _ = writeln!(out, "| Candidate | {} |", header.join(" | "));
            let _ = writeln!(out, "| --- |{}", " --- |".repeat(units.len()));
            for sum in &sums {
                let figures: Vec<String> = units
                    .iter()
                    .map(|&u| fig(cell(run, sum, u, op, hot, |c| &c.gib_per_sec)))
                    .collect();
                if figures.iter().all(|f| f == "-") {
                    continue;
                }
                let _ = writeln!(out, "| {} | {} |", sum, figures.join(" | "));
            }
            let _ = writeln!(out);
        }
    }
    // how long one call holds the core, cold
    let _ = writeln!(
        out,
        "#### {}, one call over a unit, cold: microseconds a call\n",
        run.labels.short()
    );
    let header: Vec<String> = units.iter().map(|&u| unit_name(u)).collect();
    let _ = writeln!(out, "| Candidate | {} |", header.join(" | "));
    let _ = writeln!(out, "| --- |{}", " --- |".repeat(units.len()));
    for sum in &sums {
        let figures: Vec<String> = units
            .iter()
            .map(|&u| fig(cell(run, sum, u, "one-shot", false, |c| &c.us_per_call)))
            .collect();
        let _ = writeln!(out, "| {} | {} |", sum, figures.join(" | "));
    }
    let _ = writeln!(out);
    // what one combine costs
    if !run.combine.is_empty() {
        let lens: Vec<u64> = {
            let mut lens: Vec<u64> = run.combine.iter().map(|c| c.len_b).collect();
            lens.sort_unstable();
            lens.dedup();
            lens
        };
        let _ = writeln!(
            out,
            "#### {}, one combine: nanoseconds, by the second part's length\n",
            run.labels.short()
        );
        let header: Vec<String> = lens.iter().map(|&l| unit_name(l as usize)).collect();
        let _ = writeln!(out, "| Candidate | {} |", header.join(" | "));
        let _ = writeln!(out, "| --- |{}", " --- |".repeat(lens.len()));
        let mut names: Vec<&str> = Vec::new();
        for c in &run.combine {
            if !names.contains(&c.sum.as_str()) {
                names.push(&c.sum);
            }
        }
        for sum in names {
            let figures: Vec<String> = lens
                .iter()
                .map(|&l| {
                    fig(run
                        .combine
                        .iter()
                        .find(|c| c.sum == sum && c.len_b == l)
                        .and_then(|c| median(&c.ns_per_combine)))
                })
                .collect();
            let _ = writeln!(out, "| {} | {} |", sum, figures.join(" | "));
        }
        let _ = writeln!(out);
    }
}

/// Every run side by side at 64 KiB, cold and hot
///
/// # Arguments
///
/// * `out` - Where the table goes
/// * `runs` - The runs
fn across(out: &mut String, runs: &[RunRecord]) {
    let timed: Vec<&RunRecord> = runs.iter().filter(|run| !run.speed.is_empty()).collect();
    if timed.len() < 2 {
        return;
    }
    for unit in [64 * 1024, 1 << 20] {
        let _ = writeln!(
            out,
            "## Every run at {}, one call, GiB/s cold / hot\n",
            unit_name(unit)
        );
        let header: Vec<String> = timed.iter().map(|run| run.labels.short()).collect();
        let _ = writeln!(out, "| Candidate | {} |", header.join(" | "));
        let _ = writeln!(out, "| --- |{}", " --- |".repeat(timed.len()));
        for sum in candidates(runs) {
            let figures: Vec<String> = timed
                .iter()
                .map(|run| {
                    format!(
                        "{} / {}",
                        fig(cell(run, &sum, unit, "one-shot", false, |c| &c.gib_per_sec)),
                        fig(cell(run, &sum, unit, "one-shot", true, |c| &c.gib_per_sec))
                    )
                })
                .collect();
            let _ = writeln!(out, "| {} | {} |", sum, figures.join(" | "));
        }
        let _ = writeln!(out);
    }
}

/// The summary of some runs: labels, facts, checks across them, each run's speed, and every run
/// side by side
///
/// # Arguments
///
/// * `runs` - The runs
pub fn summary(runs: &[RunRecord]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Runs\n");
    labels(&mut out, runs);
    facts(&mut out, runs);
    checks(&mut out, runs);
    across(&mut out, runs);
    let _ = writeln!(out, "## Speed, run by run\n");
    for run in runs {
        speed(&mut out, run);
    }
    out
}
