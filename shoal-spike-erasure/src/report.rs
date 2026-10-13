//! Turn the JSON of every run into the markdown tables the book carries: one run's full tables, and
//! the summaries that read the runs against each other

use std::collections::BTreeSet;
use std::fmt::Write as _;

use crate::record::{RunRecord, SpeedCell, median};

/// The order candidates are listed in
const CODES: &[&str] = &[
    "xor",
    "rusty_erasure",
    "isa-l",
    "reed-solomon-erasure",
    "reed-solomon-simd",
    "raptorq",
    "rlnc",
    "rlnc, systematic",
];

/// The candidates a run measured: the main ones in the order the tables list them, then any
/// other, such as a kernels pass's forced sets, in the order they were measured
///
/// # Arguments
///
/// * `run` - The run
fn codes_of(run: &RunRecord) -> Vec<String> {
    let mut out: Vec<String> = CODES
        .iter()
        .filter(|code| run.speed.iter().any(|c| c.code == **code))
        .map(|code| code.to_string())
        .collect();
    for c in &run.speed {
        if !out.contains(&c.code) {
            out.push(c.code.clone());
        }
    }
    out
}

/// A unit as the tables name it
///
/// # Arguments
///
/// * `unit` - The unit in bytes
fn unit_name(unit: usize) -> String {
    if unit >= 1024 * 1024 {
        format!("{} MiB", unit / (1024 * 1024))
    } else {
        format!("{} KiB", unit / 1024)
    }
}

/// A figure as the tables print it: two decimals below ten, one below a hundred, none above
///
/// # Arguments
///
/// * `value` - The figure
fn figure(value: f64) -> String {
    if value < 10.0 {
        format!("{value:.2}")
    } else if value < 100.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}

/// The cell for a key in one run, if it was measured
///
/// # Arguments
///
/// * `run` - The run
/// * `code` - The candidate
/// * `layout` - The layout
/// * `unit` - The unit
/// * `op` - The operation
/// * `hot` - Whether it is the hot measurement, one row in cache
fn cell<'a>(
    run: &'a RunRecord,
    code: &str,
    layout: &str,
    unit: usize,
    op: &str,
    hot: bool,
) -> Option<&'a SpeedCell> {
    run.speed.iter().find(|c| {
        c.code == code && c.layout == layout && c.unit == unit && c.op == op && c.hot == hot
    })
}

/// What a table prints for a cell: its median, a dash with nothing measured, or `n/a`
///
/// # Arguments
///
/// * `found` - The cell, if any
fn shown(found: Option<&SpeedCell>) -> String {
    match found {
        Some(cell) => match cell.median() {
            Some(value) => figure(value),
            None => "n/a".to_string(),
        },
        None => "-".to_string(),
    }
}

/// A markdown table with a header row
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `header` - The header cells
/// * `rows` - The rows
fn table(out: &mut String, header: &[String], rows: &[Vec<String>]) {
    let _ = writeln!(out, "| {} |", header.join(" | "));
    let _ = writeln!(
        out,
        "| {} |",
        header.iter().map(|_| "---").collect::<Vec<_>>().join(" | ")
    );
    for row in rows {
        let _ = writeln!(out, "| {} |", row.join(" | "));
    }
    let _ = writeln!(out);
}

/// One run's labels as a table
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `runs` - The runs
fn labels(out: &mut String, runs: &[RunRecord]) {
    let header = [
        "Run",
        "Host",
        "CPU",
        "Build",
        "rse C",
        "Governor",
        "Core",
        "Detected",
        "Runs × budget",
        "Date",
    ]
    .map(str::to_string);
    let rows: Vec<Vec<String>> = runs
        .iter()
        .map(|run| {
            let l = &run.labels;
            vec![
                l.short(),
                l.host.clone(),
                l.cpu.clone(),
                format!("`target-cpu={}`", l.target_cpu),
                format!("`-march={}`", l.rse_arch),
                l.governor.clone(),
                l.core.to_string(),
                l.features.join(", "),
                format!("{} × {} ms", l.runs, l.budget_ms),
                l.date.clone(),
            ]
        })
        .collect();
    table(out, &header, &rows);
}

/// A table of one run: candidates down, units across, at one layout and operation
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `run` - The run
/// * `layout` - The layout
/// * `op` - The operation
/// * `units` - The units across
/// * `hot` - Whether to show the hot measurements
fn by_unit(out: &mut String, run: &RunRecord, layout: &str, op: &str, units: &[usize], hot: bool) {
    let mut header = vec!["Candidate".to_string()];
    header.extend(units.iter().map(|&unit| unit_name(unit)));
    let rows: Vec<Vec<String>> = codes_of(run)
        .iter()
        .filter(|code| {
            units.iter().any(|&unit| {
                cell(run, code, layout, unit, op, hot).is_some_and(|c| c.median().is_some())
            })
        })
        .map(|code| {
            let mut row = vec![code.to_string()];
            row.extend(
                units
                    .iter()
                    .map(|&unit| shown(cell(run, code, layout, unit, op, hot))),
            );
            row
        })
        .collect();
    table(out, &header, &rows);
}

/// A table of one run: candidates down, layouts across, at one unit and operation
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `run` - The run
/// * `unit` - The unit
/// * `op` - An operation, where `decode-m` means as many lost as the layout has parity
/// * `layouts` - The layouts across
/// * `hot` - Whether to show the hot measurements
fn by_layout(
    out: &mut String,
    run: &RunRecord,
    unit: usize,
    op: &str,
    layouts: &[String],
    hot: bool,
) {
    let mut header = vec!["Candidate".to_string()];
    header.extend(layouts.iter().cloned());
    // the operation at a layout, where decode-m is resolved against the layout's m
    let op_at = |layout: &str| -> String {
        if op == "decode-m" {
            format!("decode-{}", layout.split('+').nth(1).unwrap_or("1"))
        } else {
            op.to_string()
        }
    };
    let rows: Vec<Vec<String>> = CODES
        .iter()
        .filter(|code| {
            layouts.iter().any(|layout| {
                cell(run, code, layout, unit, &op_at(layout), hot)
                    .is_some_and(|c| c.median().is_some())
            })
        })
        .map(|code| {
            let mut row = vec![code.to_string()];
            row.extend(
                layouts
                    .iter()
                    .map(|layout| shown(cell(run, code, layout, unit, &op_at(layout), hot))),
            );
            row
        })
        .collect();
    table(out, &header, &rows);
}

/// A table across runs: candidates down, runs across, at one layout, unit and operation
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `runs` - The runs
/// * `layout` - The layout
/// * `unit` - The unit
/// * `op` - The operation
/// * `hot` - Whether to show the hot measurements
fn by_run(out: &mut String, runs: &[RunRecord], layout: &str, unit: usize, op: &str, hot: bool) {
    let mut header = vec!["Candidate".to_string()];
    header.extend(runs.iter().map(|run| run.labels.short()));
    let rows: Vec<Vec<String>> = CODES
        .iter()
        .filter(|code| {
            runs.iter().any(|run| {
                cell(run, code, layout, unit, op, hot).is_some_and(|c| c.median().is_some())
            })
        })
        .map(|code| {
            let mut row = vec![code.to_string()];
            row.extend(
                runs.iter()
                    .map(|run| shown(cell(run, code, layout, unit, op, hot))),
            );
            row
        })
        .collect();
    table(out, &header, &rows);
}

/// The units and the shared layouts every run measured
///
/// # Arguments
///
/// * `runs` - The runs
fn axes(runs: &[RunRecord]) -> (Vec<usize>, Vec<String>) {
    let units: BTreeSet<usize> = runs
        .iter()
        .flat_map(|run| run.speed.iter().map(|c| c.unit))
        .collect();
    // the layouts every candidate runs at are the ones reed-solomon-simd was asked for
    let mut layouts: Vec<String> = Vec::new();
    for run in runs {
        for c in &run.speed {
            if c.code == "reed-solomon-simd" && !layouts.contains(&c.layout) {
                layouts.push(c.layout.clone());
            }
        }
    }
    (units.into_iter().collect(), layouts)
}

/// The correctness checks of every run, read against each other
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `runs` - The runs
fn correctness(out: &mut String, runs: &[RunRecord]) {
    let Some(first) = runs.iter().find_map(|run| run.check.as_ref()) else {
        return;
    };
    // the loss patterns, summed over every layout a candidate ran
    let _ = writeln!(out, "### Loss patterns ({})\n", runs[0].labels.short());
    let header = [
        "Candidate",
        "Layouts",
        "Patterns",
        "Decoded",
        "Undecodable",
        "Undecodable at m lost",
        "Wrong bytes",
        "Rebuilds",
        "Rebuilt wrong",
        "Recoded, then undecodable",
        "Errors",
    ]
    .map(str::to_string);
    let rows: Vec<Vec<String>> = CODES
        .iter()
        .filter_map(|code| {
            let mine: Vec<_> = first.patterns.iter().filter(|p| p.code == *code).collect();
            if mine.is_empty() {
                return None;
            }
            let sum = |f: &dyn Fn(&crate::record::PatternResult) -> u64| {
                mine.iter().map(|p| f(p)).sum::<u64>()
            };
            Some(vec![
                code.to_string(),
                mine.len().to_string(),
                sum(&|p| p.patterns).to_string(),
                sum(&|p| p.decoded).to_string(),
                sum(&|p| p.undecodable).to_string(),
                format!(
                    "{} of {}",
                    sum(&|p| p.undecodable_at_m),
                    sum(&|p| p.patterns_at_m)
                ),
                sum(&|p| p.wrong).to_string(),
                sum(&|p| p.rebuilds).to_string(),
                sum(&|p| p.rebuilt_wrong).to_string(),
                sum(&|p| p.rebuilt_undecodable).to_string(),
                mine.iter()
                    .map(|p| p.errors.len())
                    .sum::<usize>()
                    .to_string(),
            ])
        })
        .collect();
    table(out, &header, &rows);
    // a candidate with undecodable patterns, layout by layout
    for p in first
        .patterns
        .iter()
        .filter(|p| p.undecodable > 0 || !p.errors.is_empty())
    {
        let _ = writeln!(
            out,
            "- {} at {}: {} of {} patterns undecodable ({} of {} with m lost){}",
            p.code,
            p.layout,
            p.undecodable,
            p.patterns,
            p.undecodable_at_m,
            p.patterns_at_m,
            if p.errors.is_empty() {
                String::new()
            } else {
                format!("; errors: {}", p.errors.join("; "))
            }
        );
    }
    let _ = writeln!(out);
    // the random-failure trials
    let _ = writeln!(
        out,
        "### Random coefficients: k chunks that fail to decode\n"
    );
    let header = [
        "Candidate",
        "Layout",
        "Trials",
        "Failures",
        "Fraction",
        "Wrong bytes",
    ]
    .map(str::to_string);
    let rows: Vec<Vec<String>> = first
        .random
        .iter()
        .map(|r| {
            vec![
                r.code.clone(),
                r.layout.clone(),
                r.trials.to_string(),
                r.failures.to_string(),
                format!("{:.3}%", 100.0 * r.failures as f64 / r.trials.max(1) as f64),
                r.wrong.to_string(),
            ]
        })
        .collect();
    table(out, &header, &rows);
    // partial updates against a fresh encode
    let _ = writeln!(out, "### Partial updates against a fresh encode\n");
    let header = [
        "Candidate",
        "Layout",
        "Updates",
        "Parity equals a fresh encode",
        "Note",
    ]
    .map(str::to_string);
    let rows: Vec<Vec<String>> = first
        .updates
        .iter()
        .map(|u| {
            vec![
                u.code.clone(),
                u.layout.clone(),
                u.updates.to_string(),
                u.equal.map_or("-".to_string(), |e| {
                    if e {
                        "yes".to_string()
                    } else {
                        "**no**".to_string()
                    }
                }),
                u.note.clone().unwrap_or_default(),
            ]
        })
        .collect();
    table(out, &header, &rows);
    // digests across every run
    let _ = writeln!(out, "### The same input, the same bytes?\n");
    let mut header = vec!["Candidate".to_string(), "Layout".to_string()];
    header.extend(runs.iter().map(|run| run.labels.short()));
    header.push("Same in every run".to_string());
    let rows: Vec<Vec<String>> = first
        .digests
        .iter()
        .map(|d| {
            let mut row = vec![d.code.clone(), d.layout.clone()];
            let found: Vec<Option<&crate::record::DigestResult>> = runs
                .iter()
                .map(|run| {
                    run.check.as_ref().and_then(|c| {
                        c.digests
                            .iter()
                            .find(|x| x.code == d.code && x.layout == d.layout)
                    })
                })
                .collect();
            row.extend(found.iter().map(|f| match f {
                Some(x) if x.repeatable => format!("`{}`", &x.digest[..8]),
                Some(x) => format!("`{}` (not repeatable)", &x.digest[..8]),
                None => "-".to_string(),
            }));
            let same = found
                .iter()
                .all(|f| f.is_some_and(|x| x.digest == d.digest && x.repeatable));
            row.push(if same {
                "yes".to_string()
            } else {
                "**no**".to_string()
            });
            row
        })
        .collect();
    table(out, &header, &rows);
    // which candidates agree byte for byte
    let _ = writeln!(out, "### Parity that is the same bytes\n");
    let header = [
        "Encoded by",
        "Compared with",
        "Layouts",
        "Equal at every layout",
    ]
    .map(str::to_string);
    let mut pairs: Vec<(String, String)> = Vec::new();
    for c in &first.compat {
        if !pairs.contains(&(c.left.clone(), c.right.clone())) {
            pairs.push((c.left.clone(), c.right.clone()));
        }
    }
    let rows: Vec<Vec<String>> = pairs
        .iter()
        .map(|(left, right)| {
            let mine: Vec<_> = first
                .compat
                .iter()
                .filter(|c| &c.left == left && &c.right == right)
                .collect();
            vec![
                left.clone(),
                right.clone(),
                mine.iter()
                    .map(|c| c.layout.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                if mine.iter().all(|c| c.equal) {
                    "yes".to_string()
                } else {
                    "**no**".to_string()
                },
            ]
        })
        .collect();
    table(out, &header, &rows);
}

/// Each candidate's threads and allocations in every run
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `runs` - The runs
fn facts(out: &mut String, runs: &[RunRecord]) {
    for run in runs {
        let _ = writeln!(
            out,
            "### Threads, kernels and allocations ({})\n",
            run.labels.short()
        );
        let header = [
            "Candidate",
            "Kernels",
            "Threads before → after",
            "encode",
            "decode-1",
            "rebuild",
            "update",
        ]
        .map(str::to_string);
        let rows: Vec<Vec<String>> = run
            .facts
            .iter()
            .map(|f| {
                let mut row = vec![
                    f.code.clone(),
                    f.kernels.clone().unwrap_or_else(|| "-".to_string()),
                    format!("{} → {}", f.threads_before, f.threads_after),
                ];
                for op in ["encode", "decode-1", "rebuild", "update"] {
                    row.push(match f.allocations.iter().find(|a| a.op == op) {
                        Some(a) if a.note.is_some() => "n/a".to_string(),
                        Some(a) => format!("{} / {} B", a.allocations, a.bytes),
                        None => "-".to_string(),
                    });
                }
                row
            })
            .collect();
        table(out, &header, &rows);
    }
}

/// The kernels passes: `rusty_erasure` forced to each kernel set, one table a host, cold and hot
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `runs` - The kernels passes
fn kernel_sets(out: &mut String, runs: &[RunRecord]) {
    // the columns: the operations that matter at the two layouts the pass runs, at 64 KiB
    let columns = [
        ("4+2", "encode", "4+2 encode"),
        ("4+2", "decode-2", "4+2 decode, 2 lost"),
        ("4+2", "rebuild", "4+2 rebuild"),
        ("4+2", "update", "4+2 update"),
        ("10+4", "encode", "10+4 encode"),
        ("10+4", "decode-4", "10+4 decode, 4 lost"),
    ];
    for run in runs {
        // the kernel sets in the order they were measured
        let mut codes: Vec<String> = Vec::new();
        for c in &run.speed {
            if !codes.contains(&c.code) {
                codes.push(c.code.clone());
            }
        }
        for hot in [false, true] {
            let _ = writeln!(
                out,
                "#### {}, {}: GiB/s at 64 KiB\n",
                run.labels.short(),
                temperature(hot)
            );
            let mut header = vec!["Kernel set".to_string()];
            header.extend(columns.iter().map(|(_, _, name)| name.to_string()));
            let rows: Vec<Vec<String>> =
                codes
                    .iter()
                    .map(|code| {
                        let mut row = vec![code.clone()];
                        row.extend(columns.iter().map(|(layout, op, _)| {
                            shown(cell(run, code, layout, 64 * 1024, op, hot))
                        }));
                        row
                    })
                    .collect();
            table(out, &header, &rows);
        }
        let (units, _) = axes(std::slice::from_ref(run));
        for hot in [false, true] {
            let _ = writeln!(
                out,
                "#### {}, {}: encode at 4+2 by unit, GiB/s\n",
                run.labels.short(),
                temperature(hot)
            );
            let mut header = vec!["Kernel set".to_string()];
            header.extend(units.iter().map(|&unit| unit_name(unit)));
            let rows: Vec<Vec<String>> = codes
                .iter()
                .map(|code| {
                    let mut row = vec![code.clone()];
                    row.extend(
                        units
                            .iter()
                            .map(|&unit| shown(cell(run, code, "4+2", unit, "encode", hot))),
                    );
                    row
                })
                .collect();
            table(out, &header, &rows);
        }
    }
}

/// How a measurement's data sat: in cache or out of it
///
/// # Arguments
///
/// * `hot` - Whether it is the hot measurement
fn temperature(hot: bool) -> &'static str {
    if hot {
        "hot (one row, in cache)"
    } else {
        "cold (rows in turn, out of cache)"
    }
}

/// The summaries that read the runs against each other, for the book's page
///
/// # Arguments
///
/// * `all` - Every run, the kernels passes among them
pub fn summary(all: &[RunRecord]) -> String {
    let mut out = String::new();
    // the kernels passes are read on their own
    let (kernels, runs): (Vec<&RunRecord>, Vec<&RunRecord>) =
        all.iter().partition(|run| run.labels.is_kernels());
    let runs: Vec<RunRecord> = runs.into_iter().map(clone_run).collect();
    let kernels: Vec<RunRecord> = kernels.into_iter().map(clone_run).collect();
    if !kernels.is_empty() {
        let _ = writeln!(out, "## rusty_erasure by kernel set\n");
        labels(&mut out, &kernels);
        kernel_sets(&mut out, &kernels);
    }
    if runs.is_empty() {
        return out;
    }
    let runs = &runs[..];
    let (units, layouts) = axes(runs);
    let _ = writeln!(out, "## Runs\n");
    labels(&mut out, runs);
    let _ = writeln!(out, "## Correctness\n");
    correctness(&mut out, runs);
    let _ = writeln!(out, "## Facts\n");
    facts(&mut out, runs);
    // every target side by side: the operations that matter at two layouts, cold and hot
    for hot in [false, true] {
        for (layout, lost) in [("4+2", "decode-2"), ("10+4", "decode-4")] {
            let _ = writeln!(
                out,
                "## By target, {layout} at 64 KiB, {}\n",
                temperature(hot)
            );
            for (op, what) in [
                ("encode", "Encode, GiB/s of stripe data".to_string()),
                (
                    "decode-1",
                    "Decode with one data chunk lost, GiB/s of stripe data".to_string(),
                ),
                (
                    lost,
                    format!(
                        "Decode with {} data chunks lost, GiB/s of stripe data",
                        &lost[7..]
                    ),
                ),
                (
                    "rebuild",
                    "Rebuild one chunk, GiB/s of chunk rebuilt".to_string(),
                ),
                (
                    "update",
                    "Update one data chunk, GiB/s of bytes changed".to_string(),
                ),
            ] {
                let _ = writeln!(out, "#### {what}\n");
                by_run(&mut out, runs, layout, 64 * 1024, op, hot);
            }
        }
        for unit in [4 * 1024, 1024 * 1024] {
            let _ = writeln!(
                out,
                "## By target, 4+2 encode at {}, {}\n",
                unit_name(unit),
                temperature(hot)
            );
            by_run(&mut out, runs, "4+2", unit, "encode", hot);
        }
    }
    let _ = writeln!(
        out,
        "## rlnc's healthy read: decode with nothing lost at 4+2 and 64 KiB, cold\n"
    );
    by_run(&mut out, runs, "4+2", 64 * 1024, "decode-0", false);
    for run in runs {
        let _ = writeln!(out, "## {}\n", run.labels.short());
        for hot in [false, true] {
            let _ = writeln!(
                out,
                "#### Encode at 4+2 by unit, {}, GiB/s of stripe data\n",
                temperature(hot)
            );
            by_unit(&mut out, run, "4+2", "encode", &units, hot);
            let _ = writeln!(
                out,
                "#### Microseconds a call: encode at 4+2 by unit, {}\n",
                temperature(hot)
            );
            by_unit_us(&mut out, run, "4+2", "encode", &units, hot);
            let _ = writeln!(
                out,
                "#### Update one data chunk at 4+2 by unit, {}, GiB/s of bytes changed\n",
                temperature(hot)
            );
            by_unit(&mut out, run, "4+2", "update", &units, hot);
        }
        let _ = writeln!(
            out,
            "#### Encode at 64 KiB by layout, cold, GiB/s of stripe data\n"
        );
        by_layout(&mut out, run, 64 * 1024, "encode", &layouts, false);
        let _ = writeln!(
            out,
            "#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data\n"
        );
        by_layout(&mut out, run, 64 * 1024, "decode-m", &layouts, false);
        let _ = writeln!(
            out,
            "#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt\n"
        );
        by_layout(&mut out, run, 64 * 1024, "rebuild", &layouts, false);
        // the k+1 reference rows, where they were measured
        let reference: Vec<String> = ["2+1", "4+1", "6+1", "8+1", "10+1"]
            .map(str::to_string)
            .to_vec();
        if run.speed.iter().any(|c| c.layout == "4+1") {
            let _ = writeln!(
                out,
                "#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data\n"
            );
            by_layout(&mut out, run, 64 * 1024, "encode", &reference, false);
        }
        // the largest spread among the measurements, so a reader knows how far a figure moves
        let worst = run
            .speed
            .iter()
            .filter_map(|c| c.spread().map(|s| (s, c)))
            .max_by(|a, b| a.0.total_cmp(&b.0));
        let typical = median(
            &run.speed
                .iter()
                .filter_map(SpeedCell::spread)
                .collect::<Vec<_>>(),
        );
        if let (Some((spread, c)), Some(typical)) = (worst, typical) {
            let _ = writeln!(
                out,
                "Spread of the {} measurements a cell, (max - min) / median: {:.1}% typical; the widest {:.1}%, {} {} {} at {}{}.\n",
                run.labels.runs,
                typical * 100.0,
                spread * 100.0,
                c.code,
                c.op,
                c.layout,
                unit_name(c.unit),
                if c.hot { " hot" } else { "" }
            );
        }
    }
    out
}

/// A copy of a run, so the runs can be split without moving them
///
/// # Arguments
///
/// * `run` - The run
fn clone_run(run: &RunRecord) -> RunRecord {
    // a round trip through JSON is a deep copy, and the records are small
    serde_json::from_str(&serde_json::to_string(run).expect("a record serializes"))
        .expect("a record deserializes")
}

/// Like [`by_unit`], in microseconds a call
///
/// # Arguments
///
/// * `out` - Where it is written
/// * `run` - The run
/// * `layout` - The layout
/// * `op` - The operation
/// * `units` - The units across
/// * `hot` - Whether to show the hot measurements
fn by_unit_us(
    out: &mut String,
    run: &RunRecord,
    layout: &str,
    op: &str,
    units: &[usize],
    hot: bool,
) {
    let mut header = vec!["Candidate".to_string()];
    header.extend(units.iter().map(|&unit| unit_name(unit)));
    let rows: Vec<Vec<String>> = codes_of(run)
        .iter()
        .filter(|code| {
            units.iter().any(|&unit| {
                cell(run, code, layout, unit, op, hot).is_some_and(|c| c.median().is_some())
            })
        })
        .map(|code| {
            let mut row = vec![code.to_string()];
            row.extend(units.iter().map(|&unit| {
                cell(run, code, layout, unit, op, hot)
                    .and_then(SpeedCell::median_us)
                    .map_or("-".to_string(), figure)
            }));
            row
        })
        .collect();
    table(out, &header, &rows);
}

/// Every cell of one run, for the results committed beside the harness
///
/// # Arguments
///
/// * `run` - The run
pub fn full(run: &RunRecord) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# X4 results: {}\n", run.labels.short());
    let _ = writeln!(
        out,
        "Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of {} measurements, each at least {} ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.\n",
        run.labels.runs, run.labels.budget_ms
    );
    labels(&mut out, std::slice::from_ref(run));
    let (units, _) = axes(std::slice::from_ref(run));
    let mut layouts: Vec<String> = Vec::new();
    for c in &run.speed {
        if !layouts.contains(&c.layout) {
            layouts.push(c.layout.clone());
        }
    }
    for layout in &layouts {
        let m: usize = layout
            .split('+')
            .nth(1)
            .and_then(|m| m.parse().ok())
            .unwrap_or(1);
        let mut ops = vec!["encode".to_string(), "decode-0".to_string()];
        ops.extend((1..=m).map(|j| format!("decode-{j}")));
        ops.push("rebuild".to_string());
        ops.push("update".to_string());
        for op in ops {
            if !run
                .speed
                .iter()
                .any(|c| &c.layout == layout && c.op == op && c.median().is_some())
            {
                continue;
            }
            for hot in [false, true] {
                if !run.speed.iter().any(|c| {
                    &c.layout == layout && c.op == op && c.hot == hot && c.median().is_some()
                }) {
                    continue;
                }
                let _ = writeln!(out, "## {layout}, {op}, {}, GiB/s\n", temperature(hot));
                by_unit(&mut out, run, layout, &op, &units, hot);
            }
        }
    }
    // why a cell has no figure
    let _ = writeln!(out, "## Cells not measured\n");
    let mut notes: Vec<String> = run
        .speed
        .iter()
        .filter_map(|c| {
            c.note
                .as_ref()
                .map(|note| format!("- {} {} {}: {}", c.code, c.op, c.layout, note))
        })
        .collect();
    notes.sort();
    notes.dedup();
    for note in notes {
        let _ = writeln!(out, "{note}");
    }
    out
}
