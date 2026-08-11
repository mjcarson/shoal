//! Judging a capture against one or more baselines
//!
//! An optimization is judged against two baselines, not one:
//!
//! | Baseline | What it tells you |
//! | --- | --- |
//! | `B1-performance` | the frozen one: the cumulative gain since the harness was built, which is the number that matters over a series of changes |
//! | `trailing` | the last accepted run: what this change alone did, which is the number that decides whether to keep it |
//!
//! One without the other misleads. Against the frozen baseline alone a regression hides inside an
//! earlier win; against the trailing baseline alone a series of changes that each look neutral can
//! drift a long way from where they started. Both are compared unless the caller names others.

pub mod macro_layer;
pub mod micro;

use anyhow::{Result, bail};

use crate::cli::{CompareArgs, Format};
use crate::fmt;
use crate::model::meta::CaptureMeta;
use crate::registry::Layer;
use crate::store::{FROZEN_BASELINE, Store, TRAILING_BASELINE};

/// The exit code a comparison uses to report a regression
pub const EXIT_REGRESSION: i32 = 3;

/// Runs `shoal-bench compare`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_compare(store: &Store, args: &CompareArgs) -> Result<i32> {
    // the two standard baselines unless the caller named others
    let against: Vec<String> = if args.against.is_empty() {
        vec![FROZEN_BASELINE.to_string(), TRAILING_BASELINE.to_string()]
    } else {
        args.against.clone()
    };
    // which layers to compare, defaulting to the two that can be compared at all
    let wants = |layer: Layer| args.layers.is_empty() || args.layers.contains(&layer);
    // how wide a difference has to be before it is called a result
    let band = match args.noise_pct {
        Some(pct) => micro::NoiseBand::flat(pct),
        None => micro::NoiseBand::default(),
    };
    // a partial capture is called out before anything else it produced is read
    let run_meta = store.read_meta(&args.run).unwrap_or(None);
    warn_if_partial(&args.run, run_meta.as_ref());
    let mut regressed = false;
    // compare against each baseline in turn, since each answers a different question
    for baseline in &against {
        if wants(Layer::Micro) {
            regressed |= compare_micro_against(store, &args.run, baseline, &band, args.format)?;
        }
        if wants(Layer::Macro) {
            regressed |= compare_macro_against(store, &args.run, baseline, args.format)?;
        }
    }
    // the instrumented layers have no comparison, and saying so is better than leaving a caller
    // to wonder whether one silently passed
    if wants(Layer::Hotpath) || wants(Layer::Stages) {
        eprintln!(
            "\nnote: the hotpath and stage layers attribute time and are not compared; read them \
             on the benchmark results page instead"
        );
    }
    // a regression is only an error when the caller asked for it to be one
    if regressed && args.fail_on_regression {
        return Ok(EXIT_REGRESSION);
    }
    Ok(0)
}

/// Warns when a capture only covers part of the registry
///
/// Printed before any table rather than after, because a comparison against a partial capture is
/// a comparison over a narrower set of benchmarks than it looks like.
///
/// # Arguments
///
/// * `label` - The capture's name
/// * `meta` - Its provenance, if it has any
fn warn_if_partial(label: &str, meta: Option<&CaptureMeta>) {
    // only a capture that recorded its own partiality can report it
    let Some(meta) = meta else {
        return;
    };
    if meta.partial {
        eprintln!(
            "partial: {label} captured {} of {} benchmarks with filter {:?}; everything below is \
             over that subset only",
            meta.selected, meta.registry_total, meta.filter
        );
    }
}

/// Compares one capture's micro layer against one baseline
///
/// Returns whether anything regressed.
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `run` - The capture being judged
/// * `baseline` - What to judge it against
/// * `band` - How wide a difference has to be before it counts
/// * `format` - How to print the result
fn compare_micro_against(
    store: &Store,
    run: &str,
    baseline: &str,
    band: &micro::NoiseBand,
    format: Format,
) -> Result<bool> {
    let (run_path, run_capture) = store.resolve_micro(run)?;
    // a baseline that is not there is skipped with a warning rather than failing the command, so
    // that naming two baselines and having one missing still produces the other's table
    let Ok((base_path, base_capture)) = store.resolve_micro(baseline) else {
        eprintln!("warning: skipping missing baseline {baseline}");
        return Ok(false);
    };
    let comparison = micro::compare(&run_capture, &base_capture, band);
    // an empty join means the two captures have nothing in common, which is not a comparison
    if comparison.rows.is_empty() {
        bail!(
            "{} and {} share no benchmarks, so there is nothing to compare",
            run_path.display(),
            base_path.display()
        );
    }
    // print it in whichever shape was asked for
    match format {
        Format::Json => {
            println!("{}", serde_json::to_string_pretty(&comparison)?);
        }
        Format::Markdown => print_micro_markdown(&comparison),
        Format::Text => {
            println!(
                "\n=== {} vs {} ({}) ===",
                file_name(&run_path),
                file_name(&base_path),
                band.describe()
            );
            print_micro_text(&comparison);
        }
    }
    Ok(comparison.has_regression())
}

/// Prints a micro comparison as aligned columns
///
/// # Arguments
///
/// * `comparison` - What the comparison found
fn print_micro_text(comparison: &micro::MicroComparison) {
    // a header, then one line per benchmark in the order the comparison sorted them
    println!(
        "{:<50} {:>16} {:>16} {:>9}  {}",
        "benchmark", "baseline", "run", "change", ""
    );
    for row in &comparison.rows {
        // both cells share the unit the baseline's magnitude selected, so they can be read
        // against each other
        let (before, after) = fmt::duration_pair(row.baseline_ns, row.run_ns);
        println!(
            "{:<50} {:>16} {:>16} {:>9}  {}",
            row.name,
            before,
            after,
            fmt::signed_pct(row.pct),
            if row.significant { "" } else { "(within noise)" }
        );
    }
    // then anything present on only one side, which a join would otherwise hide
    print_set_differences(comparison);
}

/// Prints what only one side of a comparison measured
///
/// # Arguments
///
/// * `comparison` - What the comparison found
fn print_set_differences(comparison: &micro::MicroComparison) {
    // a benchmark that silently vanished from a comparison is how a regression gets missed
    if !comparison.only_in_baseline.is_empty() {
        println!("  not in this run: {}", comparison.only_in_baseline.join(", "));
    }
    if !comparison.only_in_run.is_empty() {
        println!("  new in this run: {}", comparison.only_in_run.join(", "));
    }
}

/// Prints a micro comparison as a markdown table
///
/// # Arguments
///
/// * `comparison` - What the comparison found
fn print_micro_markdown(comparison: &micro::MicroComparison) {
    println!("| Benchmark | Baseline | Run | Change | |");
    println!("| --- | ---: | ---: | ---: | --- |");
    for row in &comparison.rows {
        let (before, after) = fmt::duration_pair(row.baseline_ns, row.run_ns);
        println!(
            "| `{}` | {} | {} | {} | {} |",
            row.name,
            before,
            after,
            fmt::signed_pct(row.pct),
            if row.significant { "" } else { "within noise" }
        );
    }
}

/// Compares one capture's macro layer against one baseline
///
/// Returns whether anything regressed.
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `run` - The capture being judged
/// * `baseline` - What to judge it against
/// * `format` - How to print the result
fn compare_macro_against(
    store: &Store,
    run: &str,
    baseline: &str,
    format: Format,
) -> Result<bool> {
    // both sides need a macro artifact. a baseline that is only a micro capture - which
    // `trailing` is - simply has no macro layer to compare against.
    let Some((run_path, run_capture)) = store.resolve_macro(run)? else {
        return Ok(false);
    };
    let Some((base_path, base_capture)) = store.resolve_macro(baseline)? else {
        return Ok(false);
    };
    let comparison = macro_layer::compare(&run_capture, &base_capture);
    // print it in whichever shape was asked for
    match format {
        Format::Json => {
            println!("{}", serde_json::to_string_pretty(&comparison)?);
        }
        Format::Markdown => print_macro_markdown(&comparison),
        Format::Text => {
            println!(
                "\n=== macro: {} vs {} (a result is two disjoint intervals) ===",
                file_name(&run_path),
                file_name(&base_path)
            );
            print_macro_text(&comparison);
        }
    }
    Ok(comparison.has_regression())
}

/// Renders one macro measurement and its interval
///
/// # Arguments
///
/// * `row` - The row being rendered
/// * `value` - The measurement to render
/// * `interval` - Its observed interval, if it has one
fn macro_cell(row: &macro_layer::MacroRow, value: f64, interval: Option<(f64, f64)>) -> String {
    // the interval is what the verdict rests on, so it is always shown when there is one, in the
    // same unit as the measurement it brackets. throughput is a rate and does not scale like a
    // duration.
    fmt::scaled_interval(value, interval, is_throughput(row))
}

/// Describes what a macro comparison concluded about one metric
///
/// # Arguments
///
/// * `row` - The row being described
fn macro_note(row: &macro_layer::MacroRow) -> String {
    // say what was concluded and why, since the percentage alone is not the finding here
    match row.verdict {
        macro_layer::MacroVerdict::Result => {
            let gap = row.gap.unwrap_or(0.0);
            let gap = fmt::scaled_interval(gap, None, is_throughput(row));
            format!(
                "disjoint by at least {gap}, {}",
                if row.is_regression() {
                    "worse"
                } else {
                    "better"
                }
            )
        }
        macro_layer::MacroVerdict::NotAResult => "intervals overlap, not a result".to_string(),
        macro_layer::MacroVerdict::NoErrorBar => "no error bar, one run's distribution".to_string(),
        macro_layer::MacroVerdict::Absent => "not in both captures, nothing to compare".to_string(),
    }
}

/// Whether a row measures a rate rather than a duration
///
/// Every metric now carries the workload it belongs to, so the throughput row of
/// `macro/insert_unsorted` is `macro/insert_unsorted/throughput` rather than one fixed string.
///
/// # Arguments
///
/// * `row` - The row to classify
fn is_throughput(row: &macro_layer::MacroRow) -> bool {
    // a rate does not scale like a duration, so this decides which unit the value is printed in
    row.metric.ends_with("/throughput")
}

/// Prints a macro comparison as aligned columns
///
/// # Arguments
///
/// * `comparison` - What the comparison found
fn print_macro_text(comparison: &macro_layer::MacroComparison) {
    // two captures with no workload in common produce a table of nothing but absences, which reads
    // as "nothing moved" unless it is said outright
    if comparison.disjoint {
        println!(
            "  these two captures share no workload, so nothing here is comparable\n    \
             this run: {}\n    baseline: {}",
            join_or_none(&comparison.only_in_run),
            join_or_none(&comparison.only_in_baseline)
        );
        // a version 1 capture's only workload is the `tmdb` example, which was retired with its
        // dataset. saying so here saves the reader working out why nothing lines up.
        if comparison
            .only_in_baseline
            .iter()
            .any(|id| id == crate::model::macro_layer::TMDB_WORKLOAD)
        {
            println!(
                "    the baseline predates purpose built workloads: its macro half measured the \
                 `tmdb` example over a dataset that was never in the repository, and has no \
                 replacement. Its micro half is unaffected and is compared above."
            );
        }
        return;
    }
    // a header, then one line per metric in the fixed order the comparison builds them
    let width = comparison
        .rows
        .iter()
        .map(|row| row.metric.len())
        .max()
        .unwrap_or(22)
        .max(6);
    println!(
        "{:<width$} {:>30} {:>30} {:>9}  {}",
        "metric", "baseline", "run", "change", ""
    );
    for row in &comparison.rows {
        // an absent workload has no measurement on either side, and printing a zero for one would
        // read as a measurement of zero rather than as an absence
        let (baseline, run, change) = if row.verdict == macro_layer::MacroVerdict::Absent {
            ("-".to_string(), "-".to_string(), "-".to_string())
        } else {
            (
                macro_cell(row, row.baseline, row.baseline_interval),
                macro_cell(row, row.run, row.run_interval),
                fmt::signed_pct(row.pct),
            )
        };
        println!(
            "{:<width$} {baseline:>30} {run:>30} {change:>9}  {}",
            row.metric,
            macro_note(row)
        );
    }
    // then anything that was on one side only, which the rows above named but did not explain
    if !comparison.missing().is_empty() {
        println!(
            "  measured on only one side: {}",
            comparison.missing().join(", ")
        );
    }
}

/// Renders a list of workload identifiers, or says there are none
///
/// # Arguments
///
/// * `ids` - The identifiers to render
fn join_or_none(ids: &[String]) -> String {
    // an empty side is a fact worth printing rather than a blank
    if ids.is_empty() {
        "none".to_string()
    } else {
        ids.join(", ")
    }
}

/// Prints a macro comparison as a markdown table
///
/// # Arguments
///
/// * `comparison` - What the comparison found
fn print_macro_markdown(comparison: &macro_layer::MacroComparison) {
    println!("| Metric | Baseline | Run | Change | Verdict |");
    println!("| --- | ---: | ---: | ---: | --- |");
    for row in &comparison.rows {
        println!(
            "| `{}` | {} | {} | {} | {} |",
            row.metric,
            macro_cell(row, row.baseline, row.baseline_interval),
            macro_cell(row, row.run, row.run_interval),
            fmt::signed_pct(row.pct),
            macro_note(row)
        );
    }
}

/// The file name of a path, for a heading
///
/// # Arguments
///
/// * `path` - The path to name
fn file_name(path: &std::path::Path) -> String {
    // the whole path is noise in a heading; the file name is what identifies the capture
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("?")
        .to_string()
}
