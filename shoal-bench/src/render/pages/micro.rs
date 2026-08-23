//! The criterion layer: what moved, how it scales, and what it can resolve
//!
//! Kept apart from every other page on purpose. The micro layer measures functions rather than
//! workloads, so its numbers answer "did this change make this function slower" and never "how will
//! this handle my data" - and a reader arriving with the second question is worse off for meeting
//! the first.

use anyhow::Result;

use crate::compare::micro::{self, MicroComparison};
use crate::fmt;
use crate::render::badges;
use crate::render::chart;
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;
use crate::registry::Layer;

/// Builds the micro layer page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Micro,
        page,
        "Criterion benchmarks over functions, not workloads. **These numbers do not describe what \
         Shoal will do with your data** - they exist to say whether one change made one function \
         faster or slower, which is a question the end to end layers are far too noisy to answer. \
         Every other page on this site measures a live server.",
    );
    out.push_str(&against_baseline(page)?);
    out.push_str(&scaling(page)?);
    out.push_str(&resolution(page)?);
    out.push_str(&footer(Surface::Micro));
    Ok(out)
}

/// The current capture against the frozen baseline, and against the trailing one
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn against_baseline(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Against the baseline\n\n");
    let (Some(current), Some((frozen_name, frozen))) = (page.current(), page.frozen.as_ref())
    else {
        out.push_str("No capture and baseline pair is available to compare.\n\n");
        return Ok(out);
    };
    let Some(measured) = &current.micro else {
        out.push_str("The current capture has no micro layer to compare.\n\n");
        return Ok(out);
    };
    let comparison = micro::compare(measured, frozen, &micro::NoiseBand::default());
    if comparison.rows.is_empty() {
        out.push_str("The current capture and the baseline share no benchmarks.\n\n");
        return Ok(out);
    }
    out.push_str(&format!(
        "`{}` against the frozen baseline `{frozen_name}`. The shaded band behind each bar is the \
         width inside which a difference is not a result: ±{}% for a benchmark that takes under \
         {}, ±{}% for one that takes longer. **A bar that does not escape its own shading has not \
         moved.**\n\n",
        current.label,
        micro::NOISE_FAST_PCT,
        fmt::duration_ns(micro::FAST_THRESHOLD_NS),
        micro::NOISE_SLOW_PCT,
    ));
    out.push_str(
        "The tiers are screens rather than bounds - see [what the micro layer can \
         resolve](#what-the-micro-layer-can-resolve) below - and a result is only accepted after a \
         confirming repeat of the whole capture.\n\n",
    );
    if let Some(status) = page.status(&current.label) {
        let shown = badges::for_layer(status, Layer::Micro);
        if !shown.is_empty() {
            out.push_str(&format!("{shown}\n\n"));
        }
    }
    out.push_str(&chart::micro_delta::draw(&comparison.rows, frozen_name)?);
    out.push('\n');
    out.push_str(&caption(
        "Every benchmark both captures measured, from the largest improvement to the largest \
         regression. Bars inside the shaded band are drawn grey because they are not results.",
    ));
    out.push_str(&set_differences(&comparison, &current.label, frozen_name));
    out.push_str(&tables::micro_comparison(&comparison));
    out.push('\n');
    // the same capture against the trailing baseline, which answers a different question
    if let Some((trailing_name, trailing)) = page.trailing.as_ref() {
        let against_trailing = micro::compare(measured, trailing, &micro::NoiseBand::default());
        let moved = against_trailing
            .rows
            .iter()
            .filter(|row| row.significant)
            .count();
        out.push_str(&format!(
            "### Against `{trailing_name}`\n\nThe frozen baseline says how far the whole series of \
             changes has come; the trailing one says what the last change did. Against \
             `{trailing_name}`, {moved} of {} benchmarks moved outside the band.\n\n",
            against_trailing.rows.len()
        ));
        if moved > 0 {
            let rows: Vec<_> = against_trailing
                .rows
                .iter()
                .filter(|row| row.significant)
                .cloned()
                .collect();
            out.push_str(&tables::micro_comparison(&MicroComparison {
                rows,
                only_in_baseline: Vec::new(),
                only_in_run: Vec::new(),
            }));
            out.push('\n');
        }
    }
    Ok(out)
}

/// What only one side of a comparison measured
///
/// # Arguments
///
/// * `comparison` - What the comparison found
/// * `run` - The capture's name
/// * `baseline` - The baseline's name
fn set_differences(comparison: &MicroComparison, run: &str, baseline: &str) -> String {
    let mut out = String::new();
    // a benchmark that silently vanished from a comparison is how a regression gets missed, so
    // both directions are stated rather than left to be noticed
    if !comparison.only_in_baseline.is_empty() {
        out.push_str(&format!(
            "`{baseline}` measures {} benchmarks that `{run}` does not, so they are not compared \
             above: {}\n\n",
            comparison.only_in_baseline.len(),
            list_code(&comparison.only_in_baseline)
        ));
    }
    if !comparison.only_in_run.is_empty() {
        out.push_str(&format!(
            "`{run}` measures {} benchmarks that `{baseline}` predates, so they have no baseline \
             to be compared against: {}\n\n",
            comparison.only_in_run.len(),
            list_code(&comparison.only_in_run)
        ));
    }
    out
}

/// Renders a list of names as inline code, comma separated
///
/// # Arguments
///
/// * `names` - The names to render
fn list_code(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<String>>()
        .join(", ")
}

/// How each operation's cost grows with what it was measured over
///
/// Two charts rather than one, because the number at the end of a benchmark id counts rows on one
/// of them and bytes on the other. See [`chart::micro_scaling::ScalingAxis`].
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn scaling(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## How the cost grows with the partition\n\n");
    let Some(measured) = page.current().and_then(|current| current.micro.as_ref()) else {
        out.push_str("The current capture has no micro layer.\n\n");
        return Ok(out);
    };
    let all = chart::micro_scaling::families(measured);
    let rows = chart::micro_scaling::on_axis(&all, chart::micro_scaling::ScalingAxis::Rows);
    if rows.is_empty() {
        out.push_str("No benchmark was measured at more than one partition size.\n\n");
    } else {
        out.push_str(
            "Both axes are logarithmic, which makes the *shape* of the growth readable rather than \
             the magnitude: a cost proportional to the number of rows is a straight diagonal, a \
             cost independent of it is flat, and anything between is a slope. This is the chart \
             that says whether an operation seeks into a partition or walks it, which is a \
             different question from whether it is fast, and one the noise band has no bearing \
             on.\n\n",
        );
        out.push_str(&chart::micro_scaling::draw(
            &rows,
            chart::micro_scaling::ScalingAxis::Rows,
        )?);
        out.push('\n');
        out.push_str(&caption(
            "Mean time against partition size, both axes logarithmic: a cost proportional to the \
             partition is a straight diagonal, a cost independent of it is flat, and the table \
             below carries the same numbers for anyone the colours do not separate.",
        ));
        out.push_str(&tables::micro_scaling(&rows));
        out.push('\n');
    }
    out.push_str(&width_scaling(&all));
    Ok(out)
}

/// How each operation's cost grows with the width of one row
///
/// The axis the codec benchmarks did not have. `wire_codec` swept how many queries a bundle carries
/// and how many rows a response answers with, both over a row of about thirty bytes, so the
/// **per-byte** half of [O1](../appendix/optimizations.md) and [O2](../appendix/optimizations.md)
/// was unmeasured here entirely - and those two are most of what
/// [Row size and what it costs](../tables/row-size.md) argues about.
///
/// # Arguments
///
/// * `all` - Every family the capture produced, on either axis
fn width_scaling(all: &[chart::micro_scaling::Family]) -> String {
    let mut out = String::new();
    out.push_str("## How the cost grows with the width of one row\n\n");
    let widths = chart::micro_scaling::on_axis(all, chart::micro_scaling::ScalingAxis::Bytes);
    if widths.is_empty() {
        out.push_str("No benchmark was measured at more than one row width.\n\n");
        return out;
    }
    out.push_str(
        "The same log-log reading as above, over a different quantity: how wide one row is rather \
         than how many of them there are. A **flat** line is a fixed per-call cost - the header \
         decode is here as exactly that control, since eight bytes is eight bytes at every width. \
         A **diagonal** is a cost that walks the payload, and the gap between `access` and \
         `deserialize` is the one that matters: `access` validates an archive in place while \
         `deserialize` copies every string out of a buffer that already held it in a readable \
         layout.\n\n",
    );
    match chart::micro_scaling::draw(&widths, chart::micro_scaling::ScalingAxis::Bytes) {
        Ok(chart) => {
            out.push_str(&chart);
            out.push('\n');
            out.push_str(&caption(
                "Mean time against the width of one row, both axes logarithmic. The bundle size \
                 and the response cardinality are held fixed here, so the width is the only thing \
                 moving - which is what makes a diagonal a per-byte cost rather than a per-row \
                 one.",
            ));
        }
        // a chart that could not be built is not a reason for the page to fail, the same way the
        // rest of this file treats one
        Err(err) => out.push_str(&format!("The width chart could not be drawn: {err}.\n\n")),
    }
    out.push_str(&tables::micro_scaling(&widths));
    out.push('\n');
    out
}

/// What the micro layer can resolve
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn resolution(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What the micro layer can resolve\n\n");
    if page.repeats.is_empty() {
        out.push_str(&nothing_measured("a set of identical repeats"));
        return Ok(out);
    }
    out.push_str(
        "Every point is one benchmark's spread across a set of **identical repeats** - the same \
         code, the same machine, the same everything, captured several times. Whatever a point \
         sits above zero is what that benchmark moves by when nothing has changed, which is the \
         floor under any claim made about it.\n\n\
         The stepped line is the tier `shoal-bench compare` screens against. Points above it are \
         benchmarks whose ordinary variation the band does not cover, and there are some: the \
         tiers are screens, not bounds. Criterion's own confidence interval is deliberately not \
         used - across these repeats `partition_sorted/get_key/4096` moved 22% while reporting its \
         outlying value with a ±0.2% interval, tighter than any of the runs it disagreed with.\n\n",
    );
    let groups: Vec<chart::noise_band::Group> = page
        .repeats
        .iter()
        .map(|(name, points)| chart::noise_band::Group {
            name: name.clone(),
            points: points.clone(),
        })
        .collect();
    out.push_str(&chart::noise_band::draw(&groups)?);
    out.push('\n');
    out.push_str(&caption(
        "Observed spread against benchmark duration. Instability scales inversely with duration, \
         which is why one global threshold would be either too loose for the slow benchmarks or \
         too tight for the fast ones.",
    ));
    out.push_str(&tables::noise_buckets(&page.repeats));
    out.push('\n');
    Ok(out)
}
