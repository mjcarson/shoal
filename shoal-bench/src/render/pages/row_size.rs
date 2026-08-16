//! What row width costs
//!
//! The other half of the cross. Everything here is the same even mixture against the same tables;
//! the only thing that varies is how wide a row is.

use anyhow::Result;

use crate::fmt;
use crate::render::arms::{self, Arm};
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// The read share the width sweep is measured at
const REFERENCE_MIX: u32 = 50;

/// Builds the row width page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::RowSize,
        page,
        "An even mixture of reads and writes, swept from a 64 byte row to a 4 MiB one, on every \
         table. The three named mixtures at the end of the axis are declared distributions of \
         widths rather than ranges, so two captures of one measure the same thing.",
    );
    let Some(capture) = page.current().and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a row width sweep"));
        out.push_str(&footer(Surface::RowSize));
        return Ok(out);
    };
    let all: Vec<Arm<'_>> = arms::grid(capture)
        .into_iter()
        .filter(|arm| arm.read_pct() == Some(REFERENCE_MIX))
        .collect();
    // the fixed widths carry the curve; a mixture has no place on a numeric axis, since its width
    // is a mean and plotting a mean beside a measurement invites the two to be read alike
    let fixed: Vec<Arm<'_>> = all
        .iter()
        .copied()
        .filter(|arm| arm.row_profile().is_none())
        .collect();
    let mixed: Vec<Arm<'_>> = all
        .iter()
        .copied()
        .filter(|arm| arm.row_profile().is_some())
        .collect();
    if fixed.is_empty() && mixed.is_empty() {
        out.push_str(&nothing_measured("a row width sweep"));
        out.push_str(&footer(Surface::RowSize));
        return Ok(out);
    }
    let kinds = arms::table_kinds(&all);
    out.push_str(&latency(&fixed, &kinds)?);
    out.push_str(&rows_per_sec(&fixed, &kinds)?);
    out.push_str(&bytes_per_sec(&fixed, &kinds)?);
    out.push_str(&mixtures(&mixed, &fixed));
    out.push_str("## Every width\n\n");
    out.push_str(&tables::grid_cells(&all));
    out.push('\n');
    out.push_str(&footer(Surface::RowSize));
    Ok(out)
}

/// How the cost of one query moves with the row width
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn latency(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What one query costs, against the row width\n\n");
    if fixed.is_empty() {
        out.push_str("No arm was measured at a fixed width.\n\n");
        return Ok(out);
    }
    let mut series = Vec::new();
    for op in ["read", "write"] {
        for kind in kinds {
            let points: Vec<(f64, f64)> = fixed
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((arm.row_bytes() as f64, arm.stat(op, "p50")?)))
                .collect();
            if !points.is_empty() {
                series.push(sweep::Series {
                    name: format!("{op}, {}", arms::table_label(kind)),
                    points,
                });
            }
        }
    }
    if series.is_empty() {
        out.push_str("No arm at a fixed width recorded a latency.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-latency".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "p50 service time".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median service time against row width, both axes logarithmic. A flat left hand end is a \
         fixed per-response cost that the row is too narrow to matter against; a straight diagonal \
         right hand end is a per-byte cost that has taken over. Where the two meet is the width \
         above which how much data a row holds is the whole story.",
    ));
    out.push_str(&knee_note(fixed, kinds));
    Ok(out)
}

/// Where the fixed cost gives way to the per-byte one, in words
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover
fn knee_note(fixed: &[Arm<'_>], kinds: &[String]) -> String {
    let Some(kind) = kinds.first() else {
        return String::new();
    };
    // every width this table was read at, in order
    let mut points: Vec<(u64, f64)> = fixed
        .iter()
        .filter(|arm| arm.table_kind() == Some(kind.as_str()))
        .filter_map(|arm| Some((arm.row_bytes(), arm.stat("read", "p50")?)))
        .collect();
    points.sort_by_key(|(width, _)| *width);
    if points.len() < 3 {
        return String::new();
    }
    // the first width at which doubling the row more than doubles the cost is where the per-byte
    // term has taken over. stated as the width itself rather than as a fitted slope, since three
    // to eight points is not enough data to fit anything to
    let mut knee = None;
    for pair in points.windows(2) {
        let (low_width, low_cost) = pair[0];
        let (high_width, high_cost) = pair[1];
        let width_ratio = high_width as f64 / low_width.max(1) as f64;
        let cost_ratio = high_cost / low_cost.max(f64::MIN_POSITIVE);
        // "cost keeps up with width" is the signature of a per-byte cost
        if cost_ratio >= width_ratio * 0.6 {
            knee = Some(low_width);
            break;
        }
    }
    let (narrowest, narrow_cost) = points[0];
    let (widest, wide_cost) = points[points.len() - 1];
    let mut out = format!(
        "On the {} table a read of a {} row took {} and a read of a {} one took {}: **{}× the \
         bytes for {}× the time.**",
        arms::table_label(kind),
        fmt::bytes(narrowest),
        fmt::duration_ns(narrow_cost),
        fmt::bytes(widest),
        fmt::duration_ns(wide_cost),
        fmt::thousands((widest / narrowest.max(1)) as u128),
        fmt::fixed(wide_cost / narrow_cost.max(f64::MIN_POSITIVE), 1)
    );
    if let Some(width) = knee {
        out.push_str(&format!(
            " The cost starts tracking the width somewhere above {}; below that a row is too \
             narrow for its own size to be what the query is paying for.",
            fmt::bytes(width)
        ));
    }
    out.push_str("\n\n");
    out
}

/// How many queries a second each width sustained
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn rows_per_sec(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Queries a second, against the row width\n\n");
    let series = rate_series(fixed, kinds, |arm| arm.ops_per_sec());
    if series.is_empty() {
        out.push_str("No arm at a fixed width reported a query rate.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-ops".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "queries answered per second".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Rate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Queries a second against row width. This is the pessimistic reading of the axis: a wider \
         row buys fewer queries, always.",
    ));
    Ok(out)
}

/// How many payload bytes a second each width sustained
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn bytes_per_sec(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Payload bytes a second, against the row width\n\n");
    let series = rate_series(fixed, kinds, |arm| arm.bytes_per_sec());
    if series.is_empty() {
        out.push_str("No arm at a fixed width reported a byte rate.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-bytes".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "payload bytes per second".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::ByteRate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "The same runs counted in bytes rather than queries. This is the optimistic reading, and \
         it usually rises where the chart above falls - a store that moves few large rows quickly \
         is not slow. Payloads only: framing, keys and the archive's own overhead are not counted, \
         so this is a floor on what crossed the wire and never a ceiling.",
    ));
    Ok(out)
}

/// One rate series per table
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover
/// * `rate` - Which rate to read off an arm
fn rate_series(
    fixed: &[Arm<'_>],
    kinds: &[String],
    rate: impl Fn(&Arm<'_>) -> Option<f64>,
) -> Vec<sweep::Series> {
    kinds
        .iter()
        .map(|kind| sweep::Series {
            name: arms::table_label(kind),
            points: fixed
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((arm.row_bytes() as f64, rate(arm)?)))
                .collect(),
        })
        .filter(|line| !line.points.is_empty())
        .collect()
}

/// What the mixed-width arms did, against the fixed widths they average out to
///
/// # Arguments
///
/// * `mixed` - The arms drawing from a width distribution
/// * `fixed` - The arms at a fixed width, to compare them against
fn mixtures(mixed: &[Arm<'_>], fixed: &[Arm<'_>]) -> String {
    let mut out = String::new();
    out.push_str("## The mixed widths\n\n");
    if mixed.is_empty() {
        out.push_str("No arm drew its rows from a width distribution.\n\n");
        return out;
    }
    out.push_str(
        "Three arms draw each row's width from a declared set rather than using one width \
         throughout. They are not on the charts above, and deliberately: a mixture's width is the \
         **mean** of its distribution, and a mean plotted on the same axis as a measurement invites \
         the two to be read the same way.\n\n\
         What they are for is the comparison in the table below. A mixture whose cost matches the \
         fixed-width arm at its own mean is a system whose cost is linear across that range. A \
         mixture that costs more than its mean says the widths in it are not interchangeable - that \
         the wide rows in the distribution cost more than the narrow ones save.\n\n",
    );
    out.push_str(&tables::width_mixtures(mixed, fixed));
    out.push('\n');
    out
}
