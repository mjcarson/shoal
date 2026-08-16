//! What a read/write mixture costs
//!
//! The page a caller sizing Shoal for their own workload reads first. Everything on it is the same
//! row width and the same key space; the only thing that varies is how many of the queries were
//! reads and which table they went to.

use anyhow::Result;

use crate::fmt;
use crate::render::arms::{self, Arm};
use crate::render::chart::bars;
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// The row width the mixture sweep is measured at
///
/// Held to one width so that the read share is the only thing moving. Which width it is lives in
/// `crate::workloads::grid`, and is repeated here rather than imported because this half of the
/// crate compiles without the engine and therefore without that module.
const REFERENCE_WIDTH: u64 = 1024;

/// Builds the read/write mixture page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Grid,
        page,
        "One client, a fixed share of reads to writes, 1 KiB rows, thirty two queries outstanding \
         at once. Every table Shoal has is measured under the same mixture, so the four can be read \
         against each other.",
    );
    let Some(capture) = page.current().and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a read/write mixture"));
        out.push_str(&footer(Surface::Grid));
        return Ok(out);
    };
    // only the cells at the reference width, which is what makes the read share the axis
    let cells: Vec<Arm<'_>> = arms::grid(capture)
        .into_iter()
        .filter(|arm| arm.row_bytes() == REFERENCE_WIDTH && arm.row_profile().is_none())
        .collect();
    if cells.is_empty() {
        out.push_str(&nothing_measured("a read/write mixture"));
        out.push_str(&footer(Surface::Grid));
        return Ok(out);
    }
    let kinds = arms::table_kinds(&cells);
    out.push_str(&throughput(&cells, &kinds)?);
    out.push_str(&latency(&cells, &kinds)?);
    out.push_str(&halves(&cells, &kinds)?);
    out.push_str("## Every cell\n\n");
    out.push_str(&tables::grid_cells(&cells));
    out.push('\n');
    out.push_str(&footer(Surface::Grid));
    Ok(out)
}

/// How throughput moves with the read share
///
/// # Arguments
///
/// * `cells` - The arms at the reference width
/// * `kinds` - The table kinds they cover, in reading order
fn throughput(cells: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Throughput against the read share\n\n");
    // one line per table, each point one mixture
    let series: Vec<sweep::Series> = kinds
        .iter()
        .map(|kind| sweep::Series {
            name: arms::table_label(kind),
            points: cells
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((f64::from(arm.read_pct()?), arm.ops_per_sec()?)))
                .collect(),
        })
        .filter(|line| !line.points.is_empty())
        .collect();
    if series.is_empty() {
        out.push_str("No arm at this width reported a query rate.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-grid-throughput".to_string(),
            x_desc: "reads, as a percentage of all queries".to_string(),
            y_desc: "queries answered per second".to_string(),
            // the share runs from zero, which a logarithmic axis cannot place at all
            x_axis: sweep::Axis::Linear,
            y_axis: sweep::Axis::Linear,
            x_unit: Unit::Count,
            y_unit: Unit::Rate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Queries a second against the share of them that were reads, one line per table. This is \
         throughput at a load depth of thirty two, not the most the server can do - the depth \
         ladder on the access patterns page is what says how far from saturated that is.",
    ));
    // the slope is the finding, so it is stated rather than left to be measured off the chart
    out.push_str(&slope_note(cells, kinds));
    Ok(out)
}

/// What the shape of the throughput curve says, in words
///
/// # Arguments
///
/// * `cells` - The arms at the reference width
/// * `kinds` - The table kinds they cover
fn slope_note(cells: &[Arm<'_>], kinds: &[String]) -> String {
    // the two ends of the axis on the table a reader cares about most
    let Some(kind) = kinds.first() else {
        return String::new();
    };
    let at = |share: u32| {
        cells
            .iter()
            .find(|arm| arm.table_kind() == Some(kind.as_str()) && arm.read_pct() == Some(share))
            .and_then(Arm::ops_per_sec)
    };
    let (Some(writes), Some(reads)) = (at(0), at(100)) else {
        return String::new();
    };
    // a ratio rather than a percentage, since the two ends can differ by an order of magnitude and
    // "900% faster" is harder to hold than "ten times"
    let ratio = reads / writes.max(f64::MIN_POSITIVE);
    format!(
        "On the {} table, an all-read mixture answered {} queries a second against {} for an \
         all-write one - **{}× the throughput**. Everything between the two ends is a blend of \
         those two costs, so a mixture's throughput is bounded by its write share more than by \
         anything else on this page.\n\n",
        arms::table_label(kind),
        fmt::thousands(reads.round() as u128),
        fmt::thousands(writes.round() as u128),
        fmt::fixed(ratio, 1)
    )
}

/// How read and write latency move with the read share
///
/// # Arguments
///
/// * `cells` - The arms at the reference width
/// * `kinds` - The table kinds they cover, in reading order
fn latency(cells: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Latency against the read share\n\n");
    out.push_str(
        "Reads and writes are drawn apart, because a percentile over the two together describes \
         neither: a write costs several times what a read does, so the mixture's p99 is the write \
         p99 wearing a different name whenever writes are more than a percent of the traffic.\n\n",
    );
    // one line per table per operation, so a reader can see whether a busier mixture makes the
    // *other* operation slower - which is the question a mixture exists to answer
    let mut series = Vec::new();
    for op in ["read", "write"] {
        for kind in kinds {
            let points: Vec<(f64, f64)> = cells
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((f64::from(arm.read_pct()?), arm.stat(op, "p50")?)))
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
        out.push_str("No arm at this width recorded a latency.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-grid-latency".to_string(),
            x_desc: "reads, as a percentage of all queries".to_string(),
            y_desc: "p50 service time".to_string(),
            x_axis: sweep::Axis::Linear,
            // logarithmic, because a read and a write differ by enough that a linear axis would
            // draw every read line flat against the floor
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Count,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median service time against the read share, one line per operation per table. The value \
         axis is logarithmic; a flat line means that operation's cost did not depend on what the \
         rest of the traffic was doing.",
    ));
    Ok(out)
}

/// What each half of an even mixture costs, per table
///
/// # Arguments
///
/// * `cells` - The arms at the reference width
/// * `kinds` - The table kinds they cover, in reading order
fn halves(cells: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What each half costs, at an even mixture\n\n");
    // the even mixture is the reference cell, and it is the one point where both halves of every
    // table were measured under identical load
    let even: Vec<&Arm<'_>> = cells
        .iter()
        .filter(|arm| arm.read_pct() == Some(50))
        .collect();
    if even.is_empty() {
        out.push_str("No arm ran the even mixture.\n\n");
        return Ok(out);
    }
    let groups: Vec<String> = kinds.iter().map(|kind| arms::table_label(kind)).collect();
    let series: Vec<bars::Series> = ["read", "write"]
        .iter()
        .map(|op| bars::Series {
            name: (*op).to_string(),
            values: kinds
                .iter()
                .map(|kind| {
                    even.iter()
                        .find(|arm| arm.table_kind() == Some(kind.as_str()))
                        .and_then(|arm| arm.stat(op, "p50"))
                })
                .collect(),
        })
        .collect();
    out.push_str(&bars::draw(
        &bars::Spec {
            id: "chart-grid-halves".to_string(),
            x_desc: "table".to_string(),
            y_desc: "p50 service time".to_string(),
            unit: Unit::Duration,
        },
        &groups,
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median read and median write of the even mixture, on each table. The gap between a table \
         and its no-storage twin is what durability cost; the gap between the two bars within a \
         table is what a write costs over a read.",
    ));
    Ok(out)
}
