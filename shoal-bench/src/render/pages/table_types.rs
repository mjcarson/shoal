//! What kind of table costs what, and what storage costs
//!
//! Two things on one page because they are one question asked twice. The grid's four tables answer
//! it under a mixture; the isolating workloads answer it one path at a time, which is what makes a
//! difference here attributable.

use anyhow::Result;

use crate::registry::Layer;
use crate::fmt;
use crate::model::macro_layer::MacroCaptureV2;
use crate::render::arms::{self, Arm};
use crate::render::chart::bars;
use crate::render::chart::sweep::Unit;
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// The isolating workloads this page reads, and what each is
///
/// Declared rather than discovered, because the pairing is the point: each row is a workload and
/// the control it is read against, and a list built by prefix matching would not know which is
/// which.
const CONTROLS: [(&str, &str, &str); 3] = [
    (
        "macro/insert_unsorted",
        "macro/insert_ephemeral",
        "inserting rows, with storage and without",
    ),
    (
        "macro/get_resident",
        "macro/get_ephemeral",
        "reading a row already in memory, with storage under it and without",
    ),
    (
        "macro/get_archived",
        "macro/get_resident",
        "reading a row from disk, against reading the same row from memory",
    ),
];

/// Builds the table types page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::TableTypes,
        page,
        "Four tables under one mixture, and beside them the workloads that drive one path at a time \
         so that a difference between two of them can be attributed to something.",
    );
    let Some(capture) = page.current_for(Layer::Macro).and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("any table"));
        out.push_str(&footer(Surface::TableTypes));
        return Ok(out);
    };
    out.push_str(&under_a_mixture(capture)?);
    out.push_str(&isolating(capture));
    out.push_str(&footer(Surface::TableTypes));
    Ok(out)
}

/// The four tables compared under the reference mixture
///
/// # Arguments
///
/// * `capture` - The capture to draw from
fn under_a_mixture(capture: &MacroCaptureV2) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The four tables, under one mixture\n\n");
    // the reference cell of the grid: one width, one mixture, every table
    let cells: Vec<Arm<'_>> = arms::grid(capture)
        .into_iter()
        .filter(|arm| {
            arm.read_pct() == Some(50) && arm.row_bytes() == 1024 && arm.row_profile().is_none()
        })
        .collect();
    if cells.is_empty() {
        out.push_str(&nothing_measured("the four tables under one mixture"));
        return Ok(out);
    }
    let kinds = arms::table_kinds(&cells);
    let groups: Vec<String> = kinds.iter().map(|kind| arms::table_label(kind)).collect();
    let series = vec![bars::Series {
        name: "queries a second".to_string(),
        values: kinds
            .iter()
            .map(|kind| {
                cells
                    .iter()
                    .find(|arm| arm.table_kind() == Some(kind.as_str()))
                    .and_then(Arm::ops_per_sec)
            })
            .collect(),
    }];
    out.push_str(&bars::draw(
        &bars::Spec {
            id: "chart-tables-throughput".to_string(),
            x_desc: "table".to_string(),
            y_desc: "queries answered per second".to_string(),
            unit: Unit::Rate,
        },
        &groups,
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "The same even mixture of 1 KiB rows against each of the four tables. An ephemeral table is \
         the persistent one with a storage engine that stores nothing, so the two are the same code \
         paths up to the point where one of them writes.",
    ));
    out.push_str(&storage_note(&cells, &kinds));
    out.push_str(&tables::grid_cells(&cells));
    out.push('\n');
    Ok(out)
}

/// What storage cost, in words
///
/// # Arguments
///
/// * `cells` - The reference cells
/// * `kinds` - The table kinds they cover
fn storage_note(cells: &[Arm<'_>], kinds: &[String]) -> String {
    let rate = |kind: &str| {
        cells
            .iter()
            .find(|arm| arm.table_kind() == Some(kind))
            .and_then(Arm::ops_per_sec)
    };
    let mut out = String::new();
    // each persistent table against its own no-storage twin, which is the pair that differs in the
    // storage engine and in nothing else
    for (persistent, ephemeral) in [
        ("persistent_unsorted", "ephemeral_unsorted"),
        ("persistent_sorted", "ephemeral_sorted"),
    ] {
        if !kinds.iter().any(|kind| kind == persistent) {
            continue;
        }
        let (Some(with), Some(without)) = (rate(persistent), rate(ephemeral)) else {
            continue;
        };
        out.push_str(&format!(
            "The {} table answered {} queries a second with storage under it and {} without - \
             storage is **{}×** the cost of the whole mixture on that table. ",
            arms::table_label(persistent),
            fmt::thousands(with.round() as u128),
            fmt::thousands(without.round() as u128),
            fmt::fixed(without / with.max(f64::MIN_POSITIVE), 1)
        ));
    }
    if !out.is_empty() {
        out.push_str(
            "That ratio is a property of this hardware as much as of Shoal: the storage here is an \
             Intel Optane SSD, whose fsync latency is roughly an order of magnitude below a \
             consumer NVMe, so on an ordinary drive the gap is wider rather than narrower.\n\n",
        );
    }
    out
}

/// The isolating workloads and their controls
///
/// # Arguments
///
/// * `capture` - The capture to draw from
fn isolating(capture: &MacroCaptureV2) -> String {
    let mut out = String::new();
    out.push_str("## One path at a time\n\n");
    out.push_str(
        "Each row below is a workload and the control it is read against. The two halves of a pair \
         build byte-identical rows from the same seed and differ in exactly one thing, which is \
         what makes the gap between them attributable to that thing rather than to whatever else \
         moved between two captures.\n\n\
         **These are the numbers a regression is attributed with.** A mixture on the pages beside \
         this one says that something got slower; a pair here says which half.\n\n",
    );
    let rendered = tables::control_pairs(capture, &CONTROLS);
    if rendered.is_empty() {
        out.push_str(&nothing_measured("any of the isolating workloads"));
        return out;
    }
    out.push_str(&rendered);
    out.push('\n');
    out
}
