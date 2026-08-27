//! The charts the book already draws, as a starting selection
//!
//! # Why these exist
//!
//! A chart is a metric, a key axis, and a set of arms, and the last of those is sixty-four
//! checkboxes for the width sweep. Nobody is going to tick them. So the two charts a reader
//! actually opens the explorer to redraw are declared here, and each one is a button.
//!
//! # How the arms are chosen
//!
//! **By the facts each measurement recorded, and by the family that explains it - never by parsing
//! an identifier.** That is the rule `shoal_bench::render::arms` documents and exists to enforce,
//! and it is what lets a preset keep working when a workload is renamed. Each preset selects within
//! **one** capture, because the arms a capture measured are a property of that capture.
//!
//! # These are not the pages
//!
//! A preset reproduces a chart's *selection*; the prose under it, the slope notes and the tables of
//! every cell all still live on the generated page, and the panel under the chart points at it.

use crate::index::{Axis, Chart, Index, Metric, SweepAxis};

/// The row width the mixture sweep is measured at
///
/// Held to one width so the read share is the only thing moving, which is the same reason
/// `shoal_bench::render::pages::grid` holds it. Repeated here rather than imported because the
/// crate that declares it cannot be linked from a browser.
const REFERENCE_WIDTH: u64 = 1024;

/// The read share the width sweep is measured at
///
/// The same constant, and the same reason, as `render::pages::row_size`'s own `REFERENCE_MIX`.
const REFERENCE_MIX: u32 = 50;

/// The family whose arms both presets draw
///
/// Matched on the family's slug rather than on the workload identifier, so this keeps working when
/// an arm is renamed and stops working - visibly, by selecting nothing - if the family is.
const GRID_FAMILY: &str = "grid";

/// One of the book's charts, as a state the explorer can be put into
pub struct Preset {
    /// What the button is called, which is the heading the chart has on its page
    pub name: &'static str,
    /// The page it is drawn on, so the reader can go and read the prose under it
    pub page: &'static str,
    /// What goes on the value axis
    pub metric: Metric,
    /// What goes on the key axis
    pub axis: Axis,
    /// Whether it is drawn as lines or as bars
    pub chart: Chart,
    /// Whether the key axis is logarithmic
    pub log_x: bool,
    /// Whether the value axis is logarithmic
    pub log_y: bool,
    /// Which arms it draws, within one capture
    pub select: fn(&Index, u32) -> Vec<u32>,
}

impl Preset {
    /// Every preset, in the order the toolbar offers them
    ///
    /// The first is what the explorer opens on.
    pub fn all() -> Vec<Preset> {
        vec![
            // `chart-grid-throughput`, from `render::pages::grid::throughput`. the share runs from
            // zero, which a logarithmic axis cannot place at all, so both axes stay linear
            Preset {
                name: "Throughput against the read share",
                page: "grid.md",
                metric: Metric::OpsPerSec,
                axis: Axis::Sweep(SweepAxis::ReadShare),
                chart: Chart::Line,
                log_x: false,
                log_y: false,
                select: read_share_arms,
            },
            // `chart-row-size-ops`, from `render::pages::row_size::rows_per_sec`. sixteen widths
            // from 64 B to 4 MiB, which is six decades and unreadable on a linear axis
            Preset {
                name: "Queries a second, against the row width",
                page: "row-size.md",
                metric: Metric::OpsPerSec,
                axis: Axis::Sweep(SweepAxis::RowBytes),
                chart: Chart::Line,
                log_x: true,
                log_y: true,
                select: row_width_arms,
            },
        ]
    }
}

/// The arms `render::pages::grid::throughput` draws, within one capture
///
/// One width, a fixed row profile, and a workload that is a mixture at all. That last condition is
/// what makes the read share an axis rather than a single column.
///
/// # Arguments
///
/// * `index` - The corpus to select from
/// * `capture` - Which capture to select within
pub fn read_share_arms(index: &Index, capture: u32) -> Vec<u32> {
    arms(index, capture, |scale| {
        scale.row_bytes == REFERENCE_WIDTH
            && scale.row_profile.is_none()
            && scale.read_pct.is_some()
    })
}

/// The arms `render::pages::row_size::rows_per_sec` draws, within one capture
///
/// One mixture at a fixed row profile, at every width the capture measured. The mixed profiles are
/// excluded here for the reason that page excludes them: their width is a mean, and a mean has no
/// place on an axis of measured ones.
///
/// # Arguments
///
/// * `index` - The corpus to select from
/// * `capture` - Which capture to select within
pub fn row_width_arms(index: &Index, capture: u32) -> Vec<u32> {
    arms(index, capture, |scale| {
        scale.read_pct == Some(REFERENCE_MIX) && scale.row_profile.is_none()
    })
}

/// Every arm of the grid family in one capture whose facts satisfy a condition
///
/// # Arguments
///
/// * `index` - The corpus to select from
/// * `capture` - Which capture to select within
/// * `wanted` - What the arm's recorded facts have to satisfy
fn arms(
    index: &Index,
    capture: u32,
    wanted: impl Fn(&crate::index::ScaleFactsLite) -> bool,
) -> Vec<u32> {
    // the family that explains the grid, as an index, or nothing when this corpus has no such
    // family - in which case a preset selects nothing and says so by drawing nothing
    let family = index
        .families
        .iter()
        .position(|found| found.name == GRID_FAMILY)
        .map(|at| at as u32);
    let mut arms = Vec::new();
    for point in &index.macro_points {
        // only this capture's measurements, and only the ones the grid explains
        if point.capture != capture {
            continue;
        }
        if index
            .workloads
            .get(point.workload as usize)
            .is_none_or(|workload| workload.family != family)
        {
            continue;
        }
        let Some(scale) = index.scales.get(point.scale as usize) else {
            continue;
        };
        if !wanted(scale) {
            continue;
        }
        arms.push(point.workload);
    }
    arms.sort_unstable();
    arms.dedup();
    arms
}
