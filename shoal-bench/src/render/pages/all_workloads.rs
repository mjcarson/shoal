//! Every workload's raw numbers, with no interpretation attached
//!
//! The page that exists so the pages beside it can leave things out. Every other page selects the
//! arms that answer its question and says how to read them; this one lists everything every capture
//! measured, in one table, sorted, with no charts.
//!
//! # Why there are no charts here
//!
//! There used to be one per workload - eighty-eight of them, in alphabetical order, ninety percent
//! of the old page by weight. A chart of one workload's wall clock across four captures says
//! something only if you already know what that workload is, which is what the family pages are
//! for. Here the value is being able to find a number, and a table is better at that than a chart.

use anyhow::Result;

use crate::model::macro_layer::MacroCaptureV2;
use crate::render::family::{self, Surface};
use crate::render::page::Page;
use crate::render::pages::{footer, header, nothing_measured};
use crate::render::tables;

/// Builds the every-workload page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::AllWorkloads,
        page,
        "Every workload every capture measured, with no selection and no interpretation. Use the \
         pages beside this one to find out what a number means; use this one to find the number.",
    );
    // every capture that produced a macro artifact, oldest first
    let captures: Vec<(String, MacroCaptureV2)> = page
        .timeline
        .iter()
        .filter_map(|snapshot| {
            snapshot
                .macro_layer
                .clone()
                .map(|capture| (snapshot.label.clone(), capture))
        })
        .collect();
    if captures.is_empty() {
        out.push_str(&nothing_measured("any workload"));
        out.push_str(&footer(Surface::AllWorkloads));
        return Ok(out);
    }
    out.push_str(
        "Each capture is quoted as the median of its runs, with the range of those runs beside it. \
         **Two captures differ only when their ranges do not overlap.** This layer's own spread is \
         wider than most changes worth making - the frozen baseline spread 10.5% across five \
         identical runs - so a difference in medians that sits inside the ranges is not a result, \
         however large it looks.\n\n\
         **A `per_batch` number and a `per_query` number are never comparable.** A saturated \
         workload takes one timestamp per batch, so its percentiles are batch completion times; a \
         bounded concurrency workload stamps each query, so its percentiles are service times. The \
         table names which each workload is.\n\n",
    );
    out.push_str(&which_page(&captures));
    out.push_str("## Every workload, every capture\n\n");
    out.push_str(&tables::macro_summary(&captures));
    out.push('\n');
    out.push_str(&footer(Surface::AllWorkloads));
    Ok(out)
}

/// Which page explains each group of workloads in the table below
///
/// # Arguments
///
/// * `captures` - Every capture that produced a macro artifact
fn which_page(captures: &[(String, MacroCaptureV2)]) -> String {
    let mut out = String::new();
    out.push_str("## Where each of these is explained\n\n");
    out.push_str("| Family | How many | Explained on |\n| --- | --- | --- |\n");
    // every workload any capture measured, grouped by the family that explains it
    let mut ids: Vec<String> = captures
        .iter()
        .flat_map(|(_, capture)| capture.workload_ids())
        .map(String::from)
        .collect();
    ids.sort();
    ids.dedup();
    // walked in family declaration order rather than in discovery order, so the table is stable
    for family in family::FAMILIES {
        let count = ids
            .iter()
            .filter(|id| family::family_for(id).is_some_and(|found| found.name == family.name))
            .count();
        if count == 0 {
            continue;
        }
        out.push_str(&format!(
            "| {} | {count} | [{}]({}) |\n",
            family.title,
            family.surface.title(),
            family.surface.link()
        ));
    }
    // and anything no family claims, which is a workload nothing on this site explains
    let orphans: Vec<&String> = ids
        .iter()
        .filter(|id| family::family_for(id).is_none())
        .collect();
    if !orphans.is_empty() {
        out.push_str(&format!(
            "| *no family* | {} | nothing - see `shoal-bench/src/render/family.rs` |\n",
            orphans.len()
        ));
    }
    out.push('\n');
    out
}
