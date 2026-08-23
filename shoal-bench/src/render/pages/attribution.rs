//! Where the time goes, from the two instrumented builds
//!
//! **Neither build on this page ever produces a latency or a throughput.** Both stamp extra
//! timestamps on the query path, so their own wall clocks are not comparable to the uninstrumented
//! build's. They attribute time; they do not measure it. That is why they are on a page of their
//! own rather than beside numbers that do.

use anyhow::Result;

use crate::fmt;
use crate::render::badges;
use crate::render::chart;
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;
use crate::registry::Layer;

/// Builds the attribution page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Attribution,
        page,
        "Two separate, instrumented builds. `hotpath` says which scopes the time is inside; the \
         stage report says where one query's latency went between the client's `send` and the \
         response coming back. **Nothing here is a latency or a throughput**, and no number \
         anywhere else on this site comes from these builds.",
    );
    out.push_str(&hotpath(page)?);
    out.push_str(&stages(page)?);
    out.push_str(&footer(Surface::Attribution));
    Ok(out)
}

/// Which scopes the time is inside
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn hotpath(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Which scopes the time is inside\n\n");
    // the most recent capture that produced a profile, which may not be the current one
    let Some(snapshot) = page
        .timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.hotpath.is_some())
    else {
        out.push_str(&nothing_measured("a hotpath profile"));
        return Ok(out);
    };
    let profile = snapshot
        .hotpath
        .as_ref()
        .expect("the snapshot was selected for having one");
    out.push_str(&format!(
        "From `{}`, built with the `hotpath` feature.\n\n",
        snapshot.label
    ));
    out.push_str(
        "The bars are total nanoseconds **summed across all twelve shards**, so they add up to \
         more than the run's wall clock for anything that ran on many shards at once. `hotpath` \
         also reports a percentage per scope, and this page never uses it: it is not normalised \
         across concurrent scopes, and reports one scope of this very profile at over 12,000%. \
         See [known issue 53](../appendix/known-issues.md).\n\n",
    );
    if let Some(status) = page.status(&snapshot.label) {
        let shown = badges::for_layer(status, Layer::Hotpath);
        if !shown.is_empty() {
            out.push_str(&format!("{shown}\n\n"));
        }
    }
    out.push_str(&chart::hotpath_scopes::draw(profile)?);
    out.push('\n');
    out.push_str(&caption(
        "The twelve most expensive instrumented scopes, ranked by total time inside them.",
    ));
    out.push_str(&tables::hotpath_scopes(profile, 12));
    out.push('\n');
    Ok(out)
}

/// Where one query's latency went
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn stages(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Where one query's latency goes\n\n");
    // the most recent capture that produced a stage report
    let Some(snapshot) = page
        .timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.stages.is_some())
    else {
        out.push_str(&nothing_measured("a stage report"));
        return Ok(out);
    };
    let reports = snapshot
        .stages
        .as_ref()
        .expect("the snapshot was selected for having one");
    // the write path workload, which is the one this page has always drawn. the layer profiles the
    // width axis as well now, and that breakdown belongs beside the widths it is about rather than
    // here - see `pages::row_size`
    let Some(report) = reports.primary() else {
        out.push_str(&nothing_measured("a stage report"));
        return Ok(out);
    };
    out.push_str(&format!(
        "From `{}`, built with the `stage-profile` feature. Every query records when it reached \
         each of nineteen points between the client's `send` and the response coming back. See \
         [F6](../features/stage-breakdown.md).\n\n",
        snapshot.label
    ));
    out.push_str(
        "Each bar is one **latency rank**, not one stage: the queries are ranked by their total \
         latency, a window is taken around each rank, and each stage is averaged over that window. \
         Per-stage percentiles are not used, because they do not add up to the total percentile - \
         the p99 of every stage is not a description of any query that happened.\n\n\
         A stage whose cost is within twice the cost of reading the clock is folded into `other`, \
         because below that the measurement is mostly the instrument. Whatever the stages do not \
         account for is drawn as its own segment rather than spread across the rest.\n\n",
    );
    if let Some(status) = page.status(&snapshot.label) {
        let shown = badges::for_layer(status, Layer::Stages);
        if !shown.is_empty() {
            out.push_str(&format!("{shown}\n\n"));
        }
    }
    // the drawn report's own join, not the artifact's sum. the sum is across every workload the
    // layer profiled and this page draws one of them, so a capture whose other reports joined
    // nothing would have this page reporting their absence as its own evidence
    // ([Resolved #76](../../../docs/src/appendix/resolved/stage-join.md))
    out.push_str(&format!(
        "The client and server halves of `{}` joined on {} queries.\n\n",
        report
            .workload
            .as_deref()
            .unwrap_or(crate::model::stages::LEGACY_STAGE_WORKLOAD),
        fmt::thousands(report.join.joined as u128)
    ));
    // one chart and one table per operation the report covers
    for op in ["insert", "get"] {
        let Some(table) = tables::stage_ranks(report, op, 3) else {
            continue;
        };
        out.push_str(&format!("### {op}\n\n"));
        out.push_str(&chart::stages_stacked::draw(report, op)?);
        out.push('\n');
        out.push_str(&caption(&format!(
            "Stage breakdown of {op} queries at each latency rank, slowest rank at the bottom."
        )));
        out.push_str(&table);
        out.push('\n');
    }
    Ok(out)
}
