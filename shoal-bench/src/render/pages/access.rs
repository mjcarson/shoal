//! What a skewed key space and a deeper queue do
//!
//! Two sweeps that answer questions about *the shape of the load* rather than about the data: which
//! keys the reads ask for, and how many queries are outstanding while they ask.

use anyhow::Result;

use crate::fmt;
use crate::render::arms::{self, Arm};
use crate::render::chart::bars;
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// The distributions the skew sweep covers, in the order they are read
///
/// Uniform first, because it is the control the other two are read against.
const DISTRIBUTIONS: [&str; 3] = ["uniform", "zipfian", "latest"];

/// Builds the access patterns page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Access,
        page,
        "Two sweeps over the shape of the load rather than the shape of the data: which keys the \
         reads ask for, and how many queries are outstanding while they ask.",
    );
    let Some(capture) = page.current().and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a key distribution or a load depth"));
        out.push_str(&footer(Surface::Access));
        return Ok(out);
    };
    out.push_str(&skew(&arms::skew(capture))?);
    out.push_str(&ladder(&arms::depth_ladder(capture))?);
    out.push_str(&footer(Surface::Access));
    Ok(out)
}

/// What a skewed key space did
///
/// # Arguments
///
/// * `points` - The arms of the skew sweep
fn skew(points: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What skew buys\n\n");
    if points.is_empty() {
        out.push_str(&nothing_measured("a key distribution"));
        return Ok(out);
    }
    let kinds = arms::table_kinds(points);
    // the distributions present, in reading order rather than in the order they were captured
    let groups: Vec<String> = DISTRIBUTIONS
        .iter()
        .filter(|name| points.iter().any(|arm| arm.distribution() == **name))
        .map(|name| (*name).to_string())
        .collect();
    if groups.is_empty() {
        out.push_str(&nothing_measured("a key distribution"));
        return Ok(out);
    }
    let series: Vec<bars::Series> = kinds
        .iter()
        .map(|kind| bars::Series {
            name: arms::table_label(kind),
            values: groups
                .iter()
                .map(|dist| {
                    points
                        .iter()
                        .find(|arm| {
                            arm.table_kind() == Some(kind.as_str()) && arm.distribution() == dist
                        })
                        .and_then(|arm| arm.stat("read", "p50"))
                })
                .collect(),
        })
        .collect();
    out.push_str(&bars::draw(
        &bars::Spec {
            id: "chart-skew-latency".to_string(),
            x_desc: "which keys the reads asked for".to_string(),
            y_desc: "p50 read service time".to_string(),
            unit: Unit::Duration,
        },
        &groups,
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median read under each key distribution, on the two persistent tables. `uniform` asks for \
         every key equally often; `zipfian` is YCSB's scrambled Zipfian, under which a small set of \
         keys takes most of the traffic; `latest` skews toward the most recently written keys.",
    ));
    out.push_str(&skew_note(points, &kinds));
    out.push_str(&tables::skew(points, &groups));
    out.push('\n');
    Ok(out)
}

/// What the skew sweep found, in words
///
/// # Arguments
///
/// * `points` - The arms of the skew sweep
/// * `kinds` - The table kinds they cover
fn skew_note(points: &[Arm<'_>], kinds: &[String]) -> String {
    let Some(kind) = kinds.first() else {
        return String::new();
    };
    let at = |dist: &str| {
        points
            .iter()
            .find(|arm| arm.table_kind() == Some(kind.as_str()) && arm.distribution() == dist)
            .and_then(|arm| arm.stat("read", "p50"))
    };
    let (Some(uniform), Some(zipfian)) = (at("uniform"), at("zipfian")) else {
        return String::new();
    };
    // stated as a share of the uniform cost, since that is the control
    let share = (uniform - zipfian) / uniform.max(f64::MIN_POSITIVE) * 100.0;
    let direction = if share >= 0.0 { "cheaper" } else { "dearer" };
    format!(
        "On the {} table a skewed read was {} {direction} than a uniform one - {} against {}. The \
         whole of that gap is caching: the working set here fits in memory either way, so what \
         skew is buying is locality inside a table that was already resident. **A larger table \
         than memory would show a far larger gap**, and nothing on this site measures that case.\n\n",
        arms::table_label(kind),
        fmt::share_pct(share.abs() / 100.0),
        fmt::duration_ns(zipfian),
        fmt::duration_ns(uniform)
    )
}

/// What the load depth ladder found
///
/// # Arguments
///
/// * `rungs` - The arms of the depth ladder
fn ladder(rungs: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The load depth ladder\n\n");
    if rungs.is_empty() {
        out.push_str(&nothing_measured("a load depth ladder"));
        return Ok(out);
    }
    // throughput and latency against depth, on one chart each, because they are read together and
    // the whole point is the shape of the two at once
    let throughput: Vec<(f64, f64)> = rungs
        .iter()
        .filter_map(|arm| Some((f64::from(arm.depth()), arm.ops_per_sec()?)))
        .collect();
    if throughput.len() > 1 {
        out.push_str(&sweep::draw(
            &sweep::Spec {
                id: "chart-depth-throughput".to_string(),
                x_desc: "queries outstanding at once".to_string(),
                y_desc: "queries answered per second".to_string(),
                x_axis: sweep::Axis::Log,
                y_axis: sweep::Axis::Linear,
                x_unit: Unit::Count,
                y_unit: Unit::Rate,
            },
            &[sweep::Series {
                name: "throughput".to_string(),
                points: throughput.clone(),
            }],
        )?);
        out.push('\n');
        out.push_str(&caption(
            "Throughput against load depth. While this rises with depth the server has capacity \
             left; where it flattens is the knee, and past the knee an extra outstanding query buys \
             queueing rather than work.",
        ));
    }
    let mut latency = Vec::new();
    for op in ["read", "write"] {
        let points: Vec<(f64, f64)> = rungs
            .iter()
            .filter_map(|arm| Some((f64::from(arm.depth()), arm.stat(op, "p50")?)))
            .collect();
        if points.len() > 1 {
            latency.push(sweep::Series {
                name: op.to_string(),
                points,
            });
        }
    }
    if !latency.is_empty() {
        out.push_str(&sweep::draw(
            &sweep::Spec {
                id: "chart-depth-latency".to_string(),
                x_desc: "queries outstanding at once".to_string(),
                y_desc: "p50 service time".to_string(),
                x_axis: sweep::Axis::Log,
                y_axis: sweep::Axis::Log,
                x_unit: Unit::Count,
                y_unit: Unit::Duration,
            },
            &latency,
        )?);
        out.push('\n');
        out.push_str(&caption(
            "Service time against load depth, over the same runs. Read this beside the chart above: \
             where latency climbs and throughput does not, the extra time is queue and not service.",
        ));
    }
    out.push_str(&knee_note(rungs));
    out.push_str(&tables::depth_ladder(rungs));
    out.push('\n');
    Ok(out)
}

/// Where the knee is, in words, and what it means for the rest of the site
///
/// # Arguments
///
/// * `rungs` - The arms of the depth ladder
fn knee_note(rungs: &[Arm<'_>]) -> String {
    let mut points: Vec<(u32, f64)> = rungs
        .iter()
        .filter_map(|arm| Some((arm.depth(), arm.ops_per_sec()?)))
        .collect();
    points.sort_by_key(|(depth, _)| *depth);
    if points.len() < 2 {
        return String::new();
    }
    // the first rung where more load bought less work, which is [O31]'s signature for a workload
    // that has left the regime a service time means anything in
    let saturated = points
        .windows(2)
        .find(|pair| pair[1].1 < pair[0].1)
        .map(|pair| pair[1].0);
    let (low_depth, low_rate) = points[0];
    let (high_depth, high_rate) = points[points.len() - 1];
    let mut out = format!(
        "From a depth of {low_depth} to a depth of {high_depth} - {}× the outstanding queries - \
         throughput went from {} to {} a second, which is {}× the work.",
        high_depth / low_depth.max(1),
        fmt::thousands(low_rate.round() as u128),
        fmt::thousands(high_rate.round() as u128),
        fmt::fixed(high_rate / low_rate.max(f64::MIN_POSITIVE), 1)
    );
    match saturated {
        Some(depth) => out.push_str(&format!(
            " **Throughput fell at a depth of {depth}**, which is the signature of a queue past its \
             knee: at and above that depth the p50 is a measure of how long the queue is rather \
             than of how long a query takes. Every arm of the grid runs at a depth of thirty two, \
             so a reader has to check which side of {depth} that is before quoting a grid latency \
             as a service time. Nothing detects this automatically - see \
             [O31](../appendix/optimizations.md)."
        )),
        None => out.push_str(
            " Throughput did not fall at any rung, so nothing in this ladder is past the knee and \
             the grid's own depth sits inside the regime where a p50 is a service time.",
        ),
    }
    out.push_str("\n\n");
    out
}
