//! What reading many partitions in one query costs
//!
//! One get naming *n* partition keys, swept over six values of *n*. The curve is the whole point:
//! whether the cost of naming *n* keys is proportional to *n* or worse than proportional is what
//! [O13](../../../docs/src/appendix/optimizations.md) claims from reading the source and has never
//! had a number.

use anyhow::Result;

use crate::render::arms::{self, Arm};
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// Builds the fan-out page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Fanout,
        page,
        "One get naming one, two, four, sixteen, sixty four and two hundred and fifty six partition \
         keys, on a resident table, an evicted one and one with no storage under it at all.",
    );
    let Some(capture) = page.current().and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a fan-out curve"));
        out.push_str(&footer(Surface::Fanout));
        return Ok(out);
    };
    let curve = arms::with_prefix(capture, "macro/fanout/");
    if curve.is_empty() {
        out.push_str(&nothing_measured("a fan-out curve"));
        out.push_str(&footer(Surface::Fanout));
        return Ok(out);
    }
    out.push_str(&cost(&curve)?);
    out.push_str(&per_partition(&curve)?);
    out.push_str("## Every point\n\n");
    out.push_str(&tables::fanout(&curve));
    out.push('\n');
    out.push_str(&footer(Surface::Fanout));
    Ok(out)
}

/// The arms of the curve, in reading order
///
/// # Arguments
///
/// * `curve` - Every fan-out workload in the capture
fn residencies(curve: &[Arm<'_>]) -> Vec<String> {
    // the arm is the second segment of the identifier, which is the one thing about this family
    // that is not in `ScaleFacts` - the three arms differ in where their rows are, which the
    // artifact records as a server configuration rather than as a scale fact
    let mut found: Vec<String> = curve
        .iter()
        .filter_map(|arm| arm.id.strip_prefix("macro/fanout/"))
        .filter_map(|tail| tail.split('/').next())
        .map(String::from)
        .collect();
    found.sort_unstable();
    found.dedup();
    found
}

/// How many keys one point of the curve named
///
/// # Arguments
///
/// * `arm` - The workload to read
fn keys(arm: &Arm<'_>) -> Option<f64> {
    // the last segment of the identifier, which is the swept value
    arm.id.rsplit_once('/')?.1.parse::<f64>().ok()
}

/// What one get cost, against how many keys it named
///
/// # Arguments
///
/// * `curve` - Every fan-out workload in the capture
fn cost(curve: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What one get costs, against how many keys it names\n\n");
    let series: Vec<sweep::Series> = residencies(curve)
        .into_iter()
        .map(|arm_name| sweep::Series {
            points: curve
                .iter()
                .filter(|arm| arm.id.starts_with(&format!("macro/fanout/{arm_name}/")))
                .filter_map(|arm| Some((keys(arm)?, arm.stat("get", "p50")?)))
                .collect(),
            name: arm_name,
        })
        .filter(|line| !line.points.is_empty())
        .collect();
    if series.is_empty() {
        out.push_str("No fan-out arm recorded a latency.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-fanout-cost".to_string(),
            x_desc: "partition keys named by one get".to_string(),
            y_desc: "p50 service time".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Count,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median service time against the number of partition keys one get names, both axes \
         logarithmic. A straight line at forty five degrees is a cost proportional to the key \
         count, which is what a router that splits a query once should produce. Anything steeper is \
         a per-partition term being paid more than once.",
    ));
    Ok(out)
}

/// The same curve divided through by the key count
///
/// # Arguments
///
/// * `curve` - Every fan-out workload in the capture
fn per_partition(curve: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The same curve, per partition\n\n");
    out.push_str(
        "The chart above divided through by the key count. This is the one that makes a superlinear \
         term obvious: a cost proportional to *n* is a **flat line** here, so any rise at all is \
         the per-partition cost growing with the number of partitions in the query.\n\n",
    );
    let series: Vec<sweep::Series> = residencies(curve)
        .into_iter()
        .map(|arm_name| sweep::Series {
            points: curve
                .iter()
                .filter(|arm| arm.id.starts_with(&format!("macro/fanout/{arm_name}/")))
                .filter_map(|arm| {
                    let count = keys(arm)?;
                    Some((count, arm.stat("get", "p50")? / count.max(1.0)))
                })
                .collect(),
            name: arm_name,
        })
        .filter(|line| !line.points.is_empty())
        .collect();
    if series.is_empty() {
        out.push_str("No fan-out arm recorded a latency.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-fanout-per-partition".to_string(),
            x_desc: "partition keys named by one get".to_string(),
            y_desc: "p50 per partition read".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Count,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median service time divided by the number of keys named. A falling line is the fixed cost \
         of a round trip being amortised over more partitions; a rising one is the thing \
         [O13](../appendix/optimizations.md) predicts.",
    ));
    Ok(out)
}
