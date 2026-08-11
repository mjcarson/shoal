//! Where one query's latency went, at each latency rank
//!
//! One stacked bar per rank, so the question the chart answers is "what is different about the
//! slow queries" rather than "what does an average query cost". Those are different questions and
//! the second one cannot be used to answer the first: a stage that is 0.6% of the median and 32%
//! of the p99 is invisible in an average and is the whole tail.
//!
//! # Two things are deliberately folded away
//!
//! A stage flagged `at_floor` is within twice the cost of reading the clock, so it is mostly
//! instrument rather than measurement. Drawing it as a segment would claim the time went there.
//! Those are folded into `other`.
//!
//! Whatever the stages do not account for is drawn as its own segment rather than dropped or
//! spread across the rest. A breakdown whose parts do not sum to the whole is saying something,
//! and hiding it makes every chart built from one look complete when it is not.

use std::collections::BTreeMap;

use anyhow::Result;
use plotters::prelude::*;

use super::palette;
use crate::fmt;
use crate::model::stages::StageReport;

/// How many stages are drawn before the rest are folded into `other`
const MAX_STAGES: usize = 7;

/// How tall each rank's bar is drawn
const ROW_HEIGHT: u32 = 40;

/// How many legend entries fit across the chart
///
/// Five, because a stage name runs to seventeen characters and eight of them across 820 units
/// leaves each one about ninety - not enough, and the overflow is invisible until somebody looks
/// at the page.
const LEGEND_COLUMNS: usize = 5;

/// How far apart legend rows sit, in pixels
const LEGEND_ROW_PX: f64 = 17.0;

/// Draws one operation's stage breakdown at every rank
///
/// # Arguments
///
/// * `report` - The stage report to draw
/// * `op` - Which operation to draw, `insert` or `get`
pub fn draw(report: &StageReport, op: &str) -> Result<String> {
    let Some(operation) = report.ops.get(op) else {
        anyhow::bail!("the stage report has no breakdown for {op}");
    };
    // a report with no buckets has nothing to say about any rank
    if operation.buckets.is_empty() {
        anyhow::bail!("the stage report has no buckets for {op}");
    }
    // which stages are worth their own colour, decided once across every rank so that a stage
    // keeps the same colour as the eye moves down the chart
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    for bucket in &operation.buckets {
        for (name, cost) in &bucket.stages {
            // a stage at the floor is not a measurement, and never earns a colour
            if cost.at_floor {
                continue;
            }
            *totals.entry(name.as_str()).or_default() += cost.mean_ns;
        }
    }
    let mut ranked: Vec<(&str, u64)> = totals.into_iter().collect();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let named: Vec<String> = ranked
        .iter()
        .take(MAX_STAGES)
        .map(|(name, _)| (*name).to_string())
        .collect();
    // each rank's bar, as a list of (stage, nanoseconds) in a fixed order
    let mut bars: Vec<(String, u64, Vec<(String, u64)>)> = Vec::new();
    for bucket in &operation.buckets {
        let mut segments: Vec<(String, u64)> = Vec::new();
        let mut other = 0u64;
        for (name, cost) in &bucket.stages {
            // everything at the floor, and everything past the colour budget, goes to `other`
            if cost.at_floor || !named.contains(name) {
                other += cost.mean_ns;
                continue;
            }
            segments.push((name.clone(), cost.mean_ns));
        }
        // drawn in the fixed stage order rather than the order the map happened to hold
        segments.sort_by_key(|(name, _)| named.iter().position(|known| known == name).unwrap_or(usize::MAX));
        if other > 0 {
            segments.push(("other".to_string(), other));
        }
        // and whatever nothing accounted for, which is a finding rather than a rounding error
        if bucket.unaccounted_ns > 0 {
            segments.push(("unaccounted".to_string(), bucket.unaccounted_ns as u64));
        }
        bars.push((bucket.rank.clone(), bucket.total_ns, segments));
    }
    let widest = bars
        .iter()
        .map(|(_, total, segments)| {
            // the bar is as wide as what is drawn, which can exceed the recorded total when a
            // stage overlaps another
            let drawn: u64 = segments.iter().map(|(_, ns)| *ns).sum();
            drawn.max(*total)
        })
        .max()
        .unwrap_or(1) as f64;
    let count = bars.len();
    // how many rows the legend needs, and how much axis room that is. The legend is drawn inside
    // the chart's coordinate space rather than below it, so the space has to exist: a coordinate
    // outside the range is clamped to the edge, which silently stacks every legend row on the
    // same pixel.
    // the legend names what was actually drawn, rather than the stage list, so a segment can
    // never appear on the chart without a name against it
    let mut legend: Vec<String> = named.clone();
    for (_, _, segments) in &bars {
        for (name, _) in segments {
            if !legend.contains(name) {
                legend.push(name.clone());
            }
        }
    }
    let legend_rows = legend.len().div_ceil(LEGEND_COLUMNS).max(1);
    let row_gap = LEGEND_ROW_PX / f64::from(ROW_HEIGHT);
    let legend_span = 0.35 + row_gap * legend_rows as f64;
    let height = ROW_HEIGHT * count as u32 + 96 + (legend_span * f64::from(ROW_HEIGHT)) as u32;
    let aria = format!(
        "Stage breakdown of {op} queries at {count} latency ranks, the slowest totalling {}",
        fmt::duration_ns(widest)
    );
    let id = format!("chart-stages-{op}");
    super::draw(&id, &aria, height, move |root| {
        let mut chart = ChartBuilder::on(root)
            .margin(14)
            .margin_right(30)
            .x_label_area_size(44)
            .y_label_area_size(120)
            .build_cartesian_2d(
                0f64..widest * 1.02,
                (-0.5f64 - legend_span)..(count as f64 - 0.5),
            )?;
        // the y axis is one latency rank per row, in the order the report lists them
        let ranks: Vec<String> = bars
            .iter()
            .map(|(rank, total, _)| format!("{rank}  {}", fmt::duration_ns(*total as f64)))
            .collect();
        crate::themed_mesh!(chart)
            .disable_y_mesh()
            .y_labels(count)
            .y_label_formatter(&move |value: &f64| {
                // only label the ticks that land on a rank
                let index = value.round();
                if (value - index).abs() > 0.01 || index < 0.0 {
                    return String::new();
                }
                // drawn top down, so the row order is reversed against the axis
                ranks
                    .get(count - 1 - index as usize)
                    .cloned()
                    .unwrap_or_default()
            })
            .x_desc("time in the stage")
            .x_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        for (index, (_, _, segments)) in bars.iter().enumerate() {
            // the first rank is drawn at the top, so the row index counts down
            let y = (count - 1 - index) as f64;
            let mut at = 0f64;
            for (name, nanos) in segments {
                let colour = colour_for(name, &named);
                let end = at + *nanos as f64;
                chart.draw_series(std::iter::once(Rectangle::new(
                    [(at, y - 0.34), (end, y + 0.34)],
                    colour.filled(),
                )))?;
                at = end;
            }
        }
        // a legend along the bottom, since a stacked bar cannot label its own segments.
        // laid out in the chart's own coordinates, where one unit is one bar row. The row spacing
        // has to be derived from that rather than picked by eye: a stage name is drawn at 10px, so
        // a spacing that looks generous in data units can be seven pixels on the page.
        for (index, name) in legend.iter().enumerate() {
            let colour = colour_for(name, &named);
            let column = index % LEGEND_COLUMNS;
            let x = widest * (0.005 + 0.2 * column as f64);
            let y = -0.62 - row_gap * (index / LEGEND_COLUMNS) as f64;
            chart.draw_series(std::iter::once(Rectangle::new(
                [(x, y - 0.06), (x + widest * 0.012, y + 0.06)],
                colour.filled(),
            )))?;
            chart.draw_series(std::iter::once(Text::new(
                name.clone(),
                (x + widest * 0.02, y),
                super::label_font(10),
            )))?;
        }
        Ok(())
    })
}

/// The colour a stage is drawn in
///
/// # Arguments
///
/// * `name` - The stage's name
/// * `named` - The stages that earned their own colour, in order
fn colour_for(name: &str, named: &[String]) -> plotters::style::RGBColor {
    // anything folded away is drawn in the muted colour rather than borrowing a stage's
    match named.iter().position(|known| known == name) {
        Some(index) => palette::series(index),
        None => palette::MUTED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::stages::{Bucket, JoinStats, OpReport, STAGE_REPORT_VERSION, StageCost};

    /// Builds a stage cost
    ///
    /// # Arguments
    ///
    /// * `mean_ns` - What the stage cost
    /// * `at_floor` - Whether it is within the clock's own resolution
    fn cost(mean_ns: u64, at_floor: bool) -> StageCost {
        StageCost {
            mean_ns,
            share: 0.0,
            per_batch: false,
            at_floor,
            samples: 100,
        }
    }

    /// Builds a report with one operation and the given buckets
    ///
    /// # Arguments
    ///
    /// * `buckets` - Each rank, its total, and its stages
    fn report(buckets: &[(&str, u64, i64, &[(&str, u64, bool)])]) -> StageReport {
        let buckets = buckets
            .iter()
            .map(|(rank, total, unaccounted, stages)| Bucket {
                rank: (*rank).to_string(),
                total_ns: *total,
                samples: 100,
                stages: stages
                    .iter()
                    .map(|(name, ns, floor)| ((*name).to_string(), cost(*ns, *floor)))
                    .collect(),
                unaccounted_ns: *unaccounted,
            })
            .collect();
        let mut ops = BTreeMap::new();
        ops.insert(
            "insert".to_string(),
            OpReport {
                count: 100,
                rotated: 0,
                buckets,
            },
        );
        StageReport {
            version: STAGE_REPORT_VERSION,
            label: Some("L".to_string()),
            clock: "CLOCK_MONOTONIC".to_string(),
            clock_overhead_ns: 20,
            join: JoinStats::default(),
            ops,
        }
    }

    /// One bar per rank, each labelled with the rank and its total
    #[test]
    fn it_draws_a_bar_per_rank() {
        let report = report(&[
            ("all", 100, 0, &[("durable_write", 60, false), ("execute", 40, false)]),
            ("p99", 400, 0, &[("durable_write", 300, false), ("execute", 100, false)]),
        ]);
        let svg = draw(&report, "insert").expect("it draws");
        assert!(svg.contains("all"));
        assert!(svg.contains("p99"));
        assert!(svg.contains("durable_write"));
        assert!(!svg.contains("NaN"));
    }

    /// A stage at the floor is folded into `other` rather than drawn as a finding
    #[test]
    fn a_stage_at_the_floor_is_folded_away() {
        let report = report(&[(
            "all",
            100,
            0,
            &[("durable_write", 90, false), ("route", 10, true)],
        )]);
        let svg = draw(&report, "insert").expect("it draws");
        assert!(svg.contains("durable_write"));
        // the floored stage never earns a colour or a legend entry
        assert!(!svg.contains(">route<"), "a floored stage was drawn as its own segment");
        assert!(svg.contains("other"));
    }

    /// What no stage accounted for is drawn rather than hidden
    #[test]
    fn the_unaccounted_remainder_is_drawn() {
        let report = report(&[("all", 100, 25, &[("durable_write", 75, false)])]);
        let svg = draw(&report, "insert").expect("it draws");
        assert!(svg.contains("unaccounted"), "the remainder was hidden");
    }

    /// More stages than the colour budget are folded into `other`
    #[test]
    fn a_long_tail_of_stages_is_folded() {
        let owned: Vec<(String, u64, bool)> = (0..12)
            .map(|index| (format!("stage{index}"), 100 - index * 5, false))
            .collect();
        let stages: Vec<(&str, u64, bool)> = owned
            .iter()
            .map(|(name, ns, floor)| (name.as_str(), *ns, *floor))
            .collect();
        let report = report(&[("all", 700, 0, &stages)]);
        let svg = draw(&report, "insert").expect("it draws");
        // the most expensive keep their identity
        assert!(svg.contains("stage0"));
        // and the cheapest are folded away rather than crowding the legend
        assert!(!svg.contains("stage11"), "the tail was not folded");
        assert!(svg.contains("other"));
    }

    /// An operation the report does not cover is an error rather than an empty chart
    #[test]
    fn a_missing_operation_is_an_error() {
        let report = report(&[("all", 100, 0, &[("durable_write", 100, false)])]);
        assert!(draw(&report, "get").is_err());
    }

    /// The drawing is the same every time
    #[test]
    fn it_is_deterministic() {
        let report = report(&[("all", 100, 0, &[("a", 60, false), ("b", 40, false)])]);
        assert_eq!(
            draw(&report, "insert").expect("it draws"),
            draw(&report, "insert").expect("it draws")
        );
    }
}
