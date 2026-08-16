//! What encryption costs, across row width, load depth and client count
//!
//! Every arm of the `macro/encryption/*` sweeps has a twin differing in the wire and in nothing
//! else, so the gap between a pair is what encryption cost and not what else moved. These charts
//! draw that gap four ways.
//!
//! # The pairing is done here, not by `compare`
//!
//! [`crate::compare`] joins two *captures*. This joins two *workloads inside one capture*, on
//! `(row width, depth, clients)` across [`ConfFacts::tls`](crate::model::macro_layer::ConfFacts).
//! Both halves therefore ran on the same machine, minutes apart, against the same seed and the
//! same configuration — which is a far tighter control than any cross-capture comparison can be.
//!
//! The numeric axes come out of the artifact's own
//! [`ScaleFacts`](crate::model::macro_layer::ScaleFacts) rather than out of the identifier. Only
//! the *sweep* an arm belongs to is read from its id, because that is the one thing the facts do
//! not record: a depth arm at depth 1 and a client arm at one client have identical facts and
//! would otherwise collide.
//!
//! # The gap is drawn in nanoseconds, not as a share
//!
//! These charts used to draw `(tls - plain) / plain` as a percentage. A percentage cannot tell a
//! large share of a very small number from a small share of a large one, and both of those are on
//! this sweep: a 256 byte row at depth 1 and a 4 MiB row at depth 128 are not the same finding
//! however similar their percentages look. What each chart draws now is what encryption added, in
//! the same unit everything else on the page is in, and [`draw_absolute`] says what it added it
//! to. The share is still what the prose quotes, and [`Point::overhead_pct`] still computes it.
//!
//! # A point that is not a result is drawn hollow
//!
//! The macro layer's rule is that a difference is only a result when the two sides' observed
//! intervals are **disjoint** — when the slowest run of one is still faster than the fastest run of
//! the other. A pair whose intervals overlap has not been shown to differ at all, so its marker is
//! drawn hollow and the caption counts them. An overhead curve drawn through overlapping intervals
//! is a curve through noise, and it would look exactly like a real one.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::Result;
use plotters::prelude::*;

use super::sweep::{Axis, Scale, Unit};
use super::{legend, palette};
use crate::fmt;
use crate::model::macro_layer::MacroCaptureV2;

/// How tall the plotting area is, before the legend is added under it
const PLOT_HEIGHT: u32 = 400;

/// How many curves the palette can carry before two of them share a colour
const MAX_CURVES: usize = 8;

/// The operation every arm of both sweeps records its samples under
const OP: &str = "get";

/// The percentile the curves are drawn in
///
/// p50 rather than a tail, for the reason the sweep's own module header gives: the byte budget
/// that keeps the widest arms from dominating a capture leaves them with the fewest samples, so
/// their p99 is the worst few of a few hundred rather than a percentile.
const METRIC: &str = "p50";

/// The identifier prefix of the sweep that varies load depth on one client
pub const DEPTH_SWEEP: &str = "macro/encryption/depth/";

/// The identifier prefix of the sweep that varies how many independent clients there are
pub const CLIENT_SWEEP: &str = "macro/encryption/clients/";

/// One plaintext arm and its encrypted twin
#[derive(Debug, Clone, Copy)]
pub struct Point {
    /// How wide one row's payload was, in bytes
    pub row_bytes: u64,
    /// How many queries were outstanding at once, across every client
    pub depth: u32,
    /// How many independent clients produced that load
    pub clients: u32,
    /// What the plaintext arm cost, in nanoseconds
    pub plain_ns: f64,
    /// What the encrypted arm cost, in nanoseconds
    pub tls_ns: f64,
    /// Whether the two arms' observed intervals were disjoint
    ///
    /// `false` means the pair has not been shown to differ, however far apart their medians are.
    pub separated: bool,
}

impl Point {
    /// What encryption cost here, as a percentage of the plaintext cost
    ///
    /// Still what the prose on the page quotes. It is no longer what any chart draws - see the
    /// module header for why.
    pub fn overhead_pct(&self) -> f64 {
        // a plaintext arm that cost nothing would make this meaningless, and cannot happen
        if self.plain_ns <= 0.0 {
            return 0.0;
        }
        (self.tls_ns - self.plain_ns) / self.plain_ns * 100.0
    }

    /// What encryption cost here, in nanoseconds
    ///
    /// Negative where the encrypted arm came out faster, which happens and is exactly what the
    /// hollow markers are for.
    pub fn added_ns(&self) -> f64 {
        self.tls_ns - self.plain_ns
    }
}

/// Which arm of a pair a workload is
///
/// Keyed on everything that has to match for two arms to be twins.
type Key = (u64, u32, u32);

/// Pairs the arms of one sweep by everything except the wire
///
/// # Arguments
///
/// * `capture` - The capture to read
/// * `sweep` - The identifier prefix of the sweep to pair
pub fn pairs(capture: &MacroCaptureV2, sweep: &str) -> Vec<Point> {
    // gather each wire's arms, keyed on what has to match for two of them to be twins
    let mut plain: BTreeMap<Key, (f64, Option<(u128, u128)>)> = BTreeMap::new();
    let mut tls: BTreeMap<Key, (f64, Option<(u128, u128)>)> = BTreeMap::new();
    for (id, workload) in &capture.workloads {
        if !id.starts_with(sweep) {
            continue;
        }
        // the axes come from the facts rather than the id, so a renamed workload still pairs
        let Some(conf) = workload.conf.as_ref() else {
            continue;
        };
        let Some(cost) = workload.stat_ns(OP, METRIC) else {
            continue;
        };
        let key = (
            workload.scale.row_bytes,
            workload.scale.concurrency,
            workload.scale.clients.unwrap_or(1),
        );
        let entry = (cost as f64, workload.stat_interval_ns(OP, METRIC));
        if conf.tls {
            tls.insert(key, entry);
        } else {
            plain.insert(key, entry);
        }
    }
    // an arm without a twin is dropped rather than drawn against nothing
    let mut points: Vec<Point> = Vec::new();
    for (key, (plain_ns, plain_band)) in plain {
        let Some((tls_ns, tls_band)) = tls.get(&key) else {
            continue;
        };
        points.push(Point {
            row_bytes: key.0,
            depth: key.1,
            clients: key.2,
            plain_ns,
            tls_ns: *tls_ns,
            separated: disjoint(plain_band, *tls_band),
        });
    }
    points
}

/// Whether two observed intervals do not overlap
///
/// This is the macro layer's own rule for when a difference is a result, applied within a capture
/// rather than across two. A pair that ran once has no interval and cannot be separated.
///
/// # Arguments
///
/// * `left` - One arm's observed interval
/// * `right` - The other arm's
fn disjoint(left: Option<(u128, u128)>, right: Option<(u128, u128)>) -> bool {
    // without an interval on both sides there is no error bar to judge with
    let (Some((left_low, left_high)), Some((right_low, right_high))) = (left, right) else {
        return false;
    };
    left_high < right_low || right_high < left_low
}

/// Groups points into one curve per value of some field
///
/// # Arguments
///
/// * `points` - The pairs to group
/// * `series_of` - Which curve a point belongs to
/// * `x_of` - Where along the curve it sits
/// * `y_of` - What the point measures
fn curves(
    points: &[Point],
    series_of: impl Fn(&Point) -> u64,
    x_of: impl Fn(&Point) -> f64,
    y_of: impl Fn(&Point) -> f64,
) -> Vec<(u64, Vec<(f64, f64, bool)>)> {
    // one entry per series, each sorted along the x axis so the line is drawn left to right
    let mut grouped: BTreeMap<u64, Vec<(f64, f64, bool)>> = BTreeMap::new();
    for point in points {
        grouped
            .entry(series_of(point))
            .or_default()
            .push((x_of(point), y_of(point), point.separated));
    }
    let mut out: Vec<(u64, Vec<(f64, f64, bool)>)> = grouped.into_iter().collect();
    for (_, series) in &mut out {
        series.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    out
}

/// Draws one overhead chart
///
/// Shared by all three of the overhead views, which differ only in what is on the x axis and what
/// separates the curves.
///
/// # Arguments
///
/// * `id` - The element id to give the chart
/// * `points` - The pairs to draw
/// * `x_desc` - What the x axis is
/// * `x_of` - Where along the x axis a point sits
/// * `x_unit` - How x values are written
/// * `series_of` - Which curve a point belongs to
/// * `series_label` - How to name a curve
fn draw_overhead(
    id: &str,
    points: &[Point],
    x_desc: &str,
    x_of: impl Fn(&Point) -> f64,
    x_unit: Unit,
    series_of: impl Fn(&Point) -> u64,
    series_label: impl Fn(u64) -> String,
) -> Result<String> {
    // nothing to pair is not a chart
    if points.is_empty() {
        anyhow::bail!("no encryption arm had a twin to be drawn against");
    }
    let series = curves(points, series_of, x_of, Point::added_ns);
    // the axes span everything drawn. x is logarithmic because every axis here is powers of two;
    // y is not, because an added cost can be negative and a log axis cannot hold that
    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut min_y = f64::MAX;
    let mut max_y = f64::MIN;
    for (_, curve) in &series {
        for (x, y, _) in curve {
            min_x = min_x.min(*x);
            max_x = max_x.max(*x);
            min_y = min_y.min(*y);
            max_y = max_y.max(*y);
        }
    }
    // always show the zero line, so a curve that hugs it is visibly hugging it
    min_y = min_y.min(0.0);
    max_y = max_y.max(0.0);
    // pad so the extremes are not drawn on the frame
    let span = (max_y - min_y).max(1.0);
    let (low, high) = (min_y - span * 0.08, max_y + span * 0.08);
    let overlapping = points.iter().filter(|point| !point.separated).count();
    let aria = format!(
        "What encryption added against {x_desc}, {} curves, from {} to {}",
        series.len(),
        fmt::duration_ns(min_y),
        fmt::duration_ns(max_y)
    );
    // one legend entry per curve, in the order the colours were handed out
    let entries: Vec<legend::Entry> = series
        .iter()
        .enumerate()
        .map(|(index, (key, _))| legend::Entry::new(series_label(*key), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    // the ticks the x axis gets, which are the widths, depths or client counts the sweep was
    // actually run at rather than the powers of ten plotters would space a log axis with
    let ticks = distinct_x(&series);
    let x_desc = x_desc.to_string();
    let x_unit = x_unit;
    super::draw(id, &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (area, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&area)
            .margin(16)
            // no gutter is reserved on the right any more, because nothing is drawn out there
            .margin_right(24)
            .x_label_area_size(46)
            .y_label_area_size(78)
            .build_cartesian_2d(
                Scale::new(min_x * 0.85..max_x * 1.2, Axis::Log).ticks(ticks),
                Scale::new(low..high, Axis::Linear),
            )?;
        crate::themed_mesh!(chart)
            .x_desc(&x_desc)
            .x_label_formatter(&move |value: &f64| x_unit.format(*value))
            .y_desc("what TLS added")
            .y_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        // the zero line, so "no cost" is a place on the chart rather than a value to read off
        chart.draw_series(LineSeries::new(
            [(min_x * 0.85, 0.0), (max_x * 1.2, 0.0)],
            palette::AXIS.stroke_width(1),
        ))?;
        for (index, (_, curve)) in series.iter().enumerate() {
            let colour = palette::series(index);
            chart.draw_series(LineSeries::new(
                curve.iter().map(|(x, y, _)| (*x, *y)),
                colour.stroke_width(2),
            ))?;
            // a filled marker is a result, a hollow one is a pair whose intervals overlapped
            chart.draw_series(curve.iter().filter(|(_, _, ok)| *ok).map(|(x, y, _)| {
                Circle::new((*x, *y), 3, colour.filled())
            }))?;
            chart.draw_series(curve.iter().filter(|(_, _, ok)| !*ok).map(|(x, y, _)| {
                Circle::new((*x, *y), 3, colour.stroke_width(1))
            }))?;
        }
        // a note when some pairs were not separated, so the chart cannot look more certain than
        // the data is
        if overlapping > 0 {
            chart.draw_series(std::iter::once(Text::new(
                format!("{overlapping} hollow: runs overlapped, not a result"),
                (min_x * 0.85, low + span * 0.04),
                super::label_font(10),
            )))?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

/// The x values every curve shares, if there are few enough of them to be ticks
///
/// The same rule [`sweep`](super::sweep) applies, over the shape these charts hold their curves in.
///
/// # Arguments
///
/// * `series` - The curves being drawn
fn distinct_x(series: &[(u64, Vec<(f64, f64, bool)>)]) -> Option<Vec<f64>> {
    // gather every x anything was measured at
    let mut values: Vec<f64> = series
        .iter()
        .flat_map(|(_, curve)| curve.iter().map(|(x, _, _)| *x))
        .collect();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    // two curves measured at the same width contribute one tick, not two
    values.dedup_by(|left, right| (*left - *right).abs() <= right.abs() * 1e-9);
    // above a dozen these have stopped being chosen points and started being a scatter
    if values.is_empty() || values.len() > 12 {
        return None;
    }
    Some(values)
}

/// Draws what encryption costs against row width, one curve per load depth
///
/// # Arguments
///
/// * `capture` - The capture to read
pub fn draw_by_row(capture: &MacroCaptureV2) -> Result<String> {
    let points = pairs(capture, DEPTH_SWEEP);
    draw_overhead(
        "chart-encryption-by-row",
        &points,
        "row width",
        |point| point.row_bytes as f64,
        Unit::Bytes,
        |point| u64::from(point.depth),
        |depth| format!("{depth} deep"),
    )
}

/// Draws what encryption costs against load depth, one curve per row width
///
/// # Arguments
///
/// * `capture` - The capture to read
pub fn draw_by_depth(capture: &MacroCaptureV2) -> Result<String> {
    let points = pairs(capture, DEPTH_SWEEP);
    draw_overhead(
        "chart-encryption-by-depth",
        &points,
        "queries outstanding at once",
        |point| f64::from(point.depth),
        Unit::Count,
        |point| point.row_bytes,
        fmt::bytes,
    )
}

/// Draws what encryption costs against the number of independent clients
///
/// # Arguments
///
/// * `capture` - The capture to read
pub fn draw_by_clients(capture: &MacroCaptureV2) -> Result<String> {
    let points = pairs(capture, CLIENT_SWEEP);
    draw_overhead(
        "chart-encryption-by-clients",
        &points,
        "independent clients",
        |point| f64::from(point.clients),
        Unit::Count,
        |point| point.row_bytes,
        fmt::bytes,
    )
}

/// Draws what a query actually cost on each wire, against row width
///
/// The overhead chart says how much encryption added; this says what it added *to*, at every depth
/// the sweep covers rather than only the shallowest. A gap of the same number of microseconds is a
/// different finding on a query that takes forty of them and on one that takes four thousand, and
/// the depth axis is where that difference lives.
///
/// # Arguments
///
/// * `capture` - The capture to read
pub fn draw_absolute(capture: &MacroCaptureV2) -> Result<String> {
    let points = pairs(capture, DEPTH_SWEEP);
    if points.is_empty() {
        anyhow::bail!("no encryption pair was captured");
    }
    // one curve per depth per wire, in depth order so the legend reads as a ladder
    let mut depths: Vec<u32> = points.iter().map(|point| point.depth).collect();
    depths.sort_unstable();
    depths.dedup();
    // two wires to a depth, against a palette eight colours wide. beyond this the ninth curve
    // would reuse a colour and two lines would be indistinguishable, so say so rather than draw it
    if depths.len() * 2 > MAX_CURVES {
        anyhow::bail!(
            "the depth sweep has {} depths, which is {} curves against a palette of {MAX_CURVES}",
            depths.len(),
            depths.len() * 2
        );
    }
    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut min_y = f64::MAX;
    let mut max_y = f64::MIN;
    for point in &points {
        min_x = min_x.min(point.row_bytes as f64);
        max_x = max_x.max(point.row_bytes as f64);
        for cost in [point.plain_ns, point.tls_ns] {
            // a zero cost cannot be placed on a log axis, and is not a measurement
            if cost > 0.0 {
                min_y = min_y.min(cost);
                max_y = max_y.max(cost);
            }
        }
    }
    if min_y > max_y {
        anyhow::bail!("no encryption pair had a positive cost to draw");
    }
    // each curve, gathered before anything is drawn so the legend and the lines cannot disagree
    let mut lines: Vec<(String, Vec<(f64, f64)>)> = Vec::with_capacity(depths.len() * 2);
    for depth in &depths {
        // plaintext first at each depth, so it is the lower of the pair wherever encryption cost
        // anything and the two sit next to each other in the legend
        for (wire, encrypted) in [("plaintext", false), ("TLS", true)] {
            let mut curve: Vec<(f64, f64)> = points
                .iter()
                .filter(|point| point.depth == *depth)
                .map(|point| {
                    let cost = if encrypted {
                        point.tls_ns
                    } else {
                        point.plain_ns
                    };
                    (point.row_bytes as f64, cost)
                })
                .collect();
            curve.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));
            if !curve.is_empty() {
                lines.push((format!("{depth} deep, {wire}"), curve));
            }
        }
    }
    let aria = format!(
        "Cost of one get on each wire against row width, {} curves, from {} to {}, both axes \
         logarithmic",
        lines.len(),
        fmt::duration_ns(min_y),
        fmt::duration_ns(max_y)
    );
    let entries: Vec<legend::Entry> = lines
        .iter()
        .enumerate()
        .map(|(index, (name, _))| legend::Entry::new(name.clone(), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    // the widths the sweep was run at, which are powers of two and are not where a log axis would
    // otherwise put its gridlines
    let mut ticks: Vec<f64> = points.iter().map(|point| point.row_bytes as f64).collect();
    ticks.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    ticks.dedup_by(|left, right| (*left - *right).abs() <= right.abs() * 1e-9);
    let ticks = (ticks.len() <= 12).then_some(ticks);
    super::draw("chart-encryption-absolute", &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (area, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&area)
            .margin(16)
            // no gutter is reserved on the right any more, because nothing is drawn out there
            .margin_right(24)
            .x_label_area_size(46)
            .y_label_area_size(78)
            .build_cartesian_2d(
                Scale::new(min_x * 0.85..max_x * 1.2, Axis::Log).ticks(ticks),
                Scale::new(min_y * 0.7..max_y * 1.4, Axis::Log),
            )?;
        crate::themed_mesh!(chart)
            .x_desc("row width")
            .x_label_formatter(&|value: &f64| Unit::Bytes.format(*value))
            .y_desc("p50 of one get")
            .y_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        for (index, (_, curve)) in lines.iter().enumerate() {
            let colour = palette::series(index);
            chart.draw_series(LineSeries::new(curve.iter().copied(), colour.stroke_width(2)))?;
            chart.draw_series(
                curve
                    .iter()
                    .map(|point| Circle::new(*point, 3, colour.filled())),
            )?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::macro_layer::{
        ConfFacts, DurationParts, ScaleFacts, Stats, Timing, WorkloadCapture,
    };

    /// Build a stats summary whose every percentile is the same figure
    ///
    /// The pairing only reads one metric, so a summary that is flat across all of them is the
    /// smallest thing that exercises it.
    ///
    /// # Arguments
    ///
    /// * `ns` - The figure every percentile takes
    fn flat(ns: u64) -> Stats {
        let at = DurationParts {
            secs: ns / 1_000_000_000,
            nanos: (ns % 1_000_000_000) as u32,
        };
        Stats {
            count: 1000,
            max: at,
            p99: at,
            p95: at,
            p90: at,
            p50: at,
            avg: at,
            min: at,
        }
    }

    /// Build a workload capture with one metric and an interval around it
    ///
    /// # Arguments
    ///
    /// * `row_bytes` - How wide its rows were
    /// * `depth` - How many queries were outstanding
    /// * `clients` - How many clients produced them
    /// * `tls` - Whether it was encrypted
    /// * `runs_ns` - What each run measured
    fn arm(
        row_bytes: u64,
        depth: u32,
        clients: u32,
        tls: bool,
        runs_ns: &[u64],
    ) -> WorkloadCapture {
        let stats = |ns: u64| {
            let mut ops = BTreeMap::new();
            ops.insert(OP.to_string(), flat(ns));
            ops
        };
        let median = runs_ns[runs_ns.len() / 2];
        WorkloadCapture {
            timing: Timing::PerQuery,
            seed: 42,
            scale: ScaleFacts {
                scale: "full".to_string(),
                rows: 100,
                row_bytes,
                keys: 100,
                concurrency: depth,
                clients: Some(clients),
                // not a mixture, a width distribution or a skewed access pattern
                ..ScaleFacts::default()
            },
            conf: Some(ConfFacts {
                shards: 12,
                memory: "4Gi".to_string(),
                durability: "fsync".to_string(),
                tls,
                digest: "test".to_string(),
            }),
            counters: BTreeMap::new(),
            ops: stats(median),
            runs: Some(runs_ns.len() as u32),
            wall_clock_ns: Some(runs_ns.to_vec()),
            spread_pct: Some(0.0),
            runs_detail: Some(
                runs_ns
                    .iter()
                    .map(|ns| crate::model::macro_layer::WorkloadRun {
                        wall_clock_ns: *ns,
                        ops: stats(*ns),
                        counters: BTreeMap::new(),
                    })
                    .collect(),
            ),
        }
    }

    /// A capture holding one pair
    ///
    /// # Arguments
    ///
    /// * `plain` - What the plaintext arm's runs measured
    /// * `tls` - What the encrypted arm's runs measured
    fn capture_with(plain: &[u64], tls: &[u64]) -> MacroCaptureV2 {
        let mut workloads = BTreeMap::new();
        workloads.insert(
            format!("{DEPTH_SWEEP}plain/256/1"),
            arm(256, 1, 1, false, plain),
        );
        workloads.insert(format!("{DEPTH_SWEEP}tls/256/1"), arm(256, 1, 1, true, tls));
        MacroCaptureV2 {
            version: 2,
            label: Some("test".to_string()),
            workloads,
        }
    }

    #[test]
    /// An arm is paired with its twin on the facts rather than on its name
    fn a_pair_is_joined_on_its_facts() {
        let points = pairs(&capture_with(&[100, 100, 100], &[200, 200, 200]), DEPTH_SWEEP);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].row_bytes, 256);
        assert_eq!(points[0].plain_ns, 100.0);
        assert_eq!(points[0].tls_ns, 200.0);
        assert!((points[0].overhead_pct() - 100.0).abs() < 0.001);
    }

    #[test]
    /// An arm with no twin is dropped rather than drawn against nothing
    fn an_unpaired_arm_is_dropped() {
        let mut capture = capture_with(&[100, 100, 100], &[200, 200, 200]);
        capture.workloads.remove(&format!("{DEPTH_SWEEP}tls/256/1"));
        assert!(pairs(&capture, DEPTH_SWEEP).is_empty());
    }

    #[test]
    /// Overlapping runs are not a result, however far apart the medians are
    ///
    /// This is the macro layer's own rule, and it is what stops a curve being drawn through noise.
    fn overlapping_intervals_are_not_separated() {
        // the two sets of runs interleave, so neither is reliably faster
        let points = pairs(&capture_with(&[100, 150, 300], &[120, 200, 280]), DEPTH_SWEEP);
        assert_eq!(points.len(), 1);
        assert!(!points[0].separated);
        // and one where they do not overlap at all
        let points = pairs(&capture_with(&[100, 110, 120], &[200, 210, 220]), DEPTH_SWEEP);
        assert!(points[0].separated);
    }

    #[test]
    /// A pair that ran once has no interval and cannot be called a result
    fn a_single_run_pair_is_never_separated() {
        let points = pairs(&capture_with(&[100], &[900]), DEPTH_SWEEP);
        assert_eq!(points.len(), 1);
        assert!(!points[0].separated);
    }

    #[test]
    /// The two sweeps do not pair with each other
    ///
    /// A depth arm at depth one and a client arm at one client have identical facts, so pairing on
    /// the facts alone would join them. The sweep prefix is what keeps them apart.
    fn the_two_sweeps_are_kept_apart() {
        let mut capture = capture_with(&[100, 100, 100], &[200, 200, 200]);
        capture.workloads.insert(
            format!("{CLIENT_SWEEP}plain/256/1"),
            arm(256, 1, 1, false, &[100, 100, 100]),
        );
        assert_eq!(pairs(&capture, DEPTH_SWEEP).len(), 1);
        // the client sweep's plaintext arm has no encrypted twin, so it pairs with nothing
        assert!(pairs(&capture, CLIENT_SWEEP).is_empty());
    }

    #[test]
    /// What encryption cost is drawn as a duration, not as a share
    ///
    /// A percentage cannot tell a large share of a very small number from a small share of a large
    /// one, and both are on this sweep. The axis is what stops the chart making that claim.
    fn the_overhead_chart_is_drawn_in_nanoseconds() {
        let capture = capture_with(&[100, 110, 120], &[300, 310, 320]);
        let svg = draw_by_row(&capture).expect("it draws");
        assert!(svg.contains("what TLS added"), "the axis is not the added cost");
        assert!(!svg.contains("TLS cost over plaintext"), "the percentage axis survived");
        // 200 ns of overhead, written the way every other duration on the page is
        assert!(svg.contains(" ns"), "no duration was written on the axis");
        assert!(!svg.contains('%'), "a percentage was written anyway");
    }

    #[test]
    /// Every curve is named once, in the legend, and nowhere else
    fn every_curve_is_named_in_the_legend() {
        let capture = capture_with(&[100, 110, 120], &[300, 310, 320]);
        let svg = draw_by_row(&capture).expect("it draws");
        assert_eq!(svg.matches("1 deep").count(), 1, "the curve is named twice");
    }

    #[test]
    /// The absolute chart covers every depth, not only the shallowest
    fn the_absolute_chart_covers_the_whole_sweep() {
        let mut capture = capture_with(&[100, 110, 120], &[300, 310, 320]);
        capture.workloads.insert(
            format!("{DEPTH_SWEEP}plain/256/8"),
            arm(256, 8, 1, false, &[400, 410, 420]),
        );
        capture.workloads.insert(
            format!("{DEPTH_SWEEP}tls/256/8"),
            arm(256, 8, 1, true, &[500, 510, 520]),
        );
        let svg = draw_absolute(&capture).expect("it draws");
        for name in ["1 deep, plaintext", "1 deep, TLS", "8 deep, plaintext", "8 deep, TLS"] {
            assert!(svg.contains(name), "{name} is not on the chart");
        }
    }

    #[test]
    /// More depths than the palette can carry is an error, not two curves in one colour
    fn too_many_depths_is_refused() {
        let mut capture = capture_with(&[100, 110, 120], &[300, 310, 320]);
        // five depths is ten curves against a palette of eight
        for depth in [2u32, 4, 8, 16] {
            capture.workloads.insert(
                format!("{DEPTH_SWEEP}plain/256/{depth}"),
                arm(256, depth, 1, false, &[400, 410, 420]),
            );
            capture.workloads.insert(
                format!("{DEPTH_SWEEP}tls/256/{depth}"),
                arm(256, depth, 1, true, &[500, 510, 520]),
            );
        }
        let err = draw_absolute(&capture).expect_err("ten curves must not pass");
        assert!(format!("{err}").contains("palette of 8"));
    }
}
