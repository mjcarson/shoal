//! One measurement swept along one axis, with a line per series
//!
//! Generalised out of [`micro_scaling`](super::micro_scaling), which drew exactly this shape for
//! one caller. The grid needs it four times over - cost against row width, cost against read
//! share, throughput against load depth, latency against load depth - and four copies of the same
//! four hundred lines is how two of them end up drawn differently.
//!
//! # Labels at the end of the line, not in a legend
//!
//! Every series is named at its own right hand end. A legend costs the reader a colour match on
//! every glance, and three of the eight series colours fall below a 3:1 contrast ratio on the light
//! themes, so a colour match is exactly what this must not require. The labels are spread apart
//! along the value axis when two series end at the same place, which they routinely do.

use anyhow::{Result, bail};
use plotters::prelude::*;

use super::palette;

/// How many series are drawn before the rest are folded away
///
/// Eight is the palette's width. A ninth series would reuse a colour and two lines would be
/// indistinguishable, which is worse than a line that is missing and declared.
const MAX_SERIES: usize = 8;

/// Whether an axis is drawn logarithmically
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
    /// Even steps are even differences
    Linear,
    /// Even steps are even ratios, which is what makes a shape readable across four orders of
    /// magnitude
    Log,
}

/// How a value axis's numbers are written
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// A duration in nanoseconds
    Duration,
    /// A count per second
    Rate,
    /// A plain number
    Count,
}

impl Unit {
    /// Writes one value in this unit
    ///
    /// # Arguments
    ///
    /// * `value` - The value to write
    pub fn format(self, value: f64) -> String {
        match self {
            Unit::Duration => crate::fmt::duration_ns(value),
            // a rate is quoted in thousands rather than in full, since an axis label of
            // `1,284,113` is wider than the space an axis has for it
            Unit::Rate => {
                if value >= 1_000_000.0 {
                    format!("{:.1}M", value / 1_000_000.0)
                } else if value >= 1_000.0 {
                    format!("{:.0}k", value / 1_000.0)
                } else {
                    format!("{value:.0}")
                }
            }
            Unit::Count => crate::fmt::thousands(value.round() as u128),
        }
    }
}

/// One line on the chart
#[derive(Debug, Clone)]
pub struct Series {
    /// What this line is called, drawn at its right hand end
    pub name: String,
    /// The points on it, which are sorted by their x before drawing
    pub points: Vec<(f64, f64)>,
}

/// Everything a sweep chart needs that is not its data
#[derive(Debug, Clone)]
pub struct Spec {
    /// The element id, which must be unique within a page
    pub id: String,
    /// What the x axis is
    pub x_desc: String,
    /// What the y axis is
    pub y_desc: String,
    /// Whether the x axis is logarithmic
    pub x_axis: Axis,
    /// Whether the y axis is logarithmic
    pub y_axis: Axis,
    /// How x values are written
    pub x_unit: Unit,
    /// How y values are written
    pub y_unit: Unit,
}

/// Draws a sweep
///
/// # Arguments
///
/// * `spec` - What the axes are and how to write them
/// * `series` - The lines to draw, in the order they should take colours
pub fn draw(spec: &Spec, series: &[Series]) -> Result<String> {
    // nothing to draw is not a chart, and an empty one on the page would read as a measurement of
    // zero rather than as an absence
    if series.is_empty() || series.iter().all(|line| line.points.is_empty()) {
        bail!("{} has no series to draw", spec.id);
    }
    // sorted by x so the line is drawn left to right rather than folding back on itself
    let mut shown: Vec<Series> = series.iter().take(MAX_SERIES).cloned().collect();
    for line in &mut shown {
        line.points
            .sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(std::cmp::Ordering::Equal));
    }
    let dropped = series.len().saturating_sub(shown.len());
    let (min_x, max_x) = span(&shown, spec.x_axis, |point| point.0)?;
    let (min_y, max_y) = span(&shown, spec.y_axis, |point| point.1)?;
    // padded so nothing is drawn on the frame. a log axis is padded multiplicatively, because an
    // additive pad below the smallest point can reach zero, which a log axis cannot place
    let (low_x, high_x) = pad(min_x, max_x, spec.x_axis);
    let (low_y, high_y) = pad(min_y, max_y, spec.y_axis);
    let aria = format!(
        "{} against {}, {} series, from {} to {}",
        spec.y_desc,
        spec.x_desc,
        shown.len(),
        spec.y_unit.format(min_y),
        spec.y_unit.format(max_y)
    );
    let spec = spec.clone();
    super::draw(&spec.id.clone(), &aria, 420, move |root| {
        let mut chart = ChartBuilder::on(root)
            .margin(16)
            .margin_right(210)
            .x_label_area_size(46)
            .y_label_area_size(84)
            .build_cartesian_2d(
                Scale::new(low_x..high_x, spec.x_axis),
                Scale::new(low_y..high_y, spec.y_axis),
            )?;
        let x_unit = spec.x_unit;
        let y_unit = spec.y_unit;
        crate::themed_mesh!(chart)
            .x_desc(spec.x_desc.clone())
            .x_label_formatter(&move |value: &f64| x_unit.format(*value))
            .y_desc(spec.y_desc.clone())
            .y_label_formatter(&move |value: &f64| y_unit.format(*value))
            .draw()?;
        // where each name goes, spread apart so two series that end together stay readable
        let anchors = anchors(&shown, low_y, high_y, spec.y_axis);
        for (index, line) in shown.iter().enumerate() {
            let colour = palette::series(index);
            chart.draw_series(LineSeries::new(
                line.points.iter().copied(),
                colour.stroke_width(2),
            ))?;
            chart.draw_series(
                line.points
                    .iter()
                    .map(|point| Circle::new(*point, 3, colour.filled())),
            )?;
            if let (Some((x, _)), Some(at)) = (line.points.last(), anchors.get(index)) {
                // just past the end of the line, in whichever space the axis is in
                let offset = match spec.x_axis {
                    Axis::Linear => x + (high_x - low_x) * 0.02,
                    Axis::Log => x * 1.08,
                };
                chart.draw_series(std::iter::once(Text::new(
                    line.name.clone(),
                    (offset, *at),
                    super::label_font(11),
                )))?;
            }
        }
        // a chart that dropped a series says so, or it looks complete and is not
        if dropped > 0 {
            chart.draw_series(std::iter::once(Text::new(
                format!("{dropped} further series not drawn"),
                (low_x, low_y),
                super::label_font(10),
            )))?;
        }
        Ok(())
    })
}

/// A coordinate range that is linear or logarithmic, chosen at run time
///
/// plotters picks its axis kind in the type, so a chart that is linear on one page and logarithmic
/// on the next would be two functions. This is the one wrapper that lets it be a parameter.
struct Scale {
    /// The range being spanned
    range: std::ops::Range<f64>,
    /// Which way it is spaced
    axis: Axis,
}

impl Scale {
    /// Builds a scale over a range
    ///
    /// # Arguments
    ///
    /// * `range` - The values to span
    /// * `axis` - Whether the spacing is logarithmic
    fn new(range: std::ops::Range<f64>, axis: Axis) -> Self {
        Scale { range, axis }
    }
}

impl plotters::coord::ranged1d::Ranged for Scale {
    type FormatOption = plotters::coord::ranged1d::NoDefaultFormatting;
    type ValueType = f64;

    /// Maps a value onto the pixel range
    ///
    /// # Arguments
    ///
    /// * `value` - The value to place
    /// * `limit` - The pixel range to place it in
    fn map(&self, value: &f64, limit: (i32, i32)) -> i32 {
        match self.axis {
            Axis::Linear => plotters::coord::ranged1d::Ranged::map(
                &plotters::coord::types::RangedCoordf64::from(self.range.clone()),
                value,
                limit,
            ),
            Axis::Log => {
                let scaled: plotters::coord::combinators::LogCoord<f64> =
                    self.range.clone().log_scale().into();
                plotters::coord::ranged1d::Ranged::map(&scaled, value, limit)
            }
        }
    }

    /// The values to draw gridlines and labels at
    ///
    /// # Arguments
    ///
    /// * `hint` - How many the caller has room for
    fn key_points<Hint: plotters::coord::ranged1d::KeyPointHint>(&self, hint: Hint) -> Vec<f64> {
        match self.axis {
            Axis::Linear => plotters::coord::ranged1d::Ranged::key_points(
                &plotters::coord::types::RangedCoordf64::from(self.range.clone()),
                hint,
            ),
            Axis::Log => {
                let scaled: plotters::coord::combinators::LogCoord<f64> =
                    self.range.clone().log_scale().into();
                plotters::coord::ranged1d::Ranged::key_points(&scaled, hint)
            }
        }
    }

    /// The range this scale spans
    fn range(&self) -> std::ops::Range<f64> {
        self.range.clone()
    }
}

impl plotters::coord::ranged1d::ValueFormatter<f64> for Scale {
    /// Writes one axis value
    ///
    /// Never called: every caller sets its own formatter through the mesh, which is what carries
    /// the unit. This exists because the trait bound demands it.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to write
    fn format(value: &f64) -> String {
        format!("{value}")
    }
}

/// The smallest and largest value any series reaches along one dimension
///
/// # Arguments
///
/// * `series` - The lines being drawn
/// * `axis` - Whether the dimension is logarithmic, which excludes non-positive values
/// * `pick` - Which of a point's two coordinates to take
fn span<F>(series: &[Series], axis: Axis, pick: F) -> Result<(f64, f64)>
where
    F: Fn(&(f64, f64)) -> f64,
{
    let mut low = f64::MAX;
    let mut high = f64::MIN;
    for line in series {
        for point in &line.points {
            let value = pick(point);
            // a zero or negative value cannot be placed on a log axis, and plotters emits `NaN`
            // into the path data rather than failing, which draws nothing at all
            if axis == Axis::Log && value <= 0.0 {
                continue;
            }
            low = low.min(value);
            high = high.max(value);
        }
    }
    if low > high {
        bail!("no series had a value that could be placed on this axis");
    }
    Ok((low, high))
}

/// A range with room around it
///
/// # Arguments
///
/// * `low` - The smallest value drawn
/// * `high` - The largest value drawn
/// * `axis` - Whether the padding is multiplicative
fn pad(low: f64, high: f64, axis: Axis) -> (f64, f64) {
    match axis {
        Axis::Linear => {
            // a series with one distinct value would otherwise span nothing, and a zero width
            // range is an axis plotters cannot build
            let width = (high - low).max(high.abs().max(1.0) * 0.1);
            (low - width * 0.08, high + width * 0.12)
        }
        Axis::Log => (low * 0.8, high * 1.3),
    }
}

/// Where each series' end label sits, far enough apart to be read
///
/// The same spreading [`micro_scaling`](super::micro_scaling) does, generalised over the axis kind:
/// on a log axis "far enough below" is a division, and on a linear one it is a subtraction.
///
/// # Arguments
///
/// * `series` - The lines being drawn, in drawing order
/// * `low` - The bottom of the value axis
/// * `high` - The top of it
/// * `axis` - Whether the axis is logarithmic
fn anchors(series: &[Series], low: f64, high: f64, axis: Axis) -> Vec<f64> {
    /// How far apart two labels have to be, as a fraction of the whole axis
    const MIN_SEPARATION: f64 = 0.06;
    // start each label at the end of its own line
    let mut anchors: Vec<f64> = series
        .iter()
        .map(|line| line.points.last().map(|(_, value)| *value).unwrap_or(low))
        .collect();
    // then walk them from the top down, pushing any that is too close to the one above it further
    // down, keeping each as close to its own line as it can be
    let mut order: Vec<usize> = (0..anchors.len()).collect();
    order.sort_by(|left, right| {
        anchors[*right]
            .partial_cmp(&anchors[*left])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let gap = match axis {
        Axis::Linear => (high - low) * MIN_SEPARATION,
        Axis::Log => (high / low.max(f64::MIN_POSITIVE)).log10() * MIN_SEPARATION,
    };
    let mut previous: Option<f64> = None;
    for index in order {
        let mut at = anchors[index].max(low);
        if let Some(above) = previous {
            let ceiling = match axis {
                Axis::Linear => above - gap,
                Axis::Log => above / 10f64.powf(gap),
            };
            if at > ceiling {
                at = ceiling;
            }
        }
        // never push a label off the bottom of the chart trying to make room
        at = at.max(low);
        anchors[index] = at;
        previous = Some(at);
    }
    anchors
}

#[cfg(test)]
mod tests {
    use super::{Axis, Series, Spec, Unit, draw};

    /// A spec for the tests, which are about the drawing rather than the labels
    ///
    /// # Arguments
    ///
    /// * `x` - Whether the x axis is logarithmic
    /// * `y` - Whether the y axis is logarithmic
    fn spec(x: Axis, y: Axis) -> Spec {
        Spec {
            id: "chart-test-sweep".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "p50".to_string(),
            x_axis: x,
            y_axis: y,
            x_unit: Unit::Count,
            y_unit: Unit::Duration,
        }
    }

    /// Builds a series from `(x, y)` pairs
    ///
    /// # Arguments
    ///
    /// * `name` - What the line is called
    /// * `points` - The points on it
    fn series(name: &str, points: &[(f64, f64)]) -> Series {
        Series {
            name: name.to_string(),
            points: points.to_vec(),
        }
    }

    /// Every series is drawn and named
    #[test]
    fn it_draws_and_labels_every_series() {
        let svg = draw(
            &spec(Axis::Log, Axis::Log),
            &[
                series("unsorted", &[(64.0, 100.0), (1024.0, 300.0)]),
                series("sorted", &[(64.0, 120.0), (1024.0, 380.0)]),
            ],
        )
        .expect("it draws");
        assert!(svg.contains("unsorted"));
        assert!(svg.contains("sorted"));
        assert!(!svg.contains("NaN"));
    }

    /// A linear axis draws without a log axis's constraints
    ///
    /// The read share axis runs from zero, which a log axis cannot place at all - which is the
    /// reason the axis kind is a parameter rather than a constant.
    #[test]
    fn a_linear_axis_can_start_at_zero() {
        let svg = draw(
            &spec(Axis::Linear, Axis::Linear),
            &[series(
                "unsorted",
                &[(0.0, 100.0), (50.0, 140.0), (100.0, 90.0)],
            )],
        )
        .expect("it draws");
        assert!(!svg.contains("NaN"), "a zero x broke the axis");
        assert!(!svg.contains("inf"));
    }

    /// A point that cannot sit on a log axis is skipped rather than drawn as `NaN`
    #[test]
    fn a_non_positive_value_does_not_break_a_log_axis() {
        let svg = draw(
            &spec(Axis::Log, Axis::Log),
            &[series("mixed", &[(64.0, 0.0), (1024.0, 300.0)])],
        )
        .expect("it draws");
        assert!(!svg.contains("NaN"));
    }

    /// A series with one distinct value still builds an axis
    ///
    /// A zero width range is one plotters cannot build, and a flat series is exactly what a chart
    /// showing "this does not depend on the axis" looks like.
    #[test]
    fn a_flat_series_still_draws() {
        let svg = draw(
            &spec(Axis::Linear, Axis::Linear),
            &[series("flat", &[(0.0, 100.0), (50.0, 100.0), (100.0, 100.0)])],
        )
        .expect("it draws");
        assert!(!svg.contains("NaN"));
    }

    /// More series than the palette can carry are folded away, and the chart says so
    #[test]
    fn a_long_tail_is_dropped_and_declared() {
        let lines: Vec<super::Series> = (0..11)
            .map(|index| {
                series(
                    &format!("s{index}"),
                    &[(1.0, f64::from(index) + 1.0), (2.0, f64::from(index) + 2.0)],
                )
            })
            .collect();
        let svg = draw(&spec(Axis::Linear, Axis::Linear), &lines).expect("it draws");
        assert!(svg.contains("3 further series not drawn"), "the tail was not declared");
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&spec(Axis::Log, Axis::Log), &[]).is_err());
        assert!(draw(&spec(Axis::Log, Axis::Log), &[series("empty", &[])]).is_err());
    }

    /// The same input draws the same chart, byte for byte
    #[test]
    fn drawing_is_deterministic() {
        let lines = [series("a", &[(1.0, 10.0), (2.0, 20.0)])];
        let once = draw(&spec(Axis::Linear, Axis::Log), &lines).expect("it draws");
        let twice = draw(&spec(Axis::Linear, Axis::Log), &lines).expect("it draws");
        assert_eq!(once, twice);
    }

    /// A rate is written short enough to fit an axis label
    #[test]
    fn a_rate_is_written_compactly() {
        assert_eq!(Unit::Rate.format(1_284_113.0), "1.3M");
        assert_eq!(Unit::Rate.format(42_600.0), "43k");
        assert_eq!(Unit::Rate.format(812.0), "812");
    }
}
