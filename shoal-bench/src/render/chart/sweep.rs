//! One measurement swept along one axis, with a line per series
//!
//! Generalised out of [`micro_scaling`](super::micro_scaling), which drew exactly this shape for
//! one caller. The grid needs it four times over - cost against row width, cost against read
//! share, throughput against load depth, latency against load depth - and four copies of the same
//! four hundred lines is how two of them end up drawn differently.
//!
//! # A legend under the plot, not a label at the end of each line
//!
//! Every series used to be named at its own right hand end, with a pass that pushed apart any two
//! labels that landed together. On a chart of a mixture the lines routinely end within a few
//! pixels of each other, and that pass ran out of axis: `chart-grid-latency` put two of its eight
//! names three pixels apart at an eight pixel font. The names are now in a
//! [`legend`](super::legend) below the plot, which is also what let the two hundred and ten units
//! of right margin they needed go back to the plot.
//!
//! # Ticks where the measurements are
//!
//! A sweep's x values are the widths, depths and key counts a workload was actually run at, and
//! there are never many of them. Left to itself plotters ticks a log axis at powers of ten, which
//! labels a gridline at 1,000,000 on a chart whose widest series is 1 MiB - two different numbers
//! for the same place. When the x values are few enough to fit, they are the ticks.

use anyhow::{Result, bail};
use plotters::coord::ranged1d::KeyPointWeight;
use plotters::prelude::*;

use super::{legend, palette};

/// How many series are drawn before the rest are folded away
///
/// Eight is the palette's width. A ninth series would reuse a colour and two lines would be
/// indistinguishable, which is worse than a line that is missing and declared.
const MAX_SERIES: usize = 8;

/// How tall the plotting area is, before the legend is added under it
const PLOT_HEIGHT: u32 = 420;

/// How many distinct x values an axis will tick individually
///
/// Above this the values stop being a handful of chosen points and start being a scatter, and the
/// labels would collide however they were written - so plotters chooses the ticks instead.
const MAX_TICKS: usize = 12;

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
    /// A number of bytes, in binary units
    Bytes,
    /// A number of bytes per second, in binary units
    ByteRate,
    /// A share of a whole, already in percent
    ///
    /// For an axis that *is* a percentage - a read share is a knob a workload was set to, not a
    /// comparison against anything - rather than for one where a percentage stands in for a
    /// measurement nobody wrote down.
    Percent,
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
                if value >= 1_000_000_000.0 {
                    format!("{:.1}G", value / 1_000_000_000.0)
                } else if value >= 1_000_000.0 {
                    format!("{:.1}M", value / 1_000_000.0)
                } else if value >= 1_000.0 {
                    format!("{:.0}k", value / 1_000.0)
                } else {
                    format!("{value:.0}")
                }
            }
            Unit::Count => crate::fmt::thousands(value.round() as u128),
            Unit::Bytes => crate::fmt::bytes_axis(value),
            Unit::ByteRate => crate::fmt::byte_rate(value),
            Unit::Percent => format!("{}%", crate::fmt::fixed(value, 0)),
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
    // the x values the workloads were actually run at, when there are few enough to be ticks
    let ticks = distinct_x(&shown);
    // one legend entry per line, in the order the colours were handed out
    let entries: Vec<legend::Entry> = shown
        .iter()
        .enumerate()
        .map(|(index, line)| legend::Entry::new(line.name.clone(), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    let spec = spec.clone();
    super::draw(&spec.id.clone(), &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (area, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&area)
            .margin(16)
            // no gutter is reserved on the right any more, because nothing is drawn out there
            .margin_right(24)
            .x_label_area_size(46)
            .y_label_area_size(84)
            .build_cartesian_2d(
                Scale::new(low_x..high_x, spec.x_axis).ticks(ticks),
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
        }
        // a chart that dropped a series says so, or it looks complete and is not
        if dropped > 0 {
            chart.draw_series(std::iter::once(Text::new(
                format!("{dropped} further series not drawn"),
                (low_x, low_y),
                super::label_font(10),
            )))?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

/// The x values every series shares, if there are few enough of them to be ticks
///
/// # Arguments
///
/// * `series` - The lines being drawn
fn distinct_x(series: &[Series]) -> Option<Vec<f64>> {
    // gather every x anything was measured at
    let mut values: Vec<f64> = series
        .iter()
        .flat_map(|line| line.points.iter().map(|(x, _)| *x))
        .collect();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    // two series measured at the same width contribute one tick, not two. compared relatively,
    // because the values span four orders of magnitude and an absolute epsilon fits neither end
    values.dedup_by(|left, right| (*left - *right).abs() <= right.abs() * 1e-9);
    // above the cap these have stopped being chosen points and started being a scatter, and
    // plotters' own spacing reads better than a label at every one of them
    if values.is_empty() || values.len() > MAX_TICKS {
        return None;
    }
    Some(values)
}

/// A coordinate range that is linear or logarithmic, chosen at run time
///
/// plotters picks its axis kind in the type, so a chart that is linear on one page and logarithmic
/// on the next would be two functions. This is the one wrapper that lets it be a parameter.
pub(super) struct Scale {
    /// The range being spanned
    range: std::ops::Range<f64>,
    /// Which way it is spaced
    axis: Axis,
    /// The values to tick, when the caller has better ones than plotters would choose
    ticks: Option<Vec<f64>>,
}

impl Scale {
    /// Builds a scale over a range
    ///
    /// # Arguments
    ///
    /// * `range` - The values to span
    /// * `axis` - Whether the spacing is logarithmic
    pub(super) fn new(range: std::ops::Range<f64>, axis: Axis) -> Self {
        Scale {
            range,
            axis,
            ticks: None,
        }
    }

    /// Fixes the values this axis ticks at
    ///
    /// # Arguments
    ///
    /// * `ticks` - The values to tick, or `None` to let plotters space them
    pub(super) fn ticks(mut self, ticks: Option<Vec<f64>>) -> Self {
        self.ticks = ticks;
        self
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
        // a fixed tick list answers for the gridlines that carry labels, and refuses the minor
        // ones outright: a minor gridline between two ticks that are already the measurements is
        // a line at a value nothing was measured at
        if let Some(ticks) = &self.ticks {
            return match hint.weight() {
                KeyPointWeight::Bold => ticks.clone(),
                KeyPointWeight::Any => Vec::new(),
            };
        }
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

    /// Every series is drawn, and named exactly once in the legend
    ///
    /// Exactly once is the part that matters: a name drawn both at the end of its line and in the
    /// legend would mean the end labels had not actually been removed.
    #[test]
    fn it_draws_and_labels_every_series() {
        let svg = draw(
            &spec(Axis::Log, Axis::Log),
            &[
                series("read unsorted", &[(64.0, 100.0), (1024.0, 300.0)]),
                series("write sorted", &[(64.0, 120.0), (1024.0, 380.0)]),
            ],
        )
        .expect("it draws");
        assert_eq!(svg.matches("read unsorted").count(), 1);
        assert_eq!(svg.matches("write sorted").count(), 1);
        assert!(!svg.contains("NaN"));
    }

    /// The canvas grows to hold the legend rather than drawing it over the plot
    #[test]
    fn the_canvas_makes_room_for_the_legend() {
        let svg = draw(
            &spec(Axis::Log, Axis::Log),
            &[series("unsorted", &[(64.0, 100.0), (1024.0, 300.0)])],
        )
        .expect("it draws");
        let entries = [super::legend::Entry::new(
            "unsorted",
            super::palette::series(0),
        )];
        let expected = super::PLOT_HEIGHT + super::legend::height(&entries);
        assert!(
            svg.contains(&format!(r#"viewBox="0 0 820 {expected}""#)),
            "the canvas is not plot plus legend"
        );
    }

    /// A handful of measured x values become the ticks themselves
    #[test]
    fn a_short_x_axis_ticks_at_the_measurements() {
        let svg = draw(
            &Spec {
                x_unit: Unit::Bytes,
                ..spec(Axis::Log, Axis::Log)
            },
            &[series(
                "unsorted",
                &[(256.0, 100.0), (4096.0, 300.0), (1_048_576.0, 900.0)],
            )],
        )
        .expect("it draws");
        // the widths themselves, in the units the series names use
        for label in ["256 B", "4 KiB", "1 MiB"] {
            assert!(svg.contains(label), "{label} is not on the axis");
        }
        // and not plotters' powers of ten, which land between them
        assert!(!svg.contains("977 KiB"), "a power of ten was ticked anyway");
    }

    /// Too many distinct x values and plotters chooses the ticks again
    #[test]
    fn a_long_x_axis_is_left_to_plotters() {
        let points: Vec<(f64, f64)> = (1..=40)
            .map(|step| (f64::from(step) * 10.0, f64::from(step)))
            .collect();
        let svg = draw(&spec(Axis::Linear, Axis::Linear), &[series("many", &points)])
            .expect("it draws");
        assert!(!svg.contains("NaN"));
        // forty labels would not fit, so far fewer than forty were drawn
        assert!(
            svg.matches("<text ").count() < 30,
            "every one of forty x values was ticked"
        );
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
        // and a rate past a billion steps up rather than reading `1000.0M`
        assert_eq!(Unit::Rate.format(1_500_000_000.0), "1.5G");
    }

    /// A width is written in the binary units the widths were chosen in
    #[test]
    fn a_width_is_written_in_binary_units() {
        assert_eq!(Unit::Bytes.format(256.0), "256 B");
        assert_eq!(Unit::Bytes.format(65_536.0), "64 KiB");
        assert_eq!(Unit::Bytes.format(4_194_304.0), "4 MiB");
        assert_eq!(Unit::ByteRate.format(222_039_839.0), "212 MiB/s");
    }
}
