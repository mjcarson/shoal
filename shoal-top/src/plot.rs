//! Draws the selected series, and draws what is missing as missing
//!
//! # The two modes
//!
//! A **sweep** puts a recorded fact on the key axis - the read share, the row width, the load depth
//! - and one line per capture along it. That is the shape the book's charts already have, and it is
//! what makes selecting a second capture worth doing: the whole curve is redrawn beside the first,
//! rather than one point moving.
//!
//! A **timeline** puts the captures on the key axis, in the order they were taken, and one line per
//! workload along it. That is the regression question asked directly.
//!
//! Neither subsumes the other. A timeline of one capture is a single point, and a sweep says
//! nothing about when.
//!
//! # A gap is never a zero
//!
//! Fifteen hundred of the ten thousand possible (capture, workload) pairs exist. A capture that did
//! not measure a workload did not measure it slowly, so:
//!
//! - a line is **split into runs of consecutive measurements** and drawn as one `Line` per run,
//!   never as one line through a substituted value;
//! - a run of a single measurement is drawn as a `Points` as well, or a workload that only one
//!   capture ever measured would be invisible;
//! - a bar that has no measurement is **not pushed at all**, and its slot is left empty, because
//!   the x position is computed from the group index rather than from how many bars came before.
//!
//! `f64::NAN` is deliberately not used as the gap marker. `egui_plot` folds every point into the
//! plot's bounds, and one NaN takes the bounds to `[NaN, NaN]`, which renders as an empty chart.
//!
//! # Hovering answers for the column, not for the point
//!
//! `egui_plot`'s own hover label is refused - `label_formatter(|_| None)` - and [`crate::readout`]
//! answers instead. Three reasons, and only one of them is a formatting problem: it answers for the
//! nearest line where the question is about all of them, it names a run that this module leaves
//! deliberately unnamed, and it prints the *plotted* number, which on a logarithmic axis is a
//! logarithm. See `docs/src/appendix/resolved/hover-label-reads-in-decades.md`.
//!
//! # The chart is framed for what it is drawing
//!
//! The plot's id is a constant, so `egui_plot`'s `PlotMemory` - and with it the reader's zoom and
//! pan - outlives every change to the metric, the axis and the selection. That is right while the
//! same numbers are on screen and wrong the moment they are not: a chart framed on a rate and then
//! handed a p99 is framed four orders of magnitude away from its own data, and draws empty.
//!
//! So [`View::refit`] is passed in by the caller, which is the only place that knows whether what
//! is drawn has changed, and turns `auto_bounds` back on for that frame. A pan or a zoom sticks
//! until the next such change, which is what makes it worth doing by hand.

use egui_plot::{
    Bar, BarChart, GridMark, Legend, Line, LineStyle, MarkerShape, PlotPoints, Points, VLine,
};

use crate::index::{Axis, Chart, Metric, Selection, Series, Source, Unit};
use crate::theme::{self, Palette};
use crate::{fmt, readout};

/// The smallest the chart is allowed to shrink to, in points
///
/// There is no height any more - the chart fills the panel it is in. This is the floor under that,
/// so a window squeezed short degrades to a small chart rather than to a sliver.
const MIN_SIZE: egui::Vec2 = egui::vec2(240.0, 200.0);

/// How wide one bar is, as a share of its slot
const BAR_WIDTH: f64 = 0.82;

/// How the chart is presented, as opposed to what is on it
///
/// Everything here is the reader's choice about the drawing rather than about the numbers. It is a
/// struct rather than four positional arguments because `refit` is not like the other three - it is
/// true for one frame and false afterwards - and a bare `bool` in fourth place would say nothing
/// about that at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct View {
    /// Whether the series are drawn as lines or as bars
    pub chart: Chart,
    /// Whether the value axis is logarithmic
    pub log_y: bool,
    /// Whether the key axis is logarithmic, which only a swept line axis may be
    pub log_x: bool,
    /// Whether to frame the chart on its data before this frame is drawn
    ///
    /// Set by the caller when what is drawn has changed, or when the reader asked for it. The chart
    /// cannot work this out for itself: it is handed one frame's series and has nowhere to remember
    /// the last frame's.
    pub refit: bool,
}

/// Draws the chart for one selection
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `source` - Where the series come from
/// * `selection` - What the reader asked to see
/// * `view` - How to draw it, and whether to frame it on its data first
pub fn draw(ui: &mut egui::Ui, source: &dyn Source, selection: &Selection, view: View) {
    // read out once, so the body below reads the way it did before the four became a struct
    let View {
        chart,
        log_y,
        log_x,
        refit,
    } = view;
    // build the series once; everything below is presentation of the same points
    let series = source.series(selection);
    if series.is_empty() {
        ui.weak("Nothing selected has a measurement to draw.");
        return;
    }
    // the palette the ui is being drawn in. read here rather than in the two drawing functions,
    // which are handed a `PlotUi` and have no visuals of their own
    let palette = theme::palette(ui);
    // say what was not drawn, rather than leaving a hole to be discovered. said *above* the chart
    // because the chart now takes everything below this line
    let absent = series
        .iter()
        .flat_map(|line| line.points.iter())
        .filter(|(_, value)| value.is_none())
        .count();
    if absent > 0 {
        let total: usize = series.iter().map(|line| line.points.len()).sum();
        ui.colored_label(
            palette.warn,
            format!(
                "{absent} of {total} points are absent - those captures did not measure that \
                 workload. They are drawn as gaps, never as zero."
            ),
        );
    }
    let unit = selection.metric.unit();
    let labels = source.key_labels(selection);
    // the key positions every group sits at, which bar mode needs as a set and line mode does not
    let groups = group_keys(&series);
    // and where each of those is drawn, which is what the captions are placed at and what the
    // readout snaps the pointer to. one definition, so the two cannot disagree
    let columns = readout::columns(chart, &groups, series.len(), log_x);
    let metric_label = selection.metric.axis_label();
    let axis = selection.axis;
    // both formatters have to undo the log before they render, or the axes read in decades
    let value_of = move |raw: f64| if log_y { 10f64.powf(raw) } else { raw };
    let key_of = move |raw: f64| if log_x { 10f64.powf(raw) } else { raw };
    let mut plot = egui_plot::Plot::new("shoal-top-chart")
        .min_size(MIN_SIZE)
        .legend(Legend::default())
        .allow_boxed_zoom(true)
        .y_axis_label(metric_label)
        .y_axis_formatter(move |mark: GridMark, _| fmt::value(unit, value_of(mark.value)));
    // a bar chart that does not start at zero misstates every ratio on it. a line chart that is
    // forced to start at zero flattens the difference somebody opened the explorer to see, so only
    // one of the two gets the anchor
    if matches!(chart, Chart::Bars) && !log_y {
        plot = plot.include_y(0.0);
    }
    // label the key axis with what its positions stand for
    match (chart, axis) {
        // bars and timelines both sit at synthetic integer positions that mean nothing on their own
        (Chart::Bars, _) | (_, Axis::Timeline) => {
            let ticks = key_ticks(&columns, &labels, axis);
            plot = plot
                .x_axis_formatter(move |mark: GridMark, _| tick_label(&ticks, mark.value))
                .x_grid_spacer(integer_spacer);
            if let Axis::Sweep(sweep) = axis {
                plot = plot.x_axis_label(sweep.label());
            } else {
                plot = plot.x_axis_label("capture, oldest first");
            }
        }
        // a swept line axis is a real quantity, so it is formatted rather than labelled
        (Chart::Line, Axis::Sweep(sweep)) => {
            plot = plot
                .x_axis_label(sweep.label())
                .x_axis_formatter(move |mark: GridMark, _| sweep.format(key_of(mark.value)));
        }
    }
    // `egui_plot`'s own hover label is refused rather than reformatted. it answers for one line
    // where the question is about all of them, it names an unnamed run - which is every run of a
    // line after the first - and it prints the *plotted* number, so a logarithmic chart reports four
    // hundred thousand queries a second as `5.615`. `readout` answers instead
    plot = plot.label_formatter(|_| None);
    // drawn last and with nothing after it, so it fills the panel it was handed
    let drawn = plot.show(ui, |plot_ui| {
        // asked for before the items are added, but applied after them: `set_auto_bounds` queues a
        // `BoundsModification` that `egui_plot` performs once the closure has returned, so the
        // frame it produces is the one fitted to the points pushed below
        if refit {
            plot_ui.set_auto_bounds(true);
        }
        // which column the pointer is reading, worked out before the series are pushed so that the
        // rule marking it is drawn under them rather than over the line it is marking
        let hit = readout::hit(plot_ui, &columns);
        if let Some(column) = hit.and_then(|at| columns.get(at)) {
            plot_ui.vline(
                VLine::new("", column.position)
                    .color(palette.muted)
                    .width(1.0)
                    .allow_hover(false),
            );
        }
        match chart {
            Chart::Line => draw_lines(plot_ui, palette, &series, log_y, log_x, axis),
            Chart::Bars => draw_bars(plot_ui, palette, &series, &groups, log_y),
        }
        hit
    });
    // and what is in that column, for every line on the chart rather than the nearest one. built
    // from the same series the chart was drawn from, so the two cannot disagree about a value
    if let Some(column) = drawn.inner.and_then(|at| columns.get(at)) {
        let rows = readout::rows(&series, column.key);
        let header = readout::column_label(axis, column.key, &labels);
        drawn
            .response
            .show_tooltip_ui(|ui| readout::draw(ui, palette, unit, &header, &rows));
    }
}

/// The dash pattern and marker one line is drawn with
///
/// The two channels that are left once the hue has been spent on the table. On a **sweep** the dash
/// stands for the capture and the marker for which of that table's curves this is, so two captures
/// of one curve stay the same colour with the same marker in two dash patterns. On a **timeline**
/// every series is a workload measured across one set of captures, so there is no capture to encode
/// and the mark carries both: it walks the markers first and moves the dash only once it has run out
/// of them.
///
/// # Arguments
///
/// * `axis` - What the key axis is, which decides what the two channels stand for
/// * `capture_at` - The capture's position among the ones being drawn
/// * `mark` - The curve's position among the ones sharing its colour
fn stroke_of(axis: Axis, capture_at: usize, mark: usize) -> (LineStyle, Option<MarkerShape>) {
    match axis {
        Axis::Sweep(_) => (theme::line_style(capture_at), theme::series_marker(mark)),
        Axis::Timeline => (
            theme::line_style(mark / theme::MARK_CYCLE),
            theme::series_marker(mark % theme::MARK_CYCLE),
        ),
    }
}

/// Draws every series as a line, gapped wherever a measurement is absent
///
/// # Arguments
///
/// * `plot_ui` - The plot being built
/// * `palette` - The palette the chart is being drawn in
/// * `series` - The series to draw
/// * `log_y` - Whether the value axis is logarithmic
/// * `log_x` - Whether the key axis is logarithmic
/// * `axis` - What the key axis is, which decides what the dash and the marker stand for
fn draw_lines(
    plot_ui: &mut egui_plot::PlotUi<'_>,
    palette: &Palette,
    series: &[Series],
    log_y: bool,
    log_x: bool,
    axis: Axis,
) {
    // the captures being drawn, in the order they first appear, so a dash pattern stands for a
    // capture rather than for a position in the series list
    let captures = drawn_captures(series);
    for line in series {
        // hue is the table, dash is the capture and the marker is which of that table's curves this
        // is - so two captures of one curve are one colour and one marker in two dash patterns,
        // which is the comparison the explorer exists to make
        let color = theme::series(palette, line.hue as usize);
        let at = captures.iter().position(|at| *at == line.capture).unwrap_or_default();
        let (style, marker) = stroke_of(axis, at, line.mark as usize);
        // split into runs of consecutive measurements, which is what puts a gap where a capture
        // never measured this workload
        let mut named = false;
        for run in runs(&line.points, log_y, log_x) {
            // a run of one is a real measurement with nothing to join it to, and would otherwise
            // draw as a zero length line - which is to say, as nothing at all
            if run.len() == 1 {
                let mut points = Points::new(line.name.clone(), PlotPoints::from(run.clone()))
                    .color(color)
                    // the line's own marker where it has one, and a circle where it does not -
                    // which is the shape `MARKERS` leaves out for exactly this
                    .shape(marker.unwrap_or(MarkerShape::Circle))
                    .radius(3.5);
                // only the first run of a series carries the name, so the legend has one entry per
                // series rather than one per gap in it
                if named {
                    points = points.allow_hover(true).name("");
                }
                named = true;
                plot_ui.points(points);
                continue;
            }
            let mut drawn = Line::new(line.name.clone(), PlotPoints::from(run.clone()))
                .color(color)
                .style(style)
                .width(1.8);
            if named {
                drawn = drawn.name("");
            }
            named = true;
            plot_ui.line(drawn);
            // and the marker on each of its measurements, where this line has one. named empty so
            // the legend keeps one entry per series: `egui_plot` leaves an unnamed item out of it
            if let Some(shape) = marker {
                plot_ui.points(
                    Points::new("", PlotPoints::from(run))
                        .color(color)
                        .shape(shape)
                        .radius(3.0)
                        .allow_hover(false),
                );
            }
        }
    }
}

/// Draws every series as a group of bars, leaving an empty slot where a measurement is absent
///
/// # Arguments
///
/// * `plot_ui` - The plot being built
/// * `palette` - The palette the chart is being drawn in
/// * `series` - The series to draw
/// * `groups` - The key positions, in axis order
/// * `log_y` - Whether the value axis is logarithmic
fn draw_bars(
    plot_ui: &mut egui_plot::PlotUi<'_>,
    palette: &Palette,
    series: &[Series],
    groups: &[f64],
    log_y: bool,
) {
    let width = series.len() as f64 + 1.0;
    for (at, line) in series.iter().enumerate() {
        let mut bars = Vec::new();
        for (group, key) in groups.iter().enumerate() {
            // find this series' measurement at this key, if it took one
            let Some((_, Some(value))) = line
                .points
                .iter()
                .find(|(candidate, _)| (candidate - key).abs() < f64::EPSILON)
            else {
                // no bar at all rather than a zero height one. the slot below is computed from the
                // group index, so leaving this out shows as a hole in the right place
                continue;
            };
            let height = if log_y { log_of(*value) } else { *value };
            // the group's slot, plus this series' offset within it. the `+ 1.0` in `width` is the
            // gutter that keeps two groups from touching
            let slot = group as f64 * width + at as f64;
            bars.push(Bar::new(slot, height).width(BAR_WIDTH));
        }
        if bars.is_empty() {
            continue;
        }
        // hue by position rather than by table, which is the one place the crate's split of hue,
        // dash and marker does not apply: a bar has neither of the other two channels, so two
        // captures of one curve drawn in one hue would be two adjacent bars of the same colour with
        // nothing at all to tell them apart
        plot_ui.bar_chart(
            BarChart::new(line.name.clone(), bars).color(theme::series(palette, at)),
        );
    }
}

/// Splits a series into runs of consecutive measurements
///
/// A `None` ends the run it sits in and starts a new one, which is what draws the gap. Returns the
/// runs in axis order, each with at least one point.
///
/// # Arguments
///
/// * `points` - The series' points, in axis order
/// * `log_y` - Whether to take the logarithm of each value
/// * `log_x` - Whether to take the logarithm of each key
fn runs(points: &[(f64, Option<f64>)], log_y: bool, log_x: bool) -> Vec<Vec<[f64; 2]>> {
    let mut runs = Vec::new();
    let mut current: Vec<[f64; 2]> = Vec::new();
    for (key, value) in points {
        // a key a logarithm cannot take - a read share of zero is the one in the corpus - is not
        // plottable on this axis at all, so it breaks the run the same way an absent value does
        let position = if log_x { log_of(*key) } else { *key };
        match value.filter(|_| position.is_finite()) {
            // extend the run in progress
            Some(found) => {
                let scaled = if log_y { log_of(found) } else { found };
                // a value a logarithm cannot take is not plottable on this axis, and breaks the run
                // rather than being clamped to some floor that would read as a real measurement
                if scaled.is_finite() {
                    current.push([position, scaled]);
                    continue;
                }
                if !current.is_empty() {
                    runs.push(std::mem::take(&mut current));
                }
            }
            // close the run in progress, which is what leaves the gap
            None => {
                if !current.is_empty() {
                    runs.push(std::mem::take(&mut current));
                }
            }
        }
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

/// The logarithm of a value, or a non finite number when it has none
///
/// # Arguments
///
/// * `value` - The value to take the logarithm of
fn log_of(value: f64) -> f64 {
    // zero and negative values have no logarithm. returning the non finite result rather than
    // clamping is deliberate: `runs` breaks the line there instead of drawing a floor that would
    // read as a measurement
    if value > 0.0 {
        value.log10()
    } else {
        f64::NAN
    }
}

/// The captures being drawn, in the order they first appear among the series
///
/// # Arguments
///
/// * `series` - The series about to be drawn
fn drawn_captures(series: &[Series]) -> Vec<u32> {
    let mut captures = Vec::new();
    for line in series {
        // first seen order rather than sorted, so a dash pattern is stable while a reader ticks
        // and unticks the curves rather than the captures
        if !captures.contains(&line.capture) {
            captures.push(line.capture);
        }
    }
    captures
}

/// Every key position any series has a point at, in axis order
///
/// # Arguments
///
/// * `series` - The series about to be drawn
fn group_keys(series: &[Series]) -> Vec<f64> {
    let mut keys: Vec<f64> = series
        .iter()
        .flat_map(|line| line.points.iter().map(|(key, _)| *key))
        .collect();
    // sorted and deduplicated, so two series measuring the same widths share a group rather than
    // each getting their own
    keys.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    keys.dedup_by(|left, right| (*left - *right).abs() < f64::EPSILON);
    keys
}

/// The label for each integer tick on a synthetic key axis
///
/// # Arguments
///
/// * `columns` - Every column the key axis has, from [`readout::columns`]
/// * `labels` - The capture labels, when the axis is a timeline
/// * `axis` - What the key axis is
fn key_ticks(columns: &[readout::Column], labels: &[String], axis: Axis) -> Vec<(f64, String)> {
    // the columns already know where each group is drawn, so a caption and the readout that reads
    // the same column cannot end up disagreeing about which key it stands for
    columns
        .iter()
        .map(|column| {
            (
                column.position,
                readout::column_label(axis, column.key, labels),
            )
        })
        .collect()
}

/// The label at one tick, or nothing when the tick is between two of them
///
/// # Arguments
///
/// * `ticks` - Every labelled position
/// * `value` - The position being labelled
fn tick_label(ticks: &[(f64, String)], value: f64) -> String {
    // only the positions a group actually sits at are labelled. the grid spacer asks about every
    // integer, and labelling the ones in between would put a caption under empty space
    ticks
        .iter()
        .find(|(position, _)| (position - value).abs() < 0.5)
        .map(|(_, text)| text.clone())
        .unwrap_or_default()
}

/// A grid that falls on whole numbers, which is where a synthetic axis's positions are
///
/// # Arguments
///
/// * `input` - The range the plot wants marks for
fn integer_spacer(input: egui_plot::GridInput) -> Vec<GridMark> {
    let (start, end) = input.bounds;
    let mut marks = Vec::new();
    // one mark per integer in view, capped so that zooming far out cannot ask for a million of them
    let first = start.floor() as i64;
    let last = end.ceil() as i64;
    if last.saturating_sub(first) > 1_000 {
        return marks;
    }
    for at in first..=last {
        marks.push(GridMark {
            value: at as f64,
            step_size: 1.0,
        });
    }
    marks
}

/// What a value axis is measured in, for a caption that has to say so in words
///
/// # Arguments
///
/// * `unit` - The unit to name
pub fn unit_name(unit: Unit) -> &'static str {
    // used by the caption under the chart, which says what the axis is before a reader has to
    // work it out from the tick labels
    match unit {
        Unit::QueryRate => "queries per second",
        Unit::RowRate => "rows per second",
        Unit::ByteRate => "bytes per second",
        Unit::Duration => "nanoseconds",
        Unit::Percent => "percent",
    }
}

/// Whether this metric is one a reader should expect to go up
///
/// # Arguments
///
/// * `metric` - The metric being drawn
pub fn direction(metric: &Metric) -> &'static str {
    // stated rather than left to be inferred from the shape of the line
    if metric.larger_is_better() {
        "higher is better"
    } else {
        "lower is better"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SweepAxis;

    /// The two channels a sweep leaves once the hue has been spent on the table
    #[test]
    fn a_sweep_puts_the_capture_in_the_dash_and_the_curve_in_the_marker() {
        let axis = Axis::Sweep(SweepAxis::ReadShare);
        // one table, one curve, two captures: one colour, no marker, two dash patterns. this is the
        // F30 regression - the styles used to walk the series list rather than the capture list
        let (first, marker) = stroke_of(axis, 0, 0);
        let (second, also) = stroke_of(axis, 1, 0);
        assert!(matches!(first, LineStyle::Solid));
        assert_ne!(first, second);
        assert_eq!((marker, also), (None, None));
        // and a second curve of the same table takes a marker while keeping the capture's dash, so
        // two captures of it are still told apart by the same channel as the first curve's
        assert_eq!(stroke_of(axis, 0, 1).0, first);
        assert_eq!(stroke_of(axis, 1, 1).0, second);
        assert!(stroke_of(axis, 0, 1).1.is_some());
    }

    /// A timeline has no capture to encode, so the mark carries both channels
    #[test]
    fn a_timeline_walks_the_markers_before_it_moves_the_dash() {
        let axis = Axis::Timeline;
        // every mark inside one cycle is the same dash and a different marker
        let cycle: Vec<(LineStyle, Option<MarkerShape>)> = (0..theme::MARK_CYCLE)
            .map(|at| stroke_of(axis, 0, at))
            .collect();
        assert!(cycle.iter().all(|(style, _)| *style == cycle[0].0));
        let mut markers: Vec<Option<MarkerShape>> = cycle.iter().map(|(_, at)| *at).collect();
        markers.dedup();
        assert_eq!(markers.len(), theme::MARK_CYCLE);
        // and the mark after it is the next dash with no marker again, rather than a repeat
        assert_ne!(stroke_of(axis, 0, theme::MARK_CYCLE).0, cycle[0].0);
        assert_eq!(stroke_of(axis, 0, theme::MARK_CYCLE).1, None);
    }

    /// A caption sits at the column the readout reads, because there is one definition of both
    #[test]
    fn a_caption_sits_where_the_readout_reads() {
        // two pieces of arithmetic would be two things to keep in step, and the failure - a column
        // captioned with its neighbour's key - is unreadable and looks exactly like a correct one
        let groups = [64.0, 1024.0, 4096.0];
        let labels: Vec<String> = Vec::new();
        for chart in [Chart::Line, Chart::Bars] {
            let columns = readout::columns(chart, &groups, 4, false);
            let ticks = key_ticks(&columns, &labels, Axis::Sweep(SweepAxis::RowBytes));
            assert_eq!(ticks.len(), columns.len());
            for (tick, column) in ticks.iter().zip(columns.iter()) {
                assert_eq!(tick.0, column.position);
                assert_eq!(tick.1, SweepAxis::RowBytes.format(column.key));
            }
        }
    }

    /// The capture position is what a sweep reads, and it is ignored on a timeline
    #[test]
    fn a_timeline_ignores_the_capture_position() {
        // every series on a timeline is a workload measured across the whole set of captures, so
        // there is no one capture for a dash pattern to stand for
        assert_eq!(
            stroke_of(Axis::Timeline, 0, 3),
            stroke_of(Axis::Timeline, 2, 3)
        );
    }
}
