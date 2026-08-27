//! What the chart holds at the place the reader is pointing
//!
//! # Every line, not the nearest one
//!
//! `egui_plot` answers a hover with the point nearest the cursor and nothing else, which is the
//! wrong answer to the question this explorer exists to ask. A reader hovering a curve wants to know
//! what the *other* captures did at that row width, and reading it off one line at a time means
//! moving the pointer between two numbers they are trying to compare.
//!
//! So the pointer selects a **column** - one position on the key axis - and the readout lists every
//! drawn line's value in it. Largest first, because the question is usually which is ahead; absent
//! last, and said as `absent`, because [`crate::plot`]'s standing rule is that a gap is never a
//! zero and a tooltip is the one place a reader cannot check that for themselves.
//!
//! # The numbers are the measurements, not the positions
//!
//! Every value here is read out of [`Series::points`], which holds what was measured. The log
//! toggles rescale what is *drawn* and never reach this, which is what stops a readout on a
//! logarithmic chart reporting four hundred thousand queries a second as `5.615`.
//!
//! # One definition of where a column is
//!
//! [`columns`] is the only place that knows where a group sits on the key axis, and both the tick
//! captions under the chart and the snapping here are built from it. Two definitions would be two
//! things to keep in step, and the failure would be a readout captioned with the neighbouring
//! column's key - which is unreadable and looks exactly like a correct one.

use egui_plot::PlotPoint;

use crate::fmt;
use crate::index::{Axis, Chart, Series, Unit};
use crate::theme::{self, Palette};

/// How near the pointer has to be to a column for it to be read, in points
///
/// A distance in points rather than in the key's own quantity, because the key axis is a row width
/// on one chart and a percentage on the next, and a reader zoomed in on two columns is pointing at
/// one of them however far apart their keys are.
const REACH: f32 = 48.0;

/// The most rows a readout draws before it says how many it left out
///
/// A selection of eighty arms across four captures is three hundred and twenty lines, and a tooltip
/// taller than the window is one a reader cannot read the bottom of.
const MAX_ROWS: usize = 24;

/// The side of the colour swatch that ties a row to its line, in points
const SWATCH: f32 = 10.0;

/// Where one group sits on the key axis, and what it stands for
///
/// The two are different numbers whenever the chart is not a plain linear line chart: a bar group
/// sits at a synthetic slot computed from its index, and a logarithmic key axis draws a row width of
/// 1024 at 3.01. The `key` is what the measurement recorded and the `position` is where it is drawn.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Column {
    /// Where it is drawn, in the space the chart is plotted in
    pub position: f64,
    /// The key it stands for, as the quantity the measurement recorded
    pub key: f64,
}

/// One line's value in the column being read
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// What the legend calls the line this came from
    pub name: String,
    /// The line's colour, as a position in the palette
    pub hue: u32,
    /// What it measured there, or nothing when it measured nothing there
    ///
    /// Stays an `Option` all the way to the text, for the reason [`crate::plot`] keeps it one: a
    /// measurement that was never taken is not a measurement of zero.
    pub value: Option<f64>,
}

/// Every column the key axis has, in axis order
///
/// # Arguments
///
/// * `chart` - Whether the positions are bar slots or line positions
/// * `groups` - The key positions, in axis order
/// * `series` - How many series are being drawn, which sets a bar group's width
/// * `log_x` - Whether the key axis is logarithmic
pub fn columns(chart: Chart, groups: &[f64], series: usize, log_x: bool) -> Vec<Column> {
    let mut columns = Vec::new();
    for (at, key) in groups.iter().enumerate() {
        let position = match chart {
            // a bar group's centre, which is the same arithmetic the bars themselves are laid out
            // with. a slot is an index and has no logarithm, so `log_x` does not reach it
            Chart::Bars => at as f64 * (series as f64 + 1.0) + (series as f64 - 1.0) / 2.0,
            // a line sits on its key, in whatever space the axis is drawn in
            Chart::Line => match log_x {
                true => key.log10(),
                false => *key,
            },
        };
        // a key a logarithm cannot take is not on this axis at all, the same way `plot::runs` breaks
        // a line at one rather than drawing it at some floor
        if !position.is_finite() {
            continue;
        }
        columns.push(Column {
            position,
            key: *key,
        });
    }
    columns
}

/// What one column is called, which is what its tick says and what the readout is headed with
///
/// # Arguments
///
/// * `axis` - What the key axis is
/// * `key` - The key the column stands for
/// * `labels` - The capture labels, when the axis is a timeline
pub fn column_label(axis: Axis, key: f64, labels: &[String]) -> String {
    match axis {
        // a sweep axis is a real quantity, written the way that quantity is normally written
        Axis::Sweep(sweep) => sweep.format(key),
        // a timeline's key *is* the position, so it indexes the labels directly
        Axis::Timeline => labels
            .get(key as usize)
            .cloned()
            .unwrap_or_else(|| format!("{key}")),
    }
}

/// The column nearest a position on the key axis, however far away it is
///
/// # Arguments
///
/// * `columns` - Every column the axis has
/// * `at` - The position being read, in the space the chart is plotted in
pub fn nearest(columns: &[Column], at: f64) -> Option<usize> {
    // nearest by drawn position rather than by key, because that is what the pointer is over. how
    // near is near enough is a question in points, and `hit` is where it is asked
    columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.position.is_finite())
        .min_by(|(_, left), (_, right)| {
            let left = (left.position - at).abs();
            let right = (right.position - at).abs();
            left.partial_cmp(&right).unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(at, _)| at)
}

/// The column the pointer is reading, if it is over the chart and near one
///
/// # Arguments
///
/// * `plot_ui` - The plot being built, which knows where the pointer is
/// * `columns` - Every column the axis has
pub fn hit(plot_ui: &egui_plot::PlotUi<'_>, columns: &[Column]) -> Option<usize> {
    // the pointer's position is answered whether or not it is over the plot, so being over it is
    // asked separately
    if !plot_ui.response().hovered() {
        return None;
    }
    let pointer = plot_ui.pointer_coordinate()?;
    let at = nearest(columns, pointer.x)?;
    let column = columns.get(at)?;
    // and how near it is, measured on the screen. in the key's own quantity `REACH` would mean one
    // thing on a sweep of row widths and another on a sweep of read shares
    let transform = plot_ui.transform();
    let column_at = transform.position_from_point(&PlotPoint::new(column.position, 0.0)).x;
    let pointer_at = transform.position_from_point(&PlotPoint::new(pointer.x, 0.0)).x;
    ((column_at - pointer_at).abs() <= REACH).then_some(at)
}

/// Every drawn line's value in one column, largest first and absent last
///
/// A line with no point at that key at all is still a row, and still an absent one: the reader
/// asked what is in this column, and *this capture never measured it here* is the answer for as
/// many of them as it is true of.
///
/// # Arguments
///
/// * `series` - The series being drawn
/// * `key` - The key the column stands for
pub fn rows(series: &[Series], key: f64) -> Vec<Row> {
    let mut rows: Vec<Row> = series
        .iter()
        .map(|line| Row {
            name: line.name.clone(),
            hue: line.hue,
            // the measurement, never the plotted position: the log toggles do not reach this
            value: line
                .points
                .iter()
                .find(|(at, _)| (at - key).abs() < f64::EPSILON)
                .and_then(|(_, value)| *value),
        })
        .collect();
    // largest first, which is the order the question is usually asked in, and absent last because
    // there is no value to rank it by. a stable sort, so two lines at one value keep the order the
    // chart drew them in
    rows.sort_by(|left, right| match (left.value, right.value) {
        (Some(left), Some(right)) => right.partial_cmp(&left).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    rows
}

/// Draws one readout
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `palette` - The palette the chart is being drawn in
/// * `unit` - What the values are in
/// * `header` - What the column is called
/// * `rows` - The lines' values in it, in the order they are to be listed
pub fn draw(ui: &mut egui::Ui, palette: &Palette, unit: Unit, header: &str, rows: &[Row]) {
    // what column this is, before any of the numbers in it
    ui.label(egui::RichText::new(header).strong());
    egui::Grid::new("shoal-top-readout")
        .num_columns(3)
        .spacing(egui::vec2(10.0, 2.0))
        .show(ui, |ui| {
            for row in rows.iter().take(MAX_ROWS) {
                // the swatch, painted rather than written: it is the only thing tying a row to the
                // line it came from, and a glyph would depend on the font having it
                let (rect, _) =
                    ui.allocate_exact_size(egui::vec2(SWATCH, SWATCH), egui::Sense::hover());
                ui.painter()
                    .rect_filled(rect, 2.0, theme::series(palette, row.hue as usize));
                ui.label(&row.name);
                match row.value {
                    // in the metric's own units, which is the same formatter the value axis's ticks
                    // are written with
                    Some(value) => ui.label(fmt::value(unit, value)),
                    // said as a word, in the colour of something deliberately de-emphasised. never
                    // a zero, and never a blank cell that would read as one
                    None => ui.colored_label(palette.muted, "absent"),
                };
                ui.end_row();
            }
        });
    // what was left out, rather than a tooltip that quietly stops at the twenty-fourth line
    if rows.len() > MAX_ROWS {
        ui.weak(format!("and {} more", rows.len() - MAX_ROWS));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SweepAxis;

    /// One drawn line, with only the fields a readout reads filled in
    ///
    /// # Arguments
    ///
    /// * `name` - What the legend calls it
    /// * `hue` - Its colour, as a position in the palette
    /// * `points` - Its points, as key against value
    fn line(name: &str, hue: u32, points: &[(f64, Option<f64>)]) -> Series {
        Series {
            name: name.to_string(),
            capture: 0,
            hue,
            mark: 0,
            points: points.to_vec(),
        }
    }

    /// A bar sits at its slot and a line sits on its key
    #[test]
    fn a_bar_group_and_a_line_are_at_different_positions() {
        let groups = [64.0, 1024.0, 4096.0];
        // a line is drawn at the quantity it measured
        let drawn = columns(Chart::Line, &groups, 4, false);
        assert_eq!(
            drawn.iter().map(|column| column.position).collect::<Vec<_>>(),
            vec![64.0, 1024.0, 4096.0]
        );
        // and a bar group at the centre of the slot its index buys it, which is the same
        // arithmetic `plot::draw_bars` lays the bars themselves out with
        let drawn = columns(Chart::Bars, &groups, 4, false);
        assert_eq!(
            drawn.iter().map(|column| column.position).collect::<Vec<_>>(),
            vec![1.5, 6.5, 11.5]
        );
        // either way the key is what was recorded, because that is what the readout is headed with
        assert_eq!(
            drawn.iter().map(|column| column.key).collect::<Vec<_>>(),
            vec![64.0, 1024.0, 4096.0]
        );
    }

    /// A logarithmic key axis draws a column at its logarithm, and drops one that has none
    #[test]
    fn a_logarithmic_axis_places_a_column_at_its_logarithm() {
        let drawn = columns(Chart::Line, &[100.0, 1000.0], 1, true);
        assert_eq!(
            drawn.iter().map(|column| column.position).collect::<Vec<_>>(),
            vec![2.0, 3.0]
        );
        // the key is untouched, so the caption still says `1,000` rather than `3`
        assert_eq!(drawn[1].key, 1000.0);
        // and a read share of zero - the one the corpus holds - has no logarithm and no position,
        // which is the same refusal `plot::runs` makes when it breaks a line rather than drawing a
        // floor that would read as a measurement
        assert_eq!(columns(Chart::Line, &[0.0, 100.0], 1, true).len(), 1);
    }

    /// The pointer reads the column it is nearest, in the space the chart is drawn in
    #[test]
    fn the_pointer_reads_the_column_it_is_nearest() {
        let drawn = columns(Chart::Line, &[0.0, 25.0, 50.0, 75.0], 1, false);
        assert_eq!(nearest(&drawn, 26.0), Some(1));
        assert_eq!(nearest(&drawn, 38.0), Some(2));
        // outside the columns entirely still answers with the nearest one: how near is near enough
        // is a question in points, and `hit` is the only place it is asked
        assert_eq!(nearest(&drawn, 900.0), Some(3));
        // and a chart with no columns has none to read
        assert_eq!(nearest(&[], 1.0), None);
    }

    /// Every line is a row, largest first, with the ones that measured nothing last
    #[test]
    fn a_readout_lists_every_line_largest_first_and_absent_last() {
        let series = [
            line("f27-row-sink · sorted", 1, &[(1024.0, Some(388_100.0))]),
            line("f28-rearchive · sorted", 1, &[(1024.0, Some(412_500.0))]),
            // measured this workload, and not at this width
            line("f27-row-sink · unsorted", 0, &[(1024.0, None)]),
            // has no point at this key at all, which is the same answer for the reader
            line("b1-performance · unsorted", 0, &[(64.0, Some(9.0))]),
        ];
        let rows = rows(&series, 1024.0);
        // every line drawn is a row, not only the ones with a number in this column
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec![
                "f28-rearchive · sorted",
                "f27-row-sink · sorted",
                "f27-row-sink · unsorted",
                "b1-performance · unsorted",
            ]
        );
        // the two absent ones stay absent. a zero here is a measurement, and neither of these is
        assert_eq!(rows[2].value, None);
        assert_eq!(rows[3].value, None);
        // and the colour comes off the line, so a row can be tied back to what drew it
        assert_eq!(rows[0].hue, 1);
    }

    /// The column is headed with what its tick says
    #[test]
    fn a_column_is_headed_the_way_its_tick_is() {
        // a sweep axis is a quantity, written the way that quantity is written
        assert_eq!(
            column_label(Axis::Sweep(SweepAxis::RowBytes), 1024.0, &[]),
            "1 KiB"
        );
        // a timeline's key is a position, so it names the capture standing at it
        let labels = ["f27-row-sink".to_string(), "f28-rearchive".to_string()];
        assert_eq!(column_label(Axis::Timeline, 1.0, &labels), "f28-rearchive");
    }

    /// The label the chart drew before this module existed, kept as the evidence on item 86
    #[test]
    fn the_label_this_replaces_read_a_logarithmic_chart_in_decades() {
        // four hundred thousand queries a second, on a chart with `log value axis` on, is plotted
        // at its logarithm - and `egui_plot`'s own formatter prints the plotted number
        let plotted = 412_500f64.log10();
        let drawn = egui_plot::default_label_formatter(&egui_plot::HoverPosition::NearDataPoint {
            // empty, because only a series' first run carries its name and every run after a gap
            // is drawn unnamed so the legend keeps one entry per series
            plot_name: "",
            position: PlotPoint::new(3.0, plotted),
            index: 0,
        })
        .unwrap_or_default();
        assert_eq!(drawn, "\nx = 3.000\ny = 5.615");
        // what replaces it is read out of the measurement rather than off the axis, so no log
        // toggle can reach it, and it is written in the metric's own unit
        let series = [line("f28-rearchive", 0, &[(3.0, Some(412_500.0))])];
        let rows = rows(&series, 3.0);
        assert_eq!(rows[0].value, Some(412_500.0));
        assert_eq!(
            fmt::value(Unit::QueryRate, rows[0].value.unwrap_or_default()),
            "412,500/s"
        );
    }
}
