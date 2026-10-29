//! Many figures of one thing against one baseline, as ratios on a log axis centred on 1×
//!
//! What [the store comparison](../stores.rs)'s headline draws: a row for every arm, and in each
//! row a dot for every other store at its median over Shoal's median, with a whisker from its
//! lowest run over Shoal's highest to its highest run over Shoal's lowest. The whisker crosses 1×
//! exactly when the two run intervals overlap, which is when the verdict table says *within*, so
//! the chart and the table cannot disagree. A log axis, because twice as fast and half as fast are
//! the same distance from 1×.
//!
//! # Labels beside the plot, placed by arithmetic
//!
//! plotters has no categorical axis for a value axis it also has to log, so the rows are a plain
//! index and their names are drawn in a column of their own to the left, each at its row's centre,
//! which the fixed row pitch makes a sum rather than a guess.

use anyhow::{Result, bail};
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use super::{legend, palette};

/// How tall one row is, in user units
const ROW: u32 = 18;

/// How wide the column of row names is
const LABEL_WIDTH: u32 = 150;

/// The margin round the plot
const MARGIN: u32 = 12;

/// The room under the plot for the x axis's labels and description
const X_AREA: u32 = 44;

/// How many rows one chart holds before it is two charts
const MAX_ROWS: usize = 40;

/// One store's ratio on one row: its median, and its whisker's ends
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Whisker {
    /// The lowest run over the baseline's highest
    pub low: f64,
    /// The median over the baseline's median
    pub mid: f64,
    /// The highest run over the baseline's lowest
    pub high: f64,
}

/// One row: a name, and each series' whisker on it, where it has one
#[derive(Debug, Clone)]
pub struct Row {
    /// What the row is
    pub label: String,
    /// Each series' whisker, in the order the series are given
    pub whiskers: Vec<Option<Whisker>>,
}

/// One series: a name and the colour it is drawn in
#[derive(Debug, Clone)]
pub struct Series {
    /// What it is
    pub name: String,
    /// Its colour, a palette sentinel
    pub colour: RGBColor,
}

/// Everything a ratio chart needs that is not its data
#[derive(Debug, Clone)]
pub struct Spec {
    /// The element id, unique within a page
    pub id: String,
    /// What a ratio is of: `ops/s over Shoal's`
    pub x_desc: String,
}

/// Draws a ratio chart
///
/// # Arguments
///
/// * `spec` - What the axis is
/// * `series` - The series, in the order every row's whiskers are given
/// * `rows` - The rows, top to bottom
pub fn draw(spec: &Spec, series: &[Series], rows: &[Row]) -> Result<String> {
    // nothing to draw is not a chart, and a chart past its rows would crush them
    if rows.is_empty() || series.is_empty() {
        bail!("{} has nothing to draw", spec.id);
    }
    if rows.len() > MAX_ROWS {
        bail!("{} has {} rows, more than the {MAX_ROWS} one chart holds", spec.id, rows.len());
    }
    if let Some(row) = rows.iter().find(|row| row.whiskers.len() != series.len()) {
        bail!("{}: row {} has {} whiskers for {} series", spec.id, row.label, row.whiskers.len(), series.len());
    }
    // the axis spans every whisker and 1×, with room either side; a ratio is positive or nothing
    let mut low: f64 = 1.0;
    let mut high: f64 = 1.0;
    for whisker in rows.iter().flat_map(|row| row.whiskers.iter().flatten()) {
        if whisker.low > 0.0 && whisker.high > 0.0 {
            low = low.min(whisker.low);
            high = high.max(whisker.high);
        }
    }
    let (low, high) = (low * 0.8, high * 1.25);
    let entries: Vec<legend::Entry> = series.iter().map(|line| legend::Entry::new(line.name.clone(), line.colour)).collect();
    let plot = rows.len() as u32 * ROW;
    let main = MARGIN + plot + X_AREA + MARGIN;
    let height = main + legend::height(&entries);
    let aria = format!(
        "{} for {} rows and {} series, from {:.2}× to {:.2}×",
        spec.x_desc,
        rows.len(),
        series.len(),
        low / 0.8,
        high / 1.25
    );
    let spec = spec.clone();
    let series = series.to_vec();
    let rows = rows.to_vec();
    super::draw(&spec.id.clone(), &aria, height, move |root| {
        // the names on the left, the plot beside them, and the legend under both
        let (body, strip) = root.split_vertically(main);
        let (names, area) = body.split_horizontally(LABEL_WIDTH);
        let count = rows.len() as f64;
        let mut chart = ChartBuilder::on(&area)
            .margin(MARGIN)
            .margin_right(24)
            .x_label_area_size(X_AREA)
            .y_label_area_size(0)
            .build_cartesian_2d((low..high).log_scale(), 0f64..count)?;
        crate::themed_mesh!(chart)
            .disable_y_mesh()
            .y_labels(0)
            .x_desc(spec.x_desc.clone())
            .x_label_formatter(&|value: &f64| ratio_label(*value))
            .draw()?;
        // 1×, where a store is as fast as Shoal
        chart.draw_series(std::iter::once(PathElement::new(vec![(1.0, 0.0), (1.0, count)], palette::ACCENT.stroke_width(1))))?;
        // each row's whiskers, side by side within the row so none hides another
        let lanes = series.len() as f64;
        for (at, row) in rows.iter().enumerate() {
            let centre = count - at as f64 - 0.5;
            for (lane, (whisker, line)) in row.whiskers.iter().zip(&series).enumerate() {
                let Some(whisker) = whisker.filter(|whisker| whisker.low > 0.0 && whisker.mid > 0.0) else {
                    continue;
                };
                let y = centre + (lane as f64 - (lanes - 1.0) / 2.0) * (0.7 / lanes.max(1.0));
                chart.draw_series(std::iter::once(PathElement::new(
                    vec![(whisker.low, y), (whisker.high, y)],
                    line.colour.stroke_width(2),
                )))?;
                chart.draw_series(std::iter::once(Circle::new((whisker.mid, y), 3, line.colour.filled())))?;
            }
        }
        // every row's name, right aligned against the plot at its centre
        let font = super::label_font(11).pos(Pos::new(HPos::Right, VPos::Center));
        for (at, row) in rows.iter().enumerate() {
            let y = (MARGIN + at as u32 * ROW + ROW / 2) as i32;
            names.draw(&Text::new(row.label.clone(), ((LABEL_WIDTH - 6) as i32, y), font.clone()))?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

/// A ratio as an axis writes it: `0.25×`, `1×`, `4×`
///
/// # Arguments
///
/// * `value` - The ratio
fn ratio_label(value: f64) -> String {
    // whole numbers plainly, the rest to as few places as tell them apart
    if (value - value.round()).abs() < 1e-9 {
        format!("{}×", value.round())
    } else if value >= 1.0 {
        format!("{value:.1}×")
    } else {
        format!("{value:.2}×")
    }
}

#[cfg(test)]
mod tests {
    use super::{draw, ratio_label, Row, Series, Spec, Whisker};
    use crate::render::chart::palette;

    /// Two series over three rows, one row missing a series
    fn chart() -> String {
        let series = [
            Series { name: "Ceph RGW".to_string(), colour: palette::series(1) },
            Series { name: "MinIO".to_string(), colour: palette::series(2) },
        ];
        let whisker = |mid: f64| Some(Whisker { low: mid * 0.9, mid, high: mid * 1.1 });
        let rows = [
            Row { label: "put 4 KiB c1".to_string(), whiskers: vec![whisker(0.5), whisker(2.0)] },
            Row { label: "get 4 KiB c1".to_string(), whiskers: vec![whisker(1.0), None] },
            Row { label: "put 1 MiB c16".to_string(), whiskers: vec![whisker(4.0), whisker(0.25)] },
        ];
        draw(&Spec { id: "chart-test-ratio".to_string(), x_desc: "ops/s over Shoal's".to_string() }, &series, &rows).expect("it draws")
    }

    /// Every row is named once and every series in the legend, with nothing drawn as NaN
    #[test]
    fn it_names_every_row_and_series() {
        let svg = chart();
        for name in ["put 4 KiB c1", "get 4 KiB c1", "put 1 MiB c16", "Ceph RGW", "MinIO"] {
            assert_eq!(svg.matches(name).count(), 1, "{name}");
        }
        assert!(!svg.contains("NaN") && !svg.contains("inf"));
        // and only the palette's sentinels are drawn
        for hex in svg.match_indices('#').filter_map(|(at, _)| svg.get(at..at + 7)) {
            if hex.chars().skip(1).all(|ch| ch.is_ascii_hexdigit()) {
                assert!(palette::all_hex().contains(&hex.to_string()), "{hex} is not a sentinel");
            }
        }
    }

    /// The same rows draw the same chart
    #[test]
    fn drawing_is_deterministic() {
        assert_eq!(chart(), chart());
    }

    /// A row with a whisker too few is refused, as is a chart of nothing
    #[test]
    fn a_short_row_is_refused() {
        let series = [Series { name: "a".to_string(), colour: palette::series(0) }];
        let rows = [Row { label: "x".to_string(), whiskers: vec![] }];
        assert!(draw(&Spec { id: "x".to_string(), x_desc: "x".to_string() }, &series, &rows).is_err());
        assert!(draw(&Spec { id: "x".to_string(), x_desc: "x".to_string() }, &series, &[]).is_err());
    }

    /// A ratio is written as few places as tell it apart
    #[test]
    fn ratios_are_written_short() {
        assert_eq!(ratio_label(1.0), "1×");
        assert_eq!(ratio_label(4.0), "4×");
        assert_eq!(ratio_label(1.5), "1.5×");
        assert_eq!(ratio_label(0.25), "0.25×");
    }
}
