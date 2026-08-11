//! How the end to end wall clock has moved, capture by capture
//!
//! Every capture is drawn as its median with the range of its runs around it, because the median
//! on its own is a number with no error bar and this layer's spread is wide enough to swallow most
//! of what it might be asked to measure. A reader comparing two captures should be comparing the
//! ranges, and the chart is drawn so that is the easy thing to do.

use anyhow::Result;
use plotters::prelude::*;

use super::palette;
use crate::fmt;

/// One capture's end to end result
#[derive(Debug, Clone)]
pub struct Point {
    /// The capture's name
    pub label: String,
    /// Its median wall clock, in nanoseconds
    pub median_ns: f64,
    /// The fastest and slowest run it took, if it ran more than once
    pub interval_ns: Option<(f64, f64)>,
}

/// Draws the wall clock of every capture, oldest first
///
/// # Arguments
///
/// * `points` - The captures to draw, in the order they were taken
/// * `reference` - A capture to draw a reference line at, usually the frozen baseline
pub fn draw(points: &[Point], reference: Option<&Point>) -> Result<String> {
    // nothing to draw is not a chart
    if points.is_empty() {
        anyhow::bail!("no macro captures to draw");
    }
    // the vertical range covers every run of every capture, not just the medians, plus a margin
    // so the extremes are not drawn on the frame
    let mut low = f64::MAX;
    let mut high = f64::MIN;
    for point in points {
        let (from, to) = point.interval_ns.unwrap_or((point.median_ns, point.median_ns));
        low = low.min(from);
        high = high.max(to);
    }
    let pad = ((high - low) * 0.15).max(high * 0.02);
    let low = (low - pad).max(0.0);
    let high = high + pad;
    let count = points.len();
    let aria = format!(
        "End to end wall clock for {count} captures, from {} to {}",
        fmt::duration_ns(low),
        fmt::duration_ns(high)
    );
    super::draw("chart-macro-wall-clock", &aria, 320, move |root| {
        let mut chart = ChartBuilder::on(root)
            .margin(16)
            .margin_right(24)
            .x_label_area_size(52)
            .y_label_area_size(78)
            .build_cartesian_2d(-0.5f64..(count as f64 - 0.5), low..high)?;
        // the x axis is categorical, so its labels are capture names rather than numbers
        let names: Vec<String> = points.iter().map(|point| point.label.clone()).collect();
        crate::themed_mesh!(chart)
            .x_labels(count)
            .x_label_formatter(&move |value: &f64| {
                // only label the ticks that land on a capture
                let index = value.round();
                if (value - index).abs() > 0.01 || index < 0.0 {
                    return String::new();
                }
                names.get(index as usize).cloned().unwrap_or_default()
            })
            .y_desc("wall clock")
            // capped, because plotters picks a tick count from the axis length and a duration
            // label is far wider than the numeral it sizes them for - left to itself it stacks
            // them close enough to touch
            .y_labels(6)
            .y_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        // the frozen baseline, drawn behind everything as the line the rest are read against
        if let Some(reference) = reference {
            let at = reference.median_ns;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(-0.5, at), (count as f64 - 0.5, at)],
                palette::ACCENT.mix(0.8).stroke_width(1),
            )))?;
            chart.draw_series(std::iter::once(Text::new(
                format!("{} median", reference.label),
                (-0.4, at),
                super::label_font(11),
            )))?;
        }
        // each capture's range, drawn as a bar so the overlap between two captures is the thing
        // the eye lands on
        for (index, point) in points.iter().enumerate() {
            let x = index as f64;
            if let Some((from, to)) = point.interval_ns {
                chart.draw_series(std::iter::once(Rectangle::new(
                    [(x - 0.12, from), (x + 0.12, to)],
                    palette::series(0).mix(0.35).filled(),
                )))?;
                // the ends, so a range that is narrower than the bar is still visible
                for edge in [from, to] {
                    chart.draw_series(std::iter::once(PathElement::new(
                        vec![(x - 0.12, edge), (x + 0.12, edge)],
                        palette::series(0).stroke_width(1),
                    )))?;
                }
            }
            // and the median on top of it, which is the number the tables quote
            chart.draw_series(std::iter::once(Circle::new(
                (x, point.median_ns),
                4,
                palette::series(0).filled(),
            )))?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a point
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    /// * `median` - Its median wall clock
    /// * `interval` - The range of its runs
    fn point(label: &str, median: f64, interval: Option<(f64, f64)>) -> Point {
        Point {
            label: label.to_string(),
            median_ns: median,
            interval_ns: interval,
        }
    }

    /// A chart of several captures draws, and names them all
    #[test]
    fn it_draws_every_capture() {
        let points = vec![
            point("B1-performance", 1.8e9, Some((1.7e9, 1.88e9))),
            point("o17-after", 1.83e9, Some((1.8e9, 1.9e9))),
        ];
        let svg = draw(&points, Some(&points[0])).expect("it draws");
        assert!(svg.contains("B1-performance"));
        assert!(svg.contains("o17-after"));
        assert!(!svg.contains("NaN"));
    }

    /// A capture that ran once has no range, and is drawn as its median alone
    #[test]
    fn a_single_run_capture_draws_without_a_range() {
        let points = vec![point("once", 1.0e9, None)];
        let svg = draw(&points, None).expect("it draws");
        assert!(!svg.contains("NaN"));
    }

    /// Every capture having the same wall clock does not collapse the axis to nothing
    #[test]
    fn an_unvarying_axis_does_not_collapse() {
        let points = vec![point("a", 1.0e9, None), point("b", 1.0e9, None)];
        let svg = draw(&points, None).expect("it draws");
        assert!(!svg.contains("NaN"), "a zero height axis produced NaN");
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&[], None).is_err());
    }

    /// The drawing is the same every time, since the page it lands in is checked
    #[test]
    fn it_is_deterministic() {
        let points = vec![point("a", 1.0e9, Some((0.9e9, 1.1e9)))];
        assert_eq!(
            draw(&points, None).expect("it draws"),
            draw(&points, None).expect("it draws")
        );
    }

    /// The chart is drawn at the shared width, so every chart on the page lines up
    #[test]
    fn it_uses_the_shared_width() {
        let points = vec![point("a", 1.0e9, None)];
        let svg = draw(&points, None).expect("it draws");
        assert!(svg.contains(&format!(r#"viewBox="0 0 {} 320""#, super::super::WIDTH)));
    }
}
