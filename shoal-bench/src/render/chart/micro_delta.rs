//! What every micro benchmark did, against a baseline
//!
//! Bars diverge from zero and are sorted by how far they moved, so the chart reads top to bottom
//! from the biggest improvement to the worst regression - the same order the comparison table
//! uses, for the same reason.
//!
//! The noise band is drawn *behind* each bar, per row, at whatever width that row's tier gives it.
//! A bar that does not escape its own shading is not a result, and drawing the band per row rather
//! than as one global pair of lines is what makes that readable when the tier changes partway down
//! the chart.
//!
//! The diverging pair is blue and red rather than green and red. Green against red is the one
//! categorical pairing that fails outright for the commonest colour vision deficiencies, and this
//! chart's entire meaning is which side of zero a bar falls on.

use anyhow::Result;
use plotters::prelude::*;

use super::palette;
use crate::compare::micro::MicroRow;
use crate::fmt;

/// How tall each benchmark's row is drawn
const ROW_HEIGHT: u32 = 15;

/// Draws every benchmark's movement against a baseline
///
/// # Arguments
///
/// * `rows` - The comparison's rows, already sorted
/// * `baseline` - What the comparison was against, for the description
pub fn draw(rows: &[MicroRow], baseline: &str) -> Result<String> {
    // nothing to draw is not a chart
    if rows.is_empty() {
        anyhow::bail!("no benchmarks to draw");
    }
    // the horizontal range covers every bar and every band, so nothing is drawn off the frame
    let widest = rows
        .iter()
        .map(|row| row.pct.abs().max(row.band_pct))
        .fold(0.0f64, f64::max);
    let extent = (widest * 1.15).max(1.0);
    let count = rows.len();
    let significant = rows.iter().filter(|row| row.significant).count();
    let height = ROW_HEIGHT * count as u32 + 76;
    let aria = format!(
        "Change in {count} micro benchmarks against {baseline}, of which {significant} moved \
         outside the noise band"
    );
    // owned, because the drawing closure outlives this frame
    let rows: Vec<MicroRow> = rows.to_vec();
    super::draw("chart-micro-delta", &aria, height, move |root| {
        let mut chart = ChartBuilder::on(root)
            .margin(12)
            .margin_right(30)
            .x_label_area_size(40)
            .y_label_area_size(300)
            .build_cartesian_2d(-extent..extent, -0.5f64..(count as f64 - 0.5))?;
        // the y axis is one benchmark per row, labelled with its name
        let names: Vec<String> = rows.iter().map(|row| row.name.clone()).collect();
        crate::themed_mesh!(chart)
            .disable_y_mesh()
            .y_labels(count)
            .y_label_formatter(&move |value: &f64| {
                // only label the ticks that land on a benchmark
                let index = value.round();
                if (value - index).abs() > 0.01 || index < 0.0 {
                    return String::new();
                }
                names.get(index as usize).cloned().unwrap_or_default()
            })
            .x_desc("change against the baseline")
            .x_label_formatter(&|value: &f64| fmt::signed_pct(*value))
            .draw()?;
        for (index, row) in rows.iter().enumerate() {
            let y = index as f64;
            // the band this row was judged against, drawn behind the bar
            chart.draw_series(std::iter::once(Rectangle::new(
                [(-row.band_pct, y - 0.5), (row.band_pct, y + 0.5)],
                palette::MUTED.mix(0.16).filled(),
            )))?;
            // and the bar itself, coloured by direction and only when it means something
            let colour = if !row.significant {
                palette::MUTED
            } else if row.pct > 0.0 {
                palette::WORSE
            } else {
                palette::BETTER
            };
            chart.draw_series(std::iter::once(Rectangle::new(
                [(0.0, y - 0.32), (row.pct, y + 0.32)],
                colour.filled(),
            )))?;
            // a significant row is also labelled, so the chart can be read without the table
            if row.significant {
                let at = if row.pct > 0.0 {
                    row.pct + extent * 0.01
                } else {
                    row.pct - extent * 0.09
                };
                chart.draw_series(std::iter::once(Text::new(
                    fmt::signed_pct(row.pct),
                    (at, y + 0.2),
                    super::label_font(10),
                )))?;
            }
        }
        // zero, drawn last so it sits on top of the bars that cross it
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(0.0, -0.5), (0.0, count as f64 - 0.5)],
            palette::AXIS.stroke_width(1),
        )))?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a row
    ///
    /// # Arguments
    ///
    /// * `name` - The benchmark's name
    /// * `pct` - How far it moved
    /// * `band` - The band it was judged against
    fn row(name: &str, pct: f64, band: f64) -> MicroRow {
        MicroRow {
            name: name.to_string(),
            baseline_ns: 100.0,
            run_ns: 100.0 * (1.0 + pct / 100.0),
            delta_ns: pct,
            pct,
            band_pct: band,
            significant: pct.abs() > band,
        }
    }

    /// Every benchmark gets a row, and the significant ones are labelled
    #[test]
    fn it_draws_a_row_per_benchmark() {
        let rows = vec![
            row("a/faster", -20.0, 9.0),
            row("b/quiet", 1.0, 9.0),
            row("c/slower", 33.49, 9.0),
        ];
        let svg = draw(&rows, "B1-performance").expect("it draws");
        for name in ["a/faster", "b/quiet", "c/slower"] {
            assert!(svg.contains(name), "{name} is missing");
        }
        // the two that moved are labelled with their change; the quiet one is not
        assert!(svg.contains("-20.00%"));
        assert!(svg.contains("+33.49%"));
        assert!(!svg.contains("+1.00%"));
        assert!(!svg.contains("NaN"));
    }

    /// A run where nothing moved still draws, and draws nothing but bands
    #[test]
    fn a_quiet_run_still_draws() {
        let rows = vec![row("a", 0.0, 9.0), row("b", 0.0, 5.0)];
        let svg = draw(&rows, "B1-performance").expect("it draws");
        assert!(!svg.contains("NaN"));
    }

    /// The description says how many benchmarks moved, for a reader who cannot see the chart
    #[test]
    fn the_description_counts_what_moved() {
        let rows = vec![row("a", -20.0, 9.0), row("b", 1.0, 9.0)];
        let svg = draw(&rows, "trailing").expect("it draws");
        assert!(svg.contains("2 micro benchmarks against trailing"), "{}", &svg[..300]);
        assert!(svg.contains("1 moved outside"));
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&[], "B1-performance").is_err());
    }

    /// The drawing is the same every time
    #[test]
    fn it_is_deterministic() {
        let rows = vec![row("a", -20.0, 9.0)];
        assert_eq!(
            draw(&rows, "b").expect("it draws"),
            draw(&rows, "b").expect("it draws")
        );
    }
}
