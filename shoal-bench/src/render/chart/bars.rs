//! One measurement across a handful of named things, as grouped bars
//!
//! A sweep chart wants a numeric axis. Three of the questions the grid answers do not have one:
//! four kinds of table are not a number, three key distributions are not a number, and neither is
//! "read" against "write". Drawing those as a line implies an ordering between the categories that
//! does not exist, which is the most common way a chart says something its data does not.
//!
//! # Groups and series
//!
//! A **group** is one category on the x axis - one table kind, one distribution. A **series** is
//! one bar within every group - one row width, one operation. Every group carries the same series
//! in the same order, so a colour means the same thing across the whole chart, and the series are
//! named in a [`legend`](super::legend) under the plot rather than inside it.

use anyhow::{Result, bail};
use plotters::prelude::*;

use super::sweep::Unit;
use super::{legend, palette};

/// How many series a group may hold before the chart stops being readable
const MAX_SERIES: usize = 6;

/// How many groups fit across the canvas
const MAX_GROUPS: usize = 8;

/// How tall the plotting area is, before the legend is added under it
///
/// The same 372 the plot had when the legend was a 28 unit strip above it, so moving the names
/// below the plot changed where they are and nothing about the bars.
const PLOT_HEIGHT: u32 = 372;

/// One bar in every group
#[derive(Debug, Clone)]
pub struct Series {
    /// What this bar is, named in the strip above the plot
    pub name: String,
    /// Its value in each group, in the same order the groups are given
    ///
    /// `None` where a group has no measurement for this series, which is drawn as a gap rather
    /// than as a zero - a workload that was never run is not a workload that took no time.
    pub values: Vec<Option<f64>>,
}

/// Everything a bar chart needs that is not its data
#[derive(Debug, Clone)]
pub struct Spec {
    /// The element id, which must be unique within a page
    pub id: String,
    /// What the categories are
    pub x_desc: String,
    /// What the bars measure
    pub y_desc: String,
    /// How values are written
    pub unit: Unit,
}

/// Draws grouped bars
///
/// # Arguments
///
/// * `spec` - What the axes are and how to write them
/// * `groups` - The categories, in the order they appear along the x axis
/// * `series` - The bars within each group, in the order they take colours
pub fn draw(spec: &Spec, groups: &[String], series: &[Series]) -> Result<String> {
    // nothing to draw is not a chart, and an empty one reads as a measurement of zero
    if groups.is_empty() || series.is_empty() {
        bail!("{} has no bars to draw", spec.id);
    }
    if groups.len() > MAX_GROUPS {
        bail!(
            "{} has {} groups, which is more than the {MAX_GROUPS} the canvas holds",
            spec.id,
            groups.len()
        );
    }
    // every series must cover every group, or a bar would be drawn against the wrong category
    for line in series {
        if line.values.len() != groups.len() {
            bail!(
                "{} carries {} values for {} groups",
                line.name,
                line.values.len(),
                groups.len()
            );
        }
    }
    let shown: Vec<Series> = series.iter().take(MAX_SERIES).cloned().collect();
    let dropped = series.len().saturating_sub(shown.len());
    // the tallest bar sets the axis. bars start at zero, always: a bar chart with a truncated
    // baseline exaggerates every difference on it by however much was cut off
    let mut high = f64::MIN;
    for line in &shown {
        for value in line.values.iter().flatten() {
            high = high.max(*value);
        }
    }
    if high <= 0.0 {
        bail!("{} has no positive value to draw", spec.id);
    }
    let aria = format!(
        "{} by {}, {} groups of {} bars, the largest {}",
        spec.y_desc,
        spec.x_desc,
        groups.len(),
        shown.len(),
        spec.unit.format(high)
    );
    // one legend entry per series, in the order the colours were handed out
    let entries: Vec<legend::Entry> = shown
        .iter()
        .enumerate()
        .map(|(index, line)| legend::Entry::new(line.name.clone(), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    let spec = spec.clone();
    let groups = groups.to_vec();
    super::draw(&spec.id.clone(), &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (plot, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&plot)
            .margin(16)
            .x_label_area_size(42)
            .y_label_area_size(84)
            // the x axis is a plain index, offset by half a step so that a whole number falls at
            // the middle of a group. a segmented axis would put a category's name at the boundary
            // between two groups rather than under the bars it names
            .build_cartesian_2d(-0.5f64..groups.len() as f64 - 0.5, 0f64..high * 1.15)?;
        let unit = spec.unit;
        crate::themed_mesh!(chart)
            .disable_x_mesh()
            .x_desc(spec.x_desc.clone())
            .x_labels(groups.len())
            .x_label_formatter(&move |value: &f64| {
                // only the whole numbers name a group; plotters may ask for values between them
                let slot = value.round();
                if (value - slot).abs() > 0.01 || slot < 0.0 {
                    return String::new();
                }
                groups.get(slot as usize).cloned().unwrap_or_default()
            })
            .y_desc(spec.y_desc.clone())
            .y_label_formatter(&move |value: &f64| unit.format(*value))
            .draw()?;
        // each group is one unit wide, with a margin either side and the bars sharing what is left
        let count = shown.len() as f64;
        let margin = 0.12;
        let bar = (1.0 - margin * 2.0) / count;
        for (index, line) in shown.iter().enumerate() {
            let colour = palette::series(index);
            let bars: Vec<Rectangle<(f64, f64)>> = line
                .values
                .iter()
                .enumerate()
                .filter_map(|(group, value)| {
                    // an absent value is a gap, not a zero height bar sitting on the axis
                    let value = (*value)?;
                    let left = group as f64 - 0.5 + margin + bar * index as f64;
                    Some(Rectangle::new(
                        [(left, 0.0), (left + bar * 0.86, value)],
                        colour.filled(),
                    ))
                })
                .collect();
            chart.draw_series(bars)?;
        }
        // a chart that dropped a series says so, or it looks complete and is not
        if dropped > 0 {
            chart.draw_series(std::iter::once(Text::new(
                format!("{dropped} further series not drawn"),
                (-0.45, high * 1.08),
                super::label_font(10),
            )))?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::{Series, Spec, draw};
    use crate::render::chart::sweep::Unit;

    /// A spec for the tests
    fn spec() -> Spec {
        Spec {
            id: "chart-test-bars".to_string(),
            x_desc: "table".to_string(),
            y_desc: "p50".to_string(),
            unit: Unit::Duration,
        }
    }

    /// Every group and every series is drawn and named
    #[test]
    fn it_draws_every_group_and_names_every_series() {
        let svg = draw(
            &spec(),
            &["unsorted".to_string(), "sorted".to_string()],
            &[
                Series {
                    name: "read".to_string(),
                    values: vec![Some(100.0), Some(140.0)],
                },
                Series {
                    name: "write".to_string(),
                    values: vec![Some(900.0), Some(1100.0)],
                },
            ],
        )
        .expect("it draws");
        assert!(svg.contains("read"));
        assert!(svg.contains("write"));
        assert!(svg.contains("unsorted"));
        assert!(!svg.contains("NaN"));
    }

    /// A missing measurement is a gap rather than a bar of zero height
    ///
    /// A zero would say the workload took no time, which is the opposite of what a workload that
    /// was never run measured.
    #[test]
    fn an_absent_value_is_not_drawn_as_zero() {
        let with_gap = draw(
            &spec(),
            &["a".to_string(), "b".to_string()],
            &[Series {
                name: "read".to_string(),
                values: vec![Some(100.0), None],
            }],
        )
        .expect("it draws");
        let with_zero = draw(
            &spec(),
            &["a".to_string(), "b".to_string()],
            &[Series {
                name: "read".to_string(),
                values: vec![Some(100.0), Some(0.0)],
            }],
        )
        .expect("it draws");
        assert_ne!(with_gap, with_zero, "a gap drew the same as a zero");
    }

    /// A series that does not cover every group is refused
    ///
    /// Silently padding it would draw a bar under the wrong category, which is a chart that is
    /// wrong rather than one that is incomplete.
    #[test]
    fn a_ragged_series_is_refused() {
        let err = draw(
            &spec(),
            &["a".to_string(), "b".to_string()],
            &[Series {
                name: "read".to_string(),
                values: vec![Some(100.0)],
            }],
        )
        .expect_err("a ragged series must not pass");
        assert!(format!("{err}").contains("carries 1 values for 2 groups"));
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&spec(), &[], &[]).is_err());
        assert!(
            draw(
                &spec(),
                &["a".to_string()],
                &[Series {
                    name: "read".to_string(),
                    values: vec![None]
                }]
            )
            .is_err()
        );
    }

    /// More groups than the canvas holds is an error rather than an unreadable chart
    #[test]
    fn too_many_groups_is_an_error() {
        let groups: Vec<String> = (0..12).map(|index| format!("g{index}")).collect();
        let values = vec![Some(1.0); groups.len()];
        let err = draw(
            &spec(),
            &groups,
            &[Series {
                name: "read".to_string(),
                values,
            }],
        )
        .expect_err("too many groups must not pass");
        assert!(format!("{err}").contains("more than the 8"));
    }

    /// The same input draws the same chart, byte for byte
    #[test]
    fn drawing_is_deterministic() {
        let groups = ["a".to_string(), "b".to_string()];
        let series = [Series {
            name: "read".to_string(),
            values: vec![Some(1.0), Some(2.0)],
        }];
        assert_eq!(
            draw(&spec(), &groups, &series).expect("it draws"),
            draw(&spec(), &groups, &series).expect("it draws")
        );
    }
}
