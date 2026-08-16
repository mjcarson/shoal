//! How each operation's cost grows with the size of the partition
//!
//! Both axes are logarithmic, which is what makes the shape of the growth readable rather than
//! the magnitude: on a log-log chart a cost proportional to the partition size is a straight line
//! at 45 degrees, a cost independent of it is flat, and anything between the two is visible as a
//! slope without anybody having to fit a curve to it.
//!
//! This is the chart that answers "does this scan the partition or seek into it", which is a
//! different question from "is this fast", and one the noise band has no bearing on.

use std::collections::BTreeMap;

use anyhow::Result;
use plotters::prelude::*;

use super::{legend, palette};
use crate::fmt;
use crate::model::micro::MicroCapture;

/// How many operation families are drawn before the rest are folded away
const MAX_FAMILIES: usize = 8;

/// How tall the plotting area is, before the legend is added under it
const PLOT_HEIGHT: u32 = 420;

/// One operation measured at several partition sizes
#[derive(Debug, Clone)]
pub struct Family {
    /// The benchmark id with its size suffix removed
    pub name: String,
    /// Each size measured, and what it cost, sorted by size
    pub points: Vec<(f64, f64)>,
}

/// Groups a capture's benchmarks into families measured at several sizes
///
/// A benchmark whose id does not end in a size, or that was only measured at one size, is not part
/// of a scaling story and is left out rather than drawn as a dot.
///
/// # Arguments
///
/// * `capture` - The capture to group
pub fn families(capture: &MicroCapture) -> Vec<Family> {
    // group by the id with the trailing size removed
    let mut grouped: BTreeMap<String, Vec<(f64, f64)>> = BTreeMap::new();
    for (name, stat) in &capture.benchmarks {
        // the size is the last path segment, when it is a number
        let Some((prefix, tail)) = name.rsplit_once('/') else {
            continue;
        };
        let Ok(size) = tail.parse::<f64>() else {
            continue;
        };
        grouped
            .entry(prefix.to_string())
            .or_default()
            .push((size, stat.mean_ns));
    }
    // keep only the families that were measured at more than one size
    let mut families: Vec<Family> = grouped
        .into_iter()
        .filter(|(_, points)| points.len() > 1)
        .map(|(name, mut points)| {
            // sorted by size, so the line is drawn left to right
            points.sort_by(|left, right| {
                left.0.partial_cmp(&right.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            Family { name, points }
        })
        .collect();
    // the most expensive first, so the eye lands on the lines that matter and the tail is what
    // gets folded away if there are too many
    families.sort_by(|left, right| {
        let cost = |family: &Family| {
            family
                .points
                .last()
                .map(|(_, ns)| *ns)
                .unwrap_or_default()
        };
        cost(right)
            .partial_cmp(&cost(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
    });
    families
}

/// Draws how each operation's cost grows with the partition size
///
/// # Arguments
///
/// * `families` - The operations to draw
pub fn draw(families: &[Family]) -> Result<String> {
    // nothing to draw is not a chart
    if families.is_empty() {
        anyhow::bail!("no benchmark families were measured at more than one size");
    }
    // a chart with too many lines says nothing, so the cheap tail is dropped rather than drawn
    let shown: Vec<Family> = families.iter().take(MAX_FAMILIES).cloned().collect();
    let dropped = families.len().saturating_sub(shown.len());
    // both axes span everything drawn, padded multiplicatively because they are logarithmic
    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut min_y = f64::MAX;
    let mut max_y = f64::MIN;
    for family in &shown {
        for (size, cost) in &family.points {
            min_x = min_x.min(*size);
            max_x = max_x.max(*size);
            // a zero or negative cost cannot be placed on a log axis, and is not a measurement
            if *cost > 0.0 {
                min_y = min_y.min(*cost);
                max_y = max_y.max(*cost);
            }
        }
    }
    // guard the case where nothing had a positive cost, which would leave the axis unbuildable
    if min_y > max_y {
        anyhow::bail!("no benchmark family had a positive cost to draw");
    }
    let aria = format!(
        "Cost of {} operations against partition size, from {} to {}, both axes logarithmic",
        shown.len(),
        fmt::duration_ns(min_y),
        fmt::duration_ns(max_y)
    );
    // one legend entry per family, in the order the colours were handed out
    let entries: Vec<legend::Entry> = shown
        .iter()
        .enumerate()
        .map(|(index, family)| legend::Entry::new(family.name.clone(), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    super::draw("chart-micro-scaling", &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (area, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&area)
            .margin(16)
            // no gutter is reserved on the right any more, because nothing is drawn out there
            .margin_right(24)
            .x_label_area_size(46)
            .y_label_area_size(78)
            .build_cartesian_2d(
                (min_x * 0.8..max_x * 1.25).log_scale(),
                (min_y * 0.7..max_y * 1.4).log_scale(),
            )?;
        crate::themed_mesh!(chart)
            .x_desc("rows in the partition")
            .x_label_formatter(&|value: &f64| crate::fmt::thousands(value.round() as u128))
            .y_desc("mean time")
            .y_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        // one line per operation, named in the legend rather than at its own end - two of the real
        // families end within 2% of each other at 4096 rows, which is a collision no amount of
        // pushing labels apart along the axis ever made readable
        for (index, family) in shown.iter().enumerate() {
            let colour = palette::series(index);
            chart.draw_series(LineSeries::new(
                family.points.iter().copied(),
                colour.stroke_width(2),
            ))?;
            chart.draw_series(
                family
                    .points
                    .iter()
                    .map(|point| Circle::new(*point, 3, colour.filled())),
            )?;
        }
        // and a note when the cheap tail was left out, so the chart cannot look complete
        if dropped > 0 {
            chart.draw_series(std::iter::once(Text::new(
                format!("{dropped} cheaper families not drawn"),
                (min_x, min_y * 0.75),
                super::label_font(10),
            )))?;
        }
        legend::draw(&strip, &entries)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::micro::{MICRO_VERSION, MicroStat};

    /// Builds a capture from `(name, mean_ns)` pairs
    ///
    /// # Arguments
    ///
    /// * `entries` - What the capture measured
    fn capture(entries: &[(&str, f64)]) -> MicroCapture {
        let benchmarks: BTreeMap<String, MicroStat> = entries
            .iter()
            .map(|(name, mean)| {
                (
                    name.to_string(),
                    MicroStat {
                        mean_ns: *mean,
                        lower_ns: *mean,
                        upper_ns: *mean,
                        median_ns: *mean,
                    },
                )
            })
            .collect();
        MicroCapture {
            version: MICRO_VERSION,
            captured: "2026-08-09T00:00:00Z".to_string(),
            benchmarks,
        }
    }

    /// Benchmarks sharing a prefix and differing in size become one family
    #[test]
    fn a_family_is_one_operation_at_several_sizes() {
        let capture = capture(&[
            ("a/insert/16", 100.0),
            ("a/insert/256", 170.0),
            ("a/insert/4096", 180.0),
        ]);
        let families = families(&capture);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "a/insert");
        // sorted by size, whatever order the capture held them in
        assert_eq!(
            families[0].points,
            vec![(16.0, 100.0), (256.0, 170.0), (4096.0, 180.0)]
        );
    }

    /// A benchmark measured at only one size is not a scaling story
    #[test]
    fn a_single_size_is_not_a_family() {
        let capture = capture(&[("a/insert/16", 100.0), ("b/one_key", 50.0)]);
        assert!(families(&capture).is_empty());
    }

    /// A benchmark whose last segment is not a number is left out
    #[test]
    fn an_unsized_benchmark_is_left_out() {
        let capture = capture(&[
            ("seek_bytes/new/one_key", 54.0),
            ("seek_bytes/new/range", 103.0),
        ]);
        assert!(families(&capture).is_empty());
    }

    /// The most expensive families come first, so the tail is what gets dropped
    #[test]
    fn families_are_ordered_by_cost() {
        let capture = capture(&[
            ("cheap/16", 10.0),
            ("cheap/256", 20.0),
            ("dear/16", 1000.0),
            ("dear/256", 2000.0),
        ]);
        let families = families(&capture);
        assert_eq!(families[0].name, "dear");
        assert_eq!(families[1].name, "cheap");
    }

    /// A family per line, each labelled at its end
    #[test]
    fn it_draws_and_labels_every_family() {
        let capture = capture(&[
            ("a/insert/16", 100.0),
            ("a/insert/256", 170.0),
            ("b/get_all/16", 500.0),
            ("b/get_all/256", 12000.0),
        ]);
        let svg = draw(&families(&capture)).expect("it draws");
        assert!(svg.contains("a/insert"));
        assert!(svg.contains("b/get_all"));
        assert!(!svg.contains("NaN"));
    }

    /// More families than the chart can carry are folded away, and the chart says so
    #[test]
    fn a_long_tail_is_dropped_and_declared() {
        let mut entries = Vec::new();
        let names: Vec<String> = (0..12).map(|index| format!("f{index}")).collect();
        for (index, name) in names.iter().enumerate() {
            entries.push((format!("{name}/16"), 10.0 * (index as f64 + 1.0)));
            entries.push((format!("{name}/256"), 20.0 * (index as f64 + 1.0)));
        }
        let refs: Vec<(&str, f64)> = entries
            .iter()
            .map(|(name, cost)| (name.as_str(), *cost))
            .collect();
        let svg = draw(&families(&capture(&refs))).expect("it draws");
        assert!(svg.contains("4 cheaper families not drawn"), "the tail was not declared");
    }

    /// A cost of zero cannot be placed on a log axis and does not produce a broken chart
    #[test]
    fn a_zero_cost_does_not_break_the_log_axis() {
        let all_zero = capture(&[("a/16", 0.0), ("a/256", 0.0)]);
        assert!(draw(&families(&all_zero)).is_err());
        // and one real point alongside a zero still draws
        let one_real = capture(&[("a/16", 0.0), ("a/256", 100.0)]);
        let svg = draw(&families(&one_real)).expect("it draws");
        assert!(!svg.contains("NaN"));
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&[]).is_err());
    }
}
