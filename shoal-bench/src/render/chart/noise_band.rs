//! What the micro layer can actually resolve
//!
//! Each point is one benchmark's spread across a set of identical repeats - the same code, the
//! same machine, the same everything, run several times. Whatever a point is above zero is what
//! that benchmark moves by when nothing has changed, which is the floor under any claim made about
//! it.
//!
//! The step lines are the tiers `shoal-bench compare` screens against. A point above the line for
//! its duration is a benchmark whose ordinary variation the band does not cover, and there are
//! some: the tiers are screens, not bounds, and this chart is the honest picture of how good a
//! screen they are.

use std::collections::BTreeMap;

use anyhow::Result;
use plotters::prelude::*;

use super::{legend, palette};
use crate::compare::micro::{FAST_THRESHOLD_NS, NOISE_FAST_PCT, NOISE_SLOW_PCT};
use crate::fmt;
use crate::model::micro::MicroCapture;

/// How tall the plotting area is, before the legend is added under it
const PLOT_HEIGHT: u32 = 360;

/// One group of identical repeats
#[derive(Debug, Clone)]
pub struct Group {
    /// What the group is called, usually the governor the repeats were taken under
    pub name: String,
    /// Each benchmark's mean duration and the spread it showed across the repeats
    pub points: Vec<(f64, f64)>,
}

/// Measures how far each benchmark moved across a set of identical repeats
///
/// The spread reported is the range between the fastest and slowest repeat, as a percentage of the
/// fastest - the same way the macro layer's `spread_pct` is measured, so the two mean the same
/// thing.
///
/// # Arguments
///
/// * `repeats` - The captures to compare against each other
pub fn spread(repeats: &[MicroCapture]) -> Vec<(f64, f64)> {
    // a spread needs at least two observations of the same thing
    if repeats.len() < 2 {
        return Vec::new();
    }
    // gather every repeat's measurement of each benchmark
    let mut seen: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for capture in repeats {
        for (name, stat) in &capture.benchmarks {
            seen.entry(name.as_str()).or_default().push(stat.mean_ns);
        }
    }
    let mut points = Vec::new();
    for (_, mut values) in seen {
        // only a benchmark every repeat measured can be compared across them
        if values.len() != repeats.len() {
            continue;
        }
        values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let fastest = values[0];
        let slowest = values[values.len() - 1];
        // a zero measurement is not a benchmark, and would be a division by zero
        if fastest <= 0.0 {
            continue;
        }
        points.push((fastest, (slowest - fastest) / fastest * 100.0));
    }
    points
}

/// Draws the observed spread of every benchmark against how long it takes
///
/// # Arguments
///
/// * `groups` - The repeat sets to draw
pub fn draw(groups: &[Group]) -> Result<String> {
    // nothing to draw is not a chart
    let total: usize = groups.iter().map(|group| group.points.len()).sum();
    if total == 0 {
        anyhow::bail!("no repeat captures to measure a spread from");
    }
    // the axes span everything drawn. the x axis is logarithmic because the benchmarks run from
    // twenty nanoseconds to a quarter of a millisecond, which is the whole point of the tiers.
    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut max_y = 0f64;
    for group in groups {
        for (duration, spread) in &group.points {
            min_x = min_x.min(*duration);
            max_x = max_x.max(*duration);
            max_y = max_y.max(*spread);
        }
    }
    let max_y = (max_y * 1.15).max(NOISE_FAST_PCT * 1.3);
    let aria = format!(
        "Observed spread of {total} benchmarks across identical repeats, against how long each \
         takes, with the {NOISE_FAST_PCT} and {NOISE_SLOW_PCT} percent screening tiers"
    );
    let groups: Vec<Group> = groups.to_vec();
    // one legend entry per set of repeats, in the order the colours were handed out
    let entries: Vec<legend::Entry> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| legend::Entry::new(group.name.clone(), palette::series(index)))
        .collect();
    let height = PLOT_HEIGHT + legend::height(&entries);
    super::draw("chart-noise-band", &aria, height, move |root| {
        // the plot, and the strip under it that says what each colour is
        let (area, strip) = root.split_vertically(PLOT_HEIGHT);
        let mut chart = ChartBuilder::on(&area)
            .margin(16)
            .margin_right(30)
            .x_label_area_size(48)
            .y_label_area_size(64)
            .build_cartesian_2d((min_x * 0.7..max_x * 1.4).log_scale(), 0f64..max_y)?;
        crate::themed_mesh!(chart)
            .x_desc("how long the benchmark takes")
            .x_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .y_desc("spread across identical repeats")
            .y_label_formatter(&|value: &f64| fmt::signed_pct(*value).replace('+', ""))
            .draw()?;
        // the tiers, drawn as the step they are: the band changes at one duration, and a reader
        // has to be able to see which side of it a point is on
        chart.draw_series(std::iter::once(PathElement::new(
            vec![
                (min_x * 0.7, NOISE_FAST_PCT),
                (FAST_THRESHOLD_NS, NOISE_FAST_PCT),
                (FAST_THRESHOLD_NS, NOISE_SLOW_PCT),
                (max_x * 1.4, NOISE_SLOW_PCT),
            ],
            palette::WARN.stroke_width(2),
        )))?;
        chart.draw_series(std::iter::once(Text::new(
            "screening tier".to_string(),
            (min_x * 0.8, NOISE_FAST_PCT * 1.06),
            super::label_font(11),
        )))?;
        // then each set of repeats, in its own colour, named in the legend below rather than
        // stacked at a fixed spot inside the plot where a dense cloud of points can reach them
        for (index, group) in groups.iter().enumerate() {
            let colour = palette::series(index);
            chart.draw_series(
                group
                    .points
                    .iter()
                    .map(|point| Circle::new(*point, 3, colour.mix(0.75).filled())),
            )?;
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

    /// The spread is the range across repeats, measured from the fastest of them
    #[test]
    fn the_spread_is_measured_from_the_fastest_repeat() {
        let repeats = vec![
            capture(&[("a", 100.0)]),
            capture(&[("a", 110.0)]),
            capture(&[("a", 105.0)]),
        ];
        let points = spread(&repeats);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].0, 100.0);
        assert!((points[0].1 - 10.0).abs() < 1e-9, "{:?}", points[0]);
    }

    /// A benchmark only some repeats measured cannot be compared across them
    #[test]
    fn a_benchmark_missing_from_a_repeat_is_skipped() {
        let repeats = vec![
            capture(&[("a", 100.0), ("b", 100.0)]),
            capture(&[("a", 110.0)]),
        ];
        let points = spread(&repeats);
        assert_eq!(points.len(), 1);
    }

    /// One capture is not a set of repeats and has no spread
    #[test]
    fn a_single_capture_has_no_spread() {
        assert!(spread(&[capture(&[("a", 100.0)])]).is_empty());
    }

    /// The chart draws every group and both tiers
    #[test]
    fn it_draws_the_groups_and_the_tiers() {
        let groups = vec![
            Group {
                name: "performance".to_string(),
                points: vec![(23.0, 4.6), (3_100.0, 1.8), (240_000.0, 1.2)],
            },
            Group {
                name: "powersave".to_string(),
                points: vec![(23.0, 8.8), (3_100.0, 3.8)],
            },
        ];
        let svg = draw(&groups).expect("it draws");
        assert!(svg.contains("performance"));
        assert!(svg.contains("powersave"));
        assert!(svg.contains("screening tier"));
        assert!(!svg.contains("NaN"));
    }

    /// Nothing to draw is an error rather than an empty chart
    #[test]
    fn nothing_to_draw_is_an_error() {
        assert!(draw(&[]).is_err());
        assert!(
            draw(&[Group {
                name: "empty".to_string(),
                points: Vec::new()
            }])
            .is_err()
        );
    }

    /// The real repeats in the tree produce the spread the documentation claims
    ///
    /// `docs/src/operations/performance-baseline.md` and the module docs on the comparison engine
    /// both quote figures taken from these eight files. This is what stops the chart, the band and
    /// the prose drifting apart.
    #[test]
    fn the_committed_repeats_reproduce_the_documented_spread() {
        let store = crate::store::Store::new(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("shoal-bench has a parent"),
        );
        let repeats = store.repeats().expect("the repeats are readable");
        assert_eq!(repeats.len(), 8, "the repeat corpus changed size");
        // the four taken under the performance governor, which is what the band was fitted to
        let performance: Vec<MicroCapture> = repeats
            .iter()
            .filter(|(name, _)| name.starts_with("B1-performance-repeat"))
            .map(|(_, capture)| capture.clone())
            .collect();
        assert_eq!(performance.len(), 4);
        let points = spread(&performance);
        assert!(!points.is_empty());
        // the documented worst case under one microsecond is 8.8%, so nothing there may exceed a
        // little over that. this is what would catch the repeats being replaced with a set that
        // no longer supports the tiers.
        let worst_fast = points
            .iter()
            .filter(|(duration, _)| *duration < 1_000.0)
            .map(|(_, spread)| *spread)
            .fold(0.0f64, f64::max);
        assert!(
            worst_fast <= 10.0,
            "the fast tier's worst observed spread is now {worst_fast}%, and the band is \
             {NOISE_FAST_PCT}%"
        );
    }
}
