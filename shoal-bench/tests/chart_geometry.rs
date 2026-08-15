//! The charts drawn from the real artifacts have sane geometry
//!
//! plotters is built here without a font backend, so it *estimates* text extents rather than
//! measuring them. The failure that causes is a label that overflows its gutter or lands on top of
//! its neighbour, and it is invisible in every other kind of test: the SVG is well formed, every
//! colour is a sentinel, and the numbers are right. Only the layout is wrong.
//!
//! These tests are not a substitute for looking at the page. They catch the two things that can be
//! checked without eyes - a mark drawn outside the canvas, and two labels drawn on top of each
//! other - over the charts built from the artifacts actually committed to this tree.

use std::path::Path;

use shoal_bench::compare::micro::{self, NoiseBand};
use shoal_bench::render::chart::{self, WIDTH};
use shoal_bench::store::Store;

/// Opens a store on the real repository
fn repo() -> Store {
    Store::new(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("shoal-bench has a parent"),
    )
}

/// The height a chart was drawn at, read back out of its viewBox
///
/// # Arguments
///
/// * `svg` - The chart
fn height(svg: &str) -> f64 {
    let at = svg.find("viewBox=\"0 0 ").expect("every chart has a viewBox");
    let rest = &svg[at + 13..];
    let end = rest.find('"').expect("the viewBox is terminated");
    rest[..end]
        .split_whitespace()
        .nth(1)
        .expect("the viewBox has a height")
        .parse()
        .expect("the height is a number")
}

/// Every `(x, y)` a text element was drawn at, with the text
///
/// # Arguments
///
/// * `svg` - The chart
fn texts(svg: &str) -> Vec<(f64, f64, String)> {
    let mut found = Vec::new();
    let mut at = 0;
    // walk every text element, pulling its anchor and its content
    while let Some(next) = svg[at..].find("<text ") {
        let start = at + next;
        let end = svg[start..].find("</text>").map(|e| start + e).unwrap_or(svg.len());
        let element = &svg[start..end];
        let attr = |name: &str| -> Option<f64> {
            let needle = format!("{name}=\"");
            let at = element.find(&needle)? + needle.len();
            let stop = element[at..].find('"')? + at;
            element[at..stop].parse().ok()
        };
        // the body sits after the opening tag, on its own line
        let body = element
            .find('>')
            .map(|tag| element[tag + 1..].trim().to_string())
            .unwrap_or_default();
        if let (Some(x), Some(y)) = (attr("x"), attr("y")) {
            found.push((x, y, body));
        }
        at = end + 1;
    }
    found
}

/// Builds every chart this tree has the artifacts for
fn every_chart() -> Vec<(String, String)> {
    let store = repo();
    let mut charts = Vec::new();
    // the wall clock of every capture that produced a macro artifact, per workload
    //
    // the page draws one chart per workload rather than one chart of everything, because a
    // workload over 200,000 rows and one over 2,000 on the same axis makes both unreadable
    let mut per_workload: std::collections::BTreeMap<String, Vec<chart::macro_wall_clock::Point>> =
        std::collections::BTreeMap::new();
    for label in store.labels().expect("labels are readable") {
        if let Some((_, capture)) = store.resolve_macro(&label).expect("macro reads") {
            for (id, workload) in &capture.workloads {
                per_workload.entry(id.clone()).or_default().push(
                    chart::macro_wall_clock::Point {
                        label: label.clone(),
                        median_ns: workload.median_wall_clock_ns() as f64,
                        interval_ns: workload
                            .wall_clock_interval_ns()
                            .map(|(low, high)| (low as f64, high as f64)),
                    },
                );
            }
        }
    }
    let points: Vec<chart::macro_wall_clock::Point> =
        per_workload.into_values().next().unwrap_or_default();
    if !points.is_empty() {
        charts.push((
            "macro_wall_clock".to_string(),
            chart::macro_wall_clock::draw(&points, points.first()).expect("it draws"),
        ));
    }
    // the encryption sweeps, from whichever capture holds them. these are the only charts that
    // pair workloads inside one capture rather than across two, so a capture taken before the
    // sweeps existed simply yields nothing and they are skipped
    for label in store.labels().expect("labels are readable") {
        let Some((_, capture)) = store.resolve_macro(&label).expect("macro reads") else {
            continue;
        };
        if !chart::encryption::pairs(&capture, chart::encryption::DEPTH_SWEEP).is_empty() {
            charts.push((
                "encryption_by_row".to_string(),
                chart::encryption::draw_by_row(&capture).expect("it draws"),
            ));
            charts.push((
                "encryption_by_depth".to_string(),
                chart::encryption::draw_by_depth(&capture).expect("it draws"),
            ));
            charts.push((
                "encryption_absolute".to_string(),
                chart::encryption::draw_absolute(&capture).expect("it draws"),
            ));
        }
        if !chart::encryption::pairs(&capture, chart::encryption::CLIENT_SWEEP).is_empty() {
            charts.push((
                "encryption_by_clients".to_string(),
                chart::encryption::draw_by_clients(&capture).expect("it draws"),
            ));
        }
    }
    // the current capture against the frozen baseline, which is the widest label gutter of any
    // chart on the page and therefore the one most likely to collide
    let (_, frozen) = store
        .resolve_micro(shoal_bench::store::FROZEN_BASELINE)
        .expect("the frozen baseline resolves");
    let (_, trailing) = store
        .resolve_micro(shoal_bench::store::TRAILING_BASELINE)
        .expect("the trailing baseline resolves");
    let comparison = micro::compare(&trailing, &frozen, &NoiseBand::default());
    charts.push((
        "micro_delta".to_string(),
        chart::micro_delta::draw(&comparison.rows, "B1-performance").expect("it draws"),
    ));
    // and how the cost scales, whose labels sit in the right hand margin
    let families = chart::micro_scaling::families(&trailing);
    charts.push((
        "micro_scaling".to_string(),
        chart::micro_scaling::draw(&families).expect("it draws"),
    ));
    // the profile, whose scope names are the longest strings anywhere on the page
    for label in store.labels().expect("labels are readable") {
        let path = store.run_artifact(&label, shoal_bench::registry::Layer::Hotpath);
        if path.is_file() {
            let profile = store.read_hotpath(&path).expect("the profile reads");
            charts.push((
                format!("hotpath_scopes/{label}"),
                chart::hotpath_scopes::draw(&profile).expect("it draws"),
            ));
            break;
        }
    }
    // and the stage breakdowns, whose legend is laid out by hand and is therefore the part of any
    // chart here most likely to collide with itself
    for label in store.labels().expect("labels are readable") {
        let path = store.run_artifact(&label, shoal_bench::registry::Layer::Stages);
        if path.is_file() {
            let report = store.read_stages(&path).expect("the report reads");
            for op in ["insert", "get"] {
                if let Ok(svg) = chart::stages_stacked::draw(&report, op) {
                    charts.push((format!("stages_{op}/{label}"), svg));
                }
            }
            break;
        }
    }
    charts
}

/// Nothing is drawn outside the canvas it was drawn on
///
/// A label that overflows is a label the reader cannot see, and with estimated text extents that
/// is a thing that happens rather than a thing that cannot.
#[test]
fn nothing_is_drawn_far_outside_the_canvas() {
    // labels legitimately sit a little proud of the plotting area, so this is a bound on gross
    // overflow rather than a claim that nothing crosses the edge
    const SLACK: f64 = 8.0;
    for (name, svg) in every_chart() {
        let tall = height(&svg);
        for (x, y, text) in texts(&svg) {
            assert!(
                x >= -SLACK && x <= f64::from(WIDTH) + SLACK,
                "{name}: label {text:?} is drawn at x={x}, outside 0..{WIDTH}"
            );
            assert!(
                y >= -SLACK && y <= tall + SLACK,
                "{name}: label {text:?} is drawn at y={y}, outside 0..{tall}"
            );
        }
    }
}

/// No two labels are drawn on top of each other
///
/// Two labels sharing an anchor are two labels the reader sees as one smear. This checks the axis
/// gutters, where the labels are densest and where a row height that is too small shows up first.
#[test]
fn no_two_labels_share_an_anchor() {
    for (name, svg) in every_chart() {
        let mut anchors: Vec<(f64, f64, String)> = texts(&svg)
            .into_iter()
            .filter(|(_, _, text)| !text.is_empty())
            .collect();
        // sorted so that any two labels at the same spot are adjacent
        anchors.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(left.1.partial_cmp(&right.1).unwrap_or(std::cmp::Ordering::Equal))
        });
        for pair in anchors.windows(2) {
            let (left, right) = (&pair[0], &pair[1]);
            let same_spot = (left.0 - right.0).abs() < 0.5 && (left.1 - right.1).abs() < 0.5;
            assert!(
                !same_spot || left.2 == right.2,
                "{name}: {:?} and {:?} are both drawn at ({}, {})",
                left.2,
                right.2,
                left.0,
                left.1
            );
        }
    }
}

/// Labels stacked in one column are far enough apart to read
///
/// The comparison chart draws 59 benchmarks and the profile draws twelve scopes, each one label
/// per row. If a row height is ever reduced, or plotters is left to choose a tick count for an
/// axis whose labels are wider than the numerals it sizes them for, the labels in that column
/// start touching before anything else goes wrong.
///
/// Grouped by the x they are anchored at, because that is what a column is. Two labels close in y
/// but far apart in x - an axis tick and an annotation out in the plotting area - do not collide,
/// and treating them as though they did would make this test fire on charts that are fine.
#[test]
fn stacked_labels_have_room() {
    use std::collections::BTreeMap;

    for (name, svg) in every_chart() {
        // gather each column of labels, keyed by the x they share
        let mut columns: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
        for (x, y, text) in texts(&svg) {
            if !text.is_empty() {
                columns.entry(x.round() as i64).or_default().push(y);
            }
        }
        for (x, mut column) in columns {
            column.sort_by(|left, right| {
                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
            });
            column.dedup_by(|left, right| (*left - *right).abs() < 0.01);
            // nine user units is smaller than the smallest font any chart uses
            for pair in column.windows(2) {
                assert!(
                    pair[1] - pair[0] >= 9.0,
                    "{name}: two labels in the column at x={x} are {} apart at y={}, which is \
                     closer than the text is tall",
                    pair[1] - pair[0],
                    pair[0]
                );
            }
        }
    }
}

/// Every chart drawn from the real artifacts is well formed
#[test]
fn every_chart_is_well_formed() {
    for (name, svg) in every_chart() {
        assert!(svg.starts_with("<svg class=\"shoal-chart\""), "{name}");
        assert!(svg.trim_end().ends_with("</svg>"), "{name}");
        assert!(!svg.contains("NaN"), "{name} contains a NaN coordinate");
        // and the opening tag is balanced by exactly one close
        assert_eq!(svg.matches("<svg").count(), 1, "{name}");
        assert_eq!(svg.matches("</svg>").count(), 1, "{name}");
    }
}
