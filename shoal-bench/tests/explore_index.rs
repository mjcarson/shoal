//! The explorer's index describes the real corpus, and describes it the same way twice
//!
//! These tests project the committed artifacts rather than a fixture. A fixture would let the
//! projection drift away from the corpus without anything noticing, which is the same failure
//! `committed_artifacts.rs` exists to stop.
//!
//! One of them is doing something less obvious than the rest. `shoal_top::fmt` is a **copy** of
//! five functions from `shoal_bench::fmt`, because the explorer cannot link this crate - `walkdir`
//! does not build for `wasm32-unknown-unknown`. Two implementations of one rendering is a fork
//! waiting to happen, and `formatters_agree` is the only thing standing between them.

use shoal_bench::explore;
use shoal_bench::model::macro_layer::MacroCaptureV2;
use shoal_bench::render::arms::{self, Arm};
use shoal_bench::render::family::FAMILIES;
use shoal_bench::store::Store;
use shoal_top::index::{Axis, Index, Layer, Metric, Percentile, Selection, Source, SweepAxis};
use shoal_top::preset::Preset;

/// Opens a store on the real repository this test is compiled inside
fn repo() -> Store {
    // the manifest directory is `shoal-bench/`, so the repository is one level above it
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench is a workspace member, so it has a parent")
        .to_path_buf();
    Store::new(root)
}

/// Projects the committed corpus
fn index() -> Index {
    explore::project(&repo()).expect("the committed corpus projects")
}

/// The capture a preset opens on, as an index and as the artifact the pages are drawn from
///
/// The two halves have to be the same capture or the comparison below is between a chart of one
/// and a chart of another.
///
/// # Arguments
///
/// * `index` - The projection to read the newest macro capture out of
fn newest_macro(index: &Index) -> (u32, MacroCaptureV2) {
    let at = index
        .newest_with(Layer::Macro)
        .expect("the corpus has a capture with a macro layer");
    let label = &index.captures[at as usize].label;
    let store = repo();
    let capture = store
        .read_macro(&store.run_artifact(label, shoal_bench::registry::Layer::Macro))
        .expect("the capture the index named is readable");
    (at, capture)
}

/// Checks that a preset draws the arms a page draws, as one curve per table
///
/// # Arguments
///
/// * `preset` - The preset under test
/// * `expected` - The arms the generated page selects, by the page's own filter
fn reproduces(preset: &Preset, expected: &[Arm<'_>]) {
    let index = index();
    let (capture, _) = newest_macro(&index);
    // the arms the preset selects, as identifiers, which is what the two halves join on
    let mut selected: Vec<&str> = (preset.select)(&index, capture)
        .into_iter()
        .map(|at| index.workloads[at as usize].id.as_str())
        .collect();
    let mut wanted: Vec<&str> = expected.iter().map(|arm| arm.id).collect();
    selected.sort_unstable();
    wanted.sort_unstable();
    assert!(!wanted.is_empty(), "the page this reproduces draws nothing");
    assert_eq!(selected, wanted, "the preset does not select the page's arms");
    // and draws them as the page draws them: one curve per table, not one line through all four
    let selection = Selection {
        captures: vec![capture],
        workloads: (preset.select)(&index, capture),
        metric: preset.metric.clone(),
        axis: preset.axis,
    };
    let series = index.series(&selection);
    let kinds = arms::table_kinds(expected);
    assert_eq!(
        series.len(),
        kinds.len(),
        "expected one curve per table, got {:?}",
        series.iter().map(|line| line.name.as_str()).collect::<Vec<_>>()
    );
    for kind in &kinds {
        // every curve is named after the one thing that separates it from the others
        let drawn = series
            .iter()
            .find(|line| line.name.ends_with(kind.as_str()))
            .unwrap_or_else(|| panic!("no curve was drawn for {kind}"));
        let arms = expected
            .iter()
            .filter(|arm| arm.table_kind() == Some(kind.as_str()))
            .count();
        assert_eq!(drawn.points.len(), arms, "{kind} has the wrong arms");
        // and drawn in the colour that table's slot in `TABLE_ORDER` reserves for it, which is what
        // makes a hue mean the table rather than the curve's position among the ones ticked
        let slot = shoal_top::index::TABLE_ORDER
            .iter()
            .position(|known| known == kind)
            .unwrap_or_else(|| panic!("{kind} is not a table the explorer reserves a colour for"));
        assert_eq!(drawn.hue as usize, slot, "{kind} was not drawn in its own colour");
        // one curve per table, so nothing carries a marker: a chart making no second distinction
        // must not look as though it is
        assert_eq!(drawn.mark, 0, "{kind} was marked for a distinction nothing is making");
    }
}

#[test]
fn two_workloads_the_facts_cannot_tell_apart_are_two_lines() {
    let index = index();
    let (capture, _) = newest_macro(&index);
    // the resident and archived gets are one workload struct with a `residency` field that reaches
    // no `ScaleFacts`, so every entry of the curve key agrees and both sit at the same width
    let pair: Vec<u32> = ["macro/get_resident", "macro/get_archived"]
        .iter()
        .filter_map(|id| {
            index
                .workloads
                .iter()
                .position(|workload| workload.id == *id)
                .map(|at| at as u32)
        })
        .filter(|at| index.macro_point(capture, *at).is_some())
        .collect();
    assert_eq!(pair.len(), 2, "the corpus no longer carries both keyed gets");
    // and the facts really do agree, which is the premise of everything below
    let facts: Vec<&shoal_top::index::ScaleFactsLite> = pair
        .iter()
        .map(|at| {
            let point = index.macro_point(capture, *at).expect("filtered on above");
            &index.scales[point.scale as usize]
        })
        .collect();
    assert_eq!(facts[0], facts[1], "the two no longer record the same facts");
    let series = index.series(&Selection {
        captures: vec![capture],
        // a latency, because neither of these counts queries and so neither answers a rate
        metric: Metric::Latency {
            op: "get".to_string(),
            percentile: Percentile::P99,
        },
        workloads: pair,
        axis: Axis::Sweep(SweepAxis::RowBytes),
    });
    // two workloads, two lines. one line through both of them would pass through two unrelated
    // measurements at one position on the axis, and mean nothing at either
    assert_eq!(
        series.len(),
        2,
        "the two were folded into one line: {:?}",
        series.iter().map(|line| line.name.as_str()).collect::<Vec<_>>()
    );
    for line in &series {
        assert!(
            line.name.contains("macro/get_"),
            "a split line is named by the workload that separates it, not {}",
            line.name
        );
    }
    // neither records a table, and two curves that name no table are not known to have anything in
    // common, so they do not share a colour
    assert_ne!(series[0].hue, series[1].hue);
}

#[test]
fn the_corpus_projects_and_is_not_empty() {
    let index = index();
    // an index that projected nothing would pass every other test in this file
    assert_eq!(index.version, shoal_top::index::INDEX_VERSION);
    assert!(
        index.captures.len() >= 20,
        "only {} captures projected",
        index.captures.len()
    );
    assert!(
        index.macro_points.len() > 1_000,
        "only {} measurements projected",
        index.macro_points.len()
    );
}

#[test]
fn every_workload_is_explained_by_a_family() {
    let index = index();
    // the same totality `family_for` already has, asserted through the projection. a workload no
    // family claims is drawn with a warning, so this failing is a documentation gap rather than a
    // crash - which is exactly why it needs a test rather than a panic
    let orphans: Vec<&str> = index
        .workloads
        .iter()
        .filter(|workload| workload.family.is_none())
        .map(|workload| workload.id.as_str())
        .collect();
    assert!(orphans.is_empty(), "no family explains {orphans:?}");
}

#[test]
fn the_four_blocks_are_carried_whole() {
    let index = index();
    // every declared family reaches the index
    assert_eq!(index.families.len(), FAMILIES.len());
    for (projected, declared) in index.families.iter().zip(FAMILIES) {
        assert_eq!(projected.name, declared.name);
        // copied, never retyped. a truncation or a reword here would be a silent downgrade of the
        // one thing on a results page that a chart cannot carry
        assert_eq!(projected.what_it_measures, declared.what_it_measures);
        assert_eq!(projected.how_to_read_it, declared.how_to_read_it);
        assert_eq!(
            projected.what_would_make_it_wrong,
            declared.what_would_make_it_wrong
        );
        assert_eq!(projected.what_it_cannot_say, declared.what_it_cannot_say);
        // and none of them is empty, which is the property F18 enforces on the pages
        assert!(!projected.what_it_cannot_say.is_empty());
    }
}

#[test]
fn an_absent_measurement_is_absent_rather_than_zero() {
    let index = index();
    // the corpus is sparse: far fewer measurements exist than captures times workloads
    let possible = index.captures.len() * index.workloads.len();
    assert!(
        index.macro_points.len() < possible,
        "the corpus is not sparse, so this test proves nothing"
    );
    // and every pair that was not measured has no point at all, rather than a zeroed one
    for point in &index.macro_points {
        if let Some(rate) = point.ops_per_sec {
            assert!(rate > 0.0, "a measured query rate of zero was recorded");
        }
        if let Some(wall) = point.wall_clock_ns {
            assert!(wall > 0, "a measured wall clock of zero was recorded");
        }
    }
    // a capture with a macro layer answers for some workloads and not others, and the ones it did
    // not answer for return `None` rather than a default
    let with_macro = index
        .captures
        .iter()
        .position(|capture| capture.layers.contains(&Layer::Macro))
        .expect("some capture measured a macro layer");
    let missing = (0..index.workloads.len() as u32)
        .find(|workload| index.macro_point(with_macro as u32, *workload).is_none());
    assert!(
        missing.is_some(),
        "every workload was measured by one capture, so the gap path is untested"
    );
}

#[test]
fn projecting_twice_produces_the_same_bytes() {
    // the determinism rule the generated pages keep, applied to the index. every map walked is a
    // `BTreeMap` and every vector is explicitly sorted, and this is what says so
    let first = serde_json::to_string(&index()).expect("the index serializes");
    let second = serde_json::to_string(&index()).expect("the index serializes");
    assert_eq!(first, second);
}

#[test]
fn measurements_are_sorted_for_the_binary_search() {
    let index = index();
    // `Index::macro_point` binary searches on exactly this pair, so an unsorted vector would not
    // fail loudly - it would silently fail to find measurements that are there
    let mut previous: Option<(u32, u32)> = None;
    for point in &index.macro_points {
        let key = (point.capture, point.workload);
        if let Some(before) = previous {
            assert!(before < key, "{before:?} came before {key:?}");
        }
        previous = Some(key);
    }
    // and the lookup finds what the vector holds
    for point in index.macro_points.iter().take(50) {
        let found = index
            .macro_point(point.capture, point.workload)
            .expect("a measurement that is in the vector is found by the lookup");
        assert_eq!(found.workload, point.workload);
    }
}

#[test]
fn the_layer_mirror_is_total() {
    // the explorer's `Layer` is a copy of this crate's, so the two are asserted to agree here
    // rather than left to drift
    let names = [
        (Layer::Micro, shoal_bench::registry::Layer::Micro),
        (Layer::Macro, shoal_bench::registry::Layer::Macro),
        (Layer::Hotpath, shoal_bench::registry::Layer::Hotpath),
        (Layer::Stages, shoal_bench::registry::Layer::Stages),
    ];
    for (mirrored, original) in names {
        // the serialized name of each is the file name infix of every artifact, so they must match
        let json = serde_json::to_string(&mirrored).expect("a layer serializes");
        assert_eq!(json.trim_matches('"'), original.as_str());
    }
    assert_eq!(names.len(), shoal_bench::registry::Layer::ALL.len());
}

#[test]
fn formatters_agree() {
    // every branch of each of the five copied functions, over inputs chosen to land in each band
    let durations = [
        0.0, 1.5, 999.4, 1_000.0, 12_345.6, 999_999.0, 1_000_000.0, 5_500_000.0,
        999_999_999.0, 1_000_000_000.0, 2_500_000_000.0, -0.001, f64::NAN, f64::INFINITY,
    ];
    for value in durations {
        assert_eq!(
            shoal_top::fmt::duration_ns(value),
            shoal_bench::fmt::duration_ns(value),
            "duration_ns disagreed on {value}"
        );
    }
    let counts: [u128; 8] = [0, 1, 999, 1_000, 1_001, 999_999, 1_000_000, 123_456_789];
    for value in counts {
        assert_eq!(
            shoal_top::fmt::thousands(value),
            shoal_bench::fmt::thousands(value),
            "thousands disagreed on {value}"
        );
    }
    let sizes = [
        0.0, 1.0, 1023.0, 1024.0, 4096.0, 102_400.0, 1_048_576.0, 1_073_741_824.0,
        5_000_000_000.0, f64::NAN,
    ];
    for value in sizes {
        assert_eq!(
            shoal_top::fmt::bytes_axis(value),
            shoal_bench::fmt::bytes_axis(value),
            "bytes_axis disagreed on {value}"
        );
        assert_eq!(
            shoal_top::fmt::byte_rate(value),
            shoal_bench::fmt::byte_rate(value),
            "byte_rate disagreed on {value}"
        );
    }
    let fixings = [(1.005, 2), (-0.001, 2), (0.0, 0), (99.95, 1), (-12.5, 0)];
    for (value, places) in fixings {
        assert_eq!(
            shoal_top::fmt::fixed(value, places),
            shoal_bench::fmt::fixed(value, places),
            "fixed disagreed on {value} at {places}"
        );
    }
}

#[test]
fn the_explorer_crate_is_wired_the_way_the_lockfile_needs() {
    // `shoal-bench` must enter `shoal-top` with `default-features = false`, or the runner grows an
    // egui tree and `cargo tree -p shoal-bench --no-default-features` stops being clean. That is a
    // manifest property, so nothing else in the test suite can notice it breaking
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("this crate has a manifest");
    let entry = text
        .lines()
        .find(|line| line.trim_start().starts_with("shoal-top ="))
        .expect("shoal-bench depends on shoal-top");
    assert!(
        entry.contains("default-features = false"),
        "shoal-top must be entered without its default features: {entry}"
    );
}

#[test]
fn the_grid_preset_reproduces_chart_grid_throughput() {
    let index = index();
    let (_, capture) = newest_macro(&index);
    // the filter `render::pages::grid::build` applies, repeated here rather than called, so that a
    // change to either half has to be made to both deliberately
    let expected: Vec<Arm<'_>> = arms::grid(&capture)
        .into_iter()
        .filter(|arm| arm.row_bytes() == 1024 && arm.row_profile().is_none())
        .collect();
    let preset = Preset::all().remove(0);
    assert_eq!(preset.axis, Axis::Sweep(shoal_top::index::SweepAxis::ReadShare));
    reproduces(&preset, &expected);
}

#[test]
fn the_row_width_preset_reproduces_chart_row_size_ops() {
    let index = index();
    let (_, capture) = newest_macro(&index);
    // the filter `render::pages::row_size::build` applies to reach its `fixed` set. the mixed
    // profiles are excluded there because their width is a mean, and here because a mean is a
    // different quantity from a measured width and so is not on this axis at all
    let expected: Vec<Arm<'_>> = arms::grid(&capture)
        .into_iter()
        .filter(|arm| arm.read_pct() == Some(50) && arm.row_profile().is_none())
        .collect();
    let preset = Preset::all().remove(1);
    assert_eq!(preset.axis, Axis::Sweep(shoal_top::index::SweepAxis::RowBytes));
    reproduces(&preset, &expected);
}

#[test]
fn a_workload_is_only_offered_on_a_metric_it_answers() {
    let index = index();
    // every workload the picker would let a reader tick on the default metric, which is also the
    // metric both presets open on
    let metric = shoal_top::index::Metric::OpsPerSec;
    let axis = Axis::Sweep(shoal_top::index::SweepAxis::ReadShare);
    // a workload offered on an axis it carries no value for. the picker asks `workload_units`, so
    // that is what is asked here rather than the picker itself, which needs a display
    let offered: Vec<&str> = (0..index.workloads.len() as u32)
        .filter(|at| index.workload_units(*at, &metric, axis).is_some())
        .filter(|at| {
            // whether the measurement the units were read from carries the metric at all
            (0..index.captures.len() as u32)
                .rev()
                .find_map(|capture| index.macro_point(capture, *at))
                .is_some_and(|point| index.value(point, &metric).is_none())
        })
        .map(|at| index.workloads[at as usize].id.as_str())
        .collect();
    assert!(
        offered.is_empty(),
        "{} workloads are offered on a metric they carry no value for, starting with {:?}",
        offered.len(),
        &offered[..offered.len().min(5)]
    );
}

#[test]
fn a_capture_that_measured_no_macro_arm_answers_no_metric() {
    let index = index();
    // the corpus holds captures with no macro layer at all - a `--layer micro` capture is a real
    // capture that measured no workload - and every metric the explorer draws is a macro number
    let answering = index.captures_answering(&Metric::OpsPerSec, &[]);
    assert_eq!(answering.len(), index.captures.len());
    let mut without = 0;
    for (at, answers) in answering.iter().enumerate() {
        let capture = &index.captures[at];
        if !capture.layers.contains(&Layer::Macro) {
            without += 1;
            assert!(
                !answers,
                "{} carries no macro layer and answers a macro metric",
                capture.label
            );
        }
        // and no flag is true without a measurement behind it, or the greying would pass vacuously
        // on a corpus that projected nothing
        if *answers {
            assert!(
                (0..index.workloads.len() as u32).any(|workload| index
                    .macro_point(at as u32, workload)
                    .and_then(|point| index.value(point, &Metric::OpsPerSec))
                    .is_some()),
                "{} answers a metric no measurement in it carries",
                capture.label
            );
        }
    }
    assert!(
        without > 0,
        "every capture carries a macro layer, so this test asserts nothing"
    );
}

#[test]
fn a_selection_only_narrows_what_a_capture_answers() {
    let index = index();
    let metric = Metric::OpsPerSec;
    // what every capture answers with nothing ticked, which is the widest the question gets
    let open = index.captures_answering(&metric, &[]);
    let mut narrowed = 0;
    // one workload at a time, over the first fifty, which is enough to cross several captures
    // without projecting the corpus once per arm
    for workload in 0..50u32.min(index.workloads.len() as u32) {
        let held = index.captures_answering(&metric, &[workload]);
        for (at, answers) in held.iter().enumerate() {
            assert!(
                !answers || open[at],
                "{} answers {} for one workload and not for all of them",
                index.captures[at].label,
                metric.axis_label()
            );
            // and somewhere in there a capture that answers in general does not answer for this
            // one arm, or the narrowing is not doing anything on this corpus
            if open[at] && !answers {
                narrowed += 1;
            }
        }
    }
    assert!(
        narrowed > 0,
        "no capture is narrowed by any selection, so the greying can never say anything"
    );
}

#[test]
fn every_offered_metric_has_a_measurement_behind_it() {
    let index = index();
    // every workload the corpus carries a measurement of, one at a time. the metric list a reader
    // sees with one workload ticked is `metrics_for` over that one workload
    let mut checked = 0;
    for at in 0..index.workloads.len() as u32 {
        if !index.measured(at) {
            continue;
        }
        checked += 1;
        for metric in index.metrics_for(&[at]) {
            assert!(
                index.workload_answers(at, &metric),
                "{} is offered {} and carries no value for it",
                index.workloads[at as usize].id,
                metric.axis_label()
            );
        }
    }
    // a corpus that projected nothing would pass the loop above without running it once
    assert!(checked > 300, "only {checked} workloads have been measured");
}

#[test]
fn a_metric_list_narrows_to_what_the_whole_selection_answers() {
    let index = index();
    // an arm that records a read and a write, and one that records neither, which is the pairing
    // the intersection exists for. `macro/encryption/*` counts no queries at all
    let mixed = (0..index.workloads.len() as u32)
        .find(|at| {
            index.workload_metrics(*at).contains(&shoal_top::index::Metric::Latency {
                op: "write".to_string(),
                percentile: shoal_top::index::Percentile::P99,
            })
        })
        .expect("the corpus holds an arm that recorded a write");
    let counted_none = (0..index.workloads.len() as u32)
        .find(|at| {
            index.measured(*at) && !index.workload_answers(*at, &shoal_top::index::Metric::OpsPerSec)
        })
        .expect("the corpus holds an arm that counted no queries");
    // together they can answer only what both carry, which is neither the write percentiles nor
    // the queries a second
    let both = index.metrics_for(&[mixed, counted_none]);
    assert!(
        !both.contains(&shoal_top::index::Metric::OpsPerSec),
        "a metric only one of the two carries survived the intersection"
    );
    assert!(
        both.iter().all(|metric| index.workload_answers(mixed, metric)
            && index.workload_answers(counted_none, metric)),
        "the intersection offered something one of the two cannot answer"
    );
    // and it is not empty: the wall clock is recorded by everything, so there is always a chart
    assert!(
        both.contains(&shoal_top::index::Metric::WallClock),
        "the intersection dropped a metric both of them carry"
    );
}

#[test]
fn the_presets_pick_arms_that_answer_their_own_metric() {
    let index = index();
    let (capture, _) = newest_macro(&index);
    // the opening chart is the one nobody chose, so an arm on it that cannot answer its metric is
    // the one failure a reader has no way to attribute to something they did
    for preset in Preset::all() {
        let picked = (preset.select)(&index, capture);
        assert!(!picked.is_empty(), "{} selected no arms", preset.name);
        for at in &picked {
            assert!(
                index.workload_answers(*at, &preset.metric),
                "{} picks {}, which carries no {} measurement",
                preset.name,
                index.workloads[*at as usize].id,
                preset.metric.axis_label()
            );
        }
    }
}
