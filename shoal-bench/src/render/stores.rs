//! The store comparison's charts and tables, drawn from its committed captures
//! ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! `shoal-bench render-stores` reads every slim capture under [`RESULTS_DIR`] - one a store, a
//! leg and a redundancy group, `<system>-<group>[-default][-drift].json`, as
//! `shoal-spike/results/stores-lab.sh fetch` writes them - and writes a fixed set of fragments
//! under [`FRAGMENT_DIR`], which [the comparison's page](../../../docs/src/object-storage/store-comparison.md)
//! pulls in with `{{#include}}`. The page is written by hand, and the fragments never are.
//!
//! # Why fragments, and why the set is fixed
//!
//! The page's prose says why a store is faster, which no renderer can write; the numbers it says
//! it of must come from the captures, which no hand should copy. So the numbers are fragments and
//! the prose is the page. mdbook renders an `{{#include}}` of a file that is not there as nothing,
//! which is why `bench-runner.md` turned includes down for the baseline page: so the set of
//! fragments is fixed here, every one is written even with no capture behind it, and `--check` and
//! `the_page_includes_exactly_the_fragments` hold the page's includes to the set.
//!
//! # Invariants
//!
//! **A store is one colour on every chart** ([`System::colour`]), whichever stores a chart draws.
//! **A comparison is refused across two specs**: every capture of a leg must ask for the same arms,
//! runs, seconds, seed and preload, over the same dataset, from the same driver host.
//! **Shoal's interval is the union of its first capture and its drift capture**, so a lab that moved
//! between the two widens Shoal's interval rather than favouring either end.
//! **Shoal's first run is the record, never a verdict**: once Shoal is measured again alone, the first
//! run's captures are kept as `shoal-first-<group>[-drift].json`, drawn and tabled beside today's
//! Shoal, and set against it on the ratio chart (left of 1× where Shoal got faster), and no store is
//! judged against them or counted beside them.
//! **A run left out says so**: a run that put an object twice, or failed an operation, measured
//! something else and is left out of its arm's median and interval, and its store's cell in a verdict
//! table says how many were, so an arm that rests on fewer runs than the others reads as one.

pub mod capture;
pub mod verdict;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use plotters::style::RGBColor;

use crate::render::chart::{bars, palette, ratio, sweep};
use capture::{Capture, Run, split_id};
use verdict::{Interval, Verdict, judge};

/// Where the comparison's slim captures are, by leg, relative to the repository root
pub const RESULTS_DIR: &str = "shoal-spike/results/stores";

/// Where the fragments are written, relative to the repository root
pub const FRAGMENT_DIR: &str = "docs/src/object-storage/store-comparison";

/// The hand-written page that includes them
pub const PAGE: &str = "docs/src/object-storage/store-comparison.md";

/// The bytes in a mebibyte
const MIB: f64 = 1024.0 * 1024.0;

/// One store, as the comparison names and colours it
#[derive(Debug, Clone, Copy)]
pub struct System {
    /// Its key: its captures' file names start with it, and `--system` named it
    pub key: &'static str,
    /// Its name on a chart
    pub name: &'static str,
    /// Which palette slot it is drawn in, the same on every chart
    pub slot: usize,
    /// Whether Shoal is judged against it: a store run in a setting no other store was, beside its
    /// compared one as context, is drawn and tabled but never given a verdict
    pub judged: bool,
    /// Whether it is set against Shoal on the ratio chart: every store judged, and Shoal's first run,
    /// which shows how far Shoal moved and is judged by nobody
    pub on_ratio: bool,
}

impl System {
    /// The colour it is drawn in
    #[must_use]
    pub fn colour(&self) -> RGBColor {
        palette::series(self.slot)
    }
}

/// Every store, in the order every chart and table lists them
pub const SYSTEMS: [System; 7] = [
    System { key: "shoal", name: "Shoal", slot: 0, judged: true, on_ratio: false },
    System { key: "shoal-first", name: "Shoal, first run", slot: 6, judged: false, on_ratio: true },
    System { key: "ceph", name: "Ceph RGW", slot: 1, judged: true, on_ratio: true },
    System { key: "minio", name: "MinIO", slot: 2, judged: true, on_ratio: true },
    System { key: "rustfs", name: "RustFS", slot: 3, judged: true, on_ratio: true },
    System { key: "seaweedfs", name: "SeaweedFS", slot: 4, judged: true, on_ratio: true },
    System { key: "seaweedfs-default", name: "SeaweedFS, unsynced", slot: 5, judged: false, on_ratio: false },
];

/// One leg of the comparison: where it ran
#[derive(Debug, Clone, Copy)]
pub struct Leg {
    /// Its key, its directory under [`RESULTS_DIR`]
    pub key: &'static str,
    /// What a heading calls it
    pub title: &'static str,
}

/// Every leg, in the order the page shows them
pub const LEGS: [Leg; 3] = [
    Leg { key: "ssd", title: "SSDs over 1 GbE" },
    Leg { key: "loop", title: "europa's Optane over loopback" },
    Leg { key: "hdd", title: "disks over 1 GbE" },
];

/// One redundancy group: the stores that keep a bucket one way, compared only with each other
#[derive(Debug, Clone, Copy)]
pub struct Group {
    /// Its key, the `--redundancy` its captures name
    pub key: &'static str,
    /// What a heading calls it
    pub title: &'static str,
    /// Its stores, by key, Shoal first
    pub systems: &'static [&'static str],
}

/// Both groups
pub const GROUPS: [Group; 2] = [
    Group {
        key: "rep3",
        title: "three copies by host",
        systems: &["shoal", "shoal-first", "ceph", "seaweedfs", "seaweedfs-default"],
    },
    Group {
        key: "ec21",
        title: "erasure coded 2+1 by host",
        systems: &["shoal", "shoal-first", "ceph", "minio", "rustfs"],
    },
];

/// The mixes, in the order every chart and table lists them
const MIXES: [&str; 4] = ["put100", "get100", "rw50", "stat100"];

/// The system of a key
///
/// # Arguments
///
/// * `key` - The key
fn system(key: &str) -> System {
    // every key a group names is one of the systems
    *SYSTEMS.iter().find(|system| system.key == key).expect("a group names only known systems")
}

/// Which fragment of a leg and group
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// The headline: the ratio chart, throughput and p99 by size, and the verdicts counted
    Glance,
    /// Everything else: every mix at both concurrencies, the ladders, the series, the costs, the
    /// verdict tables
    Detail,
}

/// The fixed set of fragments, as file names under [`FRAGMENT_DIR`]
#[must_use]
pub fn fragment_names() -> Vec<String> {
    // a glance and a detail of every leg and group, then the facts of every capture
    let mut names = Vec::new();
    for leg in LEGS {
        for group in GROUPS {
            names.push(format!("{}-{}-glance.md", leg.key, group.key));
            names.push(format!("{}-{}-detail.md", leg.key, group.key));
        }
    }
    names.push("facts.md".to_string());
    names
}

/// Every capture of the comparison, by leg and file stem
#[derive(Debug, Default)]
pub struct Results {
    /// Each capture, by leg key, then by its file's stem
    pub captures: BTreeMap<String, BTreeMap<String, Capture>>,
}

impl Results {
    /// Read every capture under a results directory
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory holding one directory a leg
    ///
    /// # Errors
    ///
    /// When a capture cannot be read, or two of a leg asked for different arms.
    pub fn read(dir: &Path) -> Result<Self> {
        let mut results = Results::default();
        for leg in LEGS {
            let leg_dir = dir.join(leg.key);
            let Ok(entries) = std::fs::read_dir(&leg_dir) else {
                continue;
            };
            let mut paths: Vec<PathBuf> = entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
                .collect();
            paths.sort();
            for path in paths {
                let stem = path.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default().to_string();
                let capture = Capture::read(&path)?;
                results.captures.entry(leg.key.to_string()).or_default().insert(stem, capture);
            }
            results.refuse_mixed(leg.key, &leg_dir)?;
        }
        Ok(results)
    }

    /// Refuse a leg whose captures asked for different things
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `dir` - Its directory, for the refusal
    fn refuse_mixed(&self, leg: &str, dir: &Path) -> Result<()> {
        // every capture against the first: the same spec, dataset and driver host
        let Some(captures) = self.captures.get(leg) else {
            return Ok(());
        };
        let mut every = captures.iter();
        let Some((first_name, first)) = every.next() else {
            return Ok(());
        };
        for (name, capture) in every {
            let differs = if capture.spec != first.spec {
                Some("spec")
            } else if capture.dataset.digest != first.dataset.digest {
                Some("dataset")
            } else if capture.provenance.driver.hostname != first.provenance.driver.hostname {
                Some("driver host")
            } else {
                None
            };
            if let Some(fact) = differs {
                bail!(
                    "{}/{name}.json and {}/{first_name}.json differ in their {fact}, so they are not one comparison",
                    dir.display(),
                    dir.display()
                );
            }
        }
        Ok(())
    }

    /// The captures of one store in one leg and group: its own, and Shoal's drift capture beside
    /// Shoal's, as the first run's beside the first run's
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `group` - The group
    /// * `key` - The store
    #[must_use]
    pub fn of(&self, leg: &str, group: &str, key: &str) -> Vec<&Capture> {
        // the file stems a store's captures of this group have
        let stems: Vec<String> = match key {
            "shoal" => vec![format!("shoal-{group}"), format!("shoal-{group}-drift")],
            "shoal-first" => vec![format!("shoal-first-{group}"), format!("shoal-first-{group}-drift")],
            "seaweedfs-default" => vec![format!("seaweedfs-{group}-default")],
            other => vec![format!("{other}-{group}")],
        };
        let Some(captures) = self.captures.get(leg) else {
            return Vec::new();
        };
        stems.iter().filter_map(|stem| captures.get(stem)).collect()
    }

    /// Every run of an arm of one store, over each of its captures
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `group` - The group
    /// * `key` - The store
    /// * `arm` - The arm's id
    #[must_use]
    pub fn runs(&self, leg: &str, group: &str, key: &str, arm: &str) -> Vec<&Run> {
        self.of(leg, group, key)
            .into_iter()
            .filter_map(|capture| capture.arm(arm))
            .flat_map(|arm| arm.runs.iter())
            .collect()
    }

    /// Every arm id any capture of a leg and group ran, in the order the tables list them
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `group` - The group
    #[must_use]
    pub fn arms(&self, leg: &str, group: &str) -> Vec<String> {
        // every id of every store's captures, once
        let mut ids: Vec<String> = Vec::new();
        for key in GROUPS.iter().find(|candidate| candidate.key == group).map_or(&[][..], |group| group.systems) {
            for capture in self.of(leg, group, key) {
                for arm in &capture.arms {
                    if !ids.contains(&arm.id) {
                        ids.push(arm.id.clone());
                    }
                }
            }
        }
        // by mix, then size, then concurrency
        ids.sort_by_key(|id| {
            let (mix, size, concurrency) = split_id(id).unwrap_or_default();
            (MIXES.iter().position(|known| *known == mix).unwrap_or(MIXES.len()), size, concurrency)
        });
        ids
    }

    /// The stores of a group that have any capture in a leg
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `group` - The group
    #[must_use]
    pub fn present(&self, leg: &str, group: &Group) -> Vec<System> {
        group
            .systems
            .iter()
            .filter(|key| !self.of(leg, group.key, key).is_empty())
            .map(|key| system(key))
            .collect()
    }
}

/// A figure read off a run
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Figure {
    /// Every operation a second
    Ops,
    /// The payload both ways, MiB a second
    MiB,
    /// A kind's latency at a percentile
    Latency(&'static str, Percentile),
    /// The bytes the devices wrote for each byte sent
    DeviceBytes,
    /// The bytes on the wire for each payload byte moved
    WireBytes,
    /// The store's cpu-seconds a GiB moved
    CpuPerGib,
    /// The store's cpu-seconds a thousand operations
    CpuPerKop,
    /// The largest host's store memory, MiB
    PeakResident,
}

/// A percentile of a latency
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Percentile {
    /// The median
    P50,
    /// The ninetieth
    P90,
    /// The ninety-ninth
    P99,
    /// The ninety-nine point ninth
    P999,
}

impl Figure {
    /// Read the figure off a run, if the run has it
    ///
    /// # Arguments
    ///
    /// * `run` - The run
    #[must_use]
    pub fn read(&self, run: &Run) -> Option<f64> {
        match self {
            Figure::Ops => Some(run.ops_per_sec()).filter(|ops| *ops > 0.0),
            Figure::MiB => Some(run.mib_per_sec()).filter(|mib| *mib > 0.0),
            Figure::Latency(kind, percentile) => run.kind(kind).map(|kind| match percentile {
                Percentile::P50 => kind.latency.p50_ms,
                Percentile::P90 => kind.latency.p90_ms,
                Percentile::P99 => kind.latency.p99_ms,
                Percentile::P999 => kind.latency.p999_ms,
            }),
            Figure::DeviceBytes => run.device_bytes_per_sent_byte(),
            Figure::WireBytes => run.wire_bytes_per_moved_byte(),
            Figure::CpuPerGib => run.cpu_secs_per_gib(),
            Figure::CpuPerKop => run.cpu_secs_per_kop(),
            Figure::PeakResident => run.peak_resident().map(|bytes| bytes as f64 / MIB),
        }
    }

    /// Whether a higher value is better
    #[must_use]
    pub fn higher_is_better(&self) -> bool {
        matches!(self, Figure::Ops | Figure::MiB)
    }
}

/// The interval of a figure over every run of an arm of a store
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `key` - The store
/// * `arm` - The arm
/// * `figure` - The figure
#[must_use]
pub fn interval(results: &Results, leg: &str, group: &str, key: &str, arm: &str, figure: Figure) -> Option<Interval> {
    // a run that put an object twice, or failed, measured something else
    let values: Vec<f64> = results
        .runs(leg, group, key, arm)
        .into_iter()
        .filter(|run| !run.wrapped && run.failed() == 0)
        .filter_map(|run| figure.read(run))
        .collect();
    Interval::of(&values)
}

/// What a verdict table's cell says of the runs a store's interval left out, nothing when it left
/// none
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `key` - The store
/// * `arm` - The arm
fn left_out(results: &Results, leg: &str, group: &str, key: &str, arm: &str) -> String {
    // the runs `interval` passes over
    let left = results
        .runs(leg, group, key, arm)
        .into_iter()
        .filter(|run| run.wrapped || run.failed() > 0)
        .count();
    match left {
        0 => String::new(),
        1 => ", one run left out".to_string(),
        left => format!(", {left} runs left out"),
    }
}

/// An arm's id as a row names it: `put 4 KiB c16`
///
/// # Arguments
///
/// * `id` - The id
#[must_use]
pub fn arm_label(id: &str) -> String {
    // the mix's operation, the size in binary units, the concurrency
    match split_id(id) {
        Some((mix, size, concurrency)) => {
            let operation = match mix.as_str() {
                "put100" => "put",
                "get100" => "get",
                "stat100" => "stat",
                "rw50" => "rw50",
                other => other,
            };
            format!("{operation} {} c{concurrency}", size_name(size))
        }
        None => id.to_string(),
    }
}

/// A size in the binary units the arms were chosen in
///
/// # Arguments
///
/// * `size` - The bytes
fn size_name(size: u64) -> String {
    for (shift, unit) in [(30, "GiB"), (20, "MiB"), (10, "KiB")] {
        if size >= 1 << shift && size % (1 << shift) == 0 {
            return format!("{} {unit}", size >> shift);
        }
    }
    format!("{size} B")
}

/// A figure's median as a table writes it, with its interval
///
/// # Arguments
///
/// * `interval` - The interval
/// * `figure` - The figure, for its unit
fn cell(interval: Option<Interval>, figure: Figure) -> String {
    // an absent figure is a dash, never a zero
    let Some(interval) = interval else {
        return "-".to_string();
    };
    let write = |value: f64| number(value, figure);
    format!("**{}** [{}–{}]", write(interval.median), write(interval.low), write(interval.high))
}

/// A figure's value, with as much precision as it has
///
/// # Arguments
///
/// * `value` - The value
/// * `figure` - The figure
fn number(value: f64, figure: Figure) -> String {
    match figure {
        Figure::Ops | Figure::PeakResident if value >= 100.0 => crate::fmt::thousands(value.round() as u128),
        Figure::Latency(..) if value >= 100.0 => crate::fmt::fixed(value, 0),
        Figure::Latency(..) if value >= 10.0 => crate::fmt::fixed(value, 1),
        Figure::Latency(..) => crate::fmt::fixed(value, 2),
        _ if value >= 100.0 => crate::fmt::fixed(value, 0),
        _ if value >= 10.0 => crate::fmt::fixed(value, 1),
        _ => crate::fmt::fixed(value, 2),
    }
}

/// The kind an arm's latency is read from: its mix's operation, `get` for a mix of two
///
/// # Arguments
///
/// * `id` - The arm
fn main_kind(id: &str) -> &'static str {
    match split_id(id).map(|(mix, _, _)| mix).as_deref() {
        Some("put100") => "put",
        Some("stat100") => "stat",
        _ => "get",
    }
}

/// The generated notice every fragment opens with
fn notice() -> String {
    format!(
        "<!-- Generated by `shoal-bench render-stores` from the captures under `{RESULTS_DIR}`. \
         Do not edit this file by hand: `render-stores --check` fails on any difference. -->\n\n"
    )
}

/// The line a chart that has nothing to draw is replaced with
///
/// # Arguments
///
/// * `what` - What the chart would have drawn
fn absent(what: &str) -> String {
    format!("*{what}: nothing measured yet.*\n\n")
}

/// The ratio chart of every arm's operations a second over Shoal's
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
fn ratio_chart(results: &Results, leg: &Leg, group: &Group) -> Result<String> {
    // every store judged against Shoal, and Shoal's first run against today's, on every arm both ran
    let others: Vec<System> = results
        .present(leg.key, group)
        .into_iter()
        .filter(|system| system.key != "shoal" && system.on_ratio)
        .collect();
    let arms = results.arms(leg.key, group.key);
    let rows: Vec<ratio::Row> = arms
        .iter()
        .map(|arm| {
            let shoal = interval(results, leg.key, group.key, "shoal", arm, Figure::Ops);
            let whiskers = others
                .iter()
                .map(|other| {
                    let theirs = interval(results, leg.key, group.key, other.key, arm, Figure::Ops)?;
                    let ours = shoal?;
                    Some(ratio::Whisker {
                        low: theirs.low / ours.high,
                        mid: theirs.median / ours.median,
                        high: theirs.high / ours.low,
                    })
                })
                .collect();
            ratio::Row { label: arm_label(arm), whiskers }
        })
        .filter(|row| row.whiskers.iter().any(Option::is_some))
        .collect();
    if rows.is_empty() || others.is_empty() {
        return Ok(absent("Every arm's rate over Shoal's"));
    }
    let series: Vec<ratio::Series> = others.iter().map(|other| ratio::Series { name: other.name.to_string(), colour: other.colour() }).collect();
    let svg = ratio::draw(
        &ratio::Spec {
            id: format!("chart-stores-{}-{}-ratio", leg.key, group.key),
            x_desc: "operations a second over Shoal's (right of 1× is faster than Shoal)".to_string(),
        },
        &series,
        &rows,
    )?;
    Ok(format!("{svg}\n\n"))
}

/// A sweep of one figure of one mix at one concurrency against object size, a line a store
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `mix` - The mix
/// * `concurrency` - The concurrency
/// * `figure` - The figure
/// * `what` - What the chart is, for its id and its description
fn size_sweep(results: &Results, leg: &Leg, group: &Group, mix: &str, concurrency: usize, figure: Figure, what: &str) -> Result<String> {
    // each store's medians at every size it ran the mix at
    let arms = results.arms(leg.key, group.key);
    let mut series = Vec::new();
    let mut colours = Vec::new();
    for system in results.present(leg.key, group) {
        let points: Vec<(f64, f64)> = arms
            .iter()
            .filter_map(|arm| {
                let (arm_mix, size, arm_concurrency) = split_id(arm)?;
                (arm_mix == mix && arm_concurrency == concurrency).then_some(())?;
                let median = interval(results, leg.key, group.key, system.key, arm, figure)?.median;
                Some((size as f64, median))
            })
            .collect();
        if !points.is_empty() {
            series.push(sweep::Series { name: system.name.to_string(), points });
            colours.push(system.colour());
        }
    }
    let description = format!("{what}, {mix} at {concurrency} in flight");
    if series.is_empty() {
        return Ok(absent(&description));
    }
    let (y_desc, y_unit) = match figure {
        Figure::MiB => ("MiB/s, payload both ways".to_string(), sweep::Unit::Count),
        Figure::Ops => ("operations a second".to_string(), sweep::Unit::Rate),
        Figure::Latency(kind, Percentile::P99) => (format!("{kind} p99, ms"), sweep::Unit::Count),
        Figure::Latency(kind, _) => (format!("{kind} latency, ms"), sweep::Unit::Count),
        _ => ("value".to_string(), sweep::Unit::Count),
    };
    let svg = sweep::draw_coloured(
        &sweep::Spec {
            id: format!("chart-stores-{}-{}-{}-{mix}-c{concurrency}", leg.key, group.key, slug(what)),
            x_desc: "object size".to_string(),
            y_desc,
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: sweep::Unit::Bytes,
            y_unit,
        },
        &series,
        &colours,
    )?;
    Ok(format!("{svg}\n\n"))
}

/// A string as an element id writes it
///
/// # Arguments
///
/// * `text` - The text
fn slug(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Bars of figures across groups of arms, a bar a store
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `id` - The chart's id
/// * `x_desc` - What the groups are
/// * `y_desc` - What the bars measure
/// * `bars_of` - Each group's label, arm and figure
fn bar_chart(
    results: &Results,
    leg: &Leg,
    group: &Group,
    id: &str,
    x_desc: &str,
    y_desc: &str,
    bars_of: &[(String, String, Figure)],
) -> Result<String> {
    // each store's median in each group, a gap where it has none
    let groups: Vec<String> = bars_of.iter().map(|(label, _, _)| label.clone()).collect();
    let mut series = Vec::new();
    let mut colours = Vec::new();
    for system in results.present(leg.key, group) {
        let values: Vec<Option<f64>> = bars_of
            .iter()
            .map(|(_, arm, figure)| interval(results, leg.key, group.key, system.key, arm, *figure).map(|interval| interval.median))
            .collect();
        if values.iter().any(Option::is_some) {
            series.push(bars::Series { name: system.name.to_string(), values });
            colours.push(system.colour());
        }
    }
    if series.is_empty() {
        return Ok(absent(y_desc));
    }
    let svg = bars::draw_coloured(
        &bars::Spec {
            id: format!("chart-stores-{}-{}-{id}", leg.key, group.key),
            x_desc: x_desc.to_string(),
            y_desc: y_desc.to_string(),
            unit: sweep::Unit::Count,
        },
        &groups,
        &series,
        &colours,
    )?;
    Ok(format!("{svg}\n\n"))
}

/// One store's seconds of a run of an arm, MiB a second, as a line
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `arm` - The arm whose series the slim captures kept
fn series_chart(results: &Results, leg: &Leg, group: &Group, arm: &str) -> Result<String> {
    // each store's opening run that kept its seconds
    let mut series = Vec::new();
    let mut colours = Vec::new();
    for system in results.present(leg.key, group) {
        let Some(run) = results.runs(leg.key, group.key, system.key, arm).into_iter().find(|run| !run.series.is_empty()) else {
            continue;
        };
        let points: Vec<(f64, f64)> = run
            .series
            .iter()
            .filter(|second| second.phase == "measure")
            .map(|second| (second.at as f64, (second.summary.sent_per_sec + second.summary.received_per_sec) / MIB))
            .collect();
        if !points.is_empty() {
            series.push(sweep::Series { name: system.name.to_string(), points });
            colours.push(system.colour());
        }
    }
    let description = format!("Every measured second of {}", arm_label(arm));
    if series.is_empty() {
        return Ok(absent(&description));
    }
    let svg = sweep::draw_coloured(
        &sweep::Spec {
            id: format!("chart-stores-{}-{}-series-{}", leg.key, group.key, slug(arm)),
            x_desc: format!("second of the opening run of {}", arm_label(arm)),
            y_desc: "MiB/s".to_string(),
            x_axis: sweep::Axis::Linear,
            y_axis: sweep::Axis::Linear,
            x_unit: sweep::Unit::Count,
            y_unit: sweep::Unit::Count,
        },
        &series,
        &colours,
    )?;
    Ok(format!("{svg}\n\n"))
}

/// The verdicts of a figure, every arm a row and every other store a column
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
/// * `figure_of` - The figure an arm is judged by
/// * `heading` - What the figure is
fn verdict_table(results: &Results, leg: &Leg, group: &Group, figure_of: &dyn Fn(&str) -> Figure, heading: &str) -> String {
    // Shoal's column, then each other store's figure and verdict
    let systems = results.present(leg.key, group);
    let others: Vec<System> = systems.iter().copied().filter(|system| system.key != "shoal").collect();
    let mut table = format!("| {heading} | Shoal |");
    for other in &others {
        let _ = write!(table, " {} |", other.name);
    }
    table.push_str("\n| --- | --- |");
    for _ in &others {
        table.push_str(" --- |");
    }
    table.push('\n');
    for arm in results.arms(leg.key, group.key) {
        let figure = figure_of(&arm);
        let shoal = interval(results, leg.key, group.key, "shoal", &arm, figure);
        // each figure with the runs its interval left out, if it left any
        let note = left_out(results, leg.key, group.key, "shoal", &arm);
        let _ = write!(table, "| {} | {}{note} |", arm_label(&arm), cell(shoal, figure));
        for other in &others {
            let theirs = interval(results, leg.key, group.key, other.key, &arm, figure);
            let verdict = match (theirs, shoal) {
                // a store beside its compared one as context is given no verdict
                _ if !other.judged => String::new(),
                (Some(theirs), Some(ours)) => {
                    format!(" · {}", judge(theirs, ours, figure.higher_is_better()).cell(figure.higher_is_better()))
                }
                _ => String::new(),
            };
            let note = left_out(results, leg.key, group.key, other.key, &arm);
            let _ = write!(table, " {}{note}{verdict} |", cell(theirs, figure));
        }
        table.push('\n');
    }
    table.push('\n');
    table
}

/// Each other store's verdicts on a figure, counted: how many arms Shoal is ahead, within and
/// behind
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
fn verdict_counts(results: &Results, leg: &Leg, group: &Group) -> String {
    // throughput and the main kind's p99, every arm both stores ran, of every store judged
    let others: Vec<System> = results
        .present(leg.key, group)
        .into_iter()
        .filter(|system| system.key != "shoal" && system.judged)
        .collect();
    if others.is_empty() {
        return String::new();
    }
    let mut table = String::from("| Beside | Rate: Shoal ahead | within | Shoal behind | p99: Shoal ahead | within | Shoal behind |\n| --- | --- | --- | --- | --- | --- | --- |\n");
    for other in others {
        let mut counts = [0usize; 6];
        for arm in results.arms(leg.key, group.key) {
            for (offset, figure) in [(0, Figure::Ops), (3, Figure::Latency(main_kind(&arm), Percentile::P99))] {
                let (Some(theirs), Some(ours)) = (
                    interval(results, leg.key, group.key, other.key, &arm, figure),
                    interval(results, leg.key, group.key, "shoal", &arm, figure),
                ) else {
                    continue;
                };
                let slot = match judge(theirs, ours, figure.higher_is_better()) {
                    Verdict::Ahead(_) => 0,
                    Verdict::Within => 1,
                    Verdict::Behind(_) => 2,
                };
                counts[offset + slot] += 1;
            }
        }
        let _ = writeln!(
            table,
            "| {} | {} | {} | {} | {} | {} | {} |",
            other.name, counts[0], counts[1], counts[2], counts[3], counts[4], counts[5]
        );
    }
    table.push('\n');
    table
}

/// The glance of one leg and group
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
fn glance(results: &Results, leg: &Leg, group: &Group) -> Result<String> {
    let mut out = notice();
    if results.present(leg.key, group).is_empty() {
        let _ = write!(out, "*Not measured yet: no capture of {} at {} is under `{RESULTS_DIR}/{}/`.*\n", leg.title, group.title, leg.key);
        return Ok(out);
    }
    out.push_str(&ratio_chart(results, leg, group)?);
    out.push_str(&verdict_counts(results, leg, group));
    for (mix, figure, what) in [
        ("put100", Figure::MiB, "throughput"),
        ("get100", Figure::MiB, "throughput"),
        ("put100", Figure::Latency("put", Percentile::P99), "p99"),
        ("get100", Figure::Latency("get", Percentile::P99), "p99"),
    ] {
        out.push_str(&size_sweep(results, leg, group, mix, 16, figure, what)?);
    }
    Ok(out)
}

/// The detail of one leg and group
///
/// # Arguments
///
/// * `results` - The captures
/// * `leg` - The leg
/// * `group` - The group
fn detail(results: &Results, leg: &Leg, group: &Group) -> Result<String> {
    let mut out = notice();
    if results.present(leg.key, group).is_empty() {
        let _ = write!(out, "*Not measured yet: no capture of {} at {} is under `{RESULTS_DIR}/{}/`.*\n", leg.title, group.title, leg.key);
        return Ok(out);
    }
    // every mix at both concurrencies, operations a second
    out.push_str("**Every mix by object size**, operations a second, at one in flight and at sixteen:\n\n");
    for mix in ["put100", "get100", "rw50"] {
        for concurrency in [1, 16] {
            out.push_str(&size_sweep(results, leg, group, mix, concurrency, Figure::Ops, "rate")?);
        }
    }
    // stats, alone at 4 KiB
    out.push_str("**A stat at 4 KiB**, operations a second:\n\n");
    out.push_str(&bar_chart(
        results,
        leg,
        group,
        "stat",
        "in flight",
        "stats a second",
        &[
            ("1 in flight".to_string(), "stat100/s4k/c1/none".to_string(), Figure::Ops),
            ("16 in flight".to_string(), "stat100/s4k/c16/none".to_string(), Figure::Ops),
        ],
    )?);
    // the latency ladders of a 1 MiB put and get at sixteen in flight
    for (arm, kind) in [("put100/s1m/c16/none", "put"), ("get100/s1m/c16/none", "get")] {
        let _ = write!(out, "**The latency of {}**, by percentile, ms:\n\n", arm_label(arm));
        let ladder: Vec<(String, String, Figure)> = [
            ("p50", Percentile::P50),
            ("p90", Percentile::P90),
            ("p99", Percentile::P99),
            ("p99.9", Percentile::P999),
        ]
        .into_iter()
        .map(|(label, percentile)| (label.to_string(), arm.to_string(), Figure::Latency(kind, percentile)))
        .collect();
        out.push_str(&bar_chart(results, leg, group, &format!("ladder-{kind}"), "percentile", &format!("{kind} latency, ms"), &ladder)?);
    }
    // every second of the arms the slim captures kept
    out.push_str("**Every measured second** of each arm's opening run, for the arms whose seconds the captures keep:\n\n");
    for arm in ["put100/s1m/c16/none", "get100/s1m/c16/none", "put100/s4k/c16/none"] {
        out.push_str(&series_chart(results, leg, group, arm)?);
    }
    // what each byte and operation cost
    let sizes = [("4 KiB", "s4k"), ("64 KiB", "s64k"), ("1 MiB", "s1m"), ("16 MiB", "s16m")];
    let by_size = |mix: &str, figure: Figure| -> Vec<(String, String, Figure)> {
        sizes.iter().map(|(label, size)| (label.to_string(), format!("{mix}/{size}/c16/none"), figure)).collect()
    };
    out.push_str("**Device bytes written for each byte put**, a put at sixteen in flight, every host's store devices:\n\n");
    out.push_str(&bar_chart(results, leg, group, "device-bytes", "object size", "device bytes a byte put", &by_size("put100", Figure::DeviceBytes))?);
    out.push_str("**Bytes on the wire for each payload byte**, sent by every host's network interfaces:\n\n");
    out.push_str(&bar_chart(results, leg, group, "wire-put", "object size", "wire bytes a byte put", &by_size("put100", Figure::WireBytes))?);
    out.push_str(&bar_chart(results, leg, group, "wire-get", "object size", "wire bytes a byte got", &by_size("get100", Figure::WireBytes))?);
    out.push_str("**The store's cpu**, every host's store processes, cpu-seconds a thousand operations:\n\n");
    out.push_str(&bar_chart(results, leg, group, "cpu-put", "object size", "cpu-s a thousand puts", &by_size("put100", Figure::CpuPerKop))?);
    out.push_str(&bar_chart(results, leg, group, "cpu-get", "object size", "cpu-s a thousand gets", &by_size("get100", Figure::CpuPerKop))?);
    out.push_str("**The store's memory**, the largest host's store processes' high water marks, MiB, after each arm:\n\n");
    out.push_str(&bar_chart(results, leg, group, "resident", "arm", "peak resident MiB", &by_size("put100", Figure::PeakResident))?);
    // and every arm's verdicts
    out.push_str("**Every arm's rate**, operations a second, median [lowest–highest run], each other store judged against Shoal:\n\n");
    out.push_str(&verdict_table(results, leg, group, &|_| Figure::Ops, "Arm, ops/s"));
    out.push_str("**Every arm's p99**, ms, of its put, get or stat (a mix's get), judged the same way:\n\n");
    out.push_str(&verdict_table(results, leg, group, &|arm| Figure::Latency(main_kind(arm), Percentile::P99), "Arm, p99 ms"));
    Ok(out)
}

/// Every capture's facts: where, what, which build, and whether it read back what it put
///
/// # Arguments
///
/// * `results` - The captures
fn facts(results: &Results) -> String {
    let mut out = notice();
    if results.captures.is_empty() {
        out.push_str("*Not measured yet: no capture is under `shoal-spike/results/stores/`.*\n");
        return out;
    }
    out.push_str("| Leg | Capture | Store | Version | Bench commit | Started | Driver | Hosts' governors | Complete | Preload read back |\n");
    out.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for leg in LEGS {
        let Some(captures) = results.captures.get(leg.key) else {
            continue;
        };
        for (stem, capture) in captures {
            let version = capture.provenance.store.as_ref().map_or_else(
                || capture.provenance.nodes.values().find_map(|node| node.program_sha256.clone()).map_or("-".to_string(), |sha| format!("node `{}`", &sha[..sha.len().min(12)])),
                |store| store.version.clone(),
            );
            // a build of a tree with changes in it is said to be one, since no commit holds it
            let commit = capture.provenance.shoal.commit.as_deref().map_or("-".to_string(), |commit| {
                let dirty = if capture.provenance.shoal.dirty { " (dirty)" } else { "" };
                format!("`{}`{dirty}", &commit[..commit.len().min(8)])
            });
            let governors: Vec<String> = capture
                .provenance
                .nodes
                .iter()
                .map(|(name, node)| format!("{name} {}", node.governor_ran.as_deref().unwrap_or(&node.host.governor)))
                .collect();
            let read_back = capture.verify_end.as_ref().map_or("-".to_string(), |verify| {
                format!("{} of {} ({} lost, {} misread)", verify.checked - verify.lost - verify.misread, verify.checked, verify.lost, verify.misread)
            });
            let _ = writeln!(
                out,
                "| {} | `{stem}` | {} | {version} | {commit} | {} | {} | {} | {} | {read_back} |",
                leg.key,
                capture.system(),
                capture.provenance.started_at,
                capture.provenance.driver.hostname,
                governors.join(", "),
                if capture.complete { "yes" } else { "**no**" },
            );
        }
    }
    out.push('\n');
    out
}

/// Every fragment, by file name, drawn from the captures
///
/// # Arguments
///
/// * `results` - The captures
///
/// # Errors
///
/// When a chart cannot be drawn.
pub fn render(results: &Results) -> Result<Vec<(String, String)>> {
    // in the fixed order of the fragment set
    let mut out = Vec::new();
    for leg in LEGS {
        for group in GROUPS {
            out.push((format!("{}-{}-glance.md", leg.key, group.key), glance(results, &leg, &group)?));
            out.push((format!("{}-{}-detail.md", leg.key, group.key), detail(results, &leg, &group)?));
        }
    }
    out.push(("facts.md".to_string(), facts(results)));
    Ok(out)
}

/// The fragments the page includes, by file name
///
/// # Arguments
///
/// * `page` - The page's text
#[must_use]
pub fn included(page: &str) -> Vec<String> {
    // every `{{#include store-comparison/<name>}}`, in the order the page has them
    page.lines()
        .filter_map(|line| line.trim().strip_prefix("{{#include store-comparison/"))
        .filter_map(|rest| rest.strip_suffix("}}"))
        .map(str::to_string)
        .collect()
}

/// Runs `shoal-bench render-stores`: every fragment written, or with `--check` compared with what
/// is committed, and the page's includes held to the set
///
/// # Arguments
///
/// * `root` - The repository's root
/// * `check` - Whether to compare rather than write
///
/// # Errors
///
/// When a capture cannot be read, a chart drawn, a fragment written, or with `--check` when a
/// fragment differs or the page includes another set.
pub fn run(root: &Path, check: bool) -> Result<i32> {
    // every fragment, built before any is written
    let results = Results::read(&root.join(RESULTS_DIR))?;
    let built = render(&results)?;
    let dir = root.join(FRAGMENT_DIR);
    // the page's includes are the set, exactly, once the page exists
    let page_path = root.join(PAGE);
    if let Ok(page) = std::fs::read_to_string(&page_path) {
        let mut wanted = fragment_names();
        let mut found = included(&page);
        wanted.sort();
        found.sort();
        if wanted != found {
            bail!(
                "{} includes {found:?}, and the fragments are {wanted:?}: an include of a file that is not there renders as nothing",
                page_path.display()
            );
        }
    }
    if check {
        let stale: Vec<&String> = built
            .iter()
            .filter(|(name, text)| std::fs::read_to_string(dir.join(name)).ok().as_deref() != Some(text.as_str()))
            .map(|(name, _)| name)
            .collect();
        if !stale.is_empty() {
            bail!("{} of {} fragments under {FRAGMENT_DIR} are out of date: {stale:?}. Run `shoal-bench render-stores`.", stale.len(), built.len());
        }
        println!("every one of {} fragments is current", built.len());
        return Ok(0);
    }
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    for (name, text) in &built {
        std::fs::write(dir.join(name), text).with_context(|| format!("writing {name}"))?;
    }
    let captures: usize = results.captures.values().map(BTreeMap::len).sum();
    println!("wrote {} fragments under {FRAGMENT_DIR} from {captures} captures", built.len());
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::capture::Capture;
    use super::{fragment_names, included, render, Results};

    /// A capture of one store at one arm, its runs at these rates and this p99
    ///
    /// # Arguments
    ///
    /// * `system` - The store, or `shoal` for none
    /// * `rates` - Each run's puts a second
    /// * `spec_seed` - The seed its spec names
    fn capture(system: &str, rates: &[f64], spec_seed: u64) -> Capture {
        let runs: Vec<serde_json::Value> = rates
            .iter()
            .enumerate()
            .map(|(at, rate)| {
                serde_json::json!({"run": at, "measured": {"secs": 30.0, "kinds": {"put": {"ok": 100, "per_sec": rate,
                    "latency": {"p50_ms": 1.0, "p90_ms": 2.0, "p99_ms": 1000.0 / rate, "p999_ms": 5.0}}},
                    "sent_per_sec": rate * 1048576.0}})
            })
            .collect();
        let store = (system != "shoal").then(|| serde_json::json!({"system": system, "version": "1", "redundancy": "ec21"}));
        serde_json::from_value(serde_json::json!({
            "format": 1, "label": system, "complete": true,
            "provenance": {"started_at": "2026-10-18T00:00:00Z", "driver": {"hostname": "europa"}, "store": store},
            "spec": {"duration": 30, "warmup": 5, "runs": rates.len(), "seed": spec_seed},
            "dataset": {"digest": "d"},
            "arms": [{"id": "put100/s1m/c16/none", "runs": runs}, {"id": "put100/s4k/c16/none", "runs": runs}]
        }))
        .unwrap()
    }

    /// Results of one leg's captures, by file stem
    ///
    /// # Arguments
    ///
    /// * `captures` - Each stem and capture
    fn results(captures: Vec<(&str, Capture)>) -> Results {
        let mut results = Results::default();
        let leg = results.captures.entry("ssd".to_string()).or_default();
        for (stem, capture) in captures {
            leg.insert(stem.to_string(), capture);
        }
        results
    }

    /// Every fragment of the set renders with no capture at all, saying so, so no include of the
    /// page ever renders as nothing
    #[test]
    fn every_fragment_renders_with_no_captures() {
        let built = render(&Results::default()).unwrap();
        let names: Vec<String> = built.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(names, fragment_names());
        for (name, text) in &built {
            assert!(text.contains("Generated by `shoal-bench render-stores`"), "{name}");
            assert!(text.contains("Not measured yet"), "{name}");
        }
    }

    /// Captures render charts and verdicts, the same bytes every time, in sentinel colours alone
    #[test]
    fn rendering_is_deterministic_and_themed() {
        let make = || {
            results(vec![
                ("shoal-ec21", capture("shoal", &[100.0, 110.0, 105.0, 108.0], 7)),
                ("shoal-ec21-drift", capture("shoal", &[104.0, 106.0, 102.0, 109.0], 7)),
                ("minio-ec21", capture("minio", &[200.0, 210.0, 205.0, 220.0], 7)),
                ("rustfs-ec21", capture("rustfs", &[104.0, 107.0, 101.0, 103.0], 7)),
            ])
        };
        let once = render(&make()).unwrap();
        let twice = render(&make()).unwrap();
        assert_eq!(once, twice);
        let glance = &once.iter().find(|(name, _)| name == "ssd-ec21-glance.md").unwrap().1;
        assert!(glance.contains("chart-stores-ssd-ec21-ratio") && glance.contains("MinIO") && !glance.contains("NaN"));
        // MinIO is ahead of Shoal and RustFS within it, on rates both ways; its p99 gap of 81.8% is
        // a lower bound, so it is written rounded down
        let detail = &once.iter().find(|(name, _)| name == "ssd-ec21-detail.md").unwrap().1;
        assert!(detail.contains("Shoal behind ≥45%") && detail.contains("Shoal behind ≥81%"), "{detail}");
        assert!(detail.contains("within"), "{detail}");
        for (_, text) in &once {
            for hex in text.match_indices("=\"#").filter_map(|(at, _)| text.get(at + 2..at + 9)) {
                assert!(crate::render::chart::palette::all_hex().contains(&hex.to_string()), "{hex}");
            }
        }
    }

    /// A store beside its compared one as context is tabled and drawn on the size charts but never
    /// judged: no verdict beside its figures, no row of counts and no whisker on the ratio chart
    #[test]
    fn a_store_run_as_context_is_never_judged() {
        let built = render(&results(vec![
            ("shoal-rep3", capture("shoal", &[100.0, 110.0, 105.0, 108.0], 7)),
            ("seaweedfs-rep3", capture("seaweedfs", &[200.0, 210.0, 205.0, 220.0], 7)),
            ("seaweedfs-rep3-default", capture("seaweedfs-default", &[900.0, 910.0, 910.0, 920.0], 7)),
        ]))
        .unwrap();
        let text = |name: &str| built.iter().find(|(file, _)| file == name).unwrap().1.clone();
        let (glance, detail) = (text("ssd-rep3-glance.md"), text("ssd-rep3-detail.md"));
        // the synced store is judged, and the unsynced one's figures stand alone
        assert!(detail.contains("**208** [200–220] · Shoal behind ≥45%"), "{detail}");
        assert!(detail.contains("**910** [900–920] |"), "{detail}");
        // the counts name the synced store alone
        assert!(glance.contains("| SeaweedFS | ") && !glance.contains("| SeaweedFS, unsynced | "), "{glance}");
        // and so does the ratio chart, while the size charts draw both
        let start = glance.find("chart-stores-ssd-rep3-ratio").unwrap();
        let ratio = &glance[start..start + glance[start..].find("</svg>").unwrap()];
        assert!(ratio.contains("SeaweedFS") && !ratio.contains("unsynced"), "{ratio}");
        assert!(glance[start + ratio.len()..].contains("SeaweedFS, unsynced"), "{glance}");
    }

    /// Shoal's first run is the record: tabled beside today's Shoal with no verdict, set against it
    /// on the ratio chart, never counted, and never mixed into today's interval
    #[test]
    fn shoals_first_run_is_the_record_and_never_judged() {
        let built = render(&results(vec![
            ("shoal-ec21", capture("shoal", &[200.0, 210.0, 205.0, 220.0], 7)),
            ("shoal-first-ec21", capture("shoal", &[100.0, 110.0, 105.0, 108.0], 7)),
            ("shoal-first-ec21-drift", capture("shoal", &[102.0, 109.0, 104.0, 106.0], 7)),
            ("minio-ec21", capture("minio", &[104.0, 107.0, 101.0, 103.0], 7)),
        ]))
        .unwrap();
        let text = |name: &str| built.iter().find(|(file, _)| file == name).unwrap().1.clone();
        let (glance, detail) = (text("ssd-ec21-glance.md"), text("ssd-ec21-detail.md"));
        // today's interval is today's alone, the first run's stands beside it with no verdict, and
        // MinIO is judged against today's Shoal
        assert!(detail.contains("| Arm, ops/s | Shoal | Shoal, first run | MinIO |"), "{detail}");
        assert!(detail.contains("| **208** [200–220] | **106** [100–110] | **104** [101–107] · Shoal ahead"), "{detail}");
        // the counts name MinIO alone
        assert!(glance.contains("| MinIO | ") && !glance.contains("| Shoal, first run | "), "{glance}");
        // and the ratio chart sets the first run against today's Shoal beside MinIO
        let start = glance.find("chart-stores-ssd-ec21-ratio").unwrap();
        let ratio = &glance[start..start + glance[start..].find("</svg>").unwrap()];
        assert!(ratio.contains("Shoal, first run") && ratio.contains("MinIO"), "{ratio}");
    }

    /// A run that failed an operation is left out of its arm's median and interval, and its cell says
    /// so, while an arm whose every run counted says nothing
    #[test]
    fn a_run_left_out_says_so() {
        // Shoal's second run of the 1 MiB arm failed an operation
        let mut shoal = capture("shoal", &[200.0, 210.0, 205.0, 220.0], 7);
        shoal.arms[0].runs[1].measured.kinds.get_mut("put").unwrap().errors.insert("Timeout".to_string(), 1);
        let built = render(&results(vec![
            ("shoal-ec21", shoal),
            ("minio-ec21", capture("minio", &[104.0, 107.0, 101.0, 103.0], 7)),
        ]))
        .unwrap();
        let detail = built.iter().find(|(file, _)| file == "ssd-ec21-detail.md").unwrap().1.clone();
        // the failed run is out of the median and the interval, and the cell says one was
        assert!(detail.contains("| put 1 MiB c16 | **205** [200–220], one run left out | **104** [101–107] · Shoal ahead"), "{detail}");
        // and the other arm, whose four runs all counted, says nothing
        assert!(detail.contains("| put 4 KiB c16 | **208** [200–220] | **104** [101–107] · Shoal ahead"), "{detail}");
    }

    /// Two captures of one leg that asked for other arms are refused, naming both
    #[test]
    fn differing_specs_refuse_to_render() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("ssd")).unwrap();
        for (stem, seed) in [("shoal-ec21", 7), ("minio-ec21", 8)] {
            let text = serde_json::json!({"format": 1, "spec": {"seed": seed}, "dataset": {"digest": "d"}});
            std::fs::write(dir.path().join("ssd").join(format!("{stem}.json")), text.to_string()).unwrap();
        }
        let refusal = Results::read(dir.path()).unwrap_err().to_string();
        assert!(refusal.contains("minio-ec21.json") && refusal.contains("shoal-ec21.json") && refusal.contains("spec"), "{refusal}");
    }

    /// The page's includes are read in its order, and nothing else is
    #[test]
    fn includes_are_read_off_the_page() {
        let page = "text\n{{#include store-comparison/ssd-rep3-glance.md}}\nmore {{#include x}}\n  {{#include store-comparison/facts.md}}\n";
        assert_eq!(included(page), vec!["ssd-rep3-glance.md", "facts.md"]);
    }
}
