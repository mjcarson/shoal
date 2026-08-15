//! Assembling the benchmark results page
//!
//! # Why this has to be deterministic
//!
//! The page is committed, because `create-missing = false` means a `SUMMARY.md` entry pointing at
//! a file that is not there fails the whole book build, and because regenerating it needs a twelve
//! core machine, a populated `/opt/shoal` and a dataset that is not in the repository. A clean
//! checkout has to be able to build the book.
//!
//! That makes `shoal-bench render --check` the thing that says whether the committed page is
//! current, and it can only do that if rendering the same inputs twice produces the same bytes.
//! Four rules keep it that way, and all four are easy to break by accident:
//!
//! 1. **No wall clock reaches the page.** Capture timestamps do; "generated at" does not.
//! 2. **Every map walked while rendering is a `BTreeMap`, and every list is explicitly sorted.**
//! 3. **Every float goes through [`crate::fmt`]**, at a fixed precision. No `{:?}` on an `f64`.
//! 4. **Chart geometry is a pure function of the data** and the fixed canvas size.
//!
//! The current commit *is* on the page, deliberately. The staleness verdicts are relative to it, so
//! a page that did not say which commit it was rendered against would be making claims with no
//! referent. The consequence is that `--check` fails after any commit, which is not a bug: it is
//! the page telling you it no longer describes this tree.

use anyhow::Result;

use super::{badges, chart, tables};
use crate::compare::micro::{self, MicroComparison};
use crate::fmt;
use crate::model::hotpath::HotpathProfile;
use crate::model::macro_layer::MacroCaptureV2;
use crate::model::micro::MicroCapture;
use crate::model::stages::StageReport;
use crate::registry::Layer;
use crate::stale::{CaptureStatus, CodeVerdict};

/// Everything one capture produced
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The capture's name
    pub label: String,
    /// When it was taken, as recorded in its own artifacts
    pub captured: String,
    /// Its micro layer, if it captured one
    pub micro: Option<MicroCapture>,
    /// Its macro layer, if it captured one
    pub macro_layer: Option<MacroCaptureV2>,
    /// Its hotpath profile, if it captured one
    pub hotpath: Option<HotpathProfile>,
    /// Its stage report, if it captured one
    pub stages: Option<StageReport>,
}

/// Everything the page is built from
#[derive(Debug, Clone)]
pub struct Page {
    /// The commit the page was rendered against
    pub head_short: String,
    /// Whether anything was uncommitted when it was rendered
    ///
    /// A boolean rather than a count on purpose. The count churns with every unrelated file added
    /// to the tree, which would make `--check` fail for reasons that have nothing to do with the
    /// benchmarks; what a reader needs to know is that the verdicts below were taken against a
    /// tree that does not match its commit. `shoal-bench status` has the count.
    pub dirty: bool,
    /// The machine it was rendered on, for the provenance block
    pub host: String,
    /// The governor that machine was set to
    pub governor: String,
    /// Every capture, oldest first
    pub timeline: Vec<Snapshot>,
    /// What was concluded about each capture, in the same order
    pub statuses: Vec<CaptureStatus>,
    /// The capture the current numbers are drawn from
    pub current: String,
    /// The frozen baseline's name and micro capture
    pub frozen: Option<(String, MicroCapture)>,
    /// The trailing baseline's name and micro capture
    pub trailing: Option<(String, MicroCapture)>,
    /// Each set of identical repeats, and each benchmark's duration and spread within it
    pub repeats: Vec<(String, Vec<(f64, f64)>)>,
}

impl Page {
    /// The capture the current numbers are drawn from
    fn current(&self) -> Option<&Snapshot> {
        // named by the caller, or absent if that capture produced nothing
        self.timeline
            .iter()
            .find(|snapshot| snapshot.label == self.current)
    }

    /// What was concluded about one capture
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    fn status(&self, label: &str) -> Option<&CaptureStatus> {
        self.statuses
            .iter()
            .find(|status| status.label == label)
    }
}

/// Renders the whole page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = String::new();
    // the header, which says what the page is and what it was rendered against
    out.push_str(&header(page));
    out.push_str(&freshness(page));
    out.push_str(&macro_section(page)?);
    out.push_str(&micro_section(page)?);
    out.push_str(&scaling_section(page)?);
    out.push_str(&encryption_section(page)?);
    out.push_str(&hotpath_section(page)?);
    out.push_str(&stages_section(page)?);
    out.push_str(&noise_section(page)?);
    out.push_str(&footer());
    Ok(out)
}

/// The page's opening, and what it was rendered against
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn header(page: &Page) -> String {
    let mut out = String::new();
    out.push_str("# Benchmark Results\n\n");
    out.push_str(
        "Generated by `shoal-bench render` from the artifacts committed under `docs/perf/`. \
         **Do not edit this page by hand** - re-run the command instead, and `shoal-bench render \
         --check` will tell you when it is out of date.\n\n",
    );
    out.push_str(
        "This page is the current picture. [Performance Baseline](performance-baseline.md) is the \
         frozen one: what `B1` measured, what the governor experiment found, and what the micro \
         layer can and cannot resolve. Where the two disagree, this page is newer and that page \
         explains why the numbers mean what they do. How to take a capture is in \
         [Benchmarking](benchmarking.md).\n\n",
    );
    // what the verdicts below are relative to. without this the badges are claims with no referent.
    out.push_str(&format!(
        "Rendered against commit `{}`{}, on `{}` with the `{}` governor.\n\n",
        page.head_short,
        if page.dirty {
            " with uncommitted changes"
        } else {
            ""
        },
        page.host,
        page.governor
    ));
    // and a one line summary, so the state of the page is legible before any of it is read
    let stale: Vec<&CaptureStatus> = page
        .statuses
        .iter()
        .filter(|status| {
            status
                .code
                .iter()
                .any(|(_, verdict)| !verdict.is_current())
        })
        .collect();
    out.push_str(&format!(
        "{} captures are recorded. {}\n\n",
        page.statuses.len(),
        if stale.is_empty() {
            "Every one of them still describes the current code.".to_string()
        } else {
            format!(
                "{} of them have at least one layer that no longer describes the current code, or \
                 that was taken before provenance was recorded. The freshness table below says \
                 which.",
                stale.len()
            )
        }
    ));
    out
}

/// The table of what has been captured and whether it can still be believed
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn freshness(page: &Page) -> String {
    let mut out = String::new();
    out.push_str("## What has been captured\n\n");
    out.push_str(
        "One row per capture and layer. A capture is **fresh** when it was taken at this commit on \
         a clean tree, **unaffected** when the commit has moved but nothing that layer measures \
         has, **stale** when that layer's sources have changed since, and **uncommitted** when the \
         bytes it measured exist in no commit at all. **No provenance** is how a capture taken \
         before `shoal-bench` existed is reported: it is deliberately neither fresh nor stale, \
         because neither is known.\n\n",
    );
    out.push_str("| Capture | Taken | Layer | Standing |\n| --- | --- | --- | --- |\n");
    // newest first, which is the order somebody looking for the current numbers reads in
    for snapshot in page.timeline.iter().rev() {
        let Some(status) = page.status(&snapshot.label) else {
            continue;
        };
        for (layer, _) in &status.code {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                snapshot.label,
                snapshot.captured,
                layer,
                badges::for_layer(status, *layer)
            ));
        }
    }
    out.push('\n');
    out
}

/// The end to end section
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn macro_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## End to end\n\n");
    // every capture that produced a macro artifact, in the order they were taken
    let captures: Vec<(String, MacroCaptureV2)> = page
        .timeline
        .iter()
        .filter_map(|snapshot| {
            snapshot
                .macro_layer
                .clone()
                .map(|capture| (snapshot.label.clone(), capture))
        })
        .collect();
    if captures.is_empty() {
        out.push_str("No capture has produced an end to end result yet.\n\n");
        return Ok(out);
    }
    out.push_str(
        "Purpose built workloads against a live server, each isolating one path through the \
         engine. Every workload builds its own rows from a seed, so a clean checkout reproduces \
         this layer with nothing fetched. Each capture is drawn as the median of its runs with \
         the range of those runs around it.\n\n**Two captures differ only when their ranges do \
         not overlap.** This layer's own spread is wider than most changes worth making - the \
         frozen baseline spread 10.5% across five identical runs - so a difference in medians \
         that sits inside the ranges is not a result, however large it looks.\n\n**A `per_batch` \
         number and a `per_query` number are never comparable.** A saturated workload takes one \
         timestamp per batch, so its percentiles are batch completion times; a bounded \
         concurrency workload stamps each query, so its percentiles are service times. The table \
         below names which each workload is.\n\n",
    );
    // every workload any capture measured, in a stable order
    let mut workload_ids: Vec<String> = captures
        .iter()
        .flat_map(|(_, capture)| capture.workload_ids())
        .map(String::from)
        .collect();
    workload_ids.sort();
    workload_ids.dedup();
    // one chart per workload, since a chart that mixed them would put a wall clock over 200,000
    // rows on the same axis as one over 2,000 and make both unreadable
    for id in &workload_ids {
        // only the captures that actually measured this workload contribute a point. a capture
        // taken before a workload existed is a gap in the series rather than a zero.
        let points: Vec<chart::macro_wall_clock::Point> = captures
            .iter()
            .filter_map(|(label, capture)| {
                capture
                    .workloads
                    .get(id)
                    .map(|workload| chart::macro_wall_clock::Point {
                        label: label.clone(),
                        median_ns: workload.median_wall_clock_ns() as f64,
                        interval_ns: workload
                            .wall_clock_interval_ns()
                            .map(|(low, high)| (low as f64, high as f64)),
                    })
            })
            .collect();
        // a workload measured once has a chart with one point on it, which says nothing
        if points.len() < 2 {
            continue;
        }
        out.push_str(&format!("### `{id}`\n\n"));
        let reference = points
            .iter()
            .find(|point| Some(&point.label) == page.frozen.as_ref().map(|(name, _)| name));
        out.push_str(&chart::macro_wall_clock::draw(&points, reference)?);
        out.push('\n');
        out.push_str(&caption(&format!(
            "Wall clock of `{id}` in every capture that measured it, oldest first. The bar is the \
             range across runs; the dot is the median that the table quotes."
        )));
    }
    out.push_str(&tables::macro_summary(&captures));
    out.push('\n');
    // and the current capture's own distribution, which is what a client actually experiences
    if let Some(current) = page.current()
        && let Some(capture) = &current.macro_layer
    {
        out.push_str(&format!(
            "### Latency distribution of `{}`\n\n",
            current.label
        ));
        if let Some(status) = page.status(&current.label) {
            let badges = badges::for_layer(status, Layer::Macro);
            if !badges.is_empty() {
                out.push_str(&format!("{badges}\n\n"));
            }
        }
        out.push_str(
            "Insert and get are summarised apart, because the client pools its requests and a \
             percentile over the two together is not a percentile of anything.\n\n",
        );
        out.push_str(&tables::macro_percentiles(capture));
        out.push('\n');
    }
    Ok(out)
}

/// The micro comparison section
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn micro_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Against the baseline\n\n");
    let (Some(current), Some((frozen_name, frozen))) = (page.current(), page.frozen.as_ref()) else {
        out.push_str("No capture and baseline pair is available to compare.\n\n");
        return Ok(out);
    };
    let Some(measured) = &current.micro else {
        out.push_str("The current capture has no micro layer to compare.\n\n");
        return Ok(out);
    };
    let comparison = micro::compare(measured, frozen, &micro::NoiseBand::default());
    if comparison.rows.is_empty() {
        out.push_str("The current capture and the baseline share no benchmarks.\n\n");
        return Ok(out);
    }
    out.push_str(&format!(
        "`{}` against the frozen baseline `{frozen_name}`. The shaded band behind each bar is the \
         width inside which a difference is not a result: ±{}% for a benchmark that takes under \
         {}, ±{}% for one that takes longer. **A bar that does not escape its own shading has not \
         moved.**\n\n",
        current.label,
        micro::NOISE_FAST_PCT,
        fmt::duration_ns(micro::FAST_THRESHOLD_NS),
        micro::NOISE_SLOW_PCT,
    ));
    out.push_str(
        "The tiers are screens rather than bounds - see [what the micro layer can \
         resolve](#what-the-micro-layer-can-resolve) below - and a result is only accepted after a \
         confirming repeat of the whole capture.\n\n",
    );
    if let Some(status) = page.status(&current.label) {
        let badges = badges::for_layer(status, Layer::Micro);
        if !badges.is_empty() {
            out.push_str(&format!("{badges}\n\n"));
        }
    }
    out.push_str(&chart::micro_delta::draw(&comparison.rows, frozen_name)?);
    out.push('\n');
    out.push_str(&caption(
        "Every benchmark both captures measured, from the largest improvement to the largest \
         regression. Bars inside the shaded band are drawn grey because they are not results.",
    ));
    // what only one side measured, which a join would otherwise hide
    out.push_str(&set_differences(&comparison, &current.label, frozen_name));
    out.push_str(&tables::micro_comparison(&comparison));
    out.push('\n');
    // the same capture against the trailing baseline, which answers a different question
    if let Some((trailing_name, trailing)) = page.trailing.as_ref() {
        let against_trailing = micro::compare(measured, trailing, &micro::NoiseBand::default());
        let moved = against_trailing
            .rows
            .iter()
            .filter(|row| row.significant)
            .count();
        out.push_str(&format!(
            "### Against `{trailing_name}`\n\nThe frozen baseline says how far the whole series of \
             changes has come; the trailing one says what the last change did. Against \
             `{trailing_name}`, {moved} of {} benchmarks moved outside the band.\n\n",
            against_trailing.rows.len()
        ));
        if moved > 0 {
            let rows: Vec<_> = against_trailing
                .rows
                .iter()
                .filter(|row| row.significant)
                .cloned()
                .collect();
            out.push_str(&tables::micro_comparison(&MicroComparison {
                rows,
                only_in_baseline: Vec::new(),
                only_in_run: Vec::new(),
            }));
            out.push('\n');
        }
    }
    Ok(out)
}

/// What only one side of a comparison measured
///
/// # Arguments
///
/// * `comparison` - What the comparison found
/// * `run` - The capture's name
/// * `baseline` - The baseline's name
fn set_differences(comparison: &MicroComparison, run: &str, baseline: &str) -> String {
    let mut out = String::new();
    // a benchmark that silently vanished from a comparison is how a regression gets missed, so
    // both directions are stated rather than left to be noticed
    if !comparison.only_in_baseline.is_empty() {
        out.push_str(&format!(
            "`{baseline}` measures {} benchmarks that `{run}` does not, so they are not compared \
             above: {}\n\n",
            comparison.only_in_baseline.len(),
            list_code(&comparison.only_in_baseline)
        ));
    }
    if !comparison.only_in_run.is_empty() {
        out.push_str(&format!(
            "`{run}` measures {} benchmarks that `{baseline}` predates, so they have no baseline \
             to be compared against: {}\n\n",
            comparison.only_in_run.len(),
            list_code(&comparison.only_in_run)
        ));
    }
    out
}

/// Renders a list of names as inline code, comma separated
///
/// # Arguments
///
/// * `names` - The names to render
fn list_code(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<String>>()
        .join(", ")
}

/// The scaling section
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn scaling_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## How the cost grows with the partition\n\n");
    let Some(measured) = page.current().and_then(|current| current.micro.as_ref()) else {
        out.push_str("The current capture has no micro layer.\n\n");
        return Ok(out);
    };
    let families = chart::micro_scaling::families(measured);
    if families.is_empty() {
        out.push_str("No benchmark was measured at more than one partition size.\n\n");
        return Ok(out);
    }
    out.push_str(
        "Both axes are logarithmic, which makes the *shape* of the growth readable rather than the \
         magnitude: a cost proportional to the number of rows is a straight diagonal, a cost \
         independent of it is flat, and anything between is a slope. This is the chart that says \
         whether an operation seeks into a partition or walks it, which is a different question \
         from whether it is fast, and one the noise band has no bearing on.\n\n",
    );
    out.push_str(&chart::micro_scaling::draw(&families)?);
    out.push('\n');
    out.push_str(&caption(
        "Mean time against partition size. Each line is labelled at its right hand end rather than \
         in a legend, so no colour has to be matched to a name.",
    ));
    out.push_str(&tables::micro_scaling(&families));
    out.push('\n');
    Ok(out)
}

/// The section saying what encryption costs
///
/// Every arm of the `macro/encryption/*` sweeps has a twin differing in the wire and in nothing
/// else, so this is the one section on the page whose comparison is *within* a capture rather than
/// across two. Both halves of every pair ran on the same machine, minutes apart, against the same
/// seed and the same configuration.
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn encryption_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What encryption costs\n\n");
    let Some(measured) = page.current().and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str("The current capture has no macro layer.\n\n");
        return Ok(out);
    };
    let depth = chart::encryption::pairs(measured, chart::encryption::DEPTH_SWEEP);
    let clients = chart::encryption::pairs(measured, chart::encryption::CLIENT_SWEEP);
    if depth.is_empty() && clients.is_empty() {
        out.push_str(
            "The current capture holds no encryption sweep, so what TLS costs this system is \
             unmeasured. See [F14](../features/encryption-in-transit.md).\n\n",
        );
        return Ok(out);
    }
    out.push_str(
        "Every point below is a pair: one workload over a plaintext wire and one over a wire the \
         kernel encrypts, differing in the wire and in nothing else — same seed, same rows, same \
         row width, same query count, same load. The gap between them is therefore what encryption \
         cost, rather than what else happened to move.\n\n\
         **A hollow marker is not a result.** The macro layer's rule is that a difference counts \
         only when the two sides' observed intervals are disjoint, and a pair whose runs overlapped \
         has not been shown to differ however far apart its medians sit.\n\n",
    );
    if !depth.is_empty() {
        out.push_str("### Against row width\n\n");
        out.push_str(&chart::encryption::draw_by_row(measured)?);
        out.push('\n');
        out.push_str(&caption(
            "What TLS added, as a share of the plaintext cost, against how wide a row is. One \
             curve per load depth, each labelled at its right hand end.",
        ));
        out.push_str("### Against load depth\n\n");
        out.push_str(&chart::encryption::draw_by_depth(measured)?);
        out.push('\n');
        out.push_str(&caption(
            "The same pairs read the other way: what TLS added against how many queries were \
             outstanding at once on one client. One curve per row width.",
        ));
        out.push_str("### What it was added to\n\n");
        out.push_str(&chart::encryption::draw_absolute(measured)?);
        out.push('\n');
        out.push_str(&caption(
            "The absolute p50 of one get on each wire, at a single outstanding query. A percentage \
             is unreadable without this — a large share of a small number is not the same finding \
             as a small share of a large one.",
        ));
    }
    if !clients.is_empty() {
        out.push_str("### Against client count\n\n");
        out.push_str(&chart::encryption::draw_by_clients(measured)?);
        out.push('\n');
        out.push_str(&caption(
            "What TLS added against how many independent clients produced the load, each one query \
             deep and each with its own connection pool and its own handshakes.",
        ));
    }
    out.push_str(&tables::encryption(&depth, &clients));
    out.push('\n');
    Ok(out)
}

/// The hotpath section
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn hotpath_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Where the time goes\n\n");
    // the most recent capture that produced a profile, which may not be the current one
    let Some(snapshot) = page
        .timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.hotpath.is_some())
    else {
        out.push_str("No capture has produced a profile yet.\n\n");
        return Ok(out);
    };
    let profile = snapshot
        .hotpath
        .as_ref()
        .expect("the snapshot was selected for having one");
    out.push_str(&format!(
        "From `{}`, built with the `hotpath` feature. **This is a separate, instrumented build.** \
         It stamps extra timestamps on the query path, so its wall clock is not comparable to the \
         uninstrumented build's and no latency or throughput on this page comes from it. It \
         attributes time; it does not measure it.\n\n",
        snapshot.label
    ));
    out.push_str(
        "The bars are total nanoseconds **summed across all twelve shards**, so they add up to \
         more than the run's wall clock for anything that ran on many shards at once. `hotpath` \
         also reports a percentage per scope, and this page never uses it: it is not normalised \
         across concurrent scopes, and reports one scope of this very profile at over 12,000%. \
         See [known issue 53](../appendix/known-issues.md).\n\n",
    );
    if let Some(status) = page.status(&snapshot.label) {
        let badges = badges::for_layer(status, Layer::Hotpath);
        if !badges.is_empty() {
            out.push_str(&format!("{badges}\n\n"));
        }
    }
    out.push_str(&chart::hotpath_scopes::draw(profile)?);
    out.push('\n');
    out.push_str(&caption(
        "The twelve most expensive instrumented scopes, ranked by total time inside them.",
    ));
    out.push_str(&tables::hotpath_scopes(profile, 12));
    out.push('\n');
    Ok(out)
}

/// The stage breakdown section
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn stages_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Where one query's latency goes\n\n");
    // the most recent capture that produced a stage report
    let Some(snapshot) = page
        .timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.stages.is_some())
    else {
        out.push_str(
            "No capture has produced a stage report yet. Take one with `shoal-bench run --label \
             <name>`, which captures every layer.\n\n",
        );
        return Ok(out);
    };
    let report = snapshot
        .stages
        .as_ref()
        .expect("the snapshot was selected for having one");
    out.push_str(&format!(
        "From `{}`, built with the `stage-profile` feature - another separate instrumented build, \
         and attribution only for the same reason. Every query records when it reached each of \
         nineteen points between the client's `send` and the response coming back. See [F6](\
         ../features/stage-breakdown.md).\n\n",
        snapshot.label
    ));
    out.push_str(
        "Each bar is one **latency rank**, not one stage: the queries are ranked by their total \
         latency, a window is taken around each rank, and each stage is averaged over that window. \
         Per-stage percentiles are not used, because they do not add up to the total percentile - \
         the p99 of every stage is not a description of any query that happened.\n\n\
         A stage whose cost is within twice the cost of reading the clock is folded into `other`, \
         because below that the measurement is mostly the instrument. Whatever the stages do not \
         account for is drawn as its own segment rather than spread across the rest.\n\n",
    );
    if let Some(status) = page.status(&snapshot.label) {
        let badges = badges::for_layer(status, Layer::Stages);
        if !badges.is_empty() {
            out.push_str(&format!("{badges}\n\n"));
        }
    }
    out.push_str(&format!(
        "The client and server halves joined on {} queries.\n\n",
        fmt::thousands(report.join.joined as u128)
    ));
    // one chart and one table per operation the report covers
    for op in ["insert", "get"] {
        let Some(table) = tables::stage_ranks(report, op, 3) else {
            continue;
        };
        out.push_str(&format!("### {op}\n\n"));
        out.push_str(&chart::stages_stacked::draw(report, op)?);
        out.push('\n');
        out.push_str(&caption(&format!(
            "Stage breakdown of {op} queries at each latency rank, slowest rank at the bottom."
        )));
        out.push_str(&table);
        out.push('\n');
    }
    Ok(out)
}

/// The section about what the micro layer can resolve
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn noise_section(page: &Page) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What the micro layer can resolve\n\n");
    if page.repeats.is_empty() {
        out.push_str("No repeat captures are recorded.\n\n");
        return Ok(out);
    }
    out.push_str(
        "Every point is one benchmark's spread across a set of **identical repeats** - the same \
         code, the same machine, the same everything, captured several times. Whatever a point \
         sits above zero is what that benchmark moves by when nothing has changed, which is the \
         floor under any claim made about it.\n\n\
         The stepped line is the tier `shoal-bench compare` screens against. Points above it are \
         benchmarks whose ordinary variation the band does not cover, and there are some: the \
         tiers are screens, not bounds. Criterion's own confidence interval is deliberately not \
         used - across these repeats `partition_sorted/get_key/4096` moved 22% while reporting its \
         outlying value with a ±0.2% interval, tighter than any of the runs it disagreed with.\n\n",
    );
    let groups: Vec<chart::noise_band::Group> = page
        .repeats
        .iter()
        .map(|(name, points)| chart::noise_band::Group {
            name: name.clone(),
            points: points.clone(),
        })
        .collect();
    out.push_str(&chart::noise_band::draw(&groups)?);
    out.push('\n');
    out.push_str(&caption(
        "Observed spread against benchmark duration. Instability scales inversely with duration, \
         which is why one global threshold would be either too loose for the slow benchmarks or \
         too tight for the fast ones.",
    ));
    out.push_str(&tables::noise_buckets(&page.repeats));
    out.push('\n');
    Ok(out)
}

/// The page's closing links
fn footer() -> String {
    let mut out = String::new();
    out.push_str("## Related\n\n");
    out.push_str(
        "- [Benchmarking](benchmarking.md) - how to take a capture, and how to get a number worth \
         believing\n\
         - [Performance Baseline](performance-baseline.md) - the frozen `B1` capture, the governor \
         experiment, and the history of accepted changes\n\
         - [F3, the performance harness](../features/performance-harness.md) - why there are four \
         layers\n\
         - [F7, the benchmark runner](../features/bench-runner.md) - how this page is produced\n\
         - [Optimizations](../appendix/optimizations.md) - the backlog these captures adjudicate\n",
    );
    out
}

/// Renders a caption under a chart
///
/// # Arguments
///
/// * `text` - What the caption says
fn caption(text: &str) -> String {
    // raw html rather than an italic line, so the stylesheet can pull it up under the chart
    format!("<span class=\"sc-caption\">{text}</span>\n\n")
}

/// Whether a page reports anything that is not current
///
/// # Arguments
///
/// * `statuses` - What was concluded about each capture
pub fn any_stale(statuses: &[CaptureStatus]) -> bool {
    statuses.iter().any(|status| {
        status
            .code
            .iter()
            .any(|(_, verdict)| !matches!(verdict, CodeVerdict::Fresh))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stale::EnvVerdict;

    /// Builds a page with nothing captured
    fn empty_page() -> Page {
        Page {
            head_short: "afd0899".to_string(),
            dirty: false,
            host: "jove".to_string(),
            governor: "performance".to_string(),
            timeline: Vec::new(),
            statuses: Vec::new(),
            current: "none".to_string(),
            frozen: None,
            trailing: None,
            repeats: Vec::new(),
        }
    }

    /// A page with nothing captured still renders, and says so in each section
    #[test]
    fn an_empty_page_still_renders() {
        let rendered = build(&empty_page()).expect("it renders");
        assert!(rendered.starts_with("# Benchmark Results"));
        assert!(rendered.contains("No capture has produced an end to end result yet."));
        assert!(rendered.contains("No capture has produced a stage report yet."));
        // and it still says what it was rendered against
        assert!(rendered.contains("commit `afd0899`"));
    }

    /// The page never embeds a wall clock, or `--check` would fail every time it ran
    #[test]
    fn the_page_carries_no_wall_clock() {
        let rendered = build(&empty_page()).expect("it renders");
        for banned in ["generated at", "Generated at", "rendered at"] {
            assert!(
                !rendered.contains(banned),
                "the page embeds a wall clock via '{banned}'"
            );
        }
    }

    /// Rendering twice produces the same bytes
    #[test]
    fn rendering_is_deterministic() {
        let page = empty_page();
        assert_eq!(
            build(&page).expect("it renders"),
            build(&page).expect("it renders")
        );
    }

    /// An uncommitted tree is declared, since every verdict below depends on it
    #[test]
    fn a_dirty_tree_is_declared() {
        let mut page = empty_page();
        page.dirty = true;
        let rendered = build(&page).expect("it renders");
        assert!(rendered.contains("with uncommitted changes"));
    }

    /// A capture with a layer that is not current is counted in the summary
    #[test]
    fn the_summary_counts_what_is_not_current() {
        let mut page = empty_page();
        page.statuses = vec![CaptureStatus {
            label: "old".to_string(),
            captured: None,
            layers: vec![Layer::Micro],
            code: vec![(Layer::Micro, CodeVerdict::NoProvenance)],
            env: EnvVerdict::Unknown,
            partial: false,
        }];
        let rendered = build(&page).expect("it renders");
        assert!(rendered.contains("1 of them have at least one layer"), "{rendered}");
    }

    /// Set differences are stated in both directions
    #[test]
    fn set_differences_are_stated_both_ways() {
        let comparison = MicroComparison {
            rows: Vec::new(),
            only_in_baseline: vec!["gone".to_string()],
            only_in_run: vec!["added".to_string()],
        };
        let rendered = set_differences(&comparison, "run", "base");
        assert!(rendered.contains("`base` measures 1 benchmarks that `run` does not"));
        assert!(rendered.contains("`run` measures 1 benchmarks that `base` predates"));
    }
}
