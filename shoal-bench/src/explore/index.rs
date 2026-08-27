//! Projects the committed corpus into the shape the explorer draws
//!
//! # The only place the two vocabularies meet
//!
//! This is the one module that reads a capture type and writes an index type. Everything upstream
//! of it speaks `MacroCaptureV2` and `CaptureStatus`; everything downstream speaks
//! `shoal_top::index`. Keeping that boundary in one file is what lets the explorer be a crate that
//! compiles for a browser, which `shoal-bench` itself cannot.
//!
//! # It is a projection of a `Page`, not of a `Store`
//!
//! [`build`] takes the same [`Page`] that `render` builds its pages from, and touches no
//! filesystem. Two consequences, both wanted: the explorer and the committed pages can never
//! disagree about what the corpus holds, and if `render` is ever reimplemented on top of the
//! explorer this function is already the shared half.
//!
//! # What it drops, and what it refuses to invent
//!
//! Thirteen megabytes become about half of one. Per-run detail, per-operation counters and the
//! repeated configuration block all go. What does **not** happen is an absent measurement becoming
//! a present one: every accessor here returns `Option` and every `None` is written as an absent
//! field, because a workload a capture never ran did not run slowly.

use std::collections::BTreeMap;

use shoal_top::index::{
    Capture, ConfFactsLite, FamilyText, Index, Layer as IndexLayer, MacroPoint, OpStats,
    ScaleFactsLite, Timing, Verdict, Workload, INDEX_VERSION,
};

use crate::model::macro_layer::{ConfFacts, ScaleFacts, Timing as CaptureTiming, WorkloadCapture};
use crate::model::meta::CaptureMeta;
use crate::registry::Layer;
use crate::render::family::{self, FAMILIES};
use crate::render::page::Page;
use crate::stale::{CodeVerdict, EnvVerdict};

/// Builds the explorer's index from everything the corpus holds
///
/// # Arguments
///
/// * `page` - Every capture and what was concluded about each, as `render` gathers it
/// * `metas` - What each capture recorded about the machine it ran on, keyed by label
pub fn build(page: &Page, metas: &BTreeMap<String, CaptureMeta>) -> Index {
    // the four blocks, carried whole out of the one place they are written
    let (families, family_at) = families();
    // every workload identifier anywhere in the corpus, interned once
    let (workloads, workload_at) = workloads(page, &family_at);
    // the captures, in the order `render` sorted them, which is oldest first
    let captures = captures(page, metas);
    // the measurements, and the two tables they reference
    let mut scales: Vec<ScaleFactsLite> = Vec::new();
    let mut confs: Vec<ConfFactsLite> = Vec::new();
    let mut macro_points: Vec<MacroPoint> = Vec::new();
    for (at, snapshot) in page.timeline.iter().enumerate() {
        let Some(measured) = snapshot.macro_layer.as_ref() else {
            continue;
        };
        // `workloads` is a `BTreeMap`, so this walks in one order and two builds agree
        for (id, capture) in &measured.workloads {
            let Some(workload) = workload_at.get(id.as_str()) else {
                continue;
            };
            macro_points.push(point(
                at as u32,
                *workload,
                capture,
                &mut scales,
                &mut confs,
            ));
        }
    }
    // sorted on exactly the pair `Index::macro_point` binary searches
    macro_points.sort_by(|left, right| {
        left.capture
            .cmp(&right.capture)
            .then_with(|| left.workload.cmp(&right.workload))
    });
    Index {
        version: INDEX_VERSION,
        head_short: page.head_short.clone(),
        dirty: page.dirty,
        captures,
        families,
        workloads,
        scales,
        confs,
        macro_points,
    }
}

/// Carries the declared families across, and remembers where each one landed
///
/// The four blocks are **copied, never retyped**. `crate::render::family` is where they are written
/// and where the tests that enforce them live; this only moves them to a crate a browser can link.
fn families() -> (Vec<FamilyText>, BTreeMap<&'static str, u32>) {
    let mut families = Vec::with_capacity(FAMILIES.len());
    let mut at = BTreeMap::new();
    for (position, declared) in FAMILIES.iter().enumerate() {
        at.insert(declared.name, position as u32);
        families.push(FamilyText {
            name: declared.name.to_string(),
            title: declared.title.to_string(),
            surface_title: declared.surface.title().to_string(),
            // a bare file name, so the explorer can link to the committed page beside it
            surface_link: declared.surface.link().to_string(),
            what_it_measures: declared.what_it_measures.to_string(),
            how_to_read_it: declared.how_to_read_it.to_string(),
            what_would_make_it_wrong: declared.what_would_make_it_wrong.to_string(),
            what_it_cannot_say: declared.what_it_cannot_say.to_string(),
        });
    }
    (families, at)
}

/// Every workload identifier the corpus mentions, sorted, with the family that explains each
///
/// # Arguments
///
/// * `page` - The corpus to walk
/// * `family_at` - Where each declared family landed in the index
fn workloads(
    page: &Page,
    family_at: &BTreeMap<&'static str, u32>,
) -> (Vec<Workload>, BTreeMap<String, u32>) {
    // gather every identifier any capture measured, deduplicated by the set itself
    let mut ids: Vec<&str> = page
        .timeline
        .iter()
        .filter_map(|snapshot| snapshot.macro_layer.as_ref())
        .flat_map(|measured| measured.workloads.keys().map(String::as_str))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    let mut workloads = Vec::with_capacity(ids.len());
    let mut at = BTreeMap::new();
    for (position, id) in ids.into_iter().enumerate() {
        at.insert(id.to_string(), position as u32);
        workloads.push(Workload {
            id: id.to_string(),
            // a workload no family claims is still carried, and the picker says so. it is a gap in
            // the documentation rather than a reason to hide a measurement
            family: family::family_for(id).and_then(|found| family_at.get(found.name).copied()),
        });
    }
    (workloads, at)
}

/// Every capture, with everything that decides what it may be drawn against
///
/// # Arguments
///
/// * `page` - The corpus to walk
/// * `metas` - What each capture recorded about the machine it ran on, keyed by label
fn captures(page: &Page, metas: &BTreeMap<String, CaptureMeta>) -> Vec<Capture> {
    let mut captures = Vec::with_capacity(page.timeline.len());
    for snapshot in &page.timeline {
        // the verdicts are keyed by label rather than by position, because `statuses` is built by
        // a different walk of the tree and nothing guarantees the two orders agree
        let status = page
            .statuses
            .iter()
            .find(|candidate| candidate.label == snapshot.label);
        // what the capture recorded about its own machine, which several captures predate entirely
        let meta = metas.get(&snapshot.label);
        captures.push(Capture {
            label: snapshot.label.clone(),
            captured: snapshot.captured.clone(),
            head_short: meta.map(|meta| meta.code.head_short.clone()).unwrap_or_default(),
            dirty: meta.is_some_and(|meta| meta.code.dirty),
            layers: snapshot_layers(snapshot),
            complete: snapshot.complete.iter().copied().map(layer).collect(),
            partial: status.is_some_and(|status| status.partial),
            code: status
                .map(|status| {
                    status
                        .code
                        .iter()
                        .map(|(at, verdict)| (layer(*at), code_verdict(verdict)))
                        .collect()
                })
                .unwrap_or_default(),
            env: status
                .map(|status| env_verdict(&status.env))
                .unwrap_or_else(|| Verdict {
                    label: "environment unknown".to_string(),
                    current: None,
                    fields: Vec::new(),
                }),
            // empty where a capture recorded nothing, which `Index::incomparable` reads as
            // agreeing with anything - there is no evidence it does not
            env_digest: meta.map(|meta| meta.env.digest.clone()).unwrap_or_default(),
            host: meta.map(|meta| meta.env.hostname.clone()).unwrap_or_default(),
            governor: meta.map(|meta| meta.env.governor.clone()).unwrap_or_default(),
            rustc: meta.map(|meta| meta.env.rustc.clone()).unwrap_or_default(),
            cpu_model: meta.map(|meta| meta.env.cpu_model.clone()).unwrap_or_default(),
        });
    }
    captures
}

/// Which layers a capture produced an artifact for, in presentation order
///
/// # Arguments
///
/// * `snapshot` - The capture to read
fn snapshot_layers(snapshot: &crate::render::page::Snapshot) -> Vec<IndexLayer> {
    let mut layers = Vec::new();
    // one check per layer, in the order the layers are declared, so the list reads the same way
    // everywhere it is printed
    if snapshot.micro.is_some() {
        layers.push(IndexLayer::Micro);
    }
    if snapshot.macro_layer.is_some() {
        layers.push(IndexLayer::Macro);
    }
    if snapshot.hotpath.is_some() {
        layers.push(IndexLayer::Hotpath);
    }
    if snapshot.stages.is_some() {
        layers.push(IndexLayer::Stages);
    }
    layers
}

/// Mirrors one layer into the explorer's copy of the enum
///
/// # Arguments
///
/// * `layer` - The layer to mirror
fn layer(layer: Layer) -> IndexLayer {
    // `shoal-bench/tests/explore_index.rs` asserts this mapping is total and order preserving, so
    // the two enums cannot drift without a test failing
    match layer {
        Layer::Micro => IndexLayer::Micro,
        Layer::Macro => IndexLayer::Macro,
        Layer::Hotpath => IndexLayer::Hotpath,
        Layer::Stages => IndexLayer::Stages,
    }
}

/// Flattens a code verdict into the word and the one bit a badge needs
///
/// # Arguments
///
/// * `verdict` - What was concluded about a layer
fn code_verdict(verdict: &CodeVerdict) -> Verdict {
    // `None` rather than `false` for a capture that recorded nothing. Several captures predate the
    // tool, and answering either way about them would be inventing the answer
    let current = match verdict {
        CodeVerdict::NoProvenance => None,
        CodeVerdict::Fresh | CodeVerdict::Unaffected { .. } => Some(true),
        CodeVerdict::Uncommitted { .. } | CodeVerdict::Stale { .. } | CodeVerdict::Diverged => {
            Some(false)
        }
    };
    Verdict {
        label: verdict.label(),
        current,
        fields: Vec::new(),
    }
}

/// Flattens an environment verdict, keeping the fields that explain a mismatch
///
/// # Arguments
///
/// * `verdict` - What was concluded about where the capture ran
fn env_verdict(verdict: &EnvVerdict) -> Verdict {
    // the fields are what give a tooltip something to name; "incomparable" on its own gives nobody
    // anything to act on
    let (current, fields) = match verdict {
        EnvVerdict::Unknown => (None, Vec::new()),
        EnvVerdict::Comparable => (Some(true), Vec::new()),
        EnvVerdict::Incomparable { fields } => (Some(false), fields.clone()),
    };
    Verdict {
        label: verdict.label(),
        current,
        fields,
    }
}

/// Projects one workload's measurement, interning the facts it shares with others
///
/// # Arguments
///
/// * `capture` - Which capture this measurement came from
/// * `workload` - Which workload it measured
/// * `measured` - The measurement itself
/// * `scales` - The scale table to intern into
/// * `confs` - The configuration table to intern into
fn point(
    capture: u32,
    workload: u32,
    measured: &WorkloadCapture,
    scales: &mut Vec<ScaleFactsLite>,
    confs: &mut Vec<ConfFactsLite>,
) -> MacroPoint {
    // every percentile of every operation, keyed by the name it was recorded under
    let mut ops = BTreeMap::new();
    for name in measured.op_names() {
        let Some(stats) = op_stats(measured, &name) else {
            continue;
        };
        ops.insert(name.to_string(), stats);
    }
    MacroPoint {
        capture,
        workload,
        scale: intern(scales, scale_facts(&measured.scale)),
        conf: measured
            .conf
            .as_ref()
            .map(|found| intern(confs, conf_facts(found))),
        conf_digest: measured
            .conf
            .as_ref()
            .map(|found| found.digest.clone())
            .unwrap_or_default(),
        timing: match measured.timing {
            CaptureTiming::PerBatch => Timing::PerBatch,
            CaptureTiming::PerQuery => Timing::PerQuery,
        },
        // every rate is the accessor's own answer, `None` included. a workload that counted no
        // queries did not answer none of them
        ops_per_sec: measured.ops_per_sec(),
        rows_per_sec: Some(measured.rows_per_sec()).filter(|rate| *rate > 0.0),
        bytes_per_sec: measured.bytes_per_sec(),
        wall_clock_ns: match measured.median_wall_clock_ns() {
            // a zero wall clock is not a run that happened
            0 => None,
            found => u64::try_from(found).ok(),
        },
        wall_clock_interval_ns: measured.wall_clock_interval_ns(),
        spread_pct: measured.spread_pct,
        runs: measured.runs,
        ops,
    }
}

/// Reads every percentile of one operation, when the measurement recorded them
///
/// # Arguments
///
/// * `measured` - The measurement to read
/// * `op` - Which operation to read
fn op_stats(measured: &WorkloadCapture, op: &str) -> Option<OpStats> {
    // an operation missing any rank is not a partial summary to be filled in with zeroes, it is a
    // shape this projection does not understand, so it is dropped whole
    Some(OpStats {
        count: measured.ops.get(op)?.count,
        min_ns: u64::try_from(measured.stat_ns(op, "min")?).ok()?,
        p50_ns: u64::try_from(measured.stat_ns(op, "p50")?).ok()?,
        p90_ns: u64::try_from(measured.stat_ns(op, "p90")?).ok()?,
        p95_ns: u64::try_from(measured.stat_ns(op, "p95")?).ok()?,
        p99_ns: u64::try_from(measured.stat_ns(op, "p99")?).ok()?,
        avg_ns: u64::try_from(measured.stat_ns(op, "avg")?).ok()?,
        max_ns: u64::try_from(measured.stat_ns(op, "max")?).ok()?,
    })
}

/// Trims one measurement's scale facts to what the explorer groups and labels by
///
/// # Arguments
///
/// * `scale` - The facts the measurement recorded
fn scale_facts(scale: &ScaleFacts) -> ScaleFactsLite {
    ScaleFactsLite {
        scale: scale.scale.clone(),
        rows: scale.rows,
        row_bytes: scale.row_bytes,
        keys: scale.keys,
        concurrency: scale.concurrency,
        clients: scale.clients,
        read_pct: scale.read_pct,
        row_profile: scale.row_profile.clone(),
        distribution: scale.distribution.clone(),
        table_kind: scale.table_kind.clone(),
    }
}

/// Trims one measurement's configuration to the fields a sweep moves
///
/// The digest is deliberately not carried here. Interning on it would give one entry per workload
/// rather than one per configuration, which is thirty times the rows for no more information; it
/// rides on the measurement instead.
///
/// # Arguments
///
/// * `conf` - The configuration the measurement recorded
fn conf_facts(conf: &ConfFacts) -> ConfFactsLite {
    ConfFactsLite {
        shards: conf.shards,
        memory: conf.memory.clone(),
        durability: conf.durability.clone(),
        tls: conf.tls,
        latency_buffer_size: conf.latency_buffer_size,
        latency_write_behind: conf.latency_write_behind,
        intent_log_size: conf.intent_log_size.clone(),
        throughput_buffer_size: conf.throughput_buffer_size,
        throughput_write_behind: conf.throughput_write_behind,
        max_frame_bytes: conf.max_frame_bytes,
    }
}

/// Puts a value in a table if it is not there already, and says where it landed
///
/// # Arguments
///
/// * `table` - The table to intern into
/// * `value` - The value to intern
fn intern<T: PartialEq>(table: &mut Vec<T>, value: T) -> u32 {
    // linear rather than hashed on purpose: the tables hold a few hundred entries at most, and a
    // `Vec` keeps the index stable and the serialized order the insertion order
    match table.iter().position(|seen| *seen == value) {
        Some(at) => at as u32,
        None => {
            table.push(value);
            (table.len() - 1) as u32
        }
    }
}
