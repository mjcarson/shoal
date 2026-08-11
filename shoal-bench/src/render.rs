//! Turning the artifacts into the book's benchmark results page
//!
//! The page lands at `docs/src/operations/benchmark-results.md` and is committed. See
//! [`page`] for why it has to be, and for the four rules that keep `--check` meaningful.

pub mod badges;
pub mod chart;
pub mod page;
pub mod tables;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use crate::cli::RenderArgs;
use crate::fingerprint::{self, Facts, RealFacts};
use crate::registry::Layer;
use crate::render::page::{Page, Snapshot};
use crate::stale;
use crate::store::Store;

/// Where the generated page goes
pub const PAGE_PATH: &str = "docs/src/operations/benchmark-results.md";

/// Runs `shoal-bench render`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_render(store: &Store, args: &RenderArgs) -> Result<i32> {
    // gather everything the page is built from, then build it
    let page = gather(store, args)?;
    let rendered = page::build(&page)?;
    let target = args
        .out
        .clone()
        .unwrap_or_else(|| store.root().join(PAGE_PATH));
    // a check regenerates into memory and compares, writing nothing
    if args.check {
        return check(&target, &rendered);
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&target, &rendered)
        .with_context(|| format!("writing {}", target.display()))?;
    println!(
        "wrote {} ({} captures, {} bytes)",
        target.display(),
        page.statuses.len(),
        rendered.len()
    );
    Ok(0)
}

/// Compares a freshly rendered page against the committed one
///
/// # Arguments
///
/// * `target` - The committed page
/// * `rendered` - What the page would be now
fn check(target: &std::path::Path, rendered: &str) -> Result<i32> {
    // a page that does not exist is as out of date as one that differs
    let committed = match std::fs::read_to_string(target) {
        Ok(body) => body,
        Err(_) => {
            bail!(
                "{} does not exist. Run `shoal-bench render` to create it - the book will not \
                 build without it, because create-missing is off.",
                target.display()
            );
        }
    };
    if committed == rendered {
        println!("{} is up to date", target.display());
        return Ok(0);
    }
    // say where they first differ, since the page is mostly svg and a plain "they differ" would
    // leave nowhere to start
    let at = committed
        .lines()
        .zip(rendered.lines())
        .position(|(left, right)| left != right);
    let detail = match at {
        Some(line) => format!("first differing line is {}", line + 1),
        None => format!(
            "the committed page has {} lines and a fresh one has {}",
            committed.lines().count(),
            rendered.lines().count()
        ),
    };
    bail!(
        "{} is out of date ({detail}). Run `shoal-bench render`.\n\nThis is expected after a \
         commit: the page states which commit it was rendered against, and the staleness verdicts \
         on it are relative to that commit.",
        target.display()
    )
}

/// Reads every artifact the page is built from
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
fn gather(store: &Store, args: &RenderArgs) -> Result<Page> {
    let facts = RealFacts::new(store.root());
    // what the tree is now, which is what every verdict on the page is relative to
    let now = fingerprint::current(store, &facts, false)?;
    let (_, statuses) = stale::status_of(store, &facts, &[])?;
    // The page is an output of this command, not an input to it. Counting it would make the page
    // report itself as an uncommitted change the moment it was written, so rendering twice would
    // produce two different files and `--check` could never pass.
    let dirty = facts
        .git_dirty()
        .unwrap_or_default()
        .iter()
        .any(|path| path.as_path() != std::path::Path::new(PAGE_PATH));
    // every capture, and whatever each of them produced
    let mut timeline: Vec<Snapshot> = Vec::new();
    for label in store.labels()? {
        let mut snapshot = Snapshot {
            label: label.clone(),
            ..Snapshot::default()
        };
        // the micro layer carries the capture timestamp, which is what orders the page. it has to
        // come from a committed artifact rather than a file's mtime: git does not preserve mtimes,
        // so ordering by them would render differently in two checkouts of the same commit.
        let micro_path = store.run_artifact(&label, Layer::Micro);
        if micro_path.is_file() {
            let capture = store.read_micro(&micro_path)?;
            snapshot.captured = capture.captured.clone();
            snapshot.micro = Some(capture);
        }
        let macro_path = store.run_artifact(&label, Layer::Macro);
        if macro_path.is_file() {
            snapshot.macro_layer = Some(store.read_macro(&macro_path)?);
        }
        let hotpath_path = store.run_artifact(&label, Layer::Hotpath);
        if hotpath_path.is_file() {
            snapshot.hotpath = Some(store.read_hotpath(&hotpath_path)?);
        }
        let stages_path = store.run_artifact(&label, Layer::Stages);
        if stages_path.is_file() {
            snapshot.stages = Some(store.read_stages(&stages_path)?);
        }
        timeline.push(snapshot);
    }
    // oldest first, breaking ties on the label so the order is always defined
    timeline.sort_by(|left, right| {
        left.captured
            .cmp(&right.captured)
            .then_with(|| left.label.cmp(&right.label))
    });
    // which capture the current numbers come from: the caller's choice, or the most recent one
    // that produced a micro layer
    let current = match &args.current {
        Some(label) => label.clone(),
        None => timeline
            .iter()
            .rev()
            .find(|snapshot| snapshot.micro.is_some())
            .map(|snapshot| snapshot.label.clone())
            .unwrap_or_default(),
    };
    // the two baselines, each optional so a tree without them still renders
    let frozen = store
        .resolve_micro(&args.baseline)
        .ok()
        .map(|(_, capture)| (args.baseline.clone(), capture));
    let trailing = store
        .resolve_micro(&args.trailing)
        .ok()
        .map(|(_, capture)| (args.trailing.clone(), capture));
    Ok(Page {
        head_short: now.code.head_short.clone(),
        dirty,
        host: now.env.hostname.clone(),
        governor: now.env.governor.clone(),
        timeline,
        statuses,
        current,
        frozen,
        trailing,
        repeats: repeat_groups(store)?,
    })
}

/// Groups the repeat captures into sets, and measures each set's spread
///
/// A repeat set is named by everything before the trailing `-repeatN`, so
/// `B1-performance-repeat1` through `B1-performance-repeat4` are one set.
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
fn repeat_groups(store: &Store) -> Result<Vec<(String, Vec<(f64, f64)>)>> {
    use std::collections::BTreeMap;

    // gather the captures of each set
    let mut sets: BTreeMap<String, Vec<crate::model::micro::MicroCapture>> = BTreeMap::new();
    for (name, capture) in store.repeats()? {
        // strip the trailing repeat number to get the set's name
        let base = match name.rfind("-repeat") {
            Some(at) => name[..at].to_string(),
            None => name.clone(),
        };
        sets.entry(base).or_default().push(capture);
    }
    // and measure each set's spread, dropping any that is not actually a set of repeats
    let mut groups = Vec::new();
    for (name, captures) in sets {
        let points = chart::noise_band::spread(&captures);
        if points.is_empty() {
            continue;
        }
        groups.push((name, points));
    }
    Ok(groups)
}

/// Where the page is written, for the tests and for the promote command's messages
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
pub fn page_path(store: &Store) -> PathBuf {
    store.root().join(PAGE_PATH)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The repeat sets in the tree group by their name, not by their number
    #[test]
    fn repeats_group_into_sets() {
        let store = Store::new(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("shoal-bench has a parent"),
        );
        let groups = repeat_groups(&store).expect("the repeats are readable");
        let names: Vec<&str> = groups.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["B0-powersave", "B1-performance"]);
        // and each set produced a spread for every benchmark all four of its repeats measured
        for (name, points) in &groups {
            assert!(!points.is_empty(), "{name} produced no spreads");
        }
    }
}
