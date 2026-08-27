//! Turning the artifacts into the book's performance pages
//!
//! The pages land under `docs/src/performance/` and are committed. See [`page`] for why they have
//! to be, and for the four rules that keep `--check` meaningful, and [`pages`] for why there is
//! more than one of them.

pub mod arms;
pub mod badges;
pub mod chart;
pub mod family;
pub mod page;
pub mod pages;
pub mod tables;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use crate::cli::RenderArgs;
use crate::fingerprint::{self, Facts, RealFacts};
use crate::registry::Layer;
use crate::render::family::Surface;
use crate::render::page::{Page, Snapshot};
use crate::render::pages::PAGES;
use crate::stale;
use crate::store::Store;

/// Where the generated pages go, relative to the repository root
///
/// Each page's own path is on [`Surface`]; this is the directory they share, which is what a
/// `--out` override replaces and what the dirty check has to ignore.
pub const PAGE_DIR: &str = "docs/src/performance";

/// Runs `shoal-bench render`
///
/// Writes every page, or checks every page, and never a subset of either: a tree holding four
/// current pages and six stale ones is worse than one holding ten stale ones, because nothing on
/// the page says which kind it is.
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_render(store: &Store, args: &RenderArgs) -> Result<i32> {
    // gather everything every page is built from, once
    let page = gather(store, args)?;
    let root = args
        .out
        .clone()
        .unwrap_or_else(|| store.root().join(PAGE_DIR));
    // build them all before writing any, so a page that fails to render does not leave half the
    // set rewritten and the other half describing an older capture
    let mut built: Vec<(PathBuf, String)> = Vec::with_capacity(PAGES.len());
    for spec in PAGES {
        let rendered = (spec.build)(&page)
            .with_context(|| format!("rendering {}", spec.surface.title()))?;
        built.push((page_path_in(&root, spec.surface), rendered));
    }
    if args.check {
        return check_all(&built);
    }
    std::fs::create_dir_all(&root)
        .with_context(|| format!("creating {}", root.display()))?;
    let mut bytes = 0;
    for (target, rendered) in &built {
        std::fs::write(target, rendered)
            .with_context(|| format!("writing {}", target.display()))?;
        bytes += rendered.len();
    }
    println!(
        "wrote {} pages under {} ({} captures, {bytes} bytes)",
        built.len(),
        root.display(),
        page.statuses.len()
    );
    Ok(0)
}

/// Where one page is written, under a root directory
///
/// # Arguments
///
/// * `root` - The directory the pages are written into
/// * `surface` - Which page is wanted
fn page_path_in(root: &std::path::Path, surface: Surface) -> PathBuf {
    // the surface knows its own path relative to the repository, and the file name is the part
    // that survives an `--out` pointing somewhere else entirely
    let name = surface
        .path()
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(surface.path());
    root.join(name)
}

/// Compares every freshly rendered page against the committed one
///
/// # Arguments
///
/// * `built` - Each page's path and what it would be now
fn check_all(built: &[(PathBuf, String)]) -> Result<i32> {
    let mut stale: Vec<String> = Vec::new();
    for (target, rendered) in built {
        match check_one(target, rendered) {
            Ok(()) => println!("{} is up to date", target.display()),
            Err(detail) => stale.push(detail),
        }
    }
    if stale.is_empty() {
        return Ok(0);
    }
    // every stale page is named, rather than the first one found, so one run says how much work
    // there is rather than one page's worth of it
    bail!(
        "{} of {} pages are out of date. Run `shoal-bench render`.\n\n{}\n\nThis is expected \
         after a commit: every page states which commit it was rendered against, and the staleness \
         verdicts on them are relative to that commit.",
        stale.len(),
        built.len(),
        stale.join("\n")
    )
}

/// Whether one committed page is what a fresh render would produce
///
/// # Arguments
///
/// * `target` - The committed page
/// * `rendered` - What the page would be now
fn check_one(target: &std::path::Path, rendered: &str) -> std::result::Result<(), String> {
    // a page that does not exist is as out of date as one that differs, and worse: the book will
    // not build without it, because create-missing is off
    let Ok(committed) = std::fs::read_to_string(target) else {
        return Err(format!("  {} does not exist", target.display()));
    };
    if committed == rendered {
        return Ok(());
    }
    // say where they first differ, since a page is mostly svg and a plain "they differ" would
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
    Err(format!("  {} ({detail})", target.display()))
}

/// Reads every artifact the page is built from
///
/// `pub(crate)` rather than private because [`crate::explore::index`] projects the explorer's index
/// out of the same [`Page`]. One reader of the corpus, so the committed pages and the explorer can
/// never disagree about what was captured - which is the property that matters if the explorer is
/// ever to replace the pages.
///
/// It reads the hotpath and stages layers that a v1 index then discards, which costs about a second
/// over thirteen megabytes. That is paid deliberately: it is what makes an attribution section a
/// new arm here rather than a new pass over the corpus.
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub(crate) fn gather(store: &Store, args: &RenderArgs) -> Result<Page> {
    let facts = RealFacts::new(store.root());
    // what the tree is now, which is what every verdict on the page is relative to
    let now = fingerprint::current(store, &facts, false)?;
    let (_, statuses) = stale::status_of(store, &facts, &[])?;
    // The pages are outputs of this command, not inputs to it. Counting them would make every
    // page report itself as an uncommitted change the moment it was written, so rendering twice
    // would produce two different sets and `--check` could never pass.
    let dirty = facts
        .git_dirty()
        .unwrap_or_default()
        .iter()
        .any(|path| !path.starts_with(PAGE_DIR));
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
        // which of those layers covered the whole registry rather than the part a filter selected
        //
        // a page drawn from a filtered capture silently loses every arm the filter excluded, so
        // this is what keeps a narrow capture from outranking a whole one (item 79)
        if let Some(meta) = store.read_meta(&label)? {
            for (layer, record) in &meta.layers {
                if record.complete {
                    snapshot.complete.insert(*layer);
                }
            }
        }
        timeline.push(snapshot);
    }
    // oldest first, breaking ties on the label so the order is always defined
    timeline.sort_by(|left, right| {
        left.captured
            .cmp(&right.captured)
            .then_with(|| left.label.cmp(&right.label))
    });
    // which capture the caller asked every page to draw from, if they asked at all
    //
    // the default is **not** resolved here any more. It used to be "the most recent capture with
    // a micro layer", one label for all eleven pages, which is item 79: a capture that measured
    // one layer decided what every page drew, and the pages it did not measure rendered as though
    // nothing ever had. `Page::current_for` resolves it per layer instead
    let current = args.current.clone().unwrap_or_default();
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

/// Where one page is written, for the tests and for the promote command's messages
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `surface` - Which page is wanted
pub fn page_path(store: &Store, surface: Surface) -> PathBuf {
    store.root().join(surface.path())
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
