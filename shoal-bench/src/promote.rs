//! Advancing a baseline to a captured run
//!
//! A comparison is only as good as what it is against, so this is the one command that can make
//! every future comparison quietly wrong. It refuses three things by default, and each refusal
//! has an override that is recorded rather than merely permitted:
//!
//! - **The frozen baseline is never written.** Not with a flag, not with a force. `B1-performance`
//!   is what says how far the whole series of changes has come, and a baseline that can be
//!   advanced cannot answer that.
//! - **A partial capture is not promoted.** A baseline missing benchmarks narrows every later
//!   comparison silently, and a benchmark that vanished from a comparison is how a regression gets
//!   missed.
//! - **A capture that no longer describes the current code is not promoted**, because the next
//!   change would then be judged against a measurement of something else.

use anyhow::{Context, Result, bail};

use crate::cli::PromoteArgs;
use crate::fingerprint::RealFacts;
use crate::registry::Layer;
use crate::stale::{self, CodeVerdict};
use crate::store::{FROZEN_BASELINE, Store};

/// Runs `shoal-bench promote`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_promote(store: &Store, args: &PromoteArgs) -> Result<i32> {
    // the frozen baseline is frozen, and no flag changes that
    if args.to == FROZEN_BASELINE {
        bail!(
            "{FROZEN_BASELINE} is the frozen baseline and is never overwritten - it is what says \
             how far the whole series of changes has come. Promote to a different name."
        );
    }
    // the capture has to exist and has to have a micro layer, which is what a baseline is
    let source = store.run_artifact(&args.label, Layer::Micro);
    if !source.is_file() {
        bail!(
            "no micro capture for '{}' at {}",
            args.label,
            source.display()
        );
    }
    let capture = store.read_micro(&source)?;
    let meta = store.read_meta(&args.label)?;
    // a partial capture makes a narrower baseline than it looks like
    if let Some(meta) = &meta
        && meta.partial
        && !args.force_partial
    {
        bail!(
            "'{}' captured {} of {} benchmarks with filter {:?}. Promoting it would silently \
             narrow every comparison taken against it afterwards. Re-run without a filter, or \
             pass --force-partial.",
            args.label,
            meta.selected,
            meta.registry_total,
            meta.filter
        );
    }
    // and one that no longer describes the current code would judge the next change against a
    // measurement of something else
    let facts = RealFacts::new(store.root());
    let (_, reports) = stale::status_of(store, &facts, std::slice::from_ref(&args.label))?;
    if let Some(report) = reports.first()
        && let Some(verdict) = report.verdict(Layer::Micro)
        && !verdict.is_current()
        && !args.allow_stale
    {
        // a capture with no provenance is the ordinary state of everything captured before this
        // tool existed, so it is described rather than scolded
        let advice = match verdict {
            CodeVerdict::NoProvenance => {
                "it was captured before provenance was recorded, so there is no way to tell"
            }
            _ => "re-capture it against the current tree",
        };
        bail!(
            "'{}' is {} for the micro layer - {advice}. Pass --allow-stale to promote it anyway.",
            args.label,
            verdict.label()
        );
    }
    // write the baseline, and record what it came from beside it
    let target = store.baseline_path(&args.to);
    crate::store::write_json(&target, &capture)?;
    // the provenance travels with the baseline, so a comparison against it can still say where it
    // came from once the run it was promoted from is long gone
    if let Some(meta) = meta {
        let meta_target = store
            .baselines_dir()
            .join(format!("{}.meta.json", args.to));
        crate::store::write_json(&meta_target, &meta)
            .with_context(|| format!("recording the provenance of baseline {}", args.to))?;
    }
    println!(
        "promoted {} ({} benchmarks) to {}",
        args.label,
        capture.benchmarks.len(),
        target.display()
    );
    Ok(0)
}
