//! What every generated page is built from
//!
//! # Why the pages have to be deterministic
//!
//! They are committed, because `create-missing = false` means a `SUMMARY.md` entry pointing at a
//! file that is not there fails the whole book build, and because regenerating them needs a twelve
//! core machine and a populated `/opt/shoal`. A clean checkout has to be able to build the book.
//!
//! That makes `shoal-bench render --check` the thing that says whether the committed pages are
//! current, and it can only do that if rendering the same inputs twice produces the same bytes.
//! Four rules keep it that way, and all four are easy to break by accident:
//!
//! 1. **No wall clock reaches a page.** Capture timestamps do; "generated at" does not.
//! 2. **Every map walked while rendering is a `BTreeMap`, and every list is explicitly sorted.**
//! 3. **Every float goes through [`crate::fmt`]**, at a fixed precision. No `{:?}` on an `f64`.
//! 4. **Chart geometry is a pure function of the data** and the fixed canvas size.
//!
//! The current commit *is* on every page, deliberately. The staleness verdicts are relative to it,
//! so a page that did not say which commit it was rendered against would be making claims with no
//! referent. The consequence is that `--check` fails after any commit, which is not a bug: it is
//! the page telling you it no longer describes this tree.
//!
//! This module holds only what the pages share. What each one draws lives in
//! [`crate::render::pages`].

use std::collections::BTreeSet;

use crate::model::hotpath::HotpathProfile;
use crate::model::macro_layer::MacroCaptureV2;
use crate::model::micro::MicroCapture;
use crate::model::stages::StageReports;
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
    pub stages: Option<StageReports>,
    /// Which of the layers it produced covered the whole of the registry for that layer
    ///
    /// A capture narrowed by a filter measured some of a layer and not the rest of it, which is
    /// a different thing from not having measured that layer at all. Drawing a page from one
    /// silently drops every arm the filter excluded, so this is what
    /// [`Page::current_for`] screens on.
    pub complete: BTreeSet<Layer>,
}

impl Snapshot {
    /// Get whether this capture produced a given layer at all
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to look for
    pub fn has(&self, layer: Layer) -> bool {
        match layer {
            Layer::Micro => self.micro.is_some(),
            Layer::Macro => self.macro_layer.is_some(),
            Layer::Hotpath => self.hotpath.is_some(),
            Layer::Stages => self.stages.is_some(),
        }
    }
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
    pub fn current(&self) -> Option<&Snapshot> {
        self.current_for(Layer::Micro)
    }

    /// The capture one layer's current numbers are drawn from
    ///
    /// Resolved **per layer**, which is the whole of [item
    /// 79](../../../docs/src/appendix/resolved/micro-only-capture-current.md). A capture does not
    /// have to measure everything: `--layer micro` produces no macro layer at all and `--group`
    /// produces part of one. A single label shared by every page meant the newest capture decided
    /// what *every* page drew, so a micro only capture left the seven macro pages saying nothing
    /// had ever measured a mixture while eleven macro captures sat in the same directory.
    ///
    /// Each page therefore names the newest capture of the layer it draws, and no page ever mixes
    /// two - which is the line [Baseline](../../../docs/src/performance/baseline.md) draws and
    /// this keeps.
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer the page draws
    pub fn current_for(&self, layer: Layer) -> Option<&Snapshot> {
        // a capture the caller named is an instruction rather than a preference, so it wins
        // whenever it measured this layer at all
        if let Some(named) = self
            .timeline
            .iter()
            .find(|snapshot| snapshot.label == self.current && snapshot.has(layer))
        {
            return Some(named);
        }
        // otherwise the most recent capture that measured the whole of this layer
        self.timeline
            .iter()
            .rev()
            .find(|snapshot| snapshot.complete.contains(&layer) && snapshot.has(layer))
            // and a tree that only ever captured part of it draws the newest of those, because a
            // page built from a filtered capture still beats a page built from nothing
            .or_else(|| self.timeline.iter().rev().find(|snapshot| snapshot.has(layer)))
    }

    /// What was concluded about one capture
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    pub fn status(&self, label: &str) -> Option<&CaptureStatus> {
        self.statuses
            .iter()
            .find(|status| status.label == label)
    }
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
pub mod tests {
    use super::*;
    use crate::registry::Layer;
    use crate::stale::EnvVerdict;

    /// Builds a page with nothing captured
    ///
    /// Shared with [`crate::render::pages`], whose tests assert that every page renders from an
    /// empty tree - which is what a clean checkout building the book depends on.
    pub fn empty_page() -> Page {
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

    /// A page with no matching capture finds nothing rather than the first one it has
    #[test]
    fn a_missing_current_capture_is_absent() {
        let mut page = empty_page();
        page.timeline = vec![Snapshot {
            label: "other".to_string(),
            ..Snapshot::default()
        }];
        assert!(page.current().is_none());
    }

    /// A capture's status is found by its label
    #[test]
    fn a_status_is_found_by_label() {
        let mut page = empty_page();
        page.statuses = vec![CaptureStatus {
            label: "old".to_string(),
            captured: None,
            layers: vec![Layer::Micro],
            code: vec![(Layer::Micro, CodeVerdict::NoProvenance)],
            env: EnvVerdict::Unknown,
            partial: false,
        }];
        assert!(page.status("old").is_some());
        assert!(page.status("new").is_none());
    }

    /// A capture that measured no macro layer does not become the macro pages' source
    ///
    /// The reproduction for [item 79](../../../docs/src/appendix/known-issues.md). A capture taken
    /// with `--layer micro` is the newest capture and produced no macro layer at all, so a single
    /// `current` shared by every page takes the seven macro pages down with it.
    #[test]
    fn a_capture_with_no_macro_layer_does_not_become_the_macro_source() {
        let mut page = empty_page();
        page.timeline = vec![
            Snapshot {
                label: "full".to_string(),
                captured: "2026-08-24T01:00:00Z".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                macro_layer: Some(MacroCaptureV2::new(None)),
                complete: [Layer::Micro, Layer::Macro].into_iter().collect(),
                ..Snapshot::default()
            },
            Snapshot {
                label: "micro-only".to_string(),
                captured: "2026-08-25T01:00:00Z".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                complete: [Layer::Micro].into_iter().collect(),
                ..Snapshot::default()
            },
        ];
        page.current = String::new();
        // the micro layer comes from the newest capture that has one
        assert_eq!(
            page.current_for(Layer::Micro).map(|snapshot| snapshot.label.as_str()),
            Some("micro-only")
        );
        // and the macro layer comes from the newest capture that has *that*, not from a capture
        // that never measured it
        assert_eq!(
            page.current_for(Layer::Macro).map(|snapshot| snapshot.label.as_str()),
            Some("full")
        );
    }

    /// A capture that measured part of a layer does not outrank one that measured all of it
    ///
    /// The other half of item 79. A filtered capture is newer and has the layer, but holds only
    /// the arms the filter selected, so drawing a page from it drops every other arm.
    #[test]
    fn a_partial_capture_does_not_outrank_a_complete_one() {
        let mut page = empty_page();
        page.timeline = vec![
            Snapshot {
                label: "full".to_string(),
                captured: "2026-08-24T01:00:00Z".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                complete: [Layer::Micro].into_iter().collect(),
                ..Snapshot::default()
            },
            Snapshot {
                label: "filtered".to_string(),
                captured: "2026-08-25T01:00:00Z".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                ..Snapshot::default()
            },
        ];
        page.current = String::new();
        assert_eq!(
            page.current_for(Layer::Micro).map(|snapshot| snapshot.label.as_str()),
            Some("full")
        );
    }

    /// A capture the caller named is drawn from even when it measured part of a layer
    ///
    /// `--current` is an instruction rather than a preference, so the screens above only decide
    /// the default.
    #[test]
    fn a_named_capture_wins_over_the_newest_complete_one() {
        let mut page = empty_page();
        page.timeline = vec![
            Snapshot {
                label: "full".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                complete: [Layer::Micro].into_iter().collect(),
                ..Snapshot::default()
            },
            Snapshot {
                label: "filtered".to_string(),
                micro: Some(MicroCapture::new("2026-08-24T00:00:00Z")),
                ..Snapshot::default()
            },
        ];
        page.current = "filtered".to_string();
        assert_eq!(
            page.current_for(Layer::Micro).map(|snapshot| snapshot.label.as_str()),
            Some("filtered")
        );
    }

    /// A capture with a layer that is not fresh is reported as not current
    #[test]
    fn a_capture_with_no_provenance_is_not_current() {
        let statuses = vec![CaptureStatus {
            label: "old".to_string(),
            captured: None,
            layers: vec![Layer::Micro],
            code: vec![(Layer::Micro, CodeVerdict::NoProvenance)],
            env: EnvVerdict::Unknown,
            partial: false,
        }];
        assert!(any_stale(&statuses));
    }
}
