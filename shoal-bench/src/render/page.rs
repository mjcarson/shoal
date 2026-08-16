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

use crate::model::hotpath::HotpathProfile;
use crate::model::macro_layer::MacroCaptureV2;
use crate::model::micro::MicroCapture;
use crate::model::stages::StageReport;
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
    pub fn current(&self) -> Option<&Snapshot> {
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
