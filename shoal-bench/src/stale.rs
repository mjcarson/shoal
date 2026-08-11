//! Whether a committed number still describes the code it was measured on
//!
//! This is the question `scripts/bench.sh` could not answer, because it recorded nothing about the
//! tree a capture was taken from. A number in `docs/perf/runs/` was either believed or not, and
//! there was no way to tell which it should be.
//!
//! There are two independent axes and two flags:
//!
//! - **Code.** Judged per layer, because a change to the client does not touch what the micro
//!   benchmarks measure. Runs from `fresh` through `unaffected` and `stale` to `uncommitted`.
//! - **Environment.** Judged per capture. Two captures taken on different machines, governors or
//!   compilers are `incomparable` however fresh they both are.
//! - **`partial`**, when a filter narrowed the capture to part of the registry.
//! - **`instrumented`**, for the hotpath and stage layers, which attribute time and are never a
//!   latency.
//!
//! # The one asymmetry the whole thing rests on
//!
//! `layer_digest` may only ever *narrow* a `stale` verdict to `unaffected`. It can never turn
//! `stale` into `fresh`, and it is never consulted before the commit hash. The digest is built
//! from a hand maintained list of paths in `docs/perf/sources.json`, so a path missing from that
//! list produces a capture wrongly called `unaffected` - which still shows the commit distance -
//! and never one wrongly called `fresh`. Reverse that order and the hand maintained list silently
//! becomes load bearing.

use anyhow::Result;
use serde::Serialize;

use crate::cli::{Format, StatusArgs};
use crate::fingerprint::{self, Facts, Fingerprint, RealFacts};
use crate::model::meta::CaptureMeta;
use crate::registry::Layer;
use crate::store::Store;

/// What a capture's code state says about one of its layers
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum CodeVerdict {
    /// The capture recorded nothing about the tree it was taken from
    ///
    /// This is how the seven captures taken before this tool existed are reported. Deliberately
    /// neither fresh nor stale: both would assert something that is not known.
    NoProvenance,
    /// A source this layer measures was uncommitted, then or now
    ///
    /// The strongest warning there is. The measured bytes exist in no commit, so the measurement
    /// cannot be located in history at all.
    Uncommitted {
        /// Whether the capture itself was taken on a dirty tree
        at_capture: bool,
        /// Whether the tree is dirty in this layer's sources now
        now: bool,
    },
    /// Taken at this commit, on a clean tree
    Fresh,
    /// Taken at an earlier commit, but nothing this layer measures has changed since
    Unaffected {
        /// How many commits have landed since
        behind: u32,
    },
    /// Taken at an earlier commit, and this layer's sources have changed since
    Stale {
        /// How many commits have landed since
        behind: u32,
    },
    /// Taken at a commit this tree cannot place, so the distance is undefined
    Diverged,
}

impl CodeVerdict {
    /// A short label for a table or a badge
    pub fn label(&self) -> String {
        // the word carries the meaning, so it is never abbreviated to a colour or an icon
        match self {
            CodeVerdict::NoProvenance => "no provenance".to_string(),
            CodeVerdict::Uncommitted { .. } => "uncommitted".to_string(),
            CodeVerdict::Fresh => "fresh".to_string(),
            CodeVerdict::Unaffected { behind } => format!("unaffected · {behind} commits"),
            CodeVerdict::Stale { behind } => format!("stale · {behind} commits"),
            CodeVerdict::Diverged => "diverged".to_string(),
        }
    }

    /// The css class a badge for this verdict is styled with
    pub fn css_class(&self) -> &'static str {
        // one class per verdict, so the stylesheet decides how each is presented
        match self {
            CodeVerdict::NoProvenance => "sc-none",
            CodeVerdict::Uncommitted { .. } => "sc-uncommitted",
            CodeVerdict::Fresh => "sc-fresh",
            CodeVerdict::Unaffected { .. } => "sc-unaffected",
            CodeVerdict::Stale { .. } => "sc-stale",
            CodeVerdict::Diverged => "sc-diverged",
        }
    }

    /// Whether this verdict means the capture still describes the current code
    pub fn is_current(&self) -> bool {
        // being behind is fine as long as nothing this layer measures moved
        matches!(
            self,
            CodeVerdict::Fresh | CodeVerdict::Unaffected { .. }
        )
    }
}

/// Whether two captures were taken somewhere comparable
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum EnvVerdict {
    /// Nothing was recorded about where the capture was taken
    Unknown,
    /// Taken somewhere that matches here
    Comparable,
    /// Taken somewhere that does not
    Incomparable {
        /// Which fields differ
        fields: Vec<String>,
    },
}

impl EnvVerdict {
    /// A short label for a table or a badge
    pub fn label(&self) -> String {
        // name what differs, since "incomparable" alone gives nobody anything to act on
        match self {
            EnvVerdict::Unknown => "environment unknown".to_string(),
            EnvVerdict::Comparable => "comparable".to_string(),
            EnvVerdict::Incomparable { fields } => {
                format!("incomparable · {}", fields.join(", "))
            }
        }
    }
}

/// Everything known about one capture's standing
#[derive(Debug, Clone, Serialize)]
pub struct CaptureStatus {
    /// The capture's name
    pub label: String,
    /// When it was taken, if it recorded that
    pub captured: Option<String>,
    /// Which layers it produced an artifact for
    pub layers: Vec<Layer>,
    /// The code verdict for each layer it produced
    pub code: Vec<(Layer, CodeVerdict)>,
    /// Whether it was taken somewhere comparable to here
    pub env: EnvVerdict,
    /// Whether a filter narrowed it to part of the registry
    pub partial: bool,
}

impl CaptureStatus {
    /// The code verdict for one layer, if this capture produced that layer
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to look up
    pub fn verdict(&self, layer: Layer) -> Option<&CodeVerdict> {
        // a layer the capture never produced has no verdict, which is not the same as a bad one
        self.code
            .iter()
            .find(|(candidate, _)| *candidate == layer)
            .map(|(_, verdict)| verdict)
    }
}

/// Judges one layer of one capture against the current tree
///
/// # Arguments
///
/// * `layer` - Which layer is being judged
/// * `meta` - What the capture recorded, if anything
/// * `now` - What the tree looks like now
/// * `behind` - How many commits have landed since the capture, if that is defined
pub fn code_verdict(
    layer: Layer,
    meta: Option<&CaptureMeta>,
    now: &Fingerprint,
    behind: Option<u32>,
) -> CodeVerdict {
    // a capture that recorded nothing cannot be judged, and saying so is the honest answer
    let Some(meta) = meta else {
        return CodeVerdict::NoProvenance;
    };
    // uncommitted is checked first and outranks everything else: a measurement of bytes that
    // exist in no commit cannot be placed in history at all, however many commits have landed
    let dirty_at_capture = meta.code.dirty_layers.contains(&layer);
    let dirty_now = now.code.dirty_layers.contains(&layer);
    if dirty_at_capture || dirty_now {
        return CodeVerdict::Uncommitted {
            at_capture: dirty_at_capture,
            now: dirty_now,
        };
    }
    // then the commit, which is the coarse check that catches everything
    if meta.code.head == now.code.head {
        return CodeVerdict::Fresh;
    }
    // a commit this tree cannot place has no distance from here
    let Some(behind) = behind else {
        return CodeVerdict::Diverged;
    };
    // only now may the per layer digest speak, and only to narrow the verdict. it can turn stale
    // into unaffected and never into fresh - see the module docs for why that order is the whole
    // safety argument for a hand maintained source list.
    let unchanged = match (
        meta.code.layer_digest.get(&layer),
        now.code.layer_digest.get(&layer),
    ) {
        (Some(before), Some(after)) => before == after,
        // a capture that recorded no digest for this layer cannot claim to be unaffected
        _ => false,
    };
    if unchanged {
        CodeVerdict::Unaffected { behind }
    } else {
        CodeVerdict::Stale { behind }
    }
}

/// Judges whether a capture was taken somewhere comparable to here
///
/// # Arguments
///
/// * `meta` - What the capture recorded, if anything
/// * `now` - What this machine and toolchain are
pub fn env_verdict(meta: Option<&CaptureMeta>, now: &Fingerprint) -> EnvVerdict {
    // a capture that recorded nothing about its machine cannot be placed
    let Some(meta) = meta else {
        return EnvVerdict::Unknown;
    };
    // one hash decides comparability, and the fields are only walked to explain a mismatch
    if meta.env.digest == now.env.digest {
        return EnvVerdict::Comparable;
    }
    let mut fields = Vec::new();
    // name exactly what differs, so the reader knows whether it matters to them
    if meta.env.hostname != now.env.hostname {
        fields.push("host".to_string());
    }
    if meta.env.cpu_model != now.env.cpu_model {
        fields.push("cpu".to_string());
    }
    if meta.env.governor != now.env.governor {
        fields.push(format!(
            "governor {} vs {}",
            meta.env.governor, now.env.governor
        ));
    }
    if meta.env.rustc != now.env.rustc {
        fields.push("rustc".to_string());
    }
    if meta.env.rustflags != now.env.rustflags
        || meta.env.rustflags_env != now.env.rustflags_env
    {
        fields.push("rustflags".to_string());
    }
    // a digest that differs with no field to point at means one of the recorded strings moved in
    // a way this list does not enumerate, which is still a real difference
    if fields.is_empty() {
        fields.push("environment".to_string());
    }
    EnvVerdict::Incomparable { fields }
}

/// Judges every capture in the tree
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `facts` - Where to read the current state from
/// * `only` - Restrict the report to these labels, or every label if empty
pub fn status_of(
    store: &Store,
    facts: &dyn Facts,
    only: &[String],
) -> Result<(Fingerprint, Vec<CaptureStatus>)> {
    // what the tree looks like right now, gathered once for every capture judged against it
    let now = fingerprint::current(store, facts, false)?;
    let mut reports = Vec::new();
    for label in store.labels()? {
        // honour a restriction to particular captures
        if !only.is_empty() && !only.contains(&label) {
            continue;
        }
        let meta = store.read_meta(&label)?;
        // how far this tree has moved since the capture, which is undefined for a commit it
        // cannot place and unnecessary for one it has no record of
        let behind = match &meta {
            Some(meta) if !meta.code.head.is_empty() => facts
                .commits_between(&meta.code.head, &now.code.head)
                .unwrap_or(None),
            _ => None,
        };
        // which layers the capture actually produced, judged one at a time
        let mut layers = Vec::new();
        let mut code = Vec::new();
        for layer in Layer::ALL {
            if !store.run_artifact(&label, layer).is_file() {
                continue;
            }
            layers.push(layer);
            code.push((layer, code_verdict(layer, meta.as_ref(), &now, behind)));
        }
        reports.push(CaptureStatus {
            label,
            captured: meta.as_ref().map(|meta| meta.captured.clone()),
            layers,
            code,
            env: env_verdict(meta.as_ref(), &now),
            partial: meta.as_ref().is_some_and(|meta| meta.partial),
        });
    }
    // newest first where that is known, falling back to the name so the order is always defined
    reports.sort_by(|left, right| {
        right
            .captured
            .cmp(&left.captured)
            .then_with(|| left.label.cmp(&right.label))
    });
    Ok((now, reports))
}

/// Runs `shoal-bench status`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_status(store: &Store, args: &StatusArgs) -> Result<i32> {
    let facts = RealFacts::new(store.root());
    let (now, reports) = status_of(store, &facts, &args.labels)?;
    // print it in whichever shape was asked for
    match args.format {
        Format::Json => {
            println!("{}", serde_json::to_string_pretty(&reports)?);
        }
        Format::Text | Format::Markdown => {
            // what the tree is at, so every verdict below has something to be relative to
            println!(
                "HEAD {} ({}), governor {}, {}",
                now.code.head_short,
                if now.code.dirty {
                    format!("{} paths uncommitted", now.code.dirty_paths)
                } else {
                    "clean".to_string()
                },
                now.env.governor,
                now.env.cpu_model
            );
            println!();
            println!("{:<20} {:<22} {:<8} {}", "capture", "captured", "layer", "verdict");
            for report in &reports {
                for (layer, verdict) in &report.code {
                    println!(
                        "{:<20} {:<22} {:<8} {}{}",
                        report.label,
                        report.captured.as_deref().unwrap_or("-"),
                        layer.as_str(),
                        verdict.label(),
                        if report.partial { "  (partial)" } else { "" }
                    );
                }
                // the environment is a property of the capture, not of one of its layers
                if !matches!(report.env, EnvVerdict::Comparable) {
                    println!("{:<20} {:<22} {:<8} {}", "", "", "", report.env.label());
                }
            }
            println!("\n{} captures", reports.len());
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::model::meta::{CodeFacts, EnvFacts, META_VERSION};

    /// Facts that answer whatever a test says they should
    #[derive(Debug, Clone, Default)]
    struct FakeFacts {
        /// What `HEAD` points at
        head: String,
        /// What is uncommitted
        dirty: Vec<PathBuf>,
        /// How far apart two commits are, or `None` for a commit this tree cannot place
        behind: Option<u32>,
    }

    impl Facts for FakeFacts {
        fn git_head(&self) -> Result<String> {
            Ok(self.head.clone())
        }
        fn git_dirty(&self) -> Result<Vec<PathBuf>> {
            Ok(self.dirty.clone())
        }
        fn commits_between(&self, _from: &str, _to: &str) -> Result<Option<u32>> {
            Ok(self.behind)
        }
        fn read_file(&self, _relative: &Path) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn rust_sources(&self, _relative: &Path) -> Result<Vec<PathBuf>> {
            Ok(Vec::new())
        }
        fn hostname(&self) -> String {
            "jove".to_string()
        }
        fn cpu_model(&self) -> String {
            "test cpu".to_string()
        }
        fn cpu_online(&self) -> usize {
            32
        }
        fn governor(&self) -> String {
            "performance".to_string()
        }
        fn kernel(&self) -> String {
            "test".to_string()
        }
        fn rustc_version(&self) -> String {
            "rustc 1.99.0".to_string()
        }
        fn env_var(&self, _name: &str) -> Option<String> {
            None
        }
    }

    /// Builds a fingerprint for a tree at a commit
    ///
    /// # Arguments
    ///
    /// * `head` - What `HEAD` points at
    /// * `micro_digest` - The digest of the micro layer's sources
    /// * `dirty_layers` - Which layers have an uncommitted source
    fn tree(head: &str, micro_digest: &str, dirty_layers: &[Layer]) -> Fingerprint {
        let mut layer_digest = BTreeMap::new();
        layer_digest.insert(Layer::Micro, micro_digest.to_string());
        Fingerprint {
            code: CodeFacts {
                head: head.to_string(),
                head_short: head.chars().take(7).collect(),
                dirty: !dirty_layers.is_empty(),
                dirty_paths: dirty_layers.len(),
                allow_dirty: false,
                sources_manifest_sha: "manifest".to_string(),
                sources: BTreeMap::default(),
                layer_digest,
                dirty_layers: dirty_layers.to_vec(),
            },
            env: EnvFacts {
                hostname: "jove".to_string(),
                cpu_model: "test cpu".to_string(),
                cpu_online: 32,
                governor: "performance".to_string(),
                kernel: "test".to_string(),
                rustc: "rustc 1.99.0".to_string(),
                rustc_commit_hash: "abc".to_string(),
                rustflags: "-Ctarget-cpu=native".to_string(),
                rustflags_env: None,
                cargo_config_sha: "cargo".to_string(),
                shoal_yml_sha: "conf".to_string(),
                digest: "env-digest".to_string(),
            },
        }
    }

    /// Builds capture metadata from a fingerprint
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - What the capture was taken on
    fn meta_from(fingerprint: &Fingerprint) -> CaptureMeta {
        CaptureMeta {
            version: META_VERSION,
            label: "test".to_string(),
            captured: "2026-08-09T00:00:00Z".to_string(),
            tool_version: "test".to_string(),
            partial: false,
            filter: Vec::new(),
            exact: false,
            selected: 62,
            registry_total: 62,
            layers: BTreeMap::default(),
            code: fingerprint.code.clone(),
            env: fingerprint.env.clone(),
        }
    }

    /// A capture with no metadata is neither fresh nor stale
    #[test]
    fn no_metadata_is_no_provenance() {
        let now = tree("aaaa", "digest", &[]);
        assert_eq!(
            code_verdict(Layer::Micro, None, &now, None),
            CodeVerdict::NoProvenance
        );
        assert_eq!(env_verdict(None, &now), EnvVerdict::Unknown);
    }

    /// A capture taken at this commit on a clean tree is fresh
    #[test]
    fn the_same_clean_commit_is_fresh() {
        let now = tree("aaaa", "digest", &[]);
        let meta = meta_from(&now);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &now, Some(0)),
            CodeVerdict::Fresh
        );
    }

    /// An earlier commit that did not touch this layer's sources is unaffected, with the distance
    #[test]
    fn an_untouched_layer_is_unaffected() {
        let before = tree("aaaa", "digest", &[]);
        let now = tree("bbbb", "digest", &[]);
        let meta = meta_from(&before);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &now, Some(7)),
            CodeVerdict::Unaffected { behind: 7 }
        );
    }

    /// An earlier commit that did touch this layer's sources is stale
    #[test]
    fn a_touched_layer_is_stale() {
        let before = tree("aaaa", "digest-before", &[]);
        let now = tree("bbbb", "digest-after", &[]);
        let meta = meta_from(&before);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &now, Some(7)),
            CodeVerdict::Stale { behind: 7 }
        );
    }

    /// A digest can narrow stale to unaffected, and can never widen anything to fresh
    #[test]
    fn a_digest_can_only_narrow_a_verdict() {
        // matching digests at a different commit stop at unaffected, never reaching fresh
        let before = tree("aaaa", "same", &[]);
        let now = tree("bbbb", "same", &[]);
        let meta = meta_from(&before);
        let verdict = code_verdict(Layer::Micro, Some(&meta), &now, Some(3));
        assert_eq!(verdict, CodeVerdict::Unaffected { behind: 3 });
        assert_ne!(verdict, CodeVerdict::Fresh);
        // and a capture that recorded no digest for the layer cannot claim to be unaffected
        let mut without = meta_from(&before);
        without.code.layer_digest.clear();
        assert_eq!(
            code_verdict(Layer::Micro, Some(&without), &now, Some(3)),
            CodeVerdict::Stale { behind: 3 }
        );
    }

    /// Uncommitted outranks everything, whether the dirt was there then or is here now
    #[test]
    fn uncommitted_outranks_every_other_verdict() {
        // dirty at capture, at the very same commit
        let dirty = tree("aaaa", "digest", &[Layer::Micro]);
        let clean = tree("aaaa", "digest", &[]);
        let meta = meta_from(&dirty);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &clean, Some(0)),
            CodeVerdict::Uncommitted {
                at_capture: true,
                now: false
            }
        );
        // dirty now, having been captured clean
        let meta = meta_from(&clean);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &dirty, Some(0)),
            CodeVerdict::Uncommitted {
                at_capture: false,
                now: true
            }
        );
    }

    /// A layer whose sources are clean is not condemned by another layer's dirt
    #[test]
    fn dirt_in_another_layer_does_not_spread() {
        let dirty = tree("aaaa", "digest", &[Layer::Macro]);
        let meta = meta_from(&dirty);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &dirty, Some(0)),
            CodeVerdict::Fresh
        );
    }

    /// A commit this tree cannot place has no distance, and says so
    #[test]
    fn an_unplaceable_commit_is_diverged() {
        let before = tree("aaaa", "digest", &[]);
        let now = tree("bbbb", "digest", &[]);
        let meta = meta_from(&before);
        assert_eq!(
            code_verdict(Layer::Micro, Some(&meta), &now, None),
            CodeVerdict::Diverged
        );
    }

    /// A capture from a different environment is incomparable, and names what differs
    #[test]
    fn a_different_environment_is_incomparable() {
        let now = tree("aaaa", "digest", &[]);
        let mut meta = meta_from(&now);
        meta.env.digest = "other".to_string();
        meta.env.governor = "powersave".to_string();
        let verdict = env_verdict(Some(&meta), &now);
        match verdict {
            EnvVerdict::Incomparable { fields } => {
                assert!(
                    fields.iter().any(|field| field.contains("governor")),
                    "expected the governor to be named, got {fields:?}"
                );
            }
            other => panic!("expected incomparable, got {other:?}"),
        }
    }

    /// A digest that differs with nothing to point at still reports a difference
    #[test]
    fn an_unexplained_environment_difference_is_still_reported() {
        let now = tree("aaaa", "digest", &[]);
        let mut meta = meta_from(&now);
        meta.env.digest = "other".to_string();
        assert_eq!(
            env_verdict(Some(&meta), &now),
            EnvVerdict::Incomparable {
                fields: vec!["environment".to_string()]
            }
        );
    }

    /// Only a fresh or unaffected capture describes the current code
    #[test]
    fn only_fresh_and_unaffected_are_current() {
        assert!(CodeVerdict::Fresh.is_current());
        assert!(CodeVerdict::Unaffected { behind: 9 }.is_current());
        assert!(!CodeVerdict::Stale { behind: 1 }.is_current());
        assert!(!CodeVerdict::NoProvenance.is_current());
        assert!(!CodeVerdict::Diverged.is_current());
        assert!(
            !CodeVerdict::Uncommitted {
                at_capture: true,
                now: false
            }
            .is_current()
        );
    }

    /// The fake exists to keep the verdict tests away from git, and is exercised through it
    #[test]
    fn the_fake_answers_what_it_is_told_to() {
        let facts = FakeFacts {
            head: "cafe".to_string(),
            dirty: vec![PathBuf::from("shoal/src/lib.rs")],
            behind: Some(4),
        };
        assert_eq!(facts.git_head().unwrap(), "cafe");
        assert_eq!(facts.git_dirty().unwrap().len(), 1);
        assert_eq!(facts.commits_between("a", "b").unwrap(), Some(4));
    }
}
