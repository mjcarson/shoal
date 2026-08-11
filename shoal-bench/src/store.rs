//! Where the artifacts live, and how a label turns into a file
//!
//! The layout is the one `scripts/bench.sh` established and is kept exactly:
//!
//! ```text
//! docs/perf/baselines/<name>.json          micro captures a run is judged against
//! docs/perf/repeats/<name>.micro.json      identical repeats, which is where the noise band came from
//! docs/perf/runs/<label>.<layer>.json      one file per layer per capture
//! docs/perf/runs/<label>.meta.json         how that capture was taken (new)
//! docs/perf/sources.json                   which sources each layer is considered to measure (new)
//! ```
//!
//! A directory per label would read better in `ls`, and was rejected:
//! `docs/perf/baselines/B1-performance.json` is frozen, and freezing a baseline means freezing its
//! path too. Any grouping scheme would either move that file or leave it as the single exception,
//! and thirty two committed artifacts are referenced by exact path in the commit messages that
//! justified them. `shoal-bench status` is the intended way to see what exists, not `ls`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::model::hotpath::HotpathProfile;
use crate::model::macro_layer::MacroCaptureV2;
use crate::model::meta::CaptureMeta;
use crate::model::micro::MicroCapture;
use crate::model::stages::StageReport;
use crate::registry::Layer;

/// The default baseline a run is judged against, frozen and never overwritten
pub const FROZEN_BASELINE: &str = "B1-performance";

/// The baseline that advances to the last accepted run
pub const TRAILING_BASELINE: &str = "trailing";

/// The artifact tree, rooted at the repository
#[derive(Debug, Clone)]
pub struct Store {
    /// The repository root, the directory holding the workspace `Cargo.toml`
    root: PathBuf,
}

impl Store {
    /// Opens a store rooted at a known repository path
    ///
    /// # Arguments
    ///
    /// * `root` - The repository root
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Store { root: root.into() }
    }

    /// Finds the repository root by walking up from a starting directory
    ///
    /// The root is the directory holding a `Cargo.toml` with a `[workspace]` table. Walking up
    /// rather than assuming the current directory means the tool works from anywhere inside the
    /// tree, the way `cargo` does.
    ///
    /// # Arguments
    ///
    /// * `start` - Where to start looking
    pub fn discover(start: &Path) -> Result<Store> {
        // walk up from the starting directory looking for the workspace manifest
        for candidate in start.ancestors() {
            let manifest = candidate.join("Cargo.toml");
            // a manifest is only the root if it declares the workspace
            if manifest.is_file() {
                let body = std::fs::read_to_string(&manifest)
                    .with_context(|| format!("reading {}", manifest.display()))?;
                if body.contains("[workspace]") {
                    return Ok(Store::new(candidate));
                }
            }
        }
        // nothing above the starting point looked like the repository
        Err(anyhow!(
            "could not find the workspace root above {}",
            start.display()
        ))
    }

    /// The repository root
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory holding every performance artifact
    pub fn perf_dir(&self) -> PathBuf {
        self.root.join("docs/perf")
    }

    /// The directory holding captured runs
    pub fn runs_dir(&self) -> PathBuf {
        self.perf_dir().join("runs")
    }

    /// The directory holding the baselines a run is judged against
    pub fn baselines_dir(&self) -> PathBuf {
        self.perf_dir().join("baselines")
    }

    /// The directory holding identical repeats of a capture
    pub fn repeats_dir(&self) -> PathBuf {
        self.perf_dir().join("repeats")
    }

    /// The manifest naming which sources each layer is considered to measure
    pub fn sources_manifest(&self) -> PathBuf {
        self.perf_dir().join("sources.json")
    }

    /// The benchmark configuration every capture is taken with
    pub fn conf(&self) -> PathBuf {
        self.root.join("shoal.yml")
    }

    /// Where one layer of one capture is stored
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    /// * `layer` - Which layer's artifact to address
    pub fn run_artifact(&self, label: &str, layer: Layer) -> PathBuf {
        // one flat file per layer per label, the shape bench.sh wrote
        self.runs_dir().join(format!("{label}.{layer}.json"))
    }

    /// Where one capture's provenance is stored
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    pub fn meta_path(&self, label: &str) -> PathBuf {
        // a sibling of the layer artifacts rather than a key inside them, so that micro.json
        // never needs a schema version bump and the frozen baseline never needs migrating
        self.runs_dir().join(format!("{label}.meta.json"))
    }

    /// Where a baseline is stored
    ///
    /// # Arguments
    ///
    /// * `name` - The baseline's name
    pub fn baseline_path(&self, name: &str) -> PathBuf {
        // baselines are micro captures and carry no layer infix
        self.baselines_dir().join(format!("{name}.json"))
    }

    /// Every capture label present in the runs directory, sorted
    ///
    /// A label counts as present if any of its layer artifacts exists, so a capture that was
    /// taken with a filter and produced only a micro file is still discoverable.
    pub fn labels(&self) -> Result<Vec<String>> {
        let dir = self.runs_dir();
        // an absent directory is an empty set of labels, not an error - a fresh checkout that has
        // never captured anything is a legitimate state
        if !dir.is_dir() {
            return Ok(Vec::new());
        }
        // collect into a set so a label with four artifacts is still one label
        let mut labels = BTreeSet::new();
        let entries =
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("reading an entry of {}", dir.display()))?;
            let name = entry.file_name();
            // every artifact is json, and everything else in there is not ours
            let Some(name) = name.to_str().and_then(|n| n.strip_suffix(".json")) else {
                continue;
            };
            // strip the layer or meta infix to get back to the label that produced it
            if let Some(label) = strip_known_infix(name) {
                labels.insert(label.to_string());
            }
        }
        Ok(labels.into_iter().collect())
    }

    /// Every repeat capture, sorted by name
    ///
    /// These back the claim about what the micro layer can actually resolve, and are the only
    /// evidence in the tree for the noise band the comparison engine uses.
    pub fn repeats(&self) -> Result<Vec<(String, MicroCapture)>> {
        let dir = self.repeats_dir();
        // no repeats directory means the noise band has no evidence behind it in this tree
        if !dir.is_dir() {
            return Ok(Vec::new());
        }
        // read every micro capture in there, keyed by the name it was taken under
        let mut found = Vec::new();
        let entries =
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("reading an entry of {}", dir.display()))?;
            let path = entry.path();
            let Some(name) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".micro.json"))
            else {
                continue;
            };
            found.push((name.to_string(), self.read_micro(&path)?));
        }
        // sorted so that a table or chart built from these comes out the same every time
        found.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(found)
    }

    /// Reads a micro capture and checks its schema version
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read
    pub fn read_micro(&self, path: &Path) -> Result<MicroCapture> {
        // read it, then refuse a version this tool was not written against
        let capture: MicroCapture = read_json(path)?;
        capture.check_version(path).map_err(|err| anyhow!(err))?;
        Ok(capture)
    }

    /// Reads a macro capture and checks its schema version
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read
    pub fn read_macro(&self, path: &Path) -> Result<MacroCaptureV2> {
        // dispatch on the version, lifting a version 1 capture rather than refusing it. the seven
        // captures taken before purpose built workloads existed are the historical record and are
        // still readable, still chartable and still comparable against each other.
        crate::model::macro_layer::read(path).map_err(|err| anyhow!(err))
    }

    /// Reads a hotpath profile
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read
    pub fn read_hotpath(&self, path: &Path) -> Result<HotpathProfile> {
        // hotpath's own format carries no version field of its own to check
        read_json(path)
    }

    /// Reads a stage report and checks its schema version
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read
    pub fn read_stages(&self, path: &Path) -> Result<StageReport> {
        // read it, then refuse a version this tool was not written against - the stage list could
        // have changed underneath, and every chart segment is labelled from it
        let report: StageReport = read_json(path)?;
        report.check_version(path).map_err(|err| anyhow!(err))?;
        Ok(report)
    }

    /// Reads a capture's provenance, if it has any
    ///
    /// The seven labels captured before this tool existed have no metadata file. That is not an
    /// error: they are reported as having no provenance, which is deliberately neither fresh nor
    /// stale.
    ///
    /// # Arguments
    ///
    /// * `label` - The capture's name
    pub fn read_meta(&self, label: &str) -> Result<Option<CaptureMeta>> {
        let path = self.meta_path(label);
        // an absent metadata file is the normal state for a pre-existing capture
        if !path.is_file() {
            return Ok(None);
        }
        // read it, then refuse a version this tool was not written against
        let meta: CaptureMeta = read_json(&path)?;
        meta.check_version(&path).map_err(|err| anyhow!(err))?;
        Ok(Some(meta))
    }

    /// Resolves a micro capture named on the command line
    ///
    /// A caller may name a baseline (`B1-performance`), a capture label (`o17-after`), or a path
    /// to a file. They are tried in that order, so the two words used most often are the shortest
    /// to type, and a path always wins because it is unambiguous.
    ///
    /// # Arguments
    ///
    /// * `spec` - What the caller named
    pub fn resolve_micro(&self, spec: &str) -> Result<(PathBuf, MicroCapture)> {
        // an explicit path is unambiguous, so it is checked first
        let direct = Path::new(spec);
        if spec.contains('/') || spec.ends_with(".json") {
            // a path that was named and does not exist is an error, not a reason to guess
            if !direct.is_file() {
                bail!("no micro capture at {spec}");
            }
            return Ok((direct.to_path_buf(), self.read_micro(direct)?));
        }
        // then a baseline, which is what a comparison is usually against
        let baseline = self.baseline_path(spec);
        if baseline.is_file() {
            let capture = self.read_micro(&baseline)?;
            return Ok((baseline, capture));
        }
        // then a captured run of that name
        let run = self.run_artifact(spec, Layer::Micro);
        if run.is_file() {
            let capture = self.read_micro(&run)?;
            return Ok((run, capture));
        }
        // nothing by that name exists anywhere it could
        Err(anyhow!(
            "no baseline {}, no run {}, and no file at '{spec}'",
            baseline.display(),
            run.display()
        ))
    }

    /// Resolves a macro capture named on the command line
    ///
    /// Unlike the micro layer there are no macro baselines, so this looks only at captured runs
    /// and at explicit paths.
    ///
    /// # Arguments
    ///
    /// * `spec` - What the caller named
    pub fn resolve_macro(&self, spec: &str) -> Result<Option<(PathBuf, MacroCaptureV2)>> {
        // an explicit path is unambiguous
        let direct = Path::new(spec);
        if spec.contains('/') || spec.ends_with(".json") {
            // a named path that is not there is an error
            if !direct.is_file() {
                bail!("no macro capture at {spec}");
            }
            return Ok(Some((direct.to_path_buf(), self.read_macro(direct)?)));
        }
        // otherwise look for a captured run of that name, and report its absence rather than
        // failing: a micro only capture legitimately has no macro artifact
        let run = self.run_artifact(spec, Layer::Macro);
        if run.is_file() {
            let capture = self.read_macro(&run)?;
            return Ok(Some((run, capture)));
        }
        Ok(None)
    }
}

/// Strips a known layer or metadata infix off an artifact's stem
///
/// Returns the label that produced it, or `None` if the file is not one of ours.
///
/// # Arguments
///
/// * `stem` - The file name with its `.json` extension already removed
fn strip_known_infix(stem: &str) -> Option<&str> {
    // the four layer infixes, plus the provenance file that sits beside them
    for layer in Layer::ALL {
        if let Some(label) = stem.strip_suffix(&format!(".{layer}")) {
            return Some(label);
        }
    }
    stem.strip_suffix(".meta")
}

/// Reads and deserializes a JSON artifact
///
/// # Arguments
///
/// * `path` - The file to read
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    // read the whole file, since every artifact here is small enough to hold
    let body =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    // and parse it, naming the file in the error - a parse failure with no path is unactionable
    serde_json::from_str(&body).with_context(|| format!("parsing {}", path.display()))
}

/// Writes a JSON artifact, creating its directory if it is missing
///
/// The output is pretty printed with a trailing newline, matching what `bench.sh` produced, so
/// that these files stay readable in a diff.
///
/// # Arguments
///
/// * `path` - Where to write
/// * `value` - What to write
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    // make sure the directory exists before writing into it
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // serialize before touching the file, so a serialization failure cannot leave a half written
    // artifact behind - the same reason collect-micro.sh validated with `jq -e` before finishing
    let mut body = serde_json::to_string_pretty(value)
        .with_context(|| format!("serializing {}", path.display()))?;
    body.push('\n');
    std::fs::write(path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every layer artifact and the metadata file resolve back to the label that produced them
    #[test]
    fn infixes_strip_back_to_the_label() {
        assert_eq!(strip_known_infix("o17-after.micro"), Some("o17-after"));
        assert_eq!(strip_known_infix("o17-after.macro"), Some("o17-after"));
        assert_eq!(strip_known_infix("o17-after.hotpath"), Some("o17-after"));
        assert_eq!(strip_known_infix("o17-after.stages"), Some("o17-after"));
        assert_eq!(strip_known_infix("o17-after.meta"), Some("o17-after"));
    }

    /// A label that itself contains dots survives the round trip
    #[test]
    fn a_dotted_label_survives() {
        assert_eq!(
            strip_known_infix("o3-after.repeat.micro"),
            Some("o3-after.repeat")
        );
    }

    /// A file that is not one of ours is not mistaken for a capture
    #[test]
    fn an_unknown_file_is_not_a_label() {
        assert_eq!(strip_known_infix("notes"), None);
        assert_eq!(strip_known_infix("o17-after.profile"), None);
    }
}
