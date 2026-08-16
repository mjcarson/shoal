//! Picking the workloads a page draws, out of a capture
//!
//! # Selection reads the facts, not the identifier
//!
//! Every grid arm records what it was - its read share, its row width, its width distribution, its
//! key distribution, the kind of table it drove - in its own `ScaleFacts`. A page selects on those
//! fields rather than on the shape of the identifier string.
//!
//! That is a deliberate cost. Parsing `macro/grid/unsorted/r50/1024` back into three values is
//! fewer lines than what is here. It is also a naming convention pretending to be a schema: the
//! moment a workload is added whose identifier is shaped differently, a parser either mis-reads it
//! or drops it silently, and a chart with an arm quietly missing is worse than one that failed to
//! draw. The facts are written by the workload that ran, so they cannot disagree with it.
//!
//! The one thing selection does use the identifier for is telling the sweeps apart - which is what
//! [`Arm::is_grid`] and its neighbours are for, and they are prefix checks rather than parsers.

use crate::model::macro_layer::{MacroCaptureV2, WorkloadCapture};

/// One workload out of a capture, with its identifier
#[derive(Debug, Clone, Copy)]
pub struct Arm<'a> {
    /// The workload's identifier
    pub id: &'a str,
    /// What it measured
    pub capture: &'a WorkloadCapture,
}

impl<'a> Arm<'a> {
    /// What share of this arm's queries were reads, when it was a mixture
    pub fn read_pct(&self) -> Option<u32> {
        self.capture.scale.read_pct
    }

    /// How wide this arm's rows were, as a mean when they varied
    pub fn row_bytes(&self) -> u64 {
        self.capture.scale.row_bytes
    }

    /// Which width distribution this arm drew from, when it drew from one
    pub fn row_profile(&self) -> Option<&'a str> {
        self.capture.scale.row_profile.as_deref()
    }

    /// Which kind of table this arm drove
    pub fn table_kind(&self) -> Option<&'a str> {
        self.capture.scale.table_kind.as_deref()
    }

    /// Which key distribution this arm's reads were drawn from
    ///
    /// Uniform is recorded as absent, since it is the default every workload before the skew sweep
    /// used, so a missing value is read as uniform here rather than as unknown.
    pub fn distribution(&self) -> &'a str {
        self.capture
            .scale
            .distribution
            .as_deref()
            .unwrap_or("uniform")
    }

    /// How many queries this arm kept outstanding at once
    pub fn depth(&self) -> u32 {
        self.capture.scale.concurrency
    }

    /// How this arm names its row width on an axis
    ///
    /// A mixture names its distribution, because the number beside it is a mean and quoting the
    /// mean alone would make a mixture look like a fixed width run that happened to land there.
    pub fn width_label(&self) -> String {
        match self.row_profile() {
            Some(profile) => profile.to_string(),
            None => crate::fmt::bytes(self.row_bytes()),
        }
    }

    /// Whether this arm is a cell of the grid rather than a rung of its depth ladder
    pub fn is_grid(&self) -> bool {
        self.id.starts_with("macro/grid/") && !self.is_depth()
    }

    /// Whether this arm is a rung of the depth ladder
    pub fn is_depth(&self) -> bool {
        self.id.starts_with("macro/grid/depth/")
    }

    /// Whether this arm is a point of the key distribution sweep
    pub fn is_skew(&self) -> bool {
        self.id.starts_with("macro/skew/")
    }

    /// One latency metric of one operation, in nanoseconds
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation to read, `read` or `write`
    /// * `metric` - Which percentile to read
    pub fn stat(&self, op: &str, metric: &str) -> Option<f64> {
        self.capture.stat_ns(op, metric).map(|ns| ns as f64)
    }

    /// How many queries a second this arm answered, at the depth it ran
    pub fn ops_per_sec(&self) -> Option<f64> {
        self.capture.ops_per_sec()
    }

    /// How many payload bytes a second this arm moved
    pub fn bytes_per_sec(&self) -> Option<f64> {
        self.capture.bytes_per_sec()
    }
}

/// Every workload in a capture, in identifier order
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn all(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    // a `BTreeMap` iterates sorted, which is what keeps the rendered page deterministic
    capture
        .workloads
        .iter()
        .map(|(id, workload)| Arm {
            id,
            capture: workload,
        })
        .collect()
}

/// Every cell of the grid in a capture
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn grid(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture).into_iter().filter(Arm::is_grid).collect()
}

/// Every rung of the depth ladder in a capture
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn depth_ladder(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture).into_iter().filter(Arm::is_depth).collect()
}

/// Every point of the key distribution sweep in a capture
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn skew(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture).into_iter().filter(Arm::is_skew).collect()
}

/// Every workload whose identifier starts with a prefix
///
/// # Arguments
///
/// * `capture` - The capture to walk
/// * `prefix` - The identifier prefix to keep
pub fn with_prefix<'a>(capture: &'a MacroCaptureV2, prefix: &str) -> Vec<Arm<'a>> {
    all(capture)
        .into_iter()
        .filter(|arm| arm.id.starts_with(prefix))
        .collect()
}

/// The kinds of table a set of arms covers, in a fixed reading order
///
/// Sorted so that the persistent pair comes before the ephemeral one and the unsorted table before
/// the sorted one, which is the order every chart and table on the site puts them in. Alphabetical
/// order would put `ephemeral_sorted` first, which is the control before the thing it controls for.
///
/// # Arguments
///
/// * `arms` - The arms to collect table kinds from
pub fn table_kinds(arms: &[Arm<'_>]) -> Vec<String> {
    /// The order tables are read in, most load-bearing first
    const ORDER: [&str; 4] = [
        "persistent_unsorted",
        "persistent_sorted",
        "ephemeral_unsorted",
        "ephemeral_sorted",
    ];
    let mut found: Vec<String> = Vec::new();
    for kind in ORDER {
        if arms.iter().any(|arm| arm.table_kind() == Some(kind)) {
            found.push(kind.to_string());
        }
    }
    // anything the order does not name is still shown, after what it does, sorted so the page
    // stays deterministic. a table kind added without this list being updated is then late rather
    // than missing
    let mut extra: Vec<String> = arms
        .iter()
        .filter_map(|arm| arm.table_kind())
        .filter(|kind| !ORDER.contains(kind))
        .map(String::from)
        .collect();
    extra.sort_unstable();
    extra.dedup();
    found.extend(extra);
    found
}

/// How a table kind is written on a chart or in a table
///
/// # Arguments
///
/// * `kind` - The recorded table kind
pub fn table_label(kind: &str) -> String {
    match kind {
        "persistent_unsorted" => "unsorted".to_string(),
        "persistent_sorted" => "sorted".to_string(),
        "ephemeral_unsorted" => "unsorted, no storage".to_string(),
        "ephemeral_sorted" => "sorted, no storage".to_string(),
        other => other.replace('_', " "),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Arm, table_kinds, table_label};
    use crate::model::macro_layer::{MACRO_VERSION, MacroCaptureV2, ScaleFacts, Timing, WorkloadCapture};

    /// Builds a capture holding one workload with the given facts
    ///
    /// # Arguments
    ///
    /// * `entries` - The identifier and scale facts of each workload
    fn capture(entries: &[(&str, ScaleFacts)]) -> MacroCaptureV2 {
        let mut workloads = BTreeMap::new();
        for (id, scale) in entries {
            workloads.insert(
                (*id).to_string(),
                WorkloadCapture {
                    timing: Timing::PerQuery,
                    seed: 42,
                    scale: scale.clone(),
                    conf: None,
                    counters: BTreeMap::new(),
                    ops: BTreeMap::new(),
                    runs: None,
                    wall_clock_ns: None,
                    spread_pct: None,
                    runs_detail: None,
                },
            );
        }
        MacroCaptureV2 {
            version: MACRO_VERSION,
            label: Some("test".to_string()),
            workloads,
        }
    }

    /// Facts for a grid cell
    ///
    /// # Arguments
    ///
    /// * `read_pct` - What share of its queries were reads
    /// * `row_bytes` - How wide its rows were
    /// * `table` - Which table it drove
    fn facts(read_pct: u32, row_bytes: u64, table: &str) -> ScaleFacts {
        ScaleFacts {
            scale: "full".to_string(),
            rows: 20_000,
            row_bytes,
            keys: 20_000,
            concurrency: 32,
            read_pct: Some(read_pct),
            table_kind: Some(table.to_string()),
            ..ScaleFacts::default()
        }
    }

    /// The depth ladder is not counted as a grid cell, though its ids sit under the same prefix
    #[test]
    fn the_ladder_is_not_a_grid_cell() {
        let capture = capture(&[
            ("macro/grid/unsorted/r50/1024", facts(50, 1024, "persistent_unsorted")),
            ("macro/grid/depth/32", facts(50, 1024, "persistent_unsorted")),
        ]);
        let grid = super::grid(&capture);
        assert_eq!(grid.len(), 1);
        assert_eq!(grid[0].id, "macro/grid/unsorted/r50/1024");
        assert_eq!(super::depth_ladder(&capture).len(), 1);
    }

    /// Table kinds come out in reading order, not alphabetical order
    ///
    /// Alphabetically the ephemeral control comes before the thing it is a control for, which is
    /// backwards everywhere it is read.
    #[test]
    fn table_kinds_are_in_reading_order() {
        let capture = capture(&[
            ("a", facts(50, 1024, "ephemeral_sorted")),
            ("b", facts(50, 1024, "persistent_unsorted")),
            ("c", facts(50, 1024, "ephemeral_unsorted")),
            ("d", facts(50, 1024, "persistent_sorted")),
        ]);
        let arms = super::all(&capture);
        assert_eq!(
            table_kinds(&arms),
            vec![
                "persistent_unsorted",
                "persistent_sorted",
                "ephemeral_unsorted",
                "ephemeral_sorted"
            ]
        );
    }

    /// A table kind the order does not name is still shown rather than dropped
    #[test]
    fn an_unknown_table_kind_is_kept() {
        let capture = capture(&[("a", facts(50, 1024, "something_new"))]);
        let arms = super::all(&capture);
        assert_eq!(table_kinds(&arms), vec!["something_new"]);
        // and it is written readably rather than in its stored spelling
        assert_eq!(table_label("something_new"), "something new");
    }

    /// A mixture names its distribution on an axis, since the width beside it is a mean
    #[test]
    fn a_mixture_labels_itself_by_name() {
        let mut mixed = facts(50, 240, "persistent_unsorted");
        mixed.row_profile = Some("mixed_small".to_string());
        let capture = capture(&[("a", mixed), ("b", facts(50, 1024, "persistent_unsorted"))]);
        let arms: Vec<Arm<'_>> = super::all(&capture);
        assert_eq!(arms[0].width_label(), "mixed_small");
        assert_eq!(arms[1].width_label(), "1 KiB");
    }

    /// An arm that recorded no distribution is uniform, which is what every older workload was
    #[test]
    fn an_unrecorded_distribution_reads_as_uniform() {
        let capture = capture(&[("a", facts(50, 1024, "persistent_unsorted"))]);
        assert_eq!(super::all(&capture)[0].distribution(), "uniform");
    }
}
