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

    /// Whether this arm is a rung of either depth ladder
    pub fn is_depth(&self) -> bool {
        self.id.starts_with("macro/grid/depth/")
    }

    /// Whether this arm is a rung of the width ladder measured at one outstanding query
    ///
    /// A narrower case of [`Arm::is_depth`], and it has to be: the two ladders sweep different axes
    /// and are drawn on different pages, but they share an identifier prefix because they also
    /// share the cell `macro/grid/depth/1`, which is a rung of both.
    pub fn is_width_depth(&self) -> bool {
        self.id.starts_with("macro/grid/depth/1/")
    }

    /// Whether this arm is a point of the key distribution sweep
    pub fn is_skew(&self) -> bool {
        self.id.starts_with("macro/skew/")
    }

    /// Whether this arm is a point of a configuration sweep
    pub fn is_conf(&self) -> bool {
        self.id.starts_with("macro/conf/")
    }

    /// Which setting a configuration arm moved
    ///
    /// Read out of the identifier rather than out of the facts, and deliberately: the *value* a
    /// knob was set to is a fact and is read as one below, but which knob an arm is a sweep *of* is
    /// the arm's identity rather than something it measured. Two arms can resolve to the same
    /// configuration - `latency_buffer/4Ki` and the value `shoal.yml` already sets - and only the
    /// identifier says which sweep each belongs to. This is the same prefix check
    /// [`Arm::is_grid`] and its neighbours are.
    pub fn conf_knob(&self) -> Option<&'a str> {
        // `macro/conf/<section>/<knob>/r<pct>/<value>`, so the knob is the fourth segment
        let rest = self.id.strip_prefix("macro/conf/")?;
        let mut segments = rest.split('/');
        let _section = segments.next()?;
        segments.next()
    }

    /// Which half of the configuration a configuration arm belongs to
    pub fn conf_section(&self) -> Option<&'a str> {
        // the third segment, which is why it is in the identifier at all
        self.id.strip_prefix("macro/conf/")?.split('/').next()
    }

    /// What value a configuration arm set its knob to, as it is written on an axis
    ///
    /// The identifier's last segment. It is the value spelled the way the sweep spelled it - `4Ki`
    /// rather than `4096` - which is what a reader has to type into `shoal.yml` afterwards, and so
    /// is the only spelling worth putting on a chart.
    pub fn conf_value(&self) -> Option<&'a str> {
        // the last segment of a configuration identifier, and nothing else has this prefix
        if !self.is_conf() {
            return None;
        }
        self.id.rsplit('/').next()
    }

    /// This arm's configuration, when it recorded one
    pub fn conf(&self) -> Option<&'a crate::model::macro_layer::ConfFacts> {
        self.capture.conf.as_ref()
    }

    /// The interval this arm's wall clock spanned across its runs, in nanoseconds
    ///
    /// What a difference between two arms has to clear before it is a difference at all. A macro
    /// comparison is interval disjointness rather than a percentage, for the reason
    /// [F7](../../../docs/src/features/bench-runner.md) gives: the frozen baseline spread ten and a
    /// half percent over five identical runs, so a flat threshold either swallows real movement or
    /// reports noise as movement.
    pub fn wall_clock_interval_ns(&self) -> Option<(u64, u64)> {
        self.capture.wall_clock_interval_ns()
    }

    /// This arm's median wall clock in nanoseconds
    pub fn median_wall_clock_ns(&self) -> u128 {
        self.capture.median_wall_clock_ns()
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
/// The ladder proper: four depths at the reference width. The width ladder shares its prefix and is
/// excluded here, because the two are one axis each and plotting them together would draw fifteen
/// points at a depth of one on a chart whose x axis is the depth.
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn depth_ladder(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture)
        .into_iter()
        .filter(|arm| arm.is_depth() && !arm.is_width_depth())
        .collect()
}

/// Every rung of the width axis measured with one query outstanding
///
/// Selected by the **fact** rather than by the prefix, which is what pulls `macro/grid/depth/1` in
/// alongside the fifteen arms named `macro/grid/depth/1/...`. That arm is the reference width's
/// rung of this ladder and the depth ladder's rung at a depth of one - the same measurement, minted
/// once, belonging to both curves. Selecting on the prefix alone would leave this ladder with a
/// hole exactly where the two cross.
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn width_depth(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture)
        .into_iter()
        .filter(|arm| arm.is_depth() && arm.depth() == 1)
        .collect()
}

/// Every point of the key distribution sweep in a capture
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn skew(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture).into_iter().filter(Arm::is_skew).collect()
}

/// Every point of every configuration sweep in a capture
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn conf(capture: &MacroCaptureV2) -> Vec<Arm<'_>> {
    all(capture).into_iter().filter(Arm::is_conf).collect()
}

/// The width every configuration sweep runs at unless it says otherwise
///
/// The grid's reference cell. A sweep at any other width is a separate sweep and is named as one -
/// see [`conf_sweeps`].
const REFERENCE_WIDTH: u64 = 1024;

/// The configuration arms of a capture, gathered into one list per knob
///
/// Ordered by the knob name and, within a knob, by the read share and then by the order the arms
/// appear in the capture - which is identifier order, since a capture is a `BTreeMap`. Deterministic
/// on purpose: a page whose series changed order between two renders of the same artifact would fail
/// `render --check` for no reason.
///
/// # The row width is part of the key
///
/// A knob swept at two row widths is **two sweeps**, not one sweep with more points in it. Merging
/// them would put a 1 KiB arm and an 8 KiB arm in the same ladder and let a comparison read the
/// difference between the two widths as the difference between two values of the setting, which is
/// the one thing the whole sweep is built to prevent. So a sweep away from the reference width
/// carries the width in its name, and the arms it holds never mix with the reference sweep's.
///
/// # Arguments
///
/// * `capture` - The capture to walk
pub fn conf_sweeps(capture: &MacroCaptureV2) -> Vec<(String, u32, Vec<Arm<'_>>)> {
    let mut grouped: std::collections::BTreeMap<(String, u32), Vec<Arm<'_>>> =
        std::collections::BTreeMap::new();
    for arm in conf(capture) {
        // an arm with no knob or no read share is not a sweep point, whatever else it is
        let (Some(knob), Some(read_pct)) = (arm.conf_knob(), arm.read_pct()) else {
            continue;
        };
        // the reference width is unnamed, so the forty eight sweeps that existed before any knob
        // was repeated read exactly as they always did
        let name = if arm.row_bytes() == REFERENCE_WIDTH {
            knob.to_string()
        } else {
            format!("{knob} @ {}", crate::fmt::bytes(arm.row_bytes()))
        };
        grouped.entry((name, read_pct)).or_default().push(arm);
    }
    grouped
        .into_iter()
        .map(|((knob, read_pct), arms)| (knob, read_pct, arms))
        .collect()
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
