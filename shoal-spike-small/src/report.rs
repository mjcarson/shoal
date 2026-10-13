//! Every round's records merged into intervals, and the crossover and its triggers judged as they
//! were written
//!
//! The form of X3's `x3 report`: a figure is its interval across rounds, and a difference counts
//! only where two intervals do not overlap. The triggers, stated before the harness existed and
//! agreed with the user on 2026-10-08 (`docs/src/object-storage/small-writes.md`, How it was
//! judged), are judged on each leg apart, the lab standing for the 970 EVO over 1 GbE and loopback
//! for the Optane:
//!
//! - at each size, the staged path is compared with the inline one on writes a second at depth
//!   32 and on the median latency at depth 1, and a side wins only where its interval is wholly
//!   better than the other's;
//! - the crossover at a depth is the smallest size from which the inline path never wins again;
//! - **T1**, no threshold below a stripe: inline still wins at 256 KiB at both depths;
//! - **T2**, the log path is not worth having: the crossover is at 8 KiB or below at both depths;
//! - **T3**, the threshold is real: otherwise, set at the smaller of the two depths' crossovers.
//!
//! The row path is reported and never judged.

use std::collections::{BTreeMap, BTreeSet};

use crate::measure::{cell_name, DEPTHS, SIZES};
use crate::paths::Path;
use crate::record::Record;
use crate::stats::{fmt, Interval};
use crate::table::Table;

/// A figure's value in each round, by round
type Rounds = BTreeMap<u32, f64>;

/// The legs, in the order the report lists them
pub const LEGS: [&str; 2] = ["lab", "loopback"];

/// T2's line: a crossover at or below this size means the log path is not worth having
pub const T2_LINE: usize = 8 << 10;

/// A cell's busiest link above this share of 1 GbE is footnoted: the link, not the path, may
/// have decided it
pub const LINK_LINE: f64 = 0.9;

/// The records, indexed by leg, cell, side and figure
pub struct Index {
    /// Every figure's rounds, by `(measurement, cell, side, figure)`
    figures: BTreeMap<(String, String, String, String), Rounds>,
    /// Every label's values, by `(measurement, cell, side, label)`
    labels: BTreeMap<(String, String, String, String), BTreeSet<String>>,
}

impl Index {
    /// Index records, leaving quick runs out unless only quick runs exist
    ///
    /// # Arguments
    ///
    /// * `records` - Every round's records
    #[must_use]
    pub fn of(records: &[Record]) -> Self {
        let mut figures: BTreeMap<_, Rounds> = BTreeMap::new();
        let mut labels: BTreeMap<_, BTreeSet<String>> = BTreeMap::new();
        // a quick run proves a leg runs and is never a measurement
        let only_quick = records.iter().all(|record| record.quick);
        for record in records.iter().filter(|record| only_quick || !record.quick) {
            let key = |name: &str| {
                (
                    record.measurement.clone(),
                    record.cell.clone(),
                    record.side.clone(),
                    name.to_string(),
                )
            };
            for (name, value) in &record.metrics {
                figures.entry(key(name)).or_default().insert(record.round, *value);
            }
            for (name, value) in &record.labels {
                labels.entry(key(name)).or_default().insert(value.clone());
            }
        }
        Index { figures, labels }
    }

    /// A figure's rounds
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `figure` - The figure
    #[must_use]
    pub fn rounds(&self, leg: &str, cell: &str, side: &str, figure: &str) -> Option<&Rounds> {
        self.figures
            .get(&(leg.to_string(), cell.to_string(), side.to_string(), figure.to_string()))
    }

    /// A figure's interval across rounds
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `figure` - The figure
    #[must_use]
    pub fn interval(&self, leg: &str, cell: &str, side: &str, figure: &str) -> Option<Interval> {
        let rounds = self.rounds(leg, cell, side, figure)?;
        Interval::of(&rounds.values().copied().collect::<Vec<_>>())
    }

    /// Every figure a side recorded whose name starts with a prefix, by the rest of its name
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `prefix` - The start of the figures' names
    #[must_use]
    pub fn with_prefix(&self, leg: &str, cell: &str, side: &str, prefix: &str) -> BTreeMap<String, &Rounds> {
        self.figures
            .iter()
            .filter(|((m, c, s, figure), _)| m == leg && c == cell && s == side && figure.starts_with(prefix))
            .map(|((_, _, _, figure), rounds)| (figure[prefix.len()..].to_string(), rounds))
            .collect()
    }

    /// A label's values a side recorded over its rounds
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `label` - The label
    #[must_use]
    pub fn label(&self, leg: &str, cell: &str, side: &str, label: &str) -> Option<&BTreeSet<String>> {
        self.labels
            .get(&(leg.to_string(), cell.to_string(), side.to_string(), label.to_string()))
    }

    /// Whether a leg has any record
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    #[must_use]
    pub fn has(&self, leg: &str) -> bool {
        self.figures.keys().any(|(m, ..)| m == leg)
    }
}

/// A figure's interval written for a table, or a dash when it was not recorded
///
/// # Arguments
///
/// * `interval` - The interval, if any
/// * `scale` - What each value is divided by first
fn show(interval: Option<Interval>, scale: f64) -> String {
    interval.map_or_else(
        || "–".to_string(),
        |interval| {
            Interval {
                min: interval.min / scale,
                median: interval.median / scale,
                max: interval.max / scale,
                rounds: interval.rounds,
            }
            .show()
        },
    )
}

/// A size written as people read it
///
/// # Arguments
///
/// * `size` - The size in bytes
#[must_use]
pub fn size_name(size: usize) -> String {
    if size >= 1 << 20 {
        format!("{} MiB", size >> 20)
    } else {
        format!("{} KiB", size >> 10)
    }
}

/// Which of the two paths judged won at one size and depth
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Win {
    /// The inline path's interval is wholly better
    Inline,
    /// The staged path's interval is wholly better
    Staged,
    /// The intervals overlap
    Neither,
    /// A side has no figure
    Unjudged,
}

impl Win {
    /// The win as a table writes it
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Win::Inline => "inline",
            Win::Staged => "staged",
            Win::Neither => "neither",
            Win::Unjudged => "–",
        }
    }
}

/// The figure a depth is judged on, and whether larger is better
///
/// # Arguments
///
/// * `depth` - The depth
#[must_use]
pub fn judged_figure(depth: usize) -> (&'static str, bool) {
    // writes a second when many are in flight, the median when one is
    if depth > 1 {
        ("per_sec", true)
    } else {
        ("p50_us", false)
    }
}

/// Which path wins between two intervals of a figure
///
/// # Arguments
///
/// * `inline` - The inline path's interval
/// * `staged` - The staged path's interval
/// * `higher_better` - Whether a larger figure is better
#[must_use]
pub fn winner(inline: Option<Interval>, staged: Option<Interval>, higher_better: bool) -> Win {
    let (Some(inline), Some(staged)) = (inline, staged) else {
        return Win::Unjudged;
    };
    // wholly better: every round of one beyond every round of the other
    let (inline_better, staged_better) = if higher_better {
        (inline.min > staged.max, staged.min > inline.max)
    } else {
        (inline.max < staged.min, staged.max < inline.min)
    };
    if inline_better {
        Win::Inline
    } else if staged_better {
        Win::Staged
    } else {
        Win::Neither
    }
}

/// Where the staged path catches the inline one at a depth
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Crossover {
    /// From this size on, the inline path never wins
    At(usize),
    /// The inline path still wins at the largest size
    None,
    /// Some size has no verdict
    Unjudged,
}

impl Crossover {
    /// The crossover as the report writes it
    #[must_use]
    pub fn word(self) -> String {
        match self {
            Crossover::At(size) => size_name(size),
            Crossover::None => "none: inline still wins at the largest size".to_string(),
            Crossover::Unjudged => "not judged: missing figures".to_string(),
        }
    }
}

/// The crossover over a depth's wins, sizes ascending
///
/// # Arguments
///
/// * `wins` - Each size and who won at it, smallest first
#[must_use]
pub fn crossover(wins: &[(usize, Win)]) -> Crossover {
    if wins.is_empty() || wins.iter().any(|(_, win)| *win == Win::Unjudged) {
        return Crossover::Unjudged;
    }
    // the last size the inline path wins at, and the size after it
    match wins.iter().rposition(|(_, win)| *win == Win::Inline) {
        None => Crossover::At(wins[0].0),
        Some(last) if last + 1 == wins.len() => Crossover::None,
        Some(last) => Crossover::At(wins[last + 1].0),
    }
}

/// What a leg's crossovers come to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// T1: inline still wins at the largest size at both depths
    NoThreshold,
    /// T2: the crossover is at T2's line or below at both depths
    NotWorthHaving,
    /// T3: the threshold is real, at this size
    Threshold(usize),
    /// Some crossover has no verdict
    Unjudged,
}

impl Verdict {
    /// The verdict as the report writes it
    #[must_use]
    pub fn word(self) -> String {
        match self {
            Verdict::NoThreshold => "**T1 fires**: no threshold below a stripe".to_string(),
            Verdict::NotWorthHaving => "**T2 fires**: the log path is not worth having".to_string(),
            Verdict::Threshold(size) => format!("**T3 fires**: the threshold is real, at {}", size_name(size)),
            Verdict::Unjudged => "not judged: missing figures".to_string(),
        }
    }
}

/// A leg's verdict from its two depths' crossovers
///
/// # Arguments
///
/// * `one` - The crossover at depth one
/// * `deep` - The crossover at depth thirty-two
#[must_use]
pub fn verdict(one: Crossover, deep: Crossover) -> Verdict {
    match (one, deep) {
        (Crossover::Unjudged, _) | (_, Crossover::Unjudged) => Verdict::Unjudged,
        (Crossover::None, Crossover::None) => Verdict::NoThreshold,
        (Crossover::At(a), Crossover::At(b)) if a <= T2_LINE && b <= T2_LINE => Verdict::NotWorthHaving,
        // the smaller of the two, where a depth with no crossover counts as none at all
        (Crossover::At(a), Crossover::At(b)) => Verdict::Threshold(a.min(b)),
        (Crossover::At(size), Crossover::None) | (Crossover::None, Crossover::At(size)) => Verdict::Threshold(size),
    }
}

/// Every size's winner at a depth on a leg, and the intervals it was judged on
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `depth` - The depth
/// * `sizes` - The sizes, smallest first
#[must_use]
pub fn wins(index: &Index, leg: &str, depth: usize, sizes: &[usize]) -> Vec<(usize, Win, Option<Interval>, Option<Interval>)> {
    let (figure, higher) = judged_figure(depth);
    sizes
        .iter()
        .map(|size| {
            let cell = cell_name(*size, depth);
            let inline = index.interval(leg, &cell, Path::Inline.name(), figure);
            let staged = index.interval(leg, &cell, Path::Staged.name(), figure);
            (*size, winner(inline, staged, higher), inline, staged)
        })
        .collect()
}

/// The sizes a leg recorded, smallest first
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
fn sizes_of(index: &Index, leg: &str) -> Vec<usize> {
    SIZES
        .iter()
        .copied()
        .filter(|size| {
            DEPTHS
                .iter()
                .any(|depth| index.rounds(leg, &cell_name(*size, *depth), Path::Staged.name(), "per_sec").is_some())
        })
        .collect()
}

/// The whole report: every leg's tables, then its crossovers and verdict
///
/// # Arguments
///
/// * `records` - Every round's records
/// * `label` - The line naming where they were measured
#[must_use]
pub fn render(records: &[Record], label: &str) -> String {
    let index = Index::of(records);
    let rounds: BTreeSet<u32> = records.iter().map(|record| record.round).collect();
    let mut out = format!(
        "# X8 report\n\n{} records over {} round{}; every figure is the median of the rounds with the \
         lowest and the highest in brackets.\n\n",
        records.len(),
        rounds.len(),
        if rounds.len() == 1 { "" } else { "s" }
    );
    for leg in LEGS {
        if !index.has(leg) {
            continue;
        }
        let sizes = sizes_of(&index, leg);
        let caption = format!("{label} Leg `{leg}`.");
        out.push_str(&format!("## {leg}\n\n"));
        out.push_str(&depth_one(&index, leg, &sizes).render("Depth 1: a write's latency, µs", &caption));
        out.push_str(&deep(&index, leg, &sizes).render("Depth 32: writes a second and the tail", &caption));
        out.push_str(&parts(&index, leg, &sizes).render("A write's parts: the median, µs", &caption));
        out.push_str(&syncs(&index, leg, &sizes).render("Syncs a write: the WAL's, every member's, and the holders'", &caption));
        out.push_str(&devices(&index, leg, &sizes).render("Device bytes written a byte of payload, every host, merges finished", &caption));
        out.push_str(&behind(&index, leg, &sizes).render("Work a write left behind, and the waits for it", &caption));
        out.push_str(&costs(&index, leg, &sizes).render("Cpu a write, µs, and the busiest link", &caption));
        out.push_str(&regimes(&index, leg, &sizes).render("Each holder's sync floor before every cell, µs", &caption));
        out.push_str(&judged(&index, leg, &sizes));
        out.push_str(&notes(&index, leg, &sizes));
    }
    out
}

/// Depth one: every path's median and p99
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn depth_one(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&["Size", "row p50", "staged p50", "inline p50", "row p99", "staged p99", "inline p99"]);
    for size in sizes {
        let cell = cell_name(*size, 1);
        let mut row = vec![size_name(*size)];
        for figure in ["p50_us", "p99_us"] {
            for path in Path::ALL {
                row.push(show(index.interval(leg, &cell, path.name(), figure), 1.0));
            }
        }
        table.row(row);
    }
    table
}

/// Depth thirty-two: every path's rate and p99
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn deep(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&["Size", "row /s", "staged /s", "inline /s", "row p99 ms", "staged p99 ms", "inline p99 ms"]);
    for size in sizes {
        let cell = cell_name(*size, 32);
        let mut row = vec![size_name(*size)];
        for path in Path::ALL {
            row.push(show(index.interval(leg, &cell, path.name(), "per_sec"), 1.0));
        }
        for path in Path::ALL {
            row.push(show(index.interval(leg, &cell, path.name(), "p99_us"), 1e3));
        }
        table.row(row);
    }
    table
}

/// The staged and inline paths' parts at each depth
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn parts(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&[
        "Size",
        "Depth",
        "staged read",
        "stage, 2 of 3",
        "third stage",
        "staged commit",
        "inline read",
        "inline commit",
    ]);
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            let staged = |figure: &str| show(index.interval(leg, &cell, "staged", figure), 1.0);
            let inline = |figure: &str| show(index.interval(leg, &cell, "inline", figure), 1.0);
            table.row(vec![
                size_name(*size),
                depth.to_string(),
                staged("read_p50_us"),
                staged("stage_p50_us"),
                staged("third_p50_us"),
                staged("commit_p50_us"),
                inline("read_p50_us"),
                inline("commit_p50_us"),
            ]);
        }
    }
    table
}

/// Syncs a write on every path: the WAL's, the holders', and all of them
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn syncs(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&[
        "Size",
        "Depth",
        "row: WAL",
        "staged: WAL",
        "staged: stage",
        "staged: apply",
        "inline: WAL",
        "inline: fold",
        "records a journal sync",
    ]);
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            let figure = |side: &str, name: &str| show(index.interval(leg, &cell, side, name), 1.0);
            table.row(vec![
                size_name(*size),
                depth.to_string(),
                figure("row", "wal_syncs_per_write"),
                figure("staged", "wal_syncs_per_write"),
                figure("staged", "stage_syncs_per_write"),
                figure("staged", "apply_syncs_per_write"),
                figure("inline", "wal_syncs_per_write"),
                figure("inline", "fold_syncs_per_write"),
                figure("staged", "stage_records_per_sync"),
            ]);
        }
    }
    table
}

/// Device bytes a byte of payload on every path, once the merges finished
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn devices(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&["Size", "Depth", "row", "staged", "inline", "row flushes", "staged flushes", "inline flushes"]);
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            let mut row = vec![size_name(*size), depth.to_string()];
            for path in Path::ALL {
                row.push(show(index.interval(leg, &cell, path.name(), "settled:device_written_per_byte"), 1.0));
            }
            for path in Path::ALL {
                row.push(show(index.interval(leg, &cell, path.name(), "settled:flushes_per_write"), 1.0));
            }
            table.row(row);
        }
    }
    table
}

/// The staged and inline paths' deferred work: its latency, the waits for it and the drain
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn behind(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&[
        "Size",
        "Depth",
        "Path",
        "apply or fold p50 µs",
        "p99 µs",
        "wait p50 µs",
        "wait p99 µs",
        "drain ms",
    ]);
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            for path in [Path::Staged, Path::Inline] {
                let figure = |name: &str| show(index.interval(leg, &cell, path.name(), name), 1.0);
                table.row(vec![
                    size_name(*size),
                    depth.to_string(),
                    path.name().to_string(),
                    figure("deferred_p50_us"),
                    figure("deferred_p99_us"),
                    figure("wait_p50_us"),
                    figure("wait_p99_us"),
                    figure("drain_ms"),
                ]);
            }
        }
    }
    table
}

/// Every path's cpu a write, the members' and the holders' together, and the busiest link
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn costs(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    let mut table = Table::new(&[
        "Size",
        "Depth",
        "row: nodes",
        "staged: nodes",
        "staged: holders",
        "inline: nodes",
        "inline: holders",
        "busiest link, staged",
        "busiest link, inline",
    ]);
    // a side's figures under a prefix, added up round by round
    let summed = |cell: &str, side: &str, prefix: &str| -> Option<Interval> {
        let mut by_round: BTreeMap<u32, f64> = BTreeMap::new();
        for rounds in index.with_prefix(leg, cell, side, prefix).values() {
            for (round, value) in *rounds {
                *by_round.entry(*round).or_default() += value;
            }
        }
        Interval::of(&by_round.values().copied().collect::<Vec<_>>())
    };
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            table.row(vec![
                size_name(*size),
                depth.to_string(),
                show(summed(&cell, "row", "cpu_us_per_write:"), 1.0),
                show(summed(&cell, "staged", "cpu_us_per_write:"), 1.0),
                show(summed(&cell, "staged", "holder_cpu_us_per_write:"), 1.0),
                show(summed(&cell, "inline", "cpu_us_per_write:"), 1.0),
                show(summed(&cell, "inline", "holder_cpu_us_per_write:"), 1.0),
                show(index.interval(leg, &cell, "staged", "nic_peak_share"), 1.0),
                show(index.interval(leg, &cell, "inline", "nic_peak_share"), 1.0),
            ]);
        }
    }
    table
}

/// Each holder's sync floor before the staged and inline cells, which says what regime a device
/// was in
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn regimes(index: &Index, leg: &str, sizes: &[usize]) -> Table {
    // the holders, from any record of the leg
    let hosts: BTreeSet<String> = sizes
        .iter()
        .flat_map(|size| index.with_prefix(leg, &cell_name(*size, 1), "staged", "probe_before_us:").into_keys())
        .collect();
    let mut head = vec!["Size".to_string(), "Depth".to_string()];
    for host in &hosts {
        head.push(format!("{host} staged"));
        head.push(format!("{host} inline"));
    }
    let head: Vec<&str> = head.iter().map(String::as_str).collect();
    let mut table = Table::new(&head);
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            let mut row = vec![size_name(*size), depth.to_string()];
            for host in &hosts {
                for side in ["staged", "inline"] {
                    row.push(show(index.interval(leg, &cell, side, &format!("probe_before_us:{host}")), 1.0));
                }
            }
            table.row(row);
        }
    }
    table
}

/// The triggers: each size's winner at each depth, the crossovers, and the verdict
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn judged(index: &Index, leg: &str, sizes: &[usize]) -> String {
    let mut out = format!("### The crossover on `{leg}`\n\n");
    let mut crossovers = Vec::new();
    for depth in DEPTHS {
        let (figure, higher) = judged_figure(depth);
        let found = wins(index, leg, depth, sizes);
        let mut table = Table::new(&["Size", "inline", "staged", "wins"]);
        for (size, win, inline, staged) in &found {
            table.row(vec![size_name(*size), show(*inline, 1.0), show(*staged, 1.0), win.word().to_string()]);
        }
        let point = crossover(&found.iter().map(|(size, win, ..)| (*size, *win)).collect::<Vec<_>>());
        out.push_str(&table.render(
            &format!(
                "Depth {depth}: `{figure}`, {} better",
                if higher { "higher" } else { "lower" }
            ),
            &format!("The crossover at depth {depth}: {}.", point.word()),
        ));
        crossovers.push(point);
    }
    let verdict = verdict(crossovers[0], crossovers[1]);
    out.push_str(&format!("**Verdict on `{leg}`**: {}.\n\n", verdict.word()));
    out
}

/// What a reader should know about a leg's cells: a busy link, a moved leader, a failure
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `sizes` - The sizes
fn notes(index: &Index, leg: &str, sizes: &[usize]) -> String {
    let mut notes = Vec::new();
    for size in sizes {
        for depth in DEPTHS {
            let cell = cell_name(*size, depth);
            for path in Path::ALL {
                let side = path.name();
                let worst = |figure: &str| index.interval(leg, &cell, side, figure).map_or(0.0, |interval| interval.max);
                if worst("nic_peak_share") > LINK_LINE {
                    notes.push(format!(
                        "{cell} {side}: the busiest link reached {} of 1 GbE",
                        fmt(worst("nic_peak_share"))
                    ));
                }
                if worst("leaders_moved") > 0.0 {
                    notes.push(format!("{cell} {side}: a leader moved during the cell"));
                }
                let failed = worst("failed") + worst("refused") + worst("behind_failed");
                if failed > 0.0 {
                    let error = index
                        .label(leg, &cell, side, "error")
                        .and_then(|errors| errors.iter().next().cloned())
                        .unwrap_or_default();
                    notes.push(format!("{cell} {side}: failures in a round, the first {error:?}"));
                }
                if worst("readback_wrong") > 0.0 {
                    let error = index
                        .label(leg, &cell, side, "readback_error")
                        .and_then(|errors| errors.iter().next().cloned())
                        .unwrap_or_default();
                    notes.push(format!("{cell} {side}: a write read back wrong in a round {error:?}"));
                }
                if worst("drift") > 0.0 {
                    notes.push(format!("{cell} {side}: a read found a sequence its worker had not counted"));
                }
                if worst("settled") == 0.0 && index.interval(leg, &cell, side, "settled").is_some() {
                    notes.push(format!("{cell} {side}: the merges did not settle in a round"));
                }
            }
        }
    }
    if notes.is_empty() {
        return "No cell crossed the link line, moved a leader, failed, drifted or read back wrong.\n\n".to_string();
    }
    let mut out = String::from("Notes:\n\n");
    for note in notes {
        out.push_str(&format!("- {note}\n"));
    }
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An interval of four rounds
    fn rounds(values: [f64; 4]) -> Option<Interval> {
        Interval::of(&values)
    }

    /// A side wins only where its interval is wholly better, whichever way better is
    #[test]
    fn a_side_wins_only_wholly() {
        let low = rounds([1.0, 2.0, 3.0, 4.0]);
        let high = rounds([5.0, 6.0, 7.0, 8.0]);
        let overlap = rounds([3.5, 6.0, 7.0, 8.0]);
        assert_eq!(winner(high, low, true), Win::Inline);
        assert_eq!(winner(low, high, true), Win::Staged);
        assert_eq!(winner(low, high, false), Win::Inline);
        assert_eq!(winner(overlap, low, true), Win::Neither);
        assert_eq!(winner(None, low, true), Win::Unjudged);
    }

    /// The crossover is the size after the last inline win, the first size when inline never
    /// wins, and none when inline wins at the largest
    #[test]
    fn the_crossover_follows_the_last_inline_win() {
        let sizes = [4096, 8192, 16_384, 32_768];
        let at = |wins: [Win; 4]| crossover(&sizes.iter().copied().zip(wins).collect::<Vec<_>>());
        assert_eq!(at([Win::Inline, Win::Inline, Win::Neither, Win::Staged]), Crossover::At(16_384));
        // a win of inline after a tie moves the crossover past it
        assert_eq!(at([Win::Inline, Win::Neither, Win::Inline, Win::Staged]), Crossover::At(32_768));
        assert_eq!(at([Win::Staged, Win::Neither, Win::Staged, Win::Staged]), Crossover::At(4096));
        assert_eq!(at([Win::Inline, Win::Inline, Win::Inline, Win::Inline]), Crossover::None);
        assert_eq!(at([Win::Inline, Win::Unjudged, Win::Staged, Win::Staged]), Crossover::Unjudged);
    }

    /// The verdicts are the three outcomes named in advance
    #[test]
    fn the_verdicts_are_the_three_named() {
        assert_eq!(verdict(Crossover::None, Crossover::None), Verdict::NoThreshold);
        assert_eq!(verdict(Crossover::At(4096), Crossover::At(8192)), Verdict::NotWorthHaving);
        assert_eq!(verdict(Crossover::At(4096), Crossover::At(65_536)), Verdict::Threshold(4096));
        assert_eq!(verdict(Crossover::At(32_768), Crossover::At(65_536)), Verdict::Threshold(32_768));
        assert_eq!(verdict(Crossover::None, Crossover::At(32_768)), Verdict::Threshold(32_768));
        assert_eq!(verdict(Crossover::At(8192), Crossover::None), Verdict::Threshold(8192));
        assert_eq!(verdict(Crossover::Unjudged, Crossover::At(8192)), Verdict::Unjudged);
    }

    /// A report renders from records of one round, and judges the crossover it holds
    #[test]
    fn a_report_renders_and_judges() {
        let mut records = Vec::new();
        for size in SIZES {
            for depth in DEPTHS {
                for path in Path::ALL {
                    for round in 1..=4u32 {
                        let mut record = Record::new("loopback", &cell_name(size, depth), path.name(), round, false);
                        // inline faster below 32 KiB, staged faster from it, a little noise a round
                        let inline_wins = size < 32 << 10;
                        let base = if (path == Path::Inline) == inline_wins { 1000.0 } else { 500.0 };
                        let noise = f64::from(round);
                        record
                            .set("per_sec", base + noise)
                            .set("p50_us", 1e6 / base + noise)
                            .set("p99_us", 2e6 / base)
                            .set("probe_before_us:loop-a", 80.0);
                        records.push(record);
                    }
                }
            }
        }
        let report = render(&records, "Synthetic.");
        assert!(report.contains("The crossover at depth 1: 32 KiB"), "{report}");
        assert!(report.contains("The crossover at depth 32: 32 KiB"), "{report}");
        assert!(report.contains("**T3 fires**: the threshold is real, at 32 KiB"), "{report}");
        assert!(!report.contains("## lab"));
    }
}
