//! Every round's records merged into intervals, the triggers judged, the arithmetic done
//!
//! The form of X6's `device report`: a figure is its interval across rounds, a ratio is taken
//! round by round and judged by where its whole interval lies, and the three triggers are judged
//! as they were written before the run (`docs/src/object-storage/stripe-row-costs.md`, How it
//! was judged):
//!
//! - **T1. A cold commit stalls its group**, on the group a Zen1 host leads: at depth one the
//!   cold commit's median over the warm one's lies wholly above 1.5, or the neighbour's p99 beside
//!   cold commits in its own group over its p99 alone lies wholly above 1.5 while the same beside
//!   cold commits in another group does not.
//! - **T2. The index sets a floor under the stripe size**: a cold row's index bytes times the
//!   stripe rows a node of 16 TiB at 4+2 replicates, metadata at factor three, every stripe written
//!   in place, exceeds 1 GiB at a 4 MiB stripe.
//! - **T3. The knee is far from the benchmark host's**: the smallest size at which an even
//!   mixture does fewer than 0.7 times its 1 KiB rows a second is at 2 KiB or below, or 32 KiB
//!   or above.

use std::collections::BTreeMap;

use crate::measure::size::SIZES;
use crate::record::Record;
use crate::stats::{fmt, Interval};
use crate::table::Table;

/// A figure's value in each round, by round
type Rounds = BTreeMap<u32, f64>;

/// The records, indexed by measurement, cell, side and figure
struct Index {
    /// Every figure's rounds, by `(measurement, cell, side, figure)`
    figures: BTreeMap<(String, String, String, String), Rounds>,
    /// Every label's values, by `(measurement, cell, side, label)`
    labels: BTreeMap<(String, String, String, String), Vec<String>>,
}

impl Index {
    /// Index records, leaving quick runs out
    ///
    /// # Arguments
    ///
    /// * `records` - Every round's records
    fn of(records: &[Record]) -> Self {
        let mut figures: BTreeMap<_, Rounds> = BTreeMap::new();
        let mut labels: BTreeMap<_, Vec<String>> = BTreeMap::new();
        // a quick run proves a leg runs and is never a measurement, unless only quick runs exist
        let only_quick = records.iter().all(|record| record.quick);
        for record in records.iter().filter(|record| only_quick || !record.quick) {
            for (name, value) in &record.metrics {
                figures
                    .entry((
                        record.measurement.clone(),
                        record.cell.clone(),
                        record.side.clone(),
                        name.clone(),
                    ))
                    .or_default()
                    .insert(record.round, *value);
            }
            for (name, value) in &record.labels {
                let entry = labels
                    .entry((
                        record.measurement.clone(),
                        record.cell.clone(),
                        record.side.clone(),
                        name.clone(),
                    ))
                    .or_default();
                if !entry.contains(value) {
                    entry.push(value.clone());
                }
            }
        }
        Index { figures, labels }
    }

    /// A figure's rounds
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `figure` - The figure
    fn rounds(&self, measurement: &str, cell: &str, side: &str, figure: &str) -> Rounds {
        self.figures
            .get(&(measurement.into(), cell.into(), side.into(), figure.into()))
            .cloned()
            .unwrap_or_default()
    }

    /// A figure's interval across rounds
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `figure` - The figure
    fn interval(&self, measurement: &str, cell: &str, side: &str, figure: &str) -> Option<Interval> {
        let values: Vec<f64> = self.rounds(measurement, cell, side, figure).into_values().collect();
        Interval::of(&values)
    }

    /// A figure's interval written out, or a dash when it was not taken
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `figure` - The figure
    fn show(&self, measurement: &str, cell: &str, side: &str, figure: &str) -> String {
        self.interval(measurement, cell, side, figure)
            .map_or_else(|| "—".to_string(), |interval| interval.show())
    }

    /// The values a label took across rounds, joined
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `label` - The label
    fn label(&self, measurement: &str, cell: &str, side: &str, label: &str) -> String {
        self.labels
            .get(&(measurement.into(), cell.into(), side.into(), label.into()))
            .map_or_else(|| "—".to_string(), |values| values.join(", "))
    }

    /// Every figure of a cell and side whose name starts `<prefix>:`, written `host value`
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `prefix` - The figure's name before its host
    fn by_host(&self, measurement: &str, cell: &str, side: &str, prefix: &str) -> String {
        let shown: Vec<String> = self
            .figures
            .iter()
            .filter(|((of, at, by, name), _)| of == measurement && at == cell && by == side && name.starts_with(&format!("{prefix}:")))
            .filter_map(|((.., name), rounds)| {
                let values: Vec<f64> = rounds.values().copied().collect();
                Interval::of(&values).map(|interval| format!("{} {}", &name[prefix.len() + 1..], fmt(interval.median)))
            })
            .collect();
        if shown.is_empty() {
            "—".to_string()
        } else {
            shown.join(", ")
        }
    }

    /// The cells of a measurement, in the order they sort
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    fn cells(&self, measurement: &str) -> Vec<(String, String)> {
        let mut cells: Vec<(String, String)> = self
            .figures
            .keys()
            .filter(|(of, ..)| of == measurement)
            .map(|(_, cell, side, _)| (cell.clone(), side.clone()))
            .collect();
        cells.dedup();
        cells
    }
}

/// A ratio of two figures taken round by round, as an interval
///
/// # Arguments
///
/// * `top` - The numerator's rounds
/// * `bottom` - The denominator's rounds
fn ratio(top: &Rounds, bottom: &Rounds) -> Option<Interval> {
    // only the rounds both sides ran in, and never over zero
    let values: Vec<f64> = top
        .iter()
        .filter_map(|(round, value)| {
            bottom
                .get(round)
                .filter(|below| **below > 0.0)
                .map(|below| value / below)
        })
        .collect();
    Interval::of(&values)
}

/// A ratio's interval written out
///
/// # Arguments
///
/// * `interval` - The ratio, if it could be taken
fn show_ratio(interval: Option<Interval>) -> String {
    interval.map_or_else(|| "—".to_string(), |interval| format!("{}×", interval.show()))
}

/// Render the whole report
///
/// # Arguments
///
/// * `records` - Every round's records
/// * `label` - The line naming where they were measured
#[must_use]
pub fn render(records: &[Record], label: &str) -> String {
    let index = Index::of(records);
    let rounds: std::collections::BTreeSet<u32> = records.iter().map(|record| record.round).collect();
    let quick = records.iter().all(|record| record.quick);
    let mut out = String::from("# X10 merged across rounds\n\n");
    out.push_str(&format!(
        "{} rounds ({:?}){}. Every figure is `median [lowest–highest]` across rounds; a ratio is \
         taken round by round.\n\n",
        rounds.len(),
        rounds,
        if quick { ", **quick runs only: not a measurement**" } else { "" }
    ));
    out.push_str(&rate(&index, label));
    out.push_str(&rows(&index, label));
    out.push_str(&cold(&index, label));
    out.push_str(&size(&index, label));
    out.push_str(&remedy(&index, label));
    out.push_str(&triggers(&index));
    out
}

/// The rate leg's tables
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn rate(index: &Index, label: &str) -> String {
    let mut table = Table::new(&[
        "Cell",
        "Leader",
        "Rows/s",
        "Rows/s a group",
        "p50 µs",
        "p99 µs",
        "Device B written/row",
        "Device B read/row",
        "WAL B/row/replica",
        "Appends/sync",
        "NIC peak",
    ]);
    let mut rewrite = Table::new(&["Inline bytes", "Commits/s", "p50 µs", "p99 µs", "Device B written/commit"]);
    for (cell, side) in index.cells("rate") {
        let show = |figure: &str| index.show("rate", &cell, &side, figure);
        if cell.starts_with("rewrite") {
            rewrite.row(vec![
                show("inline"),
                show("per_sec"),
                show("p50_us"),
                show("p99_us"),
                show("device_written_per_op"),
            ]);
            continue;
        }
        table.row(vec![
            cell.clone(),
            index.label("rate", &cell, &side, "leader"),
            show("per_sec"),
            show("per_group"),
            show("p50_us"),
            show("p99_us"),
            show("device_written_per_op"),
            show("device_read_per_op"),
            show("wal_bytes_per_op_per_replica"),
            show("wal_appends_per_sync"),
            show("nic_peak_share"),
        ]);
    }
    let mut out = table.render("1. Rows a second a group", label);
    out.push_str(&rewrite.render("1b. One row rewritten, a conditional update after another (Q17)", label));
    out
}

/// The rows leg's tables: the load, a row's bytes, and each member's figures
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn rows(index: &Index, label: &str) -> String {
    let mut load = Table::new(&["Table", "Rows", "Rows/s at depth 96", "Device B written/row", "WAL B/row/replica"]);
    let mut nodes = Table::new(&[
        "Member",
        "When",
        "Partitions",
        "Archive bytes",
        "Rows in memory, B",
        "Archive map, B",
        "Table index, B",
        "LRU, B",
        "WAL index, B",
        "Resident, B",
    ]);
    for (cell, side) in index.cells("rows") {
        let show = |figure: &str| index.show("rows", &cell, &side, figure);
        if let Some(table) = cell.strip_prefix("load ") {
            load.row(vec![
                table.to_string(),
                show("rows"),
                show("per_sec"),
                show("device_written_per_row"),
                show("wal_bytes_per_row_per_replica"),
            ]);
        } else if let Some(member) = cell.strip_prefix("node ") {
            nodes.row(vec![
                member.to_string(),
                side.clone(),
                show("partitions"),
                show("archive_bytes"),
                show("memory_bytes"),
                show("archive_map_bytes"),
                show("table_index_bytes"),
                show("lru_bytes"),
                show("wal_index_bytes"),
                show("resident_bytes"),
            ]);
        }
    }
    // a row's bytes, the largest member's
    let mut per_row = Table::new(&["Figure", "Bytes a row"]);
    for (figure, name) in [
        ("archive_bytes_per_row_stripe", "Archived, a stripe row (the members' own count)"),
        ("archive_bytes_per_row_digest", "Archived, a stripe row with six digests"),
        ("archive_bytes_per_row_object", "Archived, an object row with nothing inline"),
        ("archive_map_per_partition", "Archive map, a cold row (after the restart, every partition)"),
        ("archive_map_per_row_delta", "Archive map, a loaded row (cold less the baseline)"),
        ("resident_per_row_delta", "Resident memory, a cold row (the process, cold less the baseline)"),
        ("table_index_per_row_loaded", "Table index, a resident row (loaded less the baseline)"),
        ("lru_per_partition_sealed", "Eviction list, an evictable row"),
        ("memory_per_row_loaded", "Rows in memory, a resident row (the budget's count, loaded less the baseline)"),
        ("resident_per_row_loaded", "Resident memory, a resident row (the process, loaded less the baseline)"),
    ] {
        per_row.row(vec![name.to_string(), index.show("rows", "per row", "-", figure)]);
    }
    let mut out = load.render("2. The load", label);
    out.push_str(&per_row.render("2b. Bytes a row", label));
    out.push_str(&nodes.render("2c. Each member: the baseline, sealed, cold after the restart, and after the cold phases", label));
    out
}

/// The cold commit's tables
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn cold(index: &Index, label: &str) -> String {
    let mut table = Table::new(&[
        "Cell",
        "Side",
        "Leader",
        "Ops",
        "Ops/s",
        "p50 µs",
        "p99 µs",
        "Read p50 µs",
        "Commit after the read, p50 µs",
        "Device B read/op, by host",
    ]);
    let mut neighbour = Table::new(&[
        "Beside it",
        "Writer p50 µs",
        "Writer p99 µs",
        "p99 over alone",
        "The other writer's ops/s",
    ]);
    let alone = index.rounds("cold", "neighbour aim=slow", "alone", "p99_us");
    for (cell, side) in index.cells("cold") {
        let show = |figure: &str| index.show("cold", &cell, &side, figure);
        if cell.starts_with("neighbour") {
            neighbour.row(vec![
                side.clone(),
                show("p50_us"),
                show("p99_us"),
                show_ratio(ratio(&index.rounds("cold", &cell, &side, "p99_us"), &alone)),
                show("beside_per_sec"),
            ]);
            continue;
        }
        table.row(vec![
            cell.clone(),
            side.clone(),
            index.label("cold", &cell, &side, "leader"),
            show("applied"),
            show("per_sec"),
            show("p50_us"),
            show("p99_us"),
            show("part_p50_us"),
            show("rest_p50_us"),
            index.by_host("cold", &cell, &side, "device_read_per_op"),
        ]);
    }
    let mut out = table.render("3. The cold commit", label);
    out.push_str(&neighbour.render("3b. A writer of resident rows in the slow group, depth one", label));
    out
}

/// The supplement's tables: cold commits under load, with and without the read before them
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn remedy(index: &Index, label: &str) -> String {
    // nothing to show for a set of records without the supplement
    if index.cells("remedy").is_empty() {
        return String::new();
    }
    let mut table = Table::new(&[
        "Side",
        "Ops/s",
        "p50 µs",
        "p99 µs",
        "Read p50 µs",
        "Commit after the read, p50 µs",
        "Commit after the read, p99 µs",
        "Commit over warm, p50",
    ]);
    let cell = "stripe commit aim=slow depth=32";
    let warm = index.rounds("remedy", cell, "warm", "p50_us");
    for side in ["cold", "read-leader", "read-every", "warm"] {
        let show = |figure: &str| index.show("remedy", cell, side, figure);
        // the commit's own share: after the read when there was one, else the whole operation
        let commit = if side.starts_with("read") { "rest_p50_us" } else { "p50_us" };
        table.row(vec![
            side.to_string(),
            show("per_sec"),
            show("p50_us"),
            show("p99_us"),
            show("part_p50_us"),
            show("rest_p50_us"),
            show("rest_p99_us"),
            show_ratio(ratio(&index.rounds("remedy", cell, side, commit), &warm)),
        ]);
    }
    let mut neighbour = Table::new(&[
        "Beside it",
        "Writer p50 µs",
        "Writer p99 µs",
        "p99 over alone",
        "p99 over beside warm writers",
        "The other writers' ops/s",
    ]);
    let alone = index.rounds("remedy", "neighbour aim=slow", "alone", "p99_us");
    let beside_warm = index.rounds("remedy", "neighbour aim=slow", "warm", "p99_us");
    for side in ["alone", "cold", "cold-read-leader", "warm"] {
        let show = |figure: &str| index.show("remedy", "neighbour aim=slow", side, figure);
        let p99 = index.rounds("remedy", "neighbour aim=slow", side, "p99_us");
        neighbour.row(vec![
            side.to_string(),
            show("p50_us"),
            show("p99_us"),
            show_ratio(ratio(&p99, &alone)),
            show_ratio(ratio(&p99, &beside_warm)),
            show("beside_per_sec"),
        ]);
    }
    let mut out = table.render("5. The supplement: cold commits at depth 32, with and without the read first", label);
    out.push_str(&neighbour.render("5b. The supplement: a writer of resident rows beside eight others", label));
    out
}

/// The size leg's table and the knee
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn size(index: &Index, label: &str) -> String {
    let mut table = Table::new(&[
        "Inline",
        "Mix",
        "Ops/s",
        "Over 1 KiB",
        "MiB/s",
        "p50 µs",
        "p99 µs",
        "Device B written/op",
        "NIC peak",
    ]);
    for mix in ["r0", "r50", "r100"] {
        let base = index.rounds("size", &format!("inline={} mix={mix}", SIZES[0]), "-", "per_sec");
        for size in SIZES {
            let cell = format!("inline={size} mix={mix}");
            let show = |figure: &str| index.show("size", &cell, "-", figure);
            table.row(vec![
                bytes(size),
                mix.to_string(),
                show("per_sec"),
                show_ratio(ratio(&index.rounds("size", &cell, "-", "per_sec"), &base)),
                show("payload_mib_per_sec"),
                show("p50_us"),
                show("p99_us"),
                show("device_written_per_op"),
                show("nic_peak_share"),
            ]);
        }
    }
    table.render("4. An object held inline, depth 32", label)
}

/// A size in bytes written as KiB or MiB
///
/// # Arguments
///
/// * `size` - The size
fn bytes(size: usize) -> String {
    if size >= 1 << 20 {
        format!("{} MiB", size >> 20)
    } else {
        format!("{} KiB", size >> 10)
    }
}

/// The stripe rows a node replicates for 16 TiB of pool devices at 4+2, every stripe written in
/// place, with metadata at a factor of three
///
/// # Arguments
///
/// * `stripe` - The stripe's size in bytes
#[must_use]
pub fn rows_a_node(stripe: f64) -> f64 {
    // the user bytes 16 TiB of devices hold at 4+2, cut into stripes, three copies of each row
    // spread over as many nodes as hold the devices
    3.0 * 16.0 * (1u64 << 40) as f64 * 4.0 / 6.0 / stripe
}

/// The triggers, judged as written before the run, and T2's arithmetic
///
/// # Arguments
///
/// * `index` - The records
fn triggers(index: &Index) -> String {
    let mut out = String::from("## The triggers\n\n");
    // T1 (i): the cold commit's median over the warm one's, slow group, depth one
    let cold_cell = "stripe commit aim=slow depth=1";
    let t1a = ratio(
        &index.rounds("cold", cold_cell, "cold", "p50_us"),
        &index.rounds("cold", cold_cell, "warm", "p50_us"),
    );
    // T1 (ii): the neighbour beside cold commits in its own group, and in another
    let alone = index.rounds("cold", "neighbour aim=slow", "alone", "p99_us");
    let same = ratio(&index.rounds("cold", "neighbour aim=slow", "cold-same", "p99_us"), &alone);
    let other = ratio(&index.rounds("cold", "neighbour aim=slow", "cold-other", "p99_us"), &alone);
    let fires_a = t1a.is_some_and(|interval| interval.wholly_above(1.5));
    let fires_b = same.is_some_and(|interval| interval.wholly_above(1.5))
        && !other.is_some_and(|interval| interval.wholly_above(1.5));
    out.push_str(&format!(
        "**T1. A cold commit stalls its group**: {}. (i) cold over warm median at depth one, slow \
         group: {}; (ii) the neighbour's p99 over alone, beside cold commits in its group: {}, \
         in another group: {}.\n\n",
        if fires_a || fires_b { "**fires**" } else { "does not fire" },
        show_ratio(t1a),
        show_ratio(same),
        show_ratio(other)
    ));
    // the statements made in advance beside T1
    let fast_cell = "stripe commit aim=fast depth=1";
    let deep = "stripe commit aim=slow depth=32";
    out.push_str(&format!(
        "Beside T1, as stated in advance: cold over warm rows a second at depth 32, slow group: {}, \
         fast group: {}; the neighbour's p99 beside cold commits in its group over beside warm \
         ones: {}; cold over warm median at depth one, fast group: {}; the commit after a read \
         through every member over the warm commit: {}; after a read through the leader: {}; \
         after a read through a follower, over a warm commit through that follower: {}.\n\n",
        show_ratio(ratio(
            &index.rounds("cold", deep, "cold", "per_sec"),
            &index.rounds("cold", deep, "warm", "per_sec"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", "stripe commit aim=fast depth=32", "cold", "per_sec"),
            &index.rounds("cold", "stripe commit aim=fast depth=32", "warm", "per_sec"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", "neighbour aim=slow", "cold-same", "p99_us"),
            &index.rounds("cold", "neighbour aim=slow", "warm-same", "p99_us"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", fast_cell, "cold", "p50_us"),
            &index.rounds("cold", fast_cell, "warm", "p50_us"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", cold_cell, "read-every", "rest_p50_us"),
            &index.rounds("cold", cold_cell, "warm", "p50_us"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", cold_cell, "read-leader", "rest_p50_us"),
            &index.rounds("cold", cold_cell, "warm", "p50_us"),
        )),
        show_ratio(ratio(
            &index.rounds("cold", cold_cell, "read-follower", "rest_p50_us"),
            &index.rounds("cold", cold_cell, "warm-follower", "p50_us"),
        )),
    ));
    // T2: a cold row's index bytes times the rows a node replicates
    let per_row = index.interval("rows", "per row", "-", "archive_map_per_partition");
    let resident_extra: f64 = ["table_index_per_row_loaded", "lru_per_partition_sealed", "memory_per_row_loaded"]
        .iter()
        .filter_map(|figure| index.interval("rows", "per row", "-", figure))
        .map(|interval| interval.median)
        .sum();
    let mut table = Table::new(&["Stripe", "Rows a node replicates", "Index, cold rows", "Memory, resident rows"]);
    let gib = f64::from(1u32 << 30);
    for stripe in [1u64 << 20, 4 << 20, 16 << 20, 64 << 20] {
        let rows = rows_a_node(stripe as f64);
        let cold = per_row.map_or(0.0, |interval| interval.max * rows / gib);
        let resident = per_row.map_or(0.0, |interval| (interval.max + resident_extra) * rows / gib);
        table.row(vec![
            bytes(usize::try_from(stripe).unwrap_or(usize::MAX)),
            format!("{:.2} M", rows / 1e6),
            format!("{} GiB", fmt(cold)),
            format!("{} GiB", fmt(resident)),
        ]);
    }
    let at_four = per_row.map_or(0.0, |interval| interval.max * rows_a_node(f64::from(4u32 << 20)) / gib);
    out.push_str(&format!(
        "**T2. The index sets a floor under the stripe size**: {}. A cold row's index is {} bytes; \
         at a 4 MiB stripe a node of 16 TiB at 4+2 replicates {:.2} M rows, {} GiB of index \
         (the line is 1 GiB). The resident column adds a resident row's table index, LRU entry \
         and the row ({} bytes), which is what keeping stripe rows resident would cost.\n\n",
        if at_four > 1.0 { "**fires**" } else { "does not fire" },
        per_row.map_or_else(|| "—".to_string(), |interval| interval.show()),
        rows_a_node(f64::from(4u32 << 20)) / 1e6,
        fmt(at_four),
        fmt(resident_extra)
    ));
    out.push_str(&table.render("T2's arithmetic, at the largest round's bytes a row", ""));
    // T3: the knee of the even mixture
    let base = index.rounds("size", &format!("inline={} mix=r50", SIZES[0]), "-", "per_sec");
    let mut knee = None;
    let mut network = None;
    for size in SIZES {
        let cell = format!("inline={size} mix=r50");
        let over = ratio(&index.rounds("size", &cell, "-", "per_sec"), &base);
        // the first size whose ratio lies wholly below the line is the knee
        if knee.is_none() && over.is_some_and(|interval| interval.wholly_below(0.7)) {
            knee = Some(size);
        }
        // and the first whose busiest host's link ran past 70% is where the network binds
        if network.is_none()
            && index
                .interval("size", &cell, "-", "nic_peak_share")
                .is_some_and(|interval| interval.median > 0.7)
        {
            network = Some(size);
        }
    }
    let fires = knee.is_some_and(|knee| knee <= 2 << 10 || knee >= 32 << 10);
    out.push_str(&format!(
        "**T3. The knee is far from the benchmark host's (8 KiB)**: {}. The lab's knee, the \
         smallest size whose even mixture does fewer than 0.7× its 1 KiB rows a second in every \
         round: {}. The first size at which a host's link ran past 70% of 1 GbE: {}.\n",
        if fires { "**fires**" } else { "does not fire" },
        knee.map_or_else(|| "none below 1 MiB".to_string(), bytes),
        network.map_or_else(|| "none".to_string(), bytes)
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two records a round, so ratios and triggers can be read from fixed values
    fn pair(cell: &str, side: &str, figure: &str, values: &[f64]) -> Vec<Record> {
        values
            .iter()
            .enumerate()
            .map(|(round, value)| {
                let mut record = Record::new("cold", cell, side, round as u32 + 1, false);
                record.set(figure, *value);
                record
            })
            .collect()
    }

    /// T1 fires on a cold median wholly above one and a half times the warm one
    #[test]
    fn t1_fires_on_a_ratio_wholly_above_its_line() {
        let cell = "stripe commit aim=slow depth=1";
        let mut records = pair(cell, "cold", "p50_us", &[3200.0, 3300.0, 3100.0, 3400.0]);
        records.extend(pair(cell, "warm", "p50_us", &[2000.0, 2000.0, 2000.0, 2000.0]));
        assert!(render(&records, "").contains("**T1. A cold commit stalls its group**: **fires**"));
        // and not when one round sits at the line
        let mut records = pair(cell, "cold", "p50_us", &[3200.0, 3000.0, 3100.0, 3400.0]);
        records.extend(pair(cell, "warm", "p50_us", &[2000.0, 2000.0, 2000.0, 2000.0]));
        assert!(render(&records, "").contains("**T1. A cold commit stalls its group**: does not fire"));
    }

    /// T2's rows a node: 16 TiB at 4+2 in 4 MiB stripes, three copies, is 8.39 million
    #[test]
    fn t2_counts_the_rows_a_node_replicates() {
        let rows = rows_a_node(f64::from(4u32 << 20));
        assert!((rows - 8_388_608.0).abs() < 1.0, "{rows}");
    }

    /// T3 finds the first size wholly below seven tenths of the 1 KiB rate
    #[test]
    fn t3_finds_the_knee() {
        let mut records = Vec::new();
        for (size, rate) in [(1 << 10, 1000.0), (2 << 10, 900.0), (4 << 10, 800.0), (8 << 10, 650.0)] {
            for round in 1..=2 {
                let mut record = Record::new("size", &format!("inline={size} mix=r50"), "-", round, false);
                record.set("per_sec", rate);
                records.push(record);
            }
        }
        let report = render(&records, "");
        assert!(report.contains("round: 8 KiB"), "{report}");
        assert!(report.contains("**T3. The knee is far from the benchmark host's (8 KiB)**: does not fire"));
    }
}
