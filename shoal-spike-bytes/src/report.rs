//! Every round's records merged into intervals, and the two triggers judged as they were written
//!
//! The form of X10's `x10 report`: a figure is its interval across rounds, a ratio is taken round
//! by round and judged by where its whole interval lies. The triggers, stated before the harness
//! existed and agreed with the user on 2026-10-07
//! (`docs/src/object-storage/bytes-through-groups.md`, How it was judged):
//!
//! - **T1. A is within reach of a replicated pool's rate**: on the loopback leg, the put arm at
//!   1 MiB and at 4 MiB acknowledges at least 0.5× the pool's ceiling in every round, the ceiling
//!   being the Optane's fio sequential write rate at that size over the three copies it holds.
//! - **T2. Write amplification near two**: the preload's settled device bytes written per byte
//!   stored a copy is at most 2.5 at 1 MiB and at 4 MiB on every host of every factor-three leg,
//!   in every round.
//!
//! The design moves only if both fire.

use std::collections::{BTreeMap, BTreeSet};

use crate::measure::SIZES;
use crate::record::Record;
use crate::stats::{fmt, Interval};
use crate::table::Table;

/// A figure's value in each round, by round
type Rounds = BTreeMap<u32, f64>;

/// The legs, in the order the tables list them, with the factor each runs at
pub const LEGS: [(&str, u32); 4] = [("lab", 3), ("loopback", 3), ("titan", 1), ("europa", 1)];

/// The arms, in the order the tables list them
pub const SIDES: [&str; 5] = ["preload", "put", "get", "mix", "overwrite"];

/// T1's line: the put arm over the pool's ceiling
pub const T1_LINE: f64 = 0.5;

/// T2's line: device bytes written per byte stored a copy
pub const T2_LINE: f64 = 2.5;

/// The sizes the triggers are judged at
pub const JUDGED: [usize; 2] = [1 << 20, 4 << 20];

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
    /// * `leg` - The leg, or `fio`
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
    /// * `leg` - The leg, or `fio`
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

    /// The fio rate of a host's device at a size, each round, from the root that holds a role
    ///
    /// # Arguments
    ///
    /// * `host` - The host
    /// * `role` - `latency` or `throughput`
    /// * `size` - The size
    #[must_use]
    pub fn fio(&self, host: &str, role: &str, size: usize) -> Option<&Rounds> {
        let side = format!("size={size}");
        self.figures
            .iter()
            .find(|((m, cell, s, figure), _)| {
                m == "fio"
                    && s == &side
                    && figure == "write_mib_s"
                    && cell.split(' ').next() == Some(host)
                    && self
                        .labels
                        .get(&(m.clone(), cell.clone(), s.clone(), "role".to_string()))
                        .is_some_and(|roles| roles.contains(role))
            })
            .map(|(_, rounds)| rounds)
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

/// A row size written as people read it
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

/// The interval of a ratio of two figures taken round by round
///
/// # Arguments
///
/// * `top` - The numerator's rounds
/// * `bottom` - The denominator's rounds
fn ratio(top: &Rounds, bottom: &Rounds) -> Option<Interval> {
    let values: Vec<f64> = top
        .iter()
        .filter_map(|(round, value)| bottom.get(round).filter(|b| **b > 0.0).map(|b| value / b))
        .collect();
    Interval::of(&values)
}

/// The verdict of a trigger over an interval of values against its line
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Every round on the firing side of the line
    Fires,
    /// Every round on the other side
    DoesNotFire,
    /// The rounds straddle the line
    Straddles,
    /// Nothing to judge
    Unjudged,
}

impl Verdict {
    /// The verdict as the report writes it
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Verdict::Fires => "**fires**",
            Verdict::DoesNotFire => "does not fire",
            Verdict::Straddles => "straddles its line",
            Verdict::Unjudged => "not judged: missing figures",
        }
    }

    /// Two verdicts' conjunction: fires only if both do
    #[must_use]
    pub fn and(self, other: Verdict) -> Verdict {
        match (self, other) {
            (Verdict::Fires, Verdict::Fires) => Verdict::Fires,
            (Verdict::Unjudged, _) | (_, Verdict::Unjudged) => Verdict::Unjudged,
            (Verdict::DoesNotFire, _) | (_, Verdict::DoesNotFire) => Verdict::DoesNotFire,
            _ => Verdict::Straddles,
        }
    }
}

/// T1 at one size: the loopback put arm over the Optane's ceiling a copy, round by round
///
/// # Arguments
///
/// * `index` - The records
/// * `size` - The size
#[must_use]
pub fn t1(index: &Index, size: usize) -> (Option<Interval>, Verdict) {
    let cell = format!("size={size}");
    let (Some(put), Some(fio)) = (
        index.rounds("loopback", &cell, "put", "ack_mib_s"),
        index.fio("europa", "latency", size),
    ) else {
        return (None, Verdict::Unjudged);
    };
    // the ceiling a copy is the device's rate over the three copies it holds
    let ceiling: Rounds = fio.iter().map(|(round, rate)| (*round, rate / 3.0)).collect();
    let Some(interval) = ratio(put, &ceiling) else {
        return (None, Verdict::Unjudged);
    };
    let verdict = if interval.min >= T1_LINE {
        Verdict::Fires
    } else if interval.max < T1_LINE {
        Verdict::DoesNotFire
    } else {
        Verdict::Straddles
    };
    (Some(interval), verdict)
}

/// T2 at one size: the preload's settled amplification on every host of every factor-three leg
///
/// # Arguments
///
/// * `index` - The records
/// * `size` - The size
#[must_use]
pub fn t2(index: &Index, size: usize) -> (Vec<(String, Interval)>, Verdict) {
    let cell = format!("size={size}");
    let mut found = Vec::new();
    for (leg, factor) in LEGS {
        if factor < 3 {
            continue;
        }
        for (host, rounds) in index.with_prefix(leg, &cell, "preload", "settled:amp:") {
            // a host's whole figure, not one device's
            if host.contains(':') {
                continue;
            }
            if let Some(interval) = Interval::of(&rounds.values().copied().collect::<Vec<_>>()) {
                found.push((format!("{leg} {host}"), interval));
            }
        }
    }
    if found.is_empty() {
        return (found, Verdict::Unjudged);
    }
    let verdict = if found.iter().all(|(_, interval)| interval.max <= T2_LINE) {
        Verdict::Fires
    } else if found.iter().all(|(_, interval)| interval.min > T2_LINE) {
        Verdict::DoesNotFire
    } else if found.iter().any(|(_, interval)| interval.min > T2_LINE) {
        // one host wholly above the line is enough that not every host is near two
        Verdict::DoesNotFire
    } else {
        Verdict::Straddles
    };
    (found, verdict)
}

/// The whole report: every table, then the triggers
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
        "# X3 report\n\n{} records over {} round{}; every figure is the median of the rounds with the \
         lowest and the highest in brackets.\n\n",
        records.len(),
        rounds.len(),
        if rounds.len() == 1 { "" } else { "s" }
    );
    out.push_str(&ceilings(&index, label));
    for (leg, factor) in LEGS {
        out.push_str(&throughput(&index, leg, factor, label));
        out.push_str(&amplification(&index, leg, label));
        out.push_str(&memory(&index, leg, label));
        out.push_str(&neighbour(&index, leg, label));
    }
    out.push_str(&triggers(&index));
    out
}

/// The devices' own rates, a host and root each
///
/// # Arguments
///
/// * `index` - The records
/// * `label` - The line naming where they were measured
fn ceilings(index: &Index, label: &str) -> String {
    let mut heads = vec!["Host and root".to_string()];
    heads.extend(SIZES.iter().map(|size| format!("{} MiB/s", size_name(*size))));
    let mut table = Table::new(&heads.iter().map(String::as_str).collect::<Vec<_>>());
    let cells: BTreeSet<String> = index
        .figures
        .keys()
        .filter(|(m, ..)| m == "fio")
        .map(|(_, cell, ..)| cell.clone())
        .collect();
    for cell in cells {
        let mut row = vec![cell.clone()];
        for size in SIZES {
            row.push(show(index.interval("fio", &cell, &format!("size={size}"), "write_mib_s"), 1.0));
        }
        table.row(row);
    }
    if table.is_empty() {
        return String::new();
    }
    table.render("The devices' own rates (fio, sequential, direct, eight in flight)", label)
}

/// A leg's bytes a second and tails, a size and arm each
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `factor` - Its factor
/// * `label` - The line naming where they were measured
fn throughput(index: &Index, leg: &str, factor: u32, label: &str) -> String {
    let mut table = Table::new(&[
        "Size", "Arm", "MiB/s acked", "Sustained MiB/s", "Ops/s", "p50 ms", "p99 ms", "Failed", "Driver cpu %",
    ]);
    for size in SIZES {
        let cell = format!("size={size}");
        for side in SIDES {
            if index.rounds(leg, &cell, side, "ack_mib_s").is_none() {
                continue;
            }
            // the arm's own kind leads its latency; the mixture's put is its write
            let kind = match side {
                "get" => "get",
                "overwrite" => "overwrite",
                _ => "put",
            };
            let failed: Option<Interval> = {
                let mut by_round: Rounds = BTreeMap::new();
                for (name, rounds) in index.with_prefix(leg, &cell, side, "") {
                    if name.ends_with(":failed") || name == "failed" {
                        for (round, value) in rounds {
                            *by_round.entry(*round).or_default() += value;
                        }
                    }
                }
                Interval::of(&by_round.values().copied().collect::<Vec<_>>())
            };
            let ops = if side == "preload" {
                index.interval(leg, &cell, side, "rows").zip(index.interval(leg, &cell, side, "secs")).map(
                    |(rows, secs)| Interval {
                        min: rows.min / secs.max,
                        median: rows.median / secs.median,
                        max: rows.max / secs.min,
                        rounds: rows.rounds,
                    },
                )
            } else {
                index.interval(leg, &cell, side, &format!("{kind}:ops_s"))
            };
            table.row(vec![
                size_name(size),
                side.to_string(),
                show(index.interval(leg, &cell, side, "ack_mib_s"), 1.0),
                show(index.interval(leg, &cell, side, "sustained_mib_s"), 1.0),
                show(ops, 1.0),
                show(index.interval(leg, &cell, side, &format!("{kind}:p50_ms")), 1.0),
                show(index.interval(leg, &cell, side, &format!("{kind}:p99_ms")), 1.0),
                show(failed, 1.0),
                show(index.interval(leg, &cell, side, "driver_cpu_mean"), 1.0),
            ]);
        }
    }
    if table.is_empty() {
        return String::new();
    }
    table.render(&format!("{leg}: bytes a second (factor {factor})"), label)
}

/// A leg's device bytes for each byte stored a copy, settled, every host and each device role
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `label` - The line naming where they were measured
fn amplification(index: &Index, leg: &str, label: &str) -> String {
    let mut table = Table::new(&[
        "Size", "Arm", "Host", "Settled ×", "WAL device ×", "Archive device ×", "At the arm's end ×", "WAL counted ×", "Settle s",
    ]);
    for size in SIZES {
        let cell = format!("size={size}");
        for side in ["preload", "put", "mix", "overwrite"] {
            let hosts: BTreeSet<String> = index
                .with_prefix(leg, &cell, side, "settled:amp:")
                .keys()
                .filter_map(|rest| rest.split(':').next().map(str::to_string))
                .collect();
            for host in hosts {
                let figure = |name: &str| index.interval(leg, &cell, side, name);
                table.row(vec![
                    size_name(size),
                    side.to_string(),
                    host.clone(),
                    show(figure(&format!("settled:amp:{host}")), 1.0),
                    show(figure(&format!("settled:amp:{host}:wal")), 1.0),
                    show(figure(&format!("settled:amp:{host}:archive")), 1.0),
                    show(figure(&format!("end:amp:{host}")), 1.0),
                    show(figure("settled:amp_wal_counted"), 1.0),
                    show(figure("settle_secs"), 1.0),
                ]);
            }
        }
    }
    if table.is_empty() {
        return String::new();
    }
    table.render(&format!("{leg}: device bytes written for each byte stored a copy"), label)
}

/// A leg's memory: the peak resident set and rows held by any member, and the least free memory
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `label` - The line naming where they were measured
fn memory(index: &Index, leg: &str, label: &str) -> String {
    let mut table = Table::new(&["Size", "Arm", "Member", "Resident peak GiB", "Rows held peak GiB", "WAL index MiB"]);
    let gib = f64::from(1 << 30);
    for size in SIZES {
        let cell = format!("size={size}");
        for side in SIDES {
            for (member, _) in index.with_prefix(leg, &cell, side, "resident_peak:") {
                table.row(vec![
                    size_name(size),
                    side.to_string(),
                    member.clone(),
                    show(index.interval(leg, &cell, side, &format!("resident_peak:{member}")), gib),
                    show(index.interval(leg, &cell, side, &format!("rows_peak:{member}")), gib),
                    show(
                        index.interval(leg, &cell, side, &format!("wal_index_peak:{member}")),
                        f64::from(1 << 20),
                    ),
                ]);
            }
        }
    }
    if table.is_empty() {
        return String::new();
    }
    table.render(&format!("{leg}: memory"), label)
}

/// A leg's neighbour: the paced stream's tail beside each arm, against its tail alone
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
/// * `label` - The line naming where they were measured
fn neighbour(index: &Index, leg: &str, label: &str) -> String {
    let mut table = Table::new(&[
        "Size",
        "Beside",
        "Read p99 ms",
        "Read worst second p99 ms",
        "Write p99 ms",
        "Write worst second p99 ms",
        "Read p99 over alone",
        "Failed",
    ]);
    for size in SIZES {
        let cell = format!("size={size}");
        let alone = index.rounds(leg, &cell, "alone", "paced:small_get:p99_ms");
        for side in ["alone", "put", "get", "mix", "overwrite"] {
            let Some(beside) = index.rounds(leg, &cell, side, "paced:small_get:p99_ms") else {
                continue;
            };
            let failed = index
                .rounds(leg, &cell, side, "paced:small_get:failed")
                .zip(index.rounds(leg, &cell, side, "paced:small_put:failed"))
                .and_then(|(get, put)| {
                    let sums: Vec<f64> = get.iter().map(|(r, v)| v + put.get(r).copied().unwrap_or(0.0)).collect();
                    Interval::of(&sums)
                });
            table.row(vec![
                size_name(size),
                side.to_string(),
                show(index.interval(leg, &cell, side, "paced:small_get:p99_ms"), 1.0),
                show(index.interval(leg, &cell, side, "paced:small_get:worst_second_p99_ms"), 1.0),
                show(index.interval(leg, &cell, side, "paced:small_put:p99_ms"), 1.0),
                show(index.interval(leg, &cell, side, "paced:small_put:worst_second_p99_ms"), 1.0),
                alone.and_then(|alone| ratio(beside, alone)).map_or_else(|| "–".to_string(), |r| r.show()),
                show(failed, 1.0),
            ]);
        }
    }
    if table.is_empty() {
        return String::new();
    }
    table.render(&format!("{leg}: the paced stream beside each arm"), label)
}

/// The two triggers, judged as written, and what both together say
///
/// # Arguments
///
/// * `index` - The records
fn triggers(index: &Index) -> String {
    let mut out = String::from("## The triggers\n\n");
    let mut t1_all = Verdict::Fires;
    let mut t2_all = Verdict::Fires;
    for size in JUDGED {
        let (interval, verdict) = t1(index, size);
        out.push_str(&format!(
            "- **T1 at {}**: the loopback put arm over the Optane's ceiling a copy is {} against {}: {}\n",
            size_name(size),
            interval.map_or_else(|| "–".to_string(), |interval| interval.show()),
            fmt(T1_LINE),
            verdict.word()
        ));
        t1_all = t1_all.and(verdict);
        let (hosts, verdict) = t2(index, size);
        let worst = hosts
            .iter()
            .max_by(|a, b| a.1.max.total_cmp(&b.1.max))
            .map_or_else(|| "–".to_string(), |(host, interval)| format!("{host} {}", interval.show()));
        out.push_str(&format!(
            "- **T2 at {}**: the preload's settled amplification, the highest host {} against {}: {}\n",
            size_name(size),
            worst,
            fmt(T2_LINE),
            verdict.word()
        ));
        t2_all = t2_all.and(verdict);
    }
    out.push_str(&format!(
        "\n**T1** {}; **T2** {}. The design moves only if both fire: {}.\n",
        t1_all.word(),
        t2_all.word(),
        if t1_all.and(t2_all) == Verdict::Fires {
            "**it moves**: replicated SSD pools stay tables"
        } else {
            "it does not move"
        }
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record of the loopback put arm and the Optane's fio at a size, for a round
    fn t1_records(round: u32, put: f64, fio: f64) -> Vec<Record> {
        let mut arm = Record::new("loopback", "size=1048576", "put", round, false);
        arm.set("ack_mib_s", put);
        let mut ceiling = Record::new("fio", "europa /optane/shoal-x3", "size=1048576", round, false);
        ceiling.set("write_mib_s", fio).label("role", "latency").label("host", "europa");
        vec![arm, ceiling]
    }

    /// T1 fires only when every round reaches half the ceiling a copy
    #[test]
    fn t1_is_judged_round_by_round() {
        // a ceiling of 2400 is 800 a copy, so the line is 400
        let mut records = t1_records(1, 450.0, 2400.0);
        records.extend(t1_records(2, 420.0, 2400.0));
        assert_eq!(t1(&Index::of(&records), 1 << 20).1, Verdict::Fires);
        records.extend(t1_records(3, 380.0, 2400.0));
        assert_eq!(t1(&Index::of(&records), 1 << 20).1, Verdict::Straddles);
        let low = [t1_records(1, 100.0, 2400.0), t1_records(2, 120.0, 2400.0)].concat();
        assert_eq!(t1(&Index::of(&low), 1 << 20).1, Verdict::DoesNotFire);
        assert_eq!(t1(&Index::of(&[]), 1 << 20).1, Verdict::Unjudged);
    }

    /// T2 fires only when every host of every factor-three leg is at or under the line
    #[test]
    fn t2_needs_every_host() {
        let preload = |leg: &str, round: u32, hosts: &[(&str, f64)]| {
            let mut record = Record::new(leg, "size=1048576", "preload", round, false);
            for (host, amp) in hosts {
                record.set(&format!("settled:amp:{host}"), *amp);
                // a device's figure is not a host's
                record.set(&format!("settled:amp:{host}:wal"), 9.0);
            }
            record
        };
        let near = vec![
            preload("lab", 1, &[("titan", 2.2), ("europa", 2.4)]),
            preload("loopback", 1, &[("europa", 2.3)]),
            // a factor-one leg is not judged
            preload("titan", 1, &[("titan", 9.0)]),
        ];
        assert_eq!(t2(&Index::of(&near), 1 << 20).1, Verdict::Fires);
        let far = vec![preload("lab", 1, &[("titan", 2.2), ("europa", 4.0)])];
        assert_eq!(t2(&Index::of(&far), 1 << 20).1, Verdict::DoesNotFire);
    }

    /// The design moves only if both triggers fire
    #[test]
    fn both_have_to_fire() {
        assert_eq!(Verdict::Fires.and(Verdict::Fires), Verdict::Fires);
        assert_eq!(Verdict::Fires.and(Verdict::DoesNotFire), Verdict::DoesNotFire);
        assert_eq!(Verdict::Straddles.and(Verdict::Fires), Verdict::Straddles);
        assert_eq!(Verdict::Unjudged.and(Verdict::Fires), Verdict::Unjudged);
    }
}
