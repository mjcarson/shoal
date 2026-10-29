//! X13's tables: each round's as it runs, and every round merged and judged
//!
//! The triggers are the three agreed with the user on 2026-10-05, before the harness existed.
//! Each is judged on the build the driver would run on that host - native on europa, where the
//! bench's admin program is built for the host, and `znver1` on the Zen1 hosts - over loopback,
//! plaintext, by the lab's rule: a trigger fires only where every round is past its line.
//!
//! - **T1. One core cannot make bytes as fast as a device takes them**: one client core's put,
//!   the fastest published generator's fill and CRC-64/NVME, below X6's device write rate.
//! - **T2. Regenerating a read to check it costs more than the read**: one core's get, checked by
//!   making each frame again with that generator, below X6's device read rate.
//! - **T3. No generator with a published definition is fast enough**: the fastest published
//!   generator's fill of 1 MiB cold, below X6's device write rate.

use std::collections::{BTreeMap, BTreeSet};

use crate::device::record::Record;
use crate::device::stats::{fmt, Interval};
use crate::device::table::Table;
use crate::device::SideOut;

/// The generators with published definitions, in table order
const PUBLISHED: &[&str] = &["splitmix", "xoshiro", "chacha8", "aes-ctr"];

/// Every generator, in table order
const GENERATORS: &[&str] = &["stamped", "splitmix", "xoshiro", "chacha8", "aes-ctr"];

/// X6's device rates on a host, MiB a second written and read: fio's, as X11 judged against
///
/// # Arguments
///
/// * `host` - The host
#[must_use]
pub fn device_rates(host: &str) -> (f64, f64) {
    // europa's Optane 900P, or the Zen1 hosts' 970 EVO
    if host == "europa" {
        (2501.0, 2553.0)
    } else {
        (722.0, 857.0)
    }
}

/// A figure of a side, or nothing
///
/// # Arguments
///
/// * `out` - The side
/// * `name` - The figure
fn get(out: &SideOut, name: &str) -> String {
    out.metrics.get(name).map_or_else(|| "—".to_string(), |value| fmt(*value))
}

/// One round's table of one section, as it runs
///
/// # Arguments
///
/// * `title` - The section's title
/// * `label` - The line naming where it ran
/// * `round` - The round
/// * `section` - The section's name
/// * `outs` - Its sides
#[must_use]
pub fn section_table(title: &str, label: &str, round: u32, section: &str, outs: &[SideOut]) -> String {
    let (head, figures): (Vec<&str>, Vec<&str>) = match section {
        "check" => (vec!["cell", "side", "ok", "objects/s"], vec!["ok", "objects_per_s"]),
        "make" => (vec!["cell", "side", "GiB/s", "lowest run", "highest run", "µs a call"], vec!["gib_s", "gib_s_min", "gib_s_max", "us_call"]),
        "make-cores" => (vec!["cell", "side", "GiB/s, all", "GiB/s, least core"], vec!["gib_s_total", "gib_s_core_min"]),
        "folder" => (vec!["cell", "side", "MiB/s", "cpu ms/GiB", "device MiB read", "from device"], vec!["mib_s", "cpu_ms_gib", "device_read_mib", "from_device"]),
        _ => (
            vec!["cell", "side", "MiB/s", "client cpu ms/GiB", "client busy", "core busy", "MiB/s at a busy core", "server cpu ms/GiB", "busiest executor", "host cpu ms/GiB", "mismatches", "kTLS"],
            vec!["mib_s", "client_cpu_ms_gib", "client_busy", "client_core_busy", "capacity_mib_s", "server_cpu_ms_gib", "server_busy_max", "host_cpu_ms_gib", "mismatches", "ktls"],
        ),
    };
    let mut table = Table::new(&head);
    for out in outs {
        let mut row = vec![out.cell.clone(), out.side.clone()];
        row.extend(figures.iter().map(|figure| get(out, figure)));
        table.row(row);
    }
    table.render(&format!("{title}, round {round}"), label)
}

/// Every figure of every side, by leg, section, cell and side, then by figure and round
#[derive(Default)]
struct Index {
    /// The values
    values: BTreeMap<(String, String, String, String), BTreeMap<String, BTreeMap<u32, f64>>>,
    /// Each leg's host
    hosts: BTreeMap<String, String>,
    /// The legs, in the order first seen
    legs: Vec<String>,
}

impl Index {
    /// The records of a run, quick or not
    ///
    /// # Arguments
    ///
    /// * `records` - Every record
    /// * `quick` - Whether to read the quick ones, which are never a measurement
    fn new(records: &[Record], quick: bool) -> Self {
        let mut index = Index::default();
        for record in records.iter().filter(|record| record.quick == quick) {
            if !index.legs.contains(&record.leg) {
                index.legs.push(record.leg.clone());
            }
            index.hosts.insert(record.leg.clone(), record.host.clone());
            let key = (record.leg.clone(), record.measurement.clone(), record.cell.clone(), record.side.clone());
            let figures = index.values.entry(key).or_default();
            for (name, value) in &record.metrics {
                figures.entry(name.clone()).or_default().insert(record.round, *value);
            }
        }
        index
    }

    /// A figure's value in each round
    ///
    /// # Arguments
    ///
    /// * `key` - The leg, section, cell and side
    /// * `figure` - The figure
    fn rounds(&self, key: (&str, &str, &str, &str), figure: &str) -> BTreeMap<u32, f64> {
        let key = (key.0.to_string(), key.1.to_string(), key.2.to_string(), key.3.to_string());
        self.values
            .get(&key)
            .and_then(|figures| figures.get(figure))
            .cloned()
            .unwrap_or_default()
    }

    /// A figure's interval across rounds
    ///
    /// # Arguments
    ///
    /// * `key` - The leg, section, cell and side
    /// * `figure` - The figure
    fn interval(&self, key: (&str, &str, &str, &str), figure: &str) -> Option<Interval> {
        let values: Vec<f64> = self.rounds(key, figure).into_values().collect();
        Interval::of(&values)
    }

    /// A figure's interval written for a table, or a dash
    ///
    /// # Arguments
    ///
    /// * `key` - The leg, section, cell and side
    /// * `figure` - The figure
    fn show(&self, key: (&str, &str, &str, &str), figure: &str) -> String {
        self.interval(key, figure).map_or_else(|| "—".to_string(), |interval| interval.show())
    }

    /// The legs that ran a section
    ///
    /// # Arguments
    ///
    /// * `section` - The section
    fn legs_of(&self, section: &str) -> Vec<String> {
        self.legs
            .iter()
            .filter(|leg| self.values.keys().any(|key| &key.0 == *leg && key.1 == section))
            .cloned()
            .collect()
    }

    /// Whether a leg is one a trigger is judged on: over loopback, and the build the driver runs
    /// there
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    fn judged(&self, leg: &str) -> bool {
        let host = self.hosts.get(leg).map_or("", String::as_str);
        !leg.contains(" to ") && (if host == "europa" { leg.ends_with("native") } else { leg.ends_with("znver1") })
    }
}

/// Every round merged: the checks, the sections, then the triggers and the statements
///
/// # Arguments
///
/// * `records` - Every record
/// * `quick` - Whether to report the quick records instead
#[must_use]
pub fn report(records: &[Record], quick: bool) -> String {
    let index = Index::new(records, quick);
    let mut out = String::from("# X13: the benchmark's shape, every round merged\n\n");
    out.push_str("Every figure is the median of the rounds, with the lowest and the highest round in brackets.\n\n");
    if quick {
        out.push_str("**Quick records: not a measurement.**\n\n");
    }
    out.push_str(&checks(&index));
    out.push_str(&making(&index));
    out.push_str(&making_cores(&index));
    out.push_str(&streams(&index));
    out.push_str(&stream_cores(&index));
    out.push_str(&folder(&index));
    out.push_str(&triggers(&index));
    out.push_str(&statements(&index));
    out
}

/// The checks: every reference and seek met in every round, and every digest the same on every leg
///
/// # Arguments
///
/// * `index` - The records
fn checks(index: &Index) -> String {
    let mut out = String::from("## Checks\n\n");
    let legs = index.legs_of("check");
    let mut table = Table::new(&["generator", "meets its reference", "seeks", "digests, every round of every leg alike"]);
    for generator in GENERATORS.iter().chain(std::iter::once(&"crc64nvme")) {
        let all_ok = |side: &str| -> String {
            // every round of every leg that checked it
            let per_leg: Vec<Vec<f64>> = legs
                .iter()
                .map(|leg| index.rounds((leg, "check", generator, side), "ok").into_values().collect())
                .filter(|values: &Vec<f64>| !values.is_empty())
                .collect();
            let values: Vec<f64> = per_leg.iter().flatten().copied().collect();
            if values.is_empty() {
                "—".to_string()
            } else if values.iter().all(|value| *value == 1.0) {
                format!("yes ({} rounds on {} legs)", values.len(), per_leg.len())
            } else {
                "**no**".to_string()
            }
        };
        // every digest figure of every round of every leg, which must be one value each
        let mut digests: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
        for leg in &legs {
            if let Some(figures) = index.values.get(&(leg.clone(), "check".to_string(), (*generator).to_string(), "digest".to_string())) {
                for (name, rounds) in figures {
                    digests.entry(name.clone()).or_default().extend(rounds.values().map(|value| *value as u64));
                }
            }
        }
        let alike = if digests.is_empty() {
            "—".to_string()
        } else if digests.values().all(|values| values.len() == 1) {
            format!("yes ({} legs)", legs.len())
        } else {
            "**no**".to_string()
        };
        table.row(vec![(*generator).to_string(), all_ok("reference"), all_ok("seek"), alike]);
    }
    out.push_str(&table.render("References, seeks and digests", "every leg that ran the checks"));
    // how fast a description becomes its object list
    let mut table = Table::new(&["description", "leg", "objects/s", "GiB described"]);
    for shape in ["fixed", "uniform", "doublings", "table"] {
        let cell = format!("describe {shape}");
        for leg in &legs {
            table.row(vec![
                shape.to_string(),
                leg.clone(),
                index.show((leg, "check", &cell, "expand"), "objects_per_s"),
                index.show((leg, "check", &cell, "expand"), "total_gib"),
            ]);
        }
    }
    out.push_str(&table.render("A million objects described, expanded", "paths and sizes, one core"));
    out
}

/// Section one: each leg's GiB a second by generator, operation, unit and temperature
///
/// # Arguments
///
/// * `index` - The records
fn making(index: &Index) -> String {
    let mut out = String::from("## 1. Making bytes on one core\n\n");
    for leg in index.legs_of("make") {
        for temp in ["cold", "hot"] {
            let mut head = vec!["generator".to_string()];
            let mut cells = Vec::new();
            for op in ["fill", "fill+crc", "verify"] {
                for unit in ["4K", "64K", "1M"] {
                    head.push(format!("{op} {unit}"));
                    cells.push(format!("{op} {unit} {temp}"));
                }
            }
            let head: Vec<&str> = head.iter().map(String::as_str).collect();
            let mut table = Table::new(&head);
            for generator in GENERATORS {
                let mut row = vec![(*generator).to_string()];
                row.extend(cells.iter().map(|cell| index.show((&leg, "make", cell, generator), "gib_s")));
                table.row(row);
            }
            // the references in the fill columns, where they compare
            for (reference, side) in [("crc", "crc64nvme"), ("copy", "memcpy")] {
                let mut row = vec![format!("{reference} alone")];
                for op in ["fill", "fill+crc", "verify"] {
                    for unit in ["4K", "64K", "1M"] {
                        row.push(if op == "fill" {
                            index.show((&leg, "make", &format!("{reference} {unit} {temp}"), side), "gib_s")
                        } else {
                            String::new()
                        });
                    }
                }
                table.row(row);
            }
            out.push_str(&table.render(&format!("GiB/s, {temp}"), &leg));
        }
    }
    out
}

/// Section 3a: making on several cores at once
///
/// # Arguments
///
/// * `index` - The records
fn making_cores(index: &Index) -> String {
    let mut out = String::from("## 3a. Making bytes on several cores, no wire\n\n");
    for leg in index.legs_of("make-cores") {
        let mut table = Table::new(&["generator", "x1 GiB/s", "x2 GiB/s", "x4 GiB/s", "x4 least core"]);
        for generator in GENERATORS {
            let cell = |count: u32| format!("fill+crc 1M cold x{count}");
            table.row(vec![
                (*generator).to_string(),
                index.show((&leg, "make-cores", &cell(1), generator), "gib_s_total"),
                index.show((&leg, "make-cores", &cell(2), generator), "gib_s_total"),
                index.show((&leg, "make-cores", &cell(4), generator), "gib_s_total"),
                index.show((&leg, "make-cores", &cell(4), generator), "gib_s_core_min"),
            ]);
        }
        out.push_str(&table.render("fill+crc 1M cold, every core summed", &leg));
    }
    out
}

/// Sections 2 and 5: each leg's puts and gets
///
/// # Arguments
///
/// * `index` - The records
fn streams(index: &Index) -> String {
    let mut out = String::from("## 2 and 5. Against a server that discards\n\n");
    for leg in index.legs_of("streams") {
        for verb in ["put", "get"] {
            for encryption in ["plain", "ktls"] {
                let cell = format!("{verb} 1M {encryption}");
                let sides: Vec<String> = index
                    .values
                    .keys()
                    .filter(|key| key.0 == leg && key.1 == "streams" && key.2 == cell)
                    .map(|key| key.3.clone())
                    .collect();
                if sides.is_empty() {
                    continue;
                }
                let mut table = Table::new(&["side", "MiB/s", "client cpu ms/GiB", "client busy", "MiB/s at a busy core", "server cpu ms/GiB", "busiest executor", "host cpu ms/GiB", "mismatches"]);
                for side in sides {
                    let key = (leg.as_str(), "streams", cell.as_str(), side.as_str());
                    table.row(vec![
                        side.clone(),
                        index.show(key, "mib_s"),
                        index.show(key, "client_cpu_ms_gib"),
                        index.show(key, "client_busy"),
                        index.show(key, "capacity_mib_s"),
                        index.show(key, "server_cpu_ms_gib"),
                        index.show(key, "server_busy_max"),
                        index.show(key, "host_cpu_ms_gib"),
                        index.show(key, "mismatches"),
                    ]);
                }
                out.push_str(&table.render(&cell, &leg));
            }
        }
    }
    out
}

/// Section 3b: the put from several client cores
///
/// # Arguments
///
/// * `index` - The records
fn stream_cores(index: &Index) -> String {
    let mut out = String::from("## 3b. The put from several client cores\n\n");
    for leg in index.legs_of("cores") {
        let mut table = Table::new(&["side", "streams", "MiB/s, all", "client busy, each", "MiB/s at a busy core", "busiest executor"]);
        for count in [1, 2, 4] {
            let cell = format!("put 1M plain x{count}");
            for generator in PUBLISHED {
                let side = format!("fill+crc {generator}");
                let key = (leg.as_str(), "cores", cell.as_str(), side.as_str());
                if index.interval(key, "mib_s").is_none() {
                    continue;
                }
                table.row(vec![
                    side.clone(),
                    count.to_string(),
                    index.show(key, "mib_s"),
                    index.show(key, "client_busy"),
                    index.show(key, "capacity_mib_s"),
                    index.show(key, "server_busy_max"),
                ]);
            }
        }
        out.push_str(&table.render("put 1M plain, one stream a client core", &leg));
    }
    out
}

/// Section 4: the folder
///
/// # Arguments
///
/// * `index` - The records
fn folder(index: &Index) -> String {
    let mut out = String::from("## 4. A folder of real files\n\n");
    for leg in index.legs_of("folder") {
        let mut table = Table::new(&["files", "side", "MiB/s cold", "cpu ms/GiB cold", "read from the device", "MiB/s hot", "cpu ms/GiB hot"]);
        for size in ["64K", "1M", "64M"] {
            for side in ["read", "read+crc", "read+sha256"] {
                let (cold, hot) = (format!("{size} cold"), format!("{size} hot"));
                table.row(vec![
                    size.to_string(),
                    side.to_string(),
                    index.show((&leg, "folder", &cold, side), "mib_s"),
                    index.show((&leg, "folder", &cold, side), "cpu_ms_gib"),
                    index.show((&leg, "folder", &cold, side), "from_device"),
                    index.show((&leg, "folder", &hot, side), "mib_s"),
                    index.show((&leg, "folder", &hot, side), "cpu_ms_gib"),
                ]);
            }
        }
        out.push_str(&table.render("1 MiB reads, one core", &leg));
    }
    out
}

/// The fastest published generator on a leg's plaintext put, by its median
///
/// # Arguments
///
/// * `index` - The records
/// * `leg` - The leg
fn fastest_put(index: &Index, leg: &str) -> Option<(&'static str, Interval)> {
    PUBLISHED
        .iter()
        .filter_map(|generator| {
            let side = format!("fill+crc {generator}");
            index
                .interval((leg, "streams", "put 1M plain", &side), "mib_s")
                .map(|interval| (*generator, interval))
        })
        .max_by(|left, right| left.1.median.total_cmp(&right.1.median))
}

/// "**fires**" or "holds"
///
/// # Arguments
///
/// * `fires` - Whether the trigger fired
fn verdict(fires: bool) -> &'static str {
    if fires {
        "**fires**"
    } else {
        "holds"
    }
}

/// The three triggers, judged on every leg the driver would run as built
///
/// # Arguments
///
/// * `index` - The records
fn triggers(index: &Index) -> String {
    let mut out = String::from("## The triggers\n\n");
    out.push_str("Agreed with the user on 2026-10-05 before the harness existed. A trigger fires on a leg only where every round is past its line.\n\n");
    let mut table = Table::new(&["leg", "trigger", "line", "measured", "beside it", "verdict"]);
    for leg in index.legs.iter().filter(|leg| index.judged(leg)) {
        let host = index.hosts.get(leg).cloned().unwrap_or_default();
        let (write, read) = device_rates(&host);
        // T1: the fastest published generator's put, made and checksummed, plaintext
        if let Some((generator, put)) = fastest_put(index, leg) {
            let side = format!("fill+crc {generator}");
            let capacity = index.show((leg, "streams", "put 1M plain", &side), "capacity_mib_s");
            let ktls = index.show((leg, "streams", "put 1M ktls", &side), "mib_s");
            table.row(vec![
                leg.clone(),
                format!("T1, put with {generator}"),
                format!("{write} MiB/s"),
                format!("{} MiB/s", put.show()),
                format!("{capacity} at a busy core; kTLS {ktls}"),
                verdict(put.max < write).to_string(),
            ]);
            // T2: the same generator's get, checked by making it again
            let regenerate = format!("regenerate {generator}");
            if let Some(get) = index.interval((leg, "streams", "get 1M plain", &regenerate), "mib_s") {
                let crc = index.show((leg, "streams", "get 1M plain", "crc"), "mib_s");
                let capacity = index.show((leg, "streams", "get 1M plain", &regenerate), "capacity_mib_s");
                table.row(vec![
                    leg.clone(),
                    format!("T2, get checked by {generator}"),
                    format!("{read} MiB/s"),
                    format!("{} MiB/s", get.show()),
                    format!("{capacity} at a busy core; by the ledger's CRC {crc}"),
                    verdict(get.max < read).to_string(),
                ]);
            }
        }
        // T3: the best published fill of 1 MiB cold, each round's best
        let mut best: BTreeMap<u32, (f64, &str)> = BTreeMap::new();
        for generator in PUBLISHED {
            for (round, value) in index.rounds((leg, "make", "fill 1M cold", generator), "gib_s") {
                let entry = best.entry(round).or_insert((0.0, generator));
                if value > entry.0 {
                    *entry = (value, generator);
                }
            }
        }
        let values: Vec<f64> = best.values().map(|(value, _)| value * 1024.0).collect();
        if let Some(fill) = Interval::of(&values) {
            let winners: BTreeSet<&str> = best.values().map(|(_, generator)| *generator).collect();
            table.row(vec![
                leg.clone(),
                "T3, fill 1M cold".to_string(),
                format!("{write} MiB/s"),
                format!("{} MiB/s", fill.show()),
                format!("best each round: {}", winners.into_iter().collect::<Vec<_>>().join(", ")),
                verdict(fill.max < write).to_string(),
            ]);
        }
    }
    out.push_str(&table.render("T1 to T3", "loopback, plaintext, the driver's own build on each host"));
    out
}

/// The statements made in advance, reported and not judged
///
/// # Arguments
///
/// * `index` - The records
fn statements(index: &Index) -> String {
    let mut out = String::from("## Statements made in advance\n\n");
    let mut table = Table::new(&[
        "leg",
        "fastest published, 64K hot",
        "fastest published, 1M cold",
        "its fill+crc 1M cold, GiB/s",
        "the send's share of a kTLS put's client cpu",
        "1M files cold, MiB/s",
        "the same through SHA-256",
        "1M files hot through SHA-256",
    ]);
    for leg in index.legs_of("make") {
        let fastest = |cell: &str| -> String {
            PUBLISHED
                .iter()
                .filter_map(|generator| index.interval((&leg, "make", cell, generator), "gib_s").map(|interval| (*generator, interval)))
                .max_by(|left, right| left.1.median.total_cmp(&right.1.median))
                .map_or_else(|| "—".to_string(), |(generator, interval)| format!("{generator} {}", interval.show()))
        };
        let best = PUBLISHED
            .iter()
            .filter_map(|generator| index.interval((&leg, "make", "fill 1M cold", generator), "gib_s").map(|interval| (*generator, interval)))
            .max_by(|left, right| left.1.median.total_cmp(&right.1.median))
            .map(|(generator, _)| generator);
        let made = best.map_or_else(|| "—".to_string(), |generator| index.show((&leg, "make", "fill+crc 1M cold", generator), "gib_s"));
        // the making's cost a GiB is the plaintext put's with fill and CRC less its baseline's,
        // which makes nothing; the send is everything else a kTLS put's client spent, round by
        // round
        let share = best
            .and_then(|generator| {
                let side = format!("fill+crc {generator}");
                let plain = index.rounds((&leg, "streams", "put 1M plain", &side), "client_cpu_ms_gib");
                let baseline = index.rounds((&leg, "streams", "put 1M plain", "pattern"), "client_cpu_ms_gib");
                let ktls = index.rounds((&leg, "streams", "put 1M ktls", &side), "client_cpu_ms_gib");
                let shares: Vec<f64> = plain
                    .iter()
                    .filter_map(|(round, made)| {
                        let (base, encrypted) = (baseline.get(round)?, ktls.get(round)?);
                        (*encrypted > 0.0).then(|| (1.0 - (made - base) / encrypted) * 100.0)
                    })
                    .collect();
                Interval::of(&shares)
            })
            .map_or_else(|| "—".to_string(), |share| format!("{:.0}% [{:.0}–{:.0}%]", share.median, share.min, share.max));
        table.row(vec![
            leg.clone(),
            fastest("fill 64K hot"),
            fastest("fill 1M cold"),
            made,
            share,
            index.show((&leg, "folder", "1M cold", "read"), "mib_s"),
            index.show((&leg, "folder", "1M cold", "read+sha256"), "mib_s"),
            index.show((&leg, "folder", "1M hot", "read+sha256"), "mib_s"),
        ]);
    }
    out.push_str(&table.render("Reported, not judged", "every leg that made bytes"));
    out
}
