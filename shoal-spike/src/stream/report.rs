//! X11's records merged across rounds, and its triggers judged as they were set before the run
//!
//! The triggers, as agreed with the user on 2026-10-05 before the harness existed:
//!
//! - **T1. A shared connection hurts small queries**: at 1 MiB frames, a small request's p99 on
//!   the stream's connection above 1.25 times its p99 on a connection of its own to the same
//!   executor beside the same stream, in every round, on any leg.
//! - **T2. The hop**: (a) a kTLS connection cannot be handed between executors; or (b) a 1 MiB
//!   write stream through a buffer hop runs below 0.9 times the direct stream's rate in every
//!   round, or costs more than 1.25 times its cpu a GiB in every round.
//! - **T3. kTLS bounds a stream below a device**: one kTLS connection at 1 MiB, to and from
//!   memory, on loopback, below the device's rate X6 measured on that host in every round.
//!
//! Beside them, two statements made in advance and reported, not judged: the frame is the
//! smallest within a tenth of the best rate and cpu, and the window the smallest number of
//! frames reaching nine tenths of the best rate.

use std::collections::BTreeMap;

use crate::device::record::Record;
use crate::device::stats::{fmt, Interval};

/// The line T1 is judged against
pub const T1_LINE: f64 = 1.25;

/// The rate line of T2
pub const T2_RATE: f64 = 0.9;

/// The cpu line of T2
pub const T2_CPU: f64 = 1.25;

/// The rates X6 measured with fio on each host's device, MiB/s: 1 MiB chunks written, 64 KiB reads
///
/// # Arguments
///
/// * `host` - The host
#[must_use]
pub fn device_rates(host: &str) -> Option<(f64, f64)> {
    match host {
        "europa" => Some((2501.0, 2553.0)),
        "titan" | "hyperion" => Some((722.0, 857.0)),
        _ => None,
    }
}

/// Every figure, by leg, section, cell, side and figure, one value a round
#[derive(Default)]
struct Index {
    /// The figures
    values: BTreeMap<(String, String, String, String), BTreeMap<String, BTreeMap<u32, f64>>>,
    /// The legs in the order they were first seen
    legs: Vec<String>,
    /// The cells of each leg and section in the order they were first seen
    cells: BTreeMap<(String, String), Vec<String>>,
    /// The sides of each cell in the order they were first seen
    sides: BTreeMap<(String, String, String), Vec<String>>,
    /// The host each leg's client ran on
    hosts: BTreeMap<String, String>,
    /// The server each leg's client reached
    servers: BTreeMap<String, String>,
}

impl Index {
    /// Index the records, leaving quick ones out unless only quick ones are wanted
    ///
    /// # Arguments
    ///
    /// * `records` - The records
    /// * `quick` - Whether to read quick records instead of measured ones
    fn new(records: &[Record], quick: bool) -> Self {
        let mut index = Index::default();
        for record in records.iter().filter(|record| record.quick == quick) {
            let push = |list: &mut Vec<String>, item: &str| {
                if !list.iter().any(|known| known == item) {
                    list.push(item.to_string());
                }
            };
            push(&mut index.legs, &record.leg);
            push(
                index
                    .cells
                    .entry((record.leg.clone(), record.measurement.clone()))
                    .or_default(),
                &record.cell,
            );
            push(
                index
                    .sides
                    .entry((
                        record.leg.clone(),
                        record.measurement.clone(),
                        record.cell.clone(),
                    ))
                    .or_default(),
                &record.side,
            );
            index.hosts.insert(record.leg.clone(), record.host.clone());
            index.servers.insert(record.leg.clone(), record.fs.clone());
            let figures = index
                .values
                .entry((
                    record.leg.clone(),
                    record.measurement.clone(),
                    record.cell.clone(),
                    record.side.clone(),
                ))
                .or_default();
            for (name, value) in &record.metrics {
                figures
                    .entry(name.clone())
                    .or_default()
                    .insert(record.round, *value);
            }
        }
        index
    }

    /// A figure's value in each round
    ///
    /// # Arguments
    ///
    /// * `key` - Leg, section, cell and side
    /// * `figure` - The figure
    fn rounds(&self, key: (&str, &str, &str, &str), figure: &str) -> BTreeMap<u32, f64> {
        let key = (
            key.0.to_string(),
            key.1.to_string(),
            key.2.to_string(),
            key.3.to_string(),
        );
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
    /// * `key` - Leg, section, cell and side
    /// * `figure` - The figure
    fn interval(&self, key: (&str, &str, &str, &str), figure: &str) -> Option<Interval> {
        Interval::of(&self.rounds(key, figure).into_values().collect::<Vec<_>>())
    }

    /// The ratio of one side's figure to another's, taken round by round
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `section` - The section
    /// * `cell` - The cell
    /// * `side` - The side on top
    /// * `under` - The side below
    /// * `figure` - The figure
    fn ratio(
        &self,
        leg: &str,
        section: &str,
        cell: &str,
        side: &str,
        under: &str,
        figure: &str,
    ) -> Option<Interval> {
        let top = self.rounds((leg, section, cell, side), figure);
        let bottom = self.rounds((leg, section, cell, under), figure);
        let ratios: Vec<f64> = top
            .iter()
            .filter_map(|(round, value)| bottom.get(round).filter(|b| **b > 0.0).map(|b| value / b))
            .collect();
        Interval::of(&ratios)
    }

    /// The cells of a section of a leg
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `section` - The section
    fn cells(&self, leg: &str, section: &str) -> Vec<String> {
        self.cells
            .get(&(leg.to_string(), section.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// The sides of a cell
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `section` - The section
    /// * `cell` - The cell
    fn sides(&self, leg: &str, section: &str, cell: &str) -> Vec<String> {
        self.sides
            .get(&(leg.to_string(), section.to_string(), cell.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Every side of a section's cells, in the order they were first seen
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    /// * `section` - The section
    fn all_sides(&self, leg: &str, section: &str) -> Vec<String> {
        let mut sides: Vec<String> = Vec::new();
        for cell in self.cells(leg, section) {
            for side in self.sides(leg, section, &cell) {
                if !sides.contains(&side) {
                    sides.push(side);
                }
            }
        }
        sides
    }

    /// Whether a leg ran over loopback
    ///
    /// # Arguments
    ///
    /// * `leg` - The leg
    fn loopback(&self, leg: &str) -> bool {
        self.servers
            .get(leg)
            .is_some_and(|server| server == "127.0.0.1")
    }
}

/// An interval shown, or a dash
///
/// # Arguments
///
/// * `interval` - The interval, if there is one
fn show(interval: Option<Interval>) -> String {
    interval.map_or_else(|| "—".to_string(), |interval| interval.show())
}

/// A ratio shown with its sign, or a dash
///
/// # Arguments
///
/// * `interval` - The ratio's interval, if there is one
fn show_ratio(interval: Option<Interval>) -> String {
    interval.map_or_else(
        || "—".to_string(),
        |interval| format!("{}×", interval.show()),
    )
}

/// A markdown table from a head and rows
///
/// # Arguments
///
/// * `head` - The column headings
/// * `rows` - The rows
fn table(head: &[String], rows: &[Vec<String>]) -> String {
    let mut out = format!(
        "| {} |\n|{}\n",
        head.join(" | "),
        head.iter().map(|_| " --- |").collect::<String>()
    );
    for row in rows {
        out.push_str(&format!("| {} |\n", row.join(" | ")));
    }
    out.push('\n');
    out
}

/// One table of a section: a row a cell, a column a side, each showing one figure
///
/// # Arguments
///
/// * `index` - The figures
/// * `leg` - The leg
/// * `section` - The section
/// * `figure` - The figure
fn figure_table(index: &Index, leg: &str, section: &str, figure: &str) -> String {
    let cells = index.cells(leg, section);
    if cells.is_empty() {
        return String::new();
    }
    // every side any cell has, so a section whose first cell has one side still shows the rest
    let sides = index.all_sides(leg, section);
    let mut head = vec!["cell".to_string()];
    head.extend(sides.iter().cloned());
    let rows: Vec<Vec<String>> = cells
        .iter()
        .map(|cell| {
            let mut row = vec![cell.clone()];
            for side in &sides {
                row.push(show(index.interval((leg, section, cell, side), figure)));
            }
            row
        })
        .collect();
    table(&head, &rows)
}

/// Merge the records and judge the triggers
///
/// # Arguments
///
/// * `records` - Every record of every round
/// * `quick` - Whether to read the quick records, which are never a measurement
#[must_use]
pub fn report(records: &[Record], quick: bool) -> String {
    let index = Index::new(records, quick);
    let mut out = String::from("# X11 merged across rounds\n\n");
    out.push_str("Every figure is `median [lowest–highest]` over the rounds. A ratio is taken round by round. ");
    out.push_str("cpu is milliseconds a GiB moved: the server's executors, the client's stream thread, and every cpu of the host(s).\n\n");
    for leg in &index.legs {
        out.push_str(&format!(
            "## {leg}\n\nClient on {}, server at {}.\n\n",
            index.hosts.get(leg).map_or("?", String::as_str),
            index.servers.get(leg).map_or("?", String::as_str)
        ));
        for (section, title) in [
            ("rate", "1. Rate and cpu by frame"),
            ("window", "2. The window"),
            ("tail", "3. A small request beside a stream"),
            ("route", "4. Routes to the other executor"),
        ] {
            if index.cells(leg, section).is_empty() {
                continue;
            }
            out.push_str(&format!("### {title}\n\n"));
            let figures: &[(&str, &str)] = match section {
                "rate" => &[
                    ("mib_s", "MiB/s"),
                    ("server_cpu_ms_gib", "server cpu ms/GiB"),
                    ("client_cpu_ms_gib", "client cpu ms/GiB"),
                    ("host_cpu_ms_gib", "host cpu ms/GiB"),
                ],
                "window" => &[
                    ("mib_s", "MiB/s"),
                    ("server_peak_held_kib", "server buffers held, KiB"),
                    ("server_peak_sock_kib", "server socket memory, KiB"),
                    ("client_peak_sock_kib", "client socket memory, KiB"),
                ],
                "tail" => &[
                    ("small_p99_us", "small request p99, µs"),
                    ("small_p50_us", "small request p50, µs"),
                    ("small_p999_us", "small request p99.9, µs"),
                    ("mib_s", "the stream's MiB/s"),
                ],
                _ => &[
                    ("mib_s", "MiB/s"),
                    ("server_cpu_ms_gib", "server cpu ms/GiB"),
                    ("host_cpu_ms_gib", "host cpu ms/GiB"),
                ],
            };
            for (figure, name) in figures {
                out.push_str(&format!("**{name}**\n\n"));
                out.push_str(&figure_table(&index, leg, section, figure));
            }
            // the tail's ratios to a connection of its own, and the routes' to direct
            if section == "tail" {
                out.push_str(
                    "**p99 over the p99 on a connection of its own to the same executor**\n\n",
                );
                out.push_str(&ratio_table(
                    &index,
                    leg,
                    "tail",
                    "own-same",
                    "small_p99_us",
                    &["alone"],
                ));
            }
            if section == "route" {
                out.push_str("**MiB/s over direct**\n\n");
                out.push_str(&ratio_table(&index, leg, "route", "direct", "mib_s", &[]));
                out.push_str("**server cpu a GiB over direct**\n\n");
                out.push_str(&ratio_table(
                    &index,
                    leg,
                    "route",
                    "direct",
                    "server_cpu_ms_gib",
                    &[],
                ));
            }
        }
        // the handoff check, round by round
        let checks = index.cells(leg, "handoff");
        if !checks.is_empty() {
            out.push_str("### The handoff check\n\n");
            let rows: Vec<Vec<String>> = index
                .sides(leg, "handoff", "handoff check")
                .iter()
                .map(|side| {
                    let ok = index.rounds((leg, "handoff", "handoff check", side), "ok");
                    vec![
                        side.clone(),
                        format!(
                            "{} of {}",
                            ok.values().filter(|v| **v >= 1.0).count(),
                            ok.len()
                        ),
                        show(index.interval((leg, "handoff", "handoff check", side), "write_mib")),
                        show(index.interval((leg, "handoff", "handoff check", side), "read_mib")),
                    ]
                })
                .collect();
            out.push_str(&table(
                &[
                    "connection".into(),
                    "rounds handed, checked and kTLS after".into(),
                    "MiB written".into(),
                    "MiB read".into(),
                ],
                &rows,
            ));
        }
    }
    out.push_str(&triggers(&index));
    out
}

/// A table of every side's ratio to one side, a row a cell
///
/// # Arguments
///
/// * `index` - The figures
/// * `leg` - The leg
/// * `section` - The section
/// * `under` - The side every other is divided by
/// * `figure` - The figure
/// * `skip` - Cells left out
fn ratio_table(
    index: &Index,
    leg: &str,
    section: &str,
    under: &str,
    figure: &str,
    skip: &[&str],
) -> String {
    let cells: Vec<String> = index
        .cells(leg, section)
        .into_iter()
        .filter(|cell| !skip.iter().any(|s| cell.starts_with(s)))
        .collect();
    let Some(first) = cells.first() else {
        return String::new();
    };
    let sides: Vec<String> = index
        .sides(leg, section, first)
        .into_iter()
        .filter(|side| side != under)
        .collect();
    let mut head = vec!["cell".to_string()];
    head.extend(sides.iter().cloned());
    let rows: Vec<Vec<String>> = cells
        .iter()
        .map(|cell| {
            let mut row = vec![cell.clone()];
            for side in &sides {
                row.push(show_ratio(
                    index.ratio(leg, section, cell, side, under, figure),
                ));
            }
            row
        })
        .collect();
    table(&head, &rows)
}

/// Judge the three triggers and report the two statements
///
/// # Arguments
///
/// * `index` - The figures
fn triggers(index: &Index) -> String {
    let mut out = String::from("## The triggers\n\n");
    // T1: at 1 MiB, the shared connection's p99 over its own connection's, every round above the line
    let mut t1 = false;
    let mut rows = Vec::new();
    for leg in &index.legs {
        for cell in index.cells(leg, "tail") {
            if !cell.contains(" 1M ") {
                continue;
            }
            let shared = index.ratio(leg, "tail", &cell, "shared", "own-same", "small_p99_us");
            let fires = shared.is_some_and(|ratio| ratio.min > T1_LINE);
            t1 |= fires;
            let others: Vec<String> = index
                .sides(leg, "tail", &cell)
                .into_iter()
                .filter(|side| side.starts_with("shared-"))
                .map(|side| {
                    format!(
                        "{side} {}",
                        show_ratio(index.ratio(
                            leg,
                            "tail",
                            &cell,
                            &side,
                            "own-same",
                            "small_p99_us"
                        ))
                    )
                })
                .collect();
            rows.push(vec![
                leg.clone(),
                cell.clone(),
                show_ratio(shared),
                verdict(fires),
                others.join("; "),
            ]);
        }
    }
    out.push_str(&format!(
        "**T1. A shared connection hurts small queries**: {}\n\n",
        fires_or_not(t1)
    ));
    out.push_str(&table(
        &[
            "leg".into(),
            "cell".into(),
            "shared over own".into(),
            "".into(),
            "the shared arms".into(),
        ],
        &rows,
    ));
    // and the largest frame at which each shared arm holds the line, leg by leg: the largest
    // frame such that it and every smaller one hold, since a larger frame that holds by chance
    // after a smaller one failed says nothing about the frame
    for leg in &index.legs {
        let cells = index.cells(leg, "tail");
        let mut arms: Vec<String> = Vec::new();
        for side in cells.iter().flat_map(|cell| index.sides(leg, "tail", cell)) {
            if side.starts_with("shared") && !arms.contains(&side) {
                arms.push(side);
            }
        }
        if arms.is_empty() {
            continue;
        }
        let mut rows = Vec::new();
        for dir in ["read", "write"] {
            for tls in ["ktls", "plain"] {
                let mut row = vec![leg.clone(), format!("{dir} {tls}")];
                for arm in &arms {
                    let mut largest = "none".to_string();
                    for cell in cells
                        .iter()
                        .filter(|cell| cell.starts_with(dir) && cell.ends_with(tls))
                    {
                        let holds = index
                            .ratio(leg, "tail", cell, arm, "own-same", "small_p99_us")
                            .is_some_and(|ratio| ratio.max <= T1_LINE);
                        if !holds {
                            break;
                        }
                        largest = cell.split(' ').nth(1).unwrap_or("?").to_string();
                    }
                    row.push(largest);
                }
                rows.push(row);
            }
        }
        let mut head = vec!["leg".to_string(), "stream".to_string()];
        head.extend(arms.iter().map(|arm| format!("largest frame {arm} holds")));
        out.push_str(&table(&head, &rows));
    }
    // T2: the handoff works, and the hop costs little
    let mut handoff_failed = false;
    let mut handoff_rows = Vec::new();
    for leg in &index.legs {
        for side in index.sides(leg, "handoff", "handoff check") {
            let ok = index.rounds((leg, "handoff", "handoff check", &side), "ok");
            let failed = ok.values().any(|v| *v < 1.0);
            handoff_failed |= failed;
            handoff_rows.push(vec![
                leg.clone(),
                side.clone(),
                format!(
                    "{} of {}",
                    ok.values().filter(|v| **v >= 1.0).count(),
                    ok.len()
                ),
            ]);
        }
    }
    let mut t2b = false;
    let mut rows = Vec::new();
    for leg in &index.legs {
        for cell in index.cells(leg, "route") {
            if !cell.contains(" 1M ") {
                continue;
            }
            let rate = index.ratio(leg, "route", &cell, "hop", "direct", "mib_s");
            let cpu = index.ratio(leg, "route", &cell, "hop", "direct", "server_cpu_ms_gib");
            let fires =
                rate.is_some_and(|r| r.max < T2_RATE) || cpu.is_some_and(|c| c.min > T2_CPU);
            t2b |= fires;
            rows.push(vec![
                leg.clone(),
                cell.clone(),
                show_ratio(rate),
                show_ratio(cpu),
                show_ratio(index.ratio(leg, "route", &cell, "hop-copy", "direct", "mib_s")),
                show_ratio(index.ratio(leg, "route", &cell, "handoff", "direct", "mib_s")),
                verdict(fires),
            ]);
        }
    }
    out.push_str(&format!(
        "**T2. The hop**: (a) {}; (b) {}. T2 {}\n\n",
        if handoff_failed {
            "**a handoff failed**"
        } else {
            "every handoff worked"
        },
        if t2b {
            "**the hop is dear**"
        } else {
            "the hop is cheap"
        },
        fires_or_not(handoff_failed || t2b)
    ));
    out.push_str(&table(
        &[
            "leg".into(),
            "connection".into(),
            "rounds handed and checked".into(),
        ],
        &handoff_rows,
    ));
    out.push_str(&table(
        &[
            "leg".into(),
            "cell".into(),
            "hop MiB/s over direct".into(),
            "hop server cpu over direct".into(),
            "hop-copy MiB/s over direct".into(),
            "handoff MiB/s over direct".into(),
            "".into(),
        ],
        &rows,
    ));
    // T3: one kTLS connection against the device, on loopback
    let mut t3 = false;
    let mut rows = Vec::new();
    for leg in index.legs.iter().filter(|leg| index.loopback(leg)) {
        let host = index.hosts.get(leg).cloned().unwrap_or_default();
        let Some((write_rate, read_rate)) = device_rates(&host) else {
            continue;
        };
        for (cell, line) in [
            ("write 1M memory", write_rate),
            ("read 1M memory", read_rate),
        ] {
            let rate = index.interval((leg, "rate", cell, "ktls"), "mib_s");
            let fires = rate.is_some_and(|rate| rate.max < line);
            t3 |= fires;
            rows.push(vec![
                leg.clone(),
                cell.to_string(),
                show(rate),
                fmt(line),
                verdict(fires),
            ]);
        }
    }
    out.push_str(&format!(
        "**T3. kTLS bounds a stream below a device**: {}\n\n",
        fires_or_not(t3)
    ));
    out.push_str(&table(
        &[
            "leg".into(),
            "cell".into(),
            "kTLS MiB/s".into(),
            "device MiB/s (X6, fio)".into(),
            "".into(),
        ],
        &rows,
    ));
    // the statements made in advance
    out.push_str("**Statements made in advance.** The frame: the smallest within a tenth of the best kTLS rate to and from memory and of the least cpu (server and client) a GiB. The window: the smallest number of 1 MiB frames reaching nine tenths of the best kTLS rate to the file.\n\n");
    let mut rows = Vec::new();
    for leg in index.legs.iter().filter(|leg| index.loopback(leg)) {
        for dir in ["write", "read"] {
            let frames: Vec<(String, f64, f64)> = index
                .cells(leg, "rate")
                .into_iter()
                .filter(|cell| cell.starts_with(dir) && cell.ends_with("memory"))
                .filter_map(|cell| {
                    let rate = index
                        .interval((leg, "rate", &cell, "ktls"), "mib_s")?
                        .median;
                    let cpu = index
                        .interval((leg, "rate", &cell, "ktls"), "server_cpu_ms_gib")?
                        .median
                        + index
                            .interval((leg, "rate", &cell, "ktls"), "client_cpu_ms_gib")?
                            .median;
                    Some((cell.split(' ').nth(1)?.to_string(), rate, cpu))
                })
                .collect();
            let best = frames.iter().map(|f| f.1).fold(0.0, f64::max);
            let least = frames.iter().map(|f| f.2).fold(f64::MAX, f64::min);
            let frame = frames
                .iter()
                .find(|f| f.1 >= 0.9 * best && f.2 <= 1.1 * least)
                .map_or("none".to_string(), |f| f.0.clone());
            let windows: Vec<(String, f64)> = index
                .cells(leg, "window")
                .into_iter()
                .filter(|cell| cell.starts_with(&format!("{dir} 1M")))
                .filter_map(|cell| {
                    Some((
                        cell.split(' ').nth(2)?.to_string(),
                        index
                            .interval((leg, "window", &cell, "ktls"), "mib_s")?
                            .median,
                    ))
                })
                .collect();
            let top = windows.iter().map(|w| w.1).fold(0.0, f64::max);
            let window = windows
                .iter()
                .find(|w| w.1 >= 0.9 * top)
                .map_or("—".to_string(), |w| w.0.clone());
            let top = if windows.is_empty() {
                "—".to_string()
            } else {
                fmt(top)
            };
            rows.push(vec![
                leg.clone(),
                dir.to_string(),
                frame,
                fmt(best),
                window,
                top,
            ]);
        }
    }
    out.push_str(&table(
        &[
            "leg".into(),
            "stream".into(),
            "frame chosen".into(),
            "best MiB/s".into(),
            "window chosen".into(),
            "best MiB/s at 1M".into(),
        ],
        &rows,
    ));
    out
}

/// A cell's verdict
///
/// # Arguments
///
/// * `fires` - Whether the trigger fires on it
fn verdict(fires: bool) -> String {
    if fires {
        "**fires**".to_string()
    } else {
        "holds".to_string()
    }
}

/// A trigger's verdict in a sentence
///
/// # Arguments
///
/// * `fires` - Whether it fires
fn fires_or_not(fires: bool) -> &'static str {
    if fires {
        "**fires**"
    } else {
        "does not fire"
    }
}
