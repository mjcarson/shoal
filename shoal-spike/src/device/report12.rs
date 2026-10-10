//! X12's verdicts and its arithmetic, judged on the rounds merged
//!
//! The lines were agreed with the user before the harness ran (2026-10-09). A rebuild is held to
//! S15's hypothesis that the foreground's p99 stays under twice its own, a deep scrub to its 1.25
//! times; a line fires when a figure's whole interval over the rounds lies past it, and is at the
//! line when the interval straddles it, as X6's and X7's verdicts are judged.
//!
//! - R1: at the fastest pace whose foreground stays under 2×, a 16 TiB disk takes more than 48
//!   hours to rebuild onto one destination, or a 4 TiB SSD more than 12.
//! - R2: idle pacing inside 2× rebuilds more than 1.5 times faster than the best fixed budget
//!   inside it.
//! - S1: at the fastest pace whose foreground stays within 1.25×, a deep scrub of a 16 TiB disk
//!   takes more than 7 days, or of a 4 TiB SSD more than one.
//! - S2: idle pacing with no ceiling raises the foreground above 1.25× at every piece size.
//! - P1: the fold costs more than a quarter of the checksum, as a scrub does it, in the same pass.
//!
//! Reported beside them: the source role's rate inside 2×, Q17's break-even between rebuilding a
//! missed unit as its chunk and alone, X7's H4 again with the cache off and the journal on the SSD,
//! the rebuild across hosts against its link's bound, and the hours every rate means for devices of
//! 1, 4 and 16 TiB.

use super::net::LINK_MIB_S;
use super::report::{interval, ratio, show, state, Groups};
use super::stats::{fmt, Interval};
use super::table::Table;

/// A rebuild's line, a multiple of the foreground's own p99
const REBUILD_LINE: f64 = 2.0;

/// A deep scrub's line
const SCRUB_LINE: f64 = 1.25;

/// The rebuild cell every pace is judged in
const DEST: &str = "role=dest layout=4+2 piece=1M";

/// The deep scrub cell every pace is judged in
const DEEP: &str = "layout=4+2 piece=1M";

/// MiB in a TiB
const TIB: f64 = 1024.0 * 1024.0;

/// Hours to move a device of a size at a rate
///
/// # Arguments
///
/// * `tib` - The device, TiB
/// * `mib_s` - The rate, MiB a second
fn hours(tib: f64, mib_s: f64) -> f64 {
    if mib_s <= 0.0 {
        f64::INFINITY
    } else {
        tib * TIB / mib_s / 3600.0
    }
}

/// The interval of hours a rate's interval means, the slowest round giving the most hours
///
/// # Arguments
///
/// * `tib` - The device, TiB
/// * `rate` - The rate's interval, MiB a second
fn hours_of(tib: f64, rate: Interval) -> Interval {
    Interval { min: hours(tib, rate.max), median: hours(tib, rate.median), max: hours(tib, rate.min), rounds: rate.rounds }
}

/// The sides a cell holds in a measurement, in the order the groups keep them
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `measurement` - The measurement
/// * `cell` - The cell
fn sides(groups: &Groups, leg: &str, measurement: &str, cell: &str) -> Vec<String> {
    groups
        .keys()
        .filter(|(group_leg, group_measurement, group_cell, _)| group_leg == leg && group_measurement == measurement && group_cell == cell)
        .map(|(.., side)| side.clone())
        .collect()
}

/// A paced side against the foreground alone: its read and write p99, each a ratio taken round by
/// round, and whether both lie wholly under a line
#[derive(Clone)]
struct Judged {
    /// The side
    side: String,
    /// The background's rate
    rate: Option<Interval>,
    /// The read p99's ratio to the foreground alone
    read: Option<Interval>,
    /// The write p99's ratio
    write: Option<Interval>,
    /// Whether both lie wholly under the line
    within: bool,
}

/// Judge a side against the cell's foreground alone
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `measurement` - The measurement
/// * `cell` - The side's cell
/// * `side` - The side
/// * `base` - The cell whose `none` side is the foreground alone
/// * `rate_figure` - The background's rate figure
/// * `line` - The line
fn judge(groups: &Groups, leg: &str, measurement: &str, cell: &str, side: &str, base: &str, rate_figure: &str, line: f64) -> Judged {
    let read = ratio(groups, leg, measurement, (cell, side, "read_p99"), (base, "none", "read_p99"));
    let write = ratio(groups, leg, measurement, (cell, side, "write_p99"), (base, "none", "write_p99"));
    let within = read.is_some_and(|r| r.max < line) && write.is_some_and(|w| w.max < line);
    Judged { side: side.to_string(), rate: interval(groups, leg, measurement, cell, side, rate_figure), read, write, within }
}

/// The fastest of the sides that stayed within their line, by median
///
/// # Arguments
///
/// * `judged` - The sides
fn fastest(judged: &[Judged]) -> Option<&Judged> {
    judged
        .iter()
        .filter(|one| one.within && one.rate.is_some())
        .max_by(|a, b| a.rate.map_or(0.0, |r| r.median).total_cmp(&b.rate.map_or(0.0, |r| r.median)))
}

/// Whether a leg's device spins, by the figure every X12 side records
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn rotational(groups: &Groups, leg: &str) -> Option<bool> {
    groups
        .iter()
        .find(|((group_leg, measurement, ..), figures)| group_leg == leg && ["rebuild", "deep", "granularity", "rebuild-net"].contains(&measurement.as_str()) && figures.contains_key("rotational"))
        .and_then(|(_, figures)| figures.get("rotational")?.values().next().map(|value| *value > 0.5))
}

/// The added cost of a fold in the checksum's pass, as a share of the checksum's, round by round
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `fused` - The op that checksums and folds in one pass
fn added(groups: &Groups, leg: &str, fused: &str) -> Option<Interval> {
    // a cost is the inverse of a rate, so the fused pass's cost over the checksum's is the rates' ratio
    ratio(groups, leg, "codec", ("op=crc", "cpu", "gib_s"), (&format!("op={fused}"), "cpu", "gib_s")).map(|r| Interval {
        min: r.min - 1.0,
        median: r.median - 1.0,
        max: r.max - 1.0,
        rounds: r.rounds,
    })
}

/// P1 and the planted faults, from the cpu alone
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn p1(groups: &Groups, leg: &str) -> String {
    let at = |op: &str| interval(groups, leg, "codec", &format!("op={op}"), "cpu", "gib_s");
    let Some(crc) = at("crc") else { return String::new() };
    // as written: a summary a unit long, alone and in the checksum's pass
    let standalone = ratio(groups, leg, "codec", ("op=crc", "cpu", "gib_s"), ("op=fold", "cpu", "gib_s"));
    let fused = added(groups, leg, "crc+fold");
    let fires = fused.is_some_and(|f| f.min > 0.25);
    let straddles = fused.is_some_and(|f| f.max > 0.25);
    let mut table = Table::new(&["op", "GiB/s", "µs a chunk"]);
    for op in [
        "crc", "fold", "crc+fold", "fold-4k", "crc+fold-4k", "copy", "decode-21-data", "decode-21-parity", "decode-42-data", "decode-42-parity",
        "pipeline-copy", "pipeline-21", "pipeline-42", "summary-42", "summary-42-4k",
    ] {
        let cell = format!("op={op}");
        if interval(groups, leg, "codec", &cell, "cpu", "gib_s").is_none() {
            continue;
        }
        table.row(vec![op.into(), show(interval(groups, leg, "codec", &cell, "cpu", "gib_s")), show(interval(groups, leg, "codec", &cell, "cpu", "us_per_chunk"))]);
    }
    // the supplement: a summary a block long, judged by the same line
    let block = added(groups, leg, "crc+fold-4k").map_or_else(String::new, |block| {
        let standalone = ratio(groups, leg, "codec", ("op=crc", "cpu", "gib_s"), ("op=fold-4k", "cpu", "gib_s"));
        format!(
            " Folded to a block of 4 KiB, the supplement, it adds **{}** (standalone, {}), so P1 {} for a block-long summary.",
            block.show(),
            show(standalone),
            state(block.min > 0.25, block.max > 0.25),
        )
    });
    let detect = |figure: &str| show(interval(groups, leg, "codec", "op=detect", "cpu", figure));
    format!(
        "{}P1 as written {} on this leg: the fold of a unit-long summary, in the checksum's pass, adds **{}** of the checksum's cost (standalone, {} of it; the checksum runs at {} GiB/s).{block} The planted faults, checked in memory: found {}, missed {}, false alarms {}.\n\n",
        table.render(&format!("P1. A scrub's and a rebuild's cpu, {leg}"), "one pinned core, out of cache; fires when the fold's added cost lies wholly above 0.25 of the checksum's"),
        state(fires, straddles),
        show(fused),
        show(standalone),
        crc.show(),
        detect("planted_found"),
        detect("undetected"),
        detect("false_alarms"),
    )
}

/// R1 and R2: the rebuild as a destination at every pace
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `spins` - Whether the device spins
fn r1_r2(groups: &Groups, leg: &str, spins: bool) -> String {
    let names = sides(groups, leg, "rebuild", DEST);
    if names.is_empty() {
        return String::new();
    }
    let (tib, line_hours) = if spins { (16.0, 48.0) } else { (4.0, 12.0) };
    let judged: Vec<Judged> = names
        .iter()
        .filter(|side| *side != "none")
        .map(|side| judge(groups, leg, "rebuild", DEST, side, DEST, "rebuilt_mib_s", REBUILD_LINE))
        .collect();
    let mut table = Table::new(&["pace", "rebuilt MiB/s", "achieved", "read p99 ÷ alone", "write p99 ÷ alone", "under 2×", &format!("hours, {tib} TiB")]);
    for one in &judged {
        table.row(vec![
            one.side.clone(),
            show(one.rate),
            show(interval(groups, leg, "rebuild", DEST, &one.side, "achieved_ratio")),
            show(one.read),
            show(one.write),
            if one.within { "yes".into() } else { "no".into() },
            show(one.rate.map(|rate| hours_of(tib, rate))),
        ]);
    }
    // R1: the fastest pace inside the line, and the hours it means
    let best = fastest(&judged);
    let (r1_fires, r1_straddles, r1_text) = match best {
        Some(best) => {
            let h = hours_of(tib, best.rate.expect("a rate"));
            (h.min > line_hours, h.max > line_hours, format!("the fastest pace inside 2× is **{}** at {} MiB/s, so a {tib} TiB device takes {} hours onto one destination", best.side, show(best.rate), h.show()))
        }
        None => (true, true, "no pace kept the foreground under 2×".to_string()),
    };
    // R2: idle pacing against the best fixed budget, both inside the line
    let of = |prefix: &str| judged.iter().filter(|one| one.side.starts_with(prefix)).cloned().collect::<Vec<_>>();
    let best_fixed = fastest(&of("fixed-")).map(|one| one.side.clone());
    let best_idle = fastest(&of("idle")).map(|one| one.side.clone());
    let (r2_fires, r2_straddles, r2_text) = match (&best_idle, &best_fixed) {
        (Some(idle), Some(fixed)) => {
            let r = ratio(groups, leg, "rebuild", (DEST, idle, "rebuilt_mib_s"), (DEST, fixed, "rebuilt_mib_s"));
            (r.is_some_and(|r| r.min > 1.5), r.is_some_and(|r| r.max > 1.5), format!("**{idle}** rebuilds {} times as fast as **{fixed}**, the best fixed budget inside 2×", show(r)))
        }
        (Some(idle), None) => (true, true, format!("no fixed budget stayed inside 2× and **{idle}** did")),
        (None, _) => (false, false, "idle pacing did not stay inside 2×".to_string()),
    };
    format!(
        "{}R1 {} on this leg: {} (it fires above {line_hours} hours). R2 {} on this leg: {} (it fires above 1.5×).\n\n",
        table.render(&format!("R1 and R2. A rebuild's destination at every pace, {leg}"), "4+2 chunks of 4 MiB written whole; a ratio is to the foreground alone, round by round"),
        state(r1_fires, r1_straddles),
        r1_text,
        state(r2_fires, r2_straddles),
        r2_text,
    )
}

/// S1, S2 and the source role: the deep scrub at every pace
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `spins` - Whether the device spins
fn s1_s2(groups: &Groups, leg: &str, spins: bool) -> String {
    let names = sides(groups, leg, "deep", DEEP);
    if names.is_empty() {
        return String::new();
    }
    let (tib, line_days) = if spins { (16.0, 7.0) } else { (4.0, 1.0) };
    let paced: Vec<&String> = names.iter().filter(|side| *side != "none" && *side != "cpu").collect();
    let scrub: Vec<Judged> = paced.iter().map(|side| judge(groups, leg, "deep", DEEP, side, DEEP, "scrub_mib_s", SCRUB_LINE + 1e-12)).collect();
    let source: Vec<Judged> = paced.iter().map(|side| judge(groups, leg, "deep", DEEP, side, DEEP, "scrub_mib_s", REBUILD_LINE)).collect();
    let mut table = Table::new(&["pace", "scrub MiB/s", "achieved", "read p99 ÷ alone", "write p99 ÷ alone", "lag p99 ms", "within 1.25×", "under 2×", &format!("days, {tib} TiB")]);
    for (one, as_source) in scrub.iter().zip(&source) {
        table.row(vec![
            one.side.clone(),
            show(one.rate),
            show(interval(groups, leg, "deep", DEEP, &one.side, "achieved_ratio")),
            show(one.read),
            show(one.write),
            show(interval(groups, leg, "deep", DEEP, &one.side, "lag_p99").map(|lag| Interval { min: lag.min / 1e3, median: lag.median / 1e3, max: lag.max / 1e3, rounds: lag.rounds })),
            if one.within { "yes".into() } else { "no".into() },
            if as_source.within { "yes".into() } else { "no".into() },
            show(one.rate.map(|rate| { let h = hours_of(tib, rate); Interval { min: h.min / 24.0, median: h.median / 24.0, max: h.max / 24.0, rounds: h.rounds } })),
        ]);
    }
    // the cpu side, where there is one
    let cpu = judge(groups, leg, "deep", DEEP, "cpu", DEEP, "scrub_mib_s", SCRUB_LINE + 1e-12);
    let cpu_text = if cpu.rate.is_some() {
        format!(" The scrub's cpu alone, over stripes in memory at {} MiB/s, moved the read p99 {} and the write p99 {}.", show(cpu.rate), show(cpu.read), show(cpu.write))
    } else {
        String::new()
    };
    // S1: the fastest pace within 1.25×
    let (s1_fires, s1_straddles, s1_text) = match fastest(&scrub) {
        Some(best) => {
            let h = hours_of(tib, best.rate.expect("a rate"));
            let days = Interval { min: h.min / 24.0, median: h.median / 24.0, max: h.max / 24.0, rounds: h.rounds };
            (days.min > line_days, days.max > line_days, format!("the fastest pace within 1.25× is **{}** at {} MiB/s, so a deep scrub of {tib} TiB takes {} days", best.side, show(best.rate), days.show()))
        }
        None => (true, true, "no pace kept the foreground within 1.25×".to_string()),
    };
    // S2: idle pacing with no ceiling at every piece size
    let mut pieces = Table::new(&["piece", "scrub MiB/s", "read p99 ÷ alone", "write p99 ÷ alone", "within 1.25×"]);
    let mut every_above = true;
    let mut any = false;
    for piece in ["256K", "1M", "4M"] {
        let cell = format!("layout=4+2 piece={piece}");
        let one = judge(groups, leg, "deep", &cell, "idle", DEEP, "scrub_mib_s", SCRUB_LINE + 1e-12);
        if one.rate.is_none() {
            continue;
        }
        any = true;
        let above = one.read.is_some_and(|r| r.min > SCRUB_LINE) || one.write.is_some_and(|w| w.min > SCRUB_LINE);
        every_above &= above;
        pieces.row(vec![piece.into(), show(one.rate), show(one.read), show(one.write), if one.within { "yes".into() } else { "no".into() }]);
    }
    let s2_fires = any && every_above;
    // the source role: the fastest read pace under 2×
    let source_text = match fastest(&source) {
        Some(best) => format!("As a rebuild's source, the fastest pace under 2× is **{}** at {} MiB/s.", best.side, show(best.rate)),
        None => "As a rebuild's source, no pace stayed under 2×.".to_string(),
    };
    format!(
        "{}{}S1 {} on this leg: {} (it fires above {line_days} days). S2 {} on this leg: {}. {source_text}{cpu_text}\n\n",
        table.render(&format!("S1. A deep scrub at every pace, {leg}"), "every unit verified and folded, every 4+2 stripe's summaries checked; a ratio is to the foreground alone, round by round"),
        if any { pieces.render(&format!("S2. Idle pacing by the piece, {leg}"), "no ceiling; fires when every piece size lies wholly above 1.25× in the read or the write") } else { String::new() },
        state(s1_fires, s1_straddles),
        s1_text,
        if s2_fires { "**fires**" } else { "does not fire" },
        if s2_fires { "idle pacing alone does not protect the foreground at any piece; a ceiling below the device's rate is required" } else { "idle pacing keeps the foreground within 1.25× at some piece" },
    )
}

/// Q17: one missed unit rebuilt as its chunk and alone
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn q17(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&["layout", "chunk p50 ms", "unit p50 ms", "chunk ÷ unit", "chunk device KiB", "unit device KiB", "chunk flushes", "unit flushes", "wrong"]);
    let mut any = false;
    for layout in ["2+1", "4+2"] {
        let cell = format!("layout={layout}");
        let Some(chunk) = interval(groups, leg, "granularity", &cell, "chunk", "us_p50") else { continue };
        any = true;
        let unit = interval(groups, leg, "granularity", &cell, "unit", "us_p50");
        let ms = |i: Option<Interval>| i.map(|i| Interval { min: i.min / 1e3, median: i.median / 1e3, max: i.max / 1e3, rounds: i.rounds });
        let wrong = interval(groups, leg, "granularity", &cell, "chunk", "wrong").map_or(0.0, |w| w.max) + interval(groups, leg, "granularity", &cell, "unit", "wrong").map_or(0.0, |w| w.max);
        table.row(vec![
            layout.into(),
            show(ms(Some(chunk))),
            show(ms(unit)),
            show(ratio(groups, leg, "granularity", (&cell, "chunk", "us_p50"), (&cell, "unit", "us_p50"))),
            show(interval(groups, leg, "granularity", &cell, "chunk", "dev_kib")),
            show(interval(groups, leg, "granularity", &cell, "unit", "dev_kib")),
            show(interval(groups, leg, "granularity", &cell, "chunk", "flushes")),
            show(interval(groups, leg, "granularity", &cell, "unit", "flushes")),
            fmt(wrong),
        ]);
    }
    if !any {
        return String::new();
    }
    format!(
        "{}A record of whole chunks costs a chunk's rebuild for every chunk with a missed write; a record of units costs a unit's for every unit. The ratio is how many units of one chunk have to be missed before rebuilding the chunk is the cheaper.\n\n",
        table.render(&format!("Q17. One missed unit, rebuilt as its chunk and alone, {leg}"), "one at a time, nothing beside it; reported, not judged"),
    )
}

/// X7's H4 again, with the cache off and the journal on the SSD
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h4_again(groups: &Groups, leg: &str) -> String {
    let Some(write) = ratio(groups, leg, "deep", (DEEP, "fixed-10", "write_p99"), (DEEP, "none", "write_p99")) else { return String::new() };
    let read = ratio(groups, leg, "deep", (DEEP, "fixed-10", "read_p99"), (DEEP, "none", "read_p99"));
    format!(
        "H4 again, at 10 MiB/s with the write cache off and the journal on the SSD: the write's p99 {} its own ({}), the read's {}. X7 judged it with the journal on the disk.\n\n",
        if write.min > SCRUB_LINE { "lies wholly above 1.25×" } else if write.max > SCRUB_LINE { "straddles 1.25×" } else { "lies within 1.25×" },
        write.show(),
        show(read),
    )
}

/// The rebuild across hosts against its link's bound
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn across(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&["layout", "rebuilt MiB/s", "received MiB/s", "bound MiB/s", "rebuilt ÷ bound", "read p99 ÷ alone", "write p99 ÷ alone", "mismatches"]);
    let mut any = false;
    for layout in ["copy", "2+1", "4+2"] {
        let Some(rate) = interval(groups, leg, "rebuild-net", "net", layout, "rebuilt_mib_s") else { continue };
        any = true;
        let bound = interval(groups, leg, "rebuild-net", "net", layout, "bound_mib_s").map_or(0.0, |b| b.median);
        table.row(vec![
            layout.into(),
            rate.show(),
            show(interval(groups, leg, "rebuild-net", "net", layout, "net_mib_s")),
            fmt(bound),
            fmt(rate.median / bound.max(1e-9)),
            show(ratio(groups, leg, "rebuild-net", ("net", layout, "read_p99"), ("net", "none", "read_p99"))),
            show(ratio(groups, leg, "rebuild-net", ("net", layout, "write_p99"), ("net", "none", "write_p99"))),
            show(interval(groups, leg, "rebuild-net", "net", layout, "mismatches")),
        ]);
    }
    if !any {
        return String::new();
    }
    table.render(&format!("A rebuild across hosts, {leg}"), &format!("survivors over 1 GbE, plain TCP; the bound is {LINK_MIB_S} MiB/s ÷ k"))
}

/// The hours every rate means
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn arithmetic(groups: &Groups, leg: &str) -> String {
    let mut rows: Vec<(String, Interval)> = Vec::new();
    // the destination: the fastest pace inside 2×, and no bound
    let judged: Vec<Judged> = sides(groups, leg, "rebuild", DEST)
        .iter()
        .filter(|side| *side != "none")
        .map(|side| judge(groups, leg, "rebuild", DEST, side, DEST, "rebuilt_mib_s", REBUILD_LINE))
        .collect();
    if let Some(best) = fastest(&judged) {
        rows.push((format!("rebuild, destination, {} (fastest under 2×)", best.side), best.rate.expect("a rate")));
    }
    if let Some(rate) = interval(groups, leg, "rebuild", DEST, "unbounded", "rebuilt_mib_s") {
        rows.push(("rebuild, destination, no bound".into(), rate));
    }
    for layout in ["copy", "2+1", "4+2"] {
        let cell = format!("role=local layout={layout} piece=1M");
        for pace in ["idle", "unbounded"] {
            if let Some(rate) = interval(groups, leg, "rebuild", &cell, pace, "rebuilt_mib_s") {
                rows.push((format!("rebuild on one device, {layout}, {pace}"), rate));
            }
        }
    }
    for (layout, k) in [("copy", 1.0), ("2+1", 2.0), ("4+2", 4.0)] {
        let bound = LINK_MIB_S / k;
        rows.push((format!("rebuild across 1 GbE, {layout}, the bound"), Interval { min: bound, median: bound, max: bound, rounds: 1 }));
        if let Some(rate) = interval(groups, leg, "rebuild-net", "net", layout, "rebuilt_mib_s") {
            rows.push((format!("rebuild across 1 GbE, {layout}, measured"), rate));
        }
    }
    // the scrub: the fastest pace within 1.25×, under 2× as a source, idle and no bound
    let paced: Vec<String> = sides(groups, leg, "deep", DEEP).into_iter().filter(|side| side != "none" && side != "cpu").collect();
    let scrub: Vec<Judged> = paced.iter().map(|side| judge(groups, leg, "deep", DEEP, side, DEEP, "scrub_mib_s", SCRUB_LINE + 1e-12)).collect();
    if let Some(best) = fastest(&scrub) {
        rows.push((format!("deep scrub, {} (fastest within 1.25×)", best.side), best.rate.expect("a rate")));
    }
    let source: Vec<Judged> = paced.iter().map(|side| judge(groups, leg, "deep", DEEP, side, DEEP, "scrub_mib_s", REBUILD_LINE)).collect();
    if let Some(best) = fastest(&source) {
        rows.push((format!("a rebuild's source, {} (fastest under 2×)", best.side), best.rate.expect("a rate")));
    }
    for pace in ["idle", "unbounded"] {
        if let Some(rate) = interval(groups, leg, "deep", DEEP, pace, "scrub_mib_s") {
            rows.push((format!("deep scrub, {pace}"), rate));
        }
    }
    if rows.iter().all(|(name, _)| name.contains("the bound")) {
        return String::new();
    }
    let mut table = Table::new(&["rate", "MiB/s", "hours, 1 TiB", "hours, 4 TiB", "hours, 16 TiB", "days, 16 TiB", "devices sharing a 16 TiB rebuild to finish in 24 h"]);
    for (name, rate) in rows {
        let h16 = hours(16.0, rate.median);
        table.row(vec![
            name,
            rate.show(),
            fmt(hours(1.0, rate.median)),
            fmt(hours(4.0, rate.median)),
            fmt(h16),
            fmt(h16 / 24.0),
            fmt((h16 / 24.0).ceil()),
        ]);
    }
    table.render(&format!("The arithmetic, {leg}"), "at each rate's median; a rebuild shared by several destinations runs at the sum of their rates, while the network allows")
}

/// Every X12 verdict and table of a leg
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
#[must_use]
pub fn report(groups: &Groups, leg: &str) -> String {
    let mut out = p1(groups, leg);
    if let Some(spins) = rotational(groups, leg) {
        out.push_str(&r1_r2(groups, leg, spins));
        out.push_str(&s1_s2(groups, leg, spins));
        if spins {
            out.push_str(&h4_again(groups, leg));
        }
    }
    out.push_str(&q17(groups, leg));
    out.push_str(&across(groups, leg));
    out.push_str(&arithmetic(groups, leg));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rate's hours, and the interval of hours its interval means
    #[test]
    fn hours_arithmetic() {
        // 16 TiB at 100 MiB/s is 46.6 hours
        assert!((hours(16.0, 100.0) - 46.603).abs() < 0.01);
        let h = hours_of(16.0, Interval { min: 50.0, median: 100.0, max: 200.0, rounds: 4 });
        assert!(h.min < h.median && h.median < h.max);
        assert!((h.max - hours(16.0, 50.0)).abs() < 1e-9);
    }
}
