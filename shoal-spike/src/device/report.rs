//! Rounds merged into intervals, and the triggers X6 and X7 named before they ran judged on them
//!
//! A figure is read across rounds as `median [min–max]`. The lab's rule is that a difference
//! counts only when two intervals are disjoint, and a ratio taken in each round is judged by
//! where its whole interval lies. X6's four triggers (T1 to T4) and X7's five (H1 to H5) are the
//! plans', set before the harnesses ran, and are judged here and nowhere else, so the pages quote
//! what this prints. A threshold fires when a figure's whole interval lies past it, and is said to
//! be at the line when the interval straddles it.

use std::collections::BTreeMap;

use super::record::Record;
use super::stats::Interval;
use super::table::Table;

/// Records grouped by leg, measurement, cell and side, each figure a value a round
type Groups = BTreeMap<(String, String, String, String), BTreeMap<String, BTreeMap<u32, f64>>>;

/// Group records, leaving out quick ones unless asked for them
///
/// # Arguments
///
/// * `records` - Every record
/// * `quick` - Whether quick records count, which only a check of this report wants
fn group(records: &[Record], quick: bool) -> Groups {
    let mut groups: Groups = BTreeMap::new();
    for record in records.iter().filter(|record| quick || !record.quick) {
        let key = (
            record.leg.clone(),
            record.measurement.clone(),
            record.cell.clone(),
            record.side.clone(),
        );
        let figures = groups.entry(key).or_default();
        for (name, value) in &record.metrics {
            figures.entry(name.clone()).or_default().insert(record.round, *value);
        }
    }
    groups
}

/// The interval of one figure of one side
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `measurement` - The measurement
/// * `cell` - The cell
/// * `side` - The side
/// * `figure` - The figure
fn interval(groups: &Groups, leg: &str, measurement: &str, cell: &str, side: &str, figure: &str) -> Option<Interval> {
    let values: Vec<f64> = groups
        .get(&(leg.into(), measurement.into(), cell.into(), side.into()))?
        .get(figure)?
        .values()
        .copied()
        .collect();
    Interval::of(&values)
}

/// The interval of a ratio of two sides' figures, taken round by round
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `measurement` - The measurement
/// * `top` - The numerator's cell, side and figure
/// * `bottom` - The denominator's cell, side and figure
fn ratio(
    groups: &Groups,
    leg: &str,
    measurement: &str,
    top: (&str, &str, &str),
    bottom: (&str, &str, &str),
) -> Option<Interval> {
    let rounds = |(cell, side, figure): (&str, &str, &str)| {
        groups
            .get(&(leg.to_string(), measurement.to_string(), cell.to_string(), side.to_string()))
            .and_then(|figures| figures.get(figure))
            .cloned()
    };
    let (top, bottom) = (rounds(top)?, rounds(bottom)?);
    // paired by round, so a slow round slows both sides of its ratio
    let values: Vec<f64> = top
        .iter()
        .filter_map(|(round, value)| bottom.get(round).filter(|b| **b > 0.0).map(|b| value / b))
        .collect();
    Interval::of(&values)
}

/// Show an interval, or a dash if there is none
///
/// # Arguments
///
/// * `interval` - The interval
fn show(interval: Option<Interval>) -> String {
    interval.map_or_else(|| "-".to_string(), |interval| interval.show())
}

/// The figures each measurement's merged table shows
///
/// # Arguments
///
/// * `measurement` - The measurement
fn figures(measurement: &str) -> &'static [&'static str] {
    match measurement {
        "chunk" | "chunk-recycle" => &["chunks_s", "mib_s", "cycle_p50", "cycle_tail", "sync_p50", "rename_p50", "dir_sync_p50", "dev_kib", "flushes", "cpu_us"],
        "journal" => &["records_s", "syncs_s", "per_sync_mean", "lat_p50", "lat_p99", "dev_kib", "flushes", "cpu_us"],
        "partial" => &["stage_p50", "apply_p50", "apply_sync_p50", "apply_sync_p99", "clone_p50", "release_p50", "total_p50", "total_p99", "dev_kib", "dev_ratio", "flushes"],
        "remove" => &["us_per_chunk", "unlink_p50", "dir_sync_ms", "drain_ms", "dev_kib", "discard_kib"],
        "listing" => &["chunks", "capped", "build_s", "cold_s", "cold_us", "cold_read_kib", "warm_us", "warm_written_kib", "hours_1m", "hours_4m"],
        "read" => &["open_p50", "read_p50", "total_p50", "total_p99", "total_p999", "dev_read_kib", "span_gib"],
        "frag" => &["extents_0", "extents_10pc", "extents_end", "shared_end", "seq_mib_s", "seq_ratio", "cold_p50", "cold_ratio", "writes_s"],
        "slices" => &["mib_s", "ops_s", "p50", "p99", "busy_max", "run_max", "projected_max", "sustainable_mib_s", "dev_write_mib_s", "flushes_s"],
        "probes" => &["overwrite_sync_p50", "overwrite_sync_p99", "clean_sync_p50", "rename_dir_sync_p50", "hop_p50"],
        "seq" => &["mib_s", "ops_s", "p50", "p99", "busy", "merges", "span_gib"],
        "sync" => &["syncs_s", "lat_p50", "lat_p99", "lat_max", "flushes_per_sync", "dev_kib", "busy"],
        "contend" => &["offered_s", "applies_s", "behind", "batch_p50", "batch_p99", "read_p50", "read_p99", "read_max", "stage_disk_p50", "stage_disk_p99", "stage_ssd_p50", "stage_ssd_p99", "busy", "span_gib"],
        "scrub" => &["scrub_mib_s", "write_p50", "write_p99", "write_max", "read_p50", "read_p99", "busy", "span_gib"],
        "shared" => &["ssd_read_p50", "ssd_read_p99", "ssd_read_p999", "ssd_stage_p50", "ssd_stage_p99", "ssd_exec_busy", "disk_exec_busy", "disk_applies_s", "disk_reads_s", "disk_chunks_s", "rename_p50", "dir_sync_p50", "disk_exec_us_per_op", "disk_busy"],
        _ => &[],
    }
}

/// T1: a file a chunk against layout B, at six in flight
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn t1(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&["size", "best file-a-chunk side", "its chunks/s", "B-batch chunks/s", "A÷B by round", "below 0.5"]);
    let mut floor: Option<String> = None;
    let mut any = false;
    for size in ["64K", "256K", "1M", "4M", "16M", "64M"] {
        let cell = format!("size={size} writers=6");
        // the faster of the two batched file-a-chunk sides, by median
        let best = ["A-batch", "A+-batch"]
            .into_iter()
            .filter_map(|side| interval(groups, leg, "chunk", &cell, side, "chunks_s").map(|i| (side, i)))
            .max_by(|a, b| a.1.median.total_cmp(&b.1.median));
        let (Some((side, a)), Some(b)) = (best, interval(groups, leg, "chunk", &cell, "B-batch", "chunks_s")) else {
            continue;
        };
        any = true;
        let r = ratio(groups, leg, "chunk", (&cell, side, "chunks_s"), (&cell, "B-batch", "chunks_s"));
        let fires = r.is_some_and(|r| r.max < 0.5);
        // the floor is the smallest size from which no larger size fires
        if fires {
            floor = None;
        } else if floor.is_none() {
            floor = Some(size.to_string());
        }
        table.row(vec![size.into(), side.into(), a.show(), b.show(), show(r), if fires { "**yes**".into() } else { "no".into() }]);
    }
    if !any {
        return String::new();
    }
    let floor = floor.unwrap_or_else(|| "none measured".to_string());
    let above = !matches!(floor.as_str(), "64K" | "256K" | "1M");
    format!(
        "{}Floor under a file a chunk: **{floor}**. T1 {} on this leg (it fires if the floor is above 1 MiB).\n\n{}",
        table.render(&format!("T1. A file a chunk, {leg}"), "six in flight; the ratio is taken round by round"),
        if above { "**fires**" } else { "does not fire" },
        recycled(groups, leg)
    )
}

/// The recycling supplement: a file a chunk taken from files written ahead, against a fresh
/// file and a slot of a shared file measured beside it in the same run
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn recycled(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&["size", "R-batch chunks/s", "A+-batch chunks/s", "B-batch chunks/s", "R÷B", "A+÷B", "R below 0.5"]);
    let mut any = false;
    let mut floor: Option<String> = None;
    for size in ["64K", "256K", "1M", "4M", "16M", "64M"] {
        let cell = format!("size={size} writers=6");
        let at = |side: &str| interval(groups, leg, "chunk-recycle", &cell, side, "chunks_s");
        let Some(b) = at("B-batch") else { continue };
        any = true;
        let r = ratio(groups, leg, "chunk-recycle", (&cell, "R-batch", "chunks_s"), (&cell, "B-batch", "chunks_s"));
        let a = ratio(groups, leg, "chunk-recycle", (&cell, "A+-batch", "chunks_s"), (&cell, "B-batch", "chunks_s"));
        let fires = r.is_some_and(|r| r.max < 0.5);
        if fires {
            floor = None;
        } else if floor.is_none() {
            floor = Some(size.to_string());
        }
        table.row(vec![size.into(), show(at("R-batch")), show(at("A+-batch")), b.show(), show(r), show(a), if fires { "**yes**".into() } else { "no".into() }]);
    }
    if !any {
        return String::new();
    }
    format!(
        "{}Floor under a recycled file a chunk: **{}**.\n\n",
        table.render(&format!("T1, recycled files, {leg}"), "the supplement: all three sides measured together, six in flight"),
        floor.unwrap_or_else(|| "none measured".to_string())
    )
}

/// T2: one core against several, for reads of 64 KiB and whole chunks of 1 and 4 MiB
///
/// It fires when one slice reaches less than 0.7× the best count, the gain survives the control
/// that holds the total depth fixed, and a read's single slice gains nothing from more depth.
/// 4 KiB reads are reported beside them and judged the same way, as a statement about a unit
/// that small, not as the trigger.
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn t2(groups: &Groups, leg: &str) -> String {
    let counts = [1, 2, 4, 8];
    let mut table = Table::new(&["workload", "slices", "sustainable MiB/s", "p99 µs", "runtime + checksum"]);
    // each workload: whether one slice is short, the count offered, the best count
    let mut verdicts: BTreeMap<&str, (bool, usize, usize)> = BTreeMap::new();
    for workload in ["r64", "r64-T", "r4", "r4-T", "w1M", "w1M-T", "w4M", "w4M-T"] {
        let mut by_count = Vec::new();
        for slices in counts {
            let cell = format!("{workload} slices={slices}");
            if let Some(rate) = interval(groups, leg, "slices", &cell, workload, "sustainable_mib_s") {
                let p99 = interval(groups, leg, "slices", &cell, workload, "p99");
                let busy = interval(groups, leg, "slices", &cell, workload, "projected_max");
                table.row(vec![workload.into(), slices.to_string(), rate.show(), show(p99), show(busy)]);
                by_count.push((slices, rate, p99));
            }
        }
        let Some(best) = by_count.iter().max_by(|a, b| a.1.median.total_cmp(&b.1.median)).cloned() else {
            continue;
        };
        let one = &by_count[0];
        let short = one.0 == 1 && one.1.max < 0.7 * best.1.min;
        // the smallest count close to the best in rate and in tail
        let offered = by_count
            .iter()
            .find(|(_, rate, p99)| {
                rate.median >= 0.9 * best.1.median
                    && match (p99, best.2) {
                        (Some(p99), Some(best_p99)) => p99.median <= 1.25 * best_p99.median,
                        _ => true,
                    }
            })
            .map_or(best.0, |found| found.0);
        verdicts.insert(workload, (short, offered, best.0));
    }
    if verdicts.is_empty() {
        return String::new();
    }
    // whether one slice's reads stop gaining with depth: 128 within 10% of 32
    let plateau = |name: &str| {
        let at = |depth: usize| interval(groups, leg, "slices", &format!("{name} depth={depth}"), name, "mib_s");
        match (at(32), at(128)) {
            (Some(at32), Some(at128)) => at128.median <= 1.1 * at32.median,
            _ => false,
        }
    };
    let mut out = table.render(&format!("T2. One core against several, {leg}"), "sustainable: the rate a slice could keep with the checksum's CPU added to its runtime");
    let mut fires = false;
    for (workload, control, depth) in [("r64", "r64-T", Some("depth-r64")), ("w1M", "w1M-T", None), ("w4M", "w4M-T", None), ("r4", "r4-T", Some("depth-r4"))] {
        let Some((short, offered, best)) = verdicts.get(workload).copied() else { continue };
        let control_short = verdicts.get(control).is_some_and(|(short, ..)| *short);
        let flat = depth.is_none_or(plateau);
        let holds = short && control_short && flat;
        if workload != "r4" {
            fires |= holds;
        }
        out.push_str(&format!(
            "- {workload}: one slice {} below 0.7× the best ({best} slices); at a fixed total depth it {}; {}; the number offered is **{offered}**{}\n",
            if short { "is" } else { "is not" },
            if control_short { "is too" } else { "is not" },
            match depth {
                Some(_) if flat => "one slice gains nothing past depth 32",
                Some(_) => "one slice still gains with depth",
                None => "no depth sweep for writes",
            },
            if holds { " — **one core falls short**" } else { "" }
        ));
    }
    out.push_str(&format!("\nT2 {} on this leg (4 KiB reads are reported, not judged).\n\n", if fires { "**fires**" } else { "does not fire" }));
    out
}

/// T3: the clone as the way to apply
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn t3(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "partial", "size=64K inflight=6", "C-punch", "total_p50").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["condition", "cell", "clone", "journal", "verdict"]);
    let mut pass = true;
    // (a) bytes, at 256 KiB and 1 MiB
    for size in ["256K", "1M"] {
        for inflight in [1, 6] {
            let cell = format!("size={size} inflight={inflight}");
            let r = ratio(groups, leg, "partial", (&cell, "C-punch", "dev_kib"), (&cell, "J", "dev_kib"));
            let ok = r.is_some_and(|r| r.max <= 0.6);
            pass &= ok;
            table.row(vec!["(a) device bytes ≤ 0.6×".into(), cell, show(r), "1".into(), verdict(ok)]);
        }
    }
    // (b) the apply's sync, and its flushes
    for size in ["4K", "64K", "1M"] {
        let cell = format!("size={size} inflight=6");
        for figure in ["apply_sync_p50", "apply_sync_p99"] {
            let (c, j) = (
                interval(groups, leg, "partial", &cell, "C-punch", figure),
                interval(groups, leg, "partial", &cell, "J", figure),
            );
            let ok = matches!((c, j), (Some(c), Some(j)) if !c.above(&j));
            pass &= ok;
            table.row(vec![format!("(b) {figure} no worse"), cell.clone(), show(c), show(j), verdict(ok)]);
        }
        let (c, j) = (
            interval(groups, leg, "partial", &cell, "C-punch", "flushes"),
            interval(groups, leg, "partial", &cell, "J", "flushes"),
        );
        let ok = matches!((c, j), (Some(c), Some(j)) if (c.median - j.median).abs() < 0.25);
        pass &= ok;
        table.row(vec!["(b) flushes equal".into(), cell, show(c), show(j), verdict(ok)]);
    }
    // (c) the stage, and stage and apply together, at six in flight
    for size in ["4K", "16K", "64K", "256K", "1M"] {
        let cell = format!("size={size} inflight=6");
        for figure in ["stage_p50", "total_p50"] {
            let (c, j) = (
                interval(groups, leg, "partial", &cell, "C-punch", figure),
                interval(groups, leg, "partial", &cell, "J", figure),
            );
            let ok = matches!((c, j), (Some(c), Some(j)) if !c.above(&j));
            pass &= ok;
            table.row(vec![format!("(c) {figure} no worse"), cell.clone(), show(c), show(j), verdict(ok)]);
        }
    }
    // (d) reading a chunk after a thousand clones
    let (seq, cold) = (
        interval(groups, leg, "frag", "chunk=64M", "clone", "seq_ratio"),
        interval(groups, leg, "frag", "chunk=64M", "clone", "cold_ratio"),
    );
    let ok = seq.is_some_and(|seq| seq.min >= 0.8) && cold.is_some_and(|cold| cold.max <= 1.2);
    pass &= ok;
    table.row(vec!["(d) after 1,000 clones: sequential ≥ 0.8×, cold unit ≤ 1.2× fresh".into(), "chunk=64M".into(), format!("{} · {}", show(seq), show(cold)), "1 · 1".into(), verdict(ok)]);
    // (e) the journal on chunks that have been cloned into
    for size in ["4K", "64K", "1M"] {
        let cell = format!("size={size} inflight=6");
        let (cloned, plain) = (
            interval(groups, leg, "partial", &cell, "J'", "total_p50"),
            interval(groups, leg, "partial", &cell, "J", "total_p50"),
        );
        let ok = matches!((cloned, plain), (Some(c), Some(j)) if !c.above(&j));
        pass &= ok;
        table.row(vec!["(e) J′ no worse than J".into(), cell, show(cloned), show(plain), verdict(ok)]);
    }
    format!(
        "{}T3 {} on this leg: the clone is {} the way to apply.\n\n",
        table.render(&format!("T3. The clone as the apply, {leg}"), "C-punch against J; ratios round by round"),
        if pass { "**fires**" } else { "does not fire" },
        if pass { "" } else { "not" }
    )
}

/// A verdict's cell
///
/// # Arguments
///
/// * `ok` - Whether the condition held
fn verdict(ok: bool) -> String {
    if ok { "holds".into() } else { "**fails**".into() }
}

/// T4: whether a light scrub needs an index of its own
///
/// T4's words name the cheapest walk that yields each chunk's name, length and label, for a deep
/// placement group and a wide one alike. A shape fires when listing its names alone is too slow,
/// which no place for the label mends, or when its header walk is too slow and no walk reading
/// the label from an extended attribute passes. If the only shapes that fail are header walks an
/// attribute walk passes for, the label moves out of the header and no index is needed. X6's
/// report judged the deep shape alone; X7 judged both, as the words ask.
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn t4(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "listing", "deep-1M", "header-qd32", "cold_s").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["population", "walk", "cold s", "cold µs/chunk", "µs/chunk at 1M ÷ at 100K", "verdict"]);
    let (mut fires, mut label_moves) = (false, false);
    for shape in ["deep", "wide"] {
        // each walk of this shape: whether it was measured and whether it was too slow
        let mut slow_by_walk: BTreeMap<&str, bool> = BTreeMap::new();
        for walk in ["names", "statx-ino", "xattr", "header-qd32"] {
            let (big, small) = (format!("{shape}-1M"), format!("{shape}-100K"));
            let Some(cold) = interval(groups, leg, "listing", &big, walk, "cold_s") else { continue };
            // a walk the cap stopped is judged too slow, and its cost a chunk is over what it reached
            let capped = interval(groups, leg, "listing", &big, walk, "capped").is_some_and(|capped| capped.max > 0.0);
            let walked = interval(groups, leg, "listing", &big, walk, "chunks");
            let per = interval(groups, leg, "listing", &big, walk, "cold_us").filter(|_| walked.is_some_and(|walked| walked.min > 1.0));
            let scale = ratio(groups, leg, "listing", (&big, walk, "cold_us"), (&small, walk, "cold_us")).filter(|_| !capped);
            let slow = capped || cold.min > 60.0 || scale.is_some_and(|scale| scale.min > 2.0);
            slow_by_walk.insert(walk, slow);
            let cold_shown = if capped {
                format!("≥ {} (stopped at the cap after {} chunks)", cold.show(), show(walked))
            } else {
                cold.show()
            };
            table.row(vec![big, walk.into(), cold_shown, show(per), show(scale), if slow { "**too slow**".into() } else { "fine".into() }]);
        }
        // the shape's verdict: its names alone, or its label's walks
        let names_slow = slow_by_walk.get("names").copied().unwrap_or(false);
        let header_slow = slow_by_walk.get("header-qd32").copied().unwrap_or(false);
        let xattr_passes = slow_by_walk.get("xattr").is_some_and(|slow| !slow);
        if names_slow || (header_slow && !xattr_passes) {
            fires = true;
        } else if header_slow {
            label_moves = true;
        }
    }
    let note = if fires {
        "a light scrub needs an index of its own, or more time than T4 allows"
    } else if label_moves {
        "the header walk fails and the attribute walk passes: the label moves, an index is not needed"
    } else {
        "a light scrub can walk the directories"
    };
    format!(
        "{}T4 {} on this leg: {note}.\n\n",
        table.render(&format!("T4. A light scrub's walk, {leg}"), "fires above 60 s cold for a million chunks, or above 2× the cost a chunk at 100K; a deep and a wide group each judged"),
        if fires { "**fires**" } else { "does not fire" }
    )
}

/// The journal's two statements made in advance: whether writing ahead and allocating ahead
/// earn their keep
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn journal(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&["record", "stagers", "appended ÷ written ahead", "allocated ÷ written ahead"]);
    let mut any = false;
    let mut within = true;
    for record in ["4K", "16K", "64K"] {
        for stagers in [6, 64] {
            let cell = format!("record={record} stagers={stagers}");
            let appended = ratio(groups, leg, "journal", (&cell, "appended/each", "records_s"), (&cell, "written-ahead/each", "records_s"));
            let allocated = ratio(groups, leg, "journal", (&cell, "allocated/each", "records_s"), (&cell, "written-ahead/each", "records_s"));
            if appended.is_none() {
                continue;
            }
            any = true;
            within &= appended.is_some_and(|r| r.min >= 0.9);
            table.row(vec![record.into(), stagers.to_string(), show(appended), show(allocated)]);
        }
    }
    if !any {
        return String::new();
    }
    format!(
        "{}Appended is {} within 10% of written ahead at 6 and 64 stagers on this leg.\n\n",
        table.render(&format!("The journal's files, {leg}"), "records a second, each stager writing its own, ratios round by round"),
        if within { "" } else { "not" }
    )
}

/// One figure of one side, round by round
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `measurement` - The measurement
/// * `cell` - The cell
/// * `side` - The side
/// * `figure` - The figure
fn rounds(groups: &Groups, leg: &str, measurement: &str, cell: &str, side: &str, figure: &str) -> Option<BTreeMap<u32, f64>> {
    groups
        .get(&(leg.to_string(), measurement.to_string(), cell.to_string(), side.to_string()))
        .and_then(|figures| figures.get(figure))
        .cloned()
}

/// A trigger's verdict in words: it fires when an interval lies wholly past its line, and is at
/// the line when the interval straddles it
///
/// # Arguments
///
/// * `fires` - Whether an interval lies wholly past the line
/// * `straddles` - Whether an interval reaches past it at all
fn state(fires: bool, straddles: bool) -> &'static str {
    if fires {
        "**fires**"
    } else if straddles {
        "is **at the line**, so does not fire,"
    } else {
        "does not fire"
    }
}

/// Where an interval lies against a line it fires above
///
/// # Arguments
///
/// * `interval` - The interval
/// * `line` - The line
fn against(interval: Option<Interval>, line: f64) -> &'static str {
    match interval {
        Some(interval) if interval.min > line => "**above**",
        Some(interval) if interval.max > line => "at the line",
        Some(_) => "below",
        None => "-",
    }
}

/// An interval in milliseconds, from one in microseconds
///
/// # Arguments
///
/// * `interval` - The interval, µs
fn ms(interval: Option<Interval>) -> Option<Interval> {
    interval.map(|interval| Interval {
        min: interval.min / 1e3,
        median: interval.median / 1e3,
        max: interval.max / 1e3,
        rounds: interval.rounds,
    })
}

/// H1: whether a small stage can be acknowledged from the disk, idle and while applies run
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h1(groups: &Groups, leg: &str) -> String {
    let idle = interval(groups, leg, "journal", "record=4K stagers=1", "written-ahead/each", "lat_p50");
    if idle.is_none() && interval(groups, leg, "contend", "batch=32", "idle", "stage_disk_p50").is_none() {
        return String::new();
    }
    let line = 20_000.0;
    let mut table = Table::new(&["where", "stage", "p50 ms", "p99 ms", "against 20 ms"]);
    let mut fires = false;
    let mut at_line = false;
    // idle: the journal measurement's one stager, on the disk, on the SSD, and with the cache off
    for (side, what) in [
        ("written-ahead/each", "disk, idle"),
        ("ssd-written-ahead/each", "SSD, idle"),
        ("written-ahead/each-wt", "disk, idle, write cache off"),
    ] {
        let p50 = interval(groups, leg, "journal", "record=4K stagers=1", side, "lat_p50");
        if p50.is_none() {
            continue;
        }
        let p99 = interval(groups, leg, "journal", "record=4K stagers=1", side, "lat_p99");
        if side == "written-ahead/each" {
            fires |= p50.is_some_and(|p50| p50.min > line);
            at_line |= p50.is_some_and(|p50| p50.max > line);
        }
        table.row(vec!["journal, 1 stager".into(), what.into(), show(ms(p50)), show(ms(p99)), against(p50, line).into()]);
    }
    // under applies: the contention measurement's stagers, beside each order of applies
    for batch in [32, 128] {
        let cell = format!("batch={batch}");
        for order in ["idle", "offset-qd1", "kernel", "arrival-qd1", "offset-ino"] {
            for (prefix, what) in [("stage_disk", "disk"), ("stage_ssd", "SSD")] {
                let p50 = interval(groups, leg, "contend", &cell, order, &format!("{prefix}_p50"));
                if p50.is_none() {
                    continue;
                }
                let p99 = interval(groups, leg, "contend", &cell, order, &format!("{prefix}_p99"));
                if prefix == "stage_disk" && batch == 32 && order == "offset-qd1" {
                    fires |= p50.is_some_and(|p50| p50.min > line);
                    at_line |= p50.is_some_and(|p50| p50.max > line);
                }
                table.row(vec![format!("applies {order}, batch {batch}"), what.into(), show(ms(p50)), show(ms(p99)), against(p50, line).into()]);
            }
        }
    }
    format!(
        "{}H1 {} on this leg: {}.\n\n",
        table.render(&format!("H1. A stage acknowledged from the disk, {leg}"), "a 4 KiB record and its group commit; fires when the disk's median at one stager lies wholly above 20 ms, idle or beside applies in offset order"),
        state(fires, at_line),
        if fires { "a rotational pool's journal is required on an SSD" } else { "a stage can be acknowledged from the disk itself" }
    )
}

/// H2: whether a disk's slice costs an SSD's slice on the same executor
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h2(groups: &Groups, leg: &str) -> String {
    let cell = "ssd+disk";
    if interval(groups, leg, "shared", cell, "ssd-alone", "ssd_read_p99").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["figure", "ssd-alone", "shared", "separate", "shared ÷ alone", "separate ÷ alone"]);
    let mut fires = false;
    let mut at_line = false;
    for figure in ["ssd_read_p50", "ssd_read_p99", "ssd_read_p999", "ssd_stage_p50", "ssd_stage_p99"] {
        let at = |side: &str| interval(groups, leg, "shared", cell, side, figure);
        let shared = ratio(groups, leg, "shared", (cell, "shared", figure), (cell, "ssd-alone", figure));
        let separate = ratio(groups, leg, "shared", (cell, "separate", figure), (cell, "ssd-alone", figure));
        // the trigger, as the plan set it: the SSD's read or stage p99 rising on the shared
        // executor, wholly, and not under the control
        if figure == "ssd_read_p99" || figure == "ssd_stage_p99" {
            // the control rises only if its whole interval lies past the line, the lab's rule for
            // any difference
            fires |= shared.is_some_and(|r| r.min > 1.25) && separate.is_none_or(|r| r.min <= 1.25);
            at_line |= shared.is_some_and(|r| r.max > 1.25);
        }
        table.row(vec![figure.into(), show(at("ssd-alone")), show(at("shared")), show(at("separate")), show(shared), show(separate)]);
    }
    let busy = interval(groups, leg, "shared", cell, "separate", "disk_exec_busy");
    let per_op = interval(groups, leg, "shared", cell, "separate", "disk_exec_us_per_op");
    let ops = ["disk_applies_s", "disk_reads_s", "disk_chunks_s"]
        .iter()
        .map(|figure| format!("{} {}", figure.trim_start_matches("disk_").trim_end_matches("_s"), show(interval(groups, leg, "shared", cell, "separate", figure))))
        .collect::<Vec<_>>()
        .join(", ");
    let disks = busy.map(|busy| Interval { min: 1.0 / busy.max, median: 1.0 / busy.median, max: 1.0 / busy.min, rounds: busy.rounds });
    format!(
        "{}H2 {} on this leg: {}.\n\nThe disk's slice alone on its executor (`separate`) kept its thread {} busy at {ops} a second, {} µs of the thread a disk operation: one core would drive about {} such disks, each as busy.\n\n",
        table.render(&format!("H2. One executor for an SSD's slice and a disk's, {leg}"), "µs; fires when the SSD's read or stage p99 rises above 1.25× on the shared executor, wholly, and the control's does not, wholly"),
        state(fires, at_line),
        if fires { "a rotational slice gets an executor of its own" } else { "a disk's slice may share an executor with an SSD's" },
        show(busy),
        show(per_op),
        show(disks)
    )
}

/// The counted window of a contention side, in seconds, as `contend` runs it
const CONTEND_WINDOW_S: f64 = 20.0;

/// Whether a contention side fell behind the applies it was offered, in most rounds
///
/// A batch is counted only if it ends inside the window, so a side that kept up can lose one
/// batch at the window's edges: a side is behind only when it did less than 0.95 of its offer
/// with that batch given back.
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
/// * `cell` - The cell
/// * `side` - The side
/// * `batch` - Applies in a batch
fn behind_offer(groups: &Groups, leg: &str, cell: &str, side: &str, batch: usize) -> bool {
    let (Some(done), Some(offered)) = (
        rounds(groups, leg, "contend", cell, side, "applies_s"),
        rounds(groups, leg, "contend", cell, side, "offered_s"),
    ) else {
        return false;
    };
    let edge = batch as f64 / CONTEND_WINDOW_S;
    let behind = done
        .iter()
        .filter(|(round, done)| offered.get(round).is_some_and(|offered| **done + edge < 0.95 * offered))
        .count();
    behind * 2 > done.len()
}

/// H3: whether applies in offset order keep a foreground read within one batch
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h3(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "contend", "batch=32", "offset-qd1", "read_p99").is_none() {
        return String::new();
    }
    let mut table = Table::new(&[
        "batch", "order", "applies/s offered", "applies/s done", "batch p50 ms", "read p99 ms", "idle read p99 ms",
        "read p99 ÷ (batch p50 + idle p99)", "verdict",
    ]);
    let mut fires = false;
    let mut unjudged = false;
    for batch in [32, 128] {
        let cell = format!("batch={batch}");
        let idle = rounds(groups, leg, "contend", &cell, "idle", "read_p99");
        for order in ["offset-qd1", "offset-ino", "arrival-qd1", "kernel"] {
            let (Some(read), Some(batch_p50), Some(idle)) = (
                rounds(groups, leg, "contend", &cell, order, "read_p99"),
                rounds(groups, leg, "contend", &cell, order, "batch_p50"),
                idle.clone(),
            ) else {
                continue;
            };
            // the bound S13 promises, round by round: one batch, then the read as it is when idle
            let values: Vec<f64> = read
                .iter()
                .filter_map(|(round, read)| {
                    let bound = batch_p50.get(round)? + idle.get(round)?;
                    (bound > 0.0).then(|| read / bound)
                })
                .collect();
            let over = Interval::of(&values);
            // a side that could not keep up with its offer never ran the load the trigger names; a
            // batch is counted only if it ends inside the window, so one batch is allowed for
            let behind = behind_offer(groups, leg, &cell, order, batch);
            let past = !behind && over.is_some_and(|over| over.min > 1.0);
            if order == "offset-qd1" {
                fires |= past;
                unjudged |= behind;
            }
            table.row(vec![
                batch.to_string(),
                order.into(),
                show(interval(groups, leg, "contend", &cell, order, "offered_s")),
                show(interval(groups, leg, "contend", &cell, order, "applies_s")),
                show(ms(interval(groups, leg, "contend", &cell, order, "batch_p50"))),
                show(ms(interval(groups, leg, "contend", &cell, order, "read_p99"))),
                show(ms(interval(groups, leg, "contend", &cell, "idle", "read_p99"))),
                show(over),
                if behind {
                    "fell behind its offer: not judged".into()
                } else if past {
                    "**waits past a batch**".into()
                } else if over.is_some_and(|over| over.max > 1.0) {
                    "at the line".into()
                } else {
                    "within".into()
                },
            ]);
        }
    }
    let verdict = match (fires, unjudged) {
        (true, _) => "**fires** on this leg: offset order does not bound a read's wait; the slice holds applies behind waiting reads",
        (false, true) => "is **not judged** on this leg at a batch where offset order one at a time fell behind half of what the disk takes with the batch in flight",
        (false, false) => "does not fire on this leg: offset order keeps a read within one batch",
    };
    format!(
        "{}H3 {verdict}.\n\n",
        table.render(&format!("H3. A read beside applies, {leg}"), "fires when, in offset order, the read's p99 lies wholly past one batch's median and an idle read's p99; a side that fell behind its offer is not judged"),
    )
}

/// H4: whether a scrub's byte budget leaves the foreground alone
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h4(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "scrub", "budget=0", "scrub", "write_p99").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["budget MiB/s", "scrub MiB/s", "write p99 ms", "write p99 ÷ no scrub", "read p99 ÷ no scrub", "within 1.25×"]);
    let mut fires = false;
    let mut at_line = false;
    let mut largest: Option<&str> = None;
    for budget in ["0", "10", "20", "40", "60", "unbounded"] {
        let cell = format!("budget={budget}");
        let Some(write) = interval(groups, leg, "scrub", &cell, "scrub", "write_p99") else { continue };
        let write_ratio = ratio(groups, leg, "scrub", (&cell, "scrub", "write_p99"), ("budget=0", "scrub", "write_p99"));
        let read_ratio = ratio(groups, leg, "scrub", (&cell, "scrub", "read_p99"), ("budget=0", "scrub", "read_p99"));
        let within = write_ratio.is_some_and(|r| r.max <= 1.25);
        if budget == "10" {
            fires = write_ratio.is_some_and(|r| r.min > 1.25);
            at_line = write_ratio.is_some_and(|r| r.max > 1.25);
        }
        if within && budget != "0" {
            largest = Some(budget);
        }
        table.row(vec![
            budget.into(),
            show(interval(groups, leg, "scrub", &cell, "scrub", "scrub_mib_s")),
            show(ms(Some(write))),
            show(write_ratio),
            show(read_ratio),
            if within { "yes".into() } else { "no".into() },
        ]);
    }
    format!(
        "{}H4 {} on this leg: {}. The largest budget whose write p99 stays wholly within 1.25× is **{}** MiB/s.\n\n",
        table.render(&format!("H4. A foreground under a scrub's budget, {leg}"), "a 4 KiB write staged and applied, at 10/s; fires when at 10 MiB/s its p99 lies wholly above 1.25× its p99 with no scrub"),
        state(fires, at_line),
        if fires { "a fixed budget cannot protect the foreground; the scrub adapts to it" } else { "a byte budget protects the foreground" },
        largest.unwrap_or("none")
    )
}

/// H5: whether small chunks cost a seek each to make and to find
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn h5(groups: &Groups, leg: &str) -> String {
    let mut out = String::new();
    // (a) the floor under a file a chunk taken from the pool, against a slot of a shared file
    let mut table = Table::new(&["size", "R-batch chunks/s", "B-batch chunks/s", "R÷B by round", "below 0.5"]);
    let mut floor: Option<&str> = None;
    let mut any = false;
    for size in ["64K", "256K", "1M", "4M", "16M", "64M"] {
        let cell = format!("size={size} writers=6");
        let Some(b) = interval(groups, leg, "chunk-recycle", &cell, "B-batch", "chunks_s") else { continue };
        any = true;
        let r = ratio(groups, leg, "chunk-recycle", (&cell, "R-batch", "chunks_s"), (&cell, "B-batch", "chunks_s"));
        let below = r.is_some_and(|r| r.max < 0.5);
        if below {
            floor = None;
        } else if floor.is_none() {
            floor = Some(size);
        }
        table.row(vec![size.into(), show(interval(groups, leg, "chunk-recycle", &cell, "R-batch", "chunks_s")), b.show(), show(r), if below { "**yes**".into() } else { "no".into() }]);
    }
    let floor_above = floor.is_none_or(|floor| !matches!(floor, "64K" | "256K" | "1M"));
    if any {
        out.push_str(&table.render(&format!("H5 (a). A file a chunk from the pool, {leg}"), "six in flight, one directory sync for six; the ratio is taken round by round"));
        out.push_str(&format!(
            "Floor under a file a chunk from the pool: **{}**; (a) {} (it fires if the floor is above X6's 1 MiB).\n\n",
            floor.unwrap_or("none measured"),
            if floor_above { "**fires**" } else { "does not fire" }
        ));
    }
    // (b) finding a chunk: a cold open and read against the same read with the file open
    let mut table = Table::new(&["unit", "cold p50 µs", "open p50 µs", "cold ÷ open by round", "above 1.5×"]);
    let mut find = false;
    let mut find_line = false;
    let mut found_any = false;
    for unit in ["4K", "64K", "256K", "1M"] {
        let cell = format!("unit={unit}");
        let Some(cold) = interval(groups, leg, "read", &cell, "cold", "total_p50") else { continue };
        found_any = true;
        let r = ratio(groups, leg, "read", (&cell, "cold", "total_p50"), (&cell, "open", "total_p50"));
        let above = r.is_some_and(|r| r.min > 1.5);
        if unit == "64K" {
            find = above;
            find_line = r.is_some_and(|r| r.max > 1.5);
        }
        table.row(vec![unit.into(), cold.show(), show(interval(groups, leg, "read", &cell, "open", "total_p50")), show(r), if above { "**yes**".into() } else { "no".into() }]);
    }
    if found_any {
        out.push_str(&table.render(&format!("H5 (b). Finding a chunk, {leg}"), "a cold open and read of header and unit, against a read of the unit with the file held open; queue depth 1"));
        out.push_str(&format!("(b) {} at 64 KiB (it fires above 1.5×).\n\n", state(find, find_line)));
    }
    if !any && !found_any {
        return String::new();
    }
    let fires = (any && floor_above) || find;
    out.push_str(&format!(
        "H5 {} on this leg: {}.\n\n",
        if fires { "**fires**" } else { "does not fire" },
        if fires { "small chunks cost a seek each; a rotational pool sets its chunk size and inline threshold apart" } else { "small chunks cost a disk what they cost an SSD, relative to a shared file" }
    ));
    out
}

/// What the plan reports without a trigger: how far a disk should read ahead, whether syncs
/// merge, and what the write cache costs
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn disk_facts(groups: &Groups, leg: &str) -> String {
    let mut out = String::new();
    // the sequential ceiling: the best read over every piece and depth
    let mut best: Option<Interval> = None;
    for piece in ["128K", "1M", "4M"] {
        for depth in [1, 4, 32] {
            if let Some(read) = interval(groups, leg, "seq", &format!("read piece={piece} depth={depth}"), "seq-read", "mib_s") {
                if best.is_none_or(|best| read.median > best.median) {
                    best = Some(read);
                }
            }
        }
    }
    if let Some(best) = best {
        let mut table = Table::new(&["depth", "random MiB/s by size", "reaches 0.5× sequential at", "reaches 0.8× at"]);
        for depth in [1, 4, 32] {
            let mut rates = Vec::new();
            let (mut half, mut most) = (None, None);
            for size in ["4K", "64K", "256K", "1M", "4M", "16M"] {
                // 16 MiB reads hold at most eight in flight
                let depth = if size == "16M" { depth.min(8) } else { depth };
                let Some(rate) = interval(groups, leg, "seq", &format!("random size={size} depth={depth}"), "random", "mib_s") else { continue };
                rates.push(format!("{size} {}", rate.show()));
                if half.is_none() && rate.median >= 0.5 * best.median {
                    half = Some(size);
                }
                if most.is_none() && rate.median >= 0.8 * best.median {
                    most = Some(size);
                }
            }
            table.row(vec![depth.to_string(), rates.join("; "), half.unwrap_or("none").into(), most.unwrap_or("none").into()]);
        }
        out.push_str(&table.render(&format!("Reading ahead, {leg}"), &format!("random reads over the spread population against the best sequential read, {}", best.show())));
    }
    // syncs: flushes a sync with more writers, cache on and off
    let mut table = Table::new(&["writers", "syncs/s", "p50 µs", "flushes/sync", "syncs/s, cache off", "p50 µs, cache off", "flushes/sync, cache off"]);
    let mut any = false;
    for writers in [1, 2, 4, 6] {
        let cell = format!("writers={writers}");
        let Some(syncs) = interval(groups, leg, "sync", &cell, "overwrite", "syncs_s") else { continue };
        any = true;
        let at = |side: &str, figure: &str| show(interval(groups, leg, "sync", &cell, side, figure));
        table.row(vec![
            writers.to_string(),
            syncs.show(),
            at("overwrite", "lat_p50"),
            at("overwrite", "flushes_per_sync"),
            at("overwrite-wt", "syncs_s"),
            at("overwrite-wt", "lat_p50"),
            at("overwrite-wt", "flushes_per_sync"),
        ]);
    }
    if any {
        out.push_str(&table.render(&format!("Syncs, {leg}"), "each writer a file of its own, a 4 KiB overwrite and its fdatasync; fewer flushes than syncs means the block layer merged them"));
    }
    out
}

/// The write cache supplement: the contention and scrub cells with the disk's cache on (`-wb`)
/// and off (`-wt`), alternated by round on one leg
///
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn cache_supplement(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "contend", "batch=32", "idle-wb", "read_p50").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["cell", "side", "figure", "cache on (-wb)", "cache off (-wt)", "off ÷ on by round"]);
    for batch in [32, 128] {
        let cell = format!("batch={batch}");
        for side in ["capacity", "idle", "offset-qd1", "kernel"] {
            let figures: &[&str] = if side == "capacity" {
                &["applies_s", "batch_p50"]
            } else {
                &["applies_s", "batch_p50", "read_p50", "read_p99", "stage_disk_p50", "stage_disk_p99", "stage_ssd_p99", "flushes_s"]
            };
            for figure in figures {
                let (on, off) = (format!("{side}-wb"), format!("{side}-wt"));
                let Some(on_value) = interval(groups, leg, "contend", &cell, &on, figure) else { continue };
                table.row(vec![
                    cell.clone(),
                    side.into(),
                    (*figure).into(),
                    on_value.show(),
                    show(interval(groups, leg, "contend", &cell, &off, figure)),
                    show(ratio(groups, leg, "contend", (&cell, &off, figure), (&cell, &on, figure))),
                ]);
            }
        }
    }
    for budget in ["0", "10", "20", "40", "60", "unbounded"] {
        let cell = format!("budget={budget}");
        for figure in ["scrub_mib_s", "write_p50", "write_p99", "read_p50", "read_p99"] {
            let Some(on_value) = interval(groups, leg, "scrub", &cell, "scrub-wb", figure) else { continue };
            table.row(vec![
                cell.clone(),
                "scrub".into(),
                figure.into(),
                on_value.show(),
                show(interval(groups, leg, "scrub", &cell, "scrub-wt", figure)),
                show(ratio(groups, leg, "scrub", (&cell, "scrub-wt", figure), (&cell, "scrub-wb", figure))),
            ]);
        }
    }
    let mut out = table.render(
        &format!("The write cache supplement, {leg}"),
        "contend and scrub with the disk's write cache on and off, the order alternating by round; latencies µs",
    );
    // H1 and H4 judged again on each cache setting
    for (suffix, what) in [("-wb", "cache on"), ("-wt", "cache off")] {
        let stage = interval(groups, leg, "contend", "batch=32", &format!("idle{suffix}"), "stage_disk_p50");
        let under = interval(groups, leg, "contend", "batch=32", &format!("offset-qd1{suffix}"), "stage_disk_p50");
        let scrub = ratio(
            groups,
            leg,
            "scrub",
            ("budget=10", &format!("scrub{suffix}"), "write_p99"),
            ("budget=0", &format!("scrub{suffix}"), "write_p99"),
        );
        out.push_str(&format!(
            "- {what}: the disk's stage p50 {} ms idle ({} 20 ms) and {} ms beside offset-ordered applies ({}); a 10 MiB/s scrub's write p99 ÷ none {} ({})\n",
            show(ms(stage)),
            against(stage, 20_000.0),
            show(ms(under)),
            against(under, 20_000.0),
            show(scrub),
            against(scrub, 1.25),
        ));
    }
    out.push('\n');
    out
}

/// Name each probes record by the write cache it ran under
///
/// The probes run first in every invocation, and the X7 binary that ran named every probes
/// record `probe`, so a leg whose rounds ran `wcoff` too, or the write cache supplement, holds
/// probes taken with the cache off beside those taken with it on. Every run with the cache off
/// writes sides ending in `-wt` straight after its probes (`wcoff` starts with `sync`, the
/// supplement names its sides by the cache), and records are kept in the order they were
/// written, so a probes record is the cache's off one exactly when the next record of its leg
/// that is not a probe has such a side. A binary that names its probes by the cache itself writes
/// `probe-wt`, and nothing here renames those.
///
/// # Arguments
///
/// * `records` - Every record, in the order the runs wrote them
fn name_probes(records: &[Record]) -> Vec<Record> {
    let mut named: Vec<Record> = records.to_vec();
    for at in 0..named.len() {
        if named[at].measurement != "probes" || named[at].side != "probe" {
            continue;
        }
        // the next record of the same leg that is not a probe says which run this was
        let off = named[at + 1..]
            .iter()
            .find(|next| next.leg == named[at].leg && next.measurement != "probes")
            .is_some_and(|next| next.side.ends_with("-wt"));
        if off {
            named[at].side = "probe-wt".to_string();
        }
    }
    named
}

/// The merged tables and every verdict
///
/// # Arguments
///
/// * `records` - Every record of every round and host
/// * `quick` - Whether quick records count
#[must_use]
pub fn report(records: &[Record], quick: bool) -> String {
    let records = name_probes(records);
    let groups = group(&records, quick);
    let mut out = String::from("# The device store, merged across rounds\n\nEvery figure is `median [min–max]` over the rounds that measured it.\n\n");
    // every measurement's table, a leg at a time
    let legs: Vec<String> = groups.keys().map(|(leg, ..)| leg.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    for leg in &legs {
        out.push_str(&format!("## {leg}\n\n"));
        for measurement in [
            "probes", "seq", "sync", "chunk", "chunk-recycle", "journal", "partial", "remove", "listing", "read", "frag",
            "slices", "contend", "scrub", "shared",
        ] {
            let names = figures(measurement);
            let mut head = vec!["cell", "side"];
            head.extend_from_slice(names);
            let mut table = Table::new(&head);
            let mut any = false;
            for ((group_leg, group_measurement, cell, side), figures) in &groups {
                if group_leg != leg || group_measurement != measurement {
                    continue;
                }
                any = true;
                let mut row = vec![cell.clone(), side.clone()];
                for name in names {
                    let values: Vec<f64> = figures.get(*name).map(|rounds| rounds.values().copied().collect()).unwrap_or_default();
                    row.push(show(Interval::of(&values)));
                }
                table.row(row);
            }
            if any {
                out.push_str(&table.render(measurement, leg));
            }
        }
        out.push_str(&t1(&groups, leg));
        out.push_str(&t2(&groups, leg));
        out.push_str(&t3(&groups, leg));
        out.push_str(&t4(&groups, leg));
        out.push_str(&journal(&groups, leg));
        out.push_str(&h1(&groups, leg));
        out.push_str(&h2(&groups, leg));
        out.push_str(&h3(&groups, leg));
        out.push_str(&h4(&groups, leg));
        out.push_str(&h5(&groups, leg));
        out.push_str(&disk_facts(&groups, leg));
        out.push_str(&cache_supplement(&groups, leg));
    }
    out
}
