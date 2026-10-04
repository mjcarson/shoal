//! Rounds merged into intervals, and the triggers X6 named before it ran judged on them
//!
//! A figure is read across rounds as `median [min–max]`. The lab's rule is that a difference
//! counts only when two intervals are disjoint, and a ratio taken in each round is judged by
//! where its whole interval lies. The four triggers are the plan's, set before the harness
//! existed, and are judged here and nowhere else, so the page quotes what this prints.

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
        "listing" => &["chunks", "build_s", "cold_s", "cold_us", "cold_read_kib", "warm_us", "warm_written_kib", "hours_1m", "hours_4m"],
        "read" => &["open_p50", "read_p50", "total_p50", "total_p99", "total_p999", "dev_read_kib"],
        "frag" => &["extents_0", "extents_10pc", "extents_end", "shared_end", "seq_mib_s", "seq_ratio", "cold_p50", "cold_ratio", "writes_s"],
        "slices" => &["mib_s", "ops_s", "p50", "p99", "busy_max", "run_max", "projected_max", "sustainable_mib_s", "dev_write_mib_s", "flushes_s"],
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
/// # Arguments
///
/// * `groups` - The groups
/// * `leg` - The leg
fn t4(groups: &Groups, leg: &str) -> String {
    if interval(groups, leg, "listing", "deep-1M", "header-qd32", "cold_s").is_none() {
        return String::new();
    }
    let mut table = Table::new(&["population", "walk", "cold s", "cold µs/chunk", "µs/chunk at 1M ÷ at 100K", "verdict"]);
    let mut fires = false;
    let mut xattr_passes = true;
    for shape in ["deep", "wide"] {
        for walk in ["names", "statx-ino", "xattr", "header-qd32"] {
            let (big, small) = (format!("{shape}-1M"), format!("{shape}-100K"));
            let Some(cold) = interval(groups, leg, "listing", &big, walk, "cold_s") else { continue };
            let per = interval(groups, leg, "listing", &big, walk, "cold_us");
            let scale = ratio(groups, leg, "listing", (&big, walk, "cold_us"), (&small, walk, "cold_us"));
            let slow = cold.min > 60.0 || scale.is_some_and(|scale| scale.min > 2.0);
            // the cheapest walk that yields a label is the header's while the label is in the header
            if walk == "header-qd32" && shape == "deep" {
                fires = slow;
            }
            if walk == "xattr" && slow {
                xattr_passes = false;
            }
            table.row(vec![big, walk.into(), cold.show(), show(per), show(scale), if slow { "**too slow**".into() } else { "fine".into() }]);
        }
    }
    let note = if fires && xattr_passes {
        "the header walk fails and the attribute walk passes: the label moves, an index is not needed"
    } else if fires {
        "a light scrub needs an index of its own"
    } else {
        "a light scrub can walk the directories"
    };
    format!(
        "{}T4 {} on this leg: {note}.\n\n",
        table.render(&format!("T4. A light scrub's walk, {leg}"), "fires above 60 s cold for a million chunks, or above 2× the cost a chunk at 100K"),
        if fires && !xattr_passes { "**fires**" } else { "does not fire" }
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

/// The merged tables and every verdict
///
/// # Arguments
///
/// * `records` - Every record of every round and host
/// * `quick` - Whether quick records count
#[must_use]
pub fn report(records: &[Record], quick: bool) -> String {
    let groups = group(records, quick);
    let mut out = String::from("# X6 merged across rounds\n\nEvery figure is `median [min–max]` over the rounds that measured it.\n\n");
    // every measurement's table, a leg at a time
    let legs: Vec<String> = groups.keys().map(|(leg, ..)| leg.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    for leg in &legs {
        out.push_str(&format!("## {leg}\n\n"));
        for measurement in ["chunk", "chunk-recycle", "journal", "partial", "remove", "listing", "read", "frag", "slices"] {
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
    }
    out
}
