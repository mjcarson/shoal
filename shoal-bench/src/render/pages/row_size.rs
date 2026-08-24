//! What row width costs
//!
//! The other half of the cross. Everything here is the same even mixture against the same tables;
//! the only thing that varies is how wide a row is.

use anyhow::Result;

use crate::registry::Layer;
use crate::fmt;
use crate::render::arms::{self, Arm};
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// The read share the width sweep is measured at
const REFERENCE_MIX: u32 = 50;

/// Builds the row width page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::RowSize,
        page,
        "An even mixture of reads and writes, swept from a 64 byte row to a 4 MiB one, on every \
         table. The three named mixtures at the end of the axis are declared distributions of \
         widths rather than ranges, so two captures of one measure the same thing.",
    );
    let Some(capture) = page.current_for(Layer::Macro).and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a row width sweep"));
        out.push_str(&footer(Surface::RowSize));
        return Ok(out);
    };
    let all: Vec<Arm<'_>> = arms::grid(capture)
        .into_iter()
        .filter(|arm| arm.read_pct() == Some(REFERENCE_MIX))
        .collect();
    // the fixed widths carry the curve; a mixture has no place on a numeric axis, since its width
    // is a mean and plotting a mean beside a measurement invites the two to be read alike
    let fixed: Vec<Arm<'_>> = all
        .iter()
        .copied()
        .filter(|arm| arm.row_profile().is_none())
        .collect();
    let mixed: Vec<Arm<'_>> = all
        .iter()
        .copied()
        .filter(|arm| arm.row_profile().is_some())
        .collect();
    if fixed.is_empty() && mixed.is_empty() {
        out.push_str(&nothing_measured("a row width sweep"));
        out.push_str(&footer(Surface::RowSize));
        return Ok(out);
    }
    let kinds = arms::table_kinds(&all);
    out.push_str(&latency(&fixed, &kinds)?);
    out.push_str(&rows_per_sec(&fixed, &kinds)?);
    out.push_str(&bytes_per_sec(&fixed, &kinds)?);
    out.push_str(&mixtures(&mixed, &fixed));
    // the same axis at the two ends of the mixture, which is what separates a write path cost per
    // byte from a read path one. these are grid cells like the ones above and are selected the same
    // way, by the share they recorded rather than by anything in their identifier
    let ends: Vec<Arm<'_>> = arms::grid(capture)
        .into_iter()
        .filter(|arm| {
            arm.row_profile().is_none()
                && matches!(arm.read_pct(), Some(share) if share != REFERENCE_MIX)
        })
        .collect();
    out.push_str(&by_mixture(&ends, &fixed)?);
    // and the same axis with one query outstanding, which bounds how much of the above was queue
    out.push_str(&at_depth_one(&arms::width_depth(capture), &fixed)?);
    // then the attribution: which of the nineteen stages a query passes through grows with bytes
    out.push_str(&by_stage(page, &all)?);
    out.push_str("## Every width\n\n");
    out.push_str(&tables::grid_cells(&all));
    out.push('\n');
    out.push_str(&footer(Surface::RowSize));
    Ok(out)
}

/// How the cost of one query moves with the row width
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn latency(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## What one query costs, against the row width\n\n");
    if fixed.is_empty() {
        out.push_str("No arm was measured at a fixed width.\n\n");
        return Ok(out);
    }
    let mut series = Vec::new();
    for op in ["read", "write"] {
        for kind in kinds {
            let points: Vec<(f64, f64)> = fixed
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((arm.row_bytes() as f64, arm.stat(op, "p50")?)))
                .collect();
            if !points.is_empty() {
                series.push(sweep::Series {
                    name: format!("{op}, {}", arms::table_label(kind)),
                    points,
                });
            }
        }
    }
    if series.is_empty() {
        out.push_str("No arm at a fixed width recorded a latency.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-latency".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "p50 service time".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median service time against row width, both axes logarithmic. A flat left hand end is a \
         fixed per-response cost that the row is too narrow to matter against; a straight diagonal \
         right hand end is a per-byte cost that has taken over. Where the two meet is the width \
         above which how much data a row holds is the whole story.",
    ));
    out.push_str(&knee_note(fixed, kinds));
    Ok(out)
}

/// Where the fixed cost gives way to the per-byte one, in words
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover
fn knee_note(fixed: &[Arm<'_>], kinds: &[String]) -> String {
    let Some(kind) = kinds.first() else {
        return String::new();
    };
    // every width this table was read at, in order
    let mut points: Vec<(u64, f64)> = fixed
        .iter()
        .filter(|arm| arm.table_kind() == Some(kind.as_str()))
        .filter_map(|arm| Some((arm.row_bytes(), arm.stat("read", "p50")?)))
        .collect();
    points.sort_by_key(|(width, _)| *width);
    if points.len() < 3 {
        return String::new();
    }
    // the first width at which doubling the row more than doubles the cost is where the per-byte
    // term has taken over. stated as the width itself rather than as a fitted slope, since three
    // to eight points is not enough data to fit anything to
    let mut knee = None;
    for pair in points.windows(2) {
        let (low_width, low_cost) = pair[0];
        let (high_width, high_cost) = pair[1];
        let width_ratio = high_width as f64 / low_width.max(1) as f64;
        let cost_ratio = high_cost / low_cost.max(f64::MIN_POSITIVE);
        // "cost keeps up with width" is the signature of a per-byte cost
        if cost_ratio >= width_ratio * 0.6 {
            knee = Some(low_width);
            break;
        }
    }
    let (narrowest, narrow_cost) = points[0];
    let (widest, wide_cost) = points[points.len() - 1];
    let mut out = format!(
        "On the {} table a read of a {} row took {} and a read of a {} one took {}: **{}× the \
         bytes for {}× the time.**",
        arms::table_label(kind),
        fmt::bytes(narrowest),
        fmt::duration_ns(narrow_cost),
        fmt::bytes(widest),
        fmt::duration_ns(wide_cost),
        fmt::thousands((widest / narrowest.max(1)) as u128),
        fmt::fixed(wide_cost / narrow_cost.max(f64::MIN_POSITIVE), 1)
    );
    if let Some(width) = knee {
        out.push_str(&format!(
            " The cost starts tracking the width somewhere above {}; below that a row is too \
             narrow for its own size to be what the query is paying for.",
            fmt::bytes(width)
        ));
    }
    out.push_str("\n\n");
    out
}

/// How many queries a second each width sustained
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn rows_per_sec(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Queries a second, against the row width\n\n");
    let series = rate_series(fixed, kinds, |arm| arm.ops_per_sec());
    if series.is_empty() {
        out.push_str("No arm at a fixed width reported a query rate.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-ops".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "queries answered per second".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Rate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Queries a second against row width. This is the pessimistic reading of the axis: a wider \
         row buys fewer queries, always.",
    ));
    Ok(out)
}

/// How many payload bytes a second each width sustained
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover, in reading order
fn bytes_per_sec(fixed: &[Arm<'_>], kinds: &[String]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Payload bytes a second, against the row width\n\n");
    let series = rate_series(fixed, kinds, |arm| arm.bytes_per_sec());
    if series.is_empty() {
        out.push_str("No arm at a fixed width reported a byte rate.\n\n");
        return Ok(out);
    }
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-bytes".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "payload bytes per second".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::ByteRate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "The same runs counted in bytes rather than queries. This is the optimistic reading, and \
         it usually rises where the chart above falls - a store that moves few large rows quickly \
         is not slow. Payloads only: framing, keys and the archive's own overhead are not counted, \
         so this is a floor on what crossed the wire and never a ceiling.",
    ));
    Ok(out)
}

/// How many stages are drawn before the rest are folded into one segment
///
/// Nineteen series on one chart is unreadable, which is the whole argument of
/// [F19](../features/chart-legends.md). The dearest few are what the fall is made of.
const STAGES_DRAWN: usize = 6;

/// The rank whose breakdown is drawn against the width
///
/// Every query rather than a percentile. A percentile of one stage is not the breakdown of any
/// query that happened, which is why the report ranks by the **total** and averages a window - and
/// across three widths the honest comparison is the mean of everything each of them ran.
const STAGE_RANK: &str = "all";

/// Which of the nineteen stages grows with the row width
///
/// The stage layer used to run one workload, at 256 byte rows, so nothing anywhere said *which*
/// part of a query got dearer as rows widened - only that the total did. It runs the width axis
/// now, and this is the section that replaces the argument with attribution.
///
/// # Arguments
///
/// * `page` - Everything the page is built from, which is where the stage reports are
/// * `all` - The grid arms of the current capture, which is what says how wide each one ran
fn by_stage(page: &Page, all: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Which stage grows with the bytes\n\n");
    let Some(snapshot) = page.current_for(Layer::Stages) else {
        out.push_str(&nothing_measured("a stage report"));
        return Ok(out);
    };
    let reports = snapshot
        .stages
        .as_ref()
        .expect("the snapshot was selected for having one");
    // every report whose workload is an arm of this capture, paired with the width that arm ran at.
    // read off the arm's recorded facts rather than out of its identifier, the way the rest of this
    // page reads a width
    let mut widths: Vec<(u64, &crate::model::stages::StageReport)> = reports
        .reports
        .iter()
        .filter_map(|(workload, report)| {
            let arm = all.iter().find(|arm| arm.id == workload)?;
            // a mixture has no place on a numeric axis here for the same reason it has none above
            arm.row_profile().is_none().then_some((arm.row_bytes(), report))
        })
        .collect();
    widths.sort_by_key(|(width, _)| *width);
    if widths.len() < 2 {
        out.push_str(
            "This capture profiled the stages of fewer than two widths, so nothing here can say \
             which stage grows with the bytes. The breakdown it did take is on \
             [Attribution](attribution.md).\n\n",
        );
        return Ok(out);
    }
    out.push_str(&format!(
        "From `{}`, built with the `stage-profile` feature, at {} of the widths above. **An \
         instrumented build stamps nineteen extra timestamps on every query, so none of these is a \
         latency anybody should quote** - what they are for is the ratio between them, and how each \
         moves along the axis. Read a flat line as a fixed cost the payload never reaches and a \
         rising one as a cost that walks the bytes.\n\n",
        snapshot.label,
        fmt::thousands(widths.len() as u128)
    ));
    out.push_str(&stage_chart(&widths)?);
    Ok(out)
}

/// Draws each stage's mean cost against the row width
///
/// # Arguments
///
/// * `widths` - Each width profiled and the report taken at it, in ascending width order
fn stage_chart(widths: &[(u64, &crate::model::stages::StageReport)]) -> Result<String> {
    // the operation whose write path the intent log is on, and the read beside it
    let mut out = String::new();
    for op in ["insert", "get"] {
        // one series per stage, keyed by stage name, taking the mean at every width it was
        // measured at. a stage at the floor is dropped rather than drawn, because a number below
        // the clock's own cost is the instrument and not the stage
        let mut series: std::collections::BTreeMap<String, Vec<(f64, f64)>> =
            std::collections::BTreeMap::new();
        for (width, report) in widths {
            let Some(bucket) = report.bucket(op, STAGE_RANK) else {
                continue;
            };
            for (stage, cost) in &bucket.stages {
                if cost.at_floor {
                    continue;
                }
                series
                    .entry(stage.clone())
                    .or_default()
                    .push((*width as f64, cost.mean_ns as f64));
            }
        }
        // a stage measured at one width alone is a dot, not a curve
        series.retain(|_, points| points.len() > 1);
        if series.is_empty() {
            continue;
        }
        // the dearest at the widest point first, so the chart keeps the stages the fall is made of
        let mut ranked: Vec<sweep::Series> = series
            .into_iter()
            .map(|(name, points)| sweep::Series { name, points })
            .collect();
        ranked.sort_by(|left, right| {
            let last = |line: &sweep::Series| line.points.last().map(|(_, ns)| *ns).unwrap_or(0.0);
            last(right)
                .partial_cmp(&last(left))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.name.cmp(&right.name))
        });
        let dropped = ranked.len().saturating_sub(STAGES_DRAWN);
        ranked.truncate(STAGES_DRAWN);
        out.push_str(&format!("### {op}\n\n"));
        out.push_str(&sweep::draw(
            &sweep::Spec {
                id: format!("chart-row-size-stages-{op}"),
                x_desc: "row width".to_string(),
                y_desc: "mean time in stage".to_string(),
                x_axis: sweep::Axis::Log,
                y_axis: sweep::Axis::Log,
                x_unit: Unit::Bytes,
                y_unit: Unit::Duration,
            },
            &ranked,
        )?);
        out.push('\n');
        let tail = if dropped > 0 {
            format!(
                " The {} cheaper stages are not drawn; every stage within twice the cost of \
                 reading the clock is left out entirely, because below that the number is the \
                 instrument.",
                fmt::thousands(dropped as u128)
            )
        } else {
            String::new()
        };
        out.push_str(&caption(&format!(
            "Mean time in each stage of an {op} query against the row width, both axes \
             logarithmic, from an instrumented build. A stage that walks the payload rises with \
             the width; one that does not is flat.{tail}"
        )));
    }
    if out.is_empty() {
        out.push_str("No stage was measured above the clock's own cost at more than one width.\n\n");
    }
    Ok(out)
}

/// The width axis at each end of the read/write mixture
///
/// The reference sweep runs at an even split, so the divergence it shows between the persistent and
/// ephemeral halves is a write path effect measured under a mixture half of which is reads. These
/// arms are the same axis at `r0` and at `r100`, which sizes the two halves apart.
///
/// # Arguments
///
/// * `ends` - The fixed-width arms at a mixture other than the reference one
/// * `reference` - The fixed-width arms at the reference mixture, drawn alongside for scale
fn by_mixture(ends: &[Arm<'_>], reference: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The same axis under a pure write and a pure read mixture\n\n");
    if ends.is_empty() {
        out.push_str(
            "This capture swept the width axis at the reference mixture only, so there is nothing \
             to read the write path against.\n\n",
        );
        return Ok(out);
    }
    // one series per mixture, on one table, so the three curves differ in the mixture alone. drawn
    // on the persistent unsorted table because that is the table every reference cell drives and
    // the one the intent log is actually exercised through
    const TABLE: &str = "persistent_unsorted";
    let mut shares: Vec<u32> = ends.iter().filter_map(Arm::read_pct).collect();
    shares.push(REFERENCE_MIX);
    shares.sort_unstable();
    shares.dedup();
    let mut series = Vec::new();
    for share in shares {
        let source = if share == REFERENCE_MIX { reference } else { ends };
        let points: Vec<(f64, f64)> = source
            .iter()
            .filter(|arm| arm.table_kind() == Some(TABLE) && arm.read_pct() == Some(share))
            .filter_map(|arm| Some((arm.row_bytes() as f64, arm.ops_per_sec()?)))
            .collect();
        // a share measured at one width is a point on the mixture sweep, not a width axis. drawing
        // it here would put a dot on a chart of curves and read as though the axis had been swept
        // at that share, which is exactly the gap this section exists to close
        if points.len() > 1 {
            series.push(sweep::Series {
                name: format!("r{share}"),
                points,
            });
        }
    }
    // the reference share always qualifies, so one series means the ends did not
    if series.len() < 2 {
        out.push_str(
            "This capture swept the width axis at the reference mixture only, so there is nothing \
             to read the write path against.\n\n",
        );
        return Ok(out);
    }
    out.push_str(
        "One series per mixture, on the persistent unsorted table. `r0` issues **no reads at all** \
         and `r100` issues no writes, exactly rather than probabilistically, so neither curve has \
         the other half of the mixture folded into it. The reference split is drawn between them \
         for scale.\n\n",
    );
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-mixture".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "queries answered per second".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Rate,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Queries a second against row width, at three read shares on one table. Where the `r0` \
         curve falls away from the `r100` one, the extra cost is on the write path and grows with \
         the bytes - which is the intent log. Where the two fall together, the cost is shared by \
         both and is a per-byte cost of the wire and the copies on it.",
    ));
    out.push_str("### Every width, at each end\n\n");
    out.push_str(&tables::grid_cells(ends));
    out.push('\n');
    Ok(out)
}

/// The width axis with a single query outstanding
///
/// # Arguments
///
/// * `ladder` - The rungs of the width ladder at one outstanding query
/// * `reference` - The fixed-width arms at the grid's depth, drawn alongside
fn at_depth_one(ladder: &[Arm<'_>], reference: &[Arm<'_>]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The same axis with one query outstanding\n\n");
    if ladder.is_empty() {
        out.push_str(
            "This capture ran no arm at a single outstanding query, so nothing here separates a \
             service time from a queue length.\n\n",
        );
        return Ok(out);
    }
    const TABLE: &str = "persistent_unsorted";
    let mut series = Vec::new();
    // the ladder first, then the depth the rest of the page runs at, so the two are read as a pair
    for (name, source) in [("1 outstanding", ladder), ("32 outstanding", reference)] {
        let points: Vec<(f64, f64)> = source
            .iter()
            .filter(|arm| arm.table_kind() == Some(TABLE) && arm.row_profile().is_none())
            .filter(|arm| arm.read_pct() == Some(REFERENCE_MIX))
            .filter_map(|arm| Some((arm.row_bytes() as f64, arm.stat("read", "p50")?)))
            .collect();
        // one point is the single rung the depth ladder has always had at the reference width,
        // which is a crossing point and not a curve
        if points.len() > 1 {
            series.push(sweep::Series {
                name: name.to_string(),
                points,
            });
        }
    }
    if series.len() < 2 {
        out.push_str(
            "This capture ran the width axis at one load depth only, so nothing here separates a \
             service time from a queue length.\n\n",
        );
        return Ok(out);
    }
    out.push_str(
        "Every arm above keeps thirty two queries outstanding at every width. At 4 MiB that is 128 \
         MiB in flight on one connection against a key space of sixty four partitions, so those \
         arms are measuring queueing and contention as much as service time. These are the same \
         widths with **one** query outstanding, on the persistent unsorted table.\n\n",
    );
    out.push_str(&sweep::draw(
        &sweep::Spec {
            id: "chart-row-size-depth".to_string(),
            x_desc: "row width".to_string(),
            y_desc: "p50 service time".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Bytes,
            y_unit: Unit::Duration,
        },
        &series,
    )?);
    out.push('\n');
    out.push_str(&caption(
        "Median read service time against row width, at two load depths on one table. The gap \
         between the two curves is what the depth of 32 was costing at that width: where they lie \
         together the deeper arm's latency is a service time, and where they separate it is a \
         queue length. **Read the throughput of the depth-1 curve as a floor** - one query \
         outstanding leaves eleven of twelve shards idle, so it bounds latency and says nothing \
         about capacity.",
    ));
    out.push_str("### Every width, at one query outstanding\n\n");
    out.push_str(&tables::grid_cells(ladder));
    out.push('\n');
    Ok(out)
}

/// One rate series per table
///
/// # Arguments
///
/// * `fixed` - The arms at a fixed width
/// * `kinds` - The table kinds they cover
/// * `rate` - Which rate to read off an arm
fn rate_series(
    fixed: &[Arm<'_>],
    kinds: &[String],
    rate: impl Fn(&Arm<'_>) -> Option<f64>,
) -> Vec<sweep::Series> {
    kinds
        .iter()
        .map(|kind| sweep::Series {
            name: arms::table_label(kind),
            points: fixed
                .iter()
                .filter(|arm| arm.table_kind() == Some(kind.as_str()))
                .filter_map(|arm| Some((arm.row_bytes() as f64, rate(arm)?)))
                .collect(),
        })
        .filter(|line| !line.points.is_empty())
        .collect()
}

/// What the mixed-width arms did, against the fixed widths they average out to
///
/// # Arguments
///
/// * `mixed` - The arms drawing from a width distribution
/// * `fixed` - The arms at a fixed width, to compare them against
fn mixtures(mixed: &[Arm<'_>], fixed: &[Arm<'_>]) -> String {
    let mut out = String::new();
    out.push_str("## The mixed widths\n\n");
    if mixed.is_empty() {
        out.push_str("No arm drew its rows from a width distribution.\n\n");
        return out;
    }
    out.push_str(
        "Three arms draw each row's width from a declared set rather than using one width \
         throughout. They are not on the charts above, and deliberately: a mixture's width is the \
         **mean** of its distribution, and a mean plotted on the same axis as a measurement invites \
         the two to be read the same way.\n\n\
         What they are for is the comparison in the table below. A mixture whose cost matches the \
         fixed-width arm at its own mean is a system whose cost is linear across that range. A \
         mixture that costs more than its mean says the widths in it are not interchangeable - that \
         the wide rows in the distribution cost more than the narrow ones save.\n\n",
    );
    out.push_str(&tables::width_mixtures(mixed, fixed));
    out.push('\n');
    out
}
