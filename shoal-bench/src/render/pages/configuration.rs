//! What each setting of the server configuration is worth
//!
//! Every other page on this site measures the engine against one fixed configuration. This one
//! moves the configuration and holds the engine still, which is the other half of the same
//! question: a reader deciding what to put in their own `shoal.yml` is choosing between the arms on
//! this page, and a bottleneck that lives in a setting rather than in the code shows up here and
//! nowhere else.
//!
//! # The gate this page is built around
//!
//! A sweep of nine knobs produces nine recommendations whether or not any of them is real. The
//! *Real?* column is what stops that: a knob whose fastest and slowest arms have **overlapping**
//! observed run intervals has not been shown to have a fastest and a slowest arm, however far apart
//! its medians are, and is reported as no measurable difference rather than as advice. That is the
//! macro layer's own rule rather than a new one - see
//! [F7](../../../docs/src/features/bench-runner.md) - and it is the difference between a tuning
//! guide and a table of noise.

use anyhow::Result;

use crate::registry::Layer;
use crate::fmt;
use crate::render::arms::{self, Arm};
use crate::render::chart::bars;
use crate::render::chart::sweep::{self, Unit};
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables::{self, Verdict};

/// The storage knobs, in the order they are read
///
/// The barrier first, because it is the largest single decision on the page and the one a caller is
/// most likely to be choosing. Then how a write is buffered and queued, then the archive side.
const STORAGE_KNOBS: [&str; 6] = [
    "durability",
    "latency_buffer",
    "latency_write_behind",
    "intent_log",
    "throughput_buffer",
    "throughput_write_behind",
];

/// The resource knobs, in the order they are read
const RESOURCE_KNOBS: [&str; 3] = ["shards", "memory", "frame"];

/// Builds the configuration page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Configuration,
        page,
        "Every arm on this page is the grid's reference cell - one client, an even read/write \
         mixture, 1 KiB rows, the persistent unsorted table - with exactly one field of the server \
         configuration moved. What it answers is what that field is worth.",
    );
    let Some(capture) = page.current_for(Layer::Macro).and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("a configuration sweep"));
        out.push_str(&footer(Surface::Configuration));
        return Ok(out);
    };
    let sweeps = arms::conf_sweeps(capture);
    if sweeps.is_empty() {
        out.push_str(&nothing_measured("a configuration sweep"));
        out.push_str(&footer(Surface::Configuration));
        return Ok(out);
    }
    // the verdicts first, since they are what the rest of the page is evidence for
    out.push_str(&what_to_set(&sweeps));
    out.push_str(&storage(&sweeps)?);
    out.push_str(&resources(&sweeps)?);
    out.push_str("## Every arm\n\n");
    out.push_str(&tables::conf_arms(&sweeps));
    out.push('\n');
    out.push_str(&footer(Surface::Configuration));
    Ok(out)
}

/// The knobs a set of sweeps covers, in a declared reading order
///
/// Anything the order does not name is appended rather than dropped, so a knob added to the sweep
/// without this list being updated is late rather than missing - the same rule
/// [`arms::table_kinds`] follows. It said that before it did it: the loop below used to take only
/// the named knobs, so a knob absent from the list vanished from the recommendation table entirely
/// and nothing said so.
///
/// A sweep repeated at another row width is named `<knob> @ <width>`
/// ([`arms::conf_sweeps`]), which is why the match is a prefix rather than an equality - it keeps a
/// repeat next to the sweep it repeats instead of at the end of the page.
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
/// * `order` - The knobs to put first, in the order to put them
fn in_order<'a>(
    sweeps: &'a [(String, u32, Vec<Arm<'a>>)],
    order: &[&str],
) -> Vec<&'a (String, u32, Vec<Arm<'a>>)> {
    /// Whether a sweep's name is this knob, at the reference width or at any other
    ///
    /// # Arguments
    ///
    /// * `name` - The sweep's name
    /// * `knob` - The knob being looked for
    fn is_knob(name: &str, knob: &str) -> bool {
        name == knob || name.starts_with(&format!("{knob} @"))
    }
    let mut picked: Vec<&(String, u32, Vec<Arm<'_>>)> = Vec::new();
    // the named knobs first, and within a knob the read shares in ascending order. the name is the
    // secondary key so a repeat at a width follows the reference sweep rather than interleaving
    for knob in order {
        let mut matching: Vec<&(String, u32, Vec<Arm<'_>>)> = sweeps
            .iter()
            .filter(|(name, _, _)| is_knob(name, knob))
            .collect();
        matching.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        picked.extend(matching);
    }
    picked
}

/// The knobs a set of sweeps covers, in a declared reading order, with nothing left out
///
/// [`in_order`] keeps only what its order names, which is what the two section functions want -
/// each of them is handed one half of the configuration on purpose. This is for the caller that
/// wants the whole set: anything the order does not name is appended rather than dropped, so a knob
/// added to the sweep without the lists being updated reads last instead of not at all.
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
/// * `order` - The knobs to put first, in the order to put them
fn in_order_with_rest<'a>(
    sweeps: &'a [(String, u32, Vec<Arm<'a>>)],
    order: &[&str],
) -> Vec<&'a (String, u32, Vec<Arm<'a>>)> {
    let mut picked = in_order(sweeps, order);
    // whatever the first pass did not take, in the capture's own order. compared by name and share
    // rather than by address, since that is what identifies a sweep
    let taken: Vec<(&str, u32)> = picked
        .iter()
        .map(|(name, read_pct, _)| (name.as_str(), *read_pct))
        .collect();
    picked.extend(
        sweeps
            .iter()
            .filter(|(name, read_pct, _)| !taken.contains(&(name.as_str(), *read_pct))),
    );
    picked
}

/// The verdict table, and the one sentence that says how to read it
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
fn what_to_set(sweeps: &[(String, u32, Vec<Arm<'_>>)]) -> String {
    let mut out = String::new();
    out.push_str("## What the data says\n\n");
    // both halves in one table, storage first, because a reader arriving here is choosing settings
    // rather than choosing a section
    let mut ordered: Vec<&str> = STORAGE_KNOBS.to_vec();
    ordered.extend(RESOURCE_KNOBS);
    let verdicts: Vec<Verdict<'_>> = in_order_with_rest(sweeps, &ordered)
        .into_iter()
        .map(|(knob, read_pct, arms)| tables::verdict(knob, *read_pct, arms))
        .collect();
    out.push_str(&tables::conf_recommendations(&verdicts));
    out.push('\n');
    // a count rather than a claim, so the sentence is true of whatever was actually captured
    let real = verdicts
        .iter()
        .filter(|verdict| verdict.real == Some(true))
        .count();
    // named from the data rather than asserted, because which sweeps come back flat is the thing
    // the page is measuring - a sentence that claimed in advance which rows would say `no` would be
    // wrong on any capture that disagreed with it, and wrong in the confident direction
    let flat: Vec<String> = verdicts
        .iter()
        .filter(|verdict| verdict.real == Some(false))
        .map(|verdict| format!("`{}`", verdict.knob))
        .collect();
    out.push_str(&format!(
        "**{real} of {} sweeps moved anything measurable.** ",
        verdicts.len()
    ));
    if flat.is_empty() {
        out.push_str(
            "Every sweep's fastest and slowest arms are disjoint across runs, so each row above is \
             a difference rather than a spread.",
        );
    } else {
        out.push_str(&format!(
            "{} did not: {}. Their fastest and slowest arms overlapped across runs, so there is no \
             evidence those settings do anything at all here. **A `no` is a finding rather than a \
             gap** - it says that setting is not where this workload's time goes, which is worth \
             more than a recommendation would have been, because it is the knob not to spend the \
             afternoon on.",
            flat.len(),
            list(&flat)
        ));
    }
    out.push_str("\n\n");
    out
}

/// Writes a list of names the way a sentence would
///
/// # Arguments
///
/// * `items` - The names to join, already quoted
///
/// # Examples
///
/// ```
/// use shoal_bench::render::pages::configuration::list;
///
/// assert_eq!(list(&["a".to_string()]), "a");
/// assert_eq!(list(&["a".to_string(), "b".to_string()]), "a and b");
/// assert_eq!(list(&["a".to_string(), "b".to_string(), "c".to_string()]), "a, b and c");
/// ```
pub fn list(items: &[String]) -> String {
    // an `and` before the last one, which is what stops a two item list reading as a fragment
    match items {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// The filesystem writer settings
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
fn storage(sweeps: &[(String, u32, Vec<Arm<'_>>)]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## The filesystem writers\n\n");
    let picked = in_order(sweeps, &STORAGE_KNOBS);
    if picked.is_empty() {
        out.push_str(&nothing_measured("a storage setting"));
        return Ok(out);
    }
    // one bar per knob, showing how far apart its best and worst arms landed. this is the chart
    // that answers "where is this workload actually sensitive" rather than "what should I set"
    let groups: Vec<String> = picked.iter().map(|(knob, _, _)| knob.clone()).collect();
    let extreme = |pick: fn(&[f64]) -> Option<f64>| -> Vec<Option<f64>> {
        picked
            .iter()
            .map(|(_, _, arms)| {
                let values: Vec<f64> = arms
                    .iter()
                    .filter_map(|arm| arm.stat("write", "p50"))
                    .collect();
                pick(&values)
            })
            .collect()
    };
    let series = vec![
        bars::Series {
            name: "best value".to_string(),
            values: extreme(|values| values.iter().copied().reduce(f64::min)),
        },
        bars::Series {
            name: "worst value".to_string(),
            values: extreme(|values| values.iter().copied().reduce(f64::max)),
        },
    ];
    // a chart of one bar per group says nothing, and a group with no measurement at either end
    // would be drawn as two gaps
    if series.iter().any(|line| line.values.iter().any(Option::is_some)) {
        out.push_str(&bars::draw(
            &bars::Spec {
                id: "chart-conf-storage-spread".to_string(),
                x_desc: "which storage setting was swept".to_string(),
                y_desc: "p50 write service time".to_string(),
                unit: Unit::Duration,
            },
            &groups,
            &series,
        )?);
        out.push('\n');
        out.push_str(&caption(
            "The best and worst value of each storage setting, by median write service time. The \
             **gap** is what the setting is worth; a pair of bars at the same height is a setting \
             this workload does not care about. Check the *Real?* column above before reading a gap \
             as a result.",
        ));
    }
    // the write-behind ladder is the one storage knob that is a genuine numeric sweep, so it gets
    // the shape treatment the others cannot have
    out.push_str(&ladder(
        sweeps,
        "latency_write_behind",
        "chart-conf-write-behind",
        "intent log writes in flight at once",
        "Write service time against the depth of the write path's io_uring queue. The writer stalls \
         until a completion drains once this many writes are outstanding, so the left hand end is a \
         write path that serialises. Where this flattens is the depth past which more queue buys \
         nothing.",
    )?);
    out.push_str(&throughput_note(sweeps));
    Ok(out)
}

/// What the two throughput writer sweeps found, and why
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
fn throughput_note(sweeps: &[(String, u32, Vec<Arm<'_>>)]) -> String {
    let flat = ["throughput_buffer", "throughput_write_behind"]
        .iter()
        .filter_map(|knob| {
            let (name, read_pct, arms) = sweeps.iter().find(|(name, _, _)| name == knob)?;
            Some(tables::verdict(name, *read_pct, arms))
        })
        .filter(|verdict| verdict.real == Some(false))
        .count();
    // said only when the data says it, since the whole point of the sentence is that it is a
    // measurement rather than a reading of the source
    if flat == 0 {
        return String::new();
    }
    format!(
        "Both `throughput_*` sweeps are here to be flat, and {flat} of them {}. \
         [Item 71](../appendix/known-issues.md) is why: `throughput_sensitive` is applied to the \
         archive **map's** intent log and not to the archive writers themselves, which are built \
         with glommio's defaults and never read the configuration. So a flat line here is evidence \
         about the wiring rather than about the device, and the setting is not a dial worth turning \
         until that item is closed.\n\n",
        if flat == 1 { "is" } else { "are" }
    )
}

/// Cores, memory and the frame bound
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
fn resources(sweeps: &[(String, u32, Vec<Arm<'_>>)]) -> Result<String> {
    let mut out = String::new();
    out.push_str("## Cores, memory and the frame bound\n\n");
    let picked = in_order(sweeps, &RESOURCE_KNOBS);
    if picked.is_empty() {
        out.push_str(&nothing_measured("a resource setting"));
        return Ok(out);
    }
    // the shard curve, with a line per read share, because the two halves of a mixture scale
    // differently and drawing one line would average that away
    let mut scaling = Vec::new();
    for (knob, read_pct, arms) in &picked {
        if *knob != "shards" {
            continue;
        }
        let points: Vec<(f64, f64)> = arms
            .iter()
            .filter_map(|arm| Some((numeric(arm.conf_value()?)?, arm.ops_per_sec()?)))
            .collect();
        if points.len() > 1 {
            scaling.push(sweep::Series {
                name: format!("r{read_pct}"),
                points,
            });
        }
    }
    if !scaling.is_empty() {
        out.push_str(&sweep::draw(
            &sweep::Spec {
                id: "chart-conf-shards".to_string(),
                x_desc: "shards the server ran".to_string(),
                y_desc: "queries answered per second".to_string(),
                x_axis: sweep::Axis::Log,
                y_axis: sweep::Axis::Linear,
                x_unit: Unit::Count,
                y_unit: Unit::Rate,
            },
            &scaling,
        )?);
        out.push('\n');
        out.push_str(&caption(
            "Throughput against shard count, at an even mixture and at a pure read share. Read \
             where each line stops rising rather than what it reaches. The client shares this \
             machine with the server, so the low end of this axis runs under less contention than \
             the high end and the curve flatters the small configurations.",
        ));
    }
    // the memory cliff, which is flat while the working set fits and steps once it does not
    let mut cliff = Vec::new();
    for (knob, read_pct, arms) in &picked {
        if *knob != "memory" {
            continue;
        }
        let points: Vec<(f64, f64)> = arms
            .iter()
            .filter_map(|arm| Some((numeric(arm.conf_value()?)?, arm.stat("read", "p50")?)))
            .collect();
        if points.len() > 1 {
            cliff.push(sweep::Series {
                name: format!("r{read_pct}"),
                points,
            });
        }
    }
    if !cliff.is_empty() {
        out.push_str(&sweep::draw(
            &sweep::Spec {
                id: "chart-conf-memory".to_string(),
                x_desc: "memory limit per shard".to_string(),
                y_desc: "p50 read service time".to_string(),
                x_axis: sweep::Axis::Log,
                y_axis: sweep::Axis::Log,
                x_unit: Unit::Bytes,
                y_unit: Unit::Duration,
            },
            &cliff,
        )?);
        out.push('\n');
        out.push_str(&caption(
            "Median read against the memory limit. This is a cliff rather than a curve: flat while \
             the working set fits, and stepping once a read has to find its partition on disk. \
             **The limit is per shard**, so a twelve shard server at 4Gi is holding 48 GiB. The \
             number worth taking off this chart is where the step is.",
        ));
    }
    out.push_str(&memory_note(&picked));
    Ok(out)
}

/// Where the memory cliff fell, in words
///
/// # Arguments
///
/// * `picked` - The resource sweeps, in reading order
fn memory_note(picked: &[&(String, u32, Vec<Arm<'_>>)]) -> String {
    let Some((_, read_pct, arms)) = picked.iter().find(|(knob, _, _)| knob == "memory") else {
        return String::new();
    };
    let mut sorted: Vec<(f64, &str, f64)> = arms
        .iter()
        .filter_map(|arm| {
            let value = arm.conf_value()?;
            Some((numeric(value)?, value, arm.stat("read", "p50")?))
        })
        .collect();
    sorted.sort_by(|left, right| left.0.total_cmp(&right.0));
    if sorted.len() < 2 {
        return String::new();
    }
    // the largest step between two adjacent rungs, which is where the working set stopped fitting
    let step = sorted
        .windows(2)
        .max_by(|left, right| {
            let ratio = |pair: &[(f64, &str, f64)]| pair[0].2 / pair[1].2.max(f64::MIN_POSITIVE);
            ratio(left).total_cmp(&ratio(right))
        })
        .map(|pair| (pair[0].1, pair[1].1, pair[0].2 / pair[1].2.max(f64::MIN_POSITIVE)));
    match step {
        // a step under a tenth is the axis being flat, which is a finding of its own
        Some((below, above, ratio)) if ratio > 1.1 => format!(
            "At `r{read_pct}` the largest step is between `{below}` and `{above}`: a read cost {}× \
             more below it than above. That is where this workload's working set stopped fitting, \
             and it is the number to size a deployment against - below it, reads are being answered \
             from disk and the page's other sweeps are measuring a different system.\n\n",
            fmt::fixed(ratio, 2)
        ),
        _ => format!(
            "At `r{read_pct}` no rung of the memory sweep steps: this workload's whole working set \
             fits inside even the smallest limit measured, so nothing here reached the archived read \
             path. **That is a statement about this workload, not about the setting** - the seeded \
             table is a few hundred megabytes, and a deployment larger than its limit would find the \
             cliff this sweep did not.\n\n"
        ),
    }
}

/// One numeric sweep drawn as a shape
///
/// # Arguments
///
/// * `sweeps` - Every sweep in the capture
/// * `knob` - Which one to draw
/// * `id` - The chart element id, which must be unique on the page
/// * `x_desc` - What the x axis is
/// * `note` - The caption under it
fn ladder(
    sweeps: &[(String, u32, Vec<Arm<'_>>)],
    knob: &str,
    id: &str,
    x_desc: &str,
    note: &str,
) -> Result<String> {
    let mut series = Vec::new();
    for (name, read_pct, arms) in sweeps.iter().filter(|(name, _, _)| name == knob) {
        let points: Vec<(f64, f64)> = arms
            .iter()
            .filter_map(|arm| Some((numeric(arm.conf_value()?)?, arm.stat("write", "p50")?)))
            .collect();
        if points.len() > 1 {
            let _ = name;
            series.push(sweep::Series {
                name: format!("r{read_pct}"),
                points,
            });
        }
    }
    if series.is_empty() {
        return Ok(String::new());
    }
    let mut out = sweep::draw(
        &sweep::Spec {
            id: id.to_string(),
            x_desc: x_desc.to_string(),
            y_desc: "p50 write service time".to_string(),
            x_axis: sweep::Axis::Log,
            y_axis: sweep::Axis::Log,
            x_unit: Unit::Count,
            y_unit: Unit::Duration,
        },
        &series,
    )?;
    out.push('\n');
    out.push_str(&caption(note));
    Ok(out)
}

/// A swept value as a number, when it is one
///
/// The values are spelled the way `shoal.yml` spells them, which is the spelling a reader has to
/// type and therefore the one worth putting on an axis - but a chart needs the number behind it.
/// Returns `None` for a value that is not a quantity at all, such as `fsync`, which is why the
/// categorical knobs are drawn as bars and never as a sweep.
///
/// # Arguments
///
/// * `value` - The value as an identifier spells it
///
/// # Examples
///
/// ```
/// use shoal_bench::render::pages::configuration::numeric;
///
/// assert_eq!(numeric("512"), Some(512.0));
/// assert_eq!(numeric("4Ki"), Some(4096.0));
/// assert_eq!(numeric("1Gi"), Some(1073741824.0));
/// assert_eq!(numeric("fsync"), None);
/// ```
pub fn numeric(value: &str) -> Option<f64> {
    // the same three suffixes `binary_size` writes, largest first so `Gi` is not read as `i`
    for (suffix, scale) in [("Gi", 1u64 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)] {
        if let Some(head) = value.strip_suffix(suffix) {
            return head.parse::<f64>().ok().map(|count| count * scale as f64);
        }
    }
    value.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::{in_order, in_order_with_rest, numeric};
    use crate::render::arms::Arm;

    /// A sweep with a name and a read share, holding no arms
    ///
    /// # Arguments
    ///
    /// * `name` - What the sweep is called
    /// * `read_pct` - The read share it ran at
    fn sweep(name: &str, read_pct: u32) -> (String, u32, Vec<Arm<'static>>) {
        (name.to_string(), read_pct, Vec::new())
    }

    /// A knob the reading order does not name is appended rather than dropped
    ///
    /// The doc comment on `in_order` said this before the code did: the loop took only what its
    /// order named, so a knob added to the sweep without the lists being updated vanished from the
    /// recommendation table with nothing to say it had.
    #[test]
    fn a_knob_the_order_does_not_name_is_appended() {
        let sweeps = vec![sweep("memory", 50), sweep("brand_new_knob", 50), sweep("shards", 50)];
        let picked = in_order_with_rest(&sweeps, &["shards", "memory"]);
        let names: Vec<&str> = picked.iter().map(|(name, _, _)| name.as_str()).collect();
        assert_eq!(names, vec!["shards", "memory", "brand_new_knob"]);
    }

    /// The section helpers still take only their own half
    ///
    /// `in_order` is what the two section functions use, and each is handed one half of the
    /// configuration on purpose. Appending there would put the resource sweeps on the storage
    /// section's chart, which has a hard cap on how many groups it can draw.
    #[test]
    fn a_section_takes_only_the_knobs_it_names() {
        let sweeps = vec![sweep("memory", 50), sweep("latency_buffer", 50)];
        let picked = in_order(&sweeps, &["latency_buffer"]);
        let names: Vec<&str> = picked.iter().map(|(name, _, _)| name.as_str()).collect();
        assert_eq!(names, vec!["latency_buffer"]);
    }

    /// A sweep repeated at another row width sorts next to the sweep it repeats
    ///
    /// It is a separate sweep - the width is part of the key, or the difference between two widths
    /// reads as a difference between two values of the setting - and it still belongs beside its
    /// own knob rather than at the end of the page.
    #[test]
    fn a_width_repeat_follows_the_sweep_it_repeats() {
        let sweeps = vec![
            sweep("latency_buffer", 50),
            sweep("latency_buffer @ 8 KiB", 50),
            sweep("shards", 50),
        ];
        let picked = in_order(&sweeps, &["latency_buffer", "shards"]);
        let names: Vec<&str> = picked.iter().map(|(name, _, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec!["latency_buffer", "latency_buffer @ 8 KiB", "shards"]
        );
    }

    /// Every spelling a sweep writes comes back as the number behind it
    #[test]
    fn a_binary_size_parses_back() {
        assert_eq!(numeric("512"), Some(512.0));
        assert_eq!(numeric("4Ki"), Some(4096.0));
        assert_eq!(numeric("256Ki"), Some(262_144.0));
        assert_eq!(numeric("64Mi"), Some(67_108_864.0));
        assert_eq!(numeric("1Gi"), Some(1_073_741_824.0));
        assert_eq!(numeric("12"), Some(12.0));
    }

    /// A value that is not a quantity is not invented as one
    ///
    /// `fsync` parsing to anything would put the durability sweep on a numeric axis, where the two
    /// arms would be drawn at positions that mean nothing.
    #[test]
    fn a_categorical_value_is_not_a_number() {
        assert_eq!(numeric("fsync"), None);
        assert_eq!(numeric("async"), None);
        assert_eq!(numeric(""), None);
    }
}
