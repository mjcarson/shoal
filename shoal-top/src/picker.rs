//! What is drawn: the workload tree and the capture list
//!
//! # The filter
//!
//! A case insensitive substring of the workload identifier, which is exactly what a positional
//! filter means to `shoal-bench list` and `shoal-bench run`. One idiom across the tool, so the two
//! surfaces cannot quietly mean different things by the same word. A filter matching nothing says
//! so rather than showing an empty tree.
//!
//! # What may be ticked
//!
//! Only what can be drawn. A workload whose measurements carry no value for the current metric, or
//! whose axes are not in the units the chart is already in, is **hidden**, and a family with
//! nothing left to show goes with it. `clear selection` is how the units are changed, and moving
//! the metric is how the rest come back.
//!
//! That reverses F30's *disabled rather than hidden*, which was argued on the grounds that a
//! reader hunting for a workload they know exists has nowhere to look. What replaces the tooltip is
//! arithmetic that always adds up: every family header carries how many of its members are hidden,
//! and a line under the tree carries the total and the two reasons. Nothing is dropped silently;
//! what is given up is being told which reason applies to which row. The reasoning is on F31's page
//! in the book, `docs/src/features/metric-availability.md`.
//!
//! # The capture list
//!
//! Newest first, which is the order somebody looking for a regression reads in, and the reverse of
//! the order the index stores them in. Every capture carries its verdict as a word, never as a
//! colour alone, and the tooltip carries everything that decides comparability.
//!
//! A capture that carries no value for the current metric is **greyed rather than hidden**, which is
//! the opposite of what the tree above does with a workload in the same position. The asymmetry is
//! deliberate and the reason is the list: a capture is an identity a reader knows by name out of
//! twenty-seven of them, so one disappearing reads as a corpus that lost a run, while a workload
//! disappearing out of three hundred and seventy-four reads as a filter doing its job. It stays
//! tickable either way, and the arithmetic under the list totals what the metric is keeping off the
//! chart, the same way the tree's does.

use crate::app::{Explorer, capture_tooltip};
use crate::index::AxisUnits;
use crate::theme;

/// Draws the whole left hand panel
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `state` - What is currently selected, which this panel changes
pub fn draw(ui: &mut egui::Ui, state: &mut Explorer) {
    ui.add_space(4.0);
    ui.heading("Workloads");
    // the filter, matched the way every other filter in this tool is matched
    ui.add(
        egui::TextEdit::singleline(&mut state.filter)
            .hint_text("filter, as a substring of the identifier")
            .desired_width(f32::INFINITY),
    );
    ui.horizontal(|ui| {
        // the only way to change which units the chart is drawn in, since the first ticked
        // workload is what sets them
        if ui
            .button("clear selection")
            .on_hover_text("start again, in whatever units the next workload is in")
            .clicked()
        {
            state.workloads.clear();
            state.note = None;
        }
        ui.weak(format!("{} selected", state.workloads.len()));
    });
    ui.separator();
    egui::ScrollArea::vertical()
        .id_salt("workload-tree")
        .max_height(ui.available_height() * 0.55)
        .show(ui, |ui| workloads(ui, state));
    ui.separator();
    ui.heading("Captures");
    ui.weak("Tick more than one to compare them.");
    egui::ScrollArea::vertical()
        .id_salt("capture-list")
        .show(ui, |ui| captures(ui, state));
}

/// Draws the workload tree, grouped by the family whose prose explains each group
///
/// # What is not offered
///
/// A workload that cannot be drawn is not shown, and neither is a family with nothing left in it.
/// Two things hide one: its measurements carry no value for the current metric, or its axes are not
/// in the units the chart is already drawn in. The first ticked workload is what sets those units,
/// so `clear selection` is how you change your mind about them, and the metric control is how you
/// change your mind about the other.
///
/// Every count says how many it is not showing, and the line under the tree totals them, so a tree
/// that just lost two thirds of its rows says so.
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `state` - What is currently selected, which this panel changes
fn workloads(ui: &mut egui::Ui, state: &mut Explorer) {
    let needle = state.filter.to_lowercase();
    // group the matching workloads under the family that explains them, keeping the index order so
    // the tree does not reshuffle as the filter is typed
    let mut groups: Vec<(Option<u32>, Vec<u32>)> = Vec::new();
    for (at, workload) in state.index.workloads.iter().enumerate() {
        if !needle.is_empty() && !workload.id.to_lowercase().contains(&needle) {
            continue;
        }
        // find the group this workload belongs in, or start it
        match groups.iter_mut().find(|(family, _)| *family == workload.family) {
            Some((_, members)) => members.push(at as u32),
            None => groups.push((workload.family, vec![at as u32])),
        }
    }
    // a filter that selects nothing is said out loud. an empty tree looks like a broken one
    if groups.is_empty() {
        ui.weak("No workload matches that filter.");
        return;
    }
    // the units the chart is drawn in, read once for the whole tree rather than per checkbox
    let anchor = state.anchor();
    // how many rows the metric and the units between them are keeping off the tree, totalled
    // across every family so that the line under it can account for the whole corpus
    let mut hidden = 0usize;
    let mut shown = 0usize;
    for (family, members) in groups {
        // what this group is called, and whether anything explains it at all
        let title = match family.and_then(|at| state.index.families.get(at as usize)) {
            Some(found) => found.title.clone(),
            None => "Explained by no family".to_string(),
        };
        // why each member cannot be drawn, worked out before anything is ticked, so that the
        // header count and the checkboxes below cannot disagree
        let refusals: Vec<Option<String>> = members
            .iter()
            .map(|at| match state.workloads.contains(at) {
                // a workload that is already ticked always stays clickable, or there would be no
                // way to untick it once it had set the units
                true => None,
                false => refusal(state, *at, anchor.as_ref()),
            })
            .collect();
        // the members that survive both gates, in the order the tree shows them. a workload is
        // drawn only if it could be ticked, so the tree and the chart cannot disagree
        let offered: Vec<u32> = members
            .iter()
            .zip(refusals.iter())
            .filter(|(_, why)| why.is_none())
            .map(|(at, _)| *at)
            .collect();
        let held = members.len() - offered.len();
        hidden += held;
        shown += offered.len();
        // a family with nothing left to show is not drawn at all, which is the folder disappearing
        if offered.is_empty() {
            continue;
        }
        let picked = offered
            .iter()
            .filter(|at| state.workloads.contains(at))
            .count();
        // what was left out is on the header rather than only in the total, so a group that shrank
        // says it shrank where somebody is looking at it
        let count = match held {
            0 => format!("{picked}/{}", offered.len()),
            found => format!("{picked}/{}, {found} hidden", offered.len()),
        };
        egui::CollapsingHeader::new(format!("{title}  ({count})"))
            .id_salt(family.map_or(u32::MAX, |at| at))
            .show(ui, |ui| {
                // a family nothing explains is still drawn, and still flagged: it is a gap in the
                // documentation, not a reason to hide a measurement
                if family.is_none() {
                    ui.colored_label(
                        theme::palette(ui).warn,
                        "No results page explains these, so nothing below says how to read them.",
                    );
                }
                if ui.small_button("select all shown").clicked() {
                    select_all(state, &offered);
                }
                for at in &offered {
                    let Some(workload) = state.index.workloads.get(*at as usize) else {
                        continue;
                    };
                    let mut ticked = state.workloads.contains(at);
                    if ui.checkbox(&mut ticked, &workload.id).changed() {
                        // kept in tick order, so the colours a reader has learned do not shuffle
                        // when an unrelated workload is added
                        match ticked {
                            true => state.workloads.push(*at),
                            false => state.workloads.retain(|kept| kept != at),
                        }
                        // whatever the last change to the axes said about the selection is about a
                        // selection the reader has now changed
                        state.note = None;
                    }
                }
            });
    }
    // a tree with nothing on it is a state a reader has to be able to get out of, and the two ways
    // out are different, so the message names both rather than repeating the filter's
    if shown == 0 {
        ui.weak(format!(
            "No workload matching that filter has a {} measurement in these units. Change the \
             metric, or clear the selection.",
            state.metric.axis_label()
        ));
        return;
    }
    // what the two gates are keeping off the tree, so that hiding is never the same as dropping.
    // the count plus the ones drawn above is every workload the filter matched
    if hidden > 0 {
        ui.add_space(2.0);
        ui.weak(format!(
            "{hidden} hidden: they carry no {} measurement, or they are not in the units this \
             chart is drawn in.",
            state.metric.axis_label()
        ));
    }
}

/// Why one workload cannot be drawn beside what is already ticked, if it cannot
///
/// # Arguments
///
/// * `state` - What is currently selected
/// * `at` - Which workload, as an index into the index's workloads
/// * `anchor` - The units the chart is already drawn in, if anything is ticked
fn refusal(state: &Explorer, at: u32, anchor: Option<&AxisUnits>) -> Option<String> {
    // a workload the corpus carries an identifier for and no measurement of. it is real, and there
    // is nothing to draw for it, on this metric or on any other
    if !state.index.measured(at) {
        return Some("no capture in this corpus has measured it".to_string());
    }
    // measured, but not with this number in it. distinct from the line above because this one is
    // fixed by changing the metric and that one is not fixed by anything
    if !state.index.workload_answers(at, &state.metric) {
        return Some(format!(
            "it has no {} measurement",
            state.metric.axis_label()
        ));
    }
    let Some(units) = state.index.workload_units(at, &state.metric, state.axis) else {
        // both reasons above are already ruled out, so the only way left to get here is a scale row
        // the index does not carry, which is a corrupt projection rather than a state to draw
        return Some("its measurement names a scale this index does not carry".to_string());
    };
    match anchor {
        Some(head) => units.differs_from(head),
        // nothing is ticked, so the only thing that can refuse it is the axis itself
        None => units
            .key
            .is_none()
            .then(|| "it has no position on this axis".to_string()),
    }
}

/// Ticks every member of one group that may share the chart, and says how many could not
///
/// # Arguments
///
/// * `state` - What is currently selected, which this changes
/// * `members` - The group's workloads, in the order the tree shows them
fn select_all(state: &mut Explorer, members: &[u32]) {
    let mut anchor = state.anchor();
    let mut held = 0;
    for at in members {
        if state.workloads.contains(at) {
            continue;
        }
        if refusal(state, *at, anchor.as_ref()).is_some() {
            held += 1;
            continue;
        }
        state.workloads.push(*at);
        // the first one ticked is what sets the units for every one after it, which is the same
        // rule a reader clicking the boxes one at a time is under
        if anchor.is_none() {
            anchor = state.index.workload_units(*at, &state.metric, state.axis);
        }
    }
    // said out loud, because a button called `select all shown` that selected some of them is
    // otherwise indistinguishable from one that is broken
    state.note = match held {
        0 => None,
        1 => Some("One of those is in other units and was not selected.".to_string()),
        found => Some(format!(
            "{found} of those are in other units and were not selected."
        )),
    };
}

/// Draws the capture list, newest first
///
/// # What is greyed
///
/// A capture carrying no value for the current metric, judged over the ticked workloads - or over
/// everything it measured when nothing is ticked, which is the rule [`crate::index::Index::metrics_for`]
/// follows for the metric list. It is greyed, not hidden, and it stays tickable: see the module's
/// own note for why the tree above does the opposite with a workload.
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `state` - What is currently selected, which this panel changes
fn captures(ui: &mut egui::Ui, state: &mut Explorer) {
    // which captures carry the current metric at all, worked out once for the whole list rather
    // than once per row
    let answering = state.index.captures_answering(&state.metric, &state.workloads);
    // what that answer was taken over, which is the half of it a reader cannot guess from the row
    let reason = match state.workloads.is_empty() {
        true => format!(
            "Nothing in this capture answers {}.",
            state.metric.axis_label()
        ),
        false => format!(
            "Nothing you have ticked has a {} measurement in this capture.",
            state.metric.axis_label()
        ),
    };
    // the palette the list is drawn in, read once for every row in it
    let palette = theme::palette(ui);
    // newest first, which is the reverse of how the index stores them
    for at in (0..state.index.captures.len()).rev() {
        let Some(capture) = state.index.captures.get(at) else {
            continue;
        };
        // whether ticking this one would put anything at all on the chart
        let empty = !answering.get(at).copied().unwrap_or(false);
        let at = at as u32;
        let mut ticked = state.captures.contains(&at);
        // the worst verdict any of this capture's layers reached, which is what a one line badge
        // has to carry
        let standing = capture
            .code
            .iter()
            .find(|(_, verdict)| verdict.current != Some(true))
            .map(|(_, verdict)| verdict.label.clone());
        // one row is one line, so both qualifications are words appended to the label
        let mut label = capture.label.clone();
        if let Some(word) = &standing {
            label.push_str("  \u{b7}  ");
            label.push_str(word);
        }
        if empty {
            label.push_str("  \u{b7}  nothing on this metric");
        }
        // greying takes the colour where both apply: a stale capture still draws, and one that
        // answers nothing does not. never a colour alone - the words above carry both
        let text = match (empty, &standing) {
            (true, _) => egui::RichText::new(label).color(palette.muted),
            (false, Some(_)) => egui::RichText::new(label).color(palette.warn),
            (false, None) => egui::RichText::new(label),
        };
        // still tickable, greyed or not. it is how a reader lines a capture up before moving the
        // metric onto one it answers, and a ticked row has to stay clickable to be unticked
        let tooltip = match empty {
            true => format!("{reason}\n\n{}", capture_tooltip(capture)),
            false => capture_tooltip(capture),
        };
        let response = ui.checkbox(&mut ticked, text).on_hover_text(tooltip);
        if response.changed() {
            match ticked {
                true => state.captures.push(at),
                false => state.captures.retain(|kept| *kept != at),
            }
        }
    }
    // what the metric is keeping off the chart, totalled the way the tree above totals what it
    // hides. greying rows without the arithmetic beside them is what F31's page argues against
    let empty = answering.iter().filter(|found| !**found).count();
    if empty > 0 {
        ui.add_space(2.0);
        ui.weak(format!(
            "{empty} of {} have no {} measurement {}.",
            answering.len(),
            state.metric.axis_label(),
            match state.workloads.is_empty() {
                true => "at all",
                false => "for what is ticked",
            }
        ));
    }
}
