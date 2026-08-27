//! What the explorer is currently showing, and the panels that change it
//!
//! # What it opens on
//!
//! Throughput against the read share, for the most recent capture that measured anything, as a
//! line. That is the interactive counterpart of `chart-grid-throughput` on the book's grid page,
//! and it is a real chart on sight rather than an empty one waiting to be configured. No second
//! capture is selected, so nothing is being compared until somebody asks for a comparison.
//!
//! # What the reader changes
//!
//! The metric, the key axis, whether it is lines or bars, which workloads, and **which captures** -
//! any number of them. That last one is the point of the whole crate: a curve redrawn beside the
//! one before it says far more than two numbers in a terminal do.

use crate::index::{
    Axis, AxisUnits, Capture, Chart, FamilyText, Index, Layer, Metric, MetricKind, Percentile,
    Selection, SweepAxis,
};
use crate::preset::Preset;
use crate::{picker, plot, prose, theme};

/// Everything the explorer is showing, and everything a panel may change
pub struct Explorer {
    /// Where the numbers come from
    pub index: Index,
    /// The substring the workload picker is filtered by
    ///
    /// Matched as a case insensitive substring of the identifier, which is what a positional filter
    /// means everywhere else in `shoal-bench`. One idiom for the whole tool.
    pub filter: String,
    /// Which workloads are drawn, in the order they were ticked
    pub workloads: Vec<u32>,
    /// Which captures are drawn
    ///
    /// A list, not a pair. Drawing an arbitrary number of runs together is the feature.
    pub captures: Vec<u32>,
    /// What is on the value axis
    pub metric: Metric,
    /// What is on the key axis
    pub axis: Axis,
    /// Whether the series are drawn as lines or as bars
    pub chart: Chart,
    /// Whether the value axis is logarithmic
    ///
    /// Off by default. A logarithmic axis makes a five percent regression invisible, and that is
    /// most of what somebody opens this to look for.
    pub log_y: bool,
    /// Whether the key axis is logarithmic
    ///
    /// Only meaningful for a line drawn against a swept fact - bars and timelines both sit at
    /// synthetic integer positions, where a logarithm means nothing. Off by default, and it has to
    /// be: the read share runs from zero, which a logarithmic axis cannot place at all.
    pub log_x: bool,
    /// What the last change to the selection did to it, while it is still worth saying
    ///
    /// Set when changing the metric or the axis drops workloads that no longer share units with
    /// the chart, and cleared by the next thing the reader ticks. Without it a selection would
    /// quietly shrink between two frames with nothing to say why.
    pub note: Option<String>,
    /// Whether the picker is open
    ///
    /// Open on first sight, because the reader has to select something before there is anything to
    /// read. Shut, the chart takes the whole width, which is the state somebody who has finished
    /// selecting and started reading wants.
    pub picker_open: bool,
    /// What the chart is currently framed for
    ///
    /// `None` before the first frame, which is the state in which anything drawn is new. Compared
    /// rather than hashed: the whole point is to be exactly as sensitive as [`Framing`]'s fields
    /// say, and a hash would make a collision into a chart that quietly stopped reframing.
    framing: Option<Framing>,
    /// Whether the chart is to be framed on its data before the next frame is drawn
    ///
    /// Set when [`Framing`] moves, and by the `fit` button. Cleared the moment it has been passed
    /// to the chart, so a reader's own pan or zoom sticks until the next thing that changes what is
    /// on screen.
    refit: bool,
}

/// What the chart is currently framed for
///
/// Every input that decides what is on the chart, and nothing that decides how it looks once it is
/// there. `egui_plot` remembers a zoom against the plot's id, and the id is a constant, so without
/// this a chart framed on a rate would still be framed on that rate after the reader moved to a p99
/// - which draws as an empty chart four orders of magnitude away from its own data.
///
/// The two log toggles are in here even though they are presentation, because both rescale the
/// axis they apply to and a frame fitted to one is not a frame fitted to the other.
#[derive(Debug, Clone, PartialEq)]
struct Framing {
    /// Which captures are drawn
    captures: Vec<u32>,
    /// Which workloads are drawn, in tick order - which decides the units, and so the selection
    workloads: Vec<u32>,
    /// What is on the value axis
    metric: Metric,
    /// What is on the key axis
    axis: Axis,
    /// Whether the series are drawn as lines or as bars
    chart: Chart,
    /// Whether the value axis is logarithmic
    log_y: bool,
    /// Whether the key axis is logarithmic
    log_x: bool,
}

impl Explorer {
    /// Opens the explorer on the most recent capture's throughput against the read share
    ///
    /// # Arguments
    ///
    /// * `index` - The corpus to draw from
    pub fn new(index: Index) -> Self {
        // the newest capture that measured a workload at all. the four most recent captures in the
        // corpus carry only a micro layer, so this is not simply the last one
        let newest = index.newest_with(Layer::Macro);
        let mut explorer = Explorer {
            index,
            filter: String::new(),
            workloads: Vec::new(),
            captures: newest.into_iter().collect(),
            metric: Metric::OpsPerSec,
            axis: Axis::Sweep(SweepAxis::ReadShare),
            chart: Chart::Line,
            log_y: false,
            log_x: false,
            note: None,
            picker_open: true,
            // nothing has been framed yet, so the first frame frames itself - which is what
            // `egui_plot`'s own default already did
            framing: None,
            refit: false,
        };
        // the first preset, which is the book's grid chart. a real chart on sight rather than an
        // empty one waiting to be configured
        if let Some(preset) = Preset::all().into_iter().next() {
            explorer.apply(&preset);
        }
        explorer
    }

    /// Puts the explorer into the state one of the book's charts is drawn in
    ///
    /// The arms are selected within the newest capture that measured anything, because which arms
    /// exist is a property of a capture rather than of the corpus.
    ///
    /// # Arguments
    ///
    /// * `preset` - The chart to reproduce
    pub fn apply(&mut self, preset: &Preset) {
        self.metric = preset.metric.clone();
        self.axis = preset.axis;
        self.chart = preset.chart;
        self.log_x = preset.log_x;
        self.log_y = preset.log_y;
        // the capture to select within: whichever one is already ticked, or the newest that
        // measured anything if the reader has unticked them all
        let within = self
            .captures
            .iter()
            .max()
            .copied()
            .or_else(|| self.index.newest_with(Layer::Macro));
        self.workloads = match within {
            Some(capture) => {
                if self.captures.is_empty() {
                    self.captures.push(capture);
                }
                (preset.select)(&self.index, capture)
            }
            None => Vec::new(),
        };
        // a preset is a whole new chart, so nothing said about the last one still applies
        self.note = None;
        // a preset names its own metric, and the arms it picked have to be able to answer it
        self.ensure_metric_is_offered();
    }

    /// What the chart would have to be framed for to be showing what is selected now
    fn framing(&self) -> Framing {
        Framing {
            captures: self.captures.clone(),
            workloads: self.workloads.clone(),
            metric: self.metric.clone(),
            axis: self.axis,
            chart: self.chart,
            log_y: self.log_y,
            log_x: self.log_x,
        }
    }

    /// The units the chart is currently drawn in, taken from the first ticked workload
    ///
    /// `None` when nothing is ticked, which is the state in which anything may be ticked next.
    pub fn anchor(&self) -> Option<AxisUnits> {
        self.workloads
            .iter()
            .find_map(|at| self.index.workload_units(*at, &self.metric, self.axis))
            .filter(|units| units.key.is_some())
    }

    /// Drops the workloads that no longer belong on the chart, and says how many
    ///
    /// Called after the metric or the key axis moves. Both change what the units are, so a
    /// selection that agreed a moment ago may not any more - a set of grid arms is one chart at a
    /// rate and several at a percentile, if some of them were stamped per batch.
    fn reconcile(&mut self) {
        let Some(anchor) = self.anchor() else {
            return;
        };
        let before = self.workloads.len();
        // the first ticked workload set the units, so it is the one everything is kept against
        self.workloads.retain(|at| {
            self.index
                .workload_units(*at, &self.metric, self.axis)
                .is_some_and(|units| units.differs_from(&anchor).is_none())
        });
        let dropped = before - self.workloads.len();
        // said out loud rather than left to be noticed: a selection shrinking between two frames
        // looks like a bug even when it is the only honest answer
        self.note = match dropped {
            0 => None,
            1 => Some("One workload was dropped: it is not in the units this chart is now drawn \
                       in.".to_string()),
            found => Some(format!(
                "{found} workloads were dropped: they are not in the units this chart is now \
                 drawn in."
            )),
        };
    }

    /// What the reader has currently asked to see
    pub fn selection(&self) -> Selection {
        Selection {
            captures: self.captures.clone(),
            workloads: self.workloads.clone(),
            metric: self.metric.clone(),
            axis: self.axis,
        }
    }

    /// The families whose prose explains what is currently selected
    ///
    /// In declaration order rather than selection order, so the panel reads the same way twice.
    pub fn families(&self) -> Vec<&FamilyText> {
        let mut seen: Vec<u32> = self
            .workloads
            .iter()
            .filter_map(|at| self.index.workloads.get(*at as usize))
            .filter_map(|workload| workload.family)
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen.into_iter()
            .filter_map(|at| self.index.families.get(at as usize))
            .collect()
    }

    /// Draws the toolbar, which is everything about *how* the numbers are shown
    ///
    /// # Arguments
    ///
    /// * `ui` - Where to draw it
    fn toolbar(&mut self, ui: &mut egui::Ui) {
        // the two charts the book already draws, each one a button rather than sixty four
        // checkboxes. drawn above the controls, because applying one moves every control below it
        ui.horizontal_wrapped(|ui| {
            // the two things that change how much of the window the chart gets, kept together and
            // ahead of everything that changes what is on it
            ui.toggle_value(&mut self.picker_open, "☰")
                .on_hover_text("show or hide the workload and capture picker");
            egui::global_theme_preference_switch(ui);
            ui.separator();
            ui.weak("redraw:");
            for preset in Preset::all() {
                if ui
                    .button(preset.name)
                    .on_hover_text(format!("the chart on {}", preset.page))
                    .clicked()
                {
                    self.apply(&preset);
                }
            }
        });
        ui.separator();
        // whether either axis moved, which is what decides if the selection has to be reconciled
        let mut moved = false;
        ui.horizontal_wrapped(|ui| {
            // what is on the value axis, as three lists rather than one. see `value_axis`
            let before = self.metric.clone();
            self.value_axis(ui);
            moved |= self.metric != before;
            ui.separator();
            // what is on the key axis. the two answer different questions, so both are offered
            // rather than one being derived from the other
            let before = self.axis;
            egui::ComboBox::from_label("against")
                .selected_text(match self.axis {
                    Axis::Sweep(sweep) => sweep.label().to_string(),
                    Axis::Timeline => "capture, oldest first".to_string(),
                })
                .show_ui(ui, |ui| {
                    for sweep in SweepAxis::ALL {
                        ui.selectable_value(&mut self.axis, Axis::Sweep(sweep), sweep.label());
                    }
                    ui.selectable_value(&mut self.axis, Axis::Timeline, "capture, oldest first");
                });
            moved |= self.axis != before;
            ui.separator();
            // lines or bars
            ui.selectable_value(&mut self.chart, Chart::Line, "line");
            ui.selectable_value(&mut self.chart, Chart::Bars, "bars");
            ui.separator();
            ui.checkbox(&mut self.log_y, "log value axis");
            // a logarithm of a bar slot or a capture position is a logarithm of a position in a
            // list, so the key axis is only offered one where it is a real quantity
            ui.add_enabled_ui(self.key_axis_is_a_quantity(), |ui| {
                ui.checkbox(&mut self.log_x, "log key axis")
                    .on_disabled_hover_text(
                        "the key axis is a position in a list here, not a quantity",
                    );
            });
            ui.separator();
            // the chart frames itself whenever what is on it changes, so this is for the other case:
            // a reader who panned or zoomed and wants the whole of it back without hunting for the
            // double click that also does it
            if ui
                .button("fit")
                .on_hover_text("frame the chart on the data it is drawing")
                .clicked()
            {
                self.refit = true;
            }
            ui.separator();
            ui.weak(plot::direction(&self.metric));
        });
        // a metric or an axis that moved changes what the units are, so the selection has to be
        // read against them again
        if moved {
            self.reconcile();
        }
    }

    /// Draws the metric control, which is three lists rather than one
    ///
    /// # Why three
    ///
    /// A flat list is the five whole-workload numbers plus every operation the corpus recorded
    /// crossed with seven ranks - thirty three entries against the committed corpus, of which any
    /// one workload can answer at most fourteen and most can answer twelve. Finding the live one
    /// was guesswork. Split, no list is ever longer than seven, and the operation and the rank are
    /// chosen separately because they are separate questions.
    ///
    /// Every list holds only what [`Index::metrics_for`] says the ticked workloads can answer, so a
    /// dead choice is not offered rather than being offered and drawing nothing.
    ///
    /// # Arguments
    ///
    /// * `ui` - Where to draw it
    fn value_axis(&mut self, ui: &mut egui::Ui) {
        // what everything ticked can answer. with nothing ticked this is the whole corpus, which is
        // the state in which any metric may still be chosen
        let offered = self.index.metrics_for(&self.workloads);
        // which kinds survive that, in the order `MetricKind::ALL` declares them
        let kinds: Vec<MetricKind> = MetricKind::ALL
            .into_iter()
            .filter(|kind| offered.iter().any(|metric| metric.kind() == *kind))
            .collect();
        let mut kind = self.metric.kind();
        egui::ComboBox::from_label("metric")
            .selected_text(kind.label())
            .show_ui(ui, |ui| {
                for found in &kinds {
                    ui.selectable_value(&mut kind, *found, found.label());
                }
            });
        // a kind that moved has to be resolved back to a whole metric, and a latency needs an
        // operation and a rank picked for it before it is one
        if kind != self.metric.kind() {
            if let Some(found) = offered.iter().find(|metric| metric.kind() == kind) {
                self.metric = found.clone();
            }
        }
        // the two lists that only a latency opens. `Spread` is a percentage of a wall clock and
        // `WallClock` is one number for the whole workload, so neither has an operation
        let Metric::Latency { op, percentile } = self.metric.clone() else {
            return;
        };
        // every operation the ticked workloads all recorded, in the order the corpus lists them
        let mut ops: Vec<&str> = offered
            .iter()
            .filter_map(|metric| match metric {
                Metric::Latency { op, .. } => Some(op.as_str()),
                _ => None,
            })
            .collect();
        ops.dedup();
        let mut chosen = op.clone();
        egui::ComboBox::from_label("of")
            .selected_text(&chosen)
            .show_ui(ui, |ui| {
                for found in &ops {
                    ui.selectable_value(&mut chosen, (*found).to_string(), *found);
                }
            });
        // every rank the chosen operation was recorded at, which is seven in practice and is read
        // from the corpus anyway rather than assumed
        let ranks: Vec<Percentile> = offered
            .iter()
            .filter_map(|metric| match metric {
                Metric::Latency { op, percentile } if *op == chosen => Some(*percentile),
                _ => None,
            })
            .collect();
        // the rank the reader was already on, kept when the new operation has it, because changing
        // which operation is read is not a request to change which rank of it
        let mut rank = match ranks.contains(&percentile) {
            true => percentile,
            false => *ranks.first().unwrap_or(&percentile),
        };
        egui::ComboBox::from_label("at")
            .selected_text(rank.as_str())
            .show_ui(ui, |ui| {
                for found in &ranks {
                    ui.selectable_value(&mut rank, *found, found.as_str());
                }
            });
        self.metric = Metric::Latency {
            op: chosen,
            percentile: rank,
        };
    }

    /// Snaps the metric back onto the list when the selection stops offering it
    ///
    /// Ticking cannot cause this - the picker only shows workloads that answer the current metric,
    /// so the intersection can only grow - but `clear selection` and a preset both move the set
    /// under it, and a combo box whose selected text is not one of its entries is a control that
    /// lies about what is drawn.
    fn ensure_metric_is_offered(&mut self) {
        let offered = self.index.metrics_for(&self.workloads);
        // nothing offered at all means nothing is ticked that carries any measurement, which the
        // chart says for itself rather than being papered over here
        if offered.is_empty() || offered.contains(&self.metric) {
            return;
        }
        // said out loud, for the same reason `reconcile` says what it dropped: a control moving on
        // its own between two frames looks like a bug even when it is the only honest answer
        if let Some(first) = offered.first() {
            self.note = Some(format!(
                "The metric moved to {}: nothing selected carries a {} measurement.",
                first.axis_label(),
                self.metric.axis_label()
            ));
            self.metric = first.clone();
        }
    }

    /// Whether the key axis is a real quantity rather than a position in a list
    ///
    /// Bars sit in groups whose position is computed from the group index, and a timeline's key is
    /// the capture's position among the ones drawn. Neither has a logarithm.
    fn key_axis_is_a_quantity(&self) -> bool {
        matches!((self.chart, self.axis), (Chart::Line, Axis::Sweep(_)))
    }

    /// Draws the strip that says when the selected captures cannot be read against each other
    ///
    /// It never refuses to draw them. The numbers exist and somebody will go looking for them; what
    /// would be wrong is joining them silently, which is how a governor change gets read as a
    /// regression.
    ///
    /// # Arguments
    ///
    /// * `ui` - Where to draw it
    fn warnings(&self, ui: &mut egui::Ui) {
        // what the chart is in, said before a reader has to work it out from the tick labels.
        // `anchor` only ever answers with a workload that is on the axis, so the key is always there
        if let Some((units, key)) = self.anchor().and_then(|units| {
            let key = units.key?;
            Some((units, key))
        }) {
            ui.weak(format!(
                "{} against {}, at the {} scale.",
                plot::unit_name(units.value),
                key.phrase(),
                units.scale
            ));
        }
        // the palette the ui is being drawn in, read once for the whole strip
        let palette = theme::palette(ui);
        // what the last change to the metric or the axis did to the selection
        if let Some(note) = &self.note {
            ui.colored_label(palette.muted, note);
        }
        // the environments the selected captures were taken in
        let differs = self.index.incomparable(&self.captures);
        if !differs.is_empty() {
            ui.colored_label(
                palette.warn,
                format!(
                    "These captures were not taken in the same place: {} differ. The difference \
                     between their numbers is not a result.",
                    differs.join(", ")
                ),
            );
        }
        // a capture that no longer describes the current tree is still worth drawing, and still
        // worth saying so about
        let stale: Vec<&str> = self
            .captures
            .iter()
            .filter_map(|at| self.index.captures.get(*at as usize))
            .filter(|capture| capture.code.iter().any(|(_, verdict)| verdict.current == Some(false)))
            .map(|capture| capture.label.as_str())
            .collect();
        if !stale.is_empty() {
            ui.colored_label(
                palette.muted,
                format!(
                    "{} no longer describes the current tree.",
                    stale.join(", ")
                ),
            );
        }
        // a filtered capture is missing arms rather than carrying unchanged ones
        let partial: Vec<&str> = self
            .captures
            .iter()
            .filter_map(|at| self.index.captures.get(*at as usize))
            .filter(|capture| capture.partial)
            .map(|capture| capture.label.as_str())
            .collect();
        if !partial.is_empty() {
            ui.colored_label(
                palette.muted,
                format!(
                    "{} measured only part of the registry, so its missing arms are absent rather \
                     than unchanged.",
                    partial.join(", ")
                ),
            );
        }
    }

    /// Draws one frame of the whole explorer
    ///
    /// # Arguments
    ///
    /// * `ui` - The root ui to draw into
    pub fn ui(&mut self, ui: &mut egui::Ui) {
        // what is drawn, on the left; how it is drawn, along the top; how to read it, along the
        // bottom; the chart in what is left. the order matters to egui: the first panel added is
        // the outermost, and the central one is always added last
        //
        // the flag is copied out and written back because the panel wants `&mut bool` while the
        // closure inside it wants `&mut self`, and one borrow cannot be both
        let mut open = self.picker_open;
        egui::Panel::left("picker")
            .default_size(340.0)
            .resizable(true)
            .show_collapsible(ui, &mut open, |ui| picker::draw(ui, self));
        self.picker_open = open;
        // the picker is the only thing that changes the selection, and the selection is what
        // decides which metrics are offered at all
        self.ensure_metric_is_offered();
        egui::Panel::top("toolbar").show(ui, |ui| self.toolbar(ui));
        // no panel at all when nothing is ticked, rather than an empty bar with a separator on it
        // and nothing to open
        let families = self.families();
        if !families.is_empty() {
            egui::Panel::bottom("prose").show(ui, |ui| prose::draw(ui, &families));
        }
        // no scroll area here, deliberately. one would make the available height infinite, and a
        // chart sized from an infinite height is a chart that has to be given a fixed one
        // what is on the chart, against what it was framed for. done here rather than in each
        // control, because the picker, the toolbar and `ensure_metric_is_offered` can all move it
        // and only one of them is the reader's own click
        let framing = self.framing();
        if self.framing.as_ref() != Some(&framing) {
            self.refit = true;
            self.framing = Some(framing);
        }
        egui::CentralPanel::default_margins().show(ui, |ui| {
            self.warnings(ui);
            let selection = self.selection();
            // a log key axis only reaches the chart where the key axis is a quantity, so a
            // toggle left on from a line chart does not follow the reader into bar mode
            let view = plot::View {
                chart: self.chart,
                log_y: self.log_y,
                log_x: self.log_x && self.key_axis_is_a_quantity(),
                refit: self.refit,
            };
            plot::draw(ui, &self.index, &selection, view);
        });
        // spent, so the next frame leaves whatever the reader does to the chart alone
        self.refit = false;
    }
}

/// How a capture should be described in a tooltip
///
/// # Arguments
///
/// * `capture` - The capture to describe
pub fn capture_tooltip(capture: &Capture) -> String {
    // everything that decides whether this capture may be read against another one, in one place
    let mut lines = vec![
        format!("taken {}", capture.captured),
        format!("commit {}", if capture.head_short.is_empty() { "unrecorded" } else { &capture.head_short }),
        format!("host {}", capture.host),
        format!("governor {}", capture.governor),
        format!("rustc {}", capture.rustc),
        format!("environment {}", capture.env.label),
    ];
    // the per layer verdicts, which is what says whether the numbers still describe the tree
    for (layer, verdict) in &capture.code {
        lines.push(format!("{layer:?} - {}", verdict.label));
    }
    if capture.dirty {
        lines.push("taken on a dirty tree".to_string());
    }
    lines.join("\n")
}

// The `eframe` glue, which is the same for a window and for a canvas. Gated on eframe being linked
// at all, because `shoal-bench` enters this crate with only the index types.
#[cfg(any(feature = "native", feature = "web"))]
impl eframe::App for Explorer {
    /// Draws one frame
    ///
    /// # Arguments
    ///
    /// * `ui` - The root ui for this viewport
    /// * `frame` - The window this is drawn into, which the explorer does not use
    fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        // nothing here touches the window itself - no title changes, no screenshots, no storage
        let _ = frame;
        Explorer::ui(self, ui);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A framing to move one field of
    fn framing() -> Framing {
        Framing {
            captures: vec![0],
            workloads: vec![1, 2],
            metric: Metric::OpsPerSec,
            axis: Axis::Sweep(SweepAxis::ReadShare),
            chart: Chart::Line,
            log_y: false,
            log_x: false,
        }
    }

    /// Every input that changes what is on the chart is one the chart is reframed for
    #[test]
    fn each_input_that_changes_the_chart_is_a_different_framing() {
        let base = framing();
        // one edit each, and every one of them has to be a different framing - a field left out of
        // the comparison is a chart that keeps a frame fitted to numbers no longer on it
        let moved = [
            Framing { captures: vec![0, 1], ..base.clone() },
            Framing { workloads: vec![1], ..base.clone() },
            Framing { metric: Metric::Spread, ..base.clone() },
            Framing { axis: Axis::Timeline, ..base.clone() },
            Framing { chart: Chart::Bars, ..base.clone() },
            Framing { log_y: true, ..base.clone() },
            Framing { log_x: true, ..base.clone() },
        ];
        for found in moved {
            assert_ne!(found, base);
        }
        // and nothing else is: the same selection built twice must not reframe, or a reader's own
        // zoom would be thrown away on every frame
        assert_eq!(framing(), base);
    }

    /// Tick order is part of the framing, because it is part of what is drawn
    #[test]
    fn tick_order_is_part_of_the_framing() {
        // the first ticked workload sets the units and so decides which half of a mixed selection
        // survives, which makes the same set in another order a different chart
        let base = framing();
        let reordered = Framing { workloads: vec![2, 1], ..base.clone() };
        assert_ne!(reordered, base);
    }
}
