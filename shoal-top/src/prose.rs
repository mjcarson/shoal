//! The four blocks that say how to read a chart
//!
//! # Why these are here at all
//!
//! Every generated results page opens with four mandatory blocks - what it measures, how to read
//! it, what would make it wrong, and what it cannot tell you - declared once per family in
//! `shoal_bench::render::family::FAMILIES` and enforced by tests. They are the part of a results
//! page that a chart cannot carry, and an explorer that dropped them would be a downgrade wearing
//! an upgrade's clothes.
//!
//! So they are projected into the index and shown here. The explorer needs them *more* than the
//! pages do, not less: a page's arms were chosen by somebody who knew what the page was for, and
//! the explorer's are whatever the reader ticked.
//!
//! # Why they are folded, and what folding is not
//!
//! They live in a panel along the bottom, shut on first sight. Four blocks for the default family
//! wrap to more than the chart above them is tall, so left open they are most of the window on
//! every frame after the first time anybody has read them.
//!
//! Folding is not dropping, and the difference is what the always visible row carries: the header
//! names the family the chart is being read against, and the caveat about a selection spanning
//! several of them sits **above** the fold rather than inside it. That caveat is the one line
//! somebody can be harmed by not seeing, so it is the one line that is never behind a click.

use crate::index::FamilyText;

/// Draws the four blocks for every family the current selection touches
///
/// # Arguments
///
/// * `ui` - Where to draw them
/// * `families` - The families the selection covers, in reading order
pub fn draw(ui: &mut egui::Ui, families: &[&FamilyText]) {
    // nothing selected yet, so there is nothing to explain
    if families.is_empty() {
        return;
    }
    // a selection spanning several families is a chart explained several different ways, which is
    // worth saying out loud rather than stacking four panels and hoping - and worth saying it above
    // the fold, because it is the caveat a reader is harmed by not seeing
    if families.len() > 1 {
        ui.colored_label(
            crate::theme::palette(ui).warn,
            "The selection spans more than one family. Each was written to be read on its own, so \
             a chart mixing them needs all of the caveats below, not one of them.",
        );
    }
    let header = match families {
        [only] => format!("How to read this: {}", only.title),
        many => format!("How to read this: {} families selected", many.len()),
    };
    // shut on first sight and collapsible after that. egui remembers the state against the
    // header's own id, so changing the metric does not reclose a panel the reader opened
    egui::CollapsingHeader::new(header)
        .id_salt("shoal-top-prose")
        .default_open(false)
        .show(ui, |ui| {
            // a bottom panel grows to its contents, so six families opened at once would take the
            // window from the chart. capped against the viewport rather than against the panel's
            // own height, which is the thing being decided here
            let cap = ui.ctx().viewport_rect().height() * 0.45;
            egui::ScrollArea::vertical()
                .id_salt("shoal-top-prose-body")
                .max_height(cap)
                .show(ui, |ui| {
                    for family in families {
                        block(ui, family);
                    }
                });
        });
}

/// Draws one family's four blocks, in the order a results page prints them
///
/// # Arguments
///
/// * `ui` - Where to draw them
/// * `family` - The family to explain
fn block(ui: &mut egui::Ui, family: &FamilyText) {
    ui.strong(&family.title);
    ui.horizontal_wrapped(|ui| {
        ui.weak("Drawn on");
        ui.weak(&family.surface_title);
        ui.weak(format!("({})", family.surface_link));
    });
    ui.add_space(4.0);
    // the same four, in the same order, as `Family::preamble` prints them
    labelled(ui, "What this measures", &family.what_it_measures);
    labelled(ui, "How to read it", &family.how_to_read_it);
    labelled(ui, "What would make it wrong", &family.what_would_make_it_wrong);
    // last and never abbreviated. this is the one that earns its keep, and the explorer's freeform
    // selection makes it more necessary than the pages do
    labelled(ui, "What it cannot tell you", &family.what_it_cannot_say);
    ui.add_space(10.0);
}

/// Draws one titled block of prose
///
/// # Arguments
///
/// * `ui` - Where to draw it
/// * `title` - What the block is called
/// * `body` - The prose itself
fn labelled(ui: &mut egui::Ui, title: &str, body: &str) {
    ui.label(egui::RichText::new(title).strong().size(12.0));
    ui.label(body);
    ui.add_space(6.0);
}
