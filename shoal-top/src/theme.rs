//! The colours the explorer draws with, and what each one carries
//!
//! # Why these are real colours, and the book's are not
//!
//! `shoal_bench::render::chart::palette` holds *sentinel* values - `#FE0001` and its neighbours -
//! which mean nothing until `docs/theme/charts.css` maps each onto one of mdbook's theme variables.
//! That indirection exists because plotters emits fixed RGB into an SVG and the book has five
//! themes. egui has no such problem: it paints into a live context that already knows whether it is
//! light or dark, so these are the first colours in this repository that are simply colours.
//!
//! **Nothing here may be added to `render::chart::palette`.** `shoal-bench/tests/css_sync.rs`
//! asserts a bijection between that module's sentinels and the rules in `charts.css`, so a constant
//! added there without a stylesheet rule fails the test, and a rule added for a colour that never
//! reaches an SVG fails the other half of it. That holds for both palettes below, not just one.
//!
//! # What hue means, and what style means
//!
//! A chart can hold several captures, several tables and several curves of one table at once, and
//! their product is quickly larger than the number of hues anybody can tell apart. So identity is
//! split across three channels rather than two:
//!
//! - **Hue** carries the **table** - its fixed slot in `index::TABLE_ORDER`, so a table's colour is
//!   a property of the table rather than of what happens to be ticked beside it.
//! - **Dash pattern** carries the capture, on a sweep.
//! - **Marker** carries which of that table's curves this is.
//!
//! Which means two captures of the same curve are the same colour with the same marker in two
//! different dash patterns, and that is the comparison the explorer exists to make. A timeline has
//! no capture to encode - every series on it is a workload against the same set of captures - so
//! there the mark carries both channels, walking the markers before it moves the dash.
//!
//! [F32](../../docs/src/features/chart-line-identity.md) is where this replaced *hue carries the
//! series, dash carries the capture*, which is what it said until the number of tables on one chart
//! outgrew the number of hues a reader could hold.
//!
//! # Why there are two of everything
//!
//! The chrome is [One Dark] - the theme Helix, JetBrains and VS Code all ship a version of - and
//! the explorer opens on it, because it is read in a browser over a port forward beside a terminal
//! that is already dark. Light stays reachable through the switch in the toolbar.
//!
//! A single set of hues tuned to sit on both grounds is a set tuned for neither: `#61AFEF` is
//! washed out on `#FAFAFA` and `#4078F2` is heavy on `#282C34`. So there are two arrays, and
//! [`palette`] picks between them from the visuals the ui is already being drawn with. They hold
//! **the same eight hue roles in the same order**, so a curve does not change identity when the
//! reader flips the switch - only its shade does.
//!
//! [One Dark]: https://github.com/atom/atom/tree/master/packages/one-dark-ui

use egui::{Color32, Stroke, Theme, ThemePreference, Visuals};

/// The colours one theme draws a chart in
///
/// Selected by [`palette`] from the ui's own visuals rather than from a flag the caller carries, so
/// there is one place that decides which theme is in force and nowhere that can disagree with it.
pub struct Palette {
    /// The categorical series colours, in the order they are handed out
    ///
    /// Eight, chosen to stay distinguishable against this palette's background and to avoid the
    /// red/green pairing outright - it is the one categorical pair that fails for the commonest
    /// colour vision deficiencies, and a chart whose meaning is which line is above the other
    /// cannot afford it. The same reasoning is written out at `render::chart::palette::BETTER`.
    pub series: [Color32; 8],
    /// A warning that something on the chart is not a straightforward result
    pub warn: Color32,
    /// Something present but deliberately de-emphasised
    pub muted: Color32,
}

/// The eight hues against One Dark's `#282C34` ground
///
/// Blue, orange, violet, teal, magenta, olive, slate, clay - the first five are the strongly
/// separable tier, and most charts never reach the last three.
const DARK: Palette = Palette {
    series: [
        Color32::from_rgb(0x61, 0xAF, 0xEF), // blue
        Color32::from_rgb(0xD1, 0x9A, 0x66), // orange
        Color32::from_rgb(0xC6, 0x78, 0xDD), // violet
        Color32::from_rgb(0x56, 0xB6, 0xC2), // teal
        Color32::from_rgb(0xE0, 0x6C, 0x9A), // magenta
        Color32::from_rgb(0xC5, 0xC4, 0x6B), // olive
        Color32::from_rgb(0x90, 0xA0, 0xC0), // slate
        Color32::from_rgb(0xD8, 0x81, 0x5F), // clay
    ],
    // One Dark's yellow. its comment grey is `#5C6370`, which is legible as chrome and not as a
    // caption somebody is meant to read, so `muted` is a step brighter than the theme's own
    warn: Color32::from_rgb(0xE5, 0xC0, 0x7B),
    muted: Color32::from_rgb(0x7F, 0x84, 0x8E),
};

/// The same eight hue roles against One Light's `#FAFAFA` ground
const LIGHT: Palette = Palette {
    series: [
        Color32::from_rgb(0x40, 0x78, 0xF2), // blue
        Color32::from_rgb(0xB3, 0x6A, 0x20), // orange
        Color32::from_rgb(0xA6, 0x26, 0xA4), // violet
        Color32::from_rgb(0x01, 0x84, 0xBC), // teal
        Color32::from_rgb(0xC2, 0x27, 0x7A), // magenta
        Color32::from_rgb(0x7A, 0x7F, 0x1E), // olive
        Color32::from_rgb(0x4F, 0x6C, 0x99), // slate
        Color32::from_rgb(0xA3, 0x4F, 0x31), // clay
    ],
    warn: Color32::from_rgb(0xB3, 0x72, 0x1B),
    muted: Color32::from_rgb(0x6B, 0x70, 0x79),
};

/// The colour of the band drawn between a workload's fastest and slowest run
pub const INTERVAL_ALPHA: u8 = 48;

/// The palette for the theme this ui is being drawn in
///
/// # Arguments
///
/// * `ui` - The ui whose visuals decide the theme
pub fn palette(ui: &egui::Ui) -> &'static Palette {
    // read off the visuals rather than off a stored flag, so a theme switched mid frame is drawn
    // in the palette it switched to rather than the one the last frame used
    if ui.visuals().dark_mode { &DARK } else { &LIGHT }
}

/// The colour a series at this position is drawn in
///
/// # Arguments
///
/// * `palette` - The palette the chart is being drawn in
/// * `at` - The series' position among the ones being drawn
pub fn series(palette: &Palette, at: usize) -> Color32 {
    // wrapping rather than fading: a ninth series repeats a hue, and the dash pattern is what
    // still separates it from the one it shares with
    palette.series[at % palette.series.len()]
}

/// The marker shapes a line may be dotted with, to tell it from the others of its colour
///
/// **None of them is a plain circle.** A run of a single measurement is already drawn as a circle -
/// it is a real result with nothing to join it to - so a circle here would mean two things at once.
const MARKERS: [egui_plot::MarkerShape; 5] = [
    egui_plot::MarkerShape::Diamond,
    egui_plot::MarkerShape::Up,
    egui_plot::MarkerShape::Square,
    egui_plot::MarkerShape::Cross,
    egui_plot::MarkerShape::Asterisk,
];

/// How many lines of one colour are told apart before the dash pattern has to move as well
///
/// The bare line plus one per marker. A timeline has no capture to put in the dash pattern, so it
/// walks the markers first and moves the dash only once it has run out of them.
pub const MARK_CYCLE: usize = MARKERS.len() + 1;

/// The line style that stands for a capture at this position
///
/// # Arguments
///
/// * `at` - The capture's position among the ones being drawn
pub fn line_style(at: usize) -> egui_plot::LineStyle {
    // the first capture is solid, because a chart of one capture should not look dashed for a
    // comparison that is not being made
    match at % 3 {
        0 => egui_plot::LineStyle::Solid,
        1 => egui_plot::LineStyle::dashed_dense(),
        _ => egui_plot::LineStyle::Dotted { spacing: 6.0 },
    }
}

/// The marker that stands for a curve at this position within its colour
///
/// `None` for the first, so a chart drawing one line per table carries no markers at all rather
/// than looking decorated for a distinction nothing is making.
///
/// # Arguments
///
/// * `at` - The curve's position among the ones sharing its colour
pub fn series_marker(at: usize) -> Option<egui_plot::MarkerShape> {
    // the first curve of a colour is the bare line, and every one after it takes a shape
    at.checked_sub(1).map(|found| MARKERS[found % MARKERS.len()])
}

/// The same colour at the transparency an interval band is filled with
///
/// # Arguments
///
/// * `color` - The series colour to fade
pub fn faded(color: Color32) -> Color32 {
    // the band belongs to its line, so it is the same hue rather than a neutral grey - two bands
    // overlapping then still say which line each belongs to
    Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), INTERVAL_ALPHA)
}

/// Registers both themes and opens the explorer on the dark one
///
/// Called once, from the `eframe` creation closure on each path. Doing it per frame would fight the
/// switch in the toolbar: `set_theme` writes the *preference*, so re-writing it every frame would
/// undo the reader's choice as fast as they made it.
///
/// # Arguments
///
/// * `ctx` - The context to install into
pub fn install(ctx: &egui::Context) {
    // both are registered, so the switch has somewhere to go in either direction
    ctx.set_visuals_of(Theme::Dark, one_dark());
    ctx.set_visuals_of(Theme::Light, one_light());
    // and dark is what it opens on, rather than whatever `prefers-color-scheme` the reader's
    // browser happens to report - the explorer is read beside a terminal, not on its own
    ctx.set_theme(ThemePreference::Dark);
}

/// One Dark's chrome, as egui visuals
///
/// Built from `Visuals::dark` rather than from `Default`, because `Widgets` alone carries thirty
/// fields and only the ones named below are meant to move.
fn one_dark() -> Visuals {
    // the four grounds, darkest first. the plot surface is `extreme_bg_color`, which is what
    // `egui_plot` paints a chart's background with
    let surface = Color32::from_rgb(0x1E, 0x21, 0x27);
    let chrome = Color32::from_rgb(0x21, 0x25, 0x2B);
    let editor = Color32::from_rgb(0x28, 0x2C, 0x34);
    let raised = Color32::from_rgb(0x2C, 0x31, 0x3A);
    // the interaction accent, the border, and the three text weights
    let accent = Color32::from_rgb(0x61, 0xAF, 0xEF);
    let border = Color32::from_rgb(0x18, 0x1A, 0x1F);
    let text = Color32::from_rgb(0xAB, 0xB2, 0xBF);
    let bright = Color32::from_rgb(0xD7, 0xDA, 0xE0);
    let mut visuals = Visuals::dark();
    visuals.window_fill = editor;
    visuals.panel_fill = chrome;
    visuals.extreme_bg_color = surface;
    visuals.faint_bg_color = raised;
    visuals.code_bg_color = raised;
    visuals.window_stroke = Stroke::new(1.0, border);
    visuals.weak_text_color = Some(Color32::from_rgb(0x5C, 0x63, 0x70));
    visuals.hyperlink_color = accent;
    visuals.warn_fg_color = DARK.warn;
    visuals.error_fg_color = Color32::from_rgb(0xE0, 0x6C, 0x75);
    visuals.selection.bg_fill = Color32::from_rgb(0x3E, 0x44, 0x51);
    visuals.selection.stroke = Stroke::new(1.0, accent);
    // the five widget states, from the flat background of a label to a pressed button
    visuals.widgets.noninteractive.bg_fill = editor;
    visuals.widgets.noninteractive.weak_bg_fill = editor;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, border);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, text);
    visuals.widgets.inactive.bg_fill = raised;
    visuals.widgets.inactive.weak_bg_fill = Color32::from_rgb(0x3A, 0x3F, 0x4B);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(0x3E, 0x44, 0x51));
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, text);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(0x3E, 0x44, 0x51);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(0x3E, 0x44, 0x51);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, accent);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.5, bright);
    visuals.widgets.active.bg_fill = Color32::from_rgb(0x4B, 0x52, 0x63);
    visuals.widgets.active.weak_bg_fill = Color32::from_rgb(0x4B, 0x52, 0x63);
    visuals.widgets.active.bg_stroke = Stroke::new(1.5, accent);
    visuals.widgets.active.fg_stroke = Stroke::new(1.5, bright);
    visuals.widgets.open.bg_fill = raised;
    visuals.widgets.open.weak_bg_fill = raised;
    visuals.widgets.open.bg_stroke = Stroke::new(1.0, accent);
    visuals.widgets.open.fg_stroke = Stroke::new(1.0, text);
    visuals
}

/// One Light's chrome, as egui visuals
///
/// The same table as [`one_dark`] against the other ground. It exists so the switch in the toolbar
/// lands somewhere deliberate rather than on egui's stock light theme beside a hand tuned dark one.
fn one_light() -> Visuals {
    // the same four roles, inverted: the plot surface is the *lightest* here rather than the darkest
    let surface = Color32::from_rgb(0xFF, 0xFF, 0xFF);
    let chrome = Color32::from_rgb(0xEA, 0xEA, 0xEB);
    let editor = Color32::from_rgb(0xFA, 0xFA, 0xFA);
    let raised = Color32::from_rgb(0xF0, 0xF0, 0xF1);
    let accent = Color32::from_rgb(0x40, 0x78, 0xF2);
    let border = Color32::from_rgb(0xC9, 0xC9, 0xCA);
    let text = Color32::from_rgb(0x38, 0x3A, 0x42);
    let bright = Color32::from_rgb(0x1C, 0x1D, 0x21);
    let mut visuals = Visuals::light();
    visuals.window_fill = editor;
    visuals.panel_fill = chrome;
    visuals.extreme_bg_color = surface;
    visuals.faint_bg_color = raised;
    visuals.code_bg_color = raised;
    visuals.window_stroke = Stroke::new(1.0, border);
    visuals.weak_text_color = Some(Color32::from_rgb(0xA0, 0xA1, 0xA7));
    visuals.hyperlink_color = accent;
    visuals.warn_fg_color = Color32::from_rgb(0xC1, 0x84, 0x01);
    visuals.error_fg_color = Color32::from_rgb(0xE4, 0x56, 0x49);
    visuals.selection.bg_fill = Color32::from_rgb(0xD4, 0xE2, 0xFD);
    visuals.selection.stroke = Stroke::new(1.0, accent);
    visuals.widgets.noninteractive.bg_fill = editor;
    visuals.widgets.noninteractive.weak_bg_fill = editor;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, border);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, text);
    visuals.widgets.inactive.bg_fill = raised;
    visuals.widgets.inactive.weak_bg_fill = Color32::from_rgb(0xE5, 0xE5, 0xE6);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, border);
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, text);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(0xDC, 0xDC, 0xDE);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(0xDC, 0xDC, 0xDE);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, accent);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.5, bright);
    visuals.widgets.active.bg_fill = Color32::from_rgb(0xCF, 0xCF, 0xD2);
    visuals.widgets.active.weak_bg_fill = Color32::from_rgb(0xCF, 0xCF, 0xD2);
    visuals.widgets.active.bg_stroke = Stroke::new(1.5, accent);
    visuals.widgets.active.fg_stroke = Stroke::new(1.5, bright);
    visuals.widgets.open.bg_fill = raised;
    visuals.widgets.open.weak_bg_fill = raised;
    visuals.widgets.open.bg_stroke = Stroke::new(1.0, accent);
    visuals.widgets.open.fg_stroke = Stroke::new(1.0, text);
    visuals
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The explorer opens dark, whatever the browser reports
    #[test]
    fn the_explorer_opens_dark() {
        let ctx = egui::Context::default();
        // the stock preference is `System`, so this is a real change rather than a restatement
        assert_eq!(
            ctx.options(|options| options.theme_preference),
            ThemePreference::System
        );
        install(&ctx);
        assert_eq!(
            ctx.options(|options| options.theme_preference),
            ThemePreference::Dark
        );
    }

    /// Both themes are installed, so the switch in the toolbar has somewhere to go
    #[test]
    fn both_themes_are_registered() {
        let ctx = egui::Context::default();
        install(&ctx);
        let dark = ctx.style_of(Theme::Dark).visuals.panel_fill;
        let light = ctx.style_of(Theme::Light).visuals.panel_fill;
        assert_eq!(dark, Color32::from_rgb(0x21, 0x25, 0x2B));
        assert_eq!(light, Color32::from_rgb(0xEA, 0xEA, 0xEB));
        // a theme installed over the other one's values is a switch that does nothing
        assert_ne!(dark, light);
    }

    /// A curve's position means the same index in either theme
    #[test]
    fn the_two_palettes_have_the_same_arity() {
        // `series` wraps on the palette's own length, so two lengths would mean a curve changing
        // identity when the reader flips the switch rather than changing shade
        assert_eq!(DARK.series.len(), LIGHT.series.len());
    }

    /// No theme hands out one colour for two different series
    #[test]
    fn no_theme_repeats_a_series_colour() {
        for palette in [&DARK, &LIGHT] {
            let mut seen: Vec<Color32> = palette.series.to_vec();
            seen.sort_by_key(|color| color.to_array());
            seen.dedup();
            assert_eq!(seen.len(), palette.series.len());
        }
    }

    /// A dash pattern stands for a capture, and the first capture is not dashed
    #[test]
    fn a_line_style_stands_for_a_capture() {
        assert!(matches!(line_style(0), egui_plot::LineStyle::Solid));
        // the cycle is three long, so the fourth capture repeats the first
        assert_eq!(line_style(3), line_style(0));
        assert_ne!(line_style(1), line_style(0));
        assert_ne!(line_style(2), line_style(1));
    }

    /// A marker stands for a curve within its colour, and the first curve carries none
    #[test]
    fn a_marker_stands_for_a_curve_within_its_hue() {
        // a chart of one line per table is every mark zero, and must not look decorated
        assert_eq!(series_marker(0), None);
        // every shape in the cycle is handed out before any of them repeats
        let cycle: Vec<Option<egui_plot::MarkerShape>> =
            (1..=MARKERS.len()).map(series_marker).collect();
        let mut seen = cycle.clone();
        seen.dedup();
        assert_eq!(seen.len(), MARKERS.len());
        // and the cycle is the bare line plus one per shape, so it repeats there and not before
        assert_eq!(series_marker(MARK_CYCLE), series_marker(1));
        assert_eq!(MARK_CYCLE, MARKERS.len() + 1);
    }

    /// No marker is the shape a lone measurement is already drawn with
    #[test]
    fn no_marker_is_a_plain_circle() {
        // `draw_lines` marks a run of one with a circle, so a circle here would mean two things
        assert!(!MARKERS.contains(&egui_plot::MarkerShape::Circle));
    }
}
