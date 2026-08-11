//! The sentinel colours a chart is drawn with, and what they mean
//!
//! # Why the colours in the SVG are not the colours you see
//!
//! The book has five themes and defaults to a dark one, so a chart drawn in fixed colours is
//! legible in at most one of them. plotters has no notion of a theme and emits whatever RGB it
//! was given, as a literal `#RRGGBB` in a `fill=` or `stroke=` presentation attribute.
//!
//! So the charts are drawn in *sentinel* colours - values chosen to be recognisable and to appear
//! nowhere else - and `docs/theme/charts.css` maps each sentinel onto one of mdbook's own theme
//! variables with an attribute selector:
//!
//! ```css
//! svg.shoal-chart [fill="#FE0001"]   { fill:   var(--fg); }
//! svg.shoal-chart [stroke="#FE0002"] { stroke: var(--table-border-color); }
//! ```
//!
//! This works, and works better than the obvious alternative of emitting `fill="var(--fg)"`
//! directly, for three reasons. A presentation attribute loses to *any* author stylesheet rule,
//! by specification, so the override needs no `!important` and cannot be beaten by plotters. CSS
//! variables in presentation attributes are not reliably supported, so the direct form would
//! quietly fail somewhere. And the mapping ends up in a reviewable stylesheet rather than
//! scattered through Rust string constants.
//!
//! One further property makes this cheap: plotters puts a colour's *alpha* in a separate
//! `opacity=` attribute rather than folding it into the hex. A sentinel therefore survives
//! `.mix(0.3)` untouched, so one sentinel covers a colour at every transparency it is drawn with.
//!
//! Every sentinel here must have a matching rule in `docs/theme/charts.css`, and every rule there
//! must match a sentinel here. `tests/css_sync.rs` checks both directions.

use plotters::style::RGBColor;

/// Text: chapter foreground
pub const INK: RGBColor = RGBColor(0xFE, 0x00, 0x01);

/// Gridlines: the colour the book draws table borders in
pub const GRID: RGBColor = RGBColor(0xFE, 0x00, 0x02);

/// Axis lines and ticks
pub const AXIS: RGBColor = RGBColor(0xFE, 0x00, 0x03);

/// Annotations and reference lines: the colour the book draws links in
pub const ACCENT: RGBColor = RGBColor(0xFE, 0x00, 0x04);

/// A band or marker warning that something is not a result
pub const WARN: RGBColor = RGBColor(0xFE, 0x00, 0x05);

/// Something present but deliberately de-emphasised
pub const MUTED: RGBColor = RGBColor(0xFE, 0x00, 0x06);

/// A change in the better direction
///
/// Blue, paired against red rather than against green. A green and red pair is the one categorical
/// pairing that fails outright for the commonest colour vision deficiencies, and a chart whose
/// entire meaning is which side of zero a bar is on cannot afford that.
pub const BETTER: RGBColor = RGBColor(0xFE, 0x00, 0x07);

/// A change in the worse direction
pub const WORSE: RGBColor = RGBColor(0xFE, 0x00, 0x08);

/// The categorical series colours, in the order they are handed out
///
/// Eight is the ceiling. A chart that needs a ninth category has too many, and folds the tail into
/// an `other` slot drawn in [`MUTED`] instead.
pub const SERIES: [RGBColor; 8] = [
    RGBColor(0xFE, 0x00, 0x10),
    RGBColor(0xFE, 0x00, 0x11),
    RGBColor(0xFE, 0x00, 0x12),
    RGBColor(0xFE, 0x00, 0x13),
    RGBColor(0xFE, 0x00, 0x14),
    RGBColor(0xFE, 0x00, 0x15),
    RGBColor(0xFE, 0x00, 0x16),
    RGBColor(0xFE, 0x00, 0x17),
];

/// The series colour for a category, wrapping if there are somehow more than eight
///
/// # Arguments
///
/// * `index` - Which category this is
pub fn series(index: usize) -> RGBColor {
    // wrapping rather than panicking: a chart with too many categories is a design problem to fix
    // in the chart, not a reason for the tool to fall over
    SERIES[index % SERIES.len()]
}

/// Every sentinel this crate draws with, as the uppercase hex plotters emits
///
/// Used by the stylesheet synchronisation test, and by the check that no chart smuggled a colour
/// past the palette.
pub fn all_hex() -> Vec<String> {
    // the named roles first, then the numbered series
    let mut hex: Vec<String> = [INK, GRID, AXIS, ACCENT, WARN, MUTED, BETTER, WORSE]
        .iter()
        .chain(SERIES.iter())
        .map(|color| format!("#{:02X}{:02X}{:02X}", color.0, color.1, color.2))
        .collect();
    hex.sort();
    hex.dedup();
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No two sentinels are the same colour, or the stylesheet could not tell them apart
    #[test]
    fn every_sentinel_is_distinct() {
        let hex = all_hex();
        let mut unique = hex.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(hex.len(), unique.len(), "two sentinels share a value");
        assert_eq!(hex.len(), 16);
    }

    /// Sentinels are written the way plotters writes them, or the selectors will not match
    #[test]
    fn sentinels_are_uppercase_hex() {
        for value in all_hex() {
            assert!(value.starts_with('#'), "{value}");
            assert_eq!(value.len(), 7, "{value}");
            assert!(
                value[1..]
                    .chars()
                    .all(|ch| ch.is_ascii_digit() || ch.is_ascii_uppercase()),
                "{value} is not the uppercase hex plotters emits"
            );
        }
    }

    /// The series colours wrap rather than panicking
    #[test]
    fn series_colours_wrap() {
        assert_eq!(series(0), SERIES[0]);
        assert_eq!(series(7), SERIES[7]);
        assert_eq!(series(8), SERIES[0]);
    }
}
