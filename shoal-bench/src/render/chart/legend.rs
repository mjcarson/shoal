//! The strip under a chart that says what each colour is
//!
//! Every chart that draws more than one colour names them here, in one place, in the order the
//! colours were handed out.
//!
//! # Why this replaced labelling each line at its own end
//!
//! [`sweep`](super::sweep) used to name each series just past its last point, and three other
//! modules had their own copies of that idea with their own separation constants - two of which
//! pushed a colliding label down and one of which pushed it up. The argument for it was that a
//! legend costs the reader a colour match on every glance, which is true. What it traded that for
//! was worse: a label at the end of a line is only readable when the lines end far apart, and on a
//! chart of a mixture they routinely end within a few pixels of each other. `chart-grid-latency`
//! drew eight curves whose ends collapsed into a six pixel band, and the spreading pass ran out of
//! axis, clamped against the plot floor, and put two names three pixels apart at an eight pixel
//! font. A name that has been pushed a third of the way down the chart to find room is not
//! attached to its line either, so the colour match was being paid for anyway - without the legend
//! that would have made it possible.
//!
//! The colour half of that old argument still holds, and is why an entry here is an eleven by
//! eleven filled square rather than a short sample of the line. Three of the eight series colours
//! fall below a 3:1 contrast ratio on the light themes, and a filled block of that size is legible
//! where a two pixel stroke of the same colour is not.
//!
//! # Everything here is estimated, not measured
//!
//! plotters is built without a font backend, so a string's width has to be guessed from its length.
//! The guess is deliberately generous: a column that is too wide wastes space, and a column that is
//! too narrow overlaps the next one, which is the failure this module exists to remove.

use anyhow::Result;
use plotters::coord::Shift;
use plotters::prelude::*;

use super::WIDTH;

/// The size of a colour swatch, in user units
const SWATCH: i32 = 11;

/// The font an entry's name is drawn at
const FONT: u32 = 11;

/// The gap between a swatch and the name beside it
const GAP: i32 = 5;

/// The gap between one column of entries and the next
const COLUMN_GAP: i32 = 20;

/// How far apart two rows of entries are drawn
///
/// Comfortably more than the font is tall, and more than the nine user units
/// `chart_geometry::stacked_labels_have_room` holds every column of labels to.
const ROW_PITCH: i32 = 18;

/// The space above the first row of entries, separating them from the plot
const TOP_PAD: i32 = 9;

/// The space below the last row
const BOTTOM_PAD: i32 = 7;

/// The margin either side of the strip
const SIDE_PAD: i32 = 12;

/// One named colour in a legend
#[derive(Debug, Clone)]
pub struct Entry {
    /// What this colour is
    pub name: String,
    /// The colour itself, which must be a palette sentinel
    pub colour: RGBColor,
}

impl Entry {
    /// Builds an entry
    ///
    /// # Arguments
    ///
    /// * `name` - What the colour is
    /// * `colour` - The sentinel it is drawn in
    pub fn new<N: Into<String>>(name: N, colour: RGBColor) -> Self {
        Entry {
            name: name.into(),
            colour,
        }
    }
}

/// How tall a strip holding these entries has to be, in user units
///
/// Callable before anything is drawn, because [`super::draw`] takes the canvas height as a
/// parameter and a chart therefore has to know how much room its legend wants before it has a
/// drawing area to ask.
///
/// # Arguments
///
/// * `entries` - The colours to be named
pub fn height(entries: &[Entry]) -> u32 {
    // an empty legend takes no room at all rather than an empty strip
    if entries.is_empty() {
        return 0;
    }
    let rows = layout(entries).1;
    (TOP_PAD + rows * ROW_PITCH + BOTTOM_PAD) as u32
}

/// Draws the entries onto a strip
///
/// # Arguments
///
/// * `area` - The strip to draw on, which should be [`height`] tall
/// * `entries` - The colours to name, in the order they were handed out
pub fn draw<'a>(area: &DrawingArea<SVGBackend<'a>, Shift>, entries: &[Entry]) -> Result<()> {
    // nothing to name is not a legend
    if entries.is_empty() {
        return Ok(());
    }
    let (columns, _) = layout(entries);
    let column_width = column_width(entries);
    for (index, entry) in entries.iter().enumerate() {
        // row major, so reading the legend left to right and top to bottom walks the colours in
        // the order the chart handed them out
        let column = (index % columns) as i32;
        let row = (index / columns) as i32;
        let x = SIDE_PAD + column * column_width;
        let y = TOP_PAD + row * ROW_PITCH;
        // the swatch, then the name beside it, sharing a baseline the way the bar charts' strip did
        area.draw(&Rectangle::new(
            [(x, y), (x + SWATCH, y + SWATCH)],
            entry.colour.filled(),
        ))?;
        area.draw(&Text::new(
            entry.name.clone(),
            (x + SWATCH + GAP, y + 1),
            super::label_font(FONT),
        ))?;
    }
    Ok(())
}

/// How many columns the entries are laid out in, and how many rows that takes
///
/// # Arguments
///
/// * `entries` - The colours to be named
fn layout(entries: &[Entry]) -> (usize, i32) {
    let width = column_width(entries);
    // at least one column, however long a single name is: an entry that overflows the canvas is
    // still better than a division by zero
    let fits = ((f64::from(WIDTH) as i32 - SIDE_PAD * 2) / width.max(1)).max(1) as usize;
    let columns = fits.min(entries.len()).max(1);
    let rows = entries.len().div_ceil(columns) as i32;
    (columns, rows)
}

/// How wide one column has to be to hold the longest name in it
///
/// # Arguments
///
/// * `entries` - The colours to be named
fn column_width(entries: &[Entry]) -> i32 {
    // the widest name decides the column, so every column is the same width and the entries line
    // up down the strip rather than staggering
    let widest = entries
        .iter()
        .map(|entry| text_width(&entry.name))
        .max()
        .unwrap_or(0);
    SWATCH + GAP + widest + COLUMN_GAP
}

/// About how wide a string is, in user units
///
/// # Arguments
///
/// * `text` - The string to measure
fn text_width(text: &str) -> i32 {
    // 0.6 em per character, which is wider than a sans-serif average and is meant to be: the cost
    // of overestimating is a gap, and the cost of underestimating is the overlap this module was
    // written to remove
    (text.chars().count() as i32) * (FONT as i32) * 6 / 10
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::chart::palette;

    /// Builds `count` entries whose names are `width` characters long
    ///
    /// # Arguments
    ///
    /// * `count` - How many entries
    /// * `width` - How long each name is
    fn entries(count: usize, width: usize) -> Vec<Entry> {
        (0..count)
            .map(|index| Entry::new("x".repeat(width) + &index.to_string(), palette::series(index)))
            .collect()
    }

    /// An empty legend takes no room
    #[test]
    fn an_empty_legend_is_not_a_strip() {
        assert_eq!(height(&[]), 0);
    }

    /// A handful of short names share one row
    #[test]
    fn short_names_fit_on_one_row() {
        let entries = entries(4, 6);
        assert_eq!(layout(&entries).1, 1, "four short names should share a row");
        assert_eq!(height(&entries), (TOP_PAD + ROW_PITCH + BOTTOM_PAD) as u32);
    }

    /// Names too long to share a row wrap onto the next one
    #[test]
    fn long_names_wrap() {
        let entries = entries(8, 40);
        let (columns, rows) = layout(&entries);
        assert!(columns < 8, "40 character names cannot fit 8 to a row");
        assert!(rows > 1, "the entries that did not fit went nowhere");
        assert!(height(&entries) > (TOP_PAD + ROW_PITCH + BOTTOM_PAD) as u32);
    }

    /// A single name wider than the canvas still gets a column
    ///
    /// A scope name from the profile is longer than the chart is wide, and a zero column count
    /// would be a division by zero rather than an ugly legend.
    #[test]
    fn one_enormous_name_still_lays_out() {
        let entries = entries(1, 400);
        let (columns, rows) = layout(&entries);
        assert_eq!(columns, 1);
        assert_eq!(rows, 1);
    }

    /// Every entry is named exactly once, and in its own colour
    #[test]
    fn every_entry_is_drawn_and_named() {
        let entries = entries(3, 8);
        let svg = super::super::draw("chart-test-legend", "a legend", height(&entries), |root| {
            draw(root, &entries)
        })
        .expect("it draws");
        for entry in &entries {
            assert_eq!(svg.matches(entry.name.as_str()).count(), 1, "{}", entry.name);
        }
        // three swatches in three different sentinels
        for index in 0..3 {
            let sentinel = format!("{:?}", palette::series(index));
            let hex = format!(
                "#{:02X}{:02X}{:02X}",
                palette::series(index).0,
                palette::series(index).1,
                palette::series(index).2
            );
            assert!(svg.contains(&hex), "{sentinel} was not drawn");
        }
    }

    /// The height a legend asks for is the room its entries actually take
    ///
    /// The two are computed in different places - the height before the canvas exists, the layout
    /// while drawing on it - so nothing but this keeps them agreeing.
    #[test]
    fn the_height_covers_every_row() {
        for count in [1usize, 2, 5, 8, 19] {
            for width in [4usize, 12, 30, 60] {
                let entries = entries(count, width);
                let (columns, rows) = layout(&entries);
                let last = TOP_PAD + (rows - 1) * ROW_PITCH + SWATCH;
                assert!(
                    last <= height(&entries) as i32,
                    "{count} names of {width} characters overflow their strip"
                );
                assert!(columns * rows as usize >= count, "an entry had nowhere to go");
            }
        }
    }

    /// The same entries draw the same strip, byte for byte
    #[test]
    fn drawing_is_deterministic() {
        let entries = entries(5, 10);
        let once = super::super::draw("chart-test-legend", "a legend", height(&entries), |root| {
            draw(root, &entries)
        })
        .expect("it draws");
        let twice = super::super::draw("chart-test-legend", "a legend", height(&entries), |root| {
            draw(root, &entries)
        })
        .expect("it draws");
        assert_eq!(once, twice);
    }
}
