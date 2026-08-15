//! Drawing a chart, and making the result legible in a book with five themes
//!
//! Every chart is drawn with plotters into a string, and then one substitution is made to the
//! result. See [`palette`] for why the colours in that string are sentinels rather than colours.

pub mod encryption;
pub mod hotpath_scopes;
pub mod macro_wall_clock;
pub mod micro_delta;
pub mod micro_scaling;
pub mod noise_band;
pub mod palette;
pub mod stages_stacked;

use anyhow::{Result, bail};
use plotters::coord::Shift;
use plotters::prelude::*;

/// The width every chart is drawn at, in user units
///
/// The rendered element is responsive - the width and height attributes are stripped and only the
/// `viewBox` survives - so this is an aspect ratio and a coordinate space rather than a size in
/// pixels. It is a little wider than mdbook's text column so that a dense chart has somewhere to
/// put its labels.
pub const WIDTH: u32 = 820;

/// Draws a chart and returns it as an inline SVG element
///
/// The element is inline in the page rather than an `<img>` pointing at a file, because a CSS
/// custom property only reaches an SVG that is part of the host document's DOM. An external SVG
/// inherits nothing from the page and would be drawn in raw sentinel colours - bright red on a
/// navy background.
///
/// # Arguments
///
/// * `id` - The element's id, which is also what a caption links to
/// * `aria` - What the chart shows, for a reader who cannot see it
/// * `height` - How tall to draw it, in the same user units as [`WIDTH`]
/// * `draw` - What to draw
pub fn draw<F>(id: &str, aria: &str, height: u32, draw: F) -> Result<String>
where
    F: for<'a> FnOnce(&DrawingArea<SVGBackend<'a>, Shift>) -> Result<()>,
{
    let mut buffer = String::new();
    {
        // the backing area is never filled, so the page's own background shows through and the
        // chart sits on whatever the theme's paper colour is
        let root = SVGBackend::with_string(&mut buffer, (WIDTH, height)).into_drawing_area();
        draw(&root)?;
        root.present()
            .map_err(|err| anyhow::anyhow!("presenting chart {id}: {err}"))?;
    }
    finish(&buffer, id, aria, height)
}

/// The root element plotters emits, which the post-pass replaces
///
/// Matched exactly rather than parsed. If a plotters upgrade changes this by so much as a space,
/// the substitution silently stops happening and every chart goes out with a hardcoded width and
/// no theme class - so it is better for that to be a test failure, which is what
/// `the_plotters_root_is_what_we_strip` is.
fn plotters_root(height: u32) -> String {
    format!(
        r#"<svg width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}" xmlns="http://www.w3.org/2000/svg">"#
    )
}

/// Replaces plotters' root element with a themed, responsive, labelled one
///
/// # Arguments
///
/// * `svg` - What plotters drew
/// * `id` - The element's id
/// * `aria` - What the chart shows
/// * `height` - The height it was drawn at
fn finish(svg: &str, id: &str, aria: &str, height: u32) -> Result<String> {
    let expected = plotters_root(height);
    // the substitution is the whole point of this function, so failing to make it is an error
    let Some(body) = svg.strip_prefix(expected.as_str()) else {
        bail!(
            "plotters emitted a root element this tool does not recognise, so chart {id} would \
             have gone out unthemed. Expected `{expected}`, got `{}`",
            svg.chars().take(expected.len()).collect::<String>()
        );
    };
    // width and height are dropped so the chart scales to the column it lands in; the viewBox is
    // what carries the coordinate space
    Ok(format!(
        r#"<svg class="shoal-chart" id="{id}" role="img" aria-label="{}" viewBox="0 0 {WIDTH} {height}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">{body}"#,
        escape_attribute(aria)
    ))
}

/// Escapes a string for use in an XML attribute
///
/// # Arguments
///
/// * `raw` - The text to escape
pub fn escape_attribute(raw: &str) -> String {
    // the five characters that cannot appear literally in an attribute value
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// The font a chart's labels are drawn in
///
/// # Arguments
///
/// * `size` - How large to draw them
pub fn label_font(size: u32) -> TextStyle<'static> {
    // plotters is built here without a font backend, so text extents are estimated rather than
    // measured. Labels are therefore kept short and given room, rather than trusted to fit.
    ("sans-serif", size).into_font().color(&palette::INK).into()
}

/// The font a chart's title is drawn in
pub fn title_font() -> TextStyle<'static> {
    ("sans-serif", 16).into_font().color(&palette::INK).into()
}

/// Configures a chart's mesh so that every part of it is a themed sentinel
///
/// Every one of these has to be set. plotters defaults each unset style to black, which is a
/// colour the stylesheet has no rule for and which is therefore drawn literally - invisible on
/// the book's default dark theme. `no_unthemed_colour_escapes` is what catches a missed one.
///
/// A macro rather than a function because `configure_mesh` carries a formatter bound for every
/// axis type, and spelling those out in a wrapper's signature costs more than it saves.
#[macro_export]
macro_rules! themed_mesh {
    ($chart:expr) => {{
        // gridlines, axis lines, tick labels and axis descriptions, in that order
        let mut style = $chart.configure_mesh();
        style
            .light_line_style($crate::render::chart::palette::GRID.mix(0.25))
            .bold_line_style($crate::render::chart::palette::GRID.mix(0.55))
            .axis_style($crate::render::chart::palette::AXIS)
            .label_style($crate::render::chart::label_font(12))
            .axis_desc_style($crate::render::chart::label_font(13));
        style
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Draws a trivial chart, for the tests that are about the wrapper rather than the contents
    fn trivial() -> Result<String> {
        draw("chart-test", "a test chart", 200, |root| {
            let mut chart = ChartBuilder::on(root)
                .margin(10)
                .x_label_area_size(24)
                .y_label_area_size(40)
                .build_cartesian_2d(0f64..10f64, 0f64..10f64)?;
            crate::themed_mesh!(chart).draw()?;
            chart.draw_series(LineSeries::new(
                (0..=10).map(|x| (f64::from(x), f64::from(x))),
                palette::series(0).stroke_width(2),
            ))?;
            Ok(())
        })
    }

    /// The root element plotters emits is exactly the one the post-pass strips
    ///
    /// This is the canary for a plotters upgrade. If it fails, the substitution in [`finish`] has
    /// stopped matching and every chart is going out with a hardcoded size and no theme class -
    /// which looks fine in a diff and is unreadable in the book.
    #[test]
    fn the_plotters_root_is_what_we_strip() {
        let mut buffer = String::new();
        {
            let root = SVGBackend::with_string(&mut buffer, (WIDTH, 200)).into_drawing_area();
            root.present().expect("an empty chart presents");
        }
        assert!(
            buffer.starts_with(&plotters_root(200)),
            "plotters now emits `{}`",
            buffer.chars().take(120).collect::<String>()
        );
    }

    /// The finished element is themed, responsive and labelled
    #[test]
    fn the_finished_element_is_themed_and_responsive() {
        let svg = trivial().expect("it draws");
        assert!(svg.starts_with(r#"<svg class="shoal-chart" id="chart-test""#), "{}", &svg[..80]);
        assert!(svg.contains(r#"role="img""#));
        assert!(svg.contains(r#"aria-label="a test chart""#));
        assert!(svg.contains(r#"viewBox="0 0 820 200""#));
        // the fixed size is gone, so the chart scales to its column
        assert!(!svg.contains(r#"<svg width="820""#));
    }

    /// Every colour in a drawn chart is a sentinel the stylesheet knows how to theme
    ///
    /// A colour that is not a sentinel is a colour that will be drawn literally, in every theme.
    #[test]
    fn no_unthemed_colour_escapes() {
        let svg = trivial().expect("it draws");
        let known = palette::all_hex();
        // walk every colour literal in the document
        let mut at = 0;
        let mut found = 0;
        while let Some(next) = svg[at..].find('#') {
            let start = at + next;
            let literal: String = svg[start..].chars().take(7).collect();
            // only seven character hex literals are colours; anything else is a fragment or text
            if literal.len() == 7 && literal[1..].chars().all(|ch| ch.is_ascii_hexdigit()) {
                assert!(
                    known.contains(&literal.to_uppercase()),
                    "{literal} is not a palette sentinel, so it will be drawn literally in every \
                     theme"
                );
                found += 1;
            }
            at = start + 1;
        }
        assert!(found > 0, "the chart drew nothing with a colour");
    }

    /// A chart with no coordinate that is not a number
    ///
    /// plotters emits `NaN` into the path data rather than failing when a series is empty or a log
    /// axis touches zero, and an SVG with `NaN` in it silently draws nothing.
    #[test]
    fn no_coordinate_is_nan() {
        let svg = trivial().expect("it draws");
        assert!(!svg.contains("NaN"), "a coordinate came out as NaN");
        assert!(!svg.contains("inf"), "a coordinate came out infinite");
    }

    /// An unrecognised root element is an error rather than an unthemed chart
    #[test]
    fn an_unrecognised_root_is_refused() {
        let err = finish("<svg width=\"1\">x</svg>", "chart-x", "x", 200)
            .expect_err("an unknown root must not pass silently");
        assert!(format!("{err}").contains("does not recognise"));
    }

    /// Attribute text is escaped, since a chart's description is built from benchmark names
    #[test]
    fn attribute_text_is_escaped() {
        assert_eq!(
            escape_attribute(r#"a & b <c> "d""#),
            "a &amp; b &lt;c&gt; &quot;d&quot;"
        );
    }

    /// The same input draws the same chart, byte for byte
    ///
    /// The generated page is committed and checked, so a chart that varied between two runs over
    /// the same data would make `render --check` fail on a tree nobody touched.
    #[test]
    fn drawing_is_deterministic() {
        assert_eq!(trivial().expect("it draws"), trivial().expect("it draws"));
    }
}
