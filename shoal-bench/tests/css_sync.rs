//! The sentinel colours and the stylesheet that themes them cannot drift apart
//!
//! A sentinel with no rule in `docs/theme/charts.css` is drawn literally - bright red on a navy
//! page - and a rule with no sentinel behind it is dead weight that looks like coverage. Neither
//! shows up in a diff, and neither shows up in a chart that is only ever looked at in one theme.
//!
//! There is also a rule per sentinel for `fill` *and* for `stroke`, because plotters uses whichever
//! the shape calls for and the two are separate CSS properties. A sentinel with only one of the two
//! works everywhere it is filled and is invisible everywhere it is stroked.

use std::collections::BTreeSet;
use std::path::PathBuf;

use shoal_bench::render::chart::palette;

/// Reads the stylesheet that themes the charts
fn stylesheet() -> String {
    let path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench has a parent")
        .join("docs/theme/charts.css");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()))
}

/// Every colour literal the stylesheet selects on, for a given property
///
/// # Arguments
///
/// * `css` - The stylesheet
/// * `property` - The property the selector matches on, `fill` or `stroke`
fn selected(css: &str, property: &str) -> BTreeSet<String> {
    // find every `[<property>="#RRGGBB"]` attribute selector
    let needle = format!("[{property}=\"#");
    let mut found = BTreeSet::new();
    let mut at = 0;
    while let Some(next) = css[at..].find(&needle) {
        let start = at + next + needle.len() - 1;
        let literal: String = css[start..].chars().take(7).collect();
        if literal.len() == 7 {
            found.insert(literal);
        }
        at = start + 1;
    }
    found
}

/// Every sentinel has a rule for both properties it can be drawn with
#[test]
fn every_sentinel_is_themed_for_fill_and_stroke() {
    let css = stylesheet();
    let fills = selected(&css, "fill");
    let strokes = selected(&css, "stroke");
    for sentinel in palette::all_hex() {
        assert!(
            fills.contains(&sentinel),
            "{sentinel} has no `fill` rule in docs/theme/charts.css, so anything filled with it \
             is drawn literally in every theme"
        );
        assert!(
            strokes.contains(&sentinel),
            "{sentinel} has no `stroke` rule in docs/theme/charts.css, so anything stroked with \
             it is drawn literally in every theme"
        );
    }
}

/// Every rule in the stylesheet themes a sentinel that exists
#[test]
fn every_rule_themes_a_real_sentinel() {
    let css = stylesheet();
    let known: BTreeSet<String> = palette::all_hex().into_iter().collect();
    for property in ["fill", "stroke"] {
        for literal in selected(&css, property) {
            assert!(
                known.contains(&literal),
                "docs/theme/charts.css themes {literal} for `{property}`, which is not a sentinel \
                 in shoal_bench::render::chart::palette - it was probably renamed"
            );
        }
    }
}

/// Every badge class a verdict can ask for is styled
///
/// A verdict whose class has no rule still renders its text, so this does not break the page - it
/// just silently stops looking like a badge, which is exactly the kind of thing nobody notices.
#[test]
fn every_badge_class_is_styled() {
    let css = stylesheet();
    for class in [
        "sc-badge",
        "sc-fresh",
        "sc-unaffected",
        "sc-stale",
        "sc-uncommitted",
        "sc-diverged",
        "sc-none",
        "sc-env",
        "sc-partial",
        "sc-caption",
    ] {
        assert!(
            css.contains(&format!(".{class}")),
            "docs/theme/charts.css has no rule for .{class}"
        );
    }
}

/// The stylesheet is registered with the book, or none of it is loaded at all
#[test]
fn the_stylesheet_is_registered_with_the_book() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench has a parent")
        .join("docs/book.toml");
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()));
    assert!(
        body.contains("theme/charts.css"),
        "docs/book.toml does not list theme/charts.css in additional-css, so every chart in the \
         book is drawn in raw sentinel colours"
    );
}
