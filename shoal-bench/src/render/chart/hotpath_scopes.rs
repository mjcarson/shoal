//! Where the time went, by instrumented scope
//!
//! # The one field this chart refuses to plot
//!
//! `hotpath` reports a `percent_total` for every scope, and it is not usable here. It is not
//! normalised across concurrent scopes: twelve shards each spending most of a run inside a scope
//! sum to far more than the run, and the committed `B1-performance.hotpath.json` duly reports
//! `stream::write_helper` at 12,530%. Filed as known issue 53.
//!
//! So the chart ranks and draws `total`, which is a real quantity - nanoseconds summed across
//! every shard that entered the scope. That sum exceeds the wall clock of the run for anything
//! that ran on many shards at once, which is why the axis says so rather than leaving a reader to
//! divide by the wall clock and get a fraction over one.

use anyhow::Result;
use plotters::prelude::*;

use super::palette;
use crate::fmt;
use crate::model::hotpath::HotpathProfile;

/// How many scopes are drawn
const TOP_N: usize = 12;

/// How tall each scope's row is drawn
const ROW_HEIGHT: u32 = 26;

/// Draws the most expensive scopes of a profile
///
/// # Arguments
///
/// * `profile` - The profile to draw
pub fn draw(profile: &HotpathProfile) -> Result<String> {
    let ranked = profile.top_by_total(TOP_N);
    // a profile that attributed nothing is not a chart
    if ranked.is_empty() {
        anyhow::bail!("the profile attributed time to no scopes");
    }
    // owned, because the drawing closure outlives this frame
    let rows: Vec<(String, u64, u64)> = ranked
        .iter()
        .map(|(name, scope)| (shorten(name), scope.total, scope.calls))
        .collect();
    let widest = rows.iter().map(|(_, total, _)| *total).max().unwrap_or(1) as f64;
    let count = rows.len();
    let height = ROW_HEIGHT * count as u32 + 84;
    let aria = format!(
        "The {count} most expensive instrumented scopes, summed across shards, the largest being \
         {}",
        fmt::duration_ns(widest)
    );
    super::draw("chart-hotpath-scopes", &aria, height, move |root| {
        let mut chart = ChartBuilder::on(root)
            .margin(14)
            .margin_right(150)
            .x_label_area_size(44)
            .y_label_area_size(330)
            .build_cartesian_2d(0f64..widest * 1.08, -0.5f64..(count as f64 - 0.5))?;
        // the y axis is one scope per row, most expensive at the top
        let names: Vec<String> = rows.iter().map(|(name, _, _)| name.clone()).collect();
        crate::themed_mesh!(chart)
            .disable_y_mesh()
            .y_labels(count)
            .y_label_formatter(&move |value: &f64| {
                // only label the ticks that land on a scope
                let index = value.round();
                if (value - index).abs() > 0.01 || index < 0.0 {
                    return String::new();
                }
                // drawn top down, so the row order is reversed against the axis
                names
                    .get(count - 1 - index as usize)
                    .cloned()
                    .unwrap_or_default()
            })
            .x_desc("total time inside the scope, summed across 12 shards")
            .x_label_formatter(&|value: &f64| fmt::duration_ns(*value))
            .draw()?;
        for (index, (_, total, calls)) in rows.iter().enumerate() {
            // the most expensive scope is drawn at the top, so the row index counts down
            let y = (count - 1 - index) as f64;
            chart.draw_series(std::iter::once(Rectangle::new(
                [(0.0, y - 0.34), (*total as f64, y + 0.34)],
                palette::series(0).mix(0.85).filled(),
            )))?;
            // each bar carries its own numbers, so the chart is readable without the table
            chart.draw_series(std::iter::once(Text::new(
                format!(
                    "{}  ({} calls)",
                    fmt::duration_ns(*total as f64),
                    fmt::thousands(u128::from(*calls))
                ),
                (*total as f64 + widest * 0.012, y),
                super::label_font(11),
            )))?;
        }
        Ok(())
    })
}

/// Shortens a fully qualified scope path to something that fits a label
///
/// # Arguments
///
/// * `name` - The scope's full path
fn shorten(name: &str) -> String {
    // the crate prefix is the same for every scope in the profile and carries no information
    let trimmed = name.strip_prefix("shoal::").unwrap_or(name);
    // plotters estimates text extents rather than measuring them, so a long label is a label that
    // overlaps its neighbour. keep the tail, which is the part that identifies the scope.
    const LIMIT: usize = 46;
    if trimmed.chars().count() <= LIMIT {
        return trimmed.to_string();
    }
    let tail: String = trimmed
        .chars()
        .skip(trimmed.chars().count() - (LIMIT - 1))
        .collect();
    format!("…{tail}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::hotpath::HotpathScope;

    /// Builds a profile from `(scope, total, calls)` triples
    ///
    /// # Arguments
    ///
    /// * `scopes` - What the profile measured
    fn profile(scopes: &[(&str, u64, u64)]) -> HotpathProfile {
        let output: BTreeMap<String, HotpathScope> = scopes
            .iter()
            .map(|(name, total, calls)| {
                (
                    name.to_string(),
                    HotpathScope {
                        calls: *calls,
                        avg: total / calls.max(&1),
                        p50: 1,
                        p90: 1,
                        p95: 1,
                        p99: 1,
                        total: *total,
                        // deliberately absurd, the way the real profiles report it
                        percent_total: 12_530.0,
                    },
                )
            })
            .collect();
        HotpathProfile {
            hotpath_profiling_mode: "timing".to_string(),
            total_elapsed: 12_070_266_532,
            description: None,
            caller_name: "tmdb::main".to_string(),
            output,
        }
    }

    /// The most expensive scopes are drawn, with their totals and call counts
    #[test]
    fn it_draws_the_most_expensive_scopes() {
        let profile = profile(&[
            ("shoal::server::shard::handle_query", 7_538_153_856, 617_175),
            ("shoal::server::tables::storage::fs::load_partition", 391, 1),
        ]);
        let svg = draw(&profile).expect("it draws");
        assert!(svg.contains("server::shard::handle_query"));
        // the call count is on the bar
        assert!(svg.contains("617,175 calls"));
        assert!(!svg.contains("NaN"));
    }

    /// The unnormalised percentage is never drawn
    ///
    /// It is not a fraction of anything: concurrent scopes sum well past the run. Known issue 53.
    #[test]
    fn the_unnormalised_percentage_is_never_drawn() {
        let profile = profile(&[("a::b", 100, 1)]);
        let svg = draw(&profile).expect("it draws");
        assert!(
            !svg.contains("12530") && !svg.contains("12,530"),
            "percent_total reached the chart"
        );
        // and the axis says what the bars actually are
        assert!(svg.contains("summed across 12 shards"));
    }

    /// Only the top scopes are drawn, however many the profile holds
    #[test]
    fn only_the_top_scopes_are_drawn() {
        let owned: Vec<(String, u64, u64)> = (0..30)
            .map(|index| (format!("a::scope{index}"), 100 * (index + 1), 1))
            .collect();
        let scopes: Vec<(&str, u64, u64)> = owned
            .iter()
            .map(|(name, total, calls)| (name.as_str(), *total, *calls))
            .collect();
        let profile = profile(&scopes);
        assert_eq!(profile.top_by_total(TOP_N).len(), TOP_N);
        let svg = draw(&profile).expect("it draws");
        // the cheapest scope is not among them
        assert!(!svg.contains("scope0<"), "the cheapest scope was drawn");
    }

    /// A long scope path is shortened from the front, keeping what identifies it
    #[test]
    fn a_long_scope_is_shortened_from_the_front() {
        assert_eq!(shorten("shoal::server::shard::handle_query"), "server::shard::handle_query");
        let long = shorten("shoal::server::tables::storage::fs::stream::write_helper_with_a_long_name");
        assert!(long.starts_with('…'), "{long}");
        assert!(long.ends_with("write_helper_with_a_long_name"), "{long}");
        assert!(long.chars().count() <= 46);
    }

    /// A profile with nothing in it is an error rather than an empty chart
    #[test]
    fn an_empty_profile_is_an_error() {
        assert!(draw(&profile(&[])).is_err());
    }
}
