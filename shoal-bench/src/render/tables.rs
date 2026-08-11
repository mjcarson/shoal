//! The tables that sit beside every chart
//!
//! Every chart on the generated page ships next to a table carrying the same numbers. That is not
//! belt and braces: three of the series colours fall below the 3:1 contrast floor on the book's
//! light themes, and that is only permissible under the relief rule - the numbers have to be
//! readable without the colours. Deleting a table therefore breaks the accessibility of the chart
//! above it, which is not the sort of thing anyone would guess from looking at the diff.
//!
//! They are also what a reader with the page open actually copies a number out of.

use std::collections::BTreeMap;

use crate::compare::micro::MicroComparison;
use crate::fmt;
use crate::model::hotpath::HotpathProfile;
use crate::model::macro_layer::MacroCaptureV2;
use crate::model::stages::StageReport;
use crate::render::chart::micro_scaling::Family;

/// Renders a markdown table
///
/// # Arguments
///
/// * `headers` - The column headings
/// * `alignment` - One alignment marker per column, such as `---` or `---:`
/// * `rows` - The cells, one vector per row
fn table(headers: &[&str], alignment: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    // the heading row, then the alignment row, then the body
    out.push_str(&format!("| {} |\n", headers.join(" | ")));
    out.push_str(&format!("| {} |\n", alignment.join(" | ")));
    for row in rows {
        out.push_str(&format!("| {} |\n", row.join(" | ")));
    }
    out
}

/// The end to end result of every capture
///
/// # Arguments
///
/// * `captures` - Each capture's name and macro artifact, in the order they were taken
pub fn macro_summary(captures: &[(String, MacroCaptureV2)]) -> String {
    // one row per workload per capture, with the interval that decides whether two of them differ
    //
    // the workload is a column rather than a table each, because the question this table answers
    // is how one workload moved across captures, and that reads down a column
    let mut rows: Vec<Vec<String>> = Vec::new();
    for (label, capture) in captures {
        for (id, workload) in &capture.workloads {
            let median = workload.median_wall_clock_ns() as f64;
            let interval = workload
                .wall_clock_interval_ns()
                .map(|(low, high)| {
                    format!("{} – {}", fmt::millis(low as f64), fmt::millis(high as f64))
                })
                .unwrap_or_else(|| "one run".to_string());
            let moved: u64 = workload.counters.values().sum();
            rows.push(vec![
                format!("`{label}`"),
                format!("`{id}`"),
                workload.timing.as_str().to_string(),
                fmt::millis(median),
                interval,
                workload
                    .spread_pct
                    .map(|pct| fmt::signed_pct(pct).replace('+', ""))
                    .unwrap_or_else(|| "-".to_string()),
                fmt::thousands(workload.rows_per_sec() as u128),
                fmt::thousands(u128::from(moved)),
            ]);
        }
    }
    table(
        &[
            "Capture",
            "Workload",
            "Timing",
            "Median wall clock",
            "Range across runs",
            "Spread",
            "Rows/sec",
            "Rows",
        ],
        &[
            "---", "---", "---", "---:", "---:", "---:", "---:", "---:",
        ],
        &rows,
    )
}

/// Every benchmark's movement against a baseline
///
/// # Arguments
///
/// * `comparison` - What the comparison found
pub fn micro_comparison(comparison: &MicroComparison) -> String {
    // the same order the chart draws them in, so the two can be read together
    let rows: Vec<Vec<String>> = comparison
        .rows
        .iter()
        .map(|row| {
            let (before, after) = fmt::duration_pair(row.baseline_ns, row.run_ns);
            vec![
                format!("`{}`", row.name),
                before,
                after,
                fmt::signed_pct(row.pct),
                if row.significant {
                    format!("outside ±{}%", fmt::fixed(row.band_pct, 0))
                } else {
                    "within noise".to_string()
                },
            ]
        })
        .collect();
    table(
        &["Benchmark", "Baseline", "This capture", "Change", "Verdict"],
        &["---", "---:", "---:", "---:", "---"],
        &rows,
    )
}

/// How each operation's cost grows with the partition size
///
/// # Arguments
///
/// * `families` - The operations to tabulate
pub fn micro_scaling(families: &[Family]) -> String {
    // every size any family was measured at, so the columns line up across rows
    let mut sizes: Vec<u64> = families
        .iter()
        .flat_map(|family| family.points.iter().map(|(size, _)| *size as u64))
        .collect();
    sizes.sort_unstable();
    sizes.dedup();
    let mut headers = vec!["Operation".to_string()];
    headers.extend(sizes.iter().map(|size| format!("{size} rows")));
    // and how much dearer the largest is than the smallest, which is the shape the chart shows
    headers.push("Growth".to_string());
    let rows: Vec<Vec<String>> = families
        .iter()
        .map(|family| {
            let measured: BTreeMap<u64, f64> = family
                .points
                .iter()
                .map(|(size, cost)| (*size as u64, *cost))
                .collect();
            let mut row = vec![format!("`{}`", family.name)];
            for size in &sizes {
                row.push(
                    measured
                        .get(size)
                        .map(|cost| fmt::duration_ns(*cost))
                        .unwrap_or_else(|| "-".to_string()),
                );
            }
            // the ratio between the ends, which says whether this scans or seeks
            let growth = match (family.points.first(), family.points.last()) {
                (Some((_, first)), Some((_, last))) if *first > 0.0 => {
                    format!("×{}", fmt::fixed(last / first, 1))
                }
                _ => "-".to_string(),
            };
            row.push(growth);
            row
        })
        .collect();
    let header_refs: Vec<&str> = headers.iter().map(String::as_str).collect();
    let mut alignment = vec!["---"];
    alignment.extend(std::iter::repeat_n("---:", headers.len() - 1));
    table(&header_refs, &alignment, &rows)
}

/// The most expensive instrumented scopes
///
/// # Arguments
///
/// * `profile` - The profile to tabulate
/// * `limit` - How many scopes to include
pub fn hotpath_scopes(profile: &HotpathProfile, limit: usize) -> String {
    // ranked by total time, which is the only field of a concurrent scope that means anything
    let rows: Vec<Vec<String>> = profile
        .top_by_total(limit)
        .into_iter()
        .map(|(name, scope)| {
            vec![
                format!("`{name}`"),
                fmt::thousands(u128::from(scope.calls)),
                fmt::duration_ns(scope.total as f64),
                fmt::duration_ns(scope.avg as f64),
                fmt::duration_ns(scope.p99 as f64),
            ]
        })
        .collect();
    table(
        &["Scope", "Calls", "Total across shards", "Mean", "p99"],
        &["---", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Where one kind of query's latency went, at every rank
///
/// # Arguments
///
/// * `report` - The stage report to tabulate
/// * `op` - Which operation to tabulate
/// * `top` - How many stages to name per rank before the rest are summed
pub fn stage_ranks(report: &StageReport, op: &str, top: usize) -> Option<String> {
    let operation = report.ops.get(op)?;
    // one row per rank, naming where most of that rank's time went
    let rows: Vec<Vec<String>> = operation
        .buckets
        .iter()
        .map(|bucket| {
            // the dearest stages of this bucket, skipping the ones that are mostly instrument
            let mut ranked: Vec<(&String, u64)> = bucket
                .stages
                .iter()
                .filter(|(_, cost)| !cost.at_floor)
                .map(|(name, cost)| (name, cost.mean_ns))
                .collect();
            ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
            let named = ranked
                .iter()
                .take(top)
                .map(|(name, nanos)| {
                    let share = if bucket.total_ns > 0 {
                        *nanos as f64 / bucket.total_ns as f64
                    } else {
                        0.0
                    };
                    format!("`{name}` {}", fmt::share_pct(share))
                })
                .collect::<Vec<String>>()
                .join(", ");
            vec![
                format!("`{}`", bucket.rank),
                fmt::duration_ns(bucket.total_ns as f64),
                fmt::thousands(bucket.samples as u128),
                named,
                fmt::duration_ns(bucket.unaccounted_ns as f64),
            ]
        })
        .collect();
    Some(table(
        &["Rank", "Total", "Queries", "Where the time went", "Unaccounted"],
        &["---", "---:", "---:", "---", "---:"],
        &rows,
    ))
}

/// What the micro layer can resolve, by how long a benchmark takes
///
/// # Arguments
///
/// * `groups` - Each set of repeats, and each benchmark's duration and spread within it
pub fn noise_buckets(groups: &[(String, Vec<(f64, f64)>)]) -> String {
    // the same duration buckets the noise band's tiers were fitted against
    const BUCKETS: [(&str, f64, f64); 4] = [
        ("under 100 ns", 0.0, 100.0),
        ("100 ns – 1 µs", 100.0, 1_000.0),
        ("1 – 20 µs", 1_000.0, 20_000.0),
        ("over 20 µs", 20_000.0, f64::MAX),
    ];
    let mut rows = Vec::new();
    for (name, points) in groups {
        for (bucket, low, high) in BUCKETS {
            // every benchmark of this set whose duration falls in this bucket
            let mut spreads: Vec<f64> = points
                .iter()
                .filter(|(duration, _)| *duration >= low && *duration < high)
                .map(|(_, spread)| *spread)
                .collect();
            if spreads.is_empty() {
                continue;
            }
            spreads.sort_by(|left, right| {
                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
            });
            let median = spreads[spreads.len() / 2];
            let worst = spreads[spreads.len() - 1];
            rows.push(vec![
                format!("`{name}`"),
                bucket.to_string(),
                fmt::thousands(spreads.len() as u128),
                format!("{}%", fmt::fixed(median, 1)),
                format!("{}%", fmt::fixed(worst, 1)),
            ]);
        }
    }
    table(
        &["Repeat set", "Benchmark duration", "Benchmarks", "Median spread", "Worst spread"],
        &["---", "---", "---:", "---:", "---:"],
        &rows,
    )
}

/// The percentile distribution of one capture
///
/// # Arguments
///
/// * `capture` - The capture to tabulate
pub fn macro_percentiles(capture: &MacroCaptureV2) -> String {
    // an operation column rather than the two fixed `insert` and `get` columns version 1 had:
    // workloads record whichever operations they drive, and a table with a column per operation
    // would grow a column every time a workload was added
    let mut rows: Vec<Vec<String>> = Vec::new();
    for (id, workload) in &capture.workloads {
        for op in workload.op_names() {
            let mut row = vec![format!("`{id}`"), format!("`{op}`")];
            // every percentile of that operation, in the order a distribution is read
            for metric in ["min", "p50", "p90", "p95", "p99", "avg", "max"] {
                row.push(
                    workload
                        .stat_ns(op, metric)
                        .map(|nanos| fmt::duration_ns(nanos as f64))
                        .unwrap_or_else(|| "-".to_string()),
                );
            }
            rows.push(row);
        }
    }
    table(
        &[
            "Workload", "Op", "min", "p50", "p90", "p95", "p99", "avg", "max",
        ],
        &[
            "---", "---", "---:", "---:", "---:", "---:", "---:", "---:", "---:",
        ],
        &rows,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table has a heading, an alignment row, and one row per record
    #[test]
    fn a_table_has_a_heading_and_an_alignment_row() {
        let rendered = table(
            &["A", "B"],
            &["---", "---:"],
            &[vec!["1".to_string(), "2".to_string()]],
        );
        assert_eq!(rendered, "| A | B |\n| --- | ---: |\n| 1 | 2 |\n");
    }

    /// The scaling table has a column per size and a growth ratio
    #[test]
    fn the_scaling_table_covers_every_size() {
        let families = vec![Family {
            name: "a/insert".to_string(),
            points: vec![(16.0, 100.0), (4096.0, 200.0)],
        }];
        let rendered = micro_scaling(&families);
        assert!(rendered.contains("16 rows"));
        assert!(rendered.contains("4096 rows"));
        // twice as dear at the top end
        assert!(rendered.contains("×2.0"), "{rendered}");
    }

    /// A size one family was not measured at is a gap, not a zero
    #[test]
    fn an_unmeasured_size_is_a_gap() {
        let families = vec![
            Family {
                name: "a".to_string(),
                points: vec![(16.0, 100.0), (256.0, 100.0)],
            },
            Family {
                name: "b".to_string(),
                points: vec![(16.0, 100.0), (4096.0, 100.0)],
            },
        ];
        let rendered = micro_scaling(&families);
        // three columns of sizes, and each row has a dash where it has no measurement
        assert!(rendered.contains("| - |"), "{rendered}");
    }

    /// The noise table buckets by duration, the way the tiers were fitted
    #[test]
    fn the_noise_table_buckets_by_duration() {
        let groups = vec![(
            "performance".to_string(),
            vec![(23.0, 4.6), (500.0, 3.4), (3_100.0, 1.8), (240_000.0, 1.2)],
        )];
        let rendered = noise_buckets(&groups);
        for bucket in ["under 100 ns", "100 ns – 1 µs", "1 – 20 µs", "over 20 µs"] {
            assert!(rendered.contains(bucket), "{bucket} is missing");
        }
    }

    /// A bucket nothing fell into is left out rather than shown as empty
    #[test]
    fn an_empty_bucket_is_left_out() {
        let groups = vec![("performance".to_string(), vec![(23.0, 4.6)])];
        let rendered = noise_buckets(&groups);
        assert!(rendered.contains("under 100 ns"));
        assert!(!rendered.contains("over 20 µs"));
    }
}
