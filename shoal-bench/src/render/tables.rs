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
use crate::render::arms::{self, Arm};
use crate::render::chart::encryption::Point;
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

/// What encryption cost, at every point of both sweeps
///
/// The relief for the four overhead charts: three of the series colours fall below the contrast
/// floor on the light themes, so the numbers have to be readable without them. It also carries the
/// one thing the charts can only gesture at — whether a pair's runs overlapped, which decides
/// whether its overhead is a result or noise.
///
/// # Arguments
///
/// * `depth` - The pairs of the load depth sweep
/// * `clients` - The pairs of the client count sweep
pub fn encryption(depth: &[Point], clients: &[Point]) -> String {
    // both sweeps in one table, since they share every column but the axis they varied
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut ordered: Vec<(&str, &Point)> = depth
        .iter()
        .map(|point| ("depth", point))
        .chain(clients.iter().map(|point| ("clients", point)))
        .collect();
    // sorted explicitly, because the page has to render the same bytes twice
    ordered.sort_by_key(|(sweep, point)| {
        (*sweep, point.row_bytes, point.depth, point.clients)
    });
    for (sweep, point) in ordered {
        rows.push(vec![
            sweep.to_string(),
            width(point.row_bytes),
            point.depth.to_string(),
            point.clients.to_string(),
            fmt::duration_ns(point.plain_ns),
            fmt::duration_ns(point.tls_ns),
            format!("{:+.1}%", point.overhead_pct()),
            // the verdict the macro layer's own disjointness rule reaches
            if point.separated {
                "result".to_string()
            } else {
                "runs overlapped".to_string()
            },
        ]);
    }
    if rows.is_empty() {
        return String::new();
    }
    table(
        &[
            "Sweep", "Row", "Depth", "Clients", "Plaintext p50", "TLS p50", "Overhead", "Verdict",
        ],
        &["---", "---:", "---:", "---:", "---:", "---:", "---:", "---"],
        &rows,
    )
}

/// How wide a row is, written the way a reader thinks of it
///
/// # Arguments
///
/// * `bytes` - The row width
fn width(bytes: u64) -> String {
    // binary units, matching the chart's own end labels
    if bytes >= 1024 * 1024 {
        format!("{} MiB", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
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

/// The interval a metric was observed over, or a note that there is none
///
/// # Arguments
///
/// * `arm` - The workload to read
/// * `op` - Which operation to read
/// * `metric` - Which percentile to read
fn interval(arm: &Arm<'_>, op: &str, metric: &str) -> String {
    // an arm run once has no interval, which is not the same as having a zero width one - a
    // comparison against it cannot say whether a difference is a result, and the table says so
    match arm.capture.stat_interval_ns(op, metric) {
        Some((low, high)) => format!(
            "{} – {}",
            fmt::duration_ns(low as f64),
            fmt::duration_ns(high as f64)
        ),
        None => "one run".to_string(),
    }
}

/// One value of an arm's, or a dash where it has none
///
/// # Arguments
///
/// * `value` - What was measured, if anything was
/// * `render` - How to write it
fn optional(value: Option<f64>, render: impl Fn(f64) -> String) -> String {
    // a dash rather than a zero: an arm that recorded nothing did not measure nothing
    value.map(render).unwrap_or_else(|| "–".to_string())
}

/// Every cell of the grid, with both halves of its mixture apart
///
/// # Arguments
///
/// * `cells` - The arms to tabulate
pub fn grid_cells(cells: &[Arm<'_>]) -> String {
    // sorted by table, then read share, then width, which is how the charts above are read
    let mut sorted: Vec<&Arm<'_>> = cells.iter().collect();
    sorted.sort_by(|left, right| {
        left.table_kind()
            .cmp(&right.table_kind())
            .then_with(|| left.read_pct().cmp(&right.read_pct()))
            .then_with(|| left.row_bytes().cmp(&right.row_bytes()))
            .then_with(|| left.id.cmp(right.id))
    });
    let rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|arm| {
            vec![
                format!("`{}`", arm.id),
                arm.table_kind()
                    .map(arms::table_label)
                    .unwrap_or_else(|| "–".to_string()),
                arm.read_pct()
                    .map(|pct| format!("{pct}%"))
                    .unwrap_or_else(|| "–".to_string()),
                arm.width_label(),
                optional(arm.stat("read", "p50"), fmt::duration_ns),
                optional(arm.stat("read", "p99"), fmt::duration_ns),
                optional(arm.stat("write", "p50"), fmt::duration_ns),
                optional(arm.stat("write", "p99"), fmt::duration_ns),
                optional(arm.ops_per_sec(), |rate| {
                    fmt::thousands(rate.round() as u128)
                }),
                optional(arm.bytes_per_sec(), fmt::byte_rate),
            ]
        })
        .collect();
    table(
        &[
            "Workload",
            "Table",
            "Reads",
            "Row",
            "read p50",
            "read p99",
            "write p50",
            "write p99",
            "queries/s",
            "payload/s",
        ],
        &[
            "---", "---", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:",
        ],
        &rows,
    )
}

/// Each mixed-width arm against the fixed-width arms either side of its mean
///
/// # Arguments
///
/// * `mixed` - The arms drawing from a width distribution
/// * `fixed` - The arms at a fixed width, on the same tables
pub fn width_mixtures(mixed: &[Arm<'_>], fixed: &[Arm<'_>]) -> String {
    let mut sorted: Vec<&Arm<'_>> = mixed.iter().collect();
    sorted.sort_by(|left, right| {
        left.table_kind()
            .cmp(&right.table_kind())
            .then_with(|| left.row_bytes().cmp(&right.row_bytes()))
    });
    let rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|arm| {
            // the fixed-width arm on the same table whose width is closest to this mixture's mean,
            // which is what a mixture would cost if the cost were linear across its range
            let nearest = fixed
                .iter()
                .filter(|other| other.table_kind() == arm.table_kind())
                .min_by_key(|other| other.row_bytes().abs_diff(arm.row_bytes()));
            let measured = arm.stat("read", "p50");
            let expected = nearest.and_then(|other| other.stat("read", "p50"));
            vec![
                format!("`{}`", arm.row_profile().unwrap_or("–")),
                arm.table_kind()
                    .map(arms::table_label)
                    .unwrap_or_else(|| "–".to_string()),
                fmt::bytes(arm.row_bytes()),
                optional(measured, fmt::duration_ns),
                nearest
                    .map(|other| fmt::bytes(other.row_bytes()))
                    .unwrap_or_else(|| "–".to_string()),
                optional(expected, fmt::duration_ns),
                match (measured, expected) {
                    // a share rather than a ratio, since the interesting case is a few percent
                    // either way and a ratio near one is hard to read
                    (Some(measured), Some(expected)) if expected > 0.0 => {
                        fmt::signed_pct((measured - expected) / expected * 100.0)
                    }
                    _ => "–".to_string(),
                },
            ]
        })
        .collect();
    table(
        &[
            "Mixture",
            "Table",
            "Mean row",
            "read p50",
            "Nearest fixed",
            "its read p50",
            "Difference",
        ],
        &["---", "---", "---:", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Each key distribution's read cost, per table
///
/// # Arguments
///
/// * `points` - The arms of the skew sweep
/// * `distributions` - The distributions to show, in reading order
pub fn skew(points: &[Arm<'_>], distributions: &[String]) -> String {
    let kinds = arms::table_kinds(points);
    let mut rows: Vec<Vec<String>> = Vec::new();
    for kind in &kinds {
        for dist in distributions {
            let Some(arm) = points
                .iter()
                .find(|arm| arm.table_kind() == Some(kind.as_str()) && arm.distribution() == dist)
            else {
                continue;
            };
            rows.push(vec![
                arms::table_label(kind),
                format!("`{dist}`"),
                optional(arm.stat("read", "p50"), fmt::duration_ns),
                optional(arm.stat("read", "p99"), fmt::duration_ns),
                interval(arm, "read", "p50"),
                optional(arm.ops_per_sec(), |rate| {
                    fmt::thousands(rate.round() as u128)
                }),
            ]);
        }
    }
    table(
        &[
            "Table",
            "Keys",
            "read p50",
            "read p99",
            "p50 across runs",
            "queries/s",
        ],
        &["---", "---", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Each rung of the load depth ladder
///
/// # Arguments
///
/// * `rungs` - The arms of the ladder
pub fn depth_ladder(rungs: &[Arm<'_>]) -> String {
    let mut sorted: Vec<&Arm<'_>> = rungs.iter().collect();
    sorted.sort_by_key(|arm| arm.depth());
    let rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|arm| {
            vec![
                arm.depth().to_string(),
                optional(arm.ops_per_sec(), |rate| {
                    fmt::thousands(rate.round() as u128)
                }),
                optional(arm.stat("read", "p50"), fmt::duration_ns),
                optional(arm.stat("read", "p99"), fmt::duration_ns),
                optional(arm.stat("write", "p50"), fmt::duration_ns),
                optional(arm.stat("write", "p99"), fmt::duration_ns),
            ]
        })
        .collect();
    table(
        &[
            "Depth",
            "queries/s",
            "read p50",
            "read p99",
            "write p50",
            "write p99",
        ],
        &["---:", "---:", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Every point of the fan-out curve
///
/// # Arguments
///
/// * `curve` - The fan-out workloads to tabulate
pub fn fanout(curve: &[Arm<'_>]) -> String {
    // sorted by arm, then by key count as a number rather than as a string - `256` sorts before
    // `4` alphabetically, which would draw the curve's rows in an order the curve is not in
    let mut sorted: Vec<&Arm<'_>> = curve.iter().collect();
    sorted.sort_by_key(|arm| {
        let keys = arm
            .id
            .rsplit_once('/')
            .and_then(|(_, tail)| tail.parse::<u64>().ok())
            .unwrap_or(0);
        (arm.id.rsplit_once('/').map(|(head, _)| head.to_string()), keys)
    });
    let rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|arm| {
            let keys = arm
                .id
                .rsplit_once('/')
                .and_then(|(_, tail)| tail.parse::<f64>().ok())
                .unwrap_or(1.0);
            let per_partition = arm.stat("get", "p50").map(|p50| p50 / keys.max(1.0));
            vec![
                format!("`{}`", arm.id),
                fmt::thousands(keys as u128),
                optional(arm.stat("get", "p50"), fmt::duration_ns),
                optional(arm.stat("get", "p99"), fmt::duration_ns),
                optional(per_partition, fmt::duration_ns),
                interval(arm, "get", "p50"),
            ]
        })
        .collect();
    table(
        &[
            "Workload",
            "Keys",
            "get p50",
            "get p99",
            "p50 per partition",
            "p50 across runs",
        ],
        &["---", "---:", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Every transport mode, at each row width it was measured at
///
/// # Arguments
///
/// * `modes` - The transport workloads to tabulate
pub fn transport(modes: &[Arm<'_>]) -> String {
    let mut sorted: Vec<&Arm<'_>> = modes.iter().collect();
    sorted.sort_by_key(|arm| arm.id);
    let rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|arm| {
            vec![
                format!("`{}`", arm.id),
                fmt::bytes(arm.row_bytes()),
                arm.depth().to_string(),
                fmt::millis(arm.capture.median_wall_clock_ns() as f64),
                optional(arm.stat("get", "p50"), fmt::duration_ns),
                optional(arm.stat("get", "p99"), fmt::duration_ns),
            ]
        })
        .collect();
    table(
        &["Workload", "Row", "Depth", "Wall clock", "get p50", "get p99"],
        &["---", "---:", "---:", "---:", "---:", "---:"],
        &rows,
    )
}

/// Each isolating workload beside the control it is read against
///
/// # Arguments
///
/// * `capture` - The capture to read from
/// * `pairs` - Each workload, its control, and what the pair is
pub fn control_pairs(capture: &MacroCaptureV2, pairs: &[(&str, &str, &str)]) -> String {
    let mut rows: Vec<Vec<String>> = Vec::new();
    for (subject, control, what) in pairs {
        // a pair needs both halves; one half alone is not a control for anything
        let (Some(left), Some(right)) = (
            capture.workloads.get(*subject),
            capture.workloads.get(*control),
        ) else {
            continue;
        };
        let subject_wall = left.median_wall_clock_ns() as f64;
        let control_wall = right.median_wall_clock_ns() as f64;
        rows.push(vec![
            format!("`{subject}`"),
            format!("`{control}`"),
            (*what).to_string(),
            fmt::millis(subject_wall),
            fmt::millis(control_wall),
            if control_wall > 0.0 {
                format!("{}×", fmt::fixed(subject_wall / control_wall, 2))
            } else {
                "–".to_string()
            },
        ]);
    }
    if rows.is_empty() {
        return String::new();
    }
    table(
        &["Workload", "Control", "What the pair is", "Wall clock", "Control's", "Ratio"],
        &["---", "---", "---", "---:", "---:", "---:"],
        &rows,
    )
}
