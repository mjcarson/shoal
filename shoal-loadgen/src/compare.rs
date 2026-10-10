//! Whether two captures can be compared, and what comparing them shows
//!
//! **Two captures are compared only when nothing that changes their numbers differs**: the
//! dataset, the spec, the shape of the cluster, each node's machine and governor, the driver's
//! machine, what the nodes ran, and the schema. Every difference is listed, and a comparison
//! goes ahead only when each one is waived by name - `shoal-bench compare` never checked any of
//! these, and a comparison across two datasets reads as a result.
//!
//! A difference in a metric is a **result** only when the two captures' intervals across their
//! runs - lowest to highest - do not overlap, the rule `shoal-bench` used. The effect is the gap
//! between the nearest ends, so it is a lower bound rather than a difference of means.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::results::{ArmResult, Capture, RunResult};
use crate::window::WindowSummary;

/// One fact two captures disagree on
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    /// The fact, by the name `--allow` waives it with
    pub fact: String,
    /// The baseline's value
    pub baseline: String,
    /// The other capture's value
    pub candidate: String,
}

impl std::fmt::Display for Difference {
    /// Write the fact and both values
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} vs {}", self.fact, self.baseline, self.candidate)
    }
}

/// Every fact that changes a capture's numbers, by name, as text
///
/// # Arguments
///
/// * `capture` - The capture
fn facts(capture: &Capture) -> BTreeMap<String, String> {
    // each fact by the name it is waived by
    let provenance = &capture.provenance;
    let mut facts = BTreeMap::new();
    facts.insert("format".to_string(), capture.format.to_string());
    facts.insert("dataset".to_string(), capture.dataset.digest.clone());
    facts.insert("spec".to_string(), capture.spec_digest.clone());
    facts.insert("mode".to_string(), format!("{:?}", provenance.mode));
    facts.insert("flavor".to_string(), provenance.flavor.clone());
    facts.insert("schema".to_string(), format!("{}:{:x}", provenance.schema.db, provenance.schema.fingerprint));
    facts.insert(
        "inventory".to_string(),
        provenance.inventory_shape.clone().unwrap_or_default(),
    );
    facts.insert(
        "driver".to_string(),
        format!(
            "{} ({}, {} cpus, {})",
            provenance.driver.hostname, provenance.driver.cpu, provenance.driver.cores, provenance.driver.governor
        ),
    );
    facts.insert(
        "driver-shares-host".to_string(),
        provenance.driver_shares_host.to_string(),
    );
    // each node's machine, by its name in the inventory
    let nodes: Vec<String> = provenance
        .nodes
        .iter()
        .map(|(name, node)| {
            format!(
                "{name}={} {} cpus {} bytes {}",
                node.host.cpu,
                node.host.cores,
                node.host.memory_bytes,
                node.governor_ran.as_deref().unwrap_or(&node.host.governor)
            )
        })
        .collect();
    facts.insert("nodes".to_string(), nodes.join("; "));
    facts
}

/// Every fact two captures disagree on
///
/// # Arguments
///
/// * `baseline` - The capture compared against
/// * `candidate` - The capture compared
#[must_use]
pub fn differences(baseline: &Capture, candidate: &Capture) -> Vec<Difference> {
    // every fact either has, compared by name
    let left = facts(baseline);
    let right = facts(candidate);
    let mut differences: Vec<Difference> = left
        .iter()
        .filter(|(fact, value)| right.get(*fact) != Some(*value))
        .map(|(fact, value)| Difference {
            fact: fact.clone(),
            baseline: value.clone(),
            candidate: right.get(fact).cloned().unwrap_or_default(),
        })
        .collect();
    // a single run has no interval, so nothing it shows is a result
    for (which, capture) in [("baseline", baseline), ("candidate", candidate)] {
        if capture.spec.runs < 2 {
            differences.push(Difference {
                fact: "runs".to_string(),
                baseline: format!("the {which} has {} run", capture.spec.runs),
                candidate: "at least 2 are needed for an interval".to_string(),
            });
        }
    }
    differences
}

/// The lowest and highest of a metric across an arm's runs
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    /// The lowest
    pub low: f64,
    /// The highest
    pub high: f64,
}

/// What a metric's two intervals say
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The candidate is better, by at least this share of the baseline
    Better {
        /// The gap between the nearest ends, in percent of the baseline's nearer end
        gap_pct: f64,
    },
    /// The candidate is worse, by at least this share of the baseline
    Worse {
        /// The gap between the nearest ends, in percent of the baseline's nearer end
        gap_pct: f64,
    },
    /// The intervals overlap, so no difference is established
    NoDifference,
    /// The arm ran on only one side
    Absent,
}

/// One metric of one arm, on both sides
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricVerdict {
    /// The metric
    pub metric: String,
    /// The baseline's interval
    pub baseline: Option<Interval>,
    /// The candidate's interval
    pub candidate: Option<Interval>,
    /// What they say
    pub verdict: Verdict,
}

/// Every metric of one arm
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmComparison {
    /// The arm
    pub id: String,
    /// Each metric
    pub metrics: Vec<MetricVerdict>,
}

/// Two captures compared
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    /// The differences that were waived to allow it
    pub waived: Vec<Difference>,
    /// Every arm either side ran
    pub arms: Vec<ArmComparison>,
}

/// A metric read off a run, and whether higher is better
struct Metric {
    /// Its name
    name: String,
    /// Whether a higher value is better
    higher_is_better: bool,
    /// Read it off a run, or `None` if the run has nothing to read
    read: Box<dyn Fn(&RunResult) -> Option<f64>>,
}

impl Metric {
    /// A metric of the driver's measured window
    ///
    /// # Arguments
    ///
    /// * `name` - Its name
    /// * `higher_is_better` - Whether a higher value is better
    /// * `read` - Read it off a window, or `None` if the window has nothing to read
    fn new(
        name: impl Into<String>,
        higher_is_better: bool,
        read: impl Fn(&WindowSummary) -> Option<f64> + 'static,
    ) -> Self {
        // read off the measured window, as every metric was before F71
        Metric::of_run(name, higher_is_better, move |run| read(&run.measured))
    }

    /// A metric of the whole run: its devices, its members' memory, its paced stream
    ///
    /// # Arguments
    ///
    /// * `name` - Its name
    /// * `higher_is_better` - Whether a higher value is better
    /// * `read` - Read it off a run, or `None` if the run has nothing to read
    fn of_run(
        name: impl Into<String>,
        higher_is_better: bool,
        read: impl Fn(&RunResult) -> Option<f64> + 'static,
    ) -> Self {
        Metric {
            name: name.into(),
            higher_is_better,
            read: Box::new(read),
        }
    }
}

/// Every metric a comparison of some arms reads
///
/// The driver's own: each kind's rate and latency, the bundles', and since
/// [F69](../../docs/src/features/driver-operation-kinds.md) the bytes both ways, which a capture
/// from before it has none of and so reads as absent rather than as a regression. Then each
/// kind a run was handed beside read and insert, by name, the same three a kind. Since
/// [F71](../../docs/src/features/bench-device-memory.md) what the devices wrote for each byte
/// sent and the largest member's memory, since
/// [F72](../../docs/src/features/bench-paced-stream.md) the paced stream's tail, and since
/// [F74](../../docs/src/features/client-routing.md) the hops the members took; a capture
/// without them reads as absent the same way.
///
/// # Arguments
///
/// * `arms` - The arms compared, either side's
fn metrics<'a>(arms: impl IntoIterator<Item = &'a ArmResult>) -> Vec<Metric> {
    let mib = |bytes: f64| bytes / (1024.0 * 1024.0);
    let mut metrics = vec![
        Metric::new("read/s", true, |w| (w.read.ok > 0).then_some(w.read.per_sec)),
        Metric::new("insert/s", true, |w| (w.insert.ok > 0).then_some(w.insert.per_sec)),
        Metric::new("read p50 ms", false, |w| (w.read.ok > 0).then_some(w.read.latency.p50_ms)),
        Metric::new("read p99 ms", false, |w| (w.read.ok > 0).then_some(w.read.latency.p99_ms)),
        Metric::new("insert p50 ms", false, |w| {
            (w.insert.ok > 0).then_some(w.insert.latency.p50_ms)
        }),
        Metric::new("insert p99 ms", false, |w| {
            (w.insert.ok > 0).then_some(w.insert.latency.p99_ms)
        }),
        Metric::new("bundle p99 ms", false, |w| (w.bundle.count > 0).then_some(w.bundle.p99_ms)),
        Metric::new("sent MiB/s", true, move |w| {
            (w.bytes_sent > 0).then_some(mib(w.sent_per_sec))
        }),
        Metric::new("received MiB/s", true, move |w| {
            (w.bytes_received > 0).then_some(mib(w.received_per_sec))
        }),
        // what the hosts' devices wrote, against what the driver sent them over the same time
        Metric::of_run("device bytes / sent byte", false, RunResult::device_bytes_per_sent_byte),
        // the largest member's resident peak, and its indexes' bytes once the run was done
        Metric::of_run("peak resident MiB", false, move |run| {
            run.peak_resident().values().max().map(|bytes| mib(*bytes as f64))
        }),
        Metric::of_run("index MiB", false, move |run| {
            run.last_memory()
                .and_then(|memory| memory.values().map(|member| member.index_bytes()).max())
                .map(|bytes| mib(bytes as f64))
        }),
        // the hops the members took for the run's queries, which routing by topology removes
        // ([F74](../../docs/src/features/client-routing.md))
        Metric::of_run("member hops/s", false, RunResult::mean_hops_per_sec),
        // the paced stream's tail, which is what a light neighbour of the main load feels
        Metric::of_run("paced read p99 ms", false, |run| {
            run.paced
                .as_ref()
                .filter(|paced| paced.measured.read.ok > 0)
                .map(|paced| paced.measured.read.latency.p99_ms)
        }),
        Metric::of_run("paced insert p99 ms", false, |run| {
            run.paced
                .as_ref()
                .filter(|paced| paced.measured.insert.ok > 0)
                .map(|paced| paced.measured.insert.latency.p99_ms)
        }),
    ];
    // every supplied kind either side ran, in name order
    let mut names: Vec<String> = arms
        .into_iter()
        .flat_map(|arm| arm.runs.iter())
        .flat_map(|run| run.measured.kinds.keys().cloned())
        .collect();
    names.sort();
    names.dedup();
    for name in names {
        let rate = name.clone();
        metrics.push(Metric::new(format!("{name}/s"), true, move |w| {
            w.kinds.get(&rate).filter(|stats| stats.ok > 0).map(|stats| stats.per_sec)
        }));
        let p50 = name.clone();
        metrics.push(Metric::new(format!("{name} p50 ms"), false, move |w| {
            w.kinds.get(&p50).filter(|stats| stats.ok > 0).map(|stats| stats.latency.p50_ms)
        }));
        let p99 = name.clone();
        metrics.push(Metric::new(format!("{name} p99 ms"), false, move |w| {
            w.kinds.get(&p99).filter(|stats| stats.ok > 0).map(|stats| stats.latency.p99_ms)
        }));
    }
    metrics
}

/// The interval of a metric across an arm's runs
///
/// # Arguments
///
/// * `arm` - The arm
/// * `metric` - The metric
fn interval(arm: &ArmResult, metric: &Metric) -> Option<Interval> {
    // every run that has the metric
    let values: Vec<f64> = arm
        .runs
        .iter()
        .filter_map(|run| (metric.read)(run))
        .collect();
    if values.is_empty() {
        return None;
    }
    let low = values.iter().copied().fold(f64::INFINITY, f64::min);
    let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Some(Interval { low, high })
}

/// Judge two intervals of one metric
///
/// # Arguments
///
/// * `baseline` - The baseline's interval
/// * `candidate` - The candidate's interval
/// * `higher_is_better` - Whether a higher value is better
#[must_use]
pub fn judge(baseline: Interval, candidate: Interval, higher_is_better: bool) -> Verdict {
    // the gap between the nearest ends, as a share of the baseline's nearer end
    let gap = |from: f64, to: f64| {
        if from.abs() < f64::EPSILON {
            return 0.0;
        }
        (to - from).abs() / from.abs() * 100.0
    };
    if candidate.low > baseline.high {
        let gap_pct = gap(baseline.high, candidate.low);
        return if higher_is_better {
            Verdict::Better { gap_pct }
        } else {
            Verdict::Worse { gap_pct }
        };
    }
    if candidate.high < baseline.low {
        let gap_pct = gap(baseline.low, candidate.high);
        return if higher_is_better {
            Verdict::Worse { gap_pct }
        } else {
            Verdict::Better { gap_pct }
        };
    }
    Verdict::NoDifference
}

/// Compare a candidate capture against a baseline
///
/// # Arguments
///
/// * `baseline` - The capture compared against
/// * `candidate` - The capture compared
/// * `allow` - The facts allowed to differ, by name
///
/// # Errors
///
/// Every difference that was not waived, when there is any.
pub fn compare(
    baseline: &Capture,
    candidate: &Capture,
    allow: &[String],
) -> Result<Comparison, Vec<Difference>> {
    // refuse on anything not waived
    let (waived, refused): (Vec<Difference>, Vec<Difference>) = differences(baseline, candidate)
        .into_iter()
        .partition(|difference| allow.contains(&difference.fact));
    if !refused.is_empty() {
        return Err(refused);
    }
    // every arm either side ran, baseline order first
    let mut ids: Vec<&str> = baseline.arms.iter().map(|arm| arm.id.0.as_str()).collect();
    for arm in &candidate.arms {
        if !ids.contains(&arm.id.0.as_str()) {
            ids.push(&arm.id.0);
        }
    }
    let find = |capture: &'_ Capture, id: &str| capture.arms.iter().find(|arm| arm.id.0 == id).cloned();
    let arms = ids
        .into_iter()
        .map(|id| {
            let left = find(baseline, id);
            let right = find(candidate, id);
            // a run that wrapped its inserts measured overwrites, never compared with new rows
            let wrapped = |arm: &Option<ArmResult>| {
                arm.as_ref()
                    .is_some_and(|arm| arm.runs.iter().any(|run| run.wrapped))
            };
            let mixed_wrap = wrapped(&left) != wrapped(&right);
            let metrics = metrics(left.iter().chain(right.iter()))
                .iter()
                .filter_map(|metric| {
                    let base = left.as_ref().and_then(|arm| interval(arm, metric));
                    let cand = right.as_ref().and_then(|arm| interval(arm, metric));
                    // a metric neither side has is not a row
                    if base.is_none() && cand.is_none() {
                        return None;
                    }
                    let verdict = match (base, cand) {
                        (Some(_), Some(_)) if mixed_wrap => Verdict::Absent,
                        (Some(base), Some(cand)) => judge(base, cand, metric.higher_is_better),
                        _ => Verdict::Absent,
                    };
                    Some(MetricVerdict {
                        metric: metric.name.clone(),
                        baseline: base,
                        candidate: cand,
                        verdict,
                    })
                })
                .collect();
            ArmComparison {
                id: id.to_string(),
                metrics,
            }
        })
        .collect();
    Ok(Comparison { waived, arms })
}

#[cfg(test)]
mod tests {
    use super::{compare, differences, judge, Interval, Verdict};
    use crate::results::{
        ArmResult, Capture, DatasetFacts, DeviceCounters, HostDevices, MemberMemory, PacedResult, Provenance,
        RunResult, SecondPhase, SecondSample, ServerSample, FORMAT,
    };
    use crate::spec::{ArmId, BenchSpec, EventKind};
    use crate::window::WindowSummary;
    use std::collections::BTreeMap;

    /// A run whose reads did this many a second
    ///
    /// # Arguments
    ///
    /// * `per_sec` - Reads a second
    fn run(per_sec: f64) -> RunResult {
        let mut measured = WindowSummary::default();
        measured.read.ok = 100;
        measured.read.per_sec = per_sec;
        RunResult {
            run: 0,
            order: 0,
            started_at: String::new(),
            measured,
            warmup: WindowSummary::default(),
            series: Vec::new(),
            ended_early: None,
            feeds: BTreeMap::new(),
            wrapped: false,
            verify: None,
            event: None,
            server_series: Vec::new(),
            unfigured: Vec::new(),
            figures_unread: None,
            driver_cpu_peak_pct: 0.0,
            progress_dropped: 0,
            devices: Vec::new(),
            devices_unread: None,
            paced: None,
        }
    }

    /// A capture of one arm whose runs read this many a second
    ///
    /// # Arguments
    ///
    /// * `rates` - Each run's reads a second
    fn capture(rates: &[f64]) -> Capture {
        let spec = BenchSpec {
            runs: rates.len() as u32,
            ..BenchSpec::default()
        };
        Capture {
            format: FORMAT,
            label: "x".to_string(),
            provenance: Provenance::default(),
            spec_digest: spec.digest(),
            spec,
            dataset: DatasetFacts::new(Vec::new()),
            preload: None,
            arms: vec![ArmResult {
                id: ArmId("read100/b1/none".to_string()),
                workload: "read100".to_string(),
                bundle: 1,
                in_flight: 4,
                overrides: None,
                event: EventKind::None,
                runs: rates.iter().map(|rate| run(*rate)).collect(),
            }],
            complete: true,
            error: None,
        }
    }

    /// Disjoint intervals are a result in the right direction; overlapping ones are not
    #[test]
    fn only_disjoint_intervals_are_a_result() {
        let base = Interval { low: 100.0, high: 110.0 };
        let faster = Interval { low: 121.0, high: 130.0 };
        let overlapping = Interval { low: 105.0, high: 130.0 };
        assert_eq!(judge(base, faster, true), Verdict::Better { gap_pct: 10.0 });
        // the same numbers are worse when they are latencies
        assert_eq!(judge(base, faster, false), Verdict::Worse { gap_pct: 10.0 });
        assert_eq!(judge(base, overlapping, true), Verdict::NoDifference);
    }

    /// Two comparable captures are judged arm by arm
    #[test]
    fn comparable_captures_are_judged() {
        let comparison = compare(&capture(&[100.0, 110.0]), &capture(&[121.0, 130.0]), &[]).unwrap();
        let reads = &comparison.arms[0].metrics[0];
        assert_eq!(reads.metric, "read/s");
        assert!(matches!(reads.verdict, Verdict::Better { .. }));
        // inserts were never run, so they are not a row
        assert_eq!(comparison.arms[0].metrics.len(), 3);
    }

    /// A capture of another dataset is refused unless that fact is waived by name
    #[test]
    fn incomparable_captures_are_refused_by_fact() {
        let base = capture(&[100.0, 110.0]);
        let mut other = capture(&[100.0, 110.0]);
        other.dataset.digest = "something else".to_string();
        other.provenance.driver.hostname = "elsewhere".to_string();
        let refused = compare(&base, &other, &[]).unwrap_err();
        let facts: Vec<&str> = refused.iter().map(|difference| difference.fact.as_str()).collect();
        assert_eq!(facts, vec!["dataset", "driver"]);
        let waived = compare(&base, &other, &["dataset".to_string(), "driver".to_string()]).unwrap();
        assert_eq!(waived.waived.len(), 2);
    }

    /// A single run is no interval, and says so
    #[test]
    fn a_single_run_is_refused() {
        let refused = differences(&capture(&[100.0]), &capture(&[100.0, 101.0]));
        assert!(refused.iter().any(|difference| difference.fact == "runs"));
    }

    /// A supplied kind and the bytes both ways are compared, and absent from a capture without
    /// them rather than judged against nothing (F69)
    #[test]
    fn supplied_kinds_and_bytes_are_compared() {
        // the candidate's runs looked something up and counted their bytes; the baseline's did not
        let base = capture(&[100.0, 110.0]);
        let mut candidate = capture(&[100.0, 110.0]);
        for (at, run) in candidate.arms[0].runs.iter_mut().enumerate() {
            let lookup = run.measured.kinds.entry("lookup".to_string()).or_default();
            lookup.ok = 10;
            lookup.per_sec = 5.0 + at as f64;
            run.measured.bytes_sent = 1 << 20;
            run.measured.sent_per_sec = (1 << 20) as f64;
        }
        let comparison = compare(&base, &candidate, &[]).unwrap();
        let names: Vec<&str> = comparison.arms[0]
            .metrics
            .iter()
            .map(|metric| metric.metric.as_str())
            .collect();
        assert!(names.contains(&"lookup/s") && names.contains(&"sent MiB/s"), "{names:?}");
        for metric in &comparison.arms[0].metrics {
            if metric.metric == "lookup/s" || metric.metric == "sent MiB/s" {
                assert_eq!(metric.verdict, Verdict::Absent, "{}", metric.metric);
            }
        }
        let sent = comparison.arms[0]
            .metrics
            .iter()
            .find(|metric| metric.metric == "sent MiB/s")
            .and_then(|metric| metric.candidate);
        assert_eq!(sent.map(|interval| interval.low), Some(1.0));
    }

    /// The devices, the members' memory and the paced stream are compared, from the whole run,
    /// and are absent from a capture without them (F71, F72)
    #[test]
    fn devices_memory_and_the_paced_stream_are_compared() {
        // the candidate's runs counted their devices and memory and ran a paced stream
        let base = capture(&[100.0, 110.0]);
        let mut candidate = capture(&[100.0, 110.0]);
        for (at, run) in candidate.arms[0].runs.iter_mut().enumerate() {
            // two seconds that sent a MiB each, warmup and drain alike
            for _ in 0..2 {
                run.series.push(SecondSample {
                    at: 0,
                    phase: SecondPhase::Warmup,
                    summary: WindowSummary {
                        bytes_sent: 1 << 20,
                        ..WindowSummary::default()
                    },
                    driver_cpu_pct: 0.0,
                });
            }
            // and two hosts whose devices wrote three MiB each, five with the second run
            run.devices = ["titan", "hyperion"]
                .into_iter()
                .map(|host| HostDevices {
                    host: host.to_string(),
                    nodes: vec![host.to_string()],
                    devices: vec![DeviceCounters {
                        device: "nvme0n1p2".to_string(),
                        written_bytes: (3 + 2 * at as u64) << 20,
                        ..DeviceCounters::default()
                    }],
                    unresolved: Vec::new(),
                })
                .collect();
            // a member whose resident set peaked at 300 MiB, and whose indexes ended at 6 MiB
            for resident in [100u64, 300, 200] {
                let memory = MemberMemory {
                    resident_bytes: resident << 20,
                    archive_map_bytes: 4 << 20,
                    table_index_bytes: 1 << 20,
                    wal_index_bytes: 1 << 20,
                    ..MemberMemory::default()
                };
                run.server_series.push(ServerSample {
                    memory: BTreeMap::from([("titan".to_string(), memory)]),
                    ..ServerSample::default()
                });
            }
            // and a paced stream of reads with a 2 ms tail
            let mut paced = PacedResult::default();
            paced.measured.read.ok = 10;
            paced.measured.read.latency.p99_ms = 2.0;
            run.paced = Some(paced);
        }
        let comparison = compare(&base, &candidate, &[]).unwrap();
        let read = |name: &str| {
            let metric = comparison.arms[0]
                .metrics
                .iter()
                .find(|metric| metric.metric == name)
                .unwrap_or_else(|| panic!("{name} is not compared"));
            assert_eq!(metric.verdict, Verdict::Absent, "{name}");
            metric.candidate.expect("the candidate has it")
        };
        // six MiB written for two sent is three a byte, and ten for two is five
        let amplification = read("device bytes / sent byte");
        assert_eq!((amplification.low, amplification.high), (3.0, 5.0));
        assert_eq!(read("peak resident MiB").high, 300.0);
        assert_eq!(read("index MiB").low, 6.0);
        assert_eq!(read("paced read p99 ms").low, 2.0);
        // a paced stream that inserted nothing has no insert tail to compare
        assert!(!comparison.arms[0].metrics.iter().any(|metric| metric.metric == "paced insert p99 ms"));
    }
}
