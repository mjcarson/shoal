//! A running benchmark beside the cluster's own figures ([F66](../../../../docs/src/features/dataset-benchmarks.md))
//!
//! `shoaladm bench run` draws the stats view with a [`BenchPane`] in it: a strip on the home tab
//! saying which arm is running and what the driver measures, and a tab of its own charting the
//! arm's seconds with its event's marks. The driver's latency is from each query's send; the
//! nodes' own (F65) is from when its bundle's frame arrived, so the two are labelled apart and
//! never drawn on one axis.
//!
//! Nothing here draws or reads a channel: the loop hands each [`BenchEvent`] to
//! [`BenchPane::apply`], and [`super::view`] draws what it holds.

use shoal_loadgen::events::Mark;
use shoal_loadgen::progress::{BenchEvent, Phase};
use shoal_loadgen::results::SecondSample;
use shoal_loadgen::spec::ArmPlan;
use shoal_loadgen::window::WindowSummary;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How many log lines the pane keeps
const LOG_LINES: usize = 200;

/// The driver's cpu, in percent of one cpu, above which a run is said to measure the driver
const DRIVER_BUSY: f64 = 80.0;

/// Where a run is between arms and inside one
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// Still going
    Running,
    /// Asked to stop, and putting everything back
    Stopping,
    /// Finished, with where its capture is
    Finished(PathBuf),
    /// Failed, with why
    Failed(String),
}

/// One arm as the pane lists it
#[derive(Debug, Clone)]
pub struct ArmRow {
    /// The arm
    pub arm: ArmPlan,
    /// What it measured, once done
    pub summary: Option<WindowSummary>,
    /// Why it ended early, if it did
    pub ended_early: Option<String>,
}

/// Everything the pane shows
#[derive(Debug, Clone)]
pub struct BenchPane {
    /// The capture's label
    pub label: String,
    /// Every arm of the run, in order
    pub arms: Vec<ArmRow>,
    /// The arm running, by its place
    pub current: Option<usize>,
    /// What the run is doing
    pub phase: Phase,
    /// When the current arm started, and its warmup and measured time
    pub arm_clock: Option<(Instant, Duration, Duration)>,
    /// When the run started
    pub started: Instant,
    /// The current arm's seconds
    pub seconds: Vec<SecondSample>,
    /// The current arm's marks
    pub marks: Vec<Mark>,
    /// The newest lines worth reading
    pub log: VecDeque<String>,
    /// Whether the run is going, stopping, or done
    pub status: Status,
    /// Whether a quit was pressed and waits to be confirmed
    pub confirm: bool,
}

impl BenchPane {
    /// A pane with nothing planned yet
    ///
    /// # Arguments
    ///
    /// * `now` - When the run started
    #[must_use]
    pub fn new(now: Instant) -> Self {
        BenchPane {
            label: String::new(),
            arms: Vec::new(),
            current: None,
            phase: Phase::Scan,
            arm_clock: None,
            started: now,
            seconds: Vec::new(),
            marks: Vec::new(),
            log: VecDeque::new(),
            status: Status::Running,
            confirm: false,
        }
    }

    /// Whether the run is over
    #[must_use]
    pub fn finished(&self) -> bool {
        matches!(self.status, Status::Finished(_) | Status::Failed(_))
    }

    /// Keep a line, dropping the oldest past the limit
    ///
    /// # Arguments
    ///
    /// * `line` - The line
    fn push_log(&mut self, line: String) {
        self.log.push_back(line);
        while self.log.len() > LOG_LINES {
            self.log.pop_front();
        }
    }

    /// Take one event from the run
    ///
    /// # Arguments
    ///
    /// * `event` - The event
    /// * `now` - When it arrived
    pub fn apply(&mut self, event: BenchEvent, now: Instant) {
        match event {
            BenchEvent::Planned { label, arms } => {
                self.label = label;
                self.arms = arms
                    .into_iter()
                    .map(|arm| ArmRow {
                        arm,
                        summary: None,
                        ended_early: None,
                    })
                    .collect();
            }
            BenchEvent::Phase(phase) => self.phase = phase,
            BenchEvent::ArmStarted { index, warmup, duration, .. } => {
                // a new arm starts its chart and its marks over
                self.current = Some(index);
                self.arm_clock = Some((now, Duration::from_secs(warmup), Duration::from_secs(duration)));
                self.seconds.clear();
                self.marks.clear();
            }
            BenchEvent::Second(sample) => self.seconds.push(sample),
            BenchEvent::Mark(mark) => {
                self.push_log(format!(
                    "{} at {:.1}s{}",
                    mark.kind,
                    mark.at_ms as f64 / 1000.0,
                    mark.note.as_ref().map(|note| format!(": {note}")).unwrap_or_default()
                ));
                self.marks.push(mark);
            }
            BenchEvent::ArmDone { index, summary, ended_early } => {
                if let Some(row) = self.arms.get_mut(index) {
                    row.summary = Some(summary);
                    row.ended_early = ended_early;
                }
            }
            BenchEvent::Log(line) => self.push_log(line),
            BenchEvent::Finished(Ok(dir)) => {
                self.push_log(format!("finished: {}", dir.display()));
                self.status = Status::Finished(dir);
            }
            BenchEvent::Finished(Err(error)) => {
                self.push_log(format!("failed: {error}"));
                self.status = Status::Failed(error);
            }
        }
    }

    /// The newest second of the current arm
    #[must_use]
    pub fn last(&self) -> Option<&SecondSample> {
        self.seconds.last()
    }

    /// How far into the current arm, and how long it is due to run
    ///
    /// # Arguments
    ///
    /// * `now` - The time now
    #[must_use]
    pub fn arm_progress(&self, now: Instant) -> Option<(Duration, Duration)> {
        self.arm_clock
            .map(|(started, warmup, duration)| (now.saturating_duration_since(started), warmup + duration))
    }

    /// What is worth warning about in the current arm
    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        let Some(last) = self.last() else {
            return warnings;
        };
        // a busy driver measures itself as much as the cluster
        if last.driver_cpu_pct > DRIVER_BUSY * num_cpus() as f64 {
            warnings.push(format!("the driver is at {:.0}% cpu", last.driver_cpu_pct));
        }
        // a stalled feed measures the dataset reader rather than the cluster
        if last.summary.feed_wait_ms > 10.0 {
            warnings.push(format!("inserts waited {:.0}ms on the dataset reader", last.summary.feed_wait_ms));
        }
        // timeouts and misses are named, since either means the numbers are not of a healthy run
        for kind in [&last.summary.read, &last.summary.insert] {
            if kind.errors.keys().any(|code| code.contains("Timeout")) {
                warnings.push("queries are timing out".to_string());
            }
        }
        if last.summary.read.misses > 0 {
            warnings.push(format!("{} reads found no row", last.summary.read.misses));
        }
        warnings.dedup();
        warnings
    }

    /// The strip the home tab shows above the cluster's own figures
    ///
    /// # Arguments
    ///
    /// * `now` - The time now
    #[must_use]
    pub fn strip(&self, now: Instant) -> Vec<String> {
        // where the run is
        let total = self.arms.len();
        let arm = self.current.and_then(|index| self.arms.get(index));
        let status = match &self.status {
            Status::Running => self.phase.label(),
            Status::Stopping => "stopping: tearing down and putting the hosts back".to_string(),
            Status::Finished(dir) => format!("finished: {}", dir.display()),
            Status::Failed(error) => format!("failed: {error}"),
        };
        let mut lines = vec![match arm {
            Some(row) => {
                let (elapsed, due) = self.arm_progress(now).unwrap_or_default();
                format!(
                    "bench {} · arm {}/{total} {} run {} · {status} · {:.0}s of {:.0}s · {} elapsed",
                    self.label,
                    self.current.unwrap_or(0) + 1,
                    row.arm.id,
                    row.arm.run,
                    elapsed.as_secs_f64(),
                    due.as_secs_f64(),
                    clock(now.saturating_duration_since(self.started)),
                )
            }
            None => format!("bench {} · {status} · {} elapsed", self.label, clock(now.saturating_duration_since(self.started))),
        }];
        // what the driver measures, from the send
        match self.last() {
            Some(last) => {
                let summary = &last.summary;
                lines.push(format!(
                    "client (from send): read {:.0}/s insert {:.0}/s · driver {:.0}% cpu",
                    summary.read.per_sec, summary.insert.per_sec, last.driver_cpu_pct
                ));
                lines.push(format!(
                    "client per query: read p50 {:.2}ms p99 {:.2}ms · insert p50 {:.2}ms p99 {:.2}ms · per bundle p99 {:.2}ms",
                    summary.read.latency.p50_ms,
                    summary.read.latency.p99_ms,
                    summary.insert.latency.p50_ms,
                    summary.insert.latency.p99_ms,
                    summary.bundle.p99_ms
                ));
                let errors: u64 = summary.read.failed() + summary.insert.failed();
                lines.push(format!(
                    "errors {errors}{}{}",
                    if errors > 0 {
                        format!(" {:?} {:?}", summary.read.errors, summary.insert.errors)
                    } else {
                        String::new()
                    },
                    self.warnings()
                        .iter()
                        .map(|warning| format!(" · {warning}"))
                        .collect::<String>()
                ));
            }
            None => lines.extend([String::new(), String::new(), String::new()]),
        }
        // and the newest mark or line
        lines.push(
            self.marks
                .last()
                .map(|mark| format!("mark: {} at {:.1}s", mark.kind, mark.at_ms as f64 / 1000.0))
                .or_else(|| self.log.back().cloned())
                .unwrap_or_default(),
        );
        lines
    }
}

/// How many cpus this machine has, for judging the driver's share of them
fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

/// A duration as minutes and seconds
///
/// # Arguments
///
/// * `elapsed` - The duration
#[must_use]
pub fn clock(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    format!("{}m{:02}s", secs / 60, secs % 60)
}

#[cfg(test)]
mod tests {
    use super::{BenchPane, Status};
    use shoal_loadgen::events::Mark;
    use shoal_loadgen::progress::{BenchEvent, Phase};
    use shoal_loadgen::results::{SecondPhase, SecondSample};
    use shoal_loadgen::spec::BenchSpec;
    use std::time::{Duration, Instant};

    /// A pane follows a run from its plan to its end, a new arm clearing the last one's seconds
    #[test]
    fn a_pane_follows_a_run() {
        let now = Instant::now();
        let mut pane = BenchPane::new(now);
        let spec = BenchSpec {
            dataset: "d".into(),
            runs: 1,
            bundles: vec![1],
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        pane.apply(BenchEvent::Planned { label: "x".to_string(), arms: arms.clone() }, now);
        assert_eq!(pane.arms.len(), 4);
        pane.apply(BenchEvent::ArmStarted { index: 1, arm: arms[1].clone(), warmup: 2, duration: 10 }, now);
        let mut sample = SecondSample {
            at: 0,
            phase: SecondPhase::Warmup,
            summary: Default::default(),
            driver_cpu_pct: 12.0,
        };
        sample.summary.insert.per_sec = 500.0;
        sample.summary.read.misses = 3;
        pane.apply(BenchEvent::Second(sample), now);
        pane.apply(BenchEvent::Mark(Mark { kind: "kill".to_string(), at_ms: 4000, note: None }), now);
        pane.apply(BenchEvent::Phase(Phase::Measure), now);
        let strip = pane.strip(now + Duration::from_secs(3));
        assert_eq!(strip.len(), 5);
        assert!(strip[0].contains("arm 2/4") && strip[0].contains("measure"), "{}", strip[0]);
        assert!(strip[0].contains("3s of 12s"), "{}", strip[0]);
        assert!(strip[1].starts_with("client (from send)") && strip[1].contains("insert 500/s"));
        assert!(strip[3].contains("3 reads found no row"), "{}", strip[3]);
        assert!(strip[4].contains("kill at 4.0s"));
        // the next arm starts its seconds over
        pane.apply(BenchEvent::ArmStarted { index: 2, arm: arms[2].clone(), warmup: 2, duration: 10 }, now);
        assert!(pane.seconds.is_empty() && pane.marks.is_empty());
        // and the end is kept
        pane.apply(BenchEvent::Finished(Err("no".to_string())), now);
        assert!(pane.finished());
        assert_eq!(pane.status, Status::Failed("no".to_string()));
    }
}
