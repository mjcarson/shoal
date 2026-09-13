//! Running a repair in the background of a measured run, and cutting what it cost into a record
//!
//! The background arm asks the harness to ask for a `Repair` of the reference table, in verify
//! mode, part way through its run
//! ([`Workload::background`](crate::workloads::workload::Workload::background)). A thread waits
//! for the mark, makes the request as the process through node zero's control thread, and polls
//! the record each second until every group is done or the run ends; it hands its marks back
//! with what the record said. The client's timeline is then cut at the marks into `before`,
//! `during` and `after` windows, each with its own distribution, and a per second series, so
//! the scrub's cost to the foreground is a window read against another rather than an average
//! that hides it ([C10](../../../../docs/src/distributed/performance.md), Q12).
//!
//! What the scrub read is the difference in every node's integrity counters across the run:
//! the partitions hashed and the bytes read off the archives, summed over the nodes.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use shoal::server::control::AdminSender;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::protocol::error::ErrorCode;

use crate::model::macro_layer::{BackgroundFacts, SecondFacts, WindowFacts};
use crate::workloads::workload::{BackgroundSpec, TimelineSample};

/// How often the record is polled
pub const POLL_EVERY: Duration = Duration::from_secs(1);

/// What the background thread leaves behind: when it did what, on the driver's clock
#[derive(Debug, Clone, Default)]
pub struct Marks {
    /// When the repair was asked for
    pub started_at: Option<Instant>,
    /// When every group of it was done, if that was inside the run
    pub finished_at: Option<Instant>,
    /// How many groups the record covered
    pub groups: u64,
    /// How many of them came to a clean verdict
    pub clean: u64,
    /// The operation
    pub op: Option<uuid::Uuid>,
    /// What went wrong, if something did
    pub error: Option<String>,
}

/// A background repair in progress: the thread driving it
pub struct Injected {
    /// The thread, which hands its marks back when it is done
    handle: std::thread::JoinHandle<Marks>,
    /// Told when the run is over, so the thread stops polling
    stop: mpsc::Sender<()>,
}

impl Injected {
    /// Stops polling, waits for the thread and takes the marks
    ///
    /// # Errors
    ///
    /// The thread panicked, or the repair could not be asked for.
    pub fn finish(self) -> Result<Marks> {
        let _ = self.stop.send(());
        let marks = match self.handle.join() {
            Ok(marks) => marks,
            Err(_) => bail!("the background thread panicked"),
        };
        if let Some(error) = &marks.error {
            bail!("the background repair could not be run: {error}");
        }
        Ok(marks)
    }
}

/// Starts the thread that asks for the repair on the schedule and polls it
///
/// # Arguments
///
/// * `spec` - When to ask, and for what
/// * `admin` - How to ask, as the process
/// * `started` - When the measured phase started, which the schedule counts from
pub fn inject(spec: &BackgroundSpec, admin: AdminSender, started: Instant) -> Result<Injected> {
    let spec = spec.clone();
    let (stop, stopped) = mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("background".to_string())
        .spawn(move || schedule(&spec, &admin, started, &stopped))
        .context("failed to start the background thread")?;
    Ok(Injected { handle, stop })
}

/// Runs one background schedule to its end and reports the marks
///
/// # Arguments
///
/// * `spec` - When to ask, and for what
/// * `admin` - How to ask
/// * `started` - When the measured phase started
/// * `stopped` - Fires when the run is over
fn schedule(spec: &BackgroundSpec, admin: &AdminSender, started: Instant, stopped: &mpsc::Receiver<()>) -> Marks {
    let mut marks = Marks::default();
    // wait for the mark, unless the run ends first
    let until = started + spec.at;
    let wait = until.saturating_duration_since(Instant::now());
    if stopped.recv_timeout(wait).is_ok() {
        marks.error = Some("the run ended before the repair was due".to_string());
        return marks;
    }
    // the request, retried only for a version that moved underneath it
    let op = uuid::Uuid::new_v4();
    let kind = AdminKind::Repair {
        table: spec.table.to_string(),
        tablet: None,
        mode: "verify".to_string(),
        source: None,
        release: false,
    };
    let mut asked = false;
    for _ in 0..8 {
        let version = match admin.admin(AdminRequest {
            op: uuid::Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Members,
        }) {
            Ok(response) => response.topology_version,
            Err(error) => {
                marks.error = Some(format!("reading the topology version: {error:?}"));
                return marks;
            }
        };
        match admin.admin(AdminRequest {
            op,
            expected_version: version,
            kind: kind.clone(),
        }) {
            Ok(response) => match response.outcome {
                Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => {
                    asked = true;
                    break;
                }
                Err(error) if error.code() == ErrorCode::StaleVersion => std::thread::sleep(Duration::from_millis(100)),
                other => {
                    marks.error = Some(format!("the repair was refused: {other:?}"));
                    return marks;
                }
            },
            Err(error) => {
                marks.error = Some(format!("asking for the repair: {error:?}"));
                return marks;
            }
        }
    }
    if !asked {
        marks.error = Some("the topology version kept moving under the request".to_string());
        return marks;
    }
    marks.started_at = Some(Instant::now());
    marks.op = Some(op);
    // poll the record until every group is done, or the run ends
    loop {
        let record = match admin.admin(AdminRequest {
            op: uuid::Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::RepairStatus { op },
        }) {
            Ok(response) => match response.outcome {
                Ok(AdminOutcome::Read(record)) => record,
                other => {
                    marks.error = Some(format!("reading the record: {other:?}"));
                    return marks;
                }
            },
            Err(error) => {
                marks.error = Some(format!("reading the record: {error:?}"));
                return marks;
            }
        };
        let groups = record["groups"].as_object();
        marks.groups = groups.map_or(0, |groups| groups.len() as u64);
        marks.clean = groups.map_or(0, |groups| {
            groups
                .values()
                .filter(|group| group["outcome"]["Clean"].is_object())
                .count() as u64
        });
        let done = groups.is_some_and(|groups| !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done"));
        if done {
            marks.finished_at = Some(Instant::now());
            return marks;
        }
        if stopped.recv_timeout(POLL_EVERY).is_ok() {
            return marks;
        }
    }
}

/// Cuts a timeline at the repair's marks into its windows and its series
///
/// # Arguments
///
/// * `started` - When the measured phase started, on the driver's clock
/// * `marks` - When the thread did what, and what the record said
/// * `timeline` - Every operation of the run, in the order it was sent
/// * `run_for` - How long the run was scheduled for
/// * `partitions` - Partitions the scrubs hashed across every node, over the run
/// * `bytes` - Bytes the scrubs read off the archives across every node, over the run
#[must_use]
pub fn facts(
    started: Instant,
    marks: &Marks,
    timeline: &[TimelineSample],
    run_for: Duration,
    partitions: u64,
    bytes: u64,
) -> BackgroundFacts {
    let started_at = marks.started_at.map(|at| at.saturating_duration_since(started));
    let finished_at = marks.finished_at.map(|at| at.saturating_duration_since(started));
    cut(started_at, finished_at, marks.groups, marks.clean, timeline, run_for, partitions, bytes)
}

/// The pure half of [`facts`], on durations from the start of the run
///
/// # Arguments
///
/// * `started_at` - When the repair was asked for, if it was
/// * `finished_at` - When it was done, if inside the run
/// * `groups` - How many groups the record covered
/// * `clean` - How many came to a clean verdict
/// * `timeline` - Every operation of the run, in the order it was sent
/// * `run_for` - How long the run was scheduled for
/// * `partitions` - Partitions the scrubs hashed
/// * `bytes` - Bytes the scrubs read off the archives
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn cut(
    started_at: Option<Duration>,
    finished_at: Option<Duration>,
    groups: u64,
    clean: u64,
    timeline: &[TimelineSample],
    run_for: Duration,
    partitions: u64,
    bytes: u64,
) -> BackgroundFacts {
    // the run ends at the later of its schedule and its last sample
    let end = timeline
        .iter()
        .map(|sample| sample.at + sample.elapsed)
        .max()
        .unwrap_or(run_for)
        .max(run_for);
    // a repair never asked for is a run with no `during`
    let during_from = started_at.unwrap_or(end);
    let during_to = finished_at.unwrap_or(end);
    let windows = vec![
        super::fault::window("before", Duration::ZERO, during_from, timeline),
        super::fault::window("during", during_from, during_to, timeline),
        super::fault::window("after", during_to, end, timeline),
    ];
    let seconds = end.as_secs() + u64::from(end.subsec_nanos() > 0);
    let series = (0..seconds)
        .map(|second| {
            let from = Duration::from_secs(second);
            let to = Duration::from_secs(second + 1);
            let cut = super::fault::window("second", from, to, timeline);
            SecondFacts {
                second,
                ops: cut.ops,
                errors: cut.errors,
                p50_us: cut.p50_us,
                p99_us: cut.p99_us,
            }
        })
        .collect();
    BackgroundFacts {
        kind: "verify".to_string(),
        started_ms: started_at.map(millis),
        finished_ms: finished_at.map(millis),
        seconds: match (started_at, finished_at) {
            (Some(from), Some(to)) => Some(millis(to.saturating_sub(from)) / 1000),
            _ => None,
        },
        groups,
        clean,
        partitions,
        bytes,
        windows,
        series,
    }
}

/// A duration in whole milliseconds
///
/// # Arguments
///
/// * `duration` - The duration
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The `during` window of a record, for a test or a page
///
/// # Arguments
///
/// * `facts` - The record
#[must_use]
pub fn during(facts: &BackgroundFacts) -> Option<&WindowFacts> {
    facts.windows.iter().find(|window| window.name == "during")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::cut;
    use crate::workloads::workload::TimelineSample;

    /// The record's marks, windows and series come from the timeline and the marks alone, and a
    /// record from before the arm loads without one
    #[test]
    fn background_capture_records_scrub_interference() {
        // thirty seconds of one operation every hundred milliseconds, slower in the middle ten
        let timeline: Vec<TimelineSample> = (0..300u64)
            .map(|index| {
                let at = Duration::from_millis(index * 100);
                let slow = (10..20).contains(&(index / 10));
                TimelineSample {
                    at,
                    elapsed: Duration::from_micros(if slow { 900 } else { 300 }),
                    ok: index != 150,
                }
            })
            .collect();
        let facts = cut(
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(20)),
            4,
            4,
            &timeline,
            Duration::from_secs(30),
            1234,
            56_789,
        );
        assert_eq!(facts.kind, "verify");
        assert_eq!(facts.started_ms, Some(10_000));
        assert_eq!(facts.finished_ms, Some(20_000));
        assert_eq!(facts.seconds, Some(10));
        assert_eq!((facts.groups, facts.clean, facts.partitions, facts.bytes), (4, 4, 1234, 56_789));
        // three windows cut at the marks, the middle one slower and holding the one failure
        assert_eq!(facts.windows.len(), 3);
        let names: Vec<&str> = facts.windows.iter().map(|window| window.name.as_str()).collect();
        assert_eq!(names, ["before", "during", "after"]);
        assert_eq!(facts.windows[0].ops, 100);
        assert_eq!(facts.windows[1].ops, 100);
        assert_eq!(facts.windows[2].ops, 100);
        assert_eq!(facts.windows[1].errors, 1);
        assert!(facts.windows[1].p50_us > facts.windows[0].p50_us, "{:?}", facts.windows);
        assert!(facts.windows[1].p50_us > facts.windows[2].p50_us, "{:?}", facts.windows);
        // one bucket per second, the slow ten visible
        assert_eq!(facts.series.len(), 30);
        assert!(facts.series[15].p50_us > facts.series[5].p50_us);
        assert_eq!(facts.series[15].errors, 1);
        // a repair never asked for is a run that is all `before`
        let none = cut(None, None, 0, 0, &timeline, Duration::from_secs(30), 0, 0);
        assert_eq!(none.windows[0].ops, 300);
        assert_eq!(none.windows[1].ops, 0);
        assert_eq!(none.seconds, None);
        // a record from before the arm carries no background block and loads
        let older: crate::model::macro_layer::ClusterFacts =
            serde_json::from_value(serde_json::json!({
                "nodes": 3, "desired_rf": 3, "active_rf": 3, "write_policy": "quorum",
                "read_policy": "one", "durability": "durable", "driver": "node", "cores": [],
                "tables": 1, "tablets": 4096, "emulated": true
            }))
            .expect("an F43 record loads");
        assert!(older.background.is_none());
    }
}
