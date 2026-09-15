//! Injecting a fault into a placed cluster while an arm runs, and cutting what the client saw
//!
//! # The harness does it, on its own thread, on the driver's clock
//!
//! A fault arm asks for a peer to be killed at a time and started again later
//! ([`FaultSpec`]). The peers are the harness's children, so the harness does it: a thread of
//! its own sleeps until the schedule says, kills the child, sleeps again, starts the same
//! staged identity again and waits until it is placed, and leaves a mark for each of those on
//! the clock the timed driver stamps its samples with. The workload sees none of it, which is
//! the point - it is a client, and a client is what an outage is measured by
//! ([C7](../../../../docs/src/distributed/failover.md)).
//!
//! # The windows are cut by the client, not by the schedule
//!
//! The schedule says when a process died. When the cluster stopped answering is what the
//! client's first failed operation after that says, and when it was back is the first
//! operation after which a sustained run succeeded ([`SUSTAINED`]). The three windows -
//! `before`, `during`, `after` - are cut there, and each keeps its own distribution, so the
//! outage is a number beside the run rather than a bump averaged into its tail. The per second
//! series is what keeps it visible when the windows are read as three numbers
//! ([C10](../../../../docs/src/distributed/performance.md): record the time series, do not
//! average the outage away).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};

use crate::model::macro_layer::{FaultFacts, SecondFacts, WindowFacts};
use crate::workloads::harness::cluster::{self, PeerChild, Staged};
use crate::workloads::harness::timer::Samples;
use crate::workloads::workload::{FaultSpec, TimelineSample};

/// How long a run of successes has to last before the client counts itself recovered
///
/// Two seconds: longer than a heartbeat round at any failover base an arm runs at, so a
/// success between two refusals during an election is not called the end of the outage, and
/// short beside the run, so the `after` window has most of what follows the fault.
pub const SUSTAINED: Duration = Duration::from_secs(2);

/// What the fault thread leaves behind: when it did what, on the driver's clock
#[derive(Debug, Clone, Default)]
pub struct Marks {
    /// When the peer was killed
    pub killed_at: Option<Instant>,
    /// When the peer was serving and placed again
    pub restarted_at: Option<Instant>,
    /// What the returning peer looked like each second after that, if it was watched
    /// ([F43](../../../../docs/src/features/node-recovery.md))
    pub catchup: Option<Vec<super::catchup::Sample>>,
    /// What went wrong bringing it back, if something did
    pub error: Option<String>,
}

/// A fault in progress: the thread injecting it
pub struct Injected {
    /// The thread, which hands its marks back when it is done
    handle: std::thread::JoinHandle<Marks>,
}

impl Injected {
    /// Waits for the schedule to finish and takes the marks
    ///
    /// # Errors
    ///
    /// The thread panicked, or the peer could not be brought back.
    pub fn finish(self) -> Result<Marks> {
        // the thread runs the whole schedule, so this waits out a restart the run outlasted
        let marks = match self.handle.join() {
            Ok(marks) => marks,
            Err(_) => bail!("the fault thread panicked"),
        };
        if let Some(error) = &marks.error {
            bail!("the fault could not be completed: {error}");
        }
        Ok(marks)
    }
}

/// Starts the thread that injects a fault on a schedule
///
/// # Arguments
///
/// * `spec` - What to do and when
/// * `peers` - The placed peers, node one first, which the thread kills into and respawns into
/// * `staged` - The cluster the peers were staged from, for the respawn
/// * `id` - The workload
/// * `conf` - The base configuration file the respawned peer resolves from
/// * `scale` - The scale it resolves at
/// * `started` - When the measured phase started, which the schedule counts from
/// * `watch_until` - Sample the returning peer's catch-up until this instant, if asked
///
/// # Errors
///
/// The spec names node zero, which is this process, or a node the placement does not have.
#[allow(clippy::too_many_arguments)]
pub fn inject(
    spec: &FaultSpec,
    peers: Arc<Mutex<Vec<PeerChild>>>,
    staged: &Staged,
    id: &str,
    conf: &Path,
    scale: &str,
    started: Instant,
    watch_until: Option<Instant>,
) -> Result<Injected> {
    // node zero is the driver's own process, and a peer the placement never had cannot die
    if spec.node == 0 {
        bail!("{id} asks to kill node 0, which is this process");
    }
    let position = usize::try_from(spec.node).unwrap_or(usize::MAX);
    if position >= staged.nodes.len() {
        bail!(
            "{id} asks to kill node {}, which the placement does not have",
            spec.node
        );
    }
    let spec = spec.clone();
    let staged = staged.clone();
    let id = id.to_string();
    let conf: PathBuf = conf.to_path_buf();
    let scale = scale.to_string();
    let handle = std::thread::Builder::new()
        .name("fault".to_string())
        .spawn(move || {
            schedule(
                &spec,
                &peers,
                &staged,
                &id,
                &conf,
                &scale,
                started,
                watch_until,
            )
        })
        .context("failed to start the fault thread")?;
    Ok(Injected { handle })
}

/// Runs one fault schedule to its end and reports the marks
///
/// # Arguments
///
/// * `spec` - What to do and when
/// * `peers` - The placed peers
/// * `staged` - The cluster they were staged from
/// * `id` - The workload
/// * `conf` - The base configuration file
/// * `scale` - The scale
/// * `started` - When the measured phase started
/// * `watch_until` - Sample the returning peer's catch-up until this instant, if asked
#[allow(clippy::too_many_arguments)]
fn schedule(
    spec: &FaultSpec,
    peers: &Mutex<Vec<PeerChild>>,
    staged: &Staged,
    id: &str,
    conf: &Path,
    scale: &str,
    started: Instant,
    watch_until: Option<Instant>,
) -> Marks {
    let mut marks = Marks::default();
    // wait until the schedule says, from the start of the measured phase and not from now
    sleep_until(started + spec.at);
    // kill the peer at that position; the children are held node one first
    {
        let mut peers = match peers.lock() {
            Ok(peers) => peers,
            Err(poisoned) => poisoned.into_inner(),
        };
        match peers.iter_mut().find(|peer| peer.index == spec.node) {
            Some(peer) => {
                if let Err(error) = peer.kill() {
                    marks.error =
                        Some(format!("node {} could not be killed: {error:#}", spec.node));
                    return marks;
                }
            }
            None => {
                marks.error = Some(format!("node {} is not among the peers", spec.node));
                return marks;
            }
        }
    }
    marks.killed_at = Some(Instant::now());
    // a node killed for good stays that way: the remove arm lets the grace take it out
    // ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    if !spec.restart {
        return marks;
    }
    // leave it dead for as long as the schedule says
    sleep_until(started + spec.at + spec.restart_after);
    // start the same identity again, from the same staged file and the same directory, so it
    // comes back as the member it was and catches up rather than joining as somebody new
    let respawned = match cluster::spawn_peer(staged, spec.node, id, conf, scale) {
        Ok(peer) => peer,
        Err(error) => {
            marks.error = Some(format!(
                "node {} could not be started again: {error:#}",
                spec.node
            ));
            return marks;
        }
    };
    {
        let mut peers = match peers.lock() {
            Ok(peers) => peers,
            Err(poisoned) => poisoned.into_inner(),
        };
        match peers.iter_mut().find(|peer| peer.index == spec.node) {
            Some(slot) => *slot = respawned,
            None => peers.push(respawned),
        }
    }
    // and wait until it holds the placement again, on a runtime of this thread's own since the
    // harness's is busy driving the run
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            marks.error = Some(format!("no runtime for the readiness wait: {error}"));
            return marks;
        }
    };
    if let Err(error) = cluster::wait_peer_placed(staged, spec.node, &runtime) {
        marks.error = Some(format!("node {} did not come back: {error:#}", spec.node));
        return marks;
    }
    marks.restarted_at = Some(Instant::now());
    // a catch-up arm watches the returning peer from here until it converges or the run ends
    if let Some(until) = watch_until {
        match super::catchup::sample(staged, spec.node, started, until, &runtime) {
            Ok(samples) => marks.catchup = Some(samples),
            Err(error) => {
                marks.error = Some(format!(
                    "node {} could not be watched: {error:#}",
                    spec.node
                ))
            }
        }
    }
    marks
}

/// Sleeps until an instant, or not at all if it has passed
///
/// # Arguments
///
/// * `until` - When to wake
fn sleep_until(until: Instant) {
    // saturating: a schedule already behind sleeps for nothing rather than panicking
    let now = Instant::now();
    if until > now {
        std::thread::sleep(until - now);
    }
}

/// Cuts a timed run into what the client saw before, during and after a fault
///
/// Every time is measured from `started`, which is the driver's own start, so the marks and
/// the samples are on one axis. See the module header for where the windows are cut.
///
/// # Arguments
///
/// * `kind` - What was done, `kill`
/// * `node` - The node it was done to
/// * `started` - When the measured phase started, on the driver's clock
/// * `marks` - When the fault thread did what
/// * `timeline` - Every operation of the run, in the order it was sent
/// * `run_for` - How long the run was scheduled for, which bounds the series
#[must_use]
pub fn facts(
    kind: &str,
    node: u32,
    started: Instant,
    marks: &Marks,
    timeline: &[TimelineSample],
    run_for: Duration,
) -> FaultFacts {
    // the marks, on the driver's axis; a kill that never happened is at the end of the run
    let at = marks
        .killed_at
        .map_or(run_for, |killed| killed.saturating_duration_since(started));
    let restarted_at = marks
        .restarted_at
        .map(|restarted| restarted.saturating_duration_since(started));
    cut(kind, node, at, restarted_at, timeline, run_for)
}

/// Cuts a timeline at a fault into its windows and its series
///
/// The pure half of [`facts`], on durations from the start of the run, so a test can hand it
/// a timeline it made up.
///
/// # Arguments
///
/// * `kind` - What was done
/// * `node` - The node it was done to
/// * `at` - When, from the start of the run
/// * `restarted_at` - When the node was back, if it came back
/// * `timeline` - Every operation of the run, in the order it was sent
/// * `run_for` - How long the run was scheduled for
#[must_use]
pub fn cut(
    kind: &str,
    node: u32,
    at: Duration,
    restarted_at: Option<Duration>,
    timeline: &[TimelineSample],
    run_for: Duration,
) -> FaultFacts {
    // the run ends at the later of its schedule and its last sample
    let end = timeline
        .iter()
        .map(|sample| sample.at + sample.elapsed)
        .max()
        .unwrap_or(run_for)
        .max(run_for);
    // the first failure at or after the kill is where the outage starts
    let first_failure = timeline
        .iter()
        .find(|sample| !sample.ok && sample.at >= at)
        .map(|sample| sample.at);
    // and the first success after which nothing fails for a sustained run is where it ends
    let recovered = first_failure.and_then(|failed| {
        timeline
            .iter()
            .filter(|sample| sample.ok && sample.at >= failed)
            .find(|candidate| {
                let until = candidate.at + SUSTAINED;
                !timeline
                    .iter()
                    .any(|other| !other.ok && other.at >= candidate.at && other.at < until)
            })
            .map(|sample| sample.at)
    });
    // the windows are cut at what the client saw, and at the kill when it saw nothing
    let during_from = first_failure.unwrap_or(at);
    let during_to = recovered.unwrap_or(end);
    let windows = vec![
        window("before", Duration::ZERO, during_from, timeline),
        window("during", during_from, during_to, timeline),
        window("after", during_to, end, timeline),
    ];
    // one bucket per second of the run, so the outage is a dip a reader can see
    let seconds = end.as_secs() + u64::from(end.subsec_nanos() > 0);
    let series = (0..seconds)
        .map(|second| {
            let from = Duration::from_secs(second);
            let to = Duration::from_secs(second + 1);
            let cut = window("second", from, to, timeline);
            SecondFacts {
                second,
                ops: cut.ops,
                errors: cut.errors,
                p50_us: cut.p50_us,
                p99_us: cut.p99_us,
            }
        })
        .collect();
    FaultFacts {
        kind: kind.to_string(),
        node,
        at_ms: millis(at),
        restarted_at_ms: restarted_at.map(millis),
        first_failure_ms: first_failure.map(millis),
        recovered_ms: recovered.map(millis),
        outage_ms: first_failure
            .zip(recovered)
            .map(|(failed, back)| millis(back.saturating_sub(failed))),
        sustained_ms: millis(SUSTAINED),
        windows,
        series,
    }
}

/// One window of a timeline, with the distribution of the successes in it
///
/// # Arguments
///
/// * `name` - What to call it
/// * `from` - Where it starts, inclusive
/// * `to` - Where it ends, exclusive
/// * `timeline` - Every operation of the run
pub(super) fn window(
    name: &str,
    from: Duration,
    to: Duration,
    timeline: &[TimelineSample],
) -> WindowFacts {
    // an operation is in the window it was sent in
    let mut ops = 0u64;
    let mut errors = 0u64;
    let mut samples = Samples::default();
    for sample in timeline
        .iter()
        .filter(|sample| sample.at >= from && sample.at < to)
    {
        ops += 1;
        if sample.ok {
            samples.record(sample.elapsed);
        } else {
            errors += 1;
        }
    }
    // the successes' distribution; a window with none is all zeros, and says so by its count
    let stats = samples.summarize();
    WindowFacts {
        name: name.to_string(),
        from_ms: millis(from),
        to_ms: millis(to),
        ops,
        errors,
        p50_us: micros(&stats.p50),
        p99_us: micros(&stats.p99),
        max_us: micros(&stats.max),
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

/// A duration the artifact split into parts, in whole microseconds
///
/// # Arguments
///
/// * `parts` - The duration
fn micros(parts: &crate::model::macro_layer::DurationParts) -> u64 {
    parts.secs.saturating_mul(1_000_000) + u64::from(parts.nanos / 1_000)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{SUSTAINED, cut};
    use crate::model::macro_layer::{ClusterFacts, FaultFacts};
    use crate::workloads::workload::TimelineSample;

    /// A sample at a time, in milliseconds
    fn sample(at_ms: u64, elapsed_us: u64, ok: bool) -> TimelineSample {
        TimelineSample {
            at: Duration::from_millis(at_ms),
            elapsed: Duration::from_micros(elapsed_us),
            ok,
        }
    }

    /// The outage stays visible as three windows and a series, and never averages into the run
    ///
    /// The [C10](../../../../docs/src/distributed/performance.md) row for M6: a failure and
    /// recovery window remains visible with separate before, during and after distributions.
    #[test]
    fn fault_capture_preserves_outage_time_series() {
        // ten seconds of one operation every 100 ms: fast before, the kill at three seconds,
        // failures from 3.1 to 5.9 with one lucky success in the middle of them, slow successes
        // from six to eight, fast again after
        let mut timeline = Vec::new();
        for tick in 0..100u64 {
            let at = tick * 100;
            let (elapsed, ok) = match at {
                0..=3_000 => (200, true),
                3_100..=5_900 if at == 4_500 => (300, true),
                3_100..=5_900 => (50, false),
                6_000..=8_000 => (2_000, true),
                _ => (200, true),
            };
            timeline.push(sample(at, elapsed, ok));
        }
        let facts = cut(
            "kill",
            1,
            Duration::from_secs(3),
            Some(Duration::from_millis(7_500)),
            &timeline,
            Duration::from_secs(10),
        );
        // the marks are the schedule's, the cuts are the client's
        assert_eq!(facts.at_ms, 3_000);
        assert_eq!(facts.restarted_at_ms, Some(7_500));
        assert_eq!(facts.first_failure_ms, Some(3_100));
        // the lucky success at 4.5 s is followed by a failure inside the sustained run, so the
        // recovery is the first success with two clean seconds after it
        assert_eq!(facts.recovered_ms, Some(6_000));
        assert_eq!(facts.outage_ms, Some(2_900));
        assert_eq!(facts.sustained_ms, SUSTAINED.as_millis() as u64);
        // three windows, each with its own distribution
        let names: Vec<&str> = facts
            .windows
            .iter()
            .map(|window| window.name.as_str())
            .collect();
        assert_eq!(names, ["before", "during", "after"]);
        let before = &facts.windows[0];
        let during = &facts.windows[1];
        let after = &facts.windows[2];
        assert_eq!((before.from_ms, before.to_ms), (0, 3_100));
        assert_eq!((during.from_ms, during.to_ms), (3_100, 6_000));
        assert_eq!((after.from_ms, after.to_ms), (6_000, 10_000));
        assert_eq!(before.ops, 31);
        assert_eq!(before.errors, 0);
        assert_eq!(before.p50_us, 200);
        assert_eq!(during.ops, 29);
        assert_eq!(during.errors, 28);
        // the one success during the outage is the whole distribution there
        assert_eq!(during.p50_us, 300);
        assert_eq!(after.ops, 40);
        assert_eq!(after.errors, 0);
        // the slow seconds after the recovery are in the after window's tail and not in the
        // before window's, which is what "not averaged away" means
        assert_eq!(after.max_us, 2_000);
        assert_eq!(before.max_us, 200);
        assert!(after.p99_us >= 2_000, "{}", after.p99_us);
        // the series has a bucket per second, and the outage is a dip a reader can see in it
        assert_eq!(facts.series.len(), 10);
        assert_eq!(facts.series[0].errors, 0);
        assert_eq!(facts.series[0].ops, 10);
        assert_eq!(facts.series[4].errors, 9);
        assert_eq!(facts.series[4].ops, 10);
        assert_eq!(facts.series[4].p50_us, 300);
        assert_eq!(facts.series[7].p50_us, 2_000);
        assert_eq!(facts.series[9].errors, 0);
        // the record round trips
        let text = serde_json::to_string(&facts).expect("serializes");
        let back: FaultFacts = serde_json::from_str(&text).expect("parses");
        assert_eq!(back, facts);
        // and a cluster record from before the fault arm loads with none
        let old = r#"{"nodes":3,"desired_rf":3,"active_rf":3,"write_policy":"Quorum",
            "read_policy":"One","durability":"fsync","driver":"in-process","cores":[],
            "tables":1,"tablets":4096,"emulated":true}"#;
        let cluster: ClusterFacts = serde_json::from_str(old).expect("an F40 record loads");
        assert_eq!(cluster.fault, None);
    }

    /// A run the client never saw fail has no outage, and its windows are cut at the kill
    #[test]
    fn a_fault_nobody_noticed_has_no_outage() {
        let timeline: Vec<_> = (0..50u64)
            .map(|tick| sample(tick * 100, 200, true))
            .collect();
        let facts = cut(
            "kill",
            2,
            Duration::from_secs(2),
            None,
            &timeline,
            Duration::from_secs(5),
        );
        assert_eq!(facts.first_failure_ms, None);
        assert_eq!(facts.recovered_ms, None);
        assert_eq!(facts.outage_ms, None);
        assert_eq!(facts.restarted_at_ms, None);
        // before is up to the kill, during is the rest, after is empty
        assert_eq!(
            (facts.windows[0].from_ms, facts.windows[0].to_ms),
            (0, 2_000)
        );
        assert_eq!(
            (facts.windows[1].from_ms, facts.windows[1].to_ms),
            (2_000, 5_000)
        );
        assert_eq!(
            (facts.windows[2].from_ms, facts.windows[2].to_ms),
            (5_000, 5_000)
        );
        assert_eq!(facts.windows[2].ops, 0);
        assert_eq!(facts.series.len(), 5);
    }

    /// An outage the run ended inside has a start and no end
    #[test]
    fn an_unresolved_outage_has_no_recovery() {
        let timeline: Vec<_> = (0..50u64)
            .map(|tick| sample(tick * 100, 200, tick < 30))
            .collect();
        let facts = cut(
            "kill",
            1,
            Duration::from_secs(3),
            None,
            &timeline,
            Duration::from_secs(5),
        );
        assert_eq!(facts.first_failure_ms, Some(3_000));
        assert_eq!(facts.recovered_ms, None);
        assert_eq!(facts.outage_ms, None);
        assert_eq!(facts.windows[1].errors, 20);
        assert_eq!(facts.windows[2].ops, 0);
    }
}
