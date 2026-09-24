//! A node's trailing write and stream rates, and how far a plan has got
//!
//! Every shard reports cumulative counters per group ([`GroupReport::writes`]) and the node's
//! control thread holds the newest report of each. On every report tick the
//! [`NodeStatsTracker`] takes each group's change since the tick before, divides it by the time
//! between them, and feeds the rate to three exponentially weighted moving averages - ten
//! seconds, a minute and five minutes - per table, over every copy and over the copies the node
//! leads. An average of a sum is the sum of the averages, so doing this once over the node's
//! summed counters is the same as keeping one per shard and adding them up, at a fraction of the
//! cost; nothing on the shard's apply path does more than add to a `u64`
//! ([F52](../../../../docs/src/features/cluster-stats.md)).
//!
//! # Invariants
//!
//! **A counter going backwards is a reset, never a negative rate.** A shard that started again
//! reports counters from zero; the group's reading is taken as its whole change since the tick
//! before, which is what a fresh group's is too.
//!
//! **Nothing here is committed.** The rates live in the node's memory and ride its status
//! report; a control leader that changes loses nothing but the figures it held of the others,
//! which come back with their next reports.
//!
//! [`GroupReport::writes`]: crate::server::replication::GroupReport::writes

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use tracing::instrument;
use uuid::Uuid;

use super::migrate::MoveRecord;
use super::plan::{PlanRecord, StepState};
use crate::server::replication::ShardReplication;
use crate::shared::identity::{GroupId, NodeId};
use crate::shared::protocol::stats::{
    NodeStats, PlanProgress, Rates, TableStats, WriteCounters, WriteRates,
};

/// How many status reports in a row one carries the node's figures
///
/// At the default half second report interval, the leader hears a member's figures every two
/// seconds: soon enough for a ten second window to be read as current, and a quarter of the
/// bytes of sending them on every report.
pub const STATS_EVERY_REPORTS: u32 = 4;

/// How many of its figures' intervals a member may miss before its rates are read as stale
pub const STALE_AFTER_INTERVALS: u64 = 3;

/// The time constants of the three windows, in seconds
const WINDOW_SECS: [f64; 3] = [10.0, 60.0, 300.0];

/// A rate below this is read as nothing, so a table nobody writes to decays out of the figures
const NEGLIGIBLE_RATE: f64 = 1e-6;

/// Milliseconds since the epoch, for a figure's timestamp and a plan's elapsed time
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

/// One exponentially weighted moving average of a rate
///
/// Debiased by the weight its samples have accumulated, so the average of the first sample is
/// that sample rather than a fraction of it, and a five minute window is readable a second in.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Ewma {
    /// The weighted sum of the samples, not yet divided by their weight
    value: f64,
    /// How much weight the samples have accumulated, approaching one
    weight: f64,
}

impl Ewma {
    /// Take one sample of the rate, held over an interval
    ///
    /// # Arguments
    ///
    /// * `rate` - The rate over the interval, per second
    /// * `dt` - The interval, in seconds
    /// * `tau` - The average's time constant, in seconds
    fn observe(&mut self, rate: f64, dt: f64, tau: f64) {
        // a longer interval weighs its sample more, the same as that many shorter ones would
        let alpha = 1.0 - (-dt / tau).exp();
        self.value += alpha * (rate - self.value);
        self.weight += alpha * (1.0 - self.weight);
    }

    /// The average, debiased by the weight the samples have accumulated
    fn rate(&self) -> f64 {
        if self.weight > 0.0 {
            self.value / self.weight
        } else {
            0.0
        }
    }
}

/// The three windows of one rate
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Windows([Ewma; 3]);

impl Windows {
    /// Take one sample of the rate in every window
    ///
    /// # Arguments
    ///
    /// * `rate` - The rate over the interval, per second
    /// * `dt` - The interval, in seconds
    fn observe(&mut self, rate: f64, dt: f64) {
        // each window with its own time constant
        for (ewma, tau) in self.0.iter_mut().zip(WINDOW_SECS) {
            ewma.observe(rate, dt, tau);
        }
    }

    /// The three windows as the wire carries them
    fn rates(&self) -> Rates {
        Rates {
            r10s: self.0[0].rate(),
            r1m: self.0[1].rate(),
            r5m: self.0[2].rate(),
        }
    }
}

/// The windows of every write counter
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct WriteWindows {
    /// Rows inserted
    inserts: Windows,
    /// Rows updated
    updates: Windows,
    /// Rows deleted
    deletes: Windows,
    /// Bytes inserted
    insert_bytes: Windows,
    /// Bytes updated
    update_bytes: Windows,
    /// Bytes deleted
    delete_bytes: Windows,
    /// Updates and deletes that found no row
    misses: Windows,
}

impl WriteWindows {
    /// Take one interval's change of every counter as a sample of its rate
    ///
    /// # Arguments
    ///
    /// * `delta` - What each counter gained over the interval
    /// * `dt` - The interval, in seconds
    #[allow(clippy::cast_precision_loss)]
    fn observe(&mut self, delta: &WriteCounters, dt: f64) {
        // every counter's change over the interval is its rate
        self.inserts.observe(delta.inserts as f64 / dt, dt);
        self.updates.observe(delta.updates as f64 / dt, dt);
        self.deletes.observe(delta.deletes as f64 / dt, dt);
        self.insert_bytes.observe(delta.insert_bytes as f64 / dt, dt);
        self.update_bytes.observe(delta.update_bytes as f64 / dt, dt);
        self.delete_bytes.observe(delta.delete_bytes as f64 / dt, dt);
        self.misses.observe(delta.misses as f64 / dt, dt);
    }

    /// Every counter's windows as the wire carries them
    fn rates(&self) -> WriteRates {
        WriteRates {
            inserts: self.inserts.rates(),
            updates: self.updates.rates(),
            deletes: self.deletes.rates(),
            insert_bytes: self.insert_bytes.rates(),
            update_bytes: self.update_bytes.rates(),
            delete_bytes: self.delete_bytes.rates(),
            misses: self.misses.rates(),
        }
    }
}

/// The windows one table keeps: over every copy, and over the copies the node leads
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct TableWindows {
    /// Over every copy the node hosts
    applied: WriteWindows,
    /// Over the copies whose group the node leads
    led: WriteWindows,
}

/// What one tick found of one table, before its rates are taken
#[derive(Debug, Clone, Default)]
struct TableTick {
    /// Placement, size and cumulative figures, with no rates yet
    figures: TableStats,
    /// What the table's copies applied since the tick before
    applied: WriteCounters,
    /// What the copies the node leads applied since the tick before
    led: WriteCounters,
}

/// A node's trailing rates, derived from its shards' cumulative counters tick by tick
#[derive(Debug, Default)]
pub struct NodeStatsTracker {
    /// When the first tick was taken
    started: Option<Instant>,
    /// When the last tick was taken
    last: Option<Instant>,
    /// Every group's counters as the last tick read them, by the shard hosting it
    prev: HashMap<(usize, GroupId), WriteCounters>,
    /// The node's snapshot bytes sent and received as the last tick read them
    prev_stream: Option<(u64, u64)>,
    /// Every table's windows, by the name the schema spells it
    tables: BTreeMap<String, TableWindows>,
    /// The windows of every table together
    total: TableWindows,
    /// Snapshot bytes streamed out
    stream_sent: Windows,
    /// Snapshot bytes streamed in
    stream_received: Windows,
}

impl NodeStatsTracker {
    /// Take one tick: read every shard's newest report and derive the node's figures
    ///
    /// The first tick only records where the counters stand, and reports every rate as zero.
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `now` - When the tick is taken
    /// * `now_ms` - The same, in milliseconds since the epoch, for the figures' timestamp
    /// * `shards` - Every shard's newest report
    /// * `free_bytes` - The free bytes on the node's storage, zero when it could not read them
    #[instrument(name = "NodeStatsTracker::tick", skip_all)]
    pub fn tick(
        &mut self,
        node: NodeId,
        now: Instant,
        now_ms: u64,
        shards: &BTreeMap<usize, ShardReplication>,
        free_bytes: u64,
    ) -> NodeStats {
        // the interval since the tick before, or none on the first
        let dt = self
            .last
            .map(|last| now.saturating_duration_since(last).as_secs_f64())
            .filter(|dt| *dt > 0.0);
        let started = *self.started.get_or_insert(now);
        self.last = Some(now);
        // what every table holds and gained since the tick before
        let mut ticks: BTreeMap<String, TableTick> = BTreeMap::new();
        let mut seen: HashMap<(usize, GroupId), WriteCounters> = HashMap::new();
        for (shard, report) in shards {
            for group in &report.groups {
                // a group this tick has not seen before gained everything it counts, and so
                // did a group whose shard started again
                let key = (*shard, group.group);
                let gained = match self.prev.get(&key) {
                    Some(prev) => group.writes.delta(prev).unwrap_or(group.writes),
                    None => group.writes,
                };
                seen.insert(key, group.writes);
                let tick = ticks.entry(group.table_name.clone()).or_default();
                // placement and size, over every copy and over the copies led
                let tablets = u64::from(group.tablets);
                tick.figures.groups += 1;
                tick.figures.tablets += tablets;
                tick.figures.partitions += group.partitions;
                tick.figures.bytes += group.bytes;
                tick.figures.applied_total.absorb(&group.writes);
                tick.applied.absorb(&gained);
                if group.is_leader {
                    tick.figures.groups_led += 1;
                    tick.figures.tablets_led += tablets;
                    tick.figures.partitions_led += group.partitions;
                    tick.figures.bytes_led += group.bytes;
                    tick.figures.led_total.absorb(&group.writes);
                    tick.led.absorb(&gained);
                }
            }
        }
        // the groups gone since the tick before are forgotten with it
        self.prev = seen;
        // the node's snapshot streams, gained since the tick before
        let (sent, received) = shards.values().fold((0u64, 0u64), |(sent, received), report| {
            (
                sent.saturating_add(report.snapshots.bytes_sent),
                received.saturating_add(report.snapshots.bytes_received),
            )
        });
        let stream_gained = match self.prev_stream {
            Some((prev_sent, prev_received)) => (
                sent.checked_sub(prev_sent).unwrap_or(sent),
                received.checked_sub(prev_received).unwrap_or(received),
            ),
            None => (sent, received),
        };
        self.prev_stream = Some((sent, received));
        // the rates only move once there is an interval to divide by
        if let Some(dt) = dt {
            self.observe(&ticks, stream_gained, dt);
        }
        // the figures, each table's with its rates, and their sum
        let mut stats = NodeStats::empty(node);
        stats.at_ms = now_ms;
        stats.observed_ms =
            u64::try_from(now.saturating_duration_since(started).as_millis()).unwrap_or(u64::MAX);
        stats.shards = u64::try_from(shards.len()).unwrap_or(u64::MAX);
        stats.free_bytes = free_bytes;
        stats.volatile_bytes = shards
            .values()
            .map(|report| u64::try_from(report.volatile_bytes).unwrap_or(u64::MAX))
            .sum();
        stats.stream_sent = self.stream_sent.rates();
        stats.stream_received = self.stream_received.rates();
        stats.stream_sent_total = sent;
        stats.stream_received_total = received;
        for (table, windows) in &self.tables {
            let mut row = ticks
                .remove(table)
                .map(|tick| tick.figures)
                .unwrap_or_default();
            row.table.clone_from(table);
            row.applied = windows.applied.rates();
            row.led = windows.led.rates();
            stats.total.absorb(&row);
            stats.tables.push(row);
        }
        // a table nobody has written to yet still has its placement counted
        for (table, tick) in ticks {
            let mut row = tick.figures;
            row.table = table;
            stats.total.absorb(&row);
            stats.tables.push(row);
        }
        stats.tables.sort_by(|a, b| a.table.cmp(&b.table));
        // the total's rates are its own windows', which are the tables' summed as they came in
        stats.total.applied = self.total.applied.rates();
        stats.total.led = self.total.led.rates();
        // a table holding and doing nothing says nothing
        stats.tables.retain(|row| !row.is_idle());
        stats
    }

    /// Feed one interval's gains to every window
    ///
    /// # Arguments
    ///
    /// * `ticks` - What every table gained over the interval
    /// * `stream_gained` - The snapshot bytes sent and received over the interval
    /// * `dt` - The interval, in seconds
    #[allow(clippy::cast_precision_loss)]
    fn observe(&mut self, ticks: &BTreeMap<String, TableTick>, stream_gained: (u64, u64), dt: f64) {
        // every table this tick saw gets a window if it had none
        for table in ticks.keys() {
            self.tables.entry(table.clone()).or_default();
        }
        // every table's windows see its gain, or nothing if it gained nothing
        let mut applied = WriteCounters::default();
        let mut led = WriteCounters::default();
        for (table, windows) in &mut self.tables {
            let (table_applied, table_led) = ticks
                .get(table)
                .map(|tick| (tick.applied, tick.led))
                .unwrap_or_default();
            windows.applied.observe(&table_applied, dt);
            windows.led.observe(&table_led, dt);
            applied.absorb(&table_applied);
            led.absorb(&table_led);
        }
        // a table no group of which is here any more and whose rates decayed away is dropped
        self.tables.retain(|table, windows| {
            ticks.contains_key(table) || !is_negligible(&windows.applied.rates())
        });
        // the node's own windows see the sum
        self.total.applied.observe(&applied, dt);
        self.total.led.observe(&led, dt);
        self.stream_sent.observe(stream_gained.0 as f64 / dt, dt);
        self.stream_received.observe(stream_gained.1 as f64 / dt, dt);
    }
}

/// Whether every window of every rate has decayed below what is worth reporting
///
/// # Arguments
///
/// * `rates` - The rates
fn is_negligible(rates: &WriteRates) -> bool {
    // the five minute window is the last to decay, and the byte rates the largest figures
    [
        rates.inserts,
        rates.updates,
        rates.deletes,
        rates.insert_bytes,
        rates.update_bytes,
        rates.delete_bytes,
        rates.misses,
    ]
    .iter()
    .all(|rate| rate.r10s <= NEGLIGIBLE_RATE && rate.r1m <= NEGLIGIBLE_RATE && rate.r5m <= NEGLIGIBLE_RATE)
}

/// When one step's move started and, once done, finished, in milliseconds since the epoch
///
/// Read off the committed move record: every group's `since` is when it entered its current
/// phase and `phase_ms` is what each earlier phase took, so a group started at their
/// difference. The step spans its earliest group's start to its latest group's end.
///
/// # Arguments
///
/// * `record` - The move the step issued
fn step_span(record: &MoveRecord) -> (Option<u64>, Option<u64>) {
    // a group nobody has driven has no timings yet
    let started = record
        .groups
        .values()
        .filter(|group| group.stats.since > 0)
        .map(|group| {
            let spent: u64 = group.stats.phase_ms.values().sum();
            group.stats.since.saturating_sub(spent)
        })
        .min();
    // a move is finished once every group is, and it finished when the last one entered Done
    let finished = if record.is_done() && record.groups.values().all(|group| group.is_done()) {
        record
            .groups
            .values()
            .map(|group| group.stats.since)
            .max()
            .filter(|since| *since > 0)
    } else {
        None
    };
    (started, finished)
}

/// How far a plan has got, and how fast
///
/// The counts and bytes come from the plan's steps, the timings and streamed bytes from the
/// committed records of the moves they issued, and the current throughput from the stream
/// rates of the members the running steps move from. The estimate of what is left is the
/// larger of two: the bytes left at the current throughput, and the steps left at the pace
/// finished steps kept - the second because every step waits out its source's retirement
/// however few bytes it holds.
///
/// # Arguments
///
/// * `record` - The plan
/// * `moves` - Every move record the control state holds, by operation
/// * `now_ms` - Now, in milliseconds since the epoch
/// * `stream_rate` - The one minute rate a member streams out at, if its figures are held
pub fn plan_progress<F>(
    record: &PlanRecord,
    moves: &BTreeMap<Uuid, MoveRecord>,
    now_ms: u64,
    stream_rate: F,
) -> PlanProgress
where
    F: Fn(NodeId) -> Option<f64>,
{
    // the steps by where they stand
    let mut progress = PlanProgress {
        op: record.op,
        kind: record.kind.name().to_string(),
        phase: record.phase.name().to_string(),
        blocked: record.blocked.as_ref().map(|blocked| blocked.reason.clone()),
        outcome: record.outcome.as_ref().map(|outcome| outcome.name().to_string()),
        steps_total: 0,
        pending: 0,
        moving: 0,
        moved: 0,
        failed: 0,
        bytes_planned: 0,
        bytes_moved: 0,
        bytes_streamed: 0,
        started_ms: None,
        elapsed_ms: None,
        mean_step_ms: None,
        throughput_avg_bps: None,
        throughput_now_bps: None,
        eta_ms: None,
    };
    let mut finished_steps: Vec<u64> = Vec::new();
    let mut last_finish: Option<u64> = None;
    let mut sources: Vec<NodeId> = Vec::new();
    for step in &record.steps {
        progress.steps_total += 1;
        // what the step stands at, and what its bytes count towards
        match step.state {
            StepState::Pending => progress.pending += 1,
            StepState::Moving => {
                progress.moving += 1;
                if !sources.contains(&step.from) {
                    sources.push(step.from);
                }
            }
            StepState::Moved => {
                progress.moved += 1;
                progress.bytes_moved += step.bytes;
            }
            StepState::Failed { .. } => progress.failed += 1,
        }
        if !matches!(step.state, StepState::Failed { .. }) {
            progress.bytes_planned += step.bytes;
        }
        // the move's committed timings and streamed bytes, while its record is kept
        let Some(record) = step.op.and_then(|op| moves.get(&op)) else {
            continue;
        };
        progress.bytes_streamed += record
            .groups
            .values()
            .map(|group| group.stats.bytes)
            .sum::<u64>();
        let (started, finished) = step_span(record);
        if let Some(started) = started {
            progress.started_ms = Some(progress.started_ms.map_or(started, |s| s.min(started)));
            if let Some(finished) = finished {
                finished_steps.push(finished.saturating_sub(started));
                last_finish = Some(last_finish.map_or(finished, |f| f.max(finished)));
            }
        }
    }
    // how long it has run: to now while open, to its last step's end once done
    let end = if record.is_done() {
        last_finish
    } else {
        Some(now_ms)
    };
    progress.elapsed_ms = match (progress.started_ms, end) {
        (Some(started), Some(end)) => Some(end.saturating_sub(started)),
        _ => None,
    };
    // the pace finished steps kept, and the bytes moved per second of the whole
    if !finished_steps.is_empty() {
        let count = u64::try_from(finished_steps.len()).unwrap_or(u64::MAX);
        progress.mean_step_ms = Some(finished_steps.iter().sum::<u64>() / count);
    }
    #[allow(clippy::cast_precision_loss)]
    if let Some(elapsed) = progress.elapsed_ms.filter(|elapsed| *elapsed > 0) {
        progress.throughput_avg_bps = Some(progress.bytes_moved as f64 * 1000.0 / elapsed as f64);
    }
    // an open plan's current throughput is what its running steps' sources stream out
    if !record.is_done() && !sources.is_empty() {
        let rates: Vec<f64> = sources.iter().filter_map(|node| stream_rate(*node)).collect();
        if !rates.is_empty() {
            progress.throughput_now_bps = Some(rates.iter().sum());
        }
    }
    // what is left, by bytes at the current throughput and by steps at the finished pace
    if !record.is_done() {
        progress.eta_ms = eta_ms(&progress);
    }
    progress
}

/// How long an open plan is estimated to have left, if anything can be estimated
///
/// # Arguments
///
/// * `progress` - The plan's progress, with every other figure filled in
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn eta_ms(progress: &PlanProgress) -> Option<u64> {
    // the bytes of the steps not yet moved at the rate the sources stream now
    let left_bytes = progress.bytes_planned.saturating_sub(progress.bytes_moved);
    let by_bytes = progress
        .throughput_now_bps
        .filter(|rate| *rate > 0.0)
        .map(|rate| (left_bytes as f64 / rate * 1000.0) as u64);
    // the steps not yet moved, run as many at a time as are running now, at the finished pace
    let left_steps = progress.pending + progress.moving;
    let by_steps = progress.mean_step_ms.map(|mean| {
        let at_once = progress.moving.max(1);
        left_steps.div_ceil(at_once) * mean
    });
    // nothing left is nothing to wait for
    if left_steps == 0 {
        return Some(0);
    }
    match (by_bytes, by_steps) {
        (Some(bytes), Some(steps)) => Some(bytes.max(steps)),
        (one, other) => one.or(other),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::server::control::migrate::{GroupMove, MovePhase, MoveStats};
    use crate::server::control::plan::{PlanKind, PlanPhase, PlanStep};
    use crate::server::replication::GroupReport;
    use crate::shared::identity::{ShardAddr, TableId};

    /// A group report for one table with cumulative counters
    ///
    /// # Arguments
    ///
    /// * `id` - The group's number
    /// * `table` - The table's name
    /// * `inserts` - Rows inserted so far
    /// * `is_leader` - Whether this copy leads
    fn group(id: u64, table: &str, inserts: u64, is_leader: bool) -> GroupReport {
        GroupReport {
            group: GroupId(id),
            table: TableId(0),
            table_name: table.to_string(),
            tablets: 2,
            tablet_ids: vec![0, 1],
            members: Vec::new(),
            leader: None,
            is_leader,
            applied: 0,
            committed: 0,
            last_log: 0,
            checkpoint: 0,
            purged: 0,
            pending_bytes: 0,
            volatile: false,
            up: true,
            installing: false,
            voters: Vec::new(),
            learner: false,
            quarantined: None,
            bytes: 1000,
            core_dead: None,
            term: 1,
            partitions: 10,
            writes: WriteCounters {
                inserts,
                insert_bytes: inserts * 100,
                ..WriteCounters::default()
            },
        }
    }

    /// One shard's report holding some groups
    ///
    /// # Arguments
    ///
    /// * `groups` - The groups
    fn shards(groups: Vec<GroupReport>) -> BTreeMap<usize, ShardReplication> {
        BTreeMap::from([(
            0,
            ShardReplication {
                shard: 0,
                groups,
                ..ShardReplication::default()
            },
        )])
    }

    /// The first sample of a debiased average is the sample, and a constant rate converges
    #[test]
    fn ewma_is_debiased_and_converges() {
        // one sample of any length reads as itself
        let mut ewma = Ewma::default();
        ewma.observe(50.0, 0.5, 300.0);
        assert!((ewma.rate() - 50.0).abs() < 1e-9);
        // a step change reaches most of the new rate within five time constants
        let mut ewma = Ewma::default();
        for _ in 0..20 {
            ewma.observe(10.0, 0.5, 10.0);
        }
        for _ in 0..100 {
            ewma.observe(100.0, 0.5, 10.0);
        }
        assert!((ewma.rate() - 100.0).abs() < 1.0, "{}", ewma.rate());
        // irregular intervals weigh by their length: one long interval is many short ones
        let (mut long, mut short) = (Ewma::default(), Ewma::default());
        long.observe(1.0, 1.0, 10.0);
        long.observe(5.0, 4.0, 10.0);
        short.observe(1.0, 1.0, 10.0);
        for _ in 0..4 {
            short.observe(5.0, 1.0, 10.0);
        }
        assert!((long.rate() - short.rate()).abs() < 1e-9);
    }

    /// A node's rates are its counters' change over time, split by leadership, and survive a reset
    #[test]
    fn tracker_derives_rates_from_counters() {
        let node = NodeId(Uuid::new_v4());
        let mut tracker = NodeStatsTracker::default();
        let start = Instant::now();
        // the first tick is a baseline: figures but no rates
        let first = tracker.tick(
            node,
            start,
            1,
            &shards(vec![group(1, "notes", 100, true), group(2, "notes", 50, false)]),
            7,
        );
        assert_eq!(first.total.applied.inserts.r10s, 0.0);
        assert_eq!(first.total.groups, 2);
        assert_eq!(first.total.groups_led, 1);
        assert_eq!(first.total.partitions, 20);
        assert_eq!(first.total.partitions_led, 10);
        assert_eq!(first.total.applied_total.inserts, 150);
        assert_eq!(first.total.led_total.inserts, 100);
        assert_eq!(first.free_bytes, 7);
        // a second later, 10 rows on the led copy and 20 on the other
        let second = tracker.tick(
            node,
            start + Duration::from_secs(1),
            2,
            &shards(vec![group(1, "notes", 110, true), group(2, "notes", 70, false)]),
            7,
        );
        assert!((second.total.applied.inserts.r10s - 30.0).abs() < 1e-9);
        assert!((second.total.led.inserts.r10s - 10.0).abs() < 1e-9);
        assert!((second.total.applied.insert_bytes.r5m - 3000.0).abs() < 1e-6);
        assert_eq!(second.tables.len(), 1);
        assert!((second.tables[0].applied.inserts.r1m - 30.0).abs() < 1e-9);
        assert_eq!(second.observed_ms, 1000);
        // a counter below its last reading is a restarted shard: its reading is its gain
        let third = tracker.tick(
            node,
            start + Duration::from_secs(2),
            3,
            &shards(vec![group(1, "notes", 5, true), group(2, "notes", 70, false)]),
            7,
        );
        let r10s = third.total.applied.inserts.r10s;
        assert!(r10s > 5.0 && r10s < 30.0, "{r10s}");
        // a group gone is forgotten, and its table decays rather than jumping
        let fourth = tracker.tick(
            node,
            start + Duration::from_secs(3),
            4,
            &shards(vec![group(1, "notes", 5, true)]),
            7,
        );
        assert_eq!(fourth.total.groups, 1);
        assert!(fourth.total.applied.inserts.r10s < r10s);
        assert!(fourth.total.applied.inserts.r10s > 0.0);
    }

    /// A step's move record gives its start and end, and an open plan its estimate
    #[test]
    fn plan_progress_from_steps_and_moves() {
        let (a, b, c) = (
            NodeId(Uuid::new_v4()),
            NodeId(Uuid::new_v4()),
            NodeId(Uuid::new_v4()),
        );
        // a move that took 10s of phases and finished at 20_000
        let move_of = |op: Uuid, since: u64, spent: u64, phase: MovePhase| {
            let mut stats = MoveStats {
                bytes: 400,
                since,
                ..MoveStats::default()
            };
            stats.phase_ms.insert("learner".to_string(), spent);
            let done = phase == MovePhase::Done;
            MoveRecord {
                op,
                tablets: vec![0],
                from: ShardAddr::new(a, 0),
                to: ShardAddr::new(c, 0),
                expected: Vec::new(),
                target: Vec::new(),
                phase: phase.clone(),
                groups: BTreeMap::from([(
                    GroupId(1),
                    GroupMove {
                        phase,
                        stats,
                        ..GroupMove::default()
                    },
                )]),
                principal: String::new(),
                requested_at: 0,
                outcome: done.then_some(super::super::migrate::MoveOutcome::Moved),
            }
        };
        let (done_op, running_op) = (Uuid::new_v4(), Uuid::new_v4());
        let moves = BTreeMap::from([
            (done_op, move_of(done_op, 20_000, 10_000, MovePhase::Done)),
            (running_op, move_of(running_op, 25_000, 3_000, MovePhase::CatchingUp)),
        ]);
        let step = |op: Option<Uuid>, from: NodeId, state: StepState| PlanStep {
            tablet: 0,
            from,
            to: c,
            bytes: 1_000_000,
            op,
            state,
        };
        let mut plan = PlanRecord::new(Uuid::new_v4(), PlanKind::Rebalance, "admin", 1);
        plan.phase = PlanPhase::Running;
        plan.steps = vec![
            step(Some(done_op), a, StepState::Moved),
            step(Some(running_op), b, StepState::Moving),
            step(None, a, StepState::Pending),
            step(
                None,
                a,
                StepState::Failed {
                    reason: "gone".to_string(),
                },
            ),
        ];
        // b streams at 100 kB/s, and a's figures are not held
        let rate = |node: NodeId| (node == b).then_some(100_000.0);
        let progress = plan_progress(&plan, &moves, 30_000, rate);
        assert_eq!(progress.steps_total, 4);
        assert_eq!((progress.pending, progress.moving, progress.moved, progress.failed), (1, 1, 1, 1));
        assert_eq!(progress.bytes_planned, 3_000_000);
        assert_eq!(progress.bytes_moved, 1_000_000);
        assert_eq!(progress.bytes_streamed, 800);
        assert_eq!(progress.started_ms, Some(10_000));
        assert_eq!(progress.elapsed_ms, Some(20_000));
        assert_eq!(progress.mean_step_ms, Some(10_000));
        assert_eq!(progress.throughput_avg_bps, Some(50_000.0));
        assert_eq!(progress.throughput_now_bps, Some(100_000.0));
        // 2 MB at 100 kB/s is 20s, and two steps one at a time at 10s each is 20s too; the
        // larger wins, so a slower step pace would have
        assert_eq!(progress.eta_ms, Some(20_000));
        // a done plan runs to its last step's end and has nothing left
        plan.phase = PlanPhase::Done;
        let done = plan_progress(&plan, &moves, 99_000, rate);
        assert_eq!(done.elapsed_ms, Some(10_000));
        assert_eq!(done.eta_ms, None);
        assert_eq!(done.throughput_now_bps, None);
    }
}
