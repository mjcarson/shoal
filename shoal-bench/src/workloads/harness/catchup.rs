//! Sampling a returning node's catch-up after a fault, and cutting it into a record
//!
//! A catch-up arm asks the harness to watch the node its fault brought back
//! ([`Workload::catchup`](crate::workloads::workload::Workload::catchup)). The fault thread,
//! once the node is placed again, reads the node's own `Replication` report over its client
//! endpoint each second until the node has converged or the run has ended, and hands the
//! samples back with its marks. A returning node cannot say how far behind it is until a
//! leader tells it, so its lag is judged against node zero's report of the same groups -
//! every group's committed index there against its applied index here - with every group up
//! and none installing, held for one more sample. Node zero holds every group at the
//! replication arms' factor, and its committed index is the cluster's, not a cached opinion of
//! the returning node ([C7](../../../../docs/src/distributed/failover.md)).
//!
//! The record splits what the node moved by path: the snapshot counters say what the snapshots
//! covered, and the applied position's growth less that is what the log fed. A run that ended
//! before convergence says `none` and keeps the series, so a backlog that grew is visible as
//! one rather than reported as caught up ([F43](../../../../docs/src/features/node-recovery.md)).

use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use shoal::server::replication::NodeReplication;
use shoal::shared::protocol::admin::{AdminKind, AdminRequest};

use crate::model::macro_layer::{CatchupFacts, CatchupSecondFacts};
use crate::workloads::harness::cluster::Staged;

/// How often the returning node is sampled
pub const SAMPLE_EVERY: Duration = Duration::from_secs(1);

/// How many consecutive converged samples end the sampling
pub const HELD: usize = 2;

/// One look at the returning node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    /// When it was taken, from the start of the measured phase
    pub at: Duration,
    /// The widest gap between a group's committed index on node zero and its applied index here
    pub lag_max: u64,
    /// How many of its groups were installing a snapshot
    pub installing: u64,
    /// Whether every group it hosts was up
    pub all_up: bool,
    /// Bytes of snapshots it had received
    pub snapshot_bytes: u64,
    /// Snapshots it had installed
    pub snapshots: u64,
    /// Log entries the snapshots covered
    pub snapshot_entries: u64,
    /// The sum of every group's applied index
    pub applied: u64,
}

impl Sample {
    /// A sample from the returning node's report, judged against node zero's
    ///
    /// # Arguments
    ///
    /// * `at` - When it was taken
    /// * `report` - What the returning node said
    /// * `reference` - What node zero said at about the same time
    #[must_use]
    pub fn of(at: Duration, report: &NodeReplication, reference: &NodeReplication) -> Self {
        let all_up = report.shards.iter().all(|shard| shard.groups.iter().all(|group| group.up));
        let applied = report
            .shards
            .iter()
            .flat_map(|shard| shard.groups.iter().map(|group| group.applied))
            .sum();
        // what the cluster has committed on each group, as node zero knows it
        let committed: std::collections::HashMap<_, u64> = reference
            .shards
            .iter()
            .flat_map(|shard| shard.groups.iter().map(|group| (group.group, group.committed)))
            .collect();
        let lag_max = report
            .shards
            .iter()
            .flat_map(|shard| shard.groups.iter())
            .map(|group| committed.get(&group.group).copied().unwrap_or(0).saturating_sub(group.applied))
            .max()
            .unwrap_or(0);
        Sample {
            at,
            lag_max,
            installing: u64::try_from(report.installing).unwrap_or(u64::MAX),
            all_up,
            snapshot_bytes: report.snapshots.bytes_received,
            snapshots: report.snapshots.installed,
            snapshot_entries: report.snapshots.entries_installed,
            applied,
        }
    }

    /// Whether the node looks caught up in this sample
    #[must_use]
    pub fn converged(&self) -> bool {
        self.lag_max == 0 && self.installing == 0 && self.all_up
    }
}

/// Sample the returning node until it has converged or the run has ended
///
/// # Arguments
///
/// * `staged` - The cluster the node was staged from
/// * `index` - The node
/// * `started` - When the measured phase started
/// * `until` - When the run ends
/// * `runtime` - A runtime to read the node's report on
///
/// # Errors
///
/// The node is not in the placement, or a report could not be read.
pub fn sample(
    staged: &Staged,
    index: u32,
    started: Instant,
    until: Instant,
    runtime: &tokio::runtime::Runtime,
) -> Result<Vec<Sample>> {
    let position = usize::try_from(index).unwrap_or(usize::MAX);
    let (Some(node), Some(zero)) = (staged.nodes.get(position), staged.nodes.first()) else {
        bail!("node {index} is not in the placement");
    };
    let addr = node.client_addr();
    let reference = zero.client_addr();
    let mut samples = Vec::new();
    let mut held = 0usize;
    loop {
        let now = Instant::now();
        // the node's own report and node zero's, over their client endpoints
        let report = read_report(runtime, &addr, index)?;
        let reference = read_report(runtime, &reference, 0)?;
        let sample = Sample::of(now.saturating_duration_since(started), &report, &reference);
        held = if sample.converged() { held + 1 } else { 0 };
        samples.push(sample);
        // converged and held, or out of run
        if held >= HELD || Instant::now() + SAMPLE_EVERY > until {
            return Ok(samples);
        }
        std::thread::sleep(SAMPLE_EVERY);
    }
}

/// A node's replication report, over its client endpoint
///
/// # Arguments
///
/// * `runtime` - A runtime to read it on
/// * `addr` - The endpoint
/// * `index` - The node, for the error
fn read_report(runtime: &tokio::runtime::Runtime, addr: &str, index: u32) -> Result<NodeReplication> {
    let value = runtime.block_on(async {
        let client = shoal::Shoal::<crate::workloads::schema::BenchClient>::new(addr)
            .await
            .with_context(|| format!("failed to reach node {index} at {addr}"))?;
        let response = client
            .admin(&AdminRequest {
                op: uuid::Uuid::new_v4(),
                expected_version: 0,
                kind: AdminKind::Replication,
            })
            .await
            .with_context(|| format!("node {index} did not answer a replication read"))?;
        match response.outcome {
            Ok(shoal::shared::protocol::admin::AdminOutcome::Read(value)) => Ok(value),
            other => bail!("node {index} refused a replication read: {other:?}"),
        }
    })?;
    serde_json::from_value(value).with_context(|| format!("node {index}'s replication report did not parse"))
}

/// Cut a returning node's samples into its record
///
/// Pure, so a test can hand it samples it made up. Convergence is the first sample that
/// looks caught up and is followed by [`HELD`] minus one more that do; the split by path is
/// the snapshot counters against the applied position's growth from the first sample.
///
/// # Arguments
///
/// * `restarted_at` - When the node was placed again, from the start of the run
/// * `samples` - What it looked like each second after that
#[must_use]
pub fn cut(restarted_at: Duration, samples: &[Sample]) -> CatchupFacts {
    let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    // the first sample from which every following one, `HELD` in all, is converged
    let converged = samples
        .windows(HELD)
        .find(|window| window.iter().all(Sample::converged))
        .map(|window| window[0].at);
    let first = samples.first();
    let last = samples.last();
    let snapshot_bytes = last.map_or(0, |sample| sample.snapshot_bytes);
    let snapshots = last.map_or(0, |sample| sample.snapshots);
    let snapshot_entries = last.map_or(0, |sample| sample.snapshot_entries);
    let grown = last.zip(first).map_or(0, |(last, first)| last.applied.saturating_sub(first.applied));
    let by = match (converged, snapshots) {
        (None, _) => "none",
        (Some(_), 0) => "log",
        (Some(_), _) => "snapshot",
    };
    CatchupFacts {
        by: by.to_string(),
        restarted_ms: millis(restarted_at),
        converged_ms: converged.map(millis),
        seconds_to_converge: converged.map(|at| at.saturating_sub(restarted_at).as_secs()),
        snapshot_bytes,
        snapshots,
        log_entries: grown.saturating_sub(snapshot_entries),
        snapshot_entries,
        series: samples
            .iter()
            .map(|sample| CatchupSecondFacts {
                second: sample.at.as_secs(),
                lag_max: sample.lag_max,
                installing: sample.installing,
                snapshot_bytes: sample.snapshot_bytes,
                applied: sample.applied,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{cut, Sample};
    use crate::model::macro_layer::{CatchupFacts, ClusterFacts, FaultFacts};

    /// A sample at a second with the given lag and counters
    fn at(second: u64, lag: u64, installing: u64, snapshot_bytes: u64, snapshots: u64, snapshot_entries: u64, applied: u64) -> Sample {
        Sample {
            at: Duration::from_secs(second),
            lag_max: lag,
            installing,
            all_up: true,
            snapshot_bytes,
            snapshots,
            snapshot_entries,
            applied,
        }
    }

    /// A catch-up record round-trips its marks, its split by path and its series, judges
    /// convergence from held samples, and an F42 record with no catch-up loads
    #[test]
    fn catchup_capture_records_convergence() {
        // a snapshot catch-up: behind, installing, then a jump and a tail from the log
        let samples = vec![
            at(40, 500, 0, 0, 0, 0, 1_000),
            at(41, 480, 1, 300_000, 0, 0, 1_000),
            at(42, 20, 0, 900_000, 3, 1_400, 2_400),
            at(43, 0, 0, 900_000, 3, 1_400, 2_450),
            at(44, 0, 0, 900_000, 3, 1_400, 2_460),
        ];
        let facts = cut(Duration::from_secs(40), &samples);
        assert_eq!(facts.by, "snapshot");
        assert_eq!(facts.restarted_ms, 40_000);
        assert_eq!(facts.converged_ms, Some(43_000));
        assert_eq!(facts.seconds_to_converge, Some(3));
        assert_eq!(facts.snapshot_bytes, 900_000);
        assert_eq!(facts.snapshots, 3);
        assert_eq!(facts.snapshot_entries, 1_400);
        // grown by 1,460, of which the snapshots covered 1,400
        assert_eq!(facts.log_entries, 60);
        assert_eq!(facts.series.len(), 5);
        assert_eq!(facts.series[1].installing, 1);
        assert_eq!(facts.series[4].second, 44);
        // the record round-trips
        let json = serde_json::to_string(&facts).expect("serializes");
        let back: CatchupFacts = serde_json::from_str(&json).expect("loads");
        assert_eq!(back, facts);
        // a log catch-up converges with no snapshot; a single converged sample is not held
        let by_log = cut(Duration::from_secs(10), &[at(10, 12, 0, 0, 0, 0, 100), at(11, 0, 0, 0, 0, 0, 112), at(12, 0, 0, 0, 0, 0, 112)]);
        assert_eq!(by_log.by, "log");
        assert_eq!(by_log.converged_ms, Some(11_000));
        assert_eq!(by_log.log_entries, 12);
        let flicker = cut(Duration::from_secs(10), &[at(10, 12, 0, 0, 0, 0, 100), at(11, 0, 0, 0, 0, 0, 112), at(12, 3, 0, 0, 0, 0, 115)]);
        assert_eq!(flicker.by, "none");
        assert_eq!(flicker.converged_ms, None);
        assert_eq!(flicker.seconds_to_converge, None);
        // a run that ended with a backlog says so, and keeps what it saw
        let backlog = cut(Duration::from_secs(20), &[at(20, 900, 0, 0, 0, 0, 10), at(21, 950, 0, 0, 0, 0, 20)]);
        assert_eq!(backlog.by, "none");
        assert_eq!(backlog.series.len(), 2);
        assert_eq!(backlog.log_entries, 10);
        // an F42 cluster record, with a fault and no catch-up, loads
        let f42 = serde_json::json!({
            "nodes": 3, "desired_rf": 3, "active_rf": 3, "write_policy": "Quorum", "read_policy": "One",
            "durability": "fsync", "driver": "separate", "cores": [], "driver_cores": [], "tables": 1,
            "tablets": 4096, "emulated": true, "placement": [], "members": [], "map_version": 1,
            "voters": 3, "learners": 0,
            "fault": { "kind": "kill", "node": 1, "at_ms": 20000, "sustained_ms": 2000, "windows": [], "series": [] }
        });
        let loaded: ClusterFacts = serde_json::from_value(f42).expect("an F42 record loads");
        assert!(loaded.catchup.is_none());
        let fault: FaultFacts = loaded.fault.expect("the fault travels");
        assert_eq!(fault.node, 1);
    }
}
