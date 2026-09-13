//! `macro/cluster/catchup/{log,snapshot}` - what a returning node's catch-up costs, by log and
//! by snapshot
//!
//! # The question
//!
//! [M7](../../../docs/src/distributed/milestones.md#m7-recover-a-node-brought-back-online)
//! brings a member back past the purge point and asks, as its exit criterion, for *log and
//! snapshot catch-up rates and foreground tails* ([C10](../../../docs/src/distributed/performance.md)).
//! These two arms are that capture: the kill arm's shape - the durable replication placement,
//! the reference mixture, a client that does not retry, node one killed a third of the way
//! through and started again two thirds through - differing in one thing, how far the
//! survivors' logs reach back when the node returns. The `log` arm runs at the configuration's
//! defaults, where ten thousand retained entries cover everything the node missed and it is fed
//! from the log. The `snapshot` arm shortens `checkpoint_entries` to sixteen and
//! `retained_entries` to thirty-two, so by the time the node returns every group it hosts has
//! purged past what it holds and it is fed a snapshot per group
//! ([F43](../../../docs/src/features/node-recovery.md)).
//!
//! # What is recorded
//!
//! Beside the kill arm's `cluster.fault` - the outage and the three windows - the harness
//! samples the returning node each second after it is placed
//! ([`catchup`](crate::workloads::harness::catchup)) and the record carries `cluster.catchup`:
//! how it caught up, the seconds from its restart to its convergence, the bytes and entries the
//! snapshots moved and the entries the log fed, and a per second series of its lag. The
//! foreground tail is the fault record's `after` window, read against `before`.
//!
//! # What a smoke run shows
//!
//! Nothing about catch-up, and that is worth knowing before reading one. At smoke scale the
//! run is twenty-four seconds, the node is away for eight, and the survivors' elections at the
//! default failover base take ten to fifteen - so the client's outage outlasts the absence,
//! nothing is written while the node is gone, and it returns behind by the handful of entries
//! that were in flight at the kill, inside the log on both arms. The lag then sits at one to
//! three entries on the groups the node led until its old lease runs out (item 103), and the
//! run ends before it is held at zero, so both arms record `by: none` with the series kept. The
//! snapshot arm exercises its path at full scale, where the survivors serve writes for the last
//! several seconds of the absence; the fixture's `returning_node_catches_up_by_log_or_snapshot`
//! is what proves the path, and this arm is what prices it.

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_failover::Failover;
use crate::workloads::cluster_replication::{placement, REPLICATED};
use crate::workloads::grid::{Grid, Sweep, Table, DEPTH, REFERENCE_MIX, REFERENCE_WIDTH};
use crate::workloads::harness::keys::KeyDistribution;
use crate::workloads::harness::seed::Scale;
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, FaultSpec, Measurement, RetentionOverride, Workload, WorkloadPlan,
};

/// The log arm's identifier
pub const LOG_ID: &str = "macro/cluster/catchup/log";

/// The snapshot arm's identifier
pub const SNAPSHOT_ID: &str = "macro/cluster/catchup/snapshot";

/// How many entries a group commits between snapshots on the snapshot arm
pub const SNAPSHOT_CHECKPOINT_ENTRIES: u64 = 16;

/// How many entries a group keeps behind its snapshot on the snapshot arm
///
/// Well under what the mixture writes to each group in the third of the run the node is away,
/// at either scale: a smoke run on the development host writes about two hundred entries to
/// each of a node's thirty-six groups in that third, and the plan's two hundred and fifty-six
/// would have kept the returning node inside the log at that scale.
pub const SNAPSHOT_RETAINED_ENTRIES: u64 = 32;

/// A catch-up arm: the kill arm with the survivors' retention moved
pub struct Catchup {
    /// The kill arm this drives as, under this arm's own identity and retention
    kill: Failover,
    /// Which arm this is
    id: &'static str,
    /// What the arm says it measures
    summary: &'static str,
    /// The retention the arm moves the placement to, or none for the defaults
    retention: Option<RetentionOverride>,
}

impl Catchup {
    /// The log arm: the kill arm at the defaults, whose returning node the log feeds
    #[must_use]
    pub fn by_log() -> Self {
        Catchup {
            kill: Failover::new(),
            id: LOG_ID,
            summary: "the kill arm with the returning node inside the retained log, fed by log",
            retention: None,
        }
    }

    /// The snapshot arm: the kill arm with the retention shortened past what the node missed
    #[must_use]
    pub fn by_snapshot() -> Self {
        Catchup {
            kill: Failover::new(),
            id: SNAPSHOT_ID,
            summary: "the kill arm with the returning node past the purge point, fed a snapshot per group",
            retention: Some(RetentionOverride {
                checkpoint_entries: SNAPSHOT_CHECKPOINT_ENTRIES,
                retained_entries: SNAPSHOT_RETAINED_ENTRIES,
            }),
        }
    }

    /// The server this arm asks for: the kill arm's placement, with the retention moved
    fn overrides(&self) -> ConfOverrides {
        let mut overrides = placement(REPLICATED);
        if let Some(cluster) = overrides.cluster.as_mut() {
            cluster.retention = self.retention;
        }
        overrides
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Catchup> {
    vec![Catchup::by_log(), Catchup::by_snapshot()]
}

impl Workload for Catchup {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        self.summary
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        self.kill.timing()
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        false
    }

    /// What this workload needs before it can run: the kill arm's plan under this arm's server
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        // the kill arm's twin, under this identity and retention, so the plan is the twin's
        // with one thing moved
        let twin = Grid {
            sweep: Sweep::Replication { durability: "durable" },
            table: Table::Unsorted,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution: KeyDistribution::Uniform,
            depth: DEPTH,
            conf: self.overrides(),
            id: self.id,
            summary: self.summary,
        };
        twin.plan(scale)
    }

    /// Writes the rows the reads will find, the way the kill arm does, untimed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        self.kill.seed(ctx)
    }

    /// Runs the mixture for the scheduled time, as the kill arm does
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        self.kill.run(ctx)
    }

    /// The fault: the kill arm's schedule, unchanged
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn fault(&self, scale: Scale) -> Option<FaultSpec> {
        self.kill.fault(scale)
    }

    /// The returning node is watched until it converges or the run ends
    fn catchup(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{all, LOG_ID, SNAPSHOT_ID, SNAPSHOT_CHECKPOINT_ENTRIES, SNAPSHOT_RETAINED_ENTRIES};
    use crate::workloads::cluster_failover::{self, KILL_ID};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// Both arms are the kill arm on the durable placement, ids in registry order, the
    /// snapshot arm's retention below the log arm's and below what the node misses
    #[test]
    fn the_catchup_arms_share_the_replication_placement() {
        let arms = all();
        assert_eq!(arms.len(), 2);
        assert_eq!(arms[0].id(), LOG_ID);
        assert_eq!(arms[1].id(), SNAPSHOT_ID);
        // registry order: the log arm before the snapshot arm, both after the kill arm
        let ids = crate::workload_ids::IDS;
        let kill = ids.iter().position(|id| *id == KILL_ID).expect("the kill arm is registered");
        let log = ids.iter().position(|id| *id == LOG_ID).expect("the log arm is registered");
        let snapshot = ids.iter().position(|id| *id == SNAPSHOT_ID).expect("the snapshot arm is registered");
        assert!(kill < log && log < snapshot, "the catch-up arms are not appended after the kill arm");
        let kill = cluster_failover::all().into_iter().next().expect("the kill arm exists");
        for scale in [Scale::Smoke, Scale::Full] {
            for arm in &arms {
                // the plan is the kill arm's: placement, factor, rows, depth and mixture, the
                // retention aside
                let mine = arm.plan(scale);
                let theirs = kill.plan(scale);
                assert_eq!(mine.scale, theirs.scale, "{}", arm.id());
                assert_eq!(mine.warmup, theirs.warmup, "{}", arm.id());
                let mine_overrides = mine.server.overrides().expect("a server");
                let theirs_overrides = theirs.server.overrides().expect("a server");
                assert_eq!(mine_overrides.shards, theirs_overrides.shards);
                let mine_cluster = mine_overrides.cluster.as_ref().expect("a placement");
                let theirs_cluster = theirs_overrides.cluster.as_ref().expect("a placement");
                assert_eq!(mine_cluster.replication_factor, theirs_cluster.replication_factor);
                assert_eq!(mine_cluster.peers, theirs_cluster.peers);
                // the same fault, and the returning node watched
                assert_eq!(arm.fault(scale), kill.fault(scale));
                assert!(arm.catchup());
                assert!(!kill.catchup());
            }
        }
        // the log arm keeps the defaults; the snapshot arm's retention is below them
        let log = all()[0].plan(Scale::Full);
        let snapshot = all()[1].plan(Scale::Full);
        let log_retention = log.server.overrides().unwrap().cluster.as_ref().unwrap().retention;
        let snapshot_retention = snapshot.server.overrides().unwrap().cluster.as_ref().unwrap().retention;
        assert_eq!(log_retention, None);
        let snapshot_retention = snapshot_retention.expect("the snapshot arm moves the retention");
        assert_eq!(snapshot_retention.checkpoint_entries, SNAPSHOT_CHECKPOINT_ENTRIES);
        assert_eq!(snapshot_retention.retained_entries, SNAPSHOT_RETAINED_ENTRIES);
        let defaults = shoal::server::conf::cluster::Replication::default();
        assert!(snapshot_retention.checkpoint_entries < defaults.checkpoint_entries);
        assert!(snapshot_retention.retained_entries < defaults.retained_entries);
        assert!(snapshot_retention.retained_entries >= snapshot_retention.checkpoint_entries);
    }
}
