//! `macro/cluster/failover/kill` - what a client sees when a node holding a third of the
//! primaries is killed under the reference mixture, and when it comes back
//!
//! # The question
//!
//! [M6](../../../docs/src/distributed/milestones.md#m6-primary-failover) moves leadership when
//! a primary dies and asks, as its exit criterion, for *an outage and recovery capture under a
//! specified fault schedule* ([C10](../../../docs/src/distributed/performance.md),
//! `macro/cluster/failover`). This arm is that capture: the durable replication arm's placement
//! and mixture - three nodes of three shards, every tablet on every node, half reads at the
//! reference width and depth - driven for a fixed time rather than a fixed count, with node one
//! killed a third of the way through and started again from the same identity two thirds of
//! the way through. What the client sees is kept on a timeline and cut into three windows
//! ([`fault`](crate::workloads::harness::fault)): `before`, from the start to its first failed
//! operation after the kill; `during`, until a sustained run of its operations succeeded again;
//! and `after`, the rest, which holds the returning node's catch-up. Each window has its own
//! distribution, the outage is their boundary in milliseconds, and a per second series keeps
//! the dip visible when the windows are read as three numbers.
//!
//! # What the client is
//!
//! A client with no retry, on purpose. [F42](../../../docs/src/features/primary-failover.md)
//! gives the client an identity and an opt-in retry, and an arm driving with them would
//! measure how well the retry hides the outage; this arm measures the outage. Every failed
//! operation is counted under `failed`, stamped on the timeline, and followed by a short pause
//! so a dead endpoint is not counted a thousand times a second. The reads are `One` from the
//! local replica and never fail - node zero holds every tablet - so the outage is the writes':
//! the third of the groups node one led, refused or unanswered until their elections, at the
//! cluster's default failover base.
//!
//! # Why node one, and why the time is fixed
//!
//! Node zero is this process and cannot be killed; node one is the first peer and leads a
//! third of the groups, since every group starts where the placement put its primary. A count
//! would end before or after the fault depending on the day's throughput, so the run is a
//! duration and the schedule is a fraction of it, at both scales.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_replication::{REPLICATED, placement};
use crate::workloads::grid::{
    DEPTH, Grid, Payloads, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table, is_read,
};
use crate::workloads::harness::driver;
use crate::workloads::harness::keys::{KeyDistribution, Keys};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::workload::{
    BoxFuture, Context, FaultSpec, Measurement, Workload, WorkloadPlan,
};

/// The kill arm's identifier
pub const KILL_ID: &str = "macro/cluster/failover/kill";

/// The node the arm kills, by placement position
///
/// The first peer: node zero is the driver's own process.
pub const KILLED_NODE: u32 = 1;

/// How long the arm runs at full scale
///
/// A minute: long enough for a default failover base of five seconds to elect, for the killed
/// node to be gone for a while, and for it to come back and catch up inside the run.
pub const RUN_FOR: Duration = Duration::from_secs(60);

/// How long the arm runs at smoke scale
///
/// Long enough for the elections at the default base to finish inside the `during` window and
/// the restart to land inside the run; a smoke run proves the arm runs, not what it costs.
pub const SMOKE_RUN_FOR: Duration = Duration::from_secs(24);

/// The schedule as fractions of the run: the kill a third in, the restart two thirds in
pub const KILL_AT: (u32, u32) = (1, 3);

/// Where the restart lands, as a fraction of the run
pub const RESTART_AT: (u32, u32) = (2, 3);

/// The failover arm
pub struct Failover {
    /// The durable replication arm under this arm's identity, whose plan and seed this shares
    twin: Grid,
}

impl Failover {
    /// The arm: the durable replication cell with a fault in the middle of it
    #[must_use]
    pub fn new() -> Self {
        Failover {
            twin: Grid {
                sweep: Sweep::Replication {
                    durability: "durable",
                },
                table: Table::Unsorted,
                read_pct: REFERENCE_MIX,
                rows: REFERENCE_WIDTH,
                distribution: KeyDistribution::Uniform,
                depth: DEPTH,
                conf: placement(REPLICATED),
                id: KILL_ID,
                summary: "the reference mixture on a durable majority of three, with node one killed a third \
                          of the way through and started again two thirds through",
            },
        }
    }

    /// How long the arm runs at a scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    #[must_use]
    pub fn run_for(scale: Scale) -> Duration {
        match scale {
            Scale::Smoke => SMOKE_RUN_FOR,
            Scale::Full => RUN_FOR,
        }
    }
}

impl Default for Failover {
    /// The arm
    fn default() -> Self {
        Failover::new()
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Failover> {
    vec![Failover::new()]
}

/// A fraction of a duration
///
/// # Arguments
///
/// * `whole` - The duration
/// * `fraction` - The numerator and denominator
pub fn fraction_of(whole: Duration, fraction: (u32, u32)) -> Duration {
    whole * fraction.0 / fraction.1
}

impl Workload for Failover {
    /// What this workload is called
    fn id(&self) -> &'static str {
        KILL_ID
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        self.twin.summary
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own, as the twin's are
        Timing::PerQuery
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        // a profile of a run with a kill in it attributes the kill to whatever was on the stack
        false
    }

    /// What this workload needs before it can run: the twin's placement and scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        self.twin.plan(scale)
    }

    /// Writes the rows the reads will find, the way the twin does, untimed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        self.twin.seed(ctx)
    }

    /// Runs the mixture for the scheduled time, keeping every operation on the timeline
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let clients = vec![Arc::new(ctx.client().await?)];
            let rows = ctx.scale.rows;
            let run_for = Failover::run_for(Grid::scale_of(ctx));
            // built before the run, as the twin builds them, so nothing is paid for inside it
            let payloads = Payloads::build(self.twin.rows, ctx.seed);
            let reads = Keys::new(self.twin.distribution, rows, ctx.seed, "grid/reads");
            let mix_seed = Seeded::stream(ctx.seed, "grid/mix").next_u64();
            let profile = self.twin.rows;
            let table = self.twin.table;
            let read_pct = self.twin.read_pct;
            let seed = ctx.seed;
            driver::drive_mixed_timed(
                &clients,
                self.twin.depth,
                run_for,
                ctx.warmup,
                move |index| {
                    if is_read(mix_seed, index, read_pct) {
                        // a read asks for a key inside the seeded range, so every read is a hit
                        ("read", table.get(reads.at(index)))
                    } else {
                        // a write lands past the seeded range, as the twin's do
                        let key = rows + index;
                        let width = profile.width(seed, key);
                        (
                            "write",
                            table.insert(key, index % 16, payloads.at(width, index)),
                        )
                    }
                },
            )
            .await
        })
    }

    /// The fault: node one killed a third of the way in and started again two thirds in
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn fault(&self, scale: Scale) -> Option<FaultSpec> {
        let run_for = Failover::run_for(scale);
        let at = fraction_of(run_for, KILL_AT);
        Some(FaultSpec {
            node: KILLED_NODE,
            at,
            restart_after: fraction_of(run_for, RESTART_AT).saturating_sub(at),
            restart: true,
            run_for,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Failover, KILL_ID, KILLED_NODE, RUN_FOR, SMOKE_RUN_FOR, all};
    use crate::workloads::cluster_replication::{self, DURABLE_ID};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// The arm is the durable replication arm with a fault: same placement, same scale, same
    /// mixture, and a schedule inside the run at both scales
    #[test]
    fn the_failover_arm_shares_the_replication_placement() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        let arm = &arms[0];
        assert_eq!(arm.id(), KILL_ID);
        let durable = cluster_replication::all()
            .into_iter()
            .find(|arm| arm.id() == DURABLE_ID)
            .expect("the durable arm exists");
        for scale in [Scale::Smoke, Scale::Full] {
            // the plan is the twin's: placement, factor, rows, depth and mixture
            let mine = arm.plan(scale);
            let theirs = durable.plan(scale);
            assert_eq!(mine.server, theirs.server);
            assert_eq!(mine.scale, theirs.scale);
            assert_eq!(mine.warmup, theirs.warmup);
            // the fault is on the first peer, never on this process, and inside the run
            let fault = arm.fault(scale).expect("a fault");
            assert_eq!(fault.node, KILLED_NODE);
            assert_ne!(fault.node, 0);
            let run_for = Failover::run_for(scale);
            assert!(fault.at > Duration::ZERO);
            assert!(fault.at + fault.restart_after < run_for, "{scale:?}");
            // and leaves the cluster a while on both sides of it
            assert!(fault.at >= run_for / 4, "{scale:?}");
            assert!(
                run_for - (fault.at + fault.restart_after) >= run_for / 4,
                "{scale:?}"
            );
        }
        assert_eq!(Failover::run_for(Scale::Full), RUN_FOR);
        assert_eq!(Failover::run_for(Scale::Smoke), SMOKE_RUN_FOR);
        // the twin asks for no fault, so nothing about it changed
        assert_eq!(durable.fault(Scale::Full), None);
    }
}
