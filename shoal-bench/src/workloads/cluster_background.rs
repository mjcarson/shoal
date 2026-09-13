//! `macro/cluster/background/repair` - what a scrub costs the foreground while it runs
//!
//! # The question
//!
//! [M8](../../../docs/src/distributed/milestones.md#m8-repair) asks, as its exit criterion,
//! for *scrub/repair resource and foreground-latency interference*
//! ([C10](../../../docs/src/distributed/performance.md)), and [Q12](../../../docs/src/distributed/protocol.md)
//! wants a cost before a scheduled scrub gets a default. This arm is that capture: the durable
//! replication placement and the reference mixture, driven for the kill arm's time by the kill
//! arm's client, with a `Repair` of the reference table in verify mode asked for a third of
//! the way through and its record polled each second until every group is done. Nothing is
//! killed and nothing is installed; every group's leader scrubs it - the resident partitions
//! hashed on the loop, the archived ones read off the disk on a task - while the mixture goes
//! on ([F44](../../../docs/src/features/repair.md)).
//!
//! # What is recorded
//!
//! `cluster.background`: when the repair was asked for and when it was done, how many groups
//! it covered and how many came to a clean verdict, what the scrubs hashed and read across
//! every node, the client's distribution before, during and after it, and a per second series.
//! The interference is `during` read against `before`; the resource is `bytes` and
//! `partitions` over `seconds`.
//!
//! # What a smoke run shows
//!
//! The shape and little else: at smoke scale the table is a hundredth of its size and every
//! partition is resident, so the scrubs read nothing off the disk and `bytes` is zero. The
//! full run is what prices a scrub that reads the archives.

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_failover::Failover;
use crate::workloads::harness::seed::Scale;
use crate::workloads::workload::{BackgroundKind, BackgroundSpec, BoxFuture, Context, Measurement, Workload, WorkloadPlan};

/// The arm's identifier
pub const REPAIR_ID: &str = "macro/cluster/background/repair";

/// The fraction of the run the repair is asked for at
pub const REPAIR_AT: (u32, u32) = (1, 3);

/// The table the repair verifies: the reference mixture's
pub const REPAIR_TABLE: &str = "Item";

/// The background arm: the kill arm's placement and mixture with a repair instead of a kill
pub struct Background {
    /// The kill arm this drives as, under this arm's own identity, without its fault
    twin: Failover,
}

impl Background {
    /// The arm
    #[must_use]
    pub fn new() -> Self {
        Background { twin: Failover::new() }
    }
}

impl Default for Background {
    /// The arm
    fn default() -> Self {
        Background::new()
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Background> {
    vec![Background::new()]
}

impl Workload for Background {
    /// What this workload is called
    fn id(&self) -> &'static str {
        REPAIR_ID
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        "the reference mixture on a durable majority of three, with a verify of the table asked for a \
         third of the way through and scrubbed in the background"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        self.twin.timing()
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        false
    }

    /// What this workload needs before it can run: the kill arm's placement and scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        self.twin.plan(scale)
    }

    /// Writes the rows the reads will find, the way the kill arm does, untimed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        self.twin.seed(ctx)
    }

    /// Runs the mixture for the scheduled time, as the kill arm does
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        self.twin.run(ctx)
    }

    /// The repair: a verify of the reference table a third of the way in
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn background(&self, scale: Scale) -> Option<BackgroundSpec> {
        let run_for = Failover::run_for(scale);
        Some(BackgroundSpec {
            at: run_for * REPAIR_AT.0 / REPAIR_AT.1,
            run_for,
            table: REPAIR_TABLE,
            kind: BackgroundKind::Repair,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{all, REPAIR_ID, REPAIR_TABLE};
    use crate::workloads::cluster_catchup::SNAPSHOT_ID;
    use crate::workloads::cluster_failover::{self, KILL_ID};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// The arm is the kill arm on the durable placement with no fault and a repair, its id
    /// appended after the catch-up arms
    #[test]
    fn the_background_arm_shares_the_replication_placement() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        assert_eq!(arms[0].id(), REPAIR_ID);
        let ids = crate::workload_ids::IDS;
        let kill = ids.iter().position(|id| *id == KILL_ID).expect("the kill arm is registered");
        let snapshot = ids.iter().position(|id| *id == SNAPSHOT_ID).expect("the snapshot arm is registered");
        let background = ids.iter().position(|id| *id == REPAIR_ID).expect("the background arm is registered");
        assert!(kill < snapshot && snapshot < background, "the background arm is not appended after the catch-up arms");
        let kill = cluster_failover::all().into_iter().next().expect("the kill arm exists");
        for scale in [Scale::Smoke, Scale::Full] {
            let mine = arms[0].plan(scale);
            let theirs = kill.plan(scale);
            assert_eq!(mine.scale, theirs.scale);
            assert_eq!(mine.warmup, theirs.warmup);
            let mine_overrides = mine.server.overrides().expect("a server");
            let theirs_overrides = theirs.server.overrides().expect("a server");
            assert_eq!(mine_overrides.shards, theirs_overrides.shards);
            let mine_cluster = mine_overrides.cluster.as_ref().expect("a placement");
            let theirs_cluster = theirs_overrides.cluster.as_ref().expect("a placement");
            assert_eq!(mine_cluster.replication_factor, theirs_cluster.replication_factor);
            assert_eq!(mine_cluster.peers, theirs_cluster.peers);
            // no fault, no catch-up, and a repair inside the run of the reference table
            assert_eq!(arms[0].fault(scale), None);
            assert!(!arms[0].catchup());
            let spec = arms[0].background(scale).expect("the arm asks for a repair");
            assert_eq!(spec.table, REPAIR_TABLE);
            assert!(spec.at < spec.run_for);
            assert_eq!(spec.run_for, kill.fault(scale).expect("the kill arm's schedule").run_for);
            assert_eq!(kill.background(scale), None);
        }
        // the table the repair names is one the schema spells, and a persistent one
        let persistent = <crate::workloads::schema::Bench as shoal::server::database::ShoalDatabase>::persistent_tables();
        assert!(persistent.contains(&REPAIR_TABLE), "{persistent:?}");
    }
}
