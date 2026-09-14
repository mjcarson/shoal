//! `macro/cluster/migration/move` - what a move costs the foreground, and how long it takes
//!
//! # The question
//!
//! [M9a](../../../docs/src/distributed/milestones.md#m9a-safe-replica-migration) asks, as its
//! exit criterion, to *record transfer bytes/duration and pauses*
//! ([C10](../../../docs/src/distributed/performance.md)). This arm is that capture: the durable
//! replication placement with a fourth member staged beside it and placed on by nothing, the
//! reference mixture driven for the kill arm's time by the kill arm's client, and a `Move` of
//! one set from node one to the spare asked for a third of the way through, its record polled
//! each second until it is done. The destination is fed as a learner, made a voter through the
//! group's own transition, published, and the source's copy retired, all while the mixture goes
//! on ([F45](../../../docs/src/features/replica-migration.md)).
//!
//! # What is recorded
//!
//! `cluster.migration`: when the move was asked for and when it was done, how long each phase
//! took, what the destination was fed - snapshot bytes and log entries - the client's
//! distribution before, during and after it, and a per second series. The cost is `during`
//! read against `before`; the transfer is `bytes` and `entries` over `seconds`.
//!
//! # What a smoke run shows
//!
//! The shape and little else: at smoke scale the set's rows fit the retained log and the
//! destination is fed by log, so `bytes` is zero. The full run is what prices a move that feeds
//! a snapshot.

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_failover::Failover;
use crate::workloads::cluster_replication::NODE_SHARDS;
use crate::workloads::harness::seed::Scale;
use crate::workloads::workload::{BackgroundKind, BackgroundSpec, BoxFuture, Context, Measurement, ServerNeed, Workload, WorkloadPlan};

/// The arm's identifier
pub const MOVE_ID: &str = "macro/cluster/migration/move";

/// The fraction of the run the move is asked for at
pub const MOVE_AT: (u32, u32) = (1, 3);

/// The tablet whose set moves
pub const MOVE_TABLET: u16 = 0;

/// The node leaving the set, by its staged position: the first placed peer
pub const MOVE_FROM: u32 = 1;

/// The member replacing it, by its staged position: the spare
pub const MOVE_TO: u32 = 3;

/// How long the source keeps its retired copy's files, so the move is done inside the run
///
/// A stale router's window, not a transfer cost: the record's `retiring` phase is this and
/// nothing else, which is why it is read apart from the rest.
pub const RETIRE_AFTER: std::time::Duration = std::time::Duration::from_secs(3);

/// The migration arm: the kill arm's placement and mixture with a spare, and a move instead of a kill
pub struct Migration {
    /// The kill arm this drives as, under this arm's own identity, without its fault
    twin: Failover,
}

impl Migration {
    /// The arm
    #[must_use]
    pub fn new() -> Self {
        Migration { twin: Failover::new() }
    }
}

impl Default for Migration {
    /// The arm
    fn default() -> Self {
        Migration::new()
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Migration> {
    vec![Migration::new()]
}

impl Workload for Migration {
    /// What this workload is called
    fn id(&self) -> &'static str {
        MOVE_ID
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        "the reference mixture on a durable majority of three with a spare beside it, one set moved to \
         the spare a third of the way through while the mixture goes on"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        self.twin.timing()
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        false
    }

    /// What this workload needs before it can run: the kill arm's placement and scale, plus a spare
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let mut plan = self.twin.plan(scale);
        // a fourth member, joined and placed on by nothing, for the move to bring in
        if let ServerNeed::Fresh(overrides) | ServerNeed::RestartAfterSeed(overrides) = &mut plan.server {
            if let Some(cluster) = overrides.cluster.as_mut() {
                cluster.spares = vec![NODE_SHARDS];
                // the source's grace, short enough for the move to be done inside the run
                cluster.retire_after = Some(RETIRE_AFTER);
            }
        }
        plan
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

    /// The move: one set from node one to the spare, a third of the way in
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn background(&self, scale: Scale) -> Option<BackgroundSpec> {
        let run_for = Failover::run_for(scale);
        Some(BackgroundSpec {
            at: run_for * MOVE_AT.0 / MOVE_AT.1,
            run_for,
            table: crate::workloads::cluster_background::REPAIR_TABLE,
            kind: BackgroundKind::Move {
                tablet: MOVE_TABLET,
                from: MOVE_FROM,
                to: MOVE_TO,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{all, MOVE_FROM, MOVE_ID, MOVE_TO};
    use crate::workloads::cluster_background::REPAIR_ID;
    use crate::workloads::cluster_failover::{self, KILL_ID};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::{BackgroundKind, Workload};

    /// The arm is the kill arm on the durable placement with a spare beside it, no fault, and a
    /// move from a placed node to the spare; its id appended after the background arm
    #[test]
    fn the_migration_arm_places_a_fourth_node() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        assert_eq!(arms[0].id(), MOVE_ID);
        let ids = crate::workload_ids::IDS;
        let kill = ids.iter().position(|id| *id == KILL_ID).expect("the kill arm is registered");
        let background = ids.iter().position(|id| *id == REPAIR_ID).expect("the background arm is registered");
        let migration = ids.iter().position(|id| *id == MOVE_ID).expect("the migration arm is registered");
        assert!(kill < background && background < migration, "the migration arm is not appended after the background arm");
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
            // one spare beside the placement, with the peers' shard count, and a short grace
            assert_eq!(mine_cluster.spares, vec![theirs_cluster.peers[0]]);
            assert_eq!(mine_cluster.retire_after, Some(super::RETIRE_AFTER));
            assert_eq!(theirs_cluster.retire_after, None);
            assert_eq!(mine_cluster.nodes(), theirs_cluster.nodes());
            assert_eq!(mine_cluster.members(), theirs_cluster.nodes() + 1);
            assert!(theirs_cluster.spares.is_empty());
            // no fault, no catch-up, and a move inside the run from a placed node to the spare
            assert_eq!(arms[0].fault(scale), None);
            assert!(!arms[0].catchup());
            let spec = arms[0].background(scale).expect("the arm asks for a move");
            assert!(spec.at < spec.run_for);
            assert_eq!(spec.run_for, kill.fault(scale).expect("the kill arm's schedule").run_for);
            match spec.kind {
                BackgroundKind::Move { from, to, .. } => {
                    assert_eq!((from, to), (MOVE_FROM, MOVE_TO));
                    assert!((from as usize) < mine_cluster.nodes(), "the source is not a placed node");
                    assert_eq!(to as usize, mine_cluster.members() - 1, "the destination is not the spare");
                }
                other => panic!("the migration arm asks for {other:?}"),
            }
        }
    }
}
