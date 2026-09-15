//! `macro/cluster/rebalance/{add,decommission,remove,capacity_blocked}` - what a plan costs
//! the foreground, how long it takes, and what a blocked one looks like
//!
//! # The question
//!
//! [M9b](../../../docs/src/distributed/milestones.md#m9b-capacity-aware-rebalancing-and-removal)
//! asks, as its exit criterion, that a healthy add or drain meets zero final errors and a p99
//! inflation of at most two times inside its documented envelope, and that a capacity-blocked
//! case stays observable ([C10](../../../docs/src/distributed/performance.md)). These four arms
//! are that capture, each the kill arm's durable placement and mixture with the plan the
//! control leader drives in the background ([F46](../../../docs/src/features/capacity-rebalancing.md)):
//!
//! - **`add`**: a spare staged beside the placement and a `Rebalance` asked for a third of the
//!   way through, which is the one way data spreads onto a new node;
//! - **`decommission`**: a spare beside the placement and a `Decommission` of node one, whose
//!   every set moves to the spare one at a time while node one keeps serving;
//! - **`remove`**: a spare beside the placement, node one killed a third of the way through
//!   and never started again, a short grace, and the expiry plan the leader records for it
//!   polled to its end;
//! - **`capacity_blocked`**: no spare, and a `Decommission` of node one that has nowhere to
//!   go at N = RF: the plan is blocked naming the missing member, and the record says so.
//!
//! # What is recorded
//!
//! `cluster.rebalance`: the kind, when the plan was asked for and when it was done, the steps
//! it derived and moved with their bytes, the blocked reason if one, the client's distribution
//! before, during and after it, a per second series, and `p99_ratio_permille` - `during` over
//! `before` - which is the number the two-times budget is judged on. The `remove` arm carries
//! `cluster.fault` beside it, with no restart mark.
//!
//! # What a smoke run shows
//!
//! The shape and little else: at smoke scale a set's rows fit the retained log and every
//! destination is fed by log, so a move is seconds and its bytes are the archives' few. The
//! full run is what prices a drain that feeds snapshots under the byte budget.

use std::time::Duration;

use anyhow::Result;

use crate::model::macro_layer::Timing;
use crate::workloads::cluster_failover::{Failover, KILLED_NODE, fraction_of};
use crate::workloads::cluster_replication::NODE_SHARDS;
use crate::workloads::harness::seed::Scale;
use crate::workloads::workload::{
    BackgroundKind, BackgroundSpec, BoxFuture, Context, FaultSpec, Measurement, ServerNeed,
    Workload, WorkloadPlan,
};

/// The add arm's identifier
pub const ADD_ID: &str = "macro/cluster/rebalance/add";

/// The decommission arm's identifier
pub const DECOMMISSION_ID: &str = "macro/cluster/rebalance/decommission";

/// The remove arm's identifier
pub const REMOVE_ID: &str = "macro/cluster/rebalance/remove";

/// The capacity-blocked arm's identifier
pub const BLOCKED_ID: &str = "macro/cluster/rebalance/capacity_blocked";

/// The fraction of the run the plan is asked for at, or the node killed at
pub const PLAN_AT: (u32, u32) = (1, 3);

/// The member the drain arms take out, by its staged position: the first placed peer
pub const DRAINED_NODE: u32 = KILLED_NODE;

/// How long the source keeps its retired copy's files, so a move is done inside the run
pub const RETIRE_AFTER: Duration = Duration::from_secs(1);

/// The grace the remove arm removes the killed node after
///
/// Short enough that the expiry, the plan and its moves all land inside the run; the
/// documented default is thirty minutes, which no capture could wait out.
pub const AUTO_REMOVE_AFTER: Duration = Duration::from_secs(5);

/// How often the leader looks at its plans on these arms, so a step follows the last closely
pub const PLAN_INTERVAL: Duration = Duration::from_secs(1);

/// How many moves one member is the source and destination of at a time on these arms
///
/// Three, over the default of one: the placement's nine sets drained one at a time would
/// outlast the run, and three at once is the pace a drain is priced at.
pub const MOVES_PER_NODE: u32 = 3;

/// What one of the four arms asks for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceKind {
    /// A spare and a `Rebalance`
    Add,
    /// A spare and a `Decommission` of node one
    Decommission,
    /// A spare, node one killed for good, and the expiry the grace records
    Remove,
    /// No spare and a `Decommission` of node one, which has nowhere to go
    CapacityBlocked,
}

impl RebalanceKind {
    /// The arm's identifier
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            RebalanceKind::Add => ADD_ID,
            RebalanceKind::Decommission => DECOMMISSION_ID,
            RebalanceKind::Remove => REMOVE_ID,
            RebalanceKind::CapacityBlocked => BLOCKED_ID,
        }
    }

    /// The kind's name, as the record spells it
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            RebalanceKind::Add => "rebalance",
            RebalanceKind::Decommission => "decommission",
            RebalanceKind::Remove => "expiry",
            RebalanceKind::CapacityBlocked => "capacity_blocked",
        }
    }

    /// Whether the arm stages a spare beside the placement
    #[must_use]
    pub const fn has_spare(self) -> bool {
        !matches!(self, RebalanceKind::CapacityBlocked)
    }
}

/// One rebalance arm: the kill arm's placement and mixture with a plan in the background
pub struct Rebalance {
    /// What this arm asks for
    kind: RebalanceKind,
    /// The kill arm this drives as, under this arm's own identity, with or without its fault
    twin: Failover,
}

impl Rebalance {
    /// The arm of a kind
    ///
    /// # Arguments
    ///
    /// * `kind` - What it asks for
    #[must_use]
    pub fn new(kind: RebalanceKind) -> Self {
        Rebalance {
            kind,
            twin: Failover::new(),
        }
    }

    /// What this arm asks for
    #[must_use]
    pub const fn kind(&self) -> RebalanceKind {
        self.kind
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Rebalance> {
    vec![
        Rebalance::new(RebalanceKind::Add),
        Rebalance::new(RebalanceKind::Decommission),
        Rebalance::new(RebalanceKind::Remove),
        Rebalance::new(RebalanceKind::CapacityBlocked),
    ]
}

impl Workload for Rebalance {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.kind.id()
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        match self.kind {
            RebalanceKind::Add => {
                "the reference mixture on a durable majority of three with a spare beside it, a \
                 rebalance onto the spare asked for a third of the way through while the mixture goes on"
            }
            RebalanceKind::Decommission => {
                "the reference mixture on a durable majority of three with a spare beside it, node one \
                 decommissioned a third of the way through and drained onto the spare while it serves"
            }
            RebalanceKind::Remove => {
                "the reference mixture on a durable majority of three with a spare beside it, node one \
                 killed a third of the way through and never restarted, its sets rebuilt on the spare \
                 once its grace elapses"
            }
            RebalanceKind::CapacityBlocked => {
                "the reference mixture on a durable majority of three with no spare, node one \
                 decommissioned a third of the way through with nowhere to go: the plan stays blocked"
            }
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        self.twin.timing()
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        false
    }

    /// What this workload needs before it can run: the kill arm's placement and scale, a spare
    /// where the arm has one, and the grace and plan knobs the arm moves
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let mut plan = self.twin.plan(scale);
        if let ServerNeed::Fresh(overrides) | ServerNeed::RestartAfterSeed(overrides) =
            &mut plan.server
        {
            if let Some(cluster) = overrides.cluster.as_mut() {
                // a fourth member, joined and placed on by nothing, for the plan to bring in
                if self.kind.has_spare() {
                    cluster.spares = vec![NODE_SHARDS];
                }
                // the source's grace, short enough for a move to be done inside the run
                cluster.retire_after = Some(RETIRE_AFTER);
                // the leader looks at its plans every second, so a step follows the last
                // closely, and moves three sets at a time so a drain fits the run
                cluster.plan_interval = Some(PLAN_INTERVAL);
                cluster.moves_per_node = Some(MOVES_PER_NODE);
                // the remove arm's grace, short enough to elapse inside the run
                if self.kind == RebalanceKind::Remove {
                    cluster.auto_remove_after = Some(AUTO_REMOVE_AFTER);
                }
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

    /// The remove arm's kill: node one a third of the way in, never started again
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn fault(&self, scale: Scale) -> Option<FaultSpec> {
        if self.kind != RebalanceKind::Remove {
            return None;
        }
        let run_for = Failover::run_for(scale);
        Some(FaultSpec {
            node: DRAINED_NODE,
            at: fraction_of(run_for, PLAN_AT),
            restart_after: Duration::ZERO,
            restart: false,
            run_for,
        })
    }

    /// The plan: asked for a third of the way in, or watched for from there
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn background(&self, scale: Scale) -> Option<BackgroundSpec> {
        let run_for = Failover::run_for(scale);
        let kind = match self.kind {
            RebalanceKind::Add => BackgroundKind::Rebalance,
            RebalanceKind::Decommission => BackgroundKind::Decommission {
                node: DRAINED_NODE,
                blocked: false,
            },
            RebalanceKind::CapacityBlocked => BackgroundKind::Decommission {
                node: DRAINED_NODE,
                blocked: true,
            },
            RebalanceKind::Remove => BackgroundKind::Expire { node: DRAINED_NODE },
        };
        Some(BackgroundSpec {
            at: fraction_of(run_for, PLAN_AT),
            run_for,
            table: crate::workloads::cluster_background::REPAIR_TABLE,
            kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ADD_ID, BLOCKED_ID, DECOMMISSION_ID, DRAINED_NODE, REMOVE_ID, RebalanceKind, all};
    use crate::workloads::cluster_failover;
    use crate::workloads::cluster_migration::MOVE_ID;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::{BackgroundKind, Workload};

    /// The four arms are the kill arm on the durable placement, three with a spare and one
    /// without, each asking for its plan inside the run - the remove arm with a kill and no
    /// restart and a short grace - and their ids appended after the migration arm in order
    #[test]
    fn the_rebalance_arms_share_the_kill_arms_placement() {
        let arms = all();
        let ids: Vec<&str> = arms.iter().map(|arm| arm.id()).collect();
        assert_eq!(ids, [ADD_ID, DECOMMISSION_ID, REMOVE_ID, BLOCKED_ID]);
        let registered = crate::workload_ids::IDS;
        let migration = registered
            .iter()
            .position(|id| *id == MOVE_ID)
            .expect("the migration arm is registered");
        for (offset, id) in ids.iter().enumerate() {
            let position = registered
                .iter()
                .position(|known| known == id)
                .unwrap_or_else(|| panic!("{id} is not registered"));
            assert_eq!(
                position,
                migration + 1 + offset,
                "{id} is not appended in order after the migration arm"
            );
        }
        let kill = cluster_failover::all()
            .into_iter()
            .next()
            .expect("the kill arm exists");
        for arm in &arms {
            for scale in [Scale::Smoke, Scale::Full] {
                let mine = arm.plan(scale);
                let theirs = kill.plan(scale);
                assert_eq!(mine.scale, theirs.scale);
                assert_eq!(mine.warmup, theirs.warmup);
                let mine_overrides = mine.server.overrides().expect("a server");
                let theirs_overrides = theirs.server.overrides().expect("a server");
                assert_eq!(mine_overrides.shards, theirs_overrides.shards);
                let mine_cluster = mine_overrides.cluster.as_ref().expect("a placement");
                let theirs_cluster = theirs_overrides.cluster.as_ref().expect("a placement");
                assert_eq!(
                    mine_cluster.replication_factor,
                    theirs_cluster.replication_factor
                );
                assert_eq!(mine_cluster.peers, theirs_cluster.peers);
                assert_eq!(mine_cluster.nodes(), theirs_cluster.nodes());
                // a spare on every arm but the blocked one, and the grace on the remove arm alone
                if arm.kind().has_spare() {
                    assert_eq!(
                        mine_cluster.spares,
                        vec![theirs_cluster.peers[0]],
                        "{}",
                        arm.id()
                    );
                    assert_eq!(mine_cluster.members(), theirs_cluster.nodes() + 1);
                } else {
                    assert!(mine_cluster.spares.is_empty(), "{}", arm.id());
                    assert_eq!(mine_cluster.members(), theirs_cluster.nodes());
                }
                assert_eq!(mine_cluster.retire_after, Some(super::RETIRE_AFTER));
                assert_eq!(mine_cluster.plan_interval, Some(super::PLAN_INTERVAL));
                assert_eq!(mine_cluster.moves_per_node, Some(super::MOVES_PER_NODE));
                assert_eq!(
                    mine_cluster.auto_remove_after,
                    (arm.kind() == RebalanceKind::Remove).then_some(super::AUTO_REMOVE_AFTER),
                    "{}",
                    arm.id()
                );
                assert!(theirs_cluster.spares.is_empty());
                assert_eq!(theirs_cluster.auto_remove_after, None);
                // the plan inside the run, and the kill on the remove arm alone, never restarted
                let spec = arm.background(scale).expect("the arm asks for a plan");
                assert!(spec.at < spec.run_for);
                assert_eq!(
                    spec.run_for,
                    kill.fault(scale).expect("the kill arm's schedule").run_for
                );
                assert!(spec.kind.is_plan());
                match (arm.kind(), &spec.kind) {
                    (RebalanceKind::Add, BackgroundKind::Rebalance) => {}
                    (
                        RebalanceKind::Decommission,
                        BackgroundKind::Decommission {
                            node,
                            blocked: false,
                        },
                    )
                    | (
                        RebalanceKind::CapacityBlocked,
                        BackgroundKind::Decommission {
                            node,
                            blocked: true,
                        },
                    ) => {
                        assert_eq!(*node, DRAINED_NODE);
                    }
                    (RebalanceKind::Remove, BackgroundKind::Expire { node }) => {
                        assert_eq!(*node, DRAINED_NODE)
                    }
                    (kind, spec) => panic!("{kind:?} asks for {spec:?}"),
                }
                match arm.fault(scale) {
                    Some(fault) => {
                        assert_eq!(arm.kind(), RebalanceKind::Remove);
                        assert_eq!(fault.node, DRAINED_NODE);
                        assert!(!fault.restart);
                        assert_eq!(fault.at, spec.at);
                    }
                    None => assert_ne!(arm.kind(), RebalanceKind::Remove),
                }
                assert!(!arm.catchup());
            }
        }
    }
}
