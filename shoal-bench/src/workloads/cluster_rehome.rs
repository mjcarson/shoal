//! `macro/rehome/shrink` - what a node pays at start to run fewer executors than it wrote with
//!
//! # The question
//!
//! [M9c](../../../docs/src/distributed/milestones.md#m9c-change-local-shard-count) retires the
//! refusal of a directory reopened under another core count with a rehome: the files the
//! vanished executors left are folded, copied, moved and reclaimed before a shard starts
//! ([F47](../../../docs/src/features/local-rehome.md)). Its exit criterion asks for the
//! resource and startup costs to be recorded. This arm records them: the one node cluster arm's
//! server is seeded at twelve executors, stopped, and started again at eight, so the start
//! between the two runs a rehome of four executors' files onto the eight that remain. What it
//! moved and how long the start was held for it is `cluster.rehome` on the capture, from the
//! pool's own report.
//!
//! # What the run measures
//!
//! The reference mixture after the rehome, on eight executors hosting twelve slots. That is
//! not the reference cell's number and is not read against it: the arm's own record is the
//! result, and the mixture after it is evidence that the node serves at all. A rehome is
//! outside every sample by construction - nothing serves while it runs - so its cost is the
//! report's `millis`, not a window of the distribution.
//!
//! # Why a cluster node, and why a shrink
//!
//! A cluster node's rehome moves the shared WAL's tablet groups as well as the archives, which
//! is the half a standalone node does not have and the half the crash matrix exercises; and a
//! cluster node is what carries a `cluster.*` record at all. A shrink is the direction that
//! deletes: a vanished executor's files are reclaimed whole, where a growth's donor keeps its
//! archives (O58). A growth arm is a second identifier for a later capture, not a variant of
//! this one.

use crate::workloads::grid::{DEPTH, Grid, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table};
use crate::workloads::harness::keys::KeyDistribution;
use crate::workloads::workload::{ClusterOverride, ConfOverrides};

/// The arm's identifier
///
/// The join key every comparison uses; see `crate::workload_ids`.
pub const ID: &str = "macro/rehome/shrink";

/// How many executors the server is seeded with
pub const SEEDED_SHARDS: usize = 12;

/// How many it restarts with, which is what the rehome moves onto
pub const RESTART_SHARDS: usize = 8;

/// Every rehome arm, which is one
pub fn all() -> Vec<Grid> {
    vec![Grid {
        sweep: Sweep::Rehome,
        // the persistent unsorted table at the reference mixture, which is the twin's table
        table: Table::Unsorted,
        read_pct: REFERENCE_MIX,
        rows: REFERENCE_WIDTH,
        distribution: KeyDistribution::Uniform,
        depth: DEPTH,
        conf: ConfOverrides {
            // a cluster of one, so the rehome moves tablet groups and the capture carries a record
            cluster: Some(ClusterOverride::alone(1)),
            // seeded at one count and measured at another: the two the rehome runs between
            shards: Some(SEEDED_SHARDS),
            restart_shards: Some(RESTART_SHARDS),
            ..ConfOverrides::default()
        },
        id: ID,
        summary: "the reference mixture after a restart from twelve executors to eight, with what the rehome moved and cost",
    }]
}

#[cfg(test)]
mod tests {
    use super::{ID, RESTART_SHARDS, SEEDED_SHARDS, all};
    use crate::workloads::grid::Sweep;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// The arm restarts its server at fewer shards than it seeded with, as a cluster of one
    ///
    /// A restart is what runs the rehome; fewer shards is the shrink the arm is named for; and
    /// the cluster block is what puts the record on the capture at all.
    #[test]
    fn the_rehome_arm_restarts_at_fewer_shards() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        let arm = &arms[0];
        assert_eq!(arm.id(), ID);
        assert_eq!(arm.sweep, Sweep::Rehome);
        // the server is cycled between the seed and the measurement
        let plan = arm.plan(Scale::Full);
        assert!(plan.server.restarts(), "the rehome arm does not restart its server");
        let overrides = plan.server.overrides().expect("the arm needs a server");
        assert_eq!(overrides.shards, Some(SEEDED_SHARDS));
        assert_eq!(overrides.restart_shards, Some(RESTART_SHARDS));
        assert!(RESTART_SHARDS < SEEDED_SHARDS, "a shrink restarts at fewer executors");
        // and it is a cluster of one, so the rehome moves groups and the capture has a record
        let cluster = overrides.cluster.as_ref().expect("a cluster block");
        assert!(cluster.peers.is_empty());
        assert_eq!(cluster.replication_factor, 1);
        // the mixture is the reference cell's, and the smoke scale runs it too
        assert_eq!(plan.scale.read_pct, Some(50));
        assert!(arm.plan(Scale::Smoke).server.restarts());
    }
}
