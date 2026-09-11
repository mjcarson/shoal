//! `macro/cluster/overhead/nodes/1` - what running as a cluster node costs a node that is alone
//!
//! # The question
//!
//! [M1](../../../docs/src/distributed/milestones.md#m1-node-identity-and-the-control-plane-thread)
//! puts a control thread, a control core reservation and an embedded consensus group beside the
//! shards of every cluster node, and its exit criterion is to *compare standalone against a
//! matched one-node cluster and investigate any material overhead*. This arm is the cluster half
//! of that comparison. Its standalone twin is `macro/grid/unsorted/r50/1024`, the grid's
//! reference cell, and the two differ in one thing: this arm's server carries a `cluster:` block
//! that bootstraps a cluster of one, on the default control core, with a replication factor of
//! one. The block's every other setting is the documented default.
//!
//! # What the difference is made of
//!
//! Three things, and the arm cannot separate them - it says a cost exists, and the F page says
//! what it is likely made of:
//!
//! - **one fewer shard.** A cluster node keeps its shards off the control core's whole physical
//!   core, both SMT threads; a standalone node keeps them off cpu 0 alone. On the benchmark
//!   host that is one shard candidate fewer, and with `resources.cores` at twelve on a machine
//!   with more than thirteen cores it is no shards fewer at all - the placement moves, the count
//!   does not.
//! - **the control thread's idle work.** A group of one sends no heartbeats; its tick task wakes
//!   on the heartbeat interval and does nothing. The Q13 spike measured that at under a percent
//!   of a core for one group.
//! - **the marker rewrite at start**, which is before the run and outside every sample.
//!
//! # Why one arm and not a sweep
//!
//! There is one node. A `nodes/3` arm needs three processes and a transport, which is M2's, and
//! the identifier is shaped for it - `macro/cluster/overhead/nodes/<n>` - so that the arm that
//! exists today and the ones that follow read as one series. A replication factor axis would be
//! recorded and not enforced at M1, which is an axis with no effect, and an arm that measures no
//! effect is a number that looks like a result.

use crate::workloads::grid::{DEPTH, Grid, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table};
use crate::workloads::harness::keys::KeyDistribution;
use crate::workloads::workload::{ClusterOverride, ConfOverrides};

/// The arm's identifier
///
/// The join key every comparison uses; see `crate::workload_ids`.
pub const ID: &str = "macro/cluster/overhead/nodes/1";

/// Every cluster overhead arm, which is one
pub fn all() -> Vec<Grid> {
    vec![Grid {
        sweep: Sweep::Cluster { nodes: 1 },
        // the persistent unsorted table at the reference mixture, which is the twin's table
        table: Table::Unsorted,
        read_pct: REFERENCE_MIX,
        rows: REFERENCE_WIDTH,
        distribution: KeyDistribution::Uniform,
        depth: DEPTH,
        conf: ConfOverrides {
            cluster: Some(ClusterOverride {
                replication_factor: 1,
            }),
            ..ConfOverrides::default()
        },
        id: ID,
        summary: "the reference mixture served by a one node cluster rather than a standalone node",
    }]
}

#[cfg(test)]
mod tests {
    use super::{ID, all};
    use crate::workloads::grid::{Grid, Sweep};
    use crate::workloads::workload::{ConfOverrides, Workload};

    /// The arm differs from its standalone twin in the cluster block and in nothing else
    ///
    /// The twin is the grid's reference cell, found by its id. Every field of the two but the
    /// override, the sweep, the id and the summary is equal, and the override moves one field.
    #[test]
    fn the_arm_is_the_reference_cell_with_a_cluster_block() {
        let arms = all();
        assert_eq!(arms.len(), 1);
        let arm = &arms[0];
        assert_eq!(arm.id(), ID);
        assert_eq!(arm.sweep, Sweep::Cluster { nodes: 1 });
        let twin = Grid::all()
            .into_iter()
            .find(|cell| cell.id() == "macro/grid/unsorted/r50/1024")
            .expect("the reference cell exists");
        assert_eq!(arm.table, twin.table);
        assert_eq!(arm.read_pct, twin.read_pct);
        assert_eq!(arm.rows, twin.rows);
        assert_eq!(arm.distribution, twin.distribution);
        assert_eq!(arm.depth, twin.depth);
        // the twin pins nothing, and this arm pins the block alone
        assert_eq!(twin.conf, ConfOverrides::default());
        let mut without = arm.conf.clone();
        without.cluster = None;
        assert_eq!(without, ConfOverrides::default(), "the arm moved a field besides the block");
        assert!(arm.conf.cluster.is_some());
    }
}
