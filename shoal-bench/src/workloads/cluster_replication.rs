//! `macro/cluster/overhead/nodes/3` and `macro/cluster/replication/{durable,volatile}` - what a
//! quorum write costs beside the same placement replicating to nobody
//!
//! # The question
//!
//! [M4](../../../docs/src/distributed/milestones.md#m4-replication-and-quorum-writes) puts a
//! tablet group under every table and asks, as its exit criterion, for *RF=1 versus RF=3 on
//! the same hardware* ([C10](../../../docs/src/distributed/performance.md)): what a write that
//! waits for two durable copies costs against the same write acknowledged by one. These three
//! arms are that capture. Each is the grid's reference mixture - half reads, the reference
//! width, the reference depth - against the same three node placement with the same cores,
//! and they differ in two things:
//!
//! - **`overhead/nodes/3`**: the persistent unsorted table at a replication factor of one.
//!   Every tablet has one copy, a write is proposed to a group of one and acknowledged by the
//!   shard's own WAL, and nothing crosses the replication lane. It is the three node point of
//!   the series `overhead/nodes/1` began, and the control the two arms below are read against.
//! - **`replication/durable`**: the same table at a factor of three. Every tablet has a copy on
//!   every node, a write is acknowledged once a majority has it in a WAL that was fsynced, and
//!   `overhead/nodes/3` less this is the durable quorum: the frames, the second fsync on a
//!   follower, the acknowledgement back.
//! - **`replication/volatile`**: the ephemeral unsorted table at a factor of three. The same
//!   frames over the same lane to the same followers, acknowledged once a majority holds the
//!   entry in memory. `durable` less this is what the followers' fsync costs; this less
//!   `nodes/3` is the lane and the round trip alone.
//!
//! # Why three shards a node
//!
//! The benchmark host leaves eleven physical cores past the excluded ones and cpu 0's. Three
//! nodes take two of them for the peers' control threads and the rest for shards, which is
//! three shards each, nine in all; the reference cell runs twelve on one node. So
//! `overhead/nodes/3` is not `overhead/nodes/1` with two nodes added, and the F page says so:
//! the series is read for its trend and the two replication arms are read against
//! `nodes/3`, which shares their every core.
//!
//! # Why the factor is refused rather than settled
//!
//! A factor past the node count is served at the node count and admitted at the quorum the
//! factor names, which on one node refuses every default write. The map calls that
//! `active_rf` and the fixture tests it as an availability property; a throughput arm built on
//! it would measure a quorum nobody configured. Every arm here places as many nodes as it asks
//! copies, and [`ClusterOverride::feasibility`] refuses one that does not before a server
//! starts.

use crate::workloads::grid::{DEPTH, Grid, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table};
use crate::workloads::harness::keys::KeyDistribution;
use crate::workloads::workload::{ClusterOverride, ConfOverrides};

/// The three node overhead arm's identifier
pub const NODES_ID: &str = "macro/cluster/overhead/nodes/3";

/// The durable replication arm's identifier
pub const DURABLE_ID: &str = "macro/cluster/replication/durable";

/// The volatile replication arm's identifier
pub const VOLATILE_ID: &str = "macro/cluster/replication/volatile";

/// How many nodes every arm places
pub const NODES: usize = 3;

/// How many shards each node runs
///
/// What eleven free physical cores leave three nodes once two control threads are seated; see
/// the module header.
pub const NODE_SHARDS: u16 = 3;

/// The factor the replication arms replicate at
pub const REPLICATED: u32 = 3;

/// The override every arm shares but for its factor
///
/// # Arguments
///
/// * `replication_factor` - The factor the arm replicates at
#[must_use]
pub fn placement(replication_factor: u32) -> ConfOverrides {
    ConfOverrides {
        shards: Some(usize::from(NODE_SHARDS)),
        cluster: Some(ClusterOverride::placed(
            replication_factor,
            NODES - 1,
            NODE_SHARDS,
        )),
        ..ConfOverrides::default()
    }
}

/// Every arm, in the order they were declared
pub fn all() -> Vec<Grid> {
    vec![
        Grid {
            sweep: Sweep::Cluster { nodes: 3 },
            table: Table::Unsorted,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution: KeyDistribution::Uniform,
            depth: DEPTH,
            conf: placement(1),
            id: NODES_ID,
            summary: "the reference mixture served by three nodes replicating to nobody",
        },
        Grid {
            sweep: Sweep::Replication {
                durability: "durable",
            },
            table: Table::Unsorted,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution: KeyDistribution::Uniform,
            depth: DEPTH,
            conf: placement(REPLICATED),
            id: DURABLE_ID,
            summary: "the reference mixture with every write acknowledged by a durable majority of three",
        },
        Grid {
            sweep: Sweep::Replication {
                durability: "volatile",
            },
            table: Table::UnsortedMem,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution: KeyDistribution::Uniform,
            depth: DEPTH,
            conf: placement(REPLICATED),
            id: VOLATILE_ID,
            summary: "the reference mixture with every write acknowledged by a majority of three holding it in memory",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::{DURABLE_ID, NODE_SHARDS, NODES, NODES_ID, REPLICATED, VOLATILE_ID, all};
    use crate::workloads::grid::{Grid, Sweep, Table};
    use crate::workloads::workload::{ClusterOverride, ConfOverrides, Workload};

    /// The three arms share the reference cell's every axis and one placement, and differ in
    /// the factor and the table alone
    #[test]
    fn the_arms_are_the_reference_cell_on_one_placement() {
        let arms = all();
        assert_eq!(arms.len(), 3);
        let twin = Grid::all()
            .into_iter()
            .find(|cell| cell.id() == "macro/grid/unsorted/r50/1024")
            .expect("the reference cell exists");
        for arm in &arms {
            assert_eq!(arm.read_pct, twin.read_pct, "{}", arm.id());
            assert_eq!(arm.rows, twin.rows, "{}", arm.id());
            assert_eq!(arm.distribution, twin.distribution, "{}", arm.id());
            assert_eq!(arm.depth, twin.depth, "{}", arm.id());
            // every arm places the same nodes with the same shards
            let cluster = arm.conf.cluster.as_ref().expect("a cluster block");
            assert_eq!(cluster.nodes(), NODES);
            assert_eq!(cluster.peers, vec![NODE_SHARDS; NODES - 1]);
            assert_eq!(arm.conf.shards, Some(usize::from(NODE_SHARDS)));
            assert!(cluster.hop.is_none());
            // and moves nothing else
            let mut rest = arm.conf.clone();
            rest.shards = None;
            rest.cluster = None;
            assert_eq!(
                rest,
                ConfOverrides::default(),
                "{} moved another field",
                arm.id()
            );
        }
        // the overhead arm is the control: the persistent table at a factor of one
        assert_eq!(arms[0].id(), NODES_ID);
        assert_eq!(arms[0].sweep, Sweep::Cluster { nodes: 3 });
        assert_eq!(arms[0].table, Table::Unsorted);
        assert_eq!(arms[0].conf.cluster.as_ref().unwrap().replication_factor, 1);
        // durable is the same table at three, volatile the ephemeral one at three
        assert_eq!(arms[1].id(), DURABLE_ID);
        assert_eq!(arms[1].table, Table::Unsorted);
        assert_eq!(
            arms[1].conf.cluster.as_ref().unwrap().replication_factor,
            REPLICATED
        );
        assert_eq!(arms[2].id(), VOLATILE_ID);
        assert_eq!(arms[2].table, Table::UnsortedMem);
        assert_eq!(
            arms[2].conf.cluster.as_ref().unwrap().replication_factor,
            REPLICATED
        );
        assert_eq!(arms[1].conf.cluster, arms[2].conf.cluster);
    }

    /// A factor the placement cannot serve is refused as an availability test rather than run
    /// as a throughput arm at a smaller quorum ([C10](../../../docs/src/distributed/performance.md))
    #[test]
    fn infeasible_rf_policy_is_not_a_throughput_arm() {
        // three copies on one node: the map would serve one and admit at a quorum of two
        let alone = ClusterOverride::alone(3);
        let refused = alone
            .feasibility()
            .expect_err("three copies on one node is infeasible");
        assert!(refused.contains("availability"), "{refused}");
        assert!(refused.contains("quorum of 2"), "{refused}");
        // three copies on two nodes is no better
        assert!(ClusterOverride::placed(3, 1, 1).feasibility().is_err());
        // one copy anywhere, and three on three, are throughput arms
        assert!(ClusterOverride::alone(1).feasibility().is_ok());
        assert!(ClusterOverride::placed(3, 2, 1).feasibility().is_ok());
        // and every arm this build declares is feasible, so no capture is taken at a settled factor
        for workload in crate::workloads::all() {
            let plan = workload.plan(crate::workloads::harness::seed::Scale::Smoke);
            if let Some(cluster) = plan
                .server
                .overrides()
                .and_then(|overrides| overrides.cluster.as_ref())
            {
                assert_eq!(cluster.feasibility(), Ok(()), "{}", workload.id());
            }
        }
    }
}
