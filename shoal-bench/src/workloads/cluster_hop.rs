//! `macro/cluster/hop/{same_shard,local_shard,remote_node}` - what a hop costs a read
//!
//! # The question
//!
//! [M2](../../../docs/src/distributed/milestones.md#m2-the-inter-node-transport) puts a peer
//! transport between nodes and asks, as its exit criterion, for a *local/local-shard/remote hop
//! capture with actual affinity/queue facts* against an initial loopback budget of a hundred
//! microseconds added at the median. These three arms are that capture. Each is the same
//! read-only query - a get of one 1024 byte row from the ephemeral unsorted table, one
//! outstanding at a time - against the same two-node static placement, and they differ in where
//! the row lives relative to the shard that accepted the connection:
//!
//! - **`same_shard`**: node zero has one shard, and every key read is node zero's. The accepting
//!   shard is the owning shard, every time. No hop.
//! - **`local_shard`**: node zero has four shards, and every key read is node zero's - the same
//!   keys `same_shard` reads, since a key's node does not depend on any node's shard count. The
//!   kernel spreads connections over the four shards and the keys spread over them too, so a
//!   quarter of the queries are served where they landed and three quarters cross the mesh.
//! - **`remote_node`**: node zero has one shard, and every key read is node one's. Every query is
//!   forwarded over the data lane, served on node one, and answered back through node zero.
//!
//! `remote_node` less `same_shard` is the peer hop: the frame, the socket, the validation on
//! arrival, the relay to the owning shard and the sealed answer back. That is the number the M2
//! budget is about.
//!
//! # Why `local_shard` is a mixture
//!
//! Every shard of a node binds the client port with `SO_REUSEPORT`, and the kernel picks which
//! shard a connection lands on; nothing in a release build tells the client which. A node with
//! more than one shard therefore cannot serve a pure local-shard control per query, and
//! `StageHop` in `shoal-core/src/server/stage_profile.rs` says as much. The arm states the mix
//! its construction implies on its artifact rather than letting a reader take its median for a
//! pure hop, and with three quarters of its queries crossing the mesh the median and every
//! percentile above it *are* mesh hops - it is the lower quarter that is not. The pure control
//! is [D7](../../../docs/src/direction/shard-aware-routing.md)'s, which makes a connection
//! shard-addressable, and is filed there. Under the `stage-profile` build the stage report
//! splits the arm's records by the hop each one took, which is the per-hop attribution.
//!
//! # Why the ephemeral table, and why depth one
//!
//! [D7](../../../docs/src/direction/shard-aware-routing.md) says where to measure a hop: on the
//! tables that store nothing, because removing storage leaves the hop as the largest share of
//! what remains. And a hop budget is an added latency, so the arms hold one query outstanding -
//! a queue would put waiting into the number and call it transport.

use std::sync::Arc;

use anyhow::Result;
use shoal::server::ring::Ring;
use shoal::shared::traits::PartitionKeySupport;

use crate::model::macro_layer::{HopFacts, HopMix, ScaleFacts, Timing};
use crate::workloads::grid::{
    Payloads, REFERENCE_WIDTH, Table, frame_bytes, queries_for, rows_for, seed_batch,
};
use crate::workloads::harness::driver::{self, Batch, StreamMode};
use crate::workloads::harness::keys::{KeyDistribution, Keys};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, MemItem};
use crate::workloads::workload::{
    BoxFuture, ClusterOverride, ConfOverrides, Context, Measurement, ServerNeed, Workload,
    WorkloadPlan,
};

/// The hop an arm is built to take
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hop {
    /// Served by the shard that accepted the connection
    Same,
    /// Served by another shard of the same node, three times in four
    Local,
    /// Served by the other node
    Remote,
}

/// Every hop arm, in the order they were declared
pub const HOPS: [Hop; 3] = [Hop::Same, Hop::Local, Hop::Remote];

/// How many shards the placed peer runs
///
/// One: the peer exists to own half the key space and answer forwards, and one shard is the
/// cheapest node that does. Its shard count does not decide which node a key lives on.
pub const PEER_SHARDS: u16 = 1;

/// How many shards node zero runs when the arm wants its queries to cross the mesh
///
/// Four rather than two, so the median is unambiguously a mesh hop: with two shards half the
/// queries would be served where they landed and the median would sit on the seam between the
/// two populations. See the module header.
pub const LOCAL_SHARDS: u16 = 4;

impl Hop {
    /// The arm's identifier
    ///
    /// The join key every comparison uses; see `crate::workload_ids`.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Hop::Same => "macro/cluster/hop/same_shard",
            Hop::Local => "macro/cluster/hop/local_shard",
            Hop::Remote => "macro/cluster/hop/remote_node",
        }
    }

    /// The name the artifact records the target under
    #[must_use]
    pub fn target(self) -> &'static str {
        match self {
            Hop::Same => "same",
            Hop::Local => "local",
            Hop::Remote => "remote",
        }
    }

    /// One line saying what this arm isolates
    #[must_use]
    pub fn summary(self) -> &'static str {
        match self {
            Hop::Same => "a read served by the shard that accepted it, on a two node placement",
            Hop::Local => "a read that crosses the mesh to another shard of its node, three times in four",
            Hop::Remote => "a read forwarded over the data lane to the other node and answered back",
        }
    }

    /// How many shards node zero runs
    #[must_use]
    pub fn node_zero_shards(self) -> u16 {
        match self {
            Hop::Same | Hop::Remote => 1,
            Hop::Local => LOCAL_SHARDS,
        }
    }

    /// Which node, by placement position, owns every key the arm reads
    #[must_use]
    pub fn owner_node(self) -> usize {
        match self {
            Hop::Same | Hop::Local => 0,
            Hop::Remote => 1,
        }
    }

    /// The share of queries expected to take each hop, in whole percentages
    ///
    /// The construction's arithmetic and nothing measured: a key on node zero is served where it
    /// landed exactly when the connection landed on its owning shard, which with `s` shards and
    /// keys spread over all of them is one time in `s`.
    #[must_use]
    pub fn expected_mix(self) -> HopMix {
        match self {
            Hop::Same => HopMix {
                same: 100,
                local: 0,
                remote: 0,
            },
            Hop::Local => {
                let same = 100 / u32::from(LOCAL_SHARDS);
                HopMix {
                    same,
                    local: 100 - same,
                    remote: 0,
                }
            }
            Hop::Remote => HopMix {
                same: 0,
                local: 0,
                remote: 100,
            },
        }
    }

    /// The hop record the artifact carries
    #[must_use]
    pub fn facts(self) -> HopFacts {
        HopFacts {
            target: self.target().to_string(),
            owner_node: u32::try_from(self.owner_node()).unwrap_or(u32::MAX),
            expected_mix: self.expected_mix(),
        }
    }

    /// The shard count of every node, in placement order
    #[must_use]
    pub fn shards_per_node(self) -> [u16; 2] {
        [self.node_zero_shards(), PEER_SHARDS]
    }

    /// The first `count` keys the arm's owner node owns, in ascending order
    ///
    /// Walks the key space from zero and keeps a key when the ring the servers route with puts
    /// its tablet on the owner node - the same hash, the same twelve bits and the same
    /// `node = tablet % nodes` rule, through [`Ring::owner_of`], which exists so a benchmark
    /// choosing keys cannot drift from the map. The node a key lands on does not depend on any
    /// node's shard count, so `same_shard` and `local_shard` get the same list.
    ///
    /// # Arguments
    ///
    /// * `count` - How many keys to find
    #[must_use]
    pub fn keys(self, count: u64) -> Vec<u64> {
        let shards = self.shards_per_node();
        let owner = self.owner_node();
        let mut found = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        let mut candidate = 0u64;
        while (found.len() as u64) < count {
            // the partition hash the table computes for this key, then the tablet and its owner
            let hash = MemItem::get_partition_key_from_values(&candidate);
            if Ring::owner_of(Ring::tablet_of(hash), &shards).0 == owner {
                found.push(candidate);
            }
            candidate += 1;
        }
        found
    }
}

/// One hop arm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterHop {
    /// The hop this arm takes
    pub hop: Hop,
}

/// Every hop arm
pub fn all() -> Vec<ClusterHop> {
    HOPS.into_iter().map(|hop| ClusterHop { hop }).collect()
}

impl ClusterHop {
    /// The scale a context was built at
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run this workload was given
    fn scale_of(ctx: &Context) -> Scale {
        if ctx.scale.scale == "smoke" {
            Scale::Smoke
        } else {
            Scale::Full
        }
    }
}

impl Workload for ClusterHop {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.hop.id()
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        self.hop.summary()
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query at a time, each stamped on its own: a sample is a round trip
        Timing::PerQuery
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        // a hotpath profile attributes process time to scopes and would mostly repeat the
        // insert workload's; the hop is a question for the stage layer
        false
    }

    /// Whether the stage layer may run this workload
    fn stage_profiles(&self) -> bool {
        // yes, all three: the stage report splits a run's records by the hop each took, which
        // is the per-hop attribution `local_shard`'s mixture needs and the other two confirm
        true
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let row_bytes = REFERENCE_WIDTH.mean();
        let rows = rows_for(row_bytes, scale);
        WorkloadPlan {
            server: ServerNeed::Fresh(ConfOverrides {
                // node zero's own shard count; the peer's is in the placement
                shards: Some(usize::from(self.hop.node_zero_shards())),
                cluster: Some(ClusterOverride {
                    replication_factor: 1,
                    peers: vec![PEER_SHARDS],
                    hop: Some(self.hop.facts()),
                    read: None,
                    retention: None,
                    retire_after: None,
                    spares: Vec::new(),
                }),
                ..ConfOverrides::default()
            }),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes,
                keys: rows,
                // one outstanding: an added latency, not a throughput
                concurrency: 1,
                clients: None,
                read_pct: Some(100),
                row_profile: REFERENCE_WIDTH.artifact_name(),
                distribution: KeyDistribution::Uniform.artifact_name(),
                table_kind: Some(Table::UnsortedMem.artifact_name()),
            },
            // a twentieth of the run, as the grid does, so the first dial of the peer link and
            // the first connections are behind the arm before it samples anything
            warmup: (queries_for(row_bytes, scale) / 20).max(10),
        }
    }

    /// Writes the rows this arm reads into the owner node, without timing any of it
    ///
    /// Through node zero's client whichever node owns them, so `remote_node`'s seed is itself a
    /// stream of forwards - which is also what dials the data lane before anything is measured.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            let payloads = Payloads::build(REFERENCE_WIDTH, ctx.seed);
            let mut filters = Seeded::stream(ctx.seed, "grid/filters");
            let keys = self.hop.keys(ctx.scale.rows);
            let table = Table::UnsortedMem;
            let width = REFERENCE_WIDTH.mean();
            let batch = seed_batch(REFERENCE_WIDTH.widest(), frame_bytes(ctx));
            let mut built = 0usize;
            let batches = move || {
                if built >= keys.len() {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..batch.min(keys.len() - built) {
                    queries.add_mut(table.insert(
                        keys[built],
                        filters.below(16),
                        payloads.at(width, built as u64),
                    ));
                    built += 1;
                }
                Some(Batch { queries })
            };
            // the measurement is dropped on purpose: seeding is untimed setup
            let _seeding =
                driver::drive_with(&client, batches, "seed", 0, StreamMode::Unordered, 64).await?;
            Ok(())
        })
    }

    /// Reads the owner node's rows one at a time, keeping every round trip
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = Arc::new(ctx.client().await?);
            let queries = queries_for(REFERENCE_WIDTH.mean(), Self::scale_of(ctx));
            // the keys are found before the run, so the walk is outside the wall clock
            let keys = self.hop.keys(ctx.scale.rows);
            let chooser = Keys::new(KeyDistribution::Uniform, ctx.scale.rows, ctx.seed, "hop/reads");
            let table = Table::UnsortedMem;
            driver::drive_per_query(client, 1, queries, ctx.warmup, "get", move |index| {
                // every read is a hit on a key the owner node holds
                table.get(keys[chooser.at(index) as usize])
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{HOPS, Hop, LOCAL_SHARDS, PEER_SHARDS, all};
    use crate::workloads::workload::{ServerNeed, Workload};
    use shoal::server::ring::Ring;
    use shoal::shared::traits::PartitionKeySupport;

    /// The three arms share one placement, one table, one query and one depth, and differ in
    /// the keys they read and in node zero's shard count alone
    #[test]
    fn the_hop_arms_share_one_placement_and_differ_in_keys_and_node_zero_shards() {
        let arms = all();
        assert_eq!(arms.len(), 3);
        let plans: Vec<_> = arms
            .iter()
            .map(|arm| arm.plan(crate::workloads::harness::seed::Scale::Full))
            .collect();
        for (arm, plan) in arms.iter().zip(&plans) {
            let ServerNeed::Fresh(conf) = &plan.server else {
                panic!("{} starts no server", arm.id());
            };
            let cluster = conf.cluster.as_ref().expect("a cluster block");
            // every arm places one peer of one shard, at replication factor one
            assert_eq!(cluster.peers, vec![PEER_SHARDS]);
            assert_eq!(cluster.replication_factor, 1);
            assert_eq!(cluster.hop.as_ref().map(|hop| hop.target.as_str()), Some(arm.hop.target()));
            // node zero's shard count is the one thing the server side moves
            assert_eq!(conf.shards, Some(usize::from(arm.hop.node_zero_shards())));
            let mut rest = conf.clone();
            rest.shards = None;
            rest.cluster = None;
            assert_eq!(rest, Default::default(), "{} moved another field", arm.id());
            // one outstanding, all reads, the ephemeral unsorted table
            assert_eq!(plan.scale.concurrency, 1);
            assert_eq!(plan.scale.read_pct, Some(100));
            assert_eq!(plan.scale.table_kind.as_deref(), Some("ephemeral_unsorted"));
        }
        // the same data at every arm
        assert!(plans.windows(2).all(|pair| pair[0].scale.rows == pair[1].scale.rows));
        assert_eq!(Hop::Same.node_zero_shards(), 1);
        assert_eq!(Hop::Remote.node_zero_shards(), 1);
        assert_eq!(Hop::Local.node_zero_shards(), LOCAL_SHARDS);
    }

    /// Every key an arm reads is owned by the node the arm names, on the ring the servers use,
    /// and the two node-zero arms read the identical list
    #[test]
    fn hop_keys_are_owned_by_the_node_the_arm_names() {
        for hop in HOPS {
            let keys = hop.keys(256);
            assert_eq!(keys.len(), 256);
            for key in &keys {
                let hash = super::MemItem::get_partition_key_from_values(key);
                let (node, _shard) = Ring::owner_of(Ring::tablet_of(hash), &hop.shards_per_node());
                assert_eq!(node, hop.owner_node(), "{} reads key {key} owned by node {node}", hop.id());
            }
            // ascending and distinct, so a chooser's index names one row
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        }
        assert_eq!(Hop::Same.keys(256), Hop::Local.keys(256));
        assert_ne!(Hop::Same.keys(256), Hop::Remote.keys(256));
    }

    /// The mixes are whole percentages summing to a hundred, and the local arm's is the
    /// arithmetic of its shard count
    #[test]
    fn expected_hop_mixes_sum_to_one() {
        for hop in HOPS {
            let mix = hop.expected_mix();
            assert_eq!(mix.same + mix.local + mix.remote, 100, "{}", hop.id());
        }
        assert_eq!(Hop::Same.expected_mix().same, 100);
        assert_eq!(Hop::Remote.expected_mix().remote, 100);
        assert_eq!(Hop::Local.expected_mix().same, 100 / u32::from(LOCAL_SHARDS));
        assert_eq!(Hop::Local.expected_mix().local, 100 - 100 / u32::from(LOCAL_SHARDS));
    }
}
