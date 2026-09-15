//! `macro/cluster/reads/{one,barrier,session}` and `macro/cluster/fanout/{get,filter,limit,empty}`
//! - what a strong read costs beside a `One` read of the same state, and what a read that fans
//! out over every node costs when its shares are full, filtered, capped or empty
//!
//! # The question
//!
//! [M5](../../../docs/src/distributed/milestones.md#m5-read-consistency-levels) puts a read
//! barrier under the strong read level and a session token under the read-your-writes one, and
//! asks, as its exit criterion, for *One/barrier/session and fanout read captures with the
//! barrier and application wait visible* ([C10](../../../docs/src/distributed/performance.md)).
//! These seven arms are that capture, in two sets on two placements:
//!
//! - **`reads/one`**, **`reads/barrier`** and **`reads/session`** are one get of one 1024 byte
//!   row from the persistent unsorted table at the reference depth, against the replication
//!   arms' placement - three nodes of three shards, every tablet on every node - and they
//!   differ in nothing but what the read asks for. `one` reads the local replica. `barrier`
//!   asks for `Quorum`: the shard obtains a read barrier from its group's leader, on this shard
//!   when it leads and over the replication lane when it does not, and applies through it
//!   before it reads. `session` reads at `One` carrying the token the seeding write to that
//!   tablet handed back, so the replica waits until it has applied past the write and no
//!   further. `barrier` less `one` is the barrier: the heartbeat round on the leader, the hop
//!   two times in three, and the application wait. `session` less `one` is the token check and
//!   a wait that is almost always already satisfied.
//! - **`fanout/get`**, **`fanout/filter`**, **`fanout/limit`** and **`fanout/empty`** are one
//!   get of six keys, two on each of three nodes at a factor of one, so every read is split
//!   three ways and two shares cross the data lane. `get` reads six rows; `filter` names a
//!   bucket half the rows are in and reads three; `limit` caps the answer at three; `empty`
//!   names six keys that were never written and reads none. Every share still covers its slot,
//!   so `empty` pays the whole fan-out for an empty answer - which is the point: a missing
//!   share and an empty one are told apart by the slot, not by the rows, and the arm shows
//!   what that costs.
//!
//! # Why the read arms have no write background
//!
//! The three read arms answer what the barrier costs against `One` on the same state. A write
//! stream running beside them would make the arms differ by write mix and follower lag rather
//! than by level, and the number a reader wants - the barrier's own cost - would be inside a
//! difference it could not take apart. The read-under-writes arm is filed with the open-loop
//! schedule [F40](../../../docs/src/features/replication.md) filed, and reads the same way.
//!
//! # Why the fanout arms sit at a factor of one
//!
//! At a factor of three every node holds every tablet and nothing fans out: a six key get is
//! served whole by the node that accepted it. A factor of one is the placement where a read
//! over several keys has to cross nodes, and it is the hop arms' placement grown to three
//! nodes, so the cost of a share is a number [F38](../../../docs/src/features/inter-node-transport.md)
//! already measured once.

use std::sync::Arc;

use anyhow::Result;
use shoal::client::{QuerySuceededOpts, ReadLevel, SendOptions};
use shoal::server::ring::Ring;
use shoal::shared::protocol::read::SessionToken;
use shoal::shared::traits::PartitionKeySupport;

use crate::model::macro_layer::{FanoutFacts, ScaleFacts, Timing};
use crate::workloads::cluster_replication::{NODE_SHARDS, NODES, placement};
use crate::workloads::grid::{
    DEPTH, Payloads, REFERENCE_WIDTH, Table, frame_bytes, queries_for, rows_for, seed_batch,
};
use crate::workloads::harness::driver::{self, Batch, StreamMode};
use crate::workloads::harness::keys::{KeyDistribution, Keys};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, Item, ItemFilter, ItemGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ReadArm, ServerNeed, Workload, WorkloadPlan,
};

/// How many keys a fanout read names
pub const FANOUT_KEYS: u32 = 6;

/// The limit the capped fanout arm sets
pub const FANOUT_LIMIT: u32 = 3;

/// The bucket the filtered fanout arm reads, which half the seeded rows are in
pub const FANOUT_BUCKET: u64 = 1;

/// How many buckets the fanout arms deal their rows over
const FANOUT_BUCKETS: u64 = 2;

/// The keys the empty fanout arm reads, past anything seeded
const EMPTY_FROM: u64 = 1 << 40;

/// The one read arm, in the order they were declared
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arm {
    /// A `One` read of the local replica
    One,
    /// A `Quorum` read through a barrier
    Barrier,
    /// A `One` read past the token of the last write to its tablet
    Session,
    /// A six key get over three nodes, every row returned
    FanoutGet,
    /// The same get with a filter half the rows pass
    FanoutFilter,
    /// The same get capped at three rows
    FanoutLimit,
    /// A six key get over keys that were never written
    FanoutEmpty,
}

/// Every arm, in the order they were declared
pub const ARMS: [Arm; 7] = [
    Arm::One,
    Arm::Barrier,
    Arm::Session,
    Arm::FanoutGet,
    Arm::FanoutFilter,
    Arm::FanoutLimit,
    Arm::FanoutEmpty,
];

impl Arm {
    /// The arm's identifier
    ///
    /// The join key every comparison uses; see `crate::workload_ids`.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Arm::One => "macro/cluster/reads/one",
            Arm::Barrier => "macro/cluster/reads/barrier",
            Arm::Session => "macro/cluster/reads/session",
            Arm::FanoutGet => "macro/cluster/fanout/get",
            Arm::FanoutFilter => "macro/cluster/fanout/filter",
            Arm::FanoutLimit => "macro/cluster/fanout/limit",
            Arm::FanoutEmpty => "macro/cluster/fanout/empty",
        }
    }

    /// One line saying what this arm isolates
    #[must_use]
    pub fn summary(self) -> &'static str {
        match self {
            Arm::One => "a One read of the local replica on the three node, factor three placement",
            Arm::Barrier => {
                "the same read at Quorum: a barrier from the group's leader, then the apply through it"
            }
            Arm::Session => {
                "the same read at One past the token of the write that seeded its tablet"
            }
            Arm::FanoutGet => {
                "a six key get split over three nodes at a factor of one, six rows back"
            }
            Arm::FanoutFilter => "the same get with a filter half the rows pass, three rows back",
            Arm::FanoutLimit => "the same get capped at three rows",
            Arm::FanoutEmpty => {
                "a six key get over keys never written: every share empty, every slot covered"
            }
        }
    }

    /// Whether this is one of the fanout arms
    #[must_use]
    pub fn is_fanout(self) -> bool {
        matches!(
            self,
            Arm::FanoutGet | Arm::FanoutFilter | Arm::FanoutLimit | Arm::FanoutEmpty
        )
    }

    /// The level every read is sent at
    #[must_use]
    pub fn level(self) -> ReadLevel {
        match self {
            Arm::Barrier => ReadLevel::Quorum,
            _ => ReadLevel::One,
        }
    }

    /// The factor the arm's placement replicates at
    #[must_use]
    pub fn replication_factor(self) -> u32 {
        if self.is_fanout() { 1 } else { 3 }
    }

    /// The fanout record the artifact carries, for a fanout arm
    #[must_use]
    pub fn fanout(self) -> Option<FanoutFacts> {
        if !self.is_fanout() {
            return None;
        }
        Some(FanoutFacts {
            nodes: u32::try_from(NODES).unwrap_or(u32::MAX),
            keys_per_query: FANOUT_KEYS,
            filtered: self == Arm::FanoutFilter,
            limit: (self == Arm::FanoutLimit).then_some(FANOUT_LIMIT),
            empty: self == Arm::FanoutEmpty,
        })
    }

    /// The read record the artifact carries
    #[must_use]
    pub fn read_arm(self) -> ReadArm {
        ReadArm {
            level: self.level().name().to_string(),
            session: self == Arm::Session,
            fanout: self.fanout(),
        }
    }

    /// The first `count` keys owned by each node of the fanout placement, node by node
    ///
    /// Walks the key space and deals each key to the node the ring puts its tablet on, through
    /// [`Ring::owner_of`] over the placement's shard counts, so a benchmark choosing keys cannot
    /// drift from the map. The lists are ascending and distinct, and `count` per node.
    ///
    /// # Arguments
    ///
    /// * `count` - How many keys to find on each node
    /// * `from` - The first key to try
    #[must_use]
    pub fn keys_per_node(count: usize, from: u64) -> Vec<Vec<u64>> {
        let shards = [NODE_SHARDS; NODES];
        let mut found: Vec<Vec<u64>> = vec![Vec::with_capacity(count); NODES];
        let mut candidate = from;
        while found.iter().any(|keys| keys.len() < count) {
            // the partition hash the table computes for this key, then the tablet and its owner
            let hash = Item::get_partition_key_from_values(&candidate);
            let (node, _) = Ring::owner_of(Ring::tablet_of(hash), &shards);
            if found[node].len() < count {
                found[node].push(candidate);
            }
            candidate += 1;
        }
        found
    }
}

/// One read arm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterReads {
    /// Which arm this is
    pub arm: Arm,
}

/// Every read arm
pub fn all() -> Vec<ClusterReads> {
    ARMS.into_iter().map(|arm| ClusterReads { arm }).collect()
}

impl ClusterReads {
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

    /// How many fanout reads a run sends: each names six keys, so a sixth of the grid's count
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn fanout_queries(scale: Scale) -> u64 {
        (queries_for(REFERENCE_WIDTH.mean(), scale) / u64::from(FANOUT_KEYS)).max(64)
    }

    /// The six keys a fanout read at an index names: two on each node, in node order
    ///
    /// # Arguments
    ///
    /// * `per_node` - The keys each node owns, as `keys_per_node` found them
    /// * `index` - The read's index
    fn fanout_keys(per_node: &[Vec<u64>], index: u64) -> Vec<u64> {
        let mut keys = Vec::with_capacity(FANOUT_KEYS as usize);
        for node in per_node {
            // two of this node's keys, walking its list so a run touches all of them
            let at = (index * 2) as usize % node.len();
            keys.push(node[at]);
            keys.push(node[(at + 1) % node.len()]);
        }
        keys
    }
}

impl Workload for ClusterReads {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.arm.id()
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        self.arm.summary()
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query at a time per slot, each stamped on its own: a sample is a round trip
        Timing::PerQuery
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        // the barrier is a wait, not process time; the stage layer is where it shows
        false
    }

    /// Whether the stage layer may run this workload
    fn stage_profiles(&self) -> bool {
        // no: the barrier and the apply wait are on the `cluster.reads` record of every
        // capture, summed and per node, and the stage report does not yet draw the two wait
        // stamps a read carries - that rendering is filed, and a stage run of these arms
        // before it lands would be three instrumented runs that show nothing new
        false
    }

    /// Whether this workload's reads are expected to find rows
    fn expects_rows(&self) -> bool {
        // the empty arm reads keys it never wrote: an empty answer is the measurement
        self.arm != Arm::FanoutEmpty
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let row_bytes = REFERENCE_WIDTH.mean();
        let rows = rows_for(row_bytes, scale);
        // the replication arms' placement, at the arm's factor, with the read record on it
        let mut conf: ConfOverrides = placement(self.arm.replication_factor());
        if let Some(cluster) = conf.cluster.as_mut() {
            cluster.read = Some(self.arm.read_arm());
        }
        WorkloadPlan {
            server: ServerNeed::Fresh(conf),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes,
                keys: rows,
                // the reference depth for the read arms; one outstanding for a fanout, which is
                // an added latency and not a throughput
                concurrency: if self.arm.is_fanout() { 1 } else { DEPTH },
                clients: None,
                read_pct: Some(100),
                row_profile: REFERENCE_WIDTH.artifact_name(),
                distribution: KeyDistribution::Uniform.artifact_name(),
                table_kind: Some(Table::Unsorted.artifact_name()),
            },
            // a twentieth of the run, as the grid does, so the first dial of every lane and the
            // first barrier of every group are behind the arm before it samples anything
            warmup: (queries_for(row_bytes, scale) / 20).max(10),
        }
    }

    /// Writes the rows every arm reads, without timing any of it
    ///
    /// The read arms seed the grid's rows; the fanout arms seed two keys a node, dealt over two
    /// buckets so the filtered arm has half to pass, and the empty arm seeds nothing it reads.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            let payloads = Payloads::build(REFERENCE_WIDTH, ctx.seed);
            let mut filters = Seeded::stream(ctx.seed, "grid/filters");
            let table = Table::Unsorted;
            let width = REFERENCE_WIDTH.mean();
            let batch = seed_batch(REFERENCE_WIDTH.widest(), frame_bytes(ctx));
            // the keys to seed: every row for a read arm, two a node for a fanout arm
            let keys: Vec<u64> = if self.arm.is_fanout() {
                Arm::keys_per_node(2, 0).into_iter().flatten().collect()
            } else {
                (0..ctx.scale.rows).collect()
            };
            let fanout = self.arm.is_fanout();
            let mut built = 0usize;
            let batches = move || {
                if built >= keys.len() {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..batch.min(keys.len() - built) {
                    // a fanout row's bucket alternates, so exactly half pass the filter
                    let bucket = if fanout {
                        built as u64 % FANOUT_BUCKETS
                    } else {
                        filters.below(16)
                    };
                    queries.add_mut(table.insert(
                        keys[built],
                        bucket,
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

    /// Reads the rows, keeping every round trip
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = Arc::new(ctx.client().await?);
            let scale = Self::scale_of(ctx);
            let table = Table::Unsorted;
            match self.arm {
                // the fanout arms: one six key get at a time, in one of four shapes
                Arm::FanoutGet | Arm::FanoutFilter | Arm::FanoutLimit | Arm::FanoutEmpty => {
                    let per_node = Arm::keys_per_node(
                        2,
                        if self.arm == Arm::FanoutEmpty {
                            EMPTY_FROM
                        } else {
                            0
                        },
                    );
                    let arm = self.arm;
                    let queries = Self::fanout_queries(scale);
                    // an empty answer is what the empty arm reads, so it is not a failure there
                    let success = QuerySuceededOpts {
                        get: arm != Arm::FanoutEmpty,
                        ..QuerySuceededOpts::default()
                    };
                    driver::drive_per_query_with(
                        client,
                        1,
                        queries,
                        ctx.warmup.min(queries / 4),
                        "get",
                        success,
                        move |index| {
                            let keys = Self::fanout_keys(&per_node, index);
                            let mut get = ItemGet::new(keys);
                            if arm == Arm::FanoutFilter {
                                get = get.filters(ItemFilter {
                                    bucket: Some(vec![FANOUT_BUCKET]),
                                    ..ItemFilter::default()
                                });
                            }
                            if arm == Arm::FanoutLimit {
                                get = get.limit(FANOUT_LIMIT as usize);
                            }
                            (get, SendOptions::default())
                        },
                    )
                    .await
                }
                // the read arms: one row at a time at the reference depth, each with its options
                Arm::One | Arm::Barrier | Arm::Session => {
                    let queries = queries_for(REFERENCE_WIDTH.mean(), scale);
                    let chooser =
                        Keys::new(KeyDistribution::Uniform, ctx.scale.rows, ctx.seed, "reads");
                    // the session arm carries, on every read, the token of the last write to
                    // the key's tablet: a write through node zero at the end of the seed, one
                    // per tablet the run reads, so every token names a committed lower bound
                    let tokens: Arc<Vec<Option<SessionToken>>> = if self.arm == Arm::Session {
                        Arc::new(session_tokens(&client, ctx.scale.rows, ctx.seed).await?)
                    } else {
                        Arc::new(Vec::new())
                    };
                    let level = self.arm.level();
                    driver::drive_per_query_with(
                        client,
                        DEPTH,
                        queries,
                        ctx.warmup,
                        "get",
                        QuerySuceededOpts::default(),
                        move |index| {
                            let key = chooser.at(index);
                            let mut options = SendOptions::new().read(level);
                            if let Some(Some(token)) = tokens.get(tablet_of(key)) {
                                options = options.token(*token);
                            }
                            (table.get(key), options)
                        },
                    )
                    .await
                }
            }
        })
    }
}

/// The tablet a key of the persistent unsorted table lands in
///
/// # Arguments
///
/// * `key` - The key
fn tablet_of(key: u64) -> usize {
    Ring::tablet_of(Item::get_partition_key_from_values(&key))
}

/// A token per tablet the run reads, from a write to one key of each
///
/// The seed's writes hand tokens back too, but through a stream the driver does not keep them
/// from; one more write per tablet, after the seed, is what the session arm reads past. Every
/// key written is one the run reads, rewritten with its own payload, so the rows are what they
/// were.
///
/// # Arguments
///
/// * `client` - The client to write through
/// * `rows` - How many rows the run has
/// * `seed` - The run's seed, for the payloads
async fn session_tokens(
    client: &shoal::Shoal<BenchClient>,
    rows: u64,
    seed: u64,
) -> Result<Vec<Option<SessionToken>>> {
    let payloads = Payloads::build(REFERENCE_WIDTH, seed);
    let width = REFERENCE_WIDTH.mean();
    let mut tokens: Vec<Option<SessionToken>> = vec![None; shoal::server::ring::TABLET_COUNT];
    for key in 0..rows {
        let tablet = tablet_of(key);
        if tokens[tablet].is_some() {
            continue;
        }
        let response = client
            .send_one(Table::Unsorted.insert(key, 0, payloads.at(width, key)))
            .await?;
        tokens[tablet] = response.session_token();
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::{ARMS, Arm, FANOUT_KEYS, all};
    use crate::workloads::cluster_replication::{NODE_SHARDS, NODES};
    use crate::workloads::workload::{ServerNeed, Workload};

    /// Three read arms on the factor three placement at every read, four fanout arms on the
    /// factor one placement, every arm feasible, and the ids in the order the registry appends
    #[test]
    fn the_read_arms_share_the_replication_placement() {
        let arms = all();
        assert_eq!(arms.len(), 7);
        for (arm, workload) in ARMS.iter().zip(&arms) {
            assert_eq!(workload.arm, *arm);
            let plan = workload.plan(crate::workloads::harness::seed::Scale::Full);
            let ServerNeed::Fresh(conf) = &plan.server else {
                panic!("{} starts no server", workload.id());
            };
            let cluster = conf.cluster.as_ref().expect("a cluster block");
            // the replication arms' placement: three nodes of three shards
            assert_eq!(conf.shards, Some(usize::from(NODE_SHARDS)));
            assert_eq!(cluster.peers, vec![NODE_SHARDS; NODES - 1]);
            assert_eq!(cluster.replication_factor, arm.replication_factor());
            cluster
                .feasibility()
                .unwrap_or_else(|error| panic!("{}: {error}", workload.id()));
            // every arm reads and nothing else, on the persistent unsorted table
            assert_eq!(plan.scale.read_pct, Some(100));
            assert_eq!(
                plan.scale.table_kind.as_deref(),
                Some("persistent_unsorted")
            );
            let read = cluster.read.as_ref().expect("a read record");
            assert_eq!(read.level, arm.level().name());
            assert_eq!(read.session, *arm == Arm::Session);
            assert_eq!(read.fanout.is_some(), arm.is_fanout());
        }
        // the read arms at a factor of three and the reference depth, the fanout arms at one
        for arm in [Arm::One, Arm::Barrier, Arm::Session] {
            assert_eq!(arm.replication_factor(), 3);
            assert_eq!(
                ClusterReads { arm }
                    .plan(crate::workloads::harness::seed::Scale::Full)
                    .scale
                    .concurrency,
                super::DEPTH
            );
        }
        for arm in [
            Arm::FanoutGet,
            Arm::FanoutFilter,
            Arm::FanoutLimit,
            Arm::FanoutEmpty,
        ] {
            assert_eq!(arm.replication_factor(), 1);
            let fanout = arm.fanout().expect("a fanout record");
            assert_eq!(fanout.keys_per_query, FANOUT_KEYS);
            assert_eq!(fanout.nodes, 3);
        }
        assert!(
            Arm::FanoutFilter
                .fanout()
                .is_some_and(|fanout| fanout.filtered)
        );
        assert_eq!(
            Arm::FanoutLimit.fanout().and_then(|fanout| fanout.limit),
            Some(3)
        );
        assert!(Arm::FanoutEmpty.fanout().is_some_and(|fanout| fanout.empty));
        // the ids are registered in declaration order
        let ids: Vec<&str> = arms.iter().map(|arm| arm.id()).collect();
        let registered: Vec<&str> = crate::workload_ids::IDS
            .iter()
            .copied()
            .filter(|id| {
                id.starts_with("macro/cluster/reads/") || id.starts_with("macro/cluster/fanout/")
            })
            .collect();
        assert_eq!(ids, registered);
    }

    /// The fanout keys are two a node, on the node the ring says, and a read names one of each
    /// node's pair in node order
    #[test]
    fn fanout_keys_are_two_per_node_in_node_order() {
        use shoal::server::ring::Ring;
        use shoal::shared::traits::PartitionKeySupport;
        let per_node = Arm::keys_per_node(2, 0);
        assert_eq!(per_node.len(), NODES);
        for (node, keys) in per_node.iter().enumerate() {
            assert_eq!(keys.len(), 2);
            for key in keys {
                let hash = super::Item::get_partition_key_from_values(key);
                let (owner, _) = Ring::owner_of(Ring::tablet_of(hash), &[NODE_SHARDS; NODES]);
                assert_eq!(owner, node, "key {key} is owned by node {owner}");
            }
        }
        let keys = super::ClusterReads::fanout_keys(&per_node, 0);
        assert_eq!(keys.len(), FANOUT_KEYS as usize);
        assert_eq!(&keys[..2], &per_node[0][..]);
    }

    use super::ClusterReads;
}
