//! `macro/fanout/{resident,evicted}/n` - one get over *n* partition keys, *n* varying
//!
//! # What this is for
//!
//! `docs/src/appendix/todos.md` asks for "a get over *n* partition keys, *n* varying, against both
//! a resident table and one whose partitions have to be read, so the O(n²) term is visible as a
//! curve rather than argued from the source". This is that, approached from above.
//!
//! That page also says the benchmark is blocked on driving a glommio `LocalExecutor` from inside
//! criterion's sampling loop. **That is true from below and false from above.** Driving
//! [`PersistentSortedTable::get`](shoal::PersistentSortedTable) over *n* keys through a live server
//! and a real client needs no executor inside criterion at all.
//!
//! # What it is not
//!
//! It is **not** the table-layer benchmark the todos asked for, and the difference matters. Every
//! sample here includes the wire, the routing, `split_by_shard`, the response merge and the client,
//! none of which the criterion benchmark would have included. So a number from this workload is not
//! a cost of `PersistentSortedTable::get`; it is a cost of a query that reaches one.
//!
//! What it *can* do is answer the question O13 poses. A quadratic term in the per-partition
//! bookkeeping shows up as a curve that bends against a flat control at *n* = 1, whatever constant
//! overhead sits on top of it. Today nothing answers that question at all. The criterion gap stays
//! open in `todos.md`.
//!
//! # The two arms
//!
//! `resident` finds every partition in memory; `evicted` finds none of them, because the server was
//! restarted after seeding. The pair brackets the cost: whatever the per-partition term is, the
//! evicted arm pays it with a disk read attached.
//!
//! # Read the median, not the tail, at the wide end
//!
//! [`Fanout::queries`] trades query count against key count so that every point on the curve reads
//! about the same number of partitions in total. That is what stops the widest point costing 256
//! times the narrowest, and it has a consequence worth stating: the widest points have the fewest
//! samples. At `n = 256` a full run takes 250 queries, so its p99 is roughly its third worst
//! sample and is not a percentile in any useful sense.
//!
//! **The curve is a curve in p50.** The tails are recorded because every workload records them,
//! and at the wide end they should be read as "the worst few of a few hundred" rather than as a
//! tail latency. Raising the floor would fix it and would cost the capture more than the answer is
//! worth; the alternative of holding query count fixed across the curve was rejected for the
//! reason above.

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{sort_key, BenchClient, Event, EventGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// The key counts the curve is measured at
///
/// Doubling would give a smoother curve and cost four times as long. These six span two and a half
/// orders of magnitude, which is enough for a quadratic term to separate from a linear one: over
/// this range a linear cost rises 256-fold and a quadratic one 65,536-fold.
pub const KEY_COUNTS: [u64; 6] = [1, 2, 4, 16, 64, 256];

/// How many partitions a full run seeds
///
/// Comfortably above the largest key count, so that even the widest query reads a small fraction of
/// the table and no query is answered by a partition another query just faulted in.
const PARTITIONS: u64 = 4_096;

/// How wide each row's payload is, in bytes
const ROW_BYTES: u64 = 256;

/// How many gets may be outstanding at once
///
/// Lower than the keyed get pair, because one query here touches up to 256 partitions and the
/// point is still to measure one query rather than a queue of them.
const CONCURRENCY: u32 = 8;

/// Which arm of the pair a workload is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Residency {
    /// Every partition is in memory when the get arrives
    Resident,
    /// No partition is in memory, so every one of them has to be read
    Evicted,
}

impl Residency {
    /// The name this arm appears under in an identifier
    fn as_str(&self) -> &'static str {
        match self {
            Residency::Resident => "resident",
            Residency::Evicted => "evicted",
        }
    }
}

/// One get over a fixed number of partition keys
pub struct Fanout {
    /// Which arm this is
    pub residency: Residency,
    /// How many partition keys each get names
    pub keys: u64,
    /// This workload's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
}

impl Fanout {
    /// Every fanout workload, both arms across every key count
    ///
    /// The identifiers are leaked deliberately. They are twelve short strings built once at
    /// startup and read for the life of the process, and leaking them is what lets the parameter
    /// live in the struct rather than forcing twelve hand written types.
    pub fn all() -> Vec<Fanout> {
        let mut built = Vec::with_capacity(KEY_COUNTS.len() * 2);
        // arm-outer so the two arms of one key count are not adjacent, which matches the order
        // `workload_ids::IDS` declares and therefore the order a capture runs them in
        for residency in [Residency::Resident, Residency::Evicted] {
            for keys in KEY_COUNTS {
                let id: &'static str =
                    Box::leak(format!("macro/fanout/{}/{keys}", residency.as_str()).into_boxed_str());
                built.push(Fanout {
                    residency,
                    keys,
                    id,
                });
            }
        }
        built
    }

    /// How many partitions this workload seeds at a scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn partitions(&self, scale: Scale) -> u64 {
        // never fewer partitions than one query names, or a get would repeat a key
        scale.rows(PARTITIONS).max(self.keys * 4)
    }

    /// How many gets this workload measures at a scale
    ///
    /// Falls as the key count rises, so that every point on the curve reads roughly the same
    /// number of partitions in total. Without that the widest point would take 256 times as long
    /// as the narrowest, and the curve would cost more than the rest of the capture.
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn queries(&self, scale: Scale) -> u64 {
        // a fixed budget of partition reads, floored so the widest point still has enough samples
        // for a percentile and capped so the narrowest does not run all day
        let budget = (32_768 / self.keys).clamp(250, 8_000);
        match scale {
            Scale::Smoke => (budget / 10).max(50),
            Scale::Full => budget,
        }
    }
}

impl Workload for Fanout {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        match self.residency {
            Residency::Resident => "one get over n resident partitions, n varying",
            Residency::Evicted => "one get over n partitions that all have to be read, n varying",
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own. a curve built from batch completion times
        // would bend with the batching rather than with n.
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // twelve workloads under two attribution layers would be twenty four instrumented runs
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let partitions = self.partitions(scale);
        // one shard, so partition placement is fixed and the curve is not also a curve in how the
        // keys happened to hash across shards. `shoal/tests/utils.rs::build_single_shard_config`
        // does the same thing for the same reason.
        let overrides = ConfOverrides {
            shards: Some(1),
            memory: None,
            tls: false,
        };
        let server = match self.residency {
            Residency::Resident => ServerNeed::Fresh(overrides),
            Residency::Evicted => ServerNeed::RestartAfterSeed(overrides),
        };
        WorkloadPlan {
            server,
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows: partitions,
                row_bytes: ROW_BYTES,
                // one row per partition, so a get over n keys returns exactly n rows and the curve
                // is a curve in the partition count rather than in the result size
                keys: partitions,
                concurrency: CONCURRENCY,
                clients: None,
                // not a mixture, a width distribution or a skewed access pattern
                ..ScaleFacts::default()
            },
            warmup: (self.queries(scale) / 20).max(10),
        }
    }

    /// Writes one row into each partition, without timing any of it
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = shoal::Shoal::<BenchClient>::new(&ctx.addr).await?;
            let mut kinds = Seeded::stream(ctx.seed, "fanout/kinds");
            let mut payloads = Seeded::stream(ctx.seed, "fanout/payloads");
            let total = ctx.scale.rows;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    queries.add_mut(Event {
                        stream: built,
                        // every partition holds its single row at the same sort key, so a get can
                        // name that one key and come back with exactly one row per partition
                        at: sort_key(0),
                        kind: kinds.below(16),
                        payload: payloads.string(ROW_BYTES as usize),
                    });
                    built += 1;
                }
                Some(Batch { queries })
            };
            driver::drive(&client, batches, "seed", 0).await?;
            Ok(())
        })
    }

    /// Reads n partitions per get, timing each get
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = std::sync::Arc::new(shoal::Shoal::<BenchClient>::new(&ctx.addr).await?);
            let partitions = ctx.scale.rows;
            let keys = self.keys;
            let stride = crate::workloads::keyed_get::stride_for(partitions);
            let scale = if ctx.scale.scale == "smoke" {
                Scale::Smoke
            } else {
                Scale::Full
            };
            driver::drive_per_query(
                client,
                ctx.scale.concurrency,
                self.queries(scale),
                ctx.warmup,
                "get",
                move |index| {
                    // n distinct keys, walked with the same coprime stride the keyed get uses, so
                    // no two keys in one query repeat and no query reads the archive in its own
                    // layout order
                    let named: Vec<u64> = (0..keys)
                        .map(|offset| {
                            (index.wrapping_mul(keys).wrapping_add(offset))
                                .wrapping_mul(stride)
                                % partitions
                        })
                        .collect();
                    // one sort key, so the result is one row per partition and the query measures
                    // the fanout rather than the size of what came back
                    EventGet::new(named).sort_keys(vec![sort_key(0)])
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Fanout, Residency, KEY_COUNTS};
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// Both arms are minted at every key count, and nothing is minted twice
    #[test]
    fn every_arm_and_key_count_is_minted() {
        let all = Fanout::all();
        assert_eq!(all.len(), KEY_COUNTS.len() * 2);
        let mut ids: Vec<&str> = all.iter().map(|f| f.id()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "a fanout id is minted twice");
    }

    /// An identifier names its arm and its key count, which is what makes the curve readable
    #[test]
    fn an_id_names_its_arm_and_key_count() {
        for workload in Fanout::all() {
            let expected = format!(
                "macro/fanout/{}/{}",
                workload.residency.as_str(),
                workload.keys
            );
            assert_eq!(workload.id(), expected);
        }
    }

    /// The two arms of one key count differ in residency and in nothing else
    #[test]
    fn the_arms_of_one_key_count_differ_only_in_residency() {
        let all = Fanout::all();
        for keys in KEY_COUNTS {
            let resident = all
                .iter()
                .find(|f| f.residency == Residency::Resident && f.keys == keys)
                .expect("a resident arm");
            let evicted = all
                .iter()
                .find(|f| f.residency == Residency::Evicted && f.keys == keys)
                .expect("an evicted arm");
            let left = resident.plan(Scale::Full);
            let right = evicted.plan(Scale::Full);
            assert_eq!(left.scale, right.scale);
            assert_eq!(left.warmup, right.warmup);
            assert_eq!(left.server.overrides(), right.server.overrides());
            // the one axis
            assert!(!left.server.restarts());
            assert!(right.server.restarts());
        }
    }

    /// Every point on the curve reads roughly the same number of partitions in total
    ///
    /// Without this the widest point would take 256 times as long as the narrowest, and the curve
    /// would cost more than every other workload put together.
    #[test]
    fn the_curve_costs_roughly_the_same_at_every_point() {
        let all = Fanout::all();
        let reads: Vec<u64> = KEY_COUNTS
            .iter()
            .map(|keys| {
                let workload = all
                    .iter()
                    .find(|f| f.residency == Residency::Resident && f.keys == *keys)
                    .expect("an arm");
                workload.queries(Scale::Full) * keys
            })
            .collect();
        // the clamps at both ends mean this is not exact - the narrow points are capped on query
        // count and the widest is floored on sample count - but no point may cost an order of
        // magnitude more than another
        let smallest = *reads.iter().min().expect("points");
        let largest = *reads.iter().max().expect("points");
        assert!(
            largest <= smallest * 10,
            "the curve is lopsided: {reads:?} partition reads per point"
        );
    }

    /// Every query names distinct keys, so n really is the partition count
    ///
    /// A repeated key would be deduplicated at the server, and the widest points of the curve
    /// would quietly measure fewer partitions than they claim.
    #[test]
    fn a_query_names_distinct_partitions() {
        let partitions = 4_096u64;
        let stride = crate::workloads::keyed_get::stride_for(partitions);
        for keys in KEY_COUNTS {
            for index in 0..32u64 {
                let named: Vec<u64> = (0..keys)
                    .map(|offset| {
                        (index.wrapping_mul(keys).wrapping_add(offset)).wrapping_mul(stride)
                            % partitions
                    })
                    .collect();
                let mut sorted = named.clone();
                sorted.sort_unstable();
                sorted.dedup();
                assert_eq!(
                    sorted.len(),
                    named.len(),
                    "a get over {keys} keys repeated one at index {index}"
                );
            }
        }
    }

    /// The table is always much larger than the widest query
    ///
    /// Otherwise the widest point would read most of the table on every query, and the evicted
    /// arm would be measuring a warm page cache rather than a cold read.
    #[test]
    fn the_table_dwarfs_the_widest_query() {
        for scale in [Scale::Smoke, Scale::Full] {
            for workload in Fanout::all() {
                let partitions = workload.partitions(scale);
                assert!(
                    partitions >= workload.keys * 4,
                    "{} reads {} of {partitions} partitions per query",
                    workload.id(),
                    workload.keys
                );
            }
        }
    }

    /// The curve runs on one shard, so it is not also a curve in how keys hashed
    #[test]
    fn the_curve_pins_its_shard_count() {
        for workload in Fanout::all() {
            let plan = workload.plan(Scale::Full);
            assert_eq!(
                plan.server.overrides().and_then(|c| c.shards),
                Some(1),
                "{} does not pin its shard count",
                workload.id()
            );
        }
    }

    /// Every point reports a service time
    #[test]
    fn every_point_is_per_query() {
        for workload in Fanout::all() {
            assert_eq!(workload.timing(), Timing::PerQuery);
        }
    }
}
