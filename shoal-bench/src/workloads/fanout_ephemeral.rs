//! `macro/fanout/ephemeral/n` - one get over *n* partition keys, with the storage layer taken out
//!
//! # What this is for
//!
//! [`macro/fanout/resident/n`](crate::workloads::fanout) answers whether the per partition
//! bookkeeping in a get carries a quadratic term, by watching a curve bend as *n* rises. It
//! answers it while every partition happens to be resident but the table still sits on a storage
//! engine, so a term that came from the engine and a term that came from the get itself land in
//! the same curve.
//!
//! This is the third arm. Same *n*, same shape of data, same single shard, same driver — an
//! ephemeral table. Whatever the resident curve does that this one does not is the storage
//! engine's share of the fanout.
//!
//! # The arms it belongs to
//!
//! - `resident` — every partition in memory, on a persistent table
//! - `evicted` — no partition in memory, so every one is read off disk
//! - `ephemeral` — every partition in memory, with no engine underneath at all
//!
//! There is no evicted counterpart here for the same reason `macro/get_ephemeral` has no archived
//! arm: an ephemeral table restarted is an empty one.
//!
//! # Read the median, not the tail, at the wide end
//!
//! Everything [`crate::workloads::fanout`] says about this applies unchanged, because the query
//! budget is computed the same way. At `n = 256` a full run takes 250 queries, so its p99 is
//! roughly its third worst sample. **The curve is a curve in p50.**

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::fanout::KEY_COUNTS;
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::keyed_get::stride_for;
use crate::workloads::schema::{BenchClient, MemEvent, MemEventGet, sort_key};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many partitions a full run seeds
///
/// The same count the persistent curve uses, so the two curves are read over the same table size.
const PARTITIONS: u64 = 4_096;

/// How wide each row's payload is, in bytes
const ROW_BYTES: u64 = 256;

/// How many gets may be outstanding at once
const CONCURRENCY: u32 = 8;

/// One get over a fixed number of partition keys, against an ephemeral table
pub struct FanoutEphemeral {
    /// How many partition keys each get names
    pub keys: u64,
    /// This workload's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
}

impl FanoutEphemeral {
    /// Every ephemeral fanout workload, one per key count
    ///
    /// The identifiers are leaked for the same reason [`crate::workloads::fanout::Fanout::all`]
    /// leaks its own: they are a handful of short strings built once at startup and read for the
    /// life of the process, and leaking them is what lets the key count live in the struct rather
    /// than forcing six hand written types.
    pub fn all() -> Vec<FanoutEphemeral> {
        let mut built = Vec::with_capacity(KEY_COUNTS.len());
        // the same key counts the persistent curve is measured at, so the two lay over each other
        for keys in KEY_COUNTS {
            let id: &'static str =
                Box::leak(format!("macro/fanout/ephemeral/{keys}").into_boxed_str());
            built.push(FanoutEphemeral { keys, id });
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
    /// Computed exactly as the persistent curve computes it, so the two curves have the same
    /// number of samples at every point and can be laid over one another.
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

impl Workload for FanoutEphemeral {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        "one get over n partitions of an ephemeral table, n varying"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own. a curve built from batch completion times
        // would bend with the batching rather than with n.
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // six more workloads under two attribution layers would be twelve instrumented runs
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
        // keys happened to hash across shards. the persistent curve pins this for the same reason,
        // and the two would not be comparable if only one of them did
        let overrides = ConfOverrides {
            shards: Some(1),
            memory: None,
            tls: false,
        };
        WorkloadPlan {
            // fresh, and only ever fresh. there is no evicted arm to reach by restarting, since a
            // restart empties this table rather than pushing it to disk
            server: ServerNeed::Fresh(overrides),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows: partitions,
                row_bytes: ROW_BYTES,
                // one row per partition, so a get over n keys returns exactly n rows and the curve
                // is a curve in the partition count rather than in the result size
                keys: partitions,
                concurrency: CONCURRENCY,
                clients: None,
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
            let mut kinds = Seeded::stream(ctx.seed, "fanout_ephemeral/kinds");
            let mut payloads = Seeded::stream(ctx.seed, "fanout_ephemeral/payloads");
            let total = ctx.scale.rows;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    queries.add_mut(MemEvent {
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
            let stride = stride_for(partitions);
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
                    // n distinct keys, walked with the same coprime stride the persistent curve
                    // uses, so the two read their key spaces in the same order
                    let named: Vec<u64> = (0..keys)
                        .map(|offset| {
                            (index.wrapping_mul(keys).wrapping_add(offset)).wrapping_mul(stride)
                                % partitions
                        })
                        .collect();
                    // one sort key, so the result is one row per partition and the query measures
                    // the fanout rather than the size of what came back
                    MemEventGet::new(named).sort_keys(vec![sort_key(0)])
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FanoutEphemeral, KEY_COUNTS};
    use crate::model::macro_layer::Timing;
    use crate::workloads::fanout::{Fanout, Residency};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// One workload is minted per key count, and nothing is minted twice
    #[test]
    fn every_key_count_is_minted_once() {
        let all = FanoutEphemeral::all();
        assert_eq!(all.len(), KEY_COUNTS.len());
        let mut ids: Vec<&str> = all.iter().map(|workload| workload.id()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), KEY_COUNTS.len());
    }

    /// This curve and the resident one differ in nothing a plan can express
    ///
    /// Every point of one is meant to be readable against the same point of the other. A
    /// difference in partition count, row width, shard count or sample count would bend the two
    /// curves relative to each other for a reason that is not the storage engine.
    #[test]
    fn the_curves_differ_only_in_the_table_they_drive() {
        let persistent = Fanout::all();
        for ephemeral in FanoutEphemeral::all() {
            // find the resident arm at this key count
            let control = persistent
                .iter()
                .find(|arm| arm.residency == Residency::Resident && arm.keys == ephemeral.keys)
                .expect("no resident arm at this key count");
            for scale in [Scale::Smoke, Scale::Full] {
                let ours = ephemeral.plan(scale);
                let theirs = control.plan(scale);
                assert_eq!(ours.scale, theirs.scale, "{} scale differs", ephemeral.id());
                assert_eq!(
                    ours.warmup,
                    theirs.warmup,
                    "{} warmup differs",
                    ephemeral.id()
                );
                assert_eq!(
                    ours.server,
                    theirs.server,
                    "{} server differs",
                    ephemeral.id()
                );
            }
        }
    }

    /// Every point on the curve names fewer keys than there are partitions to name
    ///
    /// A query naming more keys than the table has partitions would repeat one, and the widest
    /// points of the curve would measure a partition already in the answer.
    #[test]
    fn a_query_never_repeats_a_key() {
        for workload in FanoutEphemeral::all() {
            for scale in [Scale::Smoke, Scale::Full] {
                let plan = workload.plan(scale);
                assert!(
                    workload.keys < plan.scale.keys,
                    "{} names {} of {} partitions at {scale:?}",
                    workload.id(),
                    workload.keys,
                    plan.scale.keys
                );
            }
        }
    }

    /// The server is never cycled, because cycling it would empty the table rather than flush it
    #[test]
    fn the_server_is_never_restarted() {
        for workload in FanoutEphemeral::all() {
            assert!(!workload.plan(Scale::Full).server.restarts());
        }
    }

    /// Every point on this curve reports a service time, and says so
    #[test]
    fn the_whole_curve_is_per_query() {
        for workload in FanoutEphemeral::all() {
            assert_eq!(workload.timing(), Timing::PerQuery);
        }
    }

    /// The warmup never swallows the whole run, however small the scale
    #[test]
    fn the_warmup_leaves_something_to_measure() {
        for workload in FanoutEphemeral::all() {
            for scale in [Scale::Smoke, Scale::Full] {
                let plan = workload.plan(scale);
                assert!(
                    plan.warmup < workload.queries(scale),
                    "{} warms up {} of {} queries at {scale:?}",
                    workload.id(),
                    plan.warmup,
                    workload.queries(scale)
                );
            }
        }
    }
}
