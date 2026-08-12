//! `macro/get_ephemeral` - the keyed read path with the storage layer taken out
//!
//! # What it isolates
//!
//! The same thing [`macro/get_resident`](crate::workloads::keyed_get) isolates, minus storage.
//! One keyed get against an [`EphemeralUnsortedTable`](shoal::EphemeralUnsortedTable), which is
//! the same table `get_resident` drives with a storage engine that never opens a file underneath
//! it.
//!
//! # There is no archived arm, and there cannot be one
//!
//! `get_resident` has a twin, `get_archived`, reached by restarting the server after seeding so
//! that every partition has to be read back off disk. Restart an ephemeral table and the rows are
//! simply gone, so the only arm this workload can have is the resident one. That is not a gap in
//! the coverage; it is the property being measured.
//!
//! # How to read its numbers
//!
//! **Against `macro/get_resident`.** Both find every partition already in memory, so the gap
//! between them is what a read still pays for living above a storage engine: the `MaybeLoaded`
//! wrapper, the `check_disk` probe that asks the engine whether a partition it does not hold
//! might be on disk, and the pending-get bookkeeping that exists so a blocked read can be
//! replayed. **The gap here should be small.** If it is large, the read path is paying for
//! durability it does not use, which is a finding rather than an expected result.
//!
//! As a **service time**, one timestamp per query, exactly like the workload it mirrors.

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::keyed_get::stride_for;
use crate::workloads::schema::{BenchClient, MemItem, MemItemGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many rows a full run seeds, one per partition
///
/// The same count `keyed_get` uses, so the pair reads over the same key space.
const ROWS: u64 = 200_000;

/// How many gets a full run measures
const QUERIES: u64 = 50_000;

/// How wide each row's payload is, in bytes
const ROW_BYTES: u64 = 256;

/// How many gets may be outstanding at once
///
/// Low on purpose, and the same as the workload this mirrors. Every slot holds exactly one query,
/// so the time from send to response is the time that query took.
const CONCURRENCY: u32 = 16;

/// A keyed get against an ephemeral table, which is always resident
pub struct GetEphemeral;

impl GetEphemeral {
    /// How many rows this workload seeds at a scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn rows(scale: Scale) -> u64 {
        scale.rows(ROWS)
    }

    /// How many gets this workload measures at a scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn queries(scale: Scale) -> u64 {
        // never more queries than there are rows to read, so every get finds something
        scale.rows(QUERIES).min(Self::rows(scale))
    }
}

impl Workload for GetEphemeral {
    /// What this workload is called
    fn id(&self) -> &'static str {
        "macro/get_ephemeral"
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        "one keyed get against a partition in an ephemeral table"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own, which makes a sample a service time
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // the read path's profile is already covered by the workload this mirrors
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let rows = Self::rows(scale);
        WorkloadPlan {
            // fresh, and only ever fresh. a restart would empty the table rather than push it to
            // disk, so the arm `get_resident` has a twin for does not exist here
            server: ServerNeed::Fresh(ConfOverrides::default()),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes: ROW_BYTES,
                // one row per partition, so the key count is the row count
                keys: rows,
                concurrency: CONCURRENCY,
            },
            // enough to cover connection establishment before sampling starts
            warmup: (Self::queries(scale) / 20).min(2_000),
        }
    }

    /// Writes the rows this workload will read, without timing any of it
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = shoal::Shoal::<BenchClient>::new(&ctx.addr).await?;
            // named streams of its own, so a draw added to one workload cannot change another's
            let mut buckets = Seeded::stream(ctx.seed, "get_ephemeral/buckets");
            let mut payloads = Seeded::stream(ctx.seed, "get_ephemeral/payloads");
            let total = ctx.scale.rows;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    queries.add_mut(MemItem {
                        id: built,
                        bucket: buckets.below(16),
                        label: payloads.string(16),
                        payload: payloads.string(ROW_BYTES as usize),
                    });
                    built += 1;
                }
                Some(Batch { queries })
            };
            // saturated, because seeding is not the measurement and should be over quickly. no
            // warmup either, since nothing here is sampled.
            driver::drive(&client, batches, "seed", 0).await?;
            Ok(())
        })
    }

    /// Reads rows back one query at a time, timing each one
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = std::sync::Arc::new(shoal::Shoal::<BenchClient>::new(&ctx.addr).await?);
            let rows = ctx.scale.rows;
            // the same coprime walk the workload this mirrors uses. Nothing here is on disk to be
            // helped by a sequential order, but the two have to read their key spaces the same way
            // or the comparison would also be a comparison of access patterns
            let stride = stride_for(rows);
            let scale = if ctx.scale.scale == "smoke" {
                Scale::Smoke
            } else {
                Scale::Full
            };
            driver::drive_per_query(
                client,
                ctx.scale.concurrency,
                Self::queries(scale).min(rows),
                ctx.warmup,
                "get",
                move |index| {
                    // a multiplicative walk that visits every key exactly once before repeating,
                    // because `stride` is coprime with the key count
                    let key = index.wrapping_mul(stride) % rows;
                    MemItemGet::new(vec![key])
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{GetEphemeral, ROW_BYTES, ROWS};
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::keyed_get::{KeyedGet, Residency};
    use crate::workloads::workload::{ServerNeed, Workload};

    /// This workload and the resident arm it is a control for differ in nothing a plan can express
    ///
    /// The gap between the two is meant to be what a read pays for living above a storage engine.
    /// Any other difference between their plans would land in that gap.
    #[test]
    fn the_pair_differs_only_in_the_table_it_drives() {
        let control = KeyedGet {
            residency: Residency::Resident,
        };
        for scale in [Scale::Smoke, Scale::Full] {
            let ephemeral = GetEphemeral.plan(scale);
            let resident = control.plan(scale);
            assert_eq!(ephemeral.scale, resident.scale, "{scale:?} scale differs");
            assert_eq!(
                ephemeral.warmup, resident.warmup,
                "{scale:?} warmup differs"
            );
            assert_eq!(
                ephemeral.server, resident.server,
                "{scale:?} server differs"
            );
        }
        // and they are sampled the same way, or their numbers would not be comparable at all
        assert_eq!(GetEphemeral.timing(), control.timing());
    }

    /// The server is never cycled, because cycling it would empty the table rather than flush it
    #[test]
    fn the_server_is_never_restarted() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = GetEphemeral.plan(scale);
            assert!(!plan.server.restarts());
            assert!(matches!(plan.server, ServerNeed::Fresh(_)));
        }
    }

    /// This workload reports a service time, and says so
    #[test]
    fn it_declares_itself_per_query() {
        assert_eq!(GetEphemeral.timing(), Timing::PerQuery);
    }

    /// The shape of the data does not change with scale
    #[test]
    fn the_shape_of_the_data_is_fixed() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = GetEphemeral.plan(scale);
            // one row per partition, so a get names one key and comes back with one row
            assert_eq!(plan.scale.keys, plan.scale.rows);
            assert_eq!(plan.scale.row_bytes, ROW_BYTES);
        }
        assert_eq!(GetEphemeral.plan(Scale::Full).scale.rows, ROWS);
        // and a smoke run is genuinely smaller
        assert!(GetEphemeral.plan(Scale::Smoke).scale.rows < ROWS);
    }

    /// The warmup never swallows every query, however small the scale
    #[test]
    fn the_warmup_leaves_something_to_measure() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = GetEphemeral.plan(scale);
            assert!(
                plan.warmup < GetEphemeral::queries(scale),
                "{scale:?} warms up {} of {} queries",
                plan.warmup,
                GetEphemeral::queries(scale)
            );
        }
    }
}
