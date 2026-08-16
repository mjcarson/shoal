//! `macro/insert_unsorted` - the write path, saturated
//!
//! # What it isolates
//!
//! One insert per partition into a [`PersistentUnsortedTable`](shoal::PersistentUnsortedTable),
//! which is the shortest path from a client to durable bytes: route the key, append the row to the
//! intent log, wait on the durability barrier, answer. Nothing here reads, so nothing here can be
//! attributed to the read path, which was the whole problem with measuring inserts and gets
//! together and reporting one number.
//!
//! Every other workload seeds itself through this path, so it is also the control for the rest: if
//! this moves, a read workload built on top of it moved for a reason that is not about reading.
//!
//! # How to read its numbers
//!
//! As a **wall clock**, and as the throughput that implies. It runs saturated with one timestamp
//! per batch, so its percentiles describe batch completion under load and not the service time of
//! an insert. See [`crate::workloads::harness::driver`].

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, Item};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many rows a full run inserts
///
/// Chosen so a run is long enough that its wall clock is dominated by steady state rather than by
/// starting up, and short enough that five runs of it are minutes rather than an evening.
const ROWS: u64 = 200_000;

/// How wide each row's payload is, in bytes
///
/// Held fixed across scales, so a smoke run and a full run differ in how many rows they move and
/// in nothing else.
const ROW_BYTES: u64 = 256;

/// Inserts one row per partition into an unsorted table, saturated
pub struct InsertUnsorted;

impl Workload for InsertUnsorted {
    /// What this workload is called
    fn id(&self) -> &'static str {
        "macro/insert_unsorted"
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        "one insert per partition into an unsorted table, saturated"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // saturated, so a sample is a batch completion and the wall clock is the number
        Timing::PerBatch
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // the write path is where the profile has always been most of the time, so this is the
        // workload worth attributing
        true
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        // every row is its own partition, so the key count is the row count
        let rows = scale.rows(ROWS);
        WorkloadPlan {
            // the base configuration, unchanged: this workload is the one everything else is
            // compared against, so it should not pin anything the others do not
            server: ServerNeed::Fresh(ConfOverrides::default()),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes: ROW_BYTES,
                keys: rows,
                concurrency: driver::IN_FLIGHT as u32,
                clients: None,
                // not a mixture, a width distribution or a skewed access pattern
                ..ScaleFacts::default()
            },
            // enough to cover connection establishment and the first log rotation
            warmup: (rows / 40).min(5_000),
        }
    }

    /// Inserts every row and times the batches it takes to do it
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            // connect to the server the harness started and waited for
            let client = shoal::Shoal::<BenchClient>::new(&ctx.addr).await?;
            // one stream of rows, drawn from named streams off the run's seed so that adding a
            // draw to one does not silently change the other
            let mut keys = Seeded::stream(ctx.seed, "insert_unsorted/keys");
            let mut payloads = Seeded::stream(ctx.seed, "insert_unsorted/payloads");
            let total = ctx.scale.rows;
            let mut built = 0u64;
            // hand the driver one batch at a time, built as it asks for them rather than all up
            // front, so the whole dataset is never resident in the client
            let batches = move || {
                // stop once every row has been handed over
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    // a distinct partition per row, so no two rows share one
                    let id = built;
                    queries.add_mut(Item {
                        id,
                        // sixteen buckets, so a filter over one is selective without being empty
                        bucket: keys.below(16),
                        label: payloads.string(16),
                        payload: payloads.string(ROW_BYTES as usize),
                    });
                    built += 1;
                }
                Some(Batch { queries })
            };
            driver::drive(&client, batches, "insert", ctx.warmup).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{InsertUnsorted, ROWS, ROW_BYTES};
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// Every row is its own partition, which is what makes this the unsorted table's path
    #[test]
    fn every_row_is_its_own_partition() {
        let plan = InsertUnsorted.plan(Scale::Full);
        assert_eq!(plan.scale.keys, plan.scale.rows);
        assert_eq!(plan.scale.rows, ROWS);
    }

    /// Row width does not change with scale, so the two differ in row count and nothing else
    #[test]
    fn row_width_is_held_across_scales() {
        assert_eq!(InsertUnsorted.plan(Scale::Full).scale.row_bytes, ROW_BYTES);
        assert_eq!(InsertUnsorted.plan(Scale::Smoke).scale.row_bytes, ROW_BYTES);
        // and a smoke run is genuinely smaller
        assert!(InsertUnsorted.plan(Scale::Smoke).scale.rows < ROWS);
    }

    /// The warmup never swallows the whole run, however small the scale
    ///
    /// A warmup at or above the row count would discard every sample and produce a capture that
    /// measured nothing, which reads exactly like one from a workload that was fast.
    #[test]
    fn the_warmup_leaves_something_to_measure() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = InsertUnsorted.plan(scale);
            assert!(
                plan.warmup < plan.scale.rows,
                "{scale:?} warms up {} of {} rows",
                plan.warmup,
                plan.scale.rows
            );
        }
    }

    /// This workload reports a batch time, and says so
    #[test]
    fn it_declares_itself_per_batch() {
        assert_eq!(InsertUnsorted.timing(), Timing::PerBatch);
    }
}
