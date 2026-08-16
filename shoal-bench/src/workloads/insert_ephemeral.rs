//! `macro/insert_ephemeral` - the write path with the storage layer taken out
//!
//! # What it isolates
//!
//! The same thing [`macro/insert_unsorted`](crate::workloads::insert_unsorted) isolates, minus
//! storage. One insert per partition into an
//! [`EphemeralUnsortedTable`](shoal::EphemeralUnsortedTable), which is the same table
//! `insert_unsorted` drives with a storage engine that never opens a file underneath it.
//!
//! # How to read its numbers
//!
//! **Against `macro/insert_unsorted`, and almost never on its own.** Every constant here is
//! copied from that workload — the same row count, the same row width, the same saturated driver,
//! the same warmup — so the two are the same measurement with one variable changed. The gap
//! between them is the cost of durability: the intent log write, the durability barrier, and the
//! compaction that follows.
//!
//! It is not the cost of *all* storage related work. An ephemeral insert is still wrapped in an
//! intent, still parked, and still released on a shard sweep rather than answered inline, because
//! those belong to the table rather than to the engine. See
//! `docs/src/features/ephemeral-tables.md`.
//!
//! As a **wall clock**, and as the throughput that implies, exactly like the workload it mirrors.

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, MemItem};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many rows a full run inserts
///
/// The same count `insert_unsorted` uses. Holding it fixed is what makes the pair comparable; a
/// faster workload given more rows to insert would report a gap that was partly its own size.
const ROWS: u64 = 200_000;

/// How wide each row's payload is, in bytes
const ROW_BYTES: u64 = 256;

/// Inserts one row per partition into an ephemeral unsorted table, saturated
pub struct InsertEphemeral;

impl Workload for InsertEphemeral {
    /// What this workload is called
    fn id(&self) -> &'static str {
        "macro/insert_ephemeral"
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        "one insert per partition into an ephemeral unsorted table, saturated"
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // saturated, so a sample is a batch completion and the wall clock is the number
        Timing::PerBatch
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // an instrumented run of this would attribute the storage layer directly, by subtracting
        // this profile from `insert_unsorted`'s. It is off because opting in doubles the two most
        // expensive phases of a capture, and the wall clock gap already answers the question this
        // workload was built to ask. Filed in `docs/src/appendix/todos.md`.
        false
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
            // the base configuration, unchanged, because the workload this is a control for
            // pins nothing either
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
            // the same warmup as the workload this mirrors, even though there is no log rotation
            // here for it to cover, so the two discard the same share of their run
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
            // named streams of its own rather than `insert_unsorted`'s, so that adding a draw to
            // one workload cannot silently change the rows the other builds
            let mut keys = Seeded::stream(ctx.seed, "insert_ephemeral/keys");
            let mut payloads = Seeded::stream(ctx.seed, "insert_ephemeral/payloads");
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
                    queries.add_mut(MemItem {
                        id,
                        // sixteen buckets, matching the workload this mirrors
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
    use super::{InsertEphemeral, ROW_BYTES, ROWS};
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::insert_unsorted::InsertUnsorted;
    use crate::workloads::workload::Workload;

    /// This workload and the one it is a control for differ in nothing a plan can express
    ///
    /// The gap between the two is meant to be the storage layer. Any other difference between
    /// their plans - row count, row width, saturation, warmup, or what they ask of the server -
    /// would land in that gap and be read as storage.
    #[test]
    fn the_pair_differs_only_in_the_table_it_drives() {
        for scale in [Scale::Smoke, Scale::Full] {
            let ephemeral = InsertEphemeral.plan(scale);
            let persistent = InsertUnsorted.plan(scale);
            assert_eq!(ephemeral.scale, persistent.scale, "{scale:?} scale differs");
            assert_eq!(
                ephemeral.warmup, persistent.warmup,
                "{scale:?} warmup differs"
            );
            assert_eq!(
                ephemeral.server, persistent.server,
                "{scale:?} server differs"
            );
        }
        // and they are sampled the same way, or their numbers would not be comparable at all
        assert_eq!(InsertEphemeral.timing(), InsertUnsorted.timing());
    }

    /// Every row is its own partition, which is what makes this the unsorted table's path
    #[test]
    fn every_row_is_its_own_partition() {
        let plan = InsertEphemeral.plan(Scale::Full);
        assert_eq!(plan.scale.keys, plan.scale.rows);
        assert_eq!(plan.scale.rows, ROWS);
    }

    /// Row width does not change with scale, so the two scales differ in row count and nothing else
    #[test]
    fn row_width_is_held_across_scales() {
        assert_eq!(InsertEphemeral.plan(Scale::Full).scale.row_bytes, ROW_BYTES);
        assert_eq!(
            InsertEphemeral.plan(Scale::Smoke).scale.row_bytes,
            ROW_BYTES
        );
        // and a smoke run is genuinely smaller
        assert!(InsertEphemeral.plan(Scale::Smoke).scale.rows < ROWS);
    }

    /// The warmup never swallows the whole run, however small the scale
    #[test]
    fn the_warmup_leaves_something_to_measure() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = InsertEphemeral.plan(scale);
            assert!(
                plan.warmup < plan.scale.rows,
                "{scale:?} warms up {} of {} rows",
                plan.warmup,
                plan.scale.rows
            );
        }
    }
}
