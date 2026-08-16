//! `macro/get_resident` and `macro/get_archived` - one keyed get, with and without a disk read
//!
//! # Why these two are in one file
//!
//! They are a control and its null, and the only difference between them may be whether the
//! partition is in memory. Everything else - the rows, the seed, the query, the concurrency, the
//! warmup - has to be identical, or the pair stops answering the question it exists to ask. Two
//! files drift; one file with one axis in it cannot. This is the shape
//! `docs/src/features/validated-archives.md` settled on and that caught
//! [O24](../../../docs/src/appendix/optimizations.md).
//!
//! # What they isolate
//!
//! A get naming one partition key, answered out of a
//! [`PersistentUnsortedTable`](shoal::PersistentUnsortedTable):
//!
//! - **`macro/get_resident`** finds the partition in memory. The
//!   [`MaybeLoaded::Loaded`](shoal::tables::partitions::MaybeLoaded) arm: a hash lookup and a read
//!   out of a deserialized partition.
//! - **`macro/get_archived`** finds nothing in memory, because the server was restarted after the
//!   rows were written. The `Accessible` arm: the get parks, a disk read is issued, and the query
//!   is replayed when it lands.
//!
//! The gap between the two is the cost of the archived read path end to end, which nothing in this
//! repository has previously been able to state.
//!
//! # How to read their numbers
//!
//! As a **service time**. Both run at a bounded concurrency with each query stamped on its own,
//! so a p99 here is what one get cost and not what a batch of them cost. Their wall clocks are
//! **not** throughput figures: the server is deliberately left unsaturated.

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, Item, ItemGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many rows a full run seeds, one per partition
const ROWS: u64 = 200_000;

/// How many gets a full run measures
///
/// Fewer than the rows seeded, because each one is a round trip that is deliberately not
/// pipelined. This is enough for a p99 to mean something without the run taking minutes.
const QUERIES: u64 = 50_000;

/// How wide each row's payload is, in bytes
const ROW_BYTES: u64 = 256;

/// How many gets may be outstanding at once
///
/// Low on purpose. Every slot holds exactly one query, so the time from send to response is the
/// time that query took; raising this until the server saturates would turn every sample into a
/// queueing delay and the workload would stop measuring service time.
const CONCURRENCY: u32 = 16;

/// Which arm of the pair a workload is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Residency {
    /// The partition is in memory when the get arrives
    Resident,
    /// The partition is on disk, and the get has to wait for a read
    Archived,
}

/// A keyed get against a table that is either resident or archived
pub struct KeyedGet {
    /// Which arm this is
    pub residency: Residency,
}

impl KeyedGet {
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

impl Workload for KeyedGet {
    /// What this workload is called
    fn id(&self) -> &'static str {
        match self.residency {
            Residency::Resident => "macro/get_resident",
            Residency::Archived => "macro/get_archived",
        }
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        match self.residency {
            Residency::Resident => "one keyed get against a partition held in memory",
            Residency::Archived => "one keyed get against a partition that has to be read off disk",
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own, which makes a sample a service time
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // the write path already carries the attribution layers, and a second workload under them
        // would double what the two most expensive phases of a capture cost
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let rows = Self::rows(scale);
        // the one axis between the two arms: whether the server is cycled after seeding
        let overrides = ConfOverrides::default();
        let server = match self.residency {
            Residency::Resident => ServerNeed::Fresh(overrides),
            Residency::Archived => ServerNeed::RestartAfterSeed(overrides),
        };
        WorkloadPlan {
            server,
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes: ROW_BYTES,
                // one row per partition, so the key count is the row count
                keys: rows,
                concurrency: CONCURRENCY,
                clients: None,
                // not a mixture, a width distribution or a skewed access pattern
                ..ScaleFacts::default()
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
            // the same rows for both arms, from the same named streams, so the pair differs only
            // in where those rows are when the gets arrive
            let mut buckets = Seeded::stream(ctx.seed, "keyed_get/buckets");
            let mut payloads = Seeded::stream(ctx.seed, "keyed_get/payloads");
            let total = ctx.scale.rows;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    queries.add_mut(Item {
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
            // spread the reads across the whole key space rather than walking it in order, so the
            // archived arm cannot be helped by reading partitions in the order they were written
            let stride = stride_for(rows);
            driver::drive_per_query(
                client,
                ctx.scale.concurrency,
                queries_for(ctx),
                ctx.warmup,
                "get",
                move |index| {
                    // a multiplicative walk that visits every key exactly once before repeating,
                    // because `stride` is coprime with the key count
                    let key = index.wrapping_mul(stride) % rows;
                    ItemGet::new(vec![key])
                },
            )
            .await
        })
    }
}

/// How many gets a context asks for
///
/// # Arguments
///
/// * `ctx` - The run this workload was given
fn queries_for(ctx: &Context) -> u64 {
    // never more than there are rows, so every get finds a row and none of them measures a miss
    match ctx.scale.scale.as_str() {
        "smoke" => Scale::Smoke.rows(QUERIES).min(ctx.scale.rows),
        _ => Scale::Full.rows(QUERIES).min(ctx.scale.rows),
    }
}

/// A stride that visits every key exactly once before repeating
///
/// Reading keys in the order they were written would let the archived arm read partitions in the
/// order the archive laid them out, which is the one access pattern a disk is best at and is not
/// the one a client produces. A stride coprime with the key count walks the whole space in a fixed,
/// reproducible, non sequential order.
///
/// # Arguments
///
/// * `keys` - How many keys there are
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::keyed_get::stride_for;
///
/// // coprime with the key count, so the walk visits every key before repeating
/// assert_eq!(gcd(stride_for(200_000), 200_000), 1);
///
/// /// The greatest common divisor of two numbers
/// fn gcd(a: u64, b: u64) -> u64 {
///     if b == 0 { a } else { gcd(b, a % b) }
/// }
/// ```
pub fn stride_for(keys: u64) -> u64 {
    // start near the golden ratio of the key space, which spreads a multiplicative walk evenly
    let mut stride = ((keys as f64) * 0.6180339887) as u64 | 1;
    // then step up until it shares no factor with the key count, which is what makes the walk
    // cover every key rather than a cycle through some of them
    while gcd(stride, keys) != 1 {
        stride += 2;
    }
    stride
}

/// The greatest common divisor of two numbers
///
/// # Arguments
///
/// * `a` - The first number
/// * `b` - The second number
fn gcd(a: u64, b: u64) -> u64 {
    // Euclid, iteratively, since this is called once per run and never in a loop that matters
    let (mut a, mut b) = (a, b);
    while b != 0 {
        let next = a % b;
        a = b;
        b = next;
    }
    a
}

#[cfg(test)]
mod tests {
    use super::{gcd, stride_for, KeyedGet, Residency, ROWS, ROW_BYTES};
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::{ServerNeed, Workload};

    /// The two arms differ in residency and in nothing else
    ///
    /// The whole point of the pair. If anything else diverged, the gap between them would stop
    /// being the cost of the archived read path and become the cost of that difference too.
    #[test]
    fn the_two_arms_differ_only_in_residency() {
        for scale in [Scale::Smoke, Scale::Full] {
            let resident = KeyedGet {
                residency: Residency::Resident,
            }
            .plan(scale);
            let archived = KeyedGet {
                residency: Residency::Archived,
            }
            .plan(scale);
            // identical data, identical drive
            assert_eq!(resident.scale, archived.scale);
            assert_eq!(resident.warmup, archived.warmup);
            // and the same configuration, so only the restart separates them
            assert_eq!(
                resident.server.overrides(),
                archived.server.overrides(),
                "the two arms must run under the same configuration"
            );
            // which is the one axis
            assert!(!resident.server.restarts());
            assert!(archived.server.restarts());
        }
    }

    /// The archived arm reaches disk by restarting, not by squeezing memory
    ///
    /// A memory limit low enough to force eviction makes how much ends up on disk depend on how
    /// the run interleaved with compaction, so the workload would measure a different mixture of
    /// resident and archived reads every time. A restart flushes everything and holds nothing.
    #[test]
    fn the_archived_arm_restarts_rather_than_evicting() {
        let plan = KeyedGet {
            residency: Residency::Archived,
        }
        .plan(Scale::Full);
        assert!(matches!(plan.server, ServerNeed::RestartAfterSeed(_)));
        // and it does not pin a memory limit, which would be the other way of trying this
        assert_eq!(plan.server.overrides().and_then(|c| c.memory.clone()), None);
    }

    /// Both arms report a service time, and say so
    #[test]
    fn both_arms_declare_themselves_per_query() {
        for residency in [Residency::Resident, Residency::Archived] {
            let workload = KeyedGet { residency };
            assert_eq!(workload.timing(), Timing::PerQuery);
            // and stay well below saturation, or a sample stops being a service time
            assert!(workload.plan(Scale::Full).scale.concurrency <= 32);
        }
    }

    /// Row width does not change with scale, and a full run seeds what it says it does
    #[test]
    fn the_shape_of_the_data_is_fixed() {
        let full = KeyedGet {
            residency: Residency::Resident,
        }
        .plan(Scale::Full);
        assert_eq!(full.scale.rows, ROWS);
        assert_eq!(full.scale.row_bytes, ROW_BYTES);
        // one row per partition
        assert_eq!(full.scale.keys, full.scale.rows);
    }

    /// The warmup never swallows the whole run
    #[test]
    fn the_warmup_leaves_something_to_measure() {
        for scale in [Scale::Smoke, Scale::Full] {
            let plan = KeyedGet {
                residency: Residency::Resident,
            }
            .plan(scale);
            assert!(plan.warmup < KeyedGet::queries(scale));
        }
    }

    /// The stride visits every key exactly once before repeating
    ///
    /// If it shared a factor with the key count it would cycle through a subset, and most of the
    /// seeded rows would never be read - which for the archived arm would mean measuring a
    /// handful of partitions that stayed in the page cache.
    #[test]
    fn the_stride_covers_every_key() {
        for keys in [100u64, 2_000, 50_000, 200_000] {
            assert_eq!(gcd(stride_for(keys), keys), 1, "stride repeats for {keys}");
        }
        // and walking it really does hit everything, checked exhaustively at a small size
        let keys = 2_000u64;
        let stride = stride_for(keys);
        let mut seen = vec![false; keys as usize];
        for index in 0..keys {
            seen[(index.wrapping_mul(stride) % keys) as usize] = true;
        }
        assert!(seen.iter().all(|hit| *hit), "the walk missed a key");
    }

    /// The walk is not sequential, or the archived arm would read the archive in its own order
    #[test]
    fn the_walk_is_not_sequential() {
        let keys = 200_000u64;
        let stride = stride_for(keys);
        assert!(stride > 1, "a stride of one is a sequential scan");
    }
}
