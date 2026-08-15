//! `macro/encryption/...` - what TLS costs, across row width, load depth and client count
//!
//! # What this is for
//!
//! [F14](../../../docs/src/features/encryption-in-transit.md) shipped encryption with a control
//! pair of transport arms, and [D4](../../../docs/src/direction/encryption.md) called such a pair a
//! **precondition** for taking encryption at all. That pair answers *whether* encryption costs
//! anything at two row widths. It cannot answer *how the cost behaves*, because the transport arms
//! hold concurrency at a constant chosen per row width — 16 at 256 bytes and 4 at a MiB — so the
//! load depth is confounded with the row width and neither can be plotted against the other.
//!
//! This is the sweep that separates them. Every arm here is a get of one row, driven one query per
//! slot, so a sample is a service time and the only things that vary are the three axes below.
//!
//! # The three axes
//!
//! **Row width** is where a per-byte cost lives. `256 B` through `1 MiB` spans four orders of
//! magnitude, which is enough for a fixed per-response cost and a per-byte one to separate: over
//! this range a per-byte term rises 4096-fold and a fixed one does not move.
//!
//! **Depth** is how many queries are outstanding at once on **one** client. It says whether
//! encryption's cost is something a busier server absorbs or something it multiplies.
//!
//! **Client count** is how many independent [`Shoal`](shoal::Shoal) instances produce the load,
//! each with its own connection pool and its own TLS handshakes. It is a separate axis from depth
//! and it is the only thing in this repository that can see a handshake at all — every other
//! workload opens its pool before it samples anything, which is exactly what
//! [O30](../../../docs/src/appendix/optimizations.md) says is missing.
//!
//! # Every arm is one of a pair
//!
//! An encrypted arm and its plaintext twin differ in the wire and in nothing else: same seed, same
//! rows, same row width, same query count, same depth, same client count. That is the
//! control-pair shape [F9](../../../docs/src/features/ephemeral-tables.md) established for storage,
//! and it is what makes the gap between a pair attributable to encryption rather than to whatever
//! else moved. `a_tls_arm_differs_from_its_plaintext_twin_only_in_the_wire` is the test that keeps
//! it that way.
//!
//! # Read the median, not the tail, at the wide end
//!
//! [`Encryption::queries`] trades query count against row width so that every arm moves roughly
//! the same number of *bytes*. Without it the MiB arms would dominate a capture the way the
//! transport `large` arm already does. It has the same consequence
//! [`fanout`](super::fanout) records for the same trick: **the widest arms have the fewest
//! samples**, so their p99 is the worst few of a few hundred rather than a percentile. The curves
//! are curves in p50.

use anyhow::Result;
use std::sync::Arc;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch, StreamMode};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, Item, ItemGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// The row widths every sweep is measured at
///
/// Four orders of magnitude in four points. Doubling the count would smooth the curve and cost
/// twice the capture; these four are enough to tell a flat line from a rising one, which is the
/// only question being asked.
pub const ROW_WIDTHS: [u64; 4] = [256, 4 * 1024, 64 * 1024, 1024 * 1024];

/// The load depths the depth sweep is measured at
///
/// One is the floor: a single outstanding query, where a service time is the whole round trip and
/// nothing queues. 128 is deep enough that twelve shards are being asked for real work.
pub const DEPTHS: [u32; 4] = [1, 8, 32, 128];

/// The client counts the client sweep is measured at
///
/// Each client is an independent [`Shoal`](shoal::Shoal) with its own pool, so eight clients open
/// eight pools worth of connections and pay eight pools worth of handshakes.
pub const CLIENT_COUNTS: [u32; 4] = [1, 2, 4, 8];

/// The row widths the client sweep is measured at
///
/// Only the two ends of [`ROW_WIDTHS`], because the client axis is about per-connection cost
/// rather than per-byte cost and the middle two widths would say the same thing at four times the
/// price.
pub const CLIENT_WIDTHS: [u64; 2] = [256, 1024 * 1024];

/// How deep the client sweep drives each client
///
/// Held at one so the axis is the client count alone. A sweep that raised both at once would
/// measure their product and be unable to say which of them moved.
const CLIENT_SWEEP_DEPTH: u32 = 1;

/// How many bytes of response one full run of one arm moves
///
/// This is the budget that flattens the cost across the row width axis. At 256 bytes it buys far
/// more queries than the cap allows and the cap decides; at a MiB it buys 256, which is the
/// floor's neighbourhood. See the module header for what it costs in samples at the wide end.
const BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// The fewest queries any arm measures
///
/// Below a couple of hundred a p50 stops being worth reading at all.
const MIN_QUERIES: u64 = 200;

/// The most queries any arm measures
///
/// The narrow arms would otherwise run a million queries to spend their byte budget, which buys
/// precision nobody is reading at a cost the capture notices.
const MAX_QUERIES: u64 = 20_000;

/// How many rows an arm seeds
///
/// One row per partition, and the count is bounded by bytes rather than fixed, so a MiB arm seeds
/// 256 MiB and a 256 byte arm seeds a few megabytes. Every arm therefore sits far under the 4 GiB
/// limit `shoal.yml` sets and every read is answered from memory — the arm measures the wire
/// rather than a mixture of the wire and eviction, which is the same reasoning
/// [`transport`](super::transport) gives for its own row count.
const SEED_BYTES: u64 = 256 * 1024 * 1024;

/// The fewest rows any arm seeds, so a walk over the keys does not repeat immediately
const MIN_ROWS: u64 = 256;

/// The most rows any arm seeds
const MAX_ROWS: u64 = 20_000;

/// The share of a frame a seed bundle is allowed to fill
///
/// A bundle is refused outright at the frame bound, so sizing to exactly the bound would fail on
/// the archive's own overhead. The same quarter [`transport`](super::transport) uses.
const SEED_FRAME_SHARE: u64 = 4;

/// How many queries one arm measures at a row width
///
/// # Arguments
///
/// * `row_bytes` - How wide one row's payload is
/// * `scale` - How large a run was asked for
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::encryption::queries_for;
/// use shoal_bench::workloads::harness::seed::Scale;
///
/// // a narrow row is capped rather than allowed to spend its whole byte budget
/// assert_eq!(queries_for(256, Scale::Full), 20_000);
/// // a MiB row buys far fewer, which is what keeps the two costing about the same
/// assert!(queries_for(1024 * 1024, Scale::Full) < 1_000);
/// ```
pub fn queries_for(row_bytes: u64, scale: Scale) -> u64 {
    // a fixed budget of bytes moved, floored so the widest arm still has enough samples for a
    // median and capped so the narrowest does not run all day
    let budget = (BUDGET_BYTES / row_bytes.max(1)).clamp(MIN_QUERIES, MAX_QUERIES);
    match scale {
        Scale::Smoke => (budget / 100).max(20),
        Scale::Full => budget,
    }
}

/// How many rows one arm seeds at a row width
///
/// # Arguments
///
/// * `row_bytes` - How wide one row's payload is
/// * `scale` - How large a run was asked for
pub fn rows_for(row_bytes: u64, scale: Scale) -> u64 {
    // the same shape as the query budget, and for the same reason
    let rows = (SEED_BYTES / row_bytes.max(1)).clamp(MIN_ROWS, MAX_ROWS);
    match scale {
        Scale::Smoke => (rows / 100).max(100),
        Scale::Full => rows,
    }
}

/// How many rows of a given width fit in one seed bundle
///
/// # Arguments
///
/// * `row_bytes` - How wide one row's payload is
fn seed_batch(row_bytes: u64) -> usize {
    // what a quarter of a frame holds at this width, and never fewer than one row
    let budget = u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
        / SEED_FRAME_SHARE
        / row_bytes.max(1);
    (budget.max(1) as usize).min(driver::BATCH)
}

/// Whether an arm runs over a plaintext wire or an encrypted one
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    /// An unencrypted connection
    Plain,
    /// A connection the kernel encrypts, with rustls having done only the handshake
    Tls,
}

impl Wire {
    /// The segment this wire contributes to an identifier
    fn as_str(self) -> &'static str {
        match self {
            Wire::Plain => "plain",
            Wire::Tls => "tls",
        }
    }

    /// Whether this arm's server encrypts
    fn encrypted(self) -> bool {
        matches!(self, Wire::Tls)
    }
}

/// Which of the two sweeps an arm belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
    /// Queries outstanding at once on one client
    Depth,
    /// Independent clients, each one query deep
    Clients,
}

impl Axis {
    /// The segment this axis contributes to an identifier
    fn as_str(self) -> &'static str {
        match self {
            Axis::Depth => "depth",
            Axis::Clients => "clients",
        }
    }
}

/// One point of one sweep
pub struct Encryption {
    /// Which sweep this belongs to
    pub axis: Axis,
    /// Whether this arm is encrypted
    pub wire: Wire,
    /// How wide the rows it moves are
    pub row_bytes: u64,
    /// How many queries are outstanding at once, across every client
    pub depth: u32,
    /// How many independent clients produce that load
    pub clients: u32,
    /// This workload's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
}

impl Encryption {
    /// Every arm of both sweeps
    ///
    /// The identifiers are leaked for the reason [`fanout::Fanout::all`](super::fanout::Fanout::all)
    /// leaks its own: they are forty eight short strings built once at startup and read for the
    /// life of the process, and leaking them is what lets the axes live in the struct rather than
    /// forcing forty eight hand written types.
    ///
    /// **Axis outermost, then wire.** A workload's position in `workload_ids::IDS` decides the port
    /// a capture gives it, so the order here is the order that list declares and neither may be
    /// reshuffled to read better.
    pub fn all() -> Vec<Encryption> {
        let mut built =
            Vec::with_capacity(ROW_WIDTHS.len() * DEPTHS.len() * 2 + CLIENT_WIDTHS.len() * CLIENT_COUNTS.len() * 2);
        // the depth sweep: one client, load depth varying, at every row width
        for wire in [Wire::Plain, Wire::Tls] {
            for row_bytes in ROW_WIDTHS {
                for depth in DEPTHS {
                    built.push(Encryption::new(Axis::Depth, wire, row_bytes, depth, 1));
                }
            }
        }
        // the client sweep: independent clients, each one query deep, at the two extreme widths
        for wire in [Wire::Plain, Wire::Tls] {
            for row_bytes in CLIENT_WIDTHS {
                for clients in CLIENT_COUNTS {
                    built.push(Encryption::new(
                        Axis::Clients,
                        wire,
                        row_bytes,
                        clients * CLIENT_SWEEP_DEPTH,
                        clients,
                    ));
                }
            }
        }
        built
    }

    /// Build one arm and mint its identifier
    ///
    /// # Arguments
    ///
    /// * `axis` - Which sweep this belongs to
    /// * `wire` - Whether this arm is encrypted
    /// * `row_bytes` - How wide the rows it moves are
    /// * `depth` - How many queries are outstanding at once in total
    /// * `clients` - How many independent clients produce that load
    fn new(axis: Axis, wire: Wire, row_bytes: u64, depth: u32, clients: u32) -> Self {
        // the swept value goes last, so an identifier reads as a point on a named curve
        let swept = match axis {
            Axis::Depth => depth,
            Axis::Clients => clients,
        };
        let id: &'static str = Box::leak(
            format!(
                "macro/encryption/{}/{}/{row_bytes}/{swept}",
                axis.as_str(),
                wire.as_str()
            )
            .into_boxed_str(),
        );
        Encryption {
            axis,
            wire,
            row_bytes,
            depth,
            clients,
            id,
        }
    }

    /// The scale a context was built at
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run this workload was given
    fn scale_of(ctx: &Context) -> Scale {
        // anything that is not a smoke run is a full one, which is what every other workload does
        if ctx.scale.scale == "smoke" {
            Scale::Smoke
        } else {
            Scale::Full
        }
    }
}

impl Workload for Encryption {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        match (self.axis, self.wire) {
            (Axis::Depth, Wire::Plain) => "a get of one row at a fixed width and load depth",
            (Axis::Depth, Wire::Tls) => "the same get over an encrypted wire",
            (Axis::Clients, Wire::Plain) => "a get of one row from n independent clients",
            (Axis::Clients, Wire::Tls) => "the same get from n independent encrypted clients",
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own, so a sample is a service time. a curve
        // built from batch completion times would bend with the batching rather than with the axis
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // forty eight workloads under two attribution layers would be ninety six instrumented runs
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let rows = rows_for(self.row_bytes, scale);
        WorkloadPlan {
            // nothing about the server is pinned but the wire. these measure the client and the
            // path to it, so an arm that named a shard count or a memory limit would be holding
            // still something it is not about - the same choice `transport` makes
            server: ServerNeed::Fresh(ConfOverrides {
                tls: self.wire.encrypted(),
                ..ConfOverrides::default()
            }),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes: self.row_bytes,
                // one row per partition, so a get names one key and comes back with one row
                keys: rows,
                concurrency: self.depth,
                clients: Some(self.clients),
            },
            warmup: (queries_for(self.row_bytes, scale) / 20).max(10),
        }
    }

    /// Writes one row into each partition, without timing any of it
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            // named streams, so an arm and its twin seed byte identical rows and the only
            // difference between them is the wire
            let mut buckets = Seeded::stream(ctx.seed, "encryption/buckets");
            let mut payloads = Seeded::stream(ctx.seed, "encryption/payloads");
            let row_bytes = self.row_bytes;
            // sized from the row width rather than copied, since a bundle of MiB rows is a frame
            // the server refuses
            let batch = seed_batch(row_bytes);
            let total = ctx.scale.rows;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..(batch as u64).min(total - built) as usize {
                    queries.add_mut(Item {
                        id: built,
                        bucket: buckets.below(16),
                        label: payloads.string(16),
                        payload: payloads.string(row_bytes as usize),
                    });
                    built += 1;
                }
                Some(Batch { queries })
            };
            // the seed gate is bounded in bytes rather than queries, since a bundle of MiB rows
            // holds as much memory as a get's responses do
            let gate = (BUDGET_BYTES / row_bytes.max(1)).clamp(8, 1024) as usize;
            driver::drive_with(&client, batches, "seed", 0, StreamMode::Unordered, gate).await?;
            Ok(())
        })
    }

    /// Reads one row per query, timing each query on its own
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            // one client per client-axis point, opened here rather than in the driver so that
            // every one of them has finished connecting before the first sample is taken
            let mut clients = Vec::with_capacity(self.clients as usize);
            for _ in 0..self.clients.max(1) {
                clients.push(Arc::new(ctx.client().await?));
            }
            let rows = ctx.scale.rows;
            let stride = crate::workloads::keyed_get::stride_for(rows);
            driver::drive_per_query_across(
                &clients,
                self.depth,
                queries_for(self.row_bytes, Self::scale_of(ctx)),
                ctx.warmup,
                "get",
                move |index| {
                    // a multiplicative walk that visits every key exactly once before repeating,
                    // because the stride is coprime with the key count
                    ItemGet::new(vec![index.wrapping_mul(stride) % rows])
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Both sweeps mint every combination once, and nothing is minted twice
    fn every_point_of_both_sweeps_is_minted() {
        let all = Encryption::all();
        let depth = ROW_WIDTHS.len() * DEPTHS.len() * 2;
        let clients = CLIENT_WIDTHS.len() * CLIENT_COUNTS.len() * 2;
        assert_eq!(all.len(), depth + clients);
        assert_eq!(all.len(), 48);
        let mut ids: Vec<&str> = all.iter().map(|arm| arm.id()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "an encryption id is minted twice");
    }

    #[test]
    /// An identifier names its sweep, its wire, its row width and the value being swept
    fn an_id_names_its_axis_wire_width_and_point() {
        for arm in Encryption::all() {
            let swept = match arm.axis {
                Axis::Depth => arm.depth,
                Axis::Clients => arm.clients,
            };
            assert_eq!(
                arm.id(),
                format!(
                    "macro/encryption/{}/{}/{}/{swept}",
                    arm.axis.as_str(),
                    arm.wire.as_str(),
                    arm.row_bytes
                )
            );
        }
    }

    #[test]
    /// A TLS arm differs from its plaintext twin in the wire and in nothing else
    ///
    /// This is the control-pair rule, and it is the whole reason the gap between a pair can be
    /// called the cost of encryption. A constant changed in one half has to change in the other.
    fn a_tls_arm_differs_from_its_plaintext_twin_only_in_the_wire() {
        let all = Encryption::all();
        for plain in all.iter().filter(|arm| arm.wire == Wire::Plain) {
            let twin = all
                .iter()
                .find(|arm| {
                    arm.wire == Wire::Tls
                        && arm.axis == plain.axis
                        && arm.row_bytes == plain.row_bytes
                        && arm.depth == plain.depth
                        && arm.clients == plain.clients
                })
                .expect("a plaintext arm has no encrypted twin");
            assert_eq!(twin.timing(), plain.timing());
            assert_eq!(twin.profiles(), plain.profiles());
            for scale in [Scale::Smoke, Scale::Full] {
                let (left, right) = (plain.plan(scale), twin.plan(scale));
                assert_eq!(left.scale.rows, right.scale.rows);
                assert_eq!(left.scale.row_bytes, right.scale.row_bytes);
                assert_eq!(left.scale.keys, right.scale.keys);
                assert_eq!(left.scale.concurrency, right.scale.concurrency);
                assert_eq!(left.scale.clients, right.scale.clients);
                assert_eq!(left.warmup, right.warmup);
            }
        }
    }

    #[test]
    /// Only the encrypted arms ask their server for encryption
    fn only_the_tls_arms_configure_a_tls_server() {
        for arm in Encryption::all() {
            let plan = arm.plan(Scale::Full);
            let overrides = plan
                .server
                .overrides()
                .expect("an encryption arm always needs a server");
            assert_eq!(
                overrides.tls,
                arm.wire == Wire::Tls,
                "{} asked for the wrong wire",
                arm.id()
            );
        }
    }

    #[test]
    /// Every arm moves roughly the same number of bytes, so no width dominates a capture
    ///
    /// Without this the MiB arms would cost four thousand times the 256 byte ones and the sweep
    /// would be longer than the rest of the capture put together.
    fn every_width_costs_roughly_the_same() {
        let moved: Vec<u64> = ROW_WIDTHS
            .iter()
            .map(|width| queries_for(*width, Scale::Full) * width)
            .collect();
        let smallest = *moved.iter().min().expect("no widths");
        let largest = *moved.iter().max().expect("no widths");
        assert!(
            largest <= smallest * 60,
            "the widest arm moves {largest} bytes against the narrowest's {smallest}, which is \
             too wide a spread for one capture"
        );
    }

    #[test]
    /// Every arm seeds far less than the memory limit the benchmark config sets
    ///
    /// An arm that seeded past it would measure a mixture of the wire and eviction, and the gap
    /// between a pair would stop being the wire alone.
    fn no_arm_seeds_past_the_memory_limit() {
        // `shoal.yml` sets 4 GiB; a quarter of it is generous headroom for one arm
        let limit = 1024 * 1024 * 1024;
        for width in ROW_WIDTHS {
            let seeded = rows_for(width, Scale::Full) * width;
            assert!(
                seeded < limit,
                "a {width} byte arm seeds {seeded} bytes, which is past the headroom"
            );
        }
    }

    #[test]
    /// The client sweep varies clients alone, and the depth sweep varies depth alone
    ///
    /// Raising both at once would measure their product, and neither axis could then be read.
    fn each_sweep_varies_one_thing() {
        for arm in Encryption::all() {
            match arm.axis {
                Axis::Depth => assert_eq!(arm.clients, 1, "{} moved the client count", arm.id()),
                Axis::Clients => assert_eq!(
                    arm.depth,
                    arm.clients * CLIENT_SWEEP_DEPTH,
                    "{} moved the depth independently of the client count",
                    arm.id()
                ),
            }
        }
    }

    #[test]
    /// A smoke run still measures enough queries to be worth running
    fn a_smoke_run_still_has_queries_to_send() {
        for width in ROW_WIDTHS {
            assert!(queries_for(width, Scale::Smoke) >= 20);
            assert!(rows_for(width, Scale::Smoke) >= 100);
        }
    }

    #[test]
    /// A seed bundle fits in a frame at every width
    fn a_seed_bundle_fits_in_a_frame() {
        for width in ROW_WIDTHS {
            let batch = seed_batch(width) as u64;
            assert!(batch >= 1, "a {width} byte arm seeds no rows per bundle");
            assert!(
                batch * width < u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES),
                "a {width} byte arm builds a bundle larger than a frame"
            );
        }
    }
}
