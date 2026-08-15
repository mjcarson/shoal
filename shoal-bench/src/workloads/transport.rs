//! `macro/transport/[tls/]{mode}/{small,large}` - four client transport modes, two row sizes,
//! two wires
//!
//! # What these are for
//!
//! The client is the one layer of this system whose total has never been bounded. Every macro
//! workload that exists measures a path through the *engine* and charges the client's share of it
//! to the engine, because nothing separates the two. `docs/src/appendix/todos.md` filed these as
//! "the only thing that could say how much of a measured latency is the harness's own", and
//! `docs/src/direction/overview.md` is blocked on them: nine design pages argue about a transport
//! nobody has measured.
//!
//! # The three axes
//!
//! **Mode** is the subject. [`Shoal::send`](shoal::Shoal), `Shoal::stream` and
//! `Shoal::stream_unordered` are three different paths through the client with three different
//! costs, and a caller picking between them today is picking blind. `send_one` and `send_batched`
//! are the same call with one query in the bundle and with many, which is the axis that says what
//! bundling is worth.
//!
//! **Row size** is what makes the set able to answer the question
//! [D4](../../../docs/src/direction/encryption.md) needs answered. A `small` row is 256 bytes and a
//! `large` row is a MiB. They are not the same measurement at a different scale — they are two
//! regimes. At 256 bytes a response is one frame, one allocation and a round trip, and the fixed
//! per-response costs dominate. At a MiB the per-byte costs dominate and everything fixed
//! disappears into the noise. A per-byte tax such as encryption is invisible in the first regime
//! and is the entire cost in the second, so a set with only the small arm would report that
//! encryption is nearly free and be wrong by an order of magnitude.
//!
//! **Wire** is the axis that answers [D4](../../../docs/src/direction/encryption.md) rather than
//! merely being sized for it. Each of the eight arms above has a twin whose server encrypts and
//! which differs in nothing else — same seed, same rows, same query count, same concurrency — so
//! the gap between a pair is what encryption costs and nothing else. That is the control-pair
//! shape [F9](../../../docs/src/features/ephemeral-tables.md) established for storage, and D4
//! called it a *precondition* for taking encryption rather than a follow-up.
//!
//! The plaintext arms carry no `tls/` segment, so the eight identifiers that existed before this
//! axis are byte identical to what they were. Renaming one would orphan every capture taken before
//! the rename, which is why `the_plaintext_arms_kept_their_original_names` exists.
//!
//! # Why the large arm is not just the small arm with a bigger number
//!
//! Three constants have to move together with the row width, and each of them is a hang or a
//! failure if it does not:
//!
//! - **The seed bundle.** An insert bundle carries every row it inserts, so a hundred MiB rows in
//!   one bundle is a hundred MiB frame against a
//!   [64 MiB bound](shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES). [`seed_batch`] derives it
//!   from the row width instead of copying [`driver::BATCH`].
//! - **The in flight gate.** The gate bounds outstanding *responses* as well as outstanding
//!   queries. At the driver's default of 4096 a MiB arm would hold four gigabytes of responses in
//!   memory at once.
//! - **The concurrency.** Same reason, for the per-batch modes, which hold a whole bundle's
//!   responses at once per slot.
//!
//! # How to read their numbers
//!
//! `send_one` reports a **service time** and the others report **batch completion times**, because
//! that is what each mode is. A comparison never joins one to the other, and the artifact records
//! which a number came from. The number worth reading off the three per-batch modes is the wall
//! clock.

use anyhow::{Context as _, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch, StreamMode};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{BenchClient, Item, ItemGet};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How wide a `small` row's payload is, in bytes
///
/// The same 256 the keyed get pair uses, so a transport number at this width can be read beside a
/// `macro/get_resident` number without the row shape being one of the differences.
const SMALL_ROW_BYTES: u64 = 256;

/// How wide a `large` row's payload is, in bytes
///
/// A MiB, which is the size the deployment this is built for actually returns. It is also
/// comfortably more than one TLS record, which is what makes this arm able to say anything about
/// [D4](../../../docs/src/direction/encryption.md).
const LARGE_ROW_BYTES: u64 = 1024 * 1024;

/// How many rows a full `small` run seeds, one per partition
const SMALL_ROWS: u64 = 200_000;

/// How many rows a full `large` run seeds, one per partition
///
/// Far fewer, because the point is the width of a row and not how many there are. 512 MiB of rows
/// sits under the 4 GiB memory limit `shoal.yml` sets, so every read is answered from memory and
/// the arm measures the transport rather than a mixture of transport and eviction.
const LARGE_ROWS: u64 = 512;

/// How many queries a full `small` run measures
const SMALL_QUERIES: u64 = 50_000;

/// How many queries a full `large` run measures
///
/// Two thousand MiB responses is two gigabytes over the wire, which is enough for a per-byte cost
/// to separate from the noise and short enough that the arm is not the longest phase of a capture.
const LARGE_QUERIES: u64 = 2_000;

/// How many queries may be outstanding at once on a `small` run
const SMALL_CONCURRENCY: u32 = 16;

/// How many queries may be outstanding at once on a `large` run
///
/// Four rather than sixteen, because each outstanding query here holds a MiB response.
const LARGE_CONCURRENCY: u32 = 4;

/// How many queries may be outstanding at once in the streaming modes on a `large` run
///
/// This bounds memory rather than throughput: sixty four outstanding MiB responses is sixty four
/// megabytes, where the driver's default gate would be four gigabytes.
const LARGE_IN_FLIGHT: usize = 64;

/// The share of a frame a seed bundle is allowed to fill
///
/// A bundle is refused outright at the frame bound, so sizing to exactly the bound would fail on
/// the archive's own overhead. A quarter leaves room for that without needing to model it.
const SEED_FRAME_SHARE: u64 = 4;

/// How many rows of a given width fit in one seed bundle
///
/// # Arguments
///
/// * `row_bytes` - How wide one row's payload is
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::transport::seed_batch;
///
/// // a narrow row is capped by the driver's batch size rather than by the frame
/// assert_eq!(seed_batch(256), 100);
/// // a MiB row is capped by the frame bound, well below that
/// assert!(seed_batch(1024 * 1024) < 100);
/// ```
pub fn seed_batch(row_bytes: u64) -> usize {
    // what a quarter of a frame holds at this row width, and never fewer than one row
    let budget = u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
        / SEED_FRAME_SHARE
        / row_bytes.max(1);
    // a narrow row hits the driver's batch size long before it hits the frame bound, and that is
    // the number every other workload seeds with
    (budget.max(1) as usize).min(driver::BATCH)
}

/// Which client call a workload drives
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `Shoal::send` with one query in the bundle, awaited on its own
    SendOne,
    /// `Shoal::send` with many queries in the bundle
    SendBatched,
    /// `Shoal::stream`, which hands responses back in the order they were sent
    Stream,
    /// `Shoal::stream_unordered`, which hands them back as they arrive
    StreamUnordered,
}

impl Mode {
    /// The name this mode appears under in an identifier
    fn as_str(self) -> &'static str {
        match self {
            Mode::SendOne => "send_one",
            Mode::SendBatched => "send_batched",
            Mode::Stream => "stream",
            Mode::StreamUnordered => "stream_unordered",
        }
    }
}

/// How wide the rows a workload moves are
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSize {
    /// A 256 byte payload, where the fixed per-response costs dominate
    Small,
    /// A MiB payload, where the per-byte costs dominate
    Large,
}

impl RowSize {
    /// The name this size appears under in an identifier
    fn as_str(self) -> &'static str {
        match self {
            RowSize::Small => "small",
            RowSize::Large => "large",
        }
    }

    /// How wide one row's payload is at this size
    fn row_bytes(self) -> u64 {
        match self {
            RowSize::Small => SMALL_ROW_BYTES,
            RowSize::Large => LARGE_ROW_BYTES,
        }
    }

    /// How many rows a run at this size seeds
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn rows(self, scale: Scale) -> u64 {
        match self {
            RowSize::Small => scale.rows(SMALL_ROWS),
            RowSize::Large => scale.rows(LARGE_ROWS),
        }
    }

    /// How many queries a run at this size measures
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn queries(self, scale: Scale) -> u64 {
        match self {
            RowSize::Small => scale.rows(SMALL_QUERIES),
            RowSize::Large => scale.rows(LARGE_QUERIES),
        }
    }

    /// How many queries may be outstanding at once at this size
    fn concurrency(self) -> u32 {
        match self {
            RowSize::Small => SMALL_CONCURRENCY,
            RowSize::Large => LARGE_CONCURRENCY,
        }
    }

    /// How many queries the streaming modes may have outstanding at once at this size
    fn in_flight(self) -> usize {
        match self {
            RowSize::Small => driver::IN_FLIGHT,
            RowSize::Large => LARGE_IN_FLIGHT,
        }
    }

    /// How many queries one measured bundle carries at this size
    ///
    /// A get bundle carries keys rather than rows, so the frame bound does not apply to it — but
    /// the *responses* to one bundle all come back at once, so a bundle of a hundred MiB gets is a
    /// hundred megabytes of responses held together.
    fn query_batch(self) -> usize {
        match self {
            RowSize::Small => driver::BATCH,
            RowSize::Large => 8,
        }
    }
}

/// One client transport mode, at one row size
/// Whether an arm runs over a plaintext wire or an encrypted one
///
/// The third axis, and the one [D4](../../../docs/src/direction/encryption.md) called a
/// **precondition** for taking encryption at all: "a plaintext-versus-TLS pair is a precondition
/// for taking this, not a follow-up". A pair differs in this and in nothing else — same mode, same
/// row width, same seed, same query count — which is what makes the difference between them
/// attributable to the wire. It is the control-pair shape
/// [F9](../../../docs/src/features/ephemeral-tables.md) established for storage, applied here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    /// An unencrypted connection, which is what every capture before F14 measured
    Plain,
    /// A connection the kernel encrypts, with rustls having done only the handshake
    Tls,
}

impl Wire {
    /// The segment this wire contributes to an identifier
    ///
    /// The plaintext arm contributes nothing, so the eight identifiers that existed before the
    /// TLS arms were added keep the names every capture before them joins on. Renaming one would
    /// orphan every capture taken before the rename, which `workload_ids` says in full.
    fn segment(self) -> &'static str {
        match self {
            Wire::Plain => "",
            Wire::Tls => "tls/",
        }
    }

    /// Whether this arm's server encrypts
    fn encrypted(self) -> bool {
        matches!(self, Wire::Tls)
    }
}

pub struct Transport {
    /// Which client call this drives
    pub mode: Mode,
    /// How wide the rows it moves are
    pub size: RowSize,
    /// Whether this arm is encrypted
    pub wire: Wire,
    /// This workload's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
}

impl Transport {
    /// Every transport workload, all four modes at both row sizes on both wires
    ///
    /// The identifiers are leaked for the reason `fanout::Fanout::all` leaks its own: they are
    /// sixteen short strings built once at startup and read for the life of the process, and
    /// leaking them is what lets the three parameters live in the struct rather than forcing
    /// sixteen hand written types.
    ///
    /// **Wire outermost, and that ordering is load bearing.** A workload's position in
    /// `workload_ids::IDS` decides the port a capture gives it, so the eight plaintext arms have
    /// to keep the positions they had before the encrypted ones existed. Putting the wire on the
    /// inside would interleave them and move every plaintext arm onto a different port. Mode is
    /// next and size innermost, so a mode's two sizes stay adjacent — that is the pair a reader
    /// wants to see together.
    pub fn all() -> Vec<Transport> {
        let mut built = Vec::with_capacity(16);
        for wire in [Wire::Plain, Wire::Tls] {
            for mode in [
                Mode::SendOne,
                Mode::SendBatched,
                Mode::Stream,
                Mode::StreamUnordered,
            ] {
                for size in [RowSize::Small, RowSize::Large] {
                    let id: &'static str = Box::leak(
                        format!(
                            "macro/transport/{}{}/{}",
                            wire.segment(),
                            mode.as_str(),
                            size.as_str()
                        )
                        .into_boxed_str(),
                    );
                    built.push(Transport {
                        mode,
                        size,
                        wire,
                        id,
                    });
                }
            }
        }
        built
    }

    /// The scale a context was built at
    ///
    /// The context carries the scale as the string it is recorded under, so this is how a workload
    /// gets back to the value its own sizing functions take.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run this workload was given
    fn scale_of(ctx: &Context) -> Scale {
        // anything that is not a smoke run is a full one, which is what every other workload
        // asking this question assumes
        if ctx.scale.scale == "smoke" {
            Scale::Smoke
        } else {
            Scale::Full
        }
    }

    /// Drives `Shoal::send` with many queries per bundle, timing each bundle
    ///
    /// This is the one mode with no driver behind it. [`driver::drive`] streams, and
    /// [`driver::drive_per_query`] sends one query at a time, so neither of them exercises the call
    /// this mode is about: a bundle handed to `send` and drained to its end.
    ///
    /// # Arguments
    ///
    /// * `client` - The client to send on
    /// * `ctx` - The server, seed and scale this run was given
    async fn drive_batched(&self, client: Arc<shoal::Shoal<BenchClient>>, ctx: &Context) -> Result<Measurement> {
        let rows = ctx.scale.rows;
        let batch = self.size.query_batch() as u64;
        let total = self.size.queries(Self::scale_of(ctx));
        let stride = crate::workloads::keyed_get::stride_for(rows);
        // read out of the context before anything is spawned, since a slot outlives the borrow
        let warmup = ctx.warmup;
        // one shared cursor over bundles, so the slots share the work rather than each taking a
        // fixed slice and one slow slot leaving the others idle at the end
        let next = Arc::new(AtomicU64::new(0));
        let mut slots = tokio::task::JoinSet::new();
        for _ in 0..self.size.concurrency().max(1) {
            let client = client.clone();
            let next = next.clone();
            slots.spawn(async move {
                let mut measured = Measurement::default();
                loop {
                    // claim the next bundle, and stop when every query has been claimed
                    let bundle = next.fetch_add(1, Ordering::Relaxed);
                    let first = bundle * batch;
                    if first >= total {
                        break;
                    }
                    // build one bundle of gets, each naming one partition
                    let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                    for offset in 0..batch.min(total - first) {
                        // the same coprime walk the keyed get uses, so no bundle reads the archive
                        // in the order it was written
                        let key = (first + offset).wrapping_mul(stride) % rows;
                        queries.add_mut(ItemGet::new(vec![key]));
                    }
                    // one timestamp around the whole bundle, which is what makes this a batch
                    // sample rather than a service time
                    let started = Instant::now();
                    let mut stream = client
                        .send(queries)
                        .await
                        .context("failed to send a bundle")?;
                    let mut retrieved = 0u64;
                    while let Some(response) =
                        stream.next().await.context("failed to read a response")?
                    {
                        // a query that failed makes every number after it meaningless
                        response
                            .suceeded(shoal::QuerySuceededOpts::default())
                            .context("a query failed")?;
                        if let Ok(Some(found)) = response.access::<Item>() {
                            retrieved += found.len() as u64;
                        }
                    }
                    let elapsed = started.elapsed();
                    measured.count("retrieved", retrieved);
                    // the warmup is counted on the claimed index so every slot agrees where it ends
                    if first >= warmup {
                        measured.record("get", elapsed);
                    }
                }
                Ok::<Measurement, anyhow::Error>(measured)
            });
        }
        // pool what every slot gathered
        let mut measured = Measurement::default();
        while let Some(slot) = slots.join_next().await {
            measured.absorb(slot.context("a bundle slot panicked")??);
        }
        Ok(measured)
    }

    /// Drives one of the two streaming modes, timing each bundle
    ///
    /// # Arguments
    ///
    /// * `client` - The client to send on
    /// * `ctx` - The server, seed and scale this run was given
    /// * `ordering` - Which of the two streams to drain
    async fn drive_streamed(
        &self,
        client: &shoal::Shoal<BenchClient>,
        ctx: &Context,
        ordering: StreamMode,
    ) -> Result<Measurement> {
        let rows = ctx.scale.rows;
        let batch = self.size.query_batch() as u64;
        let total = self.size.queries(Self::scale_of(ctx));
        let stride = crate::workloads::keyed_get::stride_for(rows);
        let mut sent = 0u64;
        let batches = move || {
            if sent >= total {
                return None;
            }
            let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
            for _ in 0..batch.min(total - sent) {
                // the same coprime walk every other read workload uses
                let key = sent.wrapping_mul(stride) % rows;
                queries.add_mut(ItemGet::new(vec![key]));
                sent += 1;
            }
            Some(Batch { queries })
        };
        driver::drive_with(
            client,
            batches,
            "get",
            ctx.warmup,
            ordering,
            self.size.in_flight(),
        )
        .await
    }
}

impl Workload for Transport {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        match self.mode {
            Mode::SendOne => "one get per bundle through Shoal::send, awaited on its own",
            Mode::SendBatched => "many gets per bundle through Shoal::send",
            Mode::Stream => "gets through Shoal::stream, responses in the order they were sent",
            Mode::StreamUnordered => {
                "gets through Shoal::stream_unordered, responses as they arrive"
            }
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        match self.mode {
            // one query per slot, each stamped on its own, which makes a sample a service time
            Mode::SendOne => Timing::PerQuery,
            // one timestamp covers a whole bundle, so every query in it is charged for the ones
            // ahead of it. the number worth reading off these is the wall clock
            Mode::SendBatched | Mode::Stream | Mode::StreamUnordered => Timing::PerBatch,
        }
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // eight workloads under two attribution layers would be sixteen instrumented runs, and the
        // write path already carries both
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let rows = self.size.rows(scale);
        // nothing about the server is pinned. these measure the client, so a workload that named a
        // shard count or a memory limit would be holding still something it is not about
        WorkloadPlan {
            // the only thing an arm asks of its server is whether the wire is encrypted. every
            // other setting is the committed `shoal.yml`, so a pair differs in the wire alone
            server: ServerNeed::Fresh(ConfOverrides {
                tls: self.wire.encrypted(),
                ..ConfOverrides::default()
            }),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                row_bytes: self.size.row_bytes(),
                // one row per partition, so the key count is the row count and a get returns
                // exactly one row
                keys: rows,
                concurrency: self.size.concurrency(),
            },
            // enough to cover connection establishment before sampling starts
            warmup: (self.size.queries(scale) / 20).min(2_000),
        }
    }

    /// Writes the rows this workload will read, without timing any of it
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            // the same named streams for every mode, so the four modes at one size read byte
            // identical rows and the only difference between them is the call being measured
            let mut buckets = Seeded::stream(ctx.seed, "transport/buckets");
            let mut payloads = Seeded::stream(ctx.seed, "transport/payloads");
            let row_bytes = self.size.row_bytes();
            // sized from the row width rather than copied, since a hundred MiB rows in one bundle
            // is a frame the server refuses
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
            // no warmup, since nothing here is sampled, and the size's own gate rather than the
            // driver's default, since an insert bundle of MiB rows is as large as a get's responses
            driver::drive_with(
                &client,
                batches,
                "seed",
                0,
                StreamMode::Unordered,
                self.size.in_flight(),
            )
            .await?;
            Ok(())
        })
    }

    /// Reads rows back through the mode this workload is about, timing what comes back
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = Arc::new(ctx.client().await?);
            match self.mode {
                Mode::SendOne => {
                    let rows = ctx.scale.rows;
                    let stride = crate::workloads::keyed_get::stride_for(rows);
                    driver::drive_per_query(
                        client,
                        ctx.scale.concurrency,
                        self.size.queries(Self::scale_of(ctx)),
                        ctx.warmup,
                        "get",
                        move |index| {
                            // a multiplicative walk that visits every key exactly once before
                            // repeating, because the stride is coprime with the key count
                            ItemGet::new(vec![index.wrapping_mul(stride) % rows])
                        },
                    )
                    .await
                }
                Mode::SendBatched => self.drive_batched(client, ctx).await,
                Mode::Stream => self.drive_streamed(&client, ctx, StreamMode::Ordered).await,
                Mode::StreamUnordered => {
                    self.drive_streamed(&client, ctx, StreamMode::Unordered)
                        .await
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{seed_batch, Mode, RowSize, Transport, Wire, LARGE_ROW_BYTES, SMALL_ROW_BYTES};
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::driver;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// Every mode is minted at both sizes on both wires, and nothing is minted twice
    #[test]
    fn every_mode_and_size_is_minted() {
        let all = Transport::all();
        assert_eq!(all.len(), 16);
        let mut ids: Vec<&str> = all.iter().map(|t| t.id()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "a transport id is minted twice");
    }

    /// An identifier names its wire, its mode and its size, which is what makes a capture readable
    #[test]
    fn an_id_names_its_wire_mode_and_size() {
        for workload in Transport::all() {
            let expected = format!(
                "macro/transport/{}{}/{}",
                workload.wire.segment(),
                workload.mode.as_str(),
                workload.size.as_str()
            );
            assert_eq!(workload.id(), expected);
        }
    }

    /// The plaintext arms keep the names every capture before the TLS axis joins on
    ///
    /// Renaming a workload orphans every capture taken before the rename, which is why the
    /// plaintext wire contributes no segment at all. This is the test that turns an attempt to
    /// tidy that asymmetry into a failure instead of a silent loss of history.
    #[test]
    fn the_plaintext_arms_kept_their_original_names() {
        let all = Transport::all();
        for expected in [
            "macro/transport/send_one/small",
            "macro/transport/send_one/large",
            "macro/transport/send_batched/small",
            "macro/transport/send_batched/large",
            "macro/transport/stream/small",
            "macro/transport/stream/large",
            "macro/transport/stream_unordered/small",
            "macro/transport/stream_unordered/large",
        ] {
            assert!(
                all.iter().any(|t| t.id() == expected),
                "{expected} is no longer minted, which orphans every capture that named it"
            );
        }
    }

    /// A pair differs in the wire and in nothing else
    ///
    /// This is the control-pair rule F9 established for storage, applied to the wire. A constant
    /// changed in one half has to change in the other, or the gap between them stops being what
    /// encryption costs and starts being whatever else moved.
    #[test]
    fn a_tls_arm_differs_from_its_plaintext_twin_only_in_the_wire() {
        let all = Transport::all();
        for plain in all.iter().filter(|t| t.wire == Wire::Plain) {
            let twin = all
                .iter()
                .find(|t| t.wire == Wire::Tls && t.mode == plain.mode && t.size == plain.size)
                .expect("a plaintext arm has no encrypted twin");
            // everything the workload decides about its own shape has to match
            assert_eq!(twin.timing(), plain.timing());
            assert_eq!(twin.profiles(), plain.profiles());
            for scale in [Scale::Smoke, Scale::Full] {
                let (left, right) = (plain.plan(scale), twin.plan(scale));
                assert_eq!(left.scale.rows, right.scale.rows);
                assert_eq!(left.scale.row_bytes, right.scale.row_bytes);
                assert_eq!(left.scale.keys, right.scale.keys);
                assert_eq!(left.scale.concurrency, right.scale.concurrency);
                assert_eq!(left.warmup, right.warmup);
            }
        }
    }

    /// Only the encrypted arms ask their server for encryption
    #[test]
    fn only_the_tls_arms_configure_a_tls_server() {
        for workload in Transport::all() {
            let plan = workload.plan(Scale::Full);
            let overrides = plan
                .server
                .overrides()
                .expect("a transport workload always needs a server");
            assert_eq!(
                overrides.tls,
                workload.wire == Wire::Tls,
                "{} asked for the wrong wire",
                workload.id()
            );
        }
    }

    /// The two sizes of one mode differ in the row width and in nothing about the query
    ///
    /// The pair exists to isolate a per-byte cost, so anything else that moved between them would
    /// land in the gap and be read as one.
    #[test]
    fn the_sizes_of_one_mode_differ_only_in_the_row() {
        let all = Transport::all();
        for mode in [
            Mode::SendOne,
            Mode::SendBatched,
            Mode::Stream,
            Mode::StreamUnordered,
        ] {
            let small = all
                .iter()
                .find(|t| t.mode == mode && t.size == RowSize::Small)
                .expect("a small arm");
            let large = all
                .iter()
                .find(|t| t.mode == mode && t.size == RowSize::Large)
                .expect("a large arm");
            let left = small.plan(Scale::Full);
            let right = large.plan(Scale::Full);
            // neither arm pins anything about the server, so the server is not an axis
            assert_eq!(left.server.overrides(), right.server.overrides());
            assert!(!left.server.restarts() && !right.server.restarts());
            // the one axis
            assert_eq!(left.scale.row_bytes, SMALL_ROW_BYTES);
            assert_eq!(right.scale.row_bytes, LARGE_ROW_BYTES);
            // and both arms of a mode report the same kind of number, or they could not be read
            // against each other at all
            assert_eq!(small.timing(), large.timing());
        }
    }

    /// A seed bundle never exceeds the frame the server will accept
    ///
    /// This is the check that would otherwise be discovered as a refused frame partway through a
    /// capture, after the large arm had already spent minutes seeding.
    #[test]
    fn a_seed_bundle_fits_in_a_frame() {
        let max = u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES);
        for row_bytes in [SMALL_ROW_BYTES, LARGE_ROW_BYTES, 4 * 1024 * 1024, 1] {
            let carried = seed_batch(row_bytes) as u64 * row_bytes;
            assert!(
                carried < max,
                "a bundle of {row_bytes} byte rows carries {carried} bytes against a {max} byte bound"
            );
        }
    }

    /// A seed bundle always carries at least one row
    ///
    /// A batch of zero would build an empty bundle forever and the seed would never finish, which
    /// is a hang rather than a failure.
    #[test]
    fn a_seed_bundle_is_never_empty() {
        for row_bytes in [1, SMALL_ROW_BYTES, LARGE_ROW_BYTES, u64::from(u32::MAX)] {
            assert!(seed_batch(row_bytes) >= 1, "{row_bytes} byte rows batch to nothing");
        }
    }

    /// The large arm holds far less in flight than the small one
    ///
    /// Without this the MiB arm would hold four gigabytes of responses at once and be measuring the
    /// allocator.
    #[test]
    fn the_large_arm_bounds_what_it_holds() {
        assert_eq!(RowSize::Small.in_flight(), driver::IN_FLIGHT);
        assert!(RowSize::Large.in_flight() < RowSize::Small.in_flight());
        assert!(RowSize::Large.concurrency() < RowSize::Small.concurrency());
        assert!(RowSize::Large.query_batch() < RowSize::Small.query_batch());
        // and what the streaming modes may hold at once stays bounded in bytes rather than in
        // queries, which is the property that actually matters
        let held = RowSize::Large.in_flight() as u64 * RowSize::Large.row_bytes();
        assert!(held <= 128 * 1024 * 1024, "the large arm may hold {held} bytes");
    }

    /// Only the single send mode reports a service time
    ///
    /// The other three saturate, so a per-query stamp on them would measure queueing.
    #[test]
    fn only_the_single_send_is_per_query() {
        for workload in Transport::all() {
            let expected = match workload.mode {
                Mode::SendOne => Timing::PerQuery,
                _ => Timing::PerBatch,
            };
            assert_eq!(workload.timing(), expected, "{}", workload.id());
        }
    }

    /// A smoke run seeds enough rows for the walk over them to be meaningful
    #[test]
    fn a_smoke_run_still_has_rows_to_read() {
        for workload in Transport::all() {
            let plan = workload.plan(Scale::Smoke);
            assert!(plan.scale.rows >= 100, "{} seeds too little", workload.id());
            assert!(plan.scale.keys == plan.scale.rows);
        }
    }
}
