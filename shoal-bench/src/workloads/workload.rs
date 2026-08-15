//! What a workload is, and what the harness promises it
//!
//! A workload exists to isolate one path through the engine. That is the whole difference between
//! this and what it replaces: the `tmdb` example drove inserts and gets together against a live
//! server and reported one blended number, so a change to the read path and a change to the write
//! path moved the same figure and neither could be attributed. A workload names the path it is
//! about in its identifier, records how it was measured, and is compared only against the same
//! workload from another capture.
//!
//! # The trait is object safe on purpose
//!
//! Every workload owns its own client internally and hands back a [`Measurement`], rather than the
//! trait being generic over the database it drives. Two workloads want different schemas -
//! `insert_unsorted` wants an unsorted table and a fanout workload wants a sorted one - and a
//! generic trait would drag the twenty line `where` clause from `shoal/tests/utils.rs` onto every
//! signature that mentioned one. Erasing the database at the trait boundary costs a `Box` per run,
//! which is a rounding error against starting a server.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context as _, Result};
use shoal_core::shared::tls::TlsClientOptions;

use crate::model::macro_layer::{ConfFacts, ScaleFacts, Timing};
use crate::workloads::harness::seed::Scale;
use crate::workloads::harness::timer::Samples;
use crate::workloads::schema::BenchClient;

/// A future a workload returns, boxed so the trait stays object safe
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a workload needs before it can run
#[derive(Debug, Clone)]
pub struct WorkloadPlan {
    /// Whether this workload needs a server, and how it wants one configured
    pub server: ServerNeed,
    /// How much data to build and how hard to drive it
    pub scale: ScaleFacts,
    /// How many rows to move before sampling starts
    ///
    /// Without a warmup the first batches of a run carry connection establishment and cold
    /// partition faults, and they land in the distribution beside the steady state.
    pub warmup: u64,
}

/// Whether a workload needs a server, and what it needs of it
///
/// [`ServerNeed::None`] has no workload using it yet and is not speculative scaffolding: the
/// storage write path benchmark that `docs/src/appendix/todos.md` asks for is blocked on driving a
/// glommio executor from inside criterion, and that page already says a standalone binary emitting
/// the same JSON is an acceptable substitute. This binary is that substitute, and this arm is where
/// such a workload attaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerNeed {
    /// No server at all - the workload drives engine internals in process
    None,
    /// A server started fresh for this workload, with these overrides applied
    Fresh(ConfOverrides),
    /// A server started fresh, then stopped and started again once the workload has seeded it
    ///
    /// The only reliable way to reach the archived read path from a client. A shutdown flushes and
    /// compacts every partition, and the server that comes back up holds nothing in memory, so
    /// every read must find its partition on disk and go through
    /// [`MaybeLoaded::Accessible`](shoal::tables::partitions::MaybeLoaded) and the blocked read
    /// replay.
    ///
    /// The alternative was to squeeze `resources.memory` until the LRU evicted, which is what
    /// `shoal/tests/utils.rs::build_pressured_config` does. That is not usable here: a partition
    /// cannot be evicted until its generation has been compacted, so how much ends up on disk
    /// depends on how the run happened to interleave with compaction, and the workload would
    /// measure a different mixture of resident and archived reads every time it ran.
    RestartAfterSeed(ConfOverrides),
}

impl ServerNeed {
    /// The configuration overrides this need carries, if it needs a server at all
    pub fn overrides(&self) -> Option<&ConfOverrides> {
        match self {
            ServerNeed::None => None,
            ServerNeed::Fresh(overrides) | ServerNeed::RestartAfterSeed(overrides) => {
                Some(overrides)
            }
        }
    }

    /// Whether the server is cycled between the seed and the measurement
    pub fn restarts(&self) -> bool {
        matches!(self, ServerNeed::RestartAfterSeed(_))
    }
}

/// What a workload changes about the base configuration
///
/// Every field is optional because a workload should only state what it actually depends on. A
/// workload that pins a value it does not care about stops tracking the base configuration when
/// that changes, which is a silent way to stop measuring the thing everything else measures.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfOverrides {
    /// The number of shards to run, when the workload needs a fixed count
    ///
    /// A workload that names this gets deterministic partition placement, which is what a fanout
    /// curve needs to hold the routing constant while the key count varies.
    pub shards: Option<usize>,
    /// The memory limit to hold the shards to
    ///
    /// Set low to force eviction, which is the only way to reach the archived read path from a
    /// client. `shoal/tests/utils.rs::build_pressured_config` does the same thing.
    pub memory: Option<String>,
    /// Whether this workload's server encrypts its connections
    ///
    /// The axis the TLS half of the transport control pair varies, and nothing else. A pair
    /// differs in this and in nothing else, which is what makes the difference between them
    /// attributable to encryption — the same shape [F9](../../../docs/src/features/ephemeral-tables.md)
    /// established for storage, applied to the wire.
    pub tls: bool,
}

/// Everything a workload produced
#[derive(Debug, Default)]
pub struct Measurement {
    /// The latency samples, keyed by the operation they came from
    pub ops: BTreeMap<String, Samples>,
    /// How many rows moved, keyed by what they were
    pub counters: BTreeMap<String, u64>,
    /// The client half of each query's stage record, when this build records them
    ///
    /// Handed back with the measurement rather than pushed to a global, so a workload that fails
    /// partway cannot leave half its records behind for the next one to pick up. The server half
    /// is drained from the shards after they shut down, and the two are joined by
    /// [`crate::workloads::stages::build_report`].
    #[cfg(feature = "stage-profile")]
    pub stage_records: Vec<crate::workloads::stages::ClientRecord>,
}

impl Measurement {
    /// Records a sample against an operation, creating its sample set on first use
    ///
    /// # Arguments
    ///
    /// * `op` - Which operation this sample came from
    /// * `elapsed` - How long it took
    pub fn record(&mut self, op: &str, elapsed: std::time::Duration) {
        // an operation's set is created the first time it is sampled, so a workload never has to
        // declare up front which operations it will end up recording
        self.ops
            .entry(op.to_string())
            .or_default()
            .record(elapsed);
    }

    /// Adds to a counter, creating it on first use
    ///
    /// # Arguments
    ///
    /// * `name` - What is being counted
    /// * `by` - How much to add
    pub fn count(&mut self, name: &str, by: u64) {
        *self.counters.entry(name.to_string()).or_insert(0) += by;
    }

    /// Takes another measurement's samples and counters into this one
    ///
    /// Used to pool what several client workers each gathered.
    ///
    /// # Arguments
    ///
    /// * `other` - The measurement to drain into this one
    pub fn absorb(&mut self, mut other: Measurement) {
        // pool each operation's samples under the same name
        for (op, mut samples) in std::mem::take(&mut other.ops) {
            self.ops.entry(op).or_default().absorb(&mut samples);
        }
        // and sum each counter
        for (name, count) in other.counters {
            self.count(&name, count);
        }
        // and keep every stage record both sides gathered
        #[cfg(feature = "stage-profile")]
        self.stage_records.extend(other.stage_records);
    }
}

/// What a workload is handed when it runs
pub struct Context {
    /// The address the server for this workload is listening on
    pub addr: String,
    /// The seed every row of this run derives from
    pub seed: u64,
    /// How much data to build and how hard to drive it
    pub scale: ScaleFacts,
    /// How many rows to move before sampling starts
    pub warmup: u64,
    /// The configuration the server was actually started with
    pub conf: Option<ConfFacts>,
    /// What a client has to do to reach this workload's server, if it is encrypted
    ///
    /// Populated from the resolved config rather than from the workload, so that a workload asks
    /// for TLS in one place — its [`ConfOverrides`] — and gets a client that can reach it without
    /// naming a certificate anywhere.
    pub tls: Option<TlsClientOptions>,
}

impl Context {
    /// Open a client that can reach this workload's server
    ///
    /// Every workload builds its client through here rather than calling a constructor, so that an
    /// axis added to the server's configuration reaches every workload at once. Before this
    /// existed, adding TLS would have meant editing eleven call sites that all said
    /// `Shoal::new(&ctx.addr)`.
    pub async fn client(&self) -> Result<shoal::Shoal<BenchClient>> {
        // an unencrypted workload gets exactly the client it always got
        let options = match &self.tls {
            Some(tls) => shoal_core::client::ClientOptions::new().tls(tls.clone()),
            None => shoal_core::client::ClientOptions::new(),
        };
        shoal::Shoal::<BenchClient>::with_options(&self.addr, options)
            .await
            .context("failed to open a client")
    }
}

/// One purpose built benchmark
pub trait Workload: Send + Sync {
    /// What this workload is called
    ///
    /// This is the key every comparison joins on and the key the artifact is written under, so it
    /// must stay byte identical across captures. Renaming one orphans every capture taken before
    /// the rename. See `crate::workload_ids`.
    fn id(&self) -> &'static str;

    /// One line saying what path this workload isolates
    fn summary(&self) -> &'static str;

    /// How this workload's samples are taken
    fn timing(&self) -> Timing;

    /// Whether the instrumented layers may run this workload
    ///
    /// `hotpath` emits one profile per process, so attributing a profile to a workload means one
    /// instrumented run per workload that opts in. Every workload opting in would make the two
    /// attribution layers cost as much as the whole rest of a capture, for profiles that mostly
    /// repeat each other.
    fn profiles(&self) -> bool;

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan;

    /// Puts the data this workload reads into the server, before anything is timed
    ///
    /// **Nothing here is measured.** The wall clock and every sample belong to [`Workload::run`],
    /// so a read workload's numbers describe reading and not the writing that had to happen first.
    /// That separation is the reason this is a phase of its own rather than the first half of
    /// `run`: `insert_unsorted` reports the cost of inserting because inserting is its subject,
    /// and `get_resident` must not, even though it inserts exactly the same rows.
    ///
    /// The default seeds nothing, which is right for any workload whose subject is the write path.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        // a workload that writes what it measures has nothing to set up
        let _ = ctx;
        Box::pin(async { Ok(()) })
    }

    /// Runs this workload and hands back what it measured
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>>;
}

#[cfg(test)]
mod tests {
    use super::Measurement;
    use std::time::Duration;

    /// A measurement pools samples and counters from several workers under the same names
    #[test]
    fn absorbing_pools_both_halves() {
        let mut left = Measurement::default();
        left.record("get", Duration::from_millis(1));
        left.count("retrieved", 10);
        let mut right = Measurement::default();
        right.record("get", Duration::from_millis(3));
        right.record("insert", Duration::from_millis(5));
        right.count("retrieved", 5);
        right.count("inserted", 7);
        left.absorb(right);
        // the shared operation pooled, and the one only the other worker saw came across
        assert_eq!(left.ops["get"].len(), 2);
        assert_eq!(left.ops["insert"].len(), 1);
        // counters summed rather than being overwritten
        assert_eq!(left.counters["retrieved"], 15);
        assert_eq!(left.counters["inserted"], 7);
    }
}
