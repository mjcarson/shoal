//! One leg at one row size: the preload, the four arms, and the paced stream beside each
//!
//! A leg is a cluster bootstrapped for one row size and destroyed after it, by
//! `results/x3-lab.sh`. On it, in order:
//!
//! 1. the leaders are let settle, and 10,000 small rows are written for the paced stream;
//! 2. **preload**: three times a node's memory budget of stripe rows, keyed `0..rows`, by the
//!    spike's own closed loop, then every merge let finish. Its settled device bytes a byte stored
//!    a copy are T2's figure;
//! 3. **put**, **get**, **mix** and **overwrite**, each an arm of shoal-loadgen's own driver handed
//!    the kinds in [`crate::kinds`], with the paced stream over the small rows beside it on the
//!    same clock. The put arm's acknowledged bytes a second are T1's figure. Every arm that writes
//!    is let settle before the next, so no arm is charged another's merges;
//! 4. **alone**: the paced stream with no main load, the baseline its tail is read against.
//!
//! The arms run put, get, mix, overwrite in odd rounds and the other way in even ones. Around
//! every step the driver reads every host's device and network counters and every member's WAL
//! counters, and samples every member's memory and every host's free memory while it runs.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::eyre;
use shoal::client::SendOptions;
use shoal_loadgen::driver::{ArmClock, ArmOutcome, ArmSettings, Driver};
use shoal_loadgen::keys::KeyDistribution;
use shoal_loadgen::pick::Picker;
use shoal_loadgen::progress::Progress;
use shoal_loadgen::spec::{OnExhaust, Workload};
use shoal_loadgen::window::{OpKind, Window};
use shoaladm::deploy::remote::Host;

use crate::cluster::{now_ms, Client, CounterDelta, Lab};
use crate::kinds::{self, Query};
use crate::record::{append, Record};
use crate::StripesAsRowsClient;

/// The row sizes a leg is run at, one cluster each
pub const SIZES: [usize; 4] = [64 << 10, 256 << 10, 1 << 20, 4 << 20];

/// The bytes a closed loop keeps in flight, over every stream, before its depth is clamped
pub const IN_FLIGHT_BYTES: usize = 64 << 20;

/// How many streams the main load drives, spread over every member as today's client spreads them
pub const WORKERS: usize = 6;

/// How many small rows the paced stream reads and writes
pub const SMALL_ROWS: u64 = 10_000;

/// The rate the paced stream is offered at, operations a second over its streams
pub const PACED_RATE: f64 = 50.0;

/// The write every preloaded row's bytes are made under
pub const PRELOAD_SEED: u64 = 0x5EED_0003;

/// How many times the small rows' preload sends a row again after a failure before it gives up
const PRELOAD_TRIES: u32 = 8;

/// How long the stripes' preload keeps sending a row again after failures before it gives up
///
/// The preload has to leave every row it names written, or a get of one it gave up on is a miss
/// that measures the preload and not the read. On the lab at 256 KiB a forwarded write is
/// refused while the replication lane is judged silent
/// ([item 215](../../docs/src/appendix/known-issues.md)), so a row is sent again until it is
/// answered or this long has passed, and every try counted.
const PRELOAD_PATIENCE: Duration = Duration::from_secs(120);

/// The longest the preload waits between two tries of one row
const PRELOAD_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// The operations a closed loop keeps in flight over every stream, for a row size
///
/// About 64 MiB of rows, at least sixteen operations and at most 512.
///
/// # Arguments
///
/// * `size` - The row size
/// * `scale` - A multiple of the rule, for the quick run's check of it
#[must_use]
pub fn depth(size: usize, scale: usize) -> usize {
    (IN_FLIGHT_BYTES / size.max(1)).clamp(16, 512) * scale.max(1)
}

/// The bytes a memory setting names, as `shoal.yml` writes one (`8Gi`, `512Mi`)
///
/// # Arguments
///
/// * `raw` - The setting
#[must_use]
pub fn memory_bytes(raw: &str) -> Option<u64> {
    // a number then a binary unit
    let raw = raw.trim();
    let (number, unit) = raw.split_at(raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len()));
    let number: u64 = number.parse().ok()?;
    let shift = match unit {
        "Gi" => 30,
        "Mi" => 20,
        "Ki" => 10,
        "" => 0,
        _ => return None,
    };
    Some(number << shift)
}

/// What a leg runs against and how
pub struct Ctx {
    /// The cluster
    pub lab: Lab,
    /// The leg: `lab`, `loopback`, `titan` or `europa`
    pub leg: String,
    /// The round, which decides the order the arms run in
    pub round: u32,
    /// A tenth of every count and a few seconds a window, which proves the leg runs and measures nothing
    pub quick: bool,
    /// The file records are added to
    pub out: PathBuf,
    /// A multiple of the depth rule, for the quick run's check of it
    pub depth_scale: usize,
}

impl Ctx {
    /// A record for a side of this leg's cell at a size
    ///
    /// # Arguments
    ///
    /// * `size` - The row size
    /// * `side` - The side
    #[must_use]
    pub fn record(&self, size: usize, side: &str) -> Record {
        let mut record = Record::new(&self.leg, &format!("size={size}"), side, self.round, self.quick);
        record
            .label("depth_scale", self.depth_scale.to_string())
            .set("size", size as f64);
        record
    }

    /// Add records to the output file
    ///
    /// # Arguments
    ///
    /// * `records` - The records
    ///
    /// # Errors
    ///
    /// When the file cannot be written.
    pub fn emit(&self, records: &[Record]) -> color_eyre::Result<()> {
        append(&self.out, records)
    }

    /// The warm-up and the measured window of every arm
    #[must_use]
    pub fn windows(&self) -> (Duration, Duration) {
        if self.quick {
            (Duration::from_secs(2), Duration::from_secs(5))
        } else {
            (Duration::from_secs(10), Duration::from_secs(30))
        }
    }

    /// How long a settle may take before it is recorded as unsettled
    #[must_use]
    pub fn settle_limit(&self) -> Duration {
        Duration::from_secs(if self.quick { 120 } else { 1200 })
    }

    /// How many copies of every row the cluster keeps
    #[must_use]
    pub fn factor(&self) -> u32 {
        self.lab.inventory.replication_factor
    }

    /// The copies of a row each host holds, by its ssh target: the factor spread over the nodes
    #[must_use]
    pub fn copies_by_host(&self) -> BTreeMap<String, f64> {
        let nodes = self.lab.members.len().max(1) as f64;
        let factor = f64::from(self.factor());
        self.lab
            .hosts
            .iter()
            .map(|host| (host.target.clone(), factor * host.nodes.len() as f64 / nodes))
            .collect()
    }

    /// The bytes of stripe rows the preload writes: three times a node's memory budget
    ///
    /// # Errors
    ///
    /// When the inventory's memory does not parse.
    pub fn preload_bytes(&self) -> color_eyre::Result<u64> {
        // the smallest budget any node has, so reads past memory hold on every node
        let budget = self
            .lab
            .inventory
            .nodes
            .iter()
            .map(|spec| self.lab.inventory.resolve_resources(spec).0.memory)
            .filter_map(|raw| memory_bytes(&raw))
            .min()
            .ok_or_else(|| eyre!("no node's memory parses"))?;
        let bytes = budget * 3;
        Ok(if self.quick { bytes / 10 } else { bytes })
    }
}

/// Run one leg at one row size, adding its records to the output file
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `size` - The row size
///
/// # Errors
///
/// When the cluster cannot be read, or a step fails outright.
pub async fn run(ctx: &Ctx, size: usize) -> color_eyre::Result<()> {
    // leaders that stay put, so no arm is charged a move
    println!("{} {size}: settling the leaders", ctx.leg);
    ctx.lab
        .settle_leaders(Duration::from_secs(20), Duration::from_secs(300))
        .await?;
    let leads = ctx.lab.leads(&ctx.lab.replication().await?);
    // the paced stream's rows, which nothing measured writes
    small_preload(ctx).await?;
    // the stripe rows: T2's figure
    let rows = (ctx.preload_bytes()? / size as u64).max(1);
    let preload = preload(ctx, size, rows, &leads).await?;
    ctx.emit(&[preload])?;
    // the arms, their order reversing by round
    let mut arms = vec!["put", "get", "mix", "overwrite"];
    if ctx.round.is_multiple_of(2) {
        arms.reverse();
    }
    for side in arms {
        let record = arm(ctx, size, rows, side).await?;
        ctx.emit(&[record])?;
    }
    // and the paced stream with nothing beside it
    let alone = alone(ctx, size).await?;
    ctx.emit(&[alone])?;
    Ok(())
}

/// Write the small rows the paced stream reads and writes, once a leg
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When a row cannot be written after its tries.
async fn small_preload(ctx: &Ctx) -> color_eyre::Result<()> {
    let clients = clients(&ctx.lab);
    let next = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for worker in 0..32 {
        let clients = clients.clone();
        let next = next.clone();
        let failed = failed.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let key = next.fetch_add(1, Ordering::Relaxed);
                if key >= SMALL_ROWS {
                    break;
                }
                let client = &clients[(key as usize + worker) % clients.len()];
                if !send_with_tries(client, || kinds::small_row(key, 0).into()).await {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for task in tasks {
        task.await.map_err(|error| eyre!("a small row's writer: {error}"))?;
    }
    let failed = failed.load(Ordering::Relaxed);
    if failed > 0 {
        return Err(eyre!("{failed} small rows could not be written"));
    }
    Ok(())
}

/// Send a write until it is answered, a few times at most
///
/// # Arguments
///
/// * `client` - The member it is sent through
/// * `build` - The write, built again for each try
///
/// Returns whether it was answered.
async fn send_with_tries(client: &Client, build: impl Fn() -> Query) -> bool {
    for attempt in 0..PRELOAD_TRIES {
        if client.send_one(build()).await.is_ok() {
            return true;
        }
        // a shed write is let wait before it is sent again
        tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1))).await;
    }
    false
}

/// Write `rows` stripe rows keyed `0..rows` through a closed loop, then let every merge finish
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `size` - The row size
/// * `rows` - How many rows
/// * `leads` - How many groups each member led when the leg started
///
/// # Errors
///
/// When the cluster cannot be read.
async fn preload(ctx: &Ctx, size: usize, rows: u64, leads: &BTreeMap<String, usize>) -> color_eyre::Result<Record> {
    println!("{} {size}: preloading {rows} rows", ctx.leg);
    let mut record = ctx.record(size, "preload");
    for (member, led) in leads {
        record.set(&format!("leads:{member}"), *led as f64);
    }
    let before = ctx.lab.counters().await?;
    // the closed loop, a task an operation in flight, beside the memory sampler
    let clients = clients(&ctx.lab);
    let depth = depth(size, ctx.depth_scale);
    let next = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let retried = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let stop = AtomicBool::new(false);
    let load = async {
        let mut tasks = Vec::with_capacity(depth);
        for worker in 0..depth {
            let clients = clients.clone();
            let next = next.clone();
            let failed = failed.clone();
            let retried = retried.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let key = next.fetch_add(1, Ordering::Relaxed);
                    if key >= rows {
                        break;
                    }
                    let client = &clients[(key as usize + worker) % clients.len()];
                    // sent again, a little later each time, until answered or out of patience
                    let first = Instant::now();
                    let mut attempt = 0u32;
                    loop {
                        if client
                            .send_one(kinds::stripe_row(key, PRELOAD_SEED, size))
                            .await
                            .is_ok()
                        {
                            break;
                        }
                        if first.elapsed() > PRELOAD_PATIENCE {
                            failed.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        retried.fetch_add(1, Ordering::Relaxed);
                        attempt += 1;
                        let wait = Duration::from_millis(100 * u64::from(attempt)).min(PRELOAD_BACKOFF_MAX);
                        tokio::time::sleep(wait).await;
                    }
                }
            }));
        }
        for task in tasks {
            let _ = task.await;
        }
        stop.store(true, Ordering::Relaxed);
    };
    let (_, memory) = tokio::join!(load, sample_memory(&ctx.lab, &stop));
    let elapsed = started.elapsed();
    let at_end = ctx.lab.counters().await?;
    // every merge of what it wrote
    let (settle, settled) = ctx.lab.settle_merges(Duration::from_secs(10), ctx.settle_limit()).await?;
    let after = ctx.lab.counters().await?;
    let failed = failed.load(Ordering::Relaxed);
    let stored = (rows - failed.min(rows)) * size as u64;
    record
        .set("rows", rows as f64)
        .set("failed", failed as f64)
        .set("retried", retried.load(Ordering::Relaxed) as f64)
        .set("depth", depth as f64)
        .set("secs", elapsed.as_secs_f64())
        .set("ack_mib_s", stored as f64 / elapsed.as_secs_f64() / f64::from(1 << 20))
        .set(
            "sustained_mib_s",
            stored as f64 / (elapsed + settle).as_secs_f64() / f64::from(1 << 20),
        )
        .set("stored_bytes", stored as f64)
        .set("settle_secs", settle.as_secs_f64())
        .set("settled", if settled { 1.0 } else { 0.0 });
    amplification(ctx, &mut record, "end", &ctx.lab.delta(&before, &at_end), stored);
    amplification(ctx, &mut record, "settled", &ctx.lab.delta(&before, &after), stored);
    memory.write(&mut record);
    record.set("archive_disk_bytes", ctx.lab.archive_bytes()? as f64);
    println!(
        "{} {size}: preload {:.1} MiB/s acknowledged, {:.1} sustained, settled in {:.0}s, {} retried, {} failed",
        ctx.leg,
        record.metrics["ack_mib_s"],
        record.metrics["sustained_mib_s"],
        settle.as_secs_f64(),
        record.metrics["retried"],
        record.metrics["failed"]
    );
    Ok(record)
}

/// Run one arm of the main load with the paced stream beside it, then let it settle if it wrote
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `size` - The row size
/// * `rows` - How many rows the preload wrote
/// * `side` - `put`, `get`, `mix` or `overwrite`
///
/// # Errors
///
/// When the workload does not build or the cluster cannot be read.
async fn arm(ctx: &Ctx, size: usize, rows: u64, side: &str) -> color_eyre::Result<Record> {
    println!("{} {size}: {side}", ctx.leg);
    let mut record = ctx.record(size, side);
    // the kinds and the weights this arm draws them by
    let workload = match side {
        "put" => "put:1",
        "get" => "get:1",
        "mix" => "get:1,put:1",
        "overwrite" => "overwrite:1",
        other => return Err(eyre!("no arm is called {other}")),
    };
    let leads_before = ctx.lab.leads(&ctx.lab.replication().await?);
    let before = ctx.lab.counters().await?;
    let (warmup, window) = ctx.windows();
    let depth = depth(size, ctx.depth_scale);
    let main = Load {
        clients: clients(&ctx.lab),
        kinds: kinds::main_kinds(size, rows),
        workload: workload.to_string(),
        workers: WORKERS,
        in_flight: depth.div_ceil(WORKERS),
        pace: None,
    };
    let stop = AtomicBool::new(false);
    let (main, paced, memory) = {
        let clock = ArmClock::start(warmup + window);
        let beside = paced(ctx);
        let beside_side = format!("{side}-paced");
        let run_main = main.run(ctx, size, side, warmup, window, clock.clone());
        let run_paced = beside.run(ctx, size, &beside_side, warmup, window, clock);
        let both = async {
            let both = tokio::join!(run_main, run_paced);
            stop.store(true, Ordering::Relaxed);
            both
        };
        let ((main, paced), memory) = tokio::join!(both, sample_memory(&ctx.lab, &stop));
        (main?, paced?, memory)
    };
    let at_end = ctx.lab.counters().await?;
    // what the main load did over the measured window
    let measured = measured(&main, warmup, window);
    let secs = window.as_secs_f64();
    for kind in ["put", "get", "overwrite"] {
        let Some(stats) = measured.kinds.get(kind) else {
            continue;
        };
        let summary = stats_line(stats, secs, size);
        for (name, value) in summary {
            record.set(&format!("{kind}:{name}"), value);
        }
        errors(&mut record, kind, stats);
    }
    // the bytes stored over the whole arm, warm-up included, which is what the counters span
    let whole = Window::sum(&main.seconds);
    let stored: u64 = ["put", "overwrite"]
        .iter()
        .filter_map(|kind| whole.kinds.get(*kind))
        .map(|stats| stats.latency.len() * size as u64)
        .sum();
    let total_ok: u64 = measured.kinds.values().map(|stats| stats.latency.len()).sum();
    record
        .set("depth", depth as f64)
        .set("ack_mib_s", total_ok as f64 * size as f64 / secs / f64::from(1 << 20))
        .set("stored_bytes", stored as f64)
        .set("arm_secs", main.seconds.len() as f64)
        .set("sent_mib_s", measured.bytes_sent as f64 / secs / f64::from(1 << 20))
        .set("received_mib_s", measured.bytes_received as f64 / secs / f64::from(1 << 20));
    driver_cpu(&mut record, &main, warmup, window);
    if let Some(ended) = &main.ended_early {
        record.label("ended_early", format!("{ended:?}"));
    }
    paced_figures(&mut record, &paced, warmup, window);
    // the devices over the arm, and once its merges finish if it wrote
    amplification(ctx, &mut record, "end", &ctx.lab.delta(&before, &at_end), stored);
    if stored > 0 {
        let (settle, settled) = ctx.lab.settle_merges(Duration::from_secs(10), ctx.settle_limit()).await?;
        let after = ctx.lab.counters().await?;
        amplification(ctx, &mut record, "settled", &ctx.lab.delta(&before, &after), stored);
        record
            .set("settle_secs", settle.as_secs_f64())
            .set("settled", if settled { 1.0 } else { 0.0 })
            .set(
                "sustained_mib_s",
                stored as f64 / (warmup + window + settle).as_secs_f64() / f64::from(1 << 20),
            );
    }
    memory.write(&mut record);
    // a leader that moved during the arm is said, not hidden
    let leads_after = ctx.lab.leads(&ctx.lab.replication().await?);
    record.set("leaders_moved", if leads_before == leads_after { 0.0 } else { 1.0 });
    println!(
        "{} {size}: {side} {:.1} MiB/s, failures {}, paced p99 {:.2} ms",
        ctx.leg,
        record.metrics["ack_mib_s"],
        record.metrics.iter().filter(|(name, _)| name.ends_with(":failed")).map(|(_, v)| *v).sum::<f64>(),
        record.metrics.get("paced:small_get:p99_ms").copied().unwrap_or(0.0)
    );
    Ok(record)
}

/// The paced stream alone, with no main load: the baseline its tail is read against
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `size` - The row size of the leg it ran in
///
/// # Errors
///
/// When the workload does not build.
async fn alone(ctx: &Ctx, size: usize) -> color_eyre::Result<Record> {
    println!("{} {size}: the paced stream alone", ctx.leg);
    let mut record = ctx.record(size, "alone");
    let (warmup, window) = ctx.windows();
    let clock = ArmClock::start(warmup + window);
    let outcome = paced(ctx).run(ctx, size, "alone", warmup, window, clock).await?;
    paced_figures(&mut record, &outcome, warmup, window);
    Ok(record)
}

/// The paced stream over the small rows: half reads and half writes at a fixed rate, through
/// connections the main load never uses
///
/// # Arguments
///
/// * `ctx` - The leg
fn paced(ctx: &Ctx) -> Load {
    Load {
        // connections of its own, as another table's user would have
        clients: ctx.lab.members.iter().map(|member| member.paced.clone()).collect(),
        kinds: kinds::paced_kinds(SMALL_ROWS),
        workload: "small_get:1,small_put:1".to_string(),
        // a stream a member, so the stream reaches each as the main load does
        workers: ctx.lab.members.len().max(1),
        in_flight: 16,
        pace: Some(PACED_RATE),
    }
}

/// One driver's arm: the kinds it is handed, what it draws them by, and how hard it drives
struct Load {
    /// The members' clients it drives through
    clients: Vec<Client>,
    /// The kinds the driver is handed
    kinds: Vec<Arc<dyn shoal::shared::dataset::OperationKind<StripesAsRowsClient>>>,
    /// The workload, as weights of those kinds
    workload: String,
    /// How many streams, spread over every member
    workers: usize,
    /// How many operations a stream keeps outstanding
    in_flight: usize,
    /// The rate it is offered at, or none for a closed loop
    pace: Option<f64>,
}

impl Load {
    /// Run the arm on a clock, through every member
    ///
    /// # Arguments
    ///
    /// * `ctx` - The leg
    /// * `size` - The row size, which names the picker's stream
    /// * `side` - The side, which names it too
    /// * `warmup` - How long before it is measured
    /// * `window` - How long it is measured
    /// * `clock` - The arm's clock, shared with whatever runs beside it
    ///
    /// # Errors
    ///
    /// When the workload or its picker does not build.
    async fn run(
        &self,
        ctx: &Ctx,
        size: usize,
        side: &str,
        warmup: Duration,
        window: Duration,
        clock: Arc<ArmClock>,
    ) -> color_eyre::Result<ArmOutcome> {
        // a driver over every member, handed the kinds and no table
        let driver = Driver::new(self.clients.clone(), Vec::new(), SendOptions::new(), self.workers)
            .with_kinds(self.kinds.clone());
        let workload: Workload = self.workload.parse().map_err(|error: String| eyre!(error))?;
        // the choices, a stream of their own for every leg, size, side and round
        let stream = format!("x3/{}/{size}/{side}/r{}", ctx.leg, ctx.round);
        let picker = Picker::new_with_kinds(
            &workload,
            &[],
            &BTreeMap::new(),
            KeyDistribution::Uniform,
            1,
            0x5EED_0003 ^ u64::from(ctx.round),
            &stream,
            &driver.kind_names(),
        )
        .map_err(|error| eyre!(error))?;
        let settings = ArmSettings {
            bundle: 1,
            in_flight: self.in_flight,
            warmup,
            duration: window,
            on_exhaust: OnExhaust::End,
            // every failure is counted where it happened, never sent again
            retries: 0,
            picker,
            inserts: false,
            pace: self.pace,
        };
        Ok(driver.run_arm(&settings, clock, &Progress::none()).await)
    }
}

/// Every member's client, in member order
///
/// # Arguments
///
/// * `lab` - The cluster
fn clients(lab: &Lab) -> Vec<Client> {
    lab.members.iter().map(|member| member.client.clone()).collect()
}

/// The sum of an arm's windows over its measured seconds
///
/// # Arguments
///
/// * `outcome` - The arm
/// * `warmup` - How long before it was measured
/// * `window` - How long it was measured
fn measured(outcome: &ArmOutcome, warmup: Duration, window: Duration) -> Window {
    let from = warmup.as_secs() as usize;
    let to = (from + window.as_secs() as usize).min(outcome.seconds.len());
    Window::sum(outcome.seconds.get(from..to).unwrap_or_default())
}

/// One kind's figures over a window: rate, bytes, latency and failures
///
/// # Arguments
///
/// * `stats` - The kind's window
/// * `secs` - How long it covered
/// * `size` - The row size
fn stats_line(stats: &shoal_loadgen::window::KindWindow, secs: f64, size: usize) -> Vec<(&'static str, f64)> {
    let ok = stats.latency.len() as f64;
    let ms = |q: f64| stats.latency.value_at_quantile(q) as f64 / 1000.0;
    let mut line = vec![
        ("ops_s", ok / secs),
        ("mib_s", ok * size as f64 / secs / f64::from(1 << 20)),
        ("failed", stats.failed() as f64),
        ("misses", stats.misses as f64),
    ];
    if !stats.latency.is_empty() {
        line.extend([
            ("p50_ms", ms(0.5)),
            ("p99_ms", ms(0.99)),
            ("p999_ms", ms(0.999)),
            ("max_ms", stats.latency.max() as f64 / 1000.0),
        ]);
    }
    line
}

/// A kind's failures by code, and the first message each code came with, as labels
///
/// # Arguments
///
/// * `record` - Where they go
/// * `kind` - The kind's name in the record
/// * `stats` - Its window
fn errors(record: &mut Record, kind: &str, stats: &shoal_loadgen::window::KindWindow) {
    for (code, count) in &stats.errors {
        record.set(&format!("{kind}:error:{code}"), *count as f64);
    }
    for (code, message) in &stats.samples {
        // a message is kept short: it names the failure, and the node's log says the rest
        let message: String = message.chars().take(300).collect();
        record.label(&format!("{kind}:sample:{code}"), message);
    }
}

/// The paced stream's figures: each kind's tail over the window, and the worst second's p99
///
/// # Arguments
///
/// * `record` - Where they go
/// * `outcome` - The paced stream's arm
/// * `warmup` - How long before it was measured
/// * `window` - How long it was measured
fn paced_figures(record: &mut Record, outcome: &ArmOutcome, warmup: Duration, window: Duration) {
    let whole = measured(outcome, warmup, window);
    for (name, stats) in &whole.kinds {
        for (figure, value) in stats_line(stats, window.as_secs_f64(), 0) {
            if figure != "mib_s" {
                record.set(&format!("paced:{name}:{figure}"), value);
            }
        }
        errors(record, &format!("paced:{name}"), stats);
    }
    // the worst p99 of any measured second, which a whole window's percentile averages away
    let from = warmup.as_secs() as usize;
    let to = (from + window.as_secs() as usize).min(outcome.seconds.len());
    for kind in ["small_get", "small_put"] {
        let worst = outcome
            .seconds
            .get(from..to)
            .unwrap_or_default()
            .iter()
            .filter_map(|second| second.kind(&OpKind::Supplied(Arc::from(kind))))
            .filter(|stats| !stats.latency.is_empty())
            .map(|stats| stats.latency.value_at_quantile(0.99) as f64 / 1000.0)
            .fold(0.0, f64::max);
        record.set(&format!("paced:{kind}:worst_second_p99_ms"), worst);
    }
}

/// The driver's own cpu over the measured window, in percent of one cpu
///
/// # Arguments
///
/// * `record` - Where it goes
/// * `outcome` - The arm
/// * `warmup` - How long before it was measured
/// * `window` - How long it was measured
fn driver_cpu(record: &mut Record, outcome: &ArmOutcome, warmup: Duration, window: Duration) {
    let from = warmup.as_secs() as usize;
    let to = (from + window.as_secs() as usize).min(outcome.cpu.len());
    let cpu = outcome.cpu.get(from..to).unwrap_or_default();
    if cpu.is_empty() {
        return;
    }
    record
        .set("driver_cpu_mean", cpu.iter().sum::<f64>() / cpu.len() as f64)
        .set("driver_cpu_max", cpu.iter().copied().fold(0.0, f64::max));
}

/// A step's device and WAL bytes, and each host's bytes written for each byte stored a copy
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `record` - Where they go
/// * `when` - `end` for the step's own end, `settled` once its merges finished
/// * `delta` - What the counters did
/// * `stored` - The bytes the step stored, one copy
fn amplification(ctx: &Ctx, record: &mut Record, when: &str, delta: &CounterDelta, stored: u64) {
    record
        .set(&format!("{when}:device_written"), delta.device_written as f64)
        .set(&format!("{when}:device_read"), delta.device_read as f64)
        .set(&format!("{when}:wal_device_written"), delta.wal_device_written as f64)
        .set(&format!("{when}:archive_device_written"), delta.archive_device_written as f64)
        .set(&format!("{when}:shared_device_written"), delta.shared_device_written as f64)
        .set(&format!("{when}:wal_bytes"), delta.wal.bytes as f64)
        .set(&format!("{when}:wal_syncs"), delta.wal.syncs as f64)
        .set(&format!("{when}:flushes"), delta.flushes as f64)
        .set(&format!("{when}:nic_peak_share"), delta.nic_peak_share)
        .set(&format!("{when}:nic_tx"), delta.nic_tx as f64);
    // each member's cpu, and each device's busy time, over the step
    for (member, secs) in &delta.cpu_secs {
        record.set(&format!("{when}:cpu_secs:{member}"), *secs);
    }
    for host in &delta.hosts {
        for device in &host.devices {
            if device.secs > 0.0 {
                let role = match crate::cluster::role(&device.roots) {
                    crate::cluster::Role::Wal => "wal",
                    crate::cluster::Role::Archives => "archive",
                    crate::cluster::Role::Shared => "shared",
                };
                record.set(
                    &format!("{when}:busy_share:{}:{role}", host.host),
                    device.busy_ms as f64 / (device.secs * 1000.0),
                );
                record.set(&format!("{when}:secs:{}", host.host), device.secs);
            }
        }
    }
    let copies = f64::from(ctx.factor());
    if stored > 0 {
        // every host together, over every copy
        record
            .set(&format!("{when}:amp"), delta.device_written as f64 / (stored as f64 * copies))
            .set(&format!("{when}:amp_wal_counted"), delta.wal.bytes as f64 / (stored as f64 * copies));
    }
    // and each host over the copies it holds, its WAL device and archive device apart
    let by_host = ctx.copies_by_host();
    for host in &delta.hosts {
        let held = by_host.get(&host.host).copied().unwrap_or(0.0) * stored as f64;
        let written: u64 = host.devices.iter().map(|device| device.written_bytes).sum();
        record.set(&format!("{when}:written:{}", host.host), written as f64);
        for device in &host.devices {
            let role = match crate::cluster::role(&device.roots) {
                crate::cluster::Role::Wal => "wal",
                crate::cluster::Role::Archives => "archive",
                crate::cluster::Role::Shared => "shared",
            };
            record.set(&format!("{when}:written:{}:{role}", host.host), device.written_bytes as f64);
            record.label(&format!("device:{}:{role}", host.host), device.device.clone());
            if held > 0.0 {
                record.set(&format!("{when}:amp:{}:{role}", host.host), device.written_bytes as f64 / held);
            }
        }
        if held > 0.0 {
            record.set(&format!("{when}:amp:{}", host.host), written as f64 / held);
        }
    }
}

/// The most memory every member held, and the least every host had free, while a step ran
#[derive(Debug, Default)]
struct Memory {
    /// The peak resident set, by member
    resident: BTreeMap<String, u64>,
    /// The peak bytes of rows a member held as its eviction counts them
    rows: BTreeMap<String, u64>,
    /// The peak archive map index bytes, by member
    archive_map: BTreeMap<String, u64>,
    /// The peak WAL index bytes, by member
    wal_index: BTreeMap<String, u64>,
    /// The least memory a host had available, by its ssh target
    available: BTreeMap<String, u64>,
}

impl Memory {
    /// Put the figures in a record
    ///
    /// # Arguments
    ///
    /// * `record` - Where they go
    fn write(&self, record: &mut Record) {
        for (name, figures) in [
            ("resident_peak", &self.resident),
            ("rows_peak", &self.rows),
            ("archive_map_peak", &self.archive_map),
            ("wal_index_peak", &self.wal_index),
            ("available_min", &self.available),
        ] {
            for (whose, bytes) in figures {
                record.set(&format!("{name}:{whose}"), *bytes as f64);
            }
        }
    }
}

/// Sample every member's memory and every host's free memory every five seconds until told to stop
///
/// # Arguments
///
/// * `lab` - The cluster
/// * `stop` - Set once the step it samples ends
async fn sample_memory(lab: &Lab, stop: &AtomicBool) -> Memory {
    let mut memory = Memory::default();
    while !stop.load(Ordering::Relaxed) {
        // every member's own figures, as fresh as they come
        if let Ok(stats) = lab.node_stats(now_ms().saturating_sub(10_000)).await {
            for (member, stats) in lab.members.iter().zip(&stats) {
                let peak = |map: &mut BTreeMap<String, u64>, value: u64| {
                    let entry = map.entry(member.name.clone()).or_default();
                    *entry = (*entry).max(value);
                };
                peak(&mut memory.resident, stats.resident_bytes);
                peak(&mut memory.rows, stats.memory_bytes);
                peak(&mut memory.archive_map, stats.archive_map_bytes);
                peak(&mut memory.wal_index, stats.wal_index_bytes);
            }
        }
        // and every host's available memory, on blocking threads
        for host in &lab.hosts {
            let target = host.target.clone();
            let read = tokio::task::spawn_blocking(move || {
                Host { target }
                    .run("grep MemAvailable /proc/meminfo")
                    .ok()
                    .and_then(|line| line.split_whitespace().nth(1).and_then(|kib| kib.parse::<u64>().ok()))
            })
            .await
            .ok()
            .flatten();
            if let Some(kib) = read {
                let entry = memory.available.entry(host.target.clone()).or_insert(u64::MAX);
                *entry = (*entry).min(kib * 1024);
            }
        }
        // a step's end is seen within a second
        for _ in 0..5 {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    memory
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The depth holds about 64 MiB in flight, never fewer than sixteen operations or more than 512
    #[test]
    fn depth_follows_its_rule() {
        assert_eq!(depth(64 << 10, 1), 512);
        assert_eq!(depth(256 << 10, 1), 256);
        assert_eq!(depth(1 << 20, 1), 64);
        assert_eq!(depth(4 << 20, 1), 16);
        assert_eq!(depth(4 << 20, 2), 32);
        assert_eq!(depth(1, 1), 512);
    }

    /// A memory setting reads as the bytes it names
    #[test]
    fn memory_settings_read() {
        assert_eq!(memory_bytes("8Gi"), Some(8 << 30));
        assert_eq!(memory_bytes("512Mi"), Some(512 << 20));
        assert_eq!(memory_bytes("6Gi"), Some(6 << 30));
        assert_eq!(memory_bytes("lots"), None);
    }
}

/// What a read of every preloaded row through one member found, at one level
#[derive(Debug, Default)]
pub struct Readback {
    /// Rows answered whole
    pub found: u64,
    /// Rows answered empty, as if they did not exist
    pub empty: Vec<u64>,
    /// Reads the server answered with a failure, and the first of each
    pub failed: Vec<(u64, String)>,
    /// Rows answered with bytes other than the preload's
    pub wrong: Vec<u64>,
}

/// Read every preloaded row back through every member at a level, and say what each found
///
/// A get the driver counts as a miss is any answer that is not the row, so this tells an empty
/// answer from a failed one and both from a row whose bytes are not the preload's.
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `size` - The row size the preload wrote
/// * `level` - What the reads are served at
///
/// # Errors
///
/// When a member cannot be reached.
pub async fn readback(
    ctx: &Ctx,
    size: usize,
    level: shoal::shared::protocol::read::ReadLevel,
) -> color_eyre::Result<Vec<(String, Readback)>> {
    let rows = (ctx.preload_bytes()? / size as u64).max(1);
    let options = SendOptions::new().read(level);
    let mut all = Vec::new();
    for member in &ctx.lab.members {
        let mut found = Readback::default();
        for key in 0..rows {
            let query = crate::StripeRowGet::new(vec![key]);
            match member.client.send_one_with(query, &options).await {
                Ok(response) => {
                    if let Some(error) = response.error() {
                        found.failed.push((key, format!("{}: {}", error.code, error.msg)));
                        continue;
                    }
                    // the row's bytes, which the preload made under its seed
                    let first = response
                        .access::<crate::StripeRow>()
                        .ok()
                        .flatten()
                        .and_then(|rows| rows.first().map(|row| row.bytes.as_slice().to_vec()));
                    match first {
                        Some(bytes) if bytes == crate::bytes::make(PRELOAD_SEED, key, size) => found.found += 1,
                        Some(_) => found.wrong.push(key),
                        None => found.empty.push(key),
                    }
                }
                Err(error) => found.failed.push((key, format!("{error:?}"))),
            }
        }
        all.push((member.name.clone(), found));
    }
    Ok(all)
}
