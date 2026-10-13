//! One leg of the spike: every size, depth and path, each a cell of its own, on a cluster that is
//! up with a holder beside every node
//!
//! A leg settles the leaders, activates the wire version a conditional write needs, and runs a heat
//! cell that is recorded and never judged, so the first cell is not the first load the devices
//! meet. A 970 EVO's flush has two regimes
//! ([X6](../../docs/src/object-storage/device-store-ssd.md#two-things-about-the-labs-970-evos)), and
//! the probe before and after every cell says which it was in. Then, for each size and depth, the
//! three paths back to back, the order reversing by round. A cell:
//!
//! 1. preloads its stripe rows in a space of its own, at sequence zero;
//! 2. seals every shard's WAL with filler rows and lets every merge finish, so no cell is charged
//!    another's merges;
//! 3. probes every holder's sync floor and reads every counter;
//! 4. runs its workers for a warm-up and a window;
//! 5. waits for every apply and fold the writes left behind, and reads the counters again: the
//!    `end` figures, which every sync a write is divided by;
//! 6. lets the merges of the sealed segments it wrote finish and reads the devices once more: the
//!    `settled` figures, the device bytes a write cost once merged. The tail it left unsealed, at
//!    most a segment a shard, is the next cell's seal's and is counted by nobody;
//! 7. reads every key's last write back, its row or every holder's chunk range, and counts any
//!    that does not hold the bytes the write carried.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::{bail, eyre};
use shoal::shared::traits::PartitionKeySupport;
use tokio::sync::Semaphore;

use crate::cluster::{parse_cpu_ticks, Counters, Group, Lab, Route};
use crate::deferred::Gate;
use crate::drive::{self, Cell, Plan, Window};
use crate::keys::{self, Key, Tablets};
use crate::lane::Pool;
use crate::paths::{self, Behind, Path, Shared};
use crate::record::{append, Record};
use crate::stats::Samples;
use crate::wire::HolderStats;
use crate::{shape, Filler, StripeMeta, StripeRow};

/// The write sizes, smallest first: 4 KiB to 256 KiB, doubling
pub const SIZES: [usize; 7] = [4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10];

/// The depths: one write outstanding, and thirty-two
pub const DEPTHS: [usize; 2] = [1, 32];

/// A WAL segment's bytes, which a seal grows every shard's WAL by
pub const SEGMENT_BYTES: u64 = 10 << 20;

/// How many overwrites and syncs a probe of a holder's device takes
pub const PROBE_COUNT: u32 = 5;

/// What a leg runs against and how
pub struct Ctx {
    /// The cluster
    pub lab: Lab,
    /// Every holder's lanes, in the inventory's bootstrap order
    pub holders: Vec<Arc<Pool>>,
    /// Every holder's ssh target, in the same order
    pub holder_hosts: Vec<String>,
    /// The leg: `lab` or `loopback`
    pub leg: String,
    /// The round, which decides the order the cells run in
    pub round: u32,
    /// A few seconds a window and a few sizes, which proves the leg runs and measures nothing
    pub quick: bool,
    /// The file records are added to
    pub out: PathBuf,
    /// The member whose groups every key is aimed at, or none for every group
    pub lead: Option<String>,
    /// The sizes to run
    pub sizes: Vec<usize>,
    /// The depths to run
    pub depths: Vec<usize>,
    /// The paths to run
    pub paths: Vec<Path>,
    /// A multiple of the holders' gates, for the check that the bound does not decide a rate
    pub bound_scale: usize,
    /// How long the heat cell runs, or zero for none
    pub heat: Duration,
}

impl Ctx {
    /// A cell's warm-up and window
    #[must_use]
    pub fn plan(&self) -> Plan {
        if self.quick {
            Plan {
                warmup: Duration::from_secs(1),
                window: Duration::from_secs(3),
            }
        } else {
            Plan {
                warmup: Duration::from_secs(10),
                window: Duration::from_secs(30),
            }
        }
    }

    /// How long a settle may take before it is recorded as unsettled
    #[must_use]
    pub fn settle_limit(&self) -> Duration {
        Duration::from_secs(if self.quick { 60 } else { 600 })
    }

    /// Whether this round runs its cells in reverse, as every even round does
    #[must_use]
    pub fn reversed(&self) -> bool {
        self.round % 2 == 0
    }

    /// A record of one side of a cell of this leg
    ///
    /// # Arguments
    ///
    /// * `size` - The write's size
    /// * `depth` - The cell's depth
    /// * `side` - The path, or `heat`
    #[must_use]
    pub fn record(&self, size: usize, depth: usize, side: &str) -> Record {
        let mut record = Record::new(&self.leg, &cell_name(size, depth), side, self.round, self.quick);
        record
            .set("size", size as f64)
            .set("depth", depth as f64)
            .label("bound_scale", self.bound_scale.to_string());
        record
    }

    /// Add records to the output file, and print a line for each
    ///
    /// # Arguments
    ///
    /// * `records` - The records
    ///
    /// # Errors
    ///
    /// When the file cannot be written.
    pub fn emit(&self, records: &[Record]) -> color_eyre::Result<()> {
        for record in records {
            // one line a record, so a run can be followed
            let figure = |name: &str| record.metrics.get(name).copied().unwrap_or(0.0);
            println!(
                "{} | {} | {} | {:.0}/s p50 {:.0} µs p99 {:.0} µs | syncs a write {:.2} | wait p50 {:.0} µs | drain {:.0} ms{}",
                record.measurement,
                record.cell,
                record.side,
                figure("per_sec"),
                figure("p50_us"),
                figure("p99_us"),
                figure("syncs_per_write"),
                figure("wait_p50_us"),
                figure("drain_ms"),
                match record.labels.get("error") {
                    Some(error) => format!(" | {error}"),
                    None => String::new(),
                }
            );
        }
        append(&self.out, records)
    }
}

/// A cell's name in a record
///
/// # Arguments
///
/// * `size` - The write's size
/// * `depth` - The cell's depth
#[must_use]
pub fn cell_name(size: usize, depth: usize) -> String {
    format!("size={size} depth={depth}")
}

/// The groups and routes a leg aims at, a table each
pub struct Aim {
    /// Which member leads each tablet of the rows' table
    pub rows: Arc<Route>,
    /// Which member leads each tablet of the stripes' table
    pub stripes: Arc<Route>,
    /// Which member leads each tablet of the filler's table
    pub filler: Arc<Route>,
    /// The tablets row keys are chosen in
    pub row_tablets: Arc<Tablets>,
    /// The tablets stripe keys are chosen in
    pub stripe_tablets: Arc<Tablets>,
}

impl Aim {
    /// Aim at the groups one member leads, or at every group
    ///
    /// # Arguments
    ///
    /// * `lab` - The cluster
    /// * `groups` - Every table's groups, as the leaders settled
    /// * `lead` - The member whose groups keys are chosen in, or none for every group
    ///
    /// # Errors
    ///
    /// When the member is not one of the cluster's or leads no group of a table.
    pub fn of(
        lab: &Lab,
        groups: &std::collections::BTreeMap<String, Vec<Group>>,
        lead: Option<&str>,
    ) -> color_eyre::Result<Aim> {
        let table = |name: &str| groups.get(name).cloned().unwrap_or_default();
        let rows = table(<StripeRow as PartitionKeySupport>::name());
        let stripes = table(<StripeMeta as PartitionKeySupport>::name());
        let filler = table(<Filler as PartitionKeySupport>::name());
        // the tablets of the groups the member leads, or every tablet
        let tablets = |groups: &[Group]| -> color_eyre::Result<Arc<Tablets>> {
            let Some(name) = lead else {
                return Ok(Arc::new(Tablets::all()));
            };
            let member = lab
                .members
                .iter()
                .position(|member| member.name == name)
                .ok_or_else(|| eyre!("{name} is not a member"))?;
            let led: Vec<u16> = groups
                .iter()
                .filter(|group| group.leader == member)
                .flat_map(|group| group.tablets.iter().copied())
                .collect();
            if led.is_empty() {
                bail!("{name} leads no group of {}", groups.first().map_or("?", |group| group.table.as_str()));
            }
            Ok(Arc::new(Tablets::of(&led)))
        };
        Ok(Aim {
            row_tablets: tablets(&rows)?,
            stripe_tablets: tablets(&stripes)?,
            rows: Arc::new(Route::of(&rows)),
            stripes: Arc::new(Route::of(&stripes)),
            filler: Arc::new(Route::of(&filler)),
        })
    }
}

/// Run one leg: the heat cell, then every size, depth and path
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When the cluster or a holder cannot be read, or a cell fails outright.
pub async fn run(ctx: &Ctx) -> color_eyre::Result<()> {
    // leaders that stay put, and the wire version a conditional commit needs
    println!("{}: settling the leaders", ctx.leg);
    let tables = [
        <StripeRow as PartitionKeySupport>::name(),
        <StripeMeta as PartitionKeySupport>::name(),
        <Filler as PartitionKeySupport>::name(),
    ];
    let groups = ctx
        .lab
        .settle_leaders(&tables, Duration::from_secs(20), Duration::from_secs(300))
        .await?;
    ctx.lab.activate().await?;
    let aim = Aim::of(&ctx.lab, &groups, ctx.lead.as_deref())?;
    // the heat cell: B at its smallest and deepest, recorded and never judged
    if !ctx.heat.is_zero() {
        println!("{}: heat for {:?}", ctx.leg, ctx.heat);
        let plan = Plan {
            warmup: Duration::ZERO,
            window: ctx.heat,
        };
        let record = cell(ctx, &aim, SIZES[0], 32, Path::Staged, "heat", plan).await?;
        ctx.emit(&[record])?;
    }
    // every size and depth, the paths of each back to back, the order reversing by round
    let mut sizes = ctx.sizes.clone();
    let mut depths = ctx.depths.clone();
    let mut order = ctx.paths.clone();
    if ctx.reversed() {
        sizes.reverse();
        depths.reverse();
        order.reverse();
    }
    for size in &sizes {
        for depth in &depths {
            for path in &order {
                let record = cell(ctx, &aim, *size, *depth, *path, path.name(), ctx.plan()).await?;
                ctx.emit(&[record])?;
            }
        }
    }
    Ok(())
}

/// Everything a cell is read against, taken at once
struct Snap {
    /// The cluster's devices, network, WAL and members' cpu
    counters: Counters,
    /// Every holder's counters, in the holders' order
    holders: Vec<HolderStats>,
    /// The driver's own cpu, in clock ticks
    driver_ticks: u64,
}

/// Take a snapshot of every counter a cell is read against
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When a host, a member or a holder cannot be read.
async fn snap(ctx: &Ctx) -> color_eyre::Result<Snap> {
    let counters = ctx.lab.counters().await?;
    let mut holders = Vec::with_capacity(ctx.holders.len());
    for pool in &ctx.holders {
        holders.push(pool.stats().await?);
    }
    Ok(Snap {
        counters,
        holders,
        driver_ticks: driver_ticks(),
    })
}

/// The driver's own cpu so far, in clock ticks
#[must_use]
pub fn driver_ticks() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|stat| parse_cpu_ticks(&stat))
        .unwrap_or(0)
}

/// Every holder's sync floor now, in microseconds, in the holders' order
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When a holder cannot be asked.
async fn probes(ctx: &Ctx) -> color_eyre::Result<Vec<f64>> {
    let mut floors = Vec::with_capacity(ctx.holders.len());
    for pool in &ctx.holders {
        floors.push(pool.probe(PROBE_COUNT).await?.p50_ns as f64 / 1e3);
    }
    Ok(floors)
}

/// Write each stripe key's row at sequence zero, through its leader, a depth's worth at once
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `aim` - The routes
/// * `keys` - The keys
///
/// # Errors
///
/// When an insert fails three times.
async fn preload(ctx: &Ctx, aim: &Aim, keys: &[Key]) -> color_eyre::Result<()> {
    let clients: Vec<_> = ctx.lab.members.iter().map(|member| member.client.clone()).collect();
    let limit = Arc::new(Semaphore::new(32));
    let mut tasks = Vec::with_capacity(keys.len());
    for key in keys {
        let client = clients[aim.stripes.leader_of_hash(keys::stripe_hash(&key.stripe))].clone();
        let permit = limit.clone().acquire_owned().await?;
        let stripe = key.stripe;
        tasks.push(tokio::spawn(async move {
            // an insert replaces whatever is there, so one sent again is safe
            let mut last = None;
            for _ in 0..3 {
                match client.send_one(shape::stripe_row(stripe, 0)).await {
                    Ok(_) => {
                        last = None;
                        break;
                    }
                    Err(error) => last = Some(format!("{error:?}")),
                }
            }
            drop(permit);
            last
        }));
    }
    for task in tasks {
        if let Some(error) = task.await? {
            bail!("a preload insert failed three times: {error}");
        }
    }
    Ok(())
}

/// Write filler rows until every shard's WAL has grown by a segment, then let the merges finish
///
/// A segment is handed to a compactor once it is sealed, so whatever the last cell left in an
/// open segment would otherwise be merged inside this one.
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `aim` - The routes
/// * `cell` - The cell's number, which the filler's keys start from
///
/// Returns how long the seal and the settle took, and whether it settled.
///
/// # Errors
///
/// When the cluster cannot be read or the filler cannot be written.
async fn seal(ctx: &Ctx, aim: &Aim, cell: u64) -> color_eyre::Result<(Duration, bool)> {
    let started = Instant::now();
    let clients: Vec<_> = ctx.lab.members.iter().map(|member| member.client.clone()).collect();
    let before = Lab::wal_bytes_by_shard(&ctx.lab.replication().await?);
    let mut next = cell << 32;
    loop {
        // a batch of filler rows, thirty-two at once, through their leaders
        let mut tasks = Vec::with_capacity(64);
        for _ in 0..64 {
            next += 1;
            let key = next;
            let row = shape::filler(key);
            let client = clients[aim.filler.leader_of_hash(row.get_partition_key())].clone();
            tasks.push(tokio::spawn(async move {
                // an insert replaces whatever is there, so one sent again is safe
                let mut last = None;
                for _ in 0..3 {
                    match client.send_one(row.clone()).await {
                        Ok(_) => return Ok(()),
                        Err(error) => last = Some(error),
                    }
                }
                Err(last.expect("three tries failed"))
            }));
        }
        for task in tasks {
            if let Err(error) = task.await? {
                bail!("a filler row failed three times: {error:?}");
            }
        }
        // done once every shard's WAL has grown by a segment
        let now = Lab::wal_bytes_by_shard(&ctx.lab.replication().await?);
        let grown = before
            .iter()
            .all(|(shard, bytes)| now.get(shard).copied().unwrap_or(0).saturating_sub(*bytes) >= SEGMENT_BYTES);
        if grown {
            break;
        }
        if started.elapsed() > ctx.settle_limit() {
            bail!("the filler did not grow every shard's WAL by a segment in {:?}", ctx.settle_limit());
        }
    }
    // and every merge the sealed segments asked for
    let (_, settled) = ctx
        .lab
        .settle_merges(Duration::from_secs(5), ctx.settle_limit())
        .await?;
    Ok((started.elapsed(), settled))
}

/// Run one cell and record it
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `aim` - The routes and the tablets keys are chosen in
/// * `size` - The write's size
/// * `depth` - How many writes are in flight
/// * `path` - The path every write takes
/// * `side` - The record's side: the path's name, or `heat`
/// * `plan` - The warm-up and the window
///
/// # Errors
///
/// When the cluster or a holder cannot be read.
async fn cell(
    ctx: &Ctx,
    aim: &Aim,
    size: usize,
    depth: usize,
    path: Path,
    side: &str,
    plan: Plan,
) -> color_eyre::Result<Record> {
    let mut record = ctx.record(size, depth, side);
    // the cell's keys, in a space no other cell uses
    let index = if side == "heat" { 3 } else { path.index() };
    let space = keys::space(ctx.round, size, depth, index);
    let hands = keys::hands(space, depth, &aim.stripe_tablets, &aim.row_tablets);
    if path.touches_holders() {
        let stripes: Vec<Key> = hands.iter().flatten().copied().collect();
        preload(ctx, aim, &stripes).await?;
    }
    // the last cell's tail sealed and merged, then everything read
    let (sealed, sealed_alike) = seal(ctx, aim, space).await?;
    record
        .set("seal_secs", sealed.as_secs_f64())
        .set("seal_settled", f64::from(u8::from(sealed_alike)));
    let probe_before = probes(ctx).await?;
    let leaders_before = ctx.lab.leaders().await?;
    let before = snap(ctx).await?;
    // the workers, every one sharing the cell's gates and routes
    let window = Window::of(plan);
    let gate = Gate::new(ctx.holders.len(), 2 * depth * ctx.bound_scale.max(1));
    let behind = Behind::new(ctx.holders.len());
    let shared = Arc::new(Shared {
        clients: Arc::new(ctx.lab.members.iter().map(|member| member.client.clone()).collect()),
        rows: aim.rows.clone(),
        stripes: aim.stripes.clone(),
        holders: ctx.holders.clone(),
        gate: gate.clone(),
        window,
        behind: behind.clone(),
        size,
    });
    let (workers, states) = paths::workers(path, &hands, &shared);
    let driven = drive::run(window, workers).await;
    // every apply and fold the writes left behind, then the counters every write is divided by
    let drain = gate.drain().await;
    let end = snap(ctx).await?;
    // the merges of the segments the cell sealed, then the devices once more
    let (settling, settled) = ctx
        .lab
        .settle_merges(Duration::from_secs(5), ctx.settle_limit())
        .await?;
    let after = snap(ctx).await?;
    let probe_after = probes(ctx).await?;
    let leaders_after = ctx.lab.leaders().await?;
    // every key's last write read back, from the rows or from every holder's chunk, once
    // nothing it reads is counted
    let readback = paths::read_back(path, &shared, &states).await;
    record
        .set("readback_checked", readback.checked as f64)
        .set("readback_wrong", readback.wrong as f64);
    if let Some(error) = readback.errors.first() {
        record.label("readback_error", error.clone());
    }
    // what the driver saw, then what it cost
    fill_driver(&mut record, &driven, &behind, drain);
    fill_costs(ctx, &mut record, &driven, size, &before, &end, &after);
    for (at, (first, second)) in probe_before.iter().zip(&probe_after).enumerate() {
        let host = &ctx.holders[at].name;
        record
            .set(&format!("probe_before_us:{host}"), *first)
            .set(&format!("probe_after_us:{host}"), *second);
    }
    record
        .set("settle_secs", settling.as_secs_f64())
        .set("settled", f64::from(u8::from(settled)))
        .set("leaders_moved", f64::from(u8::from(leaders_before != leaders_after)));
    Ok(record)
}

/// A set of latencies' median and 99th percentile under a name
///
/// # Arguments
///
/// * `record` - The record
/// * `name` - The figures' stem
/// * `samples` - The latencies
fn percentiles(record: &mut Record, name: &str, samples: &Samples) {
    if samples.is_empty() {
        return;
    }
    let summary = samples.summary();
    record
        .set(&format!("{name}_p50_us"), summary.p50)
        .set(&format!("{name}_p99_us"), summary.p99);
}

/// Fill a record with what the driver saw: rates, latencies and their parts, and the waits
///
/// # Arguments
///
/// * `record` - The record
/// * `driven` - The cell
/// * `behind` - What the work writes left behind did
/// * `drain` - How long the drain after the window took
fn fill_driver(record: &mut Record, driven: &Cell, behind: &Behind, drain: Duration) {
    let latency = driven.latency.summary();
    let wait = driven.wait.summary();
    record
        .set("per_sec", driven.per_sec())
        .set("applied", driven.applied as f64)
        .set("refused", driven.refused as f64)
        .set("failed", driven.failed as f64)
        .set("all_completed", driven.all_completed as f64)
        .set("secs", driven.secs)
        .set("p50_us", latency.p50)
        .set("p99_us", latency.p99)
        .set("p999_us", latency.p999)
        .set("max_us", latency.max)
        .set("mean_us", latency.mean)
        .set("wait_p50_us", wait.p50)
        .set("wait_p99_us", wait.p99)
        .set("wait_mean_us", wait.mean)
        .set("drain_ms", drain.as_secs_f64() * 1e3)
        .set("behind_failed", behind.failed.load(Ordering::Relaxed) as f64)
        .set("drift", behind.drift.load(Ordering::Relaxed) as f64);
    percentiles(record, "read", &driven.read);
    percentiles(record, "stage", &driven.stage);
    percentiles(record, "commit", &driven.commit);
    percentiles(record, "third", &behind.third.lock().expect("not poisoned"));
    percentiles(record, "deferred", &behind.deferred.lock().expect("not poisoned"));
    for (holder, samples) in behind.stage.iter().enumerate() {
        percentiles(record, &format!("holder{holder}_stage"), &samples.lock().expect("not poisoned"));
    }
    // the first failure the driver or the work behind it saw, by what was said
    let errors = behind.errors.lock().expect("not poisoned");
    if let Some(error) = driven.errors.first().or(errors.first()) {
        record.label("error", error.clone());
    }
}

/// Fill a record with what the cell cost: syncs, device bytes, network and cpu, a write each
///
/// Every write the cell completed is what a figure is divided by, warm-up and tail included,
/// since the counters around it see all of them.
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `record` - The record
/// * `driven` - The cell
/// * `size` - The write's size
/// * `before` - Before the cell
/// * `end` - Once every write's deferred work was durable
/// * `after` - Once the merges of the cell's sealed segments finished
fn fill_costs(ctx: &Ctx, record: &mut Record, driven: &Cell, size: usize, before: &Snap, end: &Snap, after: &Snap) {
    let writes = driven.all_completed.max(1) as f64;
    let payload = writes * size as f64;
    // the WAL's syncs and bytes, every shard of every member, to the end of the drain
    let to_end = ctx.lab.delta(&before.counters, &end.counters);
    let wal_syncs = to_end.wal.syncs as f64;
    record
        .set("wal_syncs_per_write", wal_syncs / writes)
        .set("wal_bytes_per_write", to_end.wal.bytes as f64 / writes)
        .set(
            "wal_appends_per_sync",
            if wal_syncs > 0.0 { to_end.wal.appends as f64 / wal_syncs } else { 0.0 },
        );
    // every holder's syncs and bytes, together and each
    let mut holder_syncs = 0.0;
    let mut sums = HolderStats::default();
    for (at, (first, second)) in before.holders.iter().zip(&end.holders).enumerate() {
        let did = second.since(first);
        let host = &ctx.holders[at].name;
        record
            .set(
                &format!("holder_cpu_us_per_write:{host}"),
                did.exec_cpu_ns as f64 / 1e3 / writes,
            )
            .set(&format!("holder_staged_left:{host}"), second.staged_now as f64);
        sums.stages += did.stages;
        sums.stage_syncs += did.stage_syncs;
        sums.stage_records += did.stage_records;
        sums.stage_bytes += did.stage_bytes;
        sums.applies += did.applies;
        sums.apply_syncs += did.apply_syncs;
        sums.folds += did.folds;
        sums.fold_syncs += did.fold_syncs;
        sums.chunk_bytes += did.chunk_bytes;
        sums.apply_missing += did.apply_missing;
        sums.in_place_syncs += did.in_place_syncs;
        sums.batched = sums.batched.max(did.batched);
        holder_syncs += (did.stage_syncs + did.apply_syncs + did.fold_syncs + did.in_place_syncs) as f64;
        record.set(&format!("ktls:{host}"), second.ktls as f64);
    }
    record
        .set("stage_syncs_per_write", sums.stage_syncs as f64 / writes)
        .set(
            "stage_records_per_sync",
            if sums.stage_syncs > 0 { sums.stage_records as f64 / sums.stage_syncs as f64 } else { 0.0 },
        )
        .set("apply_syncs_per_write", sums.apply_syncs as f64 / writes)
        .set("fold_syncs_per_write", sums.fold_syncs as f64 / writes)
        .set("in_place_syncs_per_write", sums.in_place_syncs as f64 / writes)
        .set("in_place_batched", sums.batched as f64)
        .set("holder_syncs_per_write", holder_syncs / writes)
        .set("syncs_per_write", (wal_syncs + holder_syncs) / writes)
        .set("stages_per_write", sums.stages as f64 / writes)
        .set("applies_per_write", sums.applies as f64 / writes)
        .set("folds_per_write", sums.folds as f64 / writes)
        .set("apply_missing", sums.apply_missing as f64)
        .set("holder_bytes_per_byte", (sums.stage_bytes + sums.chunk_bytes) as f64 / payload);
    // the devices, to the end of the drain and once the merges finished
    for (when, delta) in [("end", &to_end), ("settled", &ctx.lab.delta(&before.counters, &after.counters))] {
        record
            .set(&format!("{when}:flushes_per_write"), delta.flushes as f64 / writes)
            .set(&format!("{when}:device_written_per_byte"), delta.device_written as f64 / payload)
            .set(&format!("{when}:device_read_per_byte"), delta.device_read as f64 / payload);
        for (host, flushes) in &delta.flushes_by_host {
            record.set(&format!("{when}:flushes_per_write:{host}"), *flushes as f64 / writes);
        }
        for (host, written) in &delta.written_by_host {
            record.set(&format!("{when}:device_written_per_byte:{host}"), *written as f64 / payload);
        }
    }
    // the network and every process's cpu, to the end of the drain
    let secs = end.counters.at.duration_since(before.counters.at).as_secs_f64();
    record
        .set("span_secs", secs)
        .set("nic_peak_share", to_end.nic_peak_share)
        .set("nic_tx_per_write", to_end.nic_tx as f64 / writes)
        .set(
            "driver_cpu_us_per_write",
            end.driver_ticks.saturating_sub(before.driver_ticks) as f64 * 1e4 / writes,
        );
    for (member, cpu) in &to_end.cpu_secs {
        record.set(&format!("cpu_us_per_write:{member}"), cpu * 1e6 / writes);
    }
}
