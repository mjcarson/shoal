//! X7: a read and a stage while applies run, on one disk's arm
//!
//! S13 puts a slice's work in an order: reads and stages for the foreground first, then applies
//! of committed writes, batched, and on a rotational disk in offset order, and says a foreground
//! read waits for no more than one batch. This puts that to the disk. One executor, as one slice
//! is, applies 64 KiB units and their headers in place into a population of chunks spread over
//! the disk, a batch at a time, then syncs every chunk the batch touched at once. Beside it, open
//! loops issue a 64 KiB read of a random chunk and a 4 KiB journal record, on the disk and on the
//! host's SSD, each counted from its slot. The applies are offered at half of what the disk takes
//! with a whole batch in flight, measured first, and issued five ways:
//!
//! - `idle`: no applies, the foreground alone
//! - `arrival-qd1`: one apply at a time, as they arrived
//! - `offset-qd1`: one at a time, sorted by where their bytes lie on the disk (FIEMAP), which is
//!   the order S13 asks for
//! - `offset-ino`: one at a time, sorted by inode and offset, which a store can do without asking
//!   the filesystem where anything is
//! - `kernel`: the whole batch in flight, ordered by the block layer's scheduler and the disk's
//!   own queue
//!
//! H1 reads the disk's stage under applies; H3 reads the foreground read against a batch.

use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::DmaFile;

use super::arm::{self, Arm, CHUNK};
use super::counters::Devices;
use super::io::{self, Payloads, HEADER};
use super::journal::{Committer, Ring};
use super::paced::{open_loop, until, Paced, Window};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// Batches of applies the cells run
const BATCHES: &[usize] = &[32, 128];

/// Chunks in the population
const POPULATION: usize = 1024;

/// An apply's unit, and a foreground read's
pub const UNIT: u64 = 64 << 10;

/// A journal's ring
const RING: u64 = 256 << 20;

/// Foreground reads a second
const READ_RATE: f64 = 20.0;

/// Foreground stages a second, on each journal
const STAGE_RATE: f64 = 20.0;

/// A stage's payload, after its header block
const RECORD: u64 = 4 << 10;

/// How a batch of applies is issued
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// No applies
    Idle,
    /// One at a time, as they arrived
    ArrivalQd1,
    /// One at a time, by where their bytes lie on the disk
    OffsetQd1,
    /// One at a time, by inode and offset
    OffsetIno,
    /// The whole batch in flight
    Kernel,
}

impl Order {
    /// Its name
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Order::Idle => "idle",
            Order::ArrivalQd1 => "arrival-qd1",
            Order::OffsetQd1 => "offset-qd1",
            Order::OffsetIno => "offset-ino",
            Order::Kernel => "kernel",
        }
    }
}

/// A batch of applies: each a chunk and a unit's offset in it
///
/// # Arguments
///
/// * `rng` - The generator
/// * `chunks` - Chunks in the population
/// * `batch` - Applies in the batch
#[must_use]
pub fn draw(rng: &mut Rng, chunks: usize, batch: usize) -> Vec<(usize, u64)> {
    (0..batch)
        .map(|_| (rng.below(chunks as u64) as usize, HEADER + rng.below(CHUNK / UNIT) * UNIT))
        .collect()
}

/// Put a batch in the order a side issues it
///
/// # Arguments
///
/// * `arm` - The population, with where each chunk lies
/// * `items` - The batch
/// * `order` - The order
pub fn arrange(arm: &Arm, items: &mut [(usize, u64)], order: Order) {
    match order {
        Order::OffsetQd1 => items.sort_by_key(|&(chunk, offset)| arm.physical(chunk, offset)),
        Order::OffsetIno => items.sort_by_key(|&(chunk, offset)| (arm.inodes[chunk], offset)),
        _ => {}
    }
}

/// Apply a batch: each unit and its chunk's header written in place, in the side's order, then
/// every chunk the batch touched synced at once
///
/// # Arguments
///
/// * `arm` - The population
/// * `payloads` - The bytes
/// * `items` - The batch, already in its order
/// * `order` - How it is issued
pub async fn apply(arm: &Arm, payloads: &Payloads, items: &[(usize, u64)], order: Order) {
    // one apply: the unit and the header together, as S6's apply writes them
    let one = |chunk: usize, offset: u64| {
        let file: &DmaFile = &arm.files[chunk];
        async move {
            futures::join!(
                io::write_body(file, payloads, UNIT, offset),
                io::write_body(file, payloads, HEADER, 0)
            );
        }
    };
    if order == Order::Kernel {
        join_all(items.iter().map(|&(chunk, offset)| one(chunk, offset))).await;
    } else {
        for &(chunk, offset) in items {
            one(chunk, offset).await;
        }
    }
    // each chunk touched once, every sync in flight together so the block layer may merge them
    let mut touched: Vec<usize> = items.iter().map(|&(chunk, _)| chunk).collect();
    touched.sort_unstable();
    touched.dedup();
    join_all(touched.iter().map(|&chunk| arm.files[chunk].fdatasync())).await
        .into_iter()
        .for_each(|synced| synced.expect("applied"));
}

/// What an applier saw
#[derive(Debug, Default)]
pub struct Applied {
    /// Applies done in the counted window
    pub applies: usize,
    /// Each counted batch's time, from its start to its last sync
    pub batches: Samples,
    /// Batches that started more than a millisecond after they were due
    pub late: usize,
}

/// Apply batches at a rate through a window, or as fast as they go if `rate` is `None`
///
/// # Arguments
///
/// * `arm` - The population
/// * `payloads` - The bytes
/// * `batch` - Applies in a batch
/// * `order` - How a batch is issued
/// * `rate` - Applies a second offered, or `None` for a closed loop
/// * `window` - The cell's window
/// * `seed` - The seed of the batches
pub async fn applier(
    arm: Rc<Arm>,
    payloads: Rc<Payloads>,
    batch: usize,
    order: Order,
    rate: Option<f64>,
    window: Window,
    seed: u64,
) -> Applied {
    let mut rng = Rng::new(seed);
    let mut seen = Applied::default();
    let mut nth = 0_u64;
    loop {
        // a batch is due at its slot, or at once in a closed loop
        let due = match rate {
            Some(rate) => window.start + Duration::from_secs_f64(nth as f64 * batch as f64 / rate),
            None => Instant::now().max(window.start),
        };
        if due >= window.end {
            break;
        }
        until(due).await;
        let begun = Instant::now();
        if begun > due + Duration::from_millis(1) {
            seen.late += 1;
        }
        let mut items = draw(&mut rng, arm.files.len(), batch);
        arrange(&arm, &mut items, order);
        apply(&arm, &payloads, &items, order).await;
        let ended = Instant::now();
        if window.counts(begun, ended) {
            seen.applies += batch;
            seen.batches.push(ended - begun);
        }
        nth += 1;
    }
    seen
}

/// A foreground read of a random unit of a random chunk, the file held open
///
/// # Arguments
///
/// * `arm` - The population
/// * `nth` - The read's number, which seeds its place
pub async fn read_one(arm: Rc<Arm>, nth: u64) {
    let mut rng = Rng::new(nth ^ 0x7ead_0000);
    let chunk = rng.below(arm.files.len() as u64) as usize;
    let offset = HEADER + rng.below(CHUNK / UNIT) * UNIT;
    arm.files[chunk].read_at_aligned(offset, UNIT as usize).await.expect("read");
}

/// A journal: its file, where its records go, and its committer
pub struct Journal {
    /// The file, written ahead
    pub file: Rc<DmaFile>,
    /// The ring of places
    pub ring: Ring,
    /// The group commit
    pub committer: Rc<Committer>,
}

impl Journal {
    /// A journal written ahead in a directory, its committer started
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory
    /// * `len` - The ring's length
    pub async fn make(dir: &std::path::Path, len: u64) -> Journal {
        std::fs::create_dir_all(dir).expect("made");
        let file = Rc::new(io::open(&dir.join("journal"), true).await);
        io::zero_fill(&file, len).await;
        Journal { file: file.clone(), ring: Ring::wrapping(len), committer: Committer::start(file) }
    }

    /// Stage one record and wait until it is durable
    ///
    /// # Arguments
    ///
    /// * `payloads` - The bytes
    pub async fn stage(&self, payloads: &Payloads) {
        let len = HEADER + RECORD;
        let at = self.ring.take(len).expect("the ring wraps");
        self.file.write_rc_at(payloads.get(len), at).await.expect("staged");
        self.committer.durable().await;
    }

    /// Stop the committer and close the file
    pub async fn close(self) {
        self.committer.stop().await;
        drop(self.committer);
        if let Ok(file) = Rc::try_unwrap(self.file) {
            file.close().await.expect("closed");
        }
    }
}

/// The figures of an open loop, under a prefix
///
/// # Arguments
///
/// * `prefix` - The figures' prefix
/// * `paced` - What the loop saw
#[must_use]
pub fn paced_figures(prefix: &str, paced: &Paced) -> Vec<(String, f64)> {
    let summary = paced.samples.summary();
    vec![
        (format!("{prefix}_n"), summary.n as f64),
        (format!("{prefix}_due"), paced.due as f64),
        (format!("{prefix}_in_flight_max"), paced.most_in_flight as f64),
        (format!("{prefix}_p50"), summary.p50),
        (format!("{prefix}_p99"), summary.tail()),
        (format!("{prefix}_p999"), summary.p999),
        (format!("{prefix}_max"), summary.max),
    ]
}

/// Everything one side needs, held for the round
struct Rig {
    /// The population
    arm: Rc<Arm>,
    /// The disk's journal
    disk: Rc<Journal>,
    /// The SSD's, if there is one
    ssd: Option<Rc<Journal>>,
    /// The bytes
    payloads: Rc<Payloads>,
}

/// Run one side
///
/// # Arguments
///
/// * `rig` - What it needs
/// * `order` - How applies are issued
/// * `batch` - Applies in a batch
/// * `offered` - Applies a second offered
/// * `window` - The side's window
/// * `devices` - The disk's devices
/// * `seed` - The seed of its batches
async fn side(rig: &Rig, order: Order, batch: usize, offered: f64, window: Window, devices: Devices, seed: u64) -> SideOut {
    // the disk at the window's edges
    let edges = glommio::spawn_local(async move {
        until(window.warm).await;
        let before = devices.snap();
        until(window.end).await;
        before.delta(&devices.snap())
    });
    let applier = (order != Order::Idle).then(|| {
        glommio::spawn_local(applier(rig.arm.clone(), rig.payloads.clone(), batch, order, Some(offered), window, seed))
    });
    let reads = {
        let arm = rig.arm.clone();
        glommio::spawn_local(open_loop(READ_RATE, window, move |nth| read_one(arm.clone(), nth)))
    };
    let stage = |journal: Rc<Journal>| {
        let payloads = rig.payloads.clone();
        glommio::spawn_local(open_loop(STAGE_RATE, window, move |_| {
            let (journal, payloads) = (journal.clone(), payloads.clone());
            async move { journal.stage(&payloads).await }
        }))
    };
    let disk_stages = stage(rig.disk.clone());
    let ssd_stages = rig.ssd.clone().map(stage);
    let reads = reads.await;
    let disk_stages = disk_stages.await;
    let ssd_stages = match ssd_stages {
        Some(task) => Some(task.await),
        None => None,
    };
    let applied = match applier {
        Some(task) => task.await,
        None => Applied::default(),
    };
    let delta = edges.await;
    let batches = applied.batches.summary();
    let mut figures: Vec<(String, f64)> = vec![
        ("offered_s".into(), if order == Order::Idle { 0.0 } else { offered }),
        ("applies_s".into(), applied.applies as f64 / window.secs()),
        ("behind".into(), if order != Order::Idle && (applied.applies as f64 / window.secs()) < 0.95 * offered { 1.0 } else { 0.0 }),
        ("late_batches".into(), applied.late as f64),
        ("batch_p50".into(), batches.p50),
        ("batch_p99".into(), batches.tail()),
        ("busy".into(), delta.busy()),
        ("flushes_s".into(), delta.flushes as f64 / delta.secs),
        ("write_mib_s".into(), delta.written as f64 / delta.secs / f64::from(1 << 20)),
        ("read_mib_s".into(), delta.read as f64 / delta.secs / f64::from(1 << 20)),
    ];
    figures.extend(paced_figures("read", &reads));
    figures.extend(paced_figures("stage_disk", &disk_stages));
    if let Some(ssd) = &ssd_stages {
        figures.extend(paced_figures("stage_ssd", ssd));
    }
    let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
    SideOut::new(format!("batch={batch}"), order.name(), &figures)
}

/// Run the contention measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let root = ctx.sub("arm");
    let count = ctx.count(POPULATION, 64);
    {
        let root = root.clone();
        on_core(ctx.core, ctx.sibling, move || arm::populate(root, count));
    }
    let (dir, ssd_dir) = (ctx.sub("contend"), ctx.ssd_sub("contend"));
    let devices = ctx.facts.devices.clone();
    let device_bytes = ctx.facts.fs_bytes;
    let quick = ctx.quick;
    let batches: Vec<usize> = if quick { vec![8] } else { BATCHES.to_vec() };
    let windows = (ctx.window(Duration::from_secs(3)), ctx.window(Duration::from_secs(20)));
    let capacity_window = (ctx.window(Duration::from_secs(1)), ctx.window(Duration::from_secs(5)));
    let sides = [Order::Idle, Order::ArrivalQd1, Order::OffsetQd1, Order::OffsetIno, Order::Kernel];
    let (outs, span) = on_core(ctx.core, ctx.sibling, move || async move {
        io::wipe(&dir);
        let ring = if quick { 16 << 20 } else { RING };
        let arm = Rc::new(Arm::open(&root, count).await);
        let span = arm.span(device_bytes);
        let disk = Rc::new(Journal::make(&dir, ring).await);
        let ssd = match &ssd_dir {
            Some(ssd_dir) => {
                io::wipe(ssd_dir);
                Some(Rc::new(Journal::make(ssd_dir, ring).await))
            }
            None => None,
        };
        let payloads = Rc::new(Payloads::new(0xc0de));
        let _ = (payloads.get(UNIT), payloads.get(HEADER), payloads.get(HEADER + RECORD));
        let rig = Rig { arm, disk, ssd, payloads };
        let mut outs = Vec::new();
        for &batch in &batches {
            // what the disk takes with a whole batch in flight, back to back
            let window = Window::new(capacity_window.0, capacity_window.1);
            let capacity = applier(rig.arm.clone(), rig.payloads.clone(), batch, Order::Kernel, None, window, 0xca9 ^ batch as u64).await;
            let rate = capacity.applies as f64 / window.secs();
            let batches = capacity.batches.summary();
            outs.push(SideOut::new(
                format!("batch={batch}"),
                "capacity",
                &[("applies_s", rate), ("batch_p50", batches.p50), ("batch_p99", batches.tail())],
            ));
            // every side offered half of it
            for order in ordered(&sides, round) {
                let window = Window::new(windows.0, windows.1);
                let seed = u64::from(round) << 32 ^ batch as u64;
                outs.push(side(&rig, order, batch, rate / 2.0, window, devices.clone(), seed).await);
            }
        }
        let Rig { arm, disk, ssd, .. } = rig;
        if let Ok(journal) = Rc::try_unwrap(disk) {
            journal.close().await;
        }
        if let Some(Ok(journal)) = ssd.map(Rc::try_unwrap) {
            journal.close().await;
        }
        if let Ok(arm) = Rc::try_unwrap(arm) {
            arm.close().await;
        }
        io::wipe(&dir);
        if let Some(ssd_dir) = &ssd_dir {
            io::wipe(ssd_dir);
        }
        (outs, span)
    });
    let mut table = Table::new(&[
        "batch", "side", "offered/s", "applies/s", "behind", "batch p50 ms", "batch p99 ms", "read p50 ms",
        "read p99 ms", "read max ms", "disk stage p50 ms", "disk stage p99 ms", "SSD stage p50 ms",
        "SSD stage p99 ms", "disk busy", "flushes/s",
    ]);
    let ms = |out: &SideOut, name: &str| fmt(out.get(name) / 1e3);
    let mut records = Vec::new();
    for mut out in outs {
        // the supplement's sides carry the write cache they ran under
        out.side.push_str(&ctx.side_suffix);
        table.row(vec![
            out.cell.trim_start_matches("batch=").to_string(),
            out.side.clone(),
            fmt(out.get("offered_s")),
            fmt(out.get("applies_s")),
            if out.get("behind") > 0.0 { "**yes**".into() } else { "no".into() },
            ms(&out, "batch_p50"),
            ms(&out, "batch_p99"),
            ms(&out, "read_p50"),
            ms(&out, "read_p99"),
            ms(&out, "read_max"),
            ms(&out, "stage_disk_p50"),
            ms(&out, "stage_disk_p99"),
            ms(&out, "stage_ssd_p50"),
            ms(&out, "stage_ssd_p99"),
            fmt(out.get("busy")),
            fmt(out.get("flushes_s")),
        ]);
        for (name, value) in span.figures() {
            out.metrics.insert(name.to_string(), value);
        }
        records.push(ctx.record("contend", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!(
                "X7 · Reads and stages while applies run, round {round} (applies of {} KiB offered at half the capacity row; reads of {} KiB and 4 KiB stages at {READ_RATE}/s and {STAGE_RATE}/s, open loop; {})",
                UNIT >> 10,
                UNIT >> 10,
                span.show()
            ),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
