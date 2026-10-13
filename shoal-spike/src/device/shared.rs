//! X7: one executor driving an SSD's slice and a disk's, against each on an executor of its own
//!
//! S13 lets a slice share its executor with other slices when one core is more than its device
//! needs, and a disk needs very little of a core. What it might cost is the SSD's slice beside it:
//! a disk's operations take milliseconds, and every rename and directory sync of a whole chunk
//! goes through the executor's one blocking thread. The SSD's slice runs an open loop of 64 KiB
//! reads and 4 KiB journal stages, counted from their slots. The disk's slice runs everything a
//! busy disk does at once: applies a batch at a time, reads four deep, and whole chunks written
//! into files written ahead, renamed and their directory synced. Three arrangements:
//!
//! - `ssd-alone`: the SSD's slice on its executor, nothing else
//! - `shared`: both slices on that one executor
//! - `separate`: the disk's slice on an executor of its own, on another core: the control, which
//!   shows what the disk costs the SSD through the host rather than through the executor
//!
//! H2 reads the SSD's tail in `shared` and `separate` against `ssd-alone`. What a disk's
//! operation costs its executor's thread is the projection that stands in for the plan's one
//! executor driving one, two and four disks, which one disk a host cannot run.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::Directory;

use super::arm::{self, Arm};
use super::contend::{applier, paced_figures, read_one, Journal, Order, UNIT};
use super::io::{self, DirSync, Payloads, HEADER};
use super::paced::{open_loop, until, Paced, Window};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{ordered, spawn_on, sys, Ctx, SideOut};

/// The disk's population, shared with `contend` and `scrub`
const DISK_POPULATION: usize = 1024;

/// The SSD slice's population
const SSD_POPULATION: usize = 128;

/// The SSD slice's reads a second
const SSD_READ_RATE: f64 = 1000.0;

/// The SSD slice's stages a second
const SSD_STAGE_RATE: f64 = 200.0;

/// The disk slice's applies in a batch
const BATCH: usize = 32;

/// The disk slice's reads in flight
const DISK_READS: usize = 4;

/// The whole chunks the disk slice writes, each in an object of its own
const POOL: usize = 32;

/// A whole chunk's units
const WHOLE: u64 = 1 << 20;

/// How the two slices are put on executors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arrangement {
    /// The SSD's slice alone
    SsdAlone,
    /// Both on one executor
    Shared,
    /// Each on its own
    Separate,
}

impl Arrangement {
    /// Its name
    fn name(self) -> &'static str {
        match self {
            Arrangement::SsdAlone => "ssd-alone",
            Arrangement::Shared => "shared",
            Arrangement::Separate => "separate",
        }
    }
}

/// Where everything is
#[derive(Debug, Clone)]
struct Paths {
    /// The disk's population
    disk_pop: PathBuf,
    /// The disk's whole-chunk pool
    pool: PathBuf,
    /// The SSD's population
    ssd_pop: PathBuf,
    /// The SSD's journal's directory
    ssd_journal: PathBuf,
    /// Chunks in the disk's population
    disk_count: usize,
    /// Chunks in the SSD's
    ssd_count: usize,
    /// The directory sync the probes chose
    form: DirSync,
}

/// What the SSD's slice saw
#[derive(Debug, Default)]
struct SsdSeen {
    /// Its reads
    reads: Paced,
    /// Its stages
    stages: Paced,
}

/// What the disk's slice did
#[derive(Debug, Default)]
struct DiskSeen {
    /// Applies in the window
    applies: usize,
    /// Reads in the window
    reads: usize,
    /// Whole chunks in the window
    chunks: usize,
    /// Each whole chunk's rename
    rename: Samples,
    /// Each whole chunk's directory sync
    dir_sync: Samples,
}

impl DiskSeen {
    /// Every operation in the window
    fn ops(&self) -> usize {
        self.applies + self.reads + self.chunks
    }
}

/// What one executor saw
#[derive(Debug, Default)]
struct ExecSeen {
    /// Its SSD slice's, if it had one
    ssd: Option<SsdSeen>,
    /// Its disk slice's, if it had one
    disk: Option<DiskSeen>,
    /// Its thread's CPU over the window, as a share of it
    busy: f64,
}

/// The SSD slice's work: reads and stages, each an open loop
///
/// # Arguments
///
/// * `paths` - Where everything is
/// * `window` - The window
async fn ssd_work(paths: Rc<Paths>, window: Window) -> SsdSeen {
    let arm = Rc::new(Arm::open(&paths.ssd_pop, paths.ssd_count).await);
    let journal = Rc::new(Journal::make(&paths.ssd_journal, 64 << 20).await);
    let payloads = Rc::new(Payloads::new(0x55d));
    let _ = payloads.get(HEADER + (4 << 10));
    let reads = {
        let arm = arm.clone();
        glommio::spawn_local(open_loop(SSD_READ_RATE, window, move |nth| read_one(arm.clone(), nth)))
    };
    let stages = {
        let (journal, payloads) = (journal.clone(), payloads.clone());
        glommio::spawn_local(open_loop(SSD_STAGE_RATE, window, move |_| {
            let (journal, payloads) = (journal.clone(), payloads.clone());
            async move { journal.stage(&payloads).await }
        }))
    };
    let seen = SsdSeen { reads: reads.await, stages: stages.await };
    if let Ok(journal) = Rc::try_unwrap(journal) {
        journal.close().await;
    }
    if let Ok(arm) = Rc::try_unwrap(arm) {
        arm.close().await;
    }
    seen
}

/// Make the disk's pool of whole-chunk files, each written ahead under the name `a`
///
/// # Arguments
///
/// * `pool` - Its directory
async fn make_pool(pool: &Path) {
    io::wipe(pool);
    for object in 0..POOL {
        let dir = pool.join(format!("{object:016x}"));
        std::fs::create_dir_all(&dir).expect("made");
        let file = io::open(&dir.join("a"), true).await;
        io::zero_fill(&file, WHOLE + HEADER).await;
        file.close().await.expect("closed");
    }
}

/// The disk slice's work: applies, reads four deep and whole chunks, each a closed loop
///
/// # Arguments
///
/// * `paths` - Where everything is
/// * `window` - The window
async fn disk_work(paths: Rc<Paths>, window: Window) -> DiskSeen {
    let arm = Rc::new(Arm::open(&paths.disk_pop, paths.disk_count).await);
    let payloads = Rc::new(Payloads::new(0xd15c));
    let _ = (payloads.get(UNIT), payloads.get(HEADER), payloads.get(WHOLE));
    let mut objects = Vec::with_capacity(POOL);
    for object in 0..POOL {
        objects.push(Rc::new(Directory::open(paths.pool.join(format!("{object:016x}"))).await.expect("opened")));
    }
    // the applies, a batch at a time with every sync in flight
    let applies = glommio::spawn_local(applier(arm.clone(), payloads.clone(), BATCH, Order::Kernel, None, window, 0x5a));
    // the reads, four deep
    let reads = (0..DISK_READS).map(|task| {
        let arm = arm.clone();
        glommio::spawn_local(async move {
            let mut count = 0;
            let mut nth = task as u64;
            until(window.start).await;
            while Instant::now() < window.end {
                let begun = Instant::now();
                read_one(arm.clone(), nth).await;
                if window.counts(begun, Instant::now()) {
                    count += 1;
                }
                nth += DISK_READS as u64;
            }
            count
        })
    });
    let reads: Vec<_> = reads.collect();
    // the whole chunks: a file written ahead, overwritten, synced, renamed and its directory synced
    let chunks = {
        let (paths, payloads) = (paths.clone(), payloads.clone());
        glommio::spawn_local(async move {
            // which name each object's file has, as the last arrangement left it
            let named_a = Rc::new(
                (0..POOL)
                    .map(|object| Cell::new(paths.pool.join(format!("{object:016x}")).join("a").exists()))
                    .collect::<Vec<_>>(),
            );
            let (mut count, mut rename, mut dir_sync) = (0, Samples::default(), Samples::default());
            let mut nth = 0;
            until(window.start).await;
            while Instant::now() < window.end {
                let object = nth % POOL;
                let dir = paths.pool.join(format!("{object:016x}"));
                let (from, to) = if named_a[object].get() { ("a", "b") } else { ("b", "a") };
                let begun = Instant::now();
                let file = io::open(&dir.join(from), false).await;
                io::write_chunk(&file, &payloads, WHOLE, 0).await;
                file.fdatasync().await.expect("synced");
                let renaming = Instant::now();
                file.rename(dir.join(to)).await.expect("renamed");
                let syncing = Instant::now();
                io::sync_dir(&objects[object], paths.form).await;
                let ended = Instant::now();
                file.close().await.expect("closed");
                named_a[object].set(!named_a[object].get());
                if window.counts(begun, ended) {
                    count += 1;
                    rename.push(syncing - renaming);
                    dir_sync.push(ended - syncing);
                }
                nth += 1;
            }
            (count, rename, dir_sync)
        })
    };
    let reads: usize = join_all(reads).await.into_iter().sum();
    let (chunks, rename, dir_sync) = chunks.await;
    let applies = applies.await.applies;
    if let Ok(arm) = Rc::try_unwrap(arm) {
        arm.close().await;
    }
    DiskSeen { applies, reads, chunks, rename, dir_sync }
}

/// One executor's part of a side: the SSD's slice, the disk's, or both, with its thread's CPU
///
/// # Arguments
///
/// * `paths` - Where everything is
/// * `ssd` - Whether it runs the SSD's slice
/// * `disk` - Whether it runs the disk's
/// * `window` - The window
async fn executor_part(paths: Paths, ssd: bool, disk: bool, window: Window) -> ExecSeen {
    let paths = Rc::new(paths);
    // the thread's CPU across the counted window, read on the thread itself
    let cpu = glommio::spawn_local(async move {
        until(window.warm).await;
        let at_warm = sys::thread_cpu_ns();
        until(window.end).await;
        sys::thread_cpu_ns() - at_warm
    });
    let ssd = ssd.then(|| glommio::spawn_local(ssd_work(paths.clone(), window)));
    let disk = disk.then(|| glommio::spawn_local(disk_work(paths.clone(), window)));
    let ssd = match ssd {
        Some(task) => Some(task.await),
        None => None,
    };
    let disk = match disk {
        Some(task) => Some(task.await),
        None => None,
    };
    let busy = cpu.await as f64 / 1e9 / window.secs();
    ExecSeen { ssd, disk, busy }
}

/// Run one arrangement
///
/// # Arguments
///
/// * `ctx` - The run
/// * `paths` - Where everything is
/// * `arrangement` - Which
fn side(ctx: &Ctx, paths: &Paths, arrangement: Arrangement) -> SideOut {
    // a lead long enough for each executor to open its files before the first operation is due
    let window = Window::after(
        ctx.window(Duration::from_secs(3)).max(Duration::from_secs(1)),
        ctx.window(Duration::from_secs(3)),
        ctx.window(Duration::from_secs(20)),
    );
    let (first, second) = (ctx.order[0], ctx.order.get(1).copied().unwrap_or(ctx.order[0]));
    let before = ctx.facts.devices.snap();
    let (one, two) = match arrangement {
        Arrangement::SsdAlone => {
            let paths = paths.clone();
            (spawn_on(first.0, first.1, move || executor_part(paths, true, false, window)), None)
        }
        Arrangement::Shared => {
            let paths = paths.clone();
            (spawn_on(first.0, first.1, move || executor_part(paths, true, true, window)), None)
        }
        Arrangement::Separate => {
            let (a, b) = (paths.clone(), paths.clone());
            (
                spawn_on(first.0, first.1, move || executor_part(a, true, false, window)),
                Some(spawn_on(second.0, second.1, move || executor_part(b, false, true, window))),
            )
        }
    };
    let one = one.join().expect("the first executor finishes");
    let two = two.map(|handle| handle.join().expect("the second executor finishes"));
    let delta = before.delta(&ctx.facts.devices.snap());
    let ssd = one.ssd.as_ref().expect("the SSD's slice ran");
    // the disk's slice is on the first executor, or the second
    let (disk, disk_busy) = match (&one.disk, &two) {
        (Some(disk), _) => (Some(disk), one.busy),
        (None, Some(two)) => (two.disk.as_ref(), two.busy),
        _ => (None, 0.0),
    };
    let secs = window.secs();
    let mut figures: Vec<(String, f64)> = vec![
        ("ssd_exec_busy".into(), one.busy),
        ("disk_exec_busy".into(), disk_busy),
        ("disk_busy".into(), delta.busy()),
    ];
    figures.extend(paced_figures("ssd_read", &ssd.reads));
    figures.extend(paced_figures("ssd_stage", &ssd.stages));
    if let Some(disk) = disk {
        let (rename, dir_sync) = (disk.rename.summary(), disk.dir_sync.summary());
        figures.extend([
            ("disk_applies_s".to_string(), disk.applies as f64 / secs),
            ("disk_reads_s".to_string(), disk.reads as f64 / secs),
            ("disk_chunks_s".to_string(), disk.chunks as f64 / secs),
            ("rename_p50".to_string(), rename.p50),
            ("rename_p99".to_string(), rename.tail()),
            ("dir_sync_p50".to_string(), dir_sync.p50),
            // the disk's executor's thread for each disk operation, alone only on its own executor
            ("disk_exec_us_per_op".to_string(), if two.is_some() { disk_busy * secs * 1e6 / disk.ops().max(1) as f64 } else { 0.0 }),
        ]);
    }
    let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
    SideOut::new("ssd+disk", arrangement.name(), &figures)
}

/// Run the shared executor measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let Some(ssd_root) = ctx.ssd_sub("shared") else {
        println!("### X7 · One executor, an SSD's slice and a disk's\n\nNot run: no --ssd-dir names the host's SSD.\n");
        return;
    };
    let paths = Paths {
        disk_pop: ctx.sub("arm"),
        pool: ctx.sub("shared").join("pool"),
        ssd_pop: ssd_root.join("pop"),
        ssd_journal: ssd_root.join("journal"),
        disk_count: ctx.count(DISK_POPULATION, 64),
        ssd_count: ctx.count(SSD_POPULATION, 16),
        form: ctx.dir_sync(),
    };
    // every population and the pool before any arrangement
    {
        let paths = paths.clone();
        super::on_core(ctx.core, ctx.sibling, move || async move {
            arm::populate(paths.disk_pop.clone(), paths.disk_count).await;
            arm::populate(paths.ssd_pop.clone(), paths.ssd_count).await;
            make_pool(&paths.pool).await;
        });
    }
    let mut outs = Vec::new();
    for arrangement in ordered(&[Arrangement::SsdAlone, Arrangement::Shared, Arrangement::Separate], round) {
        outs.push(side(ctx, &paths, arrangement));
    }
    let mut table = Table::new(&[
        "arrangement", "SSD read p50 µs", "SSD read p99 µs", "SSD read p99.9 µs", "SSD stage p50 µs",
        "SSD stage p99 µs", "SSD executor busy", "disk executor busy", "disk applies/s", "disk reads/s",
        "disk chunks/s", "rename p50 µs", "dir sync p50 ms", "disk executor µs/op", "disk busy",
    ]);
    let mut records = Vec::new();
    for out in outs {
        table.row(vec![
            out.side.clone(),
            fmt(out.get("ssd_read_p50")),
            fmt(out.get("ssd_read_p99")),
            fmt(out.get("ssd_read_p999")),
            fmt(out.get("ssd_stage_p50")),
            fmt(out.get("ssd_stage_p99")),
            fmt(out.get("ssd_exec_busy")),
            fmt(out.get("disk_exec_busy")),
            fmt(out.get("disk_applies_s")),
            fmt(out.get("disk_reads_s")),
            fmt(out.get("disk_chunks_s")),
            fmt(out.get("rename_p50")),
            fmt(out.get("dir_sync_p50") / 1e3),
            fmt(out.get("disk_exec_us_per_op")),
            fmt(out.get("disk_busy")),
        ]);
        records.push(ctx.record("shared", round, out));
    }
    let (first, second) = (ctx.order[0], ctx.order.get(1).copied().unwrap_or(ctx.order[0]));
    print!(
        "{}",
        table.render(
            &format!(
                "X7 · One executor, an SSD's slice and a disk's, round {round} (SSD: 64 KiB reads at {SSD_READ_RATE}/s and 4 KiB stages at {SSD_STAGE_RATE}/s, open loop; disk: applies in batches of {BATCH}, reads {DISK_READS} deep, 1 MiB chunks renamed; executors on cpus {} and {})",
                first.0, second.0
            ),
            &ctx.label()
        )
    );
    ctx.emit(&records);
    io::wipe(&ctx.sub("shared"));
    io::wipe(&ssd_root);
}
