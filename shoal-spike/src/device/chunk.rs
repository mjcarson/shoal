//! Measurement 1: a whole stripe chunk, a file of its own, against a slot in a shared file
//!
//! S6 stages a whole chunk as a new file beside the old one and renames it over it. That costs
//! a create, a write, a sync, a rename and a sync of the directory, where chunks sharing a large
//! file written ahead would cost a write and a sync. Layout B is that floor: no index and no
//! compaction are built for it, so it is the best a shared file could do, and a file a chunk is
//! judged against it (T1). Six in flight means six chunks on one executor, because a slice is
//! one executor; several executors are measurement 8's question.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::Directory;

use super::counters::Devices;
use super::io::{self, DirSync, Payloads, HEADER};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{drain, on_core, ordered, settle, size_name, sys, Ctx, SideOut};

/// The chunk sizes measured
const SIZES: &[u64] = &[64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20, 64 << 20];

/// Chunks an object holds, and so the chunks one directory sync can acknowledge together
const BATCH: usize = 6;

/// Where a side's chunks go
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// A file a chunk, staged under a label and renamed to its name
    File,
    /// A slot of one shared file written ahead with zeros
    Shared,
    /// A file a chunk taken from files written ahead with zeros, as the WAL recycles its
    /// segments, so the write is an overwrite and only the rename changes the filesystem
    Recycled,
}

/// One way of writing a whole chunk
#[derive(Debug, Clone, Copy)]
struct Side {
    /// Its name in the tables
    name: &'static str,
    /// Where the chunk goes
    layout: Layout,
    /// Whether the file is allocated to its full length before it is written
    ahead: bool,
    /// Whether one directory sync (or, in a shared file, one fdatasync) covers a batch of six
    batch: bool,
    /// Whether the rename replaces a chunk already there
    replace: bool,
    /// Whether the file's own data is synced; without it, only the directory's journal orders it
    file_sync: bool,
}

impl Side {
    /// A file a chunk
    ///
    /// # Arguments
    ///
    /// * `name` - Its name
    /// * `ahead` - Whether it is allocated ahead
    /// * `batch` - Whether one directory sync covers six
    const fn file(name: &'static str, ahead: bool, batch: bool) -> Side {
        Side { name, layout: Layout::File, ahead, batch, replace: false, file_sync: true }
    }

    /// A slot of a shared file
    ///
    /// # Arguments
    ///
    /// * `name` - Its name
    /// * `batch` - Whether one fdatasync covers six
    const fn shared(name: &'static str, batch: bool) -> Side {
        Side { name, layout: Layout::Shared, ahead: false, batch, replace: false, file_sync: true }
    }

    /// A file a chunk from a recycled file written ahead
    ///
    /// # Arguments
    ///
    /// * `name` - Its name
    /// * `batch` - Whether one directory sync covers six
    const fn recycled(name: &'static str, batch: bool) -> Side {
        Side { name, layout: Layout::Recycled, ahead: false, batch, replace: false, file_sync: true }
    }
}

/// The sides the recycling supplement runs: the recycled file between a file a chunk and a
/// slot of a shared file, each measured again beside it in the same run
///
/// # Arguments
///
/// * `writers` - Chunks in flight
fn recycle_sides(writers: usize) -> Vec<Side> {
    if writers == 1 {
        vec![Side::recycled("R", false), Side::file("A+", true, false), Side::shared("B-own", false)]
    } else {
        vec![
            Side::recycled("R-batch", true),
            Side::file("A+-batch", true, true),
            Side::shared("B-batch", true),
        ]
    }
}

/// The sides a cell runs
///
/// # Arguments
///
/// * `size` - The chunk size
/// * `writers` - Chunks in flight
fn sides(size: u64, writers: usize) -> Vec<Side> {
    let large = size >= 64 << 20;
    match (writers, large) {
        (1, false) => vec![
            Side::file("A", false, false),
            Side::file("A+", true, false),
            Side::shared("B-own", false),
        ],
        (1, true) => vec![Side::file("A+", true, false), Side::shared("B-own", false)],
        (_, true) => vec![Side::file("A+-batch", true, true), Side::shared("B-batch", true)],
        (_, false) => {
            let mut sides = vec![
                Side::file("A-each", false, false),
                Side::file("A-batch", false, true),
                Side::file("A+-each", true, false),
                Side::file("A+-batch", true, true),
                Side { name: "A+-dironly", file_sync: false, ..Side::file("", true, true) },
                Side::shared("B-own", false),
                Side::shared("B-batch", true),
            ];
            // a replacement frees the old chunk's blocks, measured at two sizes only
            if size == 1 << 20 || size == 16 << 20 {
                sides.push(Side { name: "A+-batch-replace", replace: true, ..Side::file("", true, true) });
            }
            sides
        }
    }
}

/// The time each step of one chunk took
#[derive(Debug, Clone, Copy, Default)]
struct Steps {
    /// Opening (creating) the file
    open: Duration,
    /// Allocating it ahead
    allocate: Duration,
    /// Writing the header and units
    write: Duration,
    /// Syncing the file
    sync: Duration,
    /// Renaming it to its name
    rename: Duration,
    /// Syncing the directory, the batch's sync for a batched side
    dir_sync: Duration,
    /// From the start to the end of the directory sync that acknowledged it
    cycle: Duration,
    /// Closing it, after the cycle
    close: Duration,
}

/// Everything one chunk's steps need
struct Shared {
    /// The placement group's directory
    pg: PathBuf,
    /// Each object's directory, opened
    objects: Vec<Rc<Directory>>,
    /// The shared file of layout B, if the side uses one
    slots: Option<glommio::io::DmaFile>,
    /// The payloads
    payloads: Payloads,
    /// The chunk size
    size: u64,
    /// The directory sync to use
    form: DirSync,
}

/// The path of chunk `index`, staged or final
///
/// # Arguments
///
/// * `pg` - The placement group's directory
/// * `index` - The chunk
/// * `staged` - Whether it is the name a stage writes, under the write's label
fn chunk_path(pg: &Path, index: usize, staged: bool) -> PathBuf {
    let object = pg.join(format!("{:016x}", index / BATCH));
    let name = format!("{:06}.{}", 0, index % BATCH);
    if staged {
        object.join(format!("{name}.7"))
    } else {
        object.join(name)
    }
}

/// The path of the recycled file chunk `index` is written into
///
/// # Arguments
///
/// * `pg` - The placement group's directory
/// * `index` - The chunk
fn free_path(pg: &Path, index: usize) -> PathBuf {
    pg.join(format!("{:016x}", index / BATCH)).join(format!("free-{}", index % BATCH))
}

/// Write one chunk as a file, up to its rename, and its directory sync if it syncs its own
///
/// # Arguments
///
/// * `shared` - What the steps need
/// * `side` - The side
/// * `index` - The chunk
/// * `own_sync` - Whether this chunk syncs its directory itself
async fn one_file(shared: &Shared, side: Side, index: usize, own_sync: bool) -> Steps {
    let mut steps = Steps::default();
    let start = Instant::now();
    // create the staged file, or open a recycled one written ahead
    let file = if side.layout == Layout::Recycled {
        io::open(&free_path(&shared.pg, index), false).await
    } else {
        io::open(&chunk_path(&shared.pg, index, true), true).await
    };
    let opened = Instant::now();
    steps.open = opened - start;
    // allocate it to its full length, on the sides that do
    if side.ahead {
        file.pre_allocate(shared.size + HEADER, false).await.expect("allocated");
    }
    let allocated = Instant::now();
    steps.allocate = allocated - opened;
    // the header and every unit
    io::write_chunk(&file, &shared.payloads, shared.size, 0).await;
    let written = Instant::now();
    steps.write = written - allocated;
    // its data durable, on the sides that sync it
    if side.file_sync {
        file.fdatasync().await.expect("synced");
    }
    let synced = Instant::now();
    steps.sync = synced - written;
    // its name
    file.rename(chunk_path(&shared.pg, index, false)).await.expect("renamed");
    let renamed = Instant::now();
    steps.rename = renamed - synced;
    // the directory, when this chunk acknowledges itself
    if own_sync {
        io::sync_dir(&shared.objects[index / BATCH], shared.form).await;
        steps.dir_sync = renamed.elapsed();
        steps.cycle = start.elapsed();
    } else {
        steps.cycle = start.elapsed();
    }
    // closing is after the acknowledgement
    let closing = Instant::now();
    file.close().await.expect("closed");
    steps.close = closing.elapsed();
    steps
}

/// Write one chunk into its slot of the shared file, and sync it if it syncs alone
///
/// # Arguments
///
/// * `shared` - What the steps need
/// * `index` - The chunk
/// * `own_sync` - Whether this chunk syncs the file itself
async fn one_slot(shared: &Shared, index: usize, own_sync: bool) -> Steps {
    let mut steps = Steps::default();
    let file = shared.slots.as_ref().expect("a shared side has its file");
    let start = Instant::now();
    io::write_chunk(file, &shared.payloads, shared.size, index as u64 * (shared.size + HEADER)).await;
    let written = Instant::now();
    steps.write = written - start;
    if own_sync {
        file.fdatasync().await.expect("synced");
        steps.sync = written.elapsed();
    }
    steps.cycle = start.elapsed();
    steps
}

/// Run one side of a cell on the executor this is called on
///
/// # Arguments
///
/// * `dir` - The side's directory
/// * `devices` - The devices to count on
/// * `form` - The directory sync
/// * `size` - The chunk size
/// * `writers` - Chunks in flight
/// * `count` - How many chunks
/// * `side` - The side
async fn run_side(
    dir: PathBuf,
    devices: Devices,
    form: DirSync,
    size: u64,
    writers: usize,
    count: usize,
    side: Side,
) -> SideOut {
    io::wipe(&dir);
    let pg = dir.join("pg0");
    // every object's directory made and synced before the clock starts
    let objects = count.div_ceil(BATCH);
    for object in 0..objects {
        std::fs::create_dir_all(pg.join(format!("{object:016x}"))).expect("made");
    }
    let mut handles = Vec::with_capacity(objects);
    for object in 0..objects {
        handles.push(Rc::new(Directory::open(pg.join(format!("{object:016x}"))).await.expect("opened")));
    }
    io::sync_dir(&Directory::open(&pg).await.expect("opened"), DirSync::Fsync).await;
    // layout B's file, written ahead with zeros
    let slots = if side.layout == Layout::Shared {
        let file = io::open(&dir.join("shared"), true).await;
        io::zero_fill(&file, count as u64 * (size + HEADER)).await;
        Some(file)
    } else {
        None
    };
    let shared = Rc::new(Shared {
        pg: pg.clone(),
        objects: handles,
        slots,
        payloads: Payloads::new(size ^ 0x5eed),
        size,
        form,
    });
    // a recycling side finds its files written ahead with zeros, as a pool of free files
    if side.layout == Layout::Recycled {
        for index in 0..count {
            let file = io::open(&free_path(&pg, index), true).await;
            io::zero_fill(&file, size + HEADER).await;
            file.close().await.expect("closed");
        }
    }
    // a replacing side finds every chunk already there
    if side.replace {
        for index in 0..count {
            let file = io::open(&chunk_path(&pg, index, false), true).await;
            io::write_chunk(&file, &shared.payloads, size, 0).await;
            file.fdatasync().await.expect("synced");
            file.close().await.expect("closed");
        }
    }
    // warm the payload buffers, then settle and read the counters
    for len in [HEADER, size.min(io::PIECE)] {
        let _ = shared.payloads.get(len);
    }
    settle(&dir).await;
    let before = devices.snap();
    let thread_before = sys::thread_cpu_ns();
    let start = Instant::now();
    let steps: Vec<Steps> = match (side.batch, writers) {
        // one at a time, each acknowledging itself
        (false, 1) => {
            let mut all = Vec::with_capacity(count);
            for index in 0..count {
                all.push(match side.layout {
                    Layout::File | Layout::Recycled => one_file(&shared, side, index, true).await,
                    Layout::Shared => one_slot(&shared, index, true).await,
                });
            }
            all
        }
        // six in flight, each acknowledging itself
        (false, _) => {
            let next = Rc::new(Cell::new(0_usize));
            let tasks = (0..writers).map(|_| {
                let (shared, next) = (shared.clone(), next.clone());
                glommio::spawn_local(async move {
                    let mut mine = Vec::new();
                    loop {
                        let index = next.get();
                        if index >= count {
                            return mine;
                        }
                        next.set(index + 1);
                        mine.push(match side.layout {
                            Layout::File | Layout::Recycled => one_file(&shared, side, index, true).await,
                            Layout::Shared => one_slot(&shared, index, true).await,
                        });
                    }
                })
            });
            join_all(tasks).await.into_iter().flatten().collect()
        }
        // six at once, then one sync acknowledging the six
        (true, _) => {
            let mut all = Vec::with_capacity(count);
            for batch in 0..count.div_ceil(BATCH) {
                let started = Instant::now();
                let indices: Vec<usize> = (batch * BATCH..((batch + 1) * BATCH).min(count)).collect();
                let mut done: Vec<Steps> = join_all(indices.iter().map(|&index| {
                    let shared = shared.clone();
                    async move {
                        match side.layout {
                            Layout::File | Layout::Recycled => one_file(&shared, side, index, false).await,
                            Layout::Shared => one_slot(&shared, index, false).await,
                        }
                    }
                }))
                .await;
                // the one sync that acknowledges the batch
                let syncing = Instant::now();
                match side.layout {
                    Layout::File | Layout::Recycled => io::sync_dir(&shared.objects[batch], form).await,
                    Layout::Shared => shared.slots.as_ref().expect("a file").fdatasync().await.expect("synced"),
                }
                let ended = Instant::now();
                for step in &mut done {
                    // a chunk's cycle runs from its own start, which is the batch's, to the sync's end
                    match side.layout {
                        Layout::File | Layout::Recycled => step.dir_sync = ended - syncing,
                        Layout::Shared => step.sync = ended - syncing,
                    }
                    step.cycle = ended - started;
                }
                all.extend(done);
            }
            all
        }
    };
    let wall = start.elapsed().as_secs_f64();
    let exec_ns = sys::thread_cpu_ns() - thread_before;
    let drain_ms = drain(&dir);
    let delta = before.delta(&devices.snap());
    // every step's samples
    let mut samples: [Samples; 8] = Default::default();
    for step in &steps {
        for (slot, took) in samples.iter_mut().zip([
            step.open,
            step.allocate,
            step.write,
            step.sync,
            step.rename,
            step.dir_sync,
            step.cycle,
            step.close,
        ]) {
            slot.push(took);
        }
    }
    let cycle = samples[6].summary();
    let out = SideOut::new(
        format!("size={} writers={writers}", size_name(size)),
        side.name,
        &[
            ("n", count as f64),
            ("wall_s", wall),
            ("chunks_s", count as f64 / wall),
            ("mib_s", (count as u64 * size) as f64 / wall / f64::from(1 << 20)),
            ("open_p50", samples[0].summary().p50),
            ("allocate_p50", samples[1].summary().p50),
            ("write_p50", samples[2].summary().p50),
            ("sync_p50", samples[3].summary().p50),
            ("rename_p50", samples[4].summary().p50),
            ("dir_sync_p50", samples[5].summary().p50),
            ("close_p50", samples[7].summary().p50),
            ("cycle_p50", cycle.p50),
            ("cycle_tail", cycle.tail()),
            ("cycle_max", cycle.max),
            ("dev_kib", delta.kib_per(count)),
            ("flushes", delta.flushes_per(count)),
            ("exec_cpu_us", exec_ns as f64 / 1e3 / count as f64),
            ("cpu_us", delta.cpu_us_per(count)),
            ("drain_ms", drain_ms),
            ("discard_kib", delta.discarded as f64 / 1024.0 / count as f64),
        ],
    );
    io::wipe(&dir);
    out
}

/// How many chunks a cell writes: its byte budget's worth, within bounds, in whole batches
///
/// # Arguments
///
/// * `ctx` - The run
/// * `size` - The chunk size
/// * `writers` - Chunks in flight
fn count(ctx: &Ctx, size: u64, writers: usize) -> usize {
    let full = ((ctx.budget / size) as usize).clamp(8, 600);
    let count = ctx.count(full, 12);
    if writers > 1 {
        count.div_ceil(BATCH) * BATCH
    } else {
        count
    }
}

/// Run measurement 1 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    run_with(ctx, round, "chunk", sides);
}

/// Run the recycling supplement for one round: a file a chunk taken from files written ahead,
/// beside a fresh file and a slot of a shared file
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run_recycle(ctx: &Ctx, round: u32) {
    run_with(ctx, round, "chunk-recycle", |_, writers| recycle_sides(writers));
}

/// Run a set of whole-chunk sides for one round, under a measurement's name
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
/// * `measurement` - The name its records carry
/// * `sides` - The sides of a cell, by chunk size and chunks in flight
fn run_with(ctx: &Ctx, round: u32, measurement: &str, sides: fn(u64, usize) -> Vec<Side>) {
    let sizes: Vec<u64> = if ctx.quick { vec![64 << 10, 1 << 20] } else { SIZES.to_vec() };
    let mut table = Table::new(&[
        "size", "writers", "side", "n", "open", "alloc", "write", "fdatasync", "rename", "dir sync",
        "cycle p50", "cycle p99/max", "chunks/s", "MiB/s", "dev KiB/chunk", "flushes/chunk",
        "cpu µs/chunk", "A÷B",
    ]);
    let mut records = Vec::new();
    for &size in &sizes {
        for writers in [1, 6] {
            let count = count(ctx, size, writers);
            let mut outs = Vec::new();
            for side in ordered(&sides(size, writers), round) {
                let (dir, devices, form) = (ctx.sub(measurement).join(side.name), ctx.facts.devices.clone(), ctx.dir_sync());
                outs.push(on_core(ctx.core, ctx.sibling, move || {
                    run_side(dir, devices, form, size, writers, count, side)
                }));
            }
            // the floor every file-a-chunk side is read against
            let floor = outs
                .iter()
                .find(|out| out.side == if writers == 1 { "B-own" } else { "B-batch" })
                .map_or(0.0, |out| out.get("chunks_s"));
            outs.sort_by_key(|out| sides(size, writers).iter().position(|side| side.name == out.side));
            for out in outs {
                table.row(vec![
                    size_name(size),
                    writers.to_string(),
                    out.side.clone(),
                    fmt(out.get("n")),
                    fmt(out.get("open_p50")),
                    fmt(out.get("allocate_p50")),
                    fmt(out.get("write_p50")),
                    fmt(out.get("sync_p50")),
                    fmt(out.get("rename_p50")),
                    fmt(out.get("dir_sync_p50")),
                    fmt(out.get("cycle_p50")),
                    fmt(out.get("cycle_tail")),
                    fmt(out.get("chunks_s")),
                    fmt(out.get("mib_s")),
                    fmt(out.get("dev_kib")),
                    fmt(out.get("flushes")),
                    fmt(out.get("cpu_us")),
                    if floor > 0.0 { fmt(out.get("chunks_s") / floor) } else { "-".into() },
                ]);
                records.push(ctx.record(measurement, round, out));
            }
        }
    }
    print!(
        "{}",
        table.render(
            &format!("1. A whole chunk ({measurement}), round {round} (latencies µs p50; directory sync {:?})", ctx.dir_sync()),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
