//! X15: a short stripe chunk's file, made fresh or written ahead
//!
//! [O107](../../../docs/src/appendix/optimizations.md) found that the store writes every whole
//! chunk, however short, into a file of its pool's whole chunk length, taken from a pool the slice
//! wrote ahead with zeros, and refills the pool with another: a 64 KiB stripe costs its device
//! about 4 MiB. F96 sizes a chunk's file to a class of its units. Where that file comes from is
//! X15's question (`docs/src/object-storage/short-chunk-files.md`). Three sides place a put's
//! chunk as a slice does: the header and units written, made durable, renamed over the chunk's
//! path, and the chunk's directory and the file's source directory synced.
//!
//! - **`today`**: a file of the whole 4 MiB chunk from a pool written ahead, made durable by the
//!   shared flush, and the pool refilled behind it with a file of zeros, as the store is built.
//! - **`class-pool`** (B): the same, the file and its refill the chunk's own length.
//! - **`class-fresh`** (A): a file made for the chunk, allocated to its length, written and synced
//!   by its own `fdatasync`, with nothing written ahead.
//!
//! The refill runs in a task queue of its own below the foreground, as a slice's does. Each pool
//! side starts full, as a slice at rest has it. A side's device counters run until its pool is full
//! again, so the zeros a chunk cost are counted to it whenever they are written. Every size measured
//! is a power of two of units, so a chunk's class is its own length and no padding is written.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::channel::{mpsc, oneshot};
use futures::future::join_all;
use futures::StreamExt;
use glommio::io::{Directory, DmaFile};

use super::counters::Devices;
use super::io::{self, DirSync, Payloads, HEADER};
use super::record::Record;
use super::report::{interval, ratio, show, Groups};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{drain, on_core, ordered, settle, size_name, sys, Ctx, SideOut};

/// The chunk sizes measured: a short object's at the store comparison's 64 KiB and 1 MiB, one
/// between, and the whole chunk, which is O102's case
const SIZES: &[u64] = &[64 << 10, 256 << 10, 1 << 20, 4 << 20];

/// The whole chunk of a replicated pool at the default geometry
const WHOLE: u64 = 4 << 20;

/// A chunk's unit at the default geometry
const UNIT: u64 = 64 << 10;

/// Files a pool keeps ready, the store's `free_files` default
const POOL: usize = 8;

/// Chunks in flight on the one executor: one, and as many as a put's pieces keep a slice busy with
const WRITERS: &[usize] = &[1, 8];

/// Where a side's chunk file comes from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// A pool written ahead with zeros, made durable by the shared flush and refilled behind
    Pool,
    /// A file made for the chunk, allocated and synced by its own `fdatasync`
    Fresh,
}

/// One way of placing a chunk
#[derive(Debug, Clone, Copy)]
struct Side {
    /// Its name in the tables
    name: &'static str,
    /// Where its file comes from
    source: Source,
    /// Whether its file is the whole chunk's length whatever the chunk holds
    whole: bool,
}

/// The sides every cell runs
const SIDES: &[Side] = &[
    Side { name: "today", source: Source::Pool, whole: true },
    Side { name: "class-pool", source: Source::Pool, whole: false },
    Side { name: "class-fresh", source: Source::Fresh, whole: false },
];

/// The units of a chunk's class: the fewest a power of two that holds it, capped at the whole chunk
///
/// # Arguments
///
/// * `size` - The chunk's bytes
fn class_bytes(size: u64) -> u64 {
    size.div_ceil(UNIT).max(1).next_power_of_two().min(WHOLE / UNIT) * UNIT
}

/// The time each step of one chunk took
#[derive(Debug, Clone, Copy, Default)]
struct Steps {
    /// Taking a file of the pool, or making one when it is empty; creating a fresh one
    take: Duration,
    /// Allocating a fresh file to its length
    allocate: Duration,
    /// Writing the header and units
    write: Duration,
    /// Making them durable: the shared flush, or the file's own sync
    durable: Duration,
    /// Renaming it over the chunk's path
    rename: Duration,
    /// Syncing the chunk's directory and the file's source
    dir_sync: Duration,
    /// From the start to the end of the directory syncs
    cycle: Duration,
}

/// The shared flush of a slice: one `fdatasync` of a small file answers every waiter whose write
/// completed before it began, as the store's committer does
struct Flush {
    /// Where a waiter asks
    tx: RefCell<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
    /// The committer's task
    task: RefCell<Option<glommio::task::JoinHandle<()>>>,
    /// Syncs it issued
    syncs: Rc<Cell<u64>>,
}

impl Flush {
    /// Start the committer of a file
    ///
    /// # Arguments
    ///
    /// * `file` - The flush file, written ahead
    fn start(file: Rc<DmaFile>) -> Rc<Flush> {
        let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<()>>();
        let syncs = Rc::new(Cell::new(0));
        let counted = syncs.clone();
        let task = glommio::spawn_local(async move {
            // one sync for every waiter queued, never one with nothing new to make durable
            while let Some(first) = rx.next().await {
                let mut batch = vec![first];
                while let Ok(Some(more)) = rx.try_next() {
                    batch.push(more);
                }
                file.fdatasync().await.expect("the flush file is synced");
                counted.set(counted.get() + 1);
                for waiter in batch {
                    let _ = waiter.send(());
                }
            }
            file.close_rc().await.expect("the flush file closes");
        })
        .detach();
        Rc::new(Flush {
            tx: RefCell::new(Some(tx)),
            task: RefCell::new(Some(task)),
            syncs,
        })
    }

    /// Wait until a write that has completed is durable
    async fn durable(&self) {
        let (waiter, done) = oneshot::channel();
        // the borrow ends before the await
        self.tx
            .borrow()
            .as_ref()
            .expect("the committer runs")
            .unbounded_send(waiter)
            .expect("the committer takes waiters");
        done.await.expect("the committer answers");
    }

    /// Stop the committer once every waiter is answered
    async fn stop(&self) {
        self.tx.borrow_mut().take();
        let task = self.task.borrow_mut().take();
        if let Some(task) = task {
            task.await;
        }
    }
}

/// A slice's pool of one length, written ahead with zeros and refilled behind its takes
struct Pool {
    /// The pool's directory, `free/<length>`
    dir: PathBuf,
    /// It, open, for its syncs
    handle: Rc<Directory>,
    /// The length of every file
    len: u64,
    /// How its directory is synced
    form: DirSync,
    /// Files ready, by number
    ready: RefCell<BTreeSet<u64>>,
    /// The number the next file made takes
    next: Cell<u64>,
    /// Chunks that found it empty and made their own file
    held: Cell<u64>,
    /// Where the refill is woken
    kick: RefCell<Option<mpsc::UnboundedSender<()>>>,
}

impl Pool {
    /// The path of a file of the pool
    ///
    /// # Arguments
    ///
    /// * `number` - Its number
    fn path(&self, number: u64) -> PathBuf {
        self.dir.join(format!("{number:010}"))
    }

    /// Make one file as the store does: a temporary name, written with zeros and synced, renamed
    /// in, and the pool's directory synced
    async fn make(&self) -> u64 {
        let number = self.next.get();
        self.next.set(number + 1);
        let temporary = self.dir.join(format!("{number:010}.tmp"));
        let file = io::open(&temporary, true).await;
        // the zeros, and their sync
        io::zero_fill(&file, self.len).await;
        file.close().await.expect("closed");
        glommio::io::rename(&temporary, self.path(number)).await.expect("renamed into the pool");
        io::sync_dir(&self.handle, self.form).await;
        number
    }

    /// Take a ready file, or make one now when none is
    async fn take(&self) -> u64 {
        // the lowest ready number, as the store takes it; the borrow ends before any await
        let ready = {
            let mut ready = self.ready.borrow_mut();
            let first = ready.iter().next().copied();
            if let Some(number) = first {
                ready.remove(&number);
            }
            first
        };
        match ready {
            Some(number) => number,
            None => {
                self.held.set(self.held.get() + 1);
                self.make().await
            }
        }
    }

    /// Wake the refill
    fn refill(&self) {
        if let Some(kick) = self.kick.borrow().as_ref() {
            let _ = kick.unbounded_send(());
        }
    }

    /// Whether every file the pool keeps is ready
    fn full(&self) -> bool {
        self.ready.borrow().len() >= POOL
    }
}

/// Start a pool's refill in the background queue: a file at a time until it is full again
///
/// # Arguments
///
/// * `pool` - The pool
fn start_refill(pool: &Rc<Pool>) -> glommio::task::JoinHandle<()> {
    let (tx, mut rx) = mpsc::unbounded::<()>();
    *pool.kick.borrow_mut() = Some(tx);
    let pool = pool.clone();
    let queue = super::background::queue(super::background::GOAL_US);
    glommio::spawn_local_into(
        async move {
            // woken by a take, it makes files until the pool is full, as the store's refill does
            while rx.next().await.is_some() {
                while !pool.full() {
                    let number = pool.make().await;
                    pool.ready.borrow_mut().insert(number);
                }
            }
        },
        queue,
    )
    .expect("the refill spawns")
    .detach()
}

/// Everything one side's chunks need
struct State {
    /// The chunks' directory
    chunks: PathBuf,
    /// Each object's directory, opened: a short object is one chunk
    objects: Vec<Rc<Directory>>,
    /// The payloads
    payloads: Payloads,
    /// The chunk's bytes
    size: u64,
    /// The length of its file
    file_len: u64,
    /// How a directory is synced
    form: DirSync,
    /// The pool, for a side that takes from one
    pool: Option<Rc<Pool>>,
    /// The shared flush, for a side that takes from a pool
    flush: Option<Rc<Flush>>,
    /// Where a fresh file is made, opened, for a side that makes them
    fresh: Option<(PathBuf, Rc<Directory>)>,
    /// The next fresh file's number
    next: Cell<u64>,
}

/// The path of chunk `index`
///
/// # Arguments
///
/// * `chunks` - The chunks' directory
/// * `index` - The chunk
fn chunk_path(chunks: &Path, index: usize) -> PathBuf {
    chunks.join(format!("{index:016x}")).join("0000000000000000.0")
}

/// Place one chunk as a put's chunk is placed, and time its steps
///
/// # Arguments
///
/// * `state` - What the steps need
/// * `index` - The chunk
async fn one_chunk(state: &State, index: usize) -> Steps {
    let mut steps = Steps::default();
    let start = Instant::now();
    // the file, and the directory it is renamed out of
    let (file, source, source_dir) = match (&state.pool, &state.fresh) {
        (Some(pool), _) => {
            let number = pool.take().await;
            let path = pool.path(number);
            (io::open(&path, false).await, path, pool.handle.clone())
        }
        (None, Some((dir, handle))) => {
            let number = state.next.get();
            state.next.set(number + 1);
            let path = dir.join(format!("{number:010}.tmp"));
            (io::open(&path, true).await, path, handle.clone())
        }
        (None, None) => unreachable!("every side has a pool or makes fresh files"),
    };
    let taken = Instant::now();
    steps.take = taken - start;
    // a fresh file allocated to its length first, so its writes land inside it
    if state.fresh.is_some() {
        file.pre_allocate(state.file_len, false).await.expect("allocated");
    }
    let allocated = Instant::now();
    steps.allocate = allocated - taken;
    // the header and every unit holding bytes; a pool file's past them are its zeros
    io::write_chunk(&file, &state.payloads, state.size, 0).await;
    let written = Instant::now();
    steps.write = written - allocated;
    // durable: through the shared flush begun after the write, or by the file's own sync
    match &state.flush {
        Some(flush) => flush.durable().await,
        None => file.fdatasync().await.expect("synced"),
    }
    let durable = Instant::now();
    steps.durable = durable - written;
    // over the chunk's path, and both directories made durable, as an apply does
    glommio::io::rename(&source, chunk_path(&state.chunks, index)).await.expect("renamed");
    let renamed = Instant::now();
    steps.rename = renamed - durable;
    io::sync_dir(&state.objects[index], state.form).await;
    io::sync_dir(&source_dir, state.form).await;
    steps.dir_sync = renamed.elapsed();
    steps.cycle = start.elapsed();
    // closed after the acknowledgement, and the pool refilled behind it
    file.close().await.expect("closed");
    if let Some(pool) = &state.pool {
        pool.refill();
    }
    steps
}

/// Run one side of a cell on the executor this is called on
///
/// # Arguments
///
/// * `dir` - The side's directory
/// * `devices` - The devices to count on
/// * `form` - The directory sync
/// * `size` - The chunk's bytes
/// * `writers` - Chunks in flight
/// * `count` - How many chunks
/// * `side` - The side
/// * `cap` - The longest it may write for, whatever its count
#[allow(clippy::too_many_arguments)]
async fn run_side(
    dir: PathBuf,
    devices: Devices,
    form: DirSync,
    size: u64,
    writers: usize,
    count: usize,
    side: Side,
    cap: Option<Duration>,
) -> SideOut {
    io::wipe(&dir);
    let file_len = HEADER + if side.whole { WHOLE } else { class_bytes(size) };
    let chunks = dir.join("chunks");
    // every object's directory made and synced before the clock starts
    for index in 0..count {
        std::fs::create_dir_all(chunks.join(format!("{index:016x}"))).expect("made");
    }
    let mut objects = Vec::with_capacity(count);
    for index in 0..count {
        objects.push(Rc::new(Directory::open(chunks.join(format!("{index:016x}"))).await.expect("opened")));
    }
    io::sync_dir(&Directory::open(&chunks).await.expect("opened"), DirSync::Fsync).await;
    // the pool's directory, which a fresh side makes its files in too
    let free = dir.join("free").join(file_len.to_string());
    std::fs::create_dir_all(&free).expect("made");
    let free_handle = Rc::new(Directory::open(&free).await.expect("opened"));
    io::sync_dir(&Directory::open(dir.join("free")).await.expect("opened"), DirSync::Fsync).await;
    // a pool side's pool full and its shared flush's file written ahead, as a slice at rest has them
    let (pool, flush, refill) = if side.source == Source::Pool {
        let pool = Rc::new(Pool {
            dir: free.clone(),
            handle: free_handle.clone(),
            len: file_len,
            form,
            ready: RefCell::new(BTreeSet::new()),
            next: Cell::new(0),
            held: Cell::new(0),
            kick: RefCell::new(None),
        });
        while !pool.full() {
            let number = pool.make().await;
            pool.ready.borrow_mut().insert(number);
        }
        let flush_file = io::open(&dir.join("flush"), true).await;
        io::zero_fill(&flush_file, HEADER).await;
        let refill = start_refill(&pool);
        (Some(pool), Some(Flush::start(Rc::new(flush_file))), Some(refill))
    } else {
        (None, None, None)
    };
    let state = Rc::new(State {
        chunks: chunks.clone(),
        objects,
        payloads: Payloads::new(size ^ 0x15),
        size,
        file_len,
        form,
        pool: pool.clone(),
        flush: flush.clone(),
        fresh: (side.source == Source::Fresh).then(|| (free.clone(), free_handle.clone())),
        next: Cell::new(0),
    });
    // warm the payload buffers, then settle and read the counters
    for len in [HEADER, size.min(io::PIECE)] {
        let _ = state.payloads.get(len);
    }
    settle(&dir).await;
    let before = devices.snap();
    let thread_before = sys::thread_cpu_ns();
    let start = Instant::now();
    // past the cap no new chunk is begun
    let deadline = cap.map(|cap| start + cap);
    let over = move || deadline.is_some_and(|deadline| Instant::now() >= deadline);
    let next = Rc::new(Cell::new(0_usize));
    let tasks = (0..writers).map(|_| {
        let (state, next) = (state.clone(), next.clone());
        glommio::spawn_local(async move {
            let mut mine = Vec::new();
            loop {
                let index = next.get();
                if index >= count || over() {
                    return mine;
                }
                next.set(index + 1);
                mine.push(one_chunk(&state, index).await);
            }
        })
    });
    let steps: Vec<Steps> = join_all(tasks).await.into_iter().flatten().collect();
    let wall = start.elapsed().as_secs_f64();
    // the zeros the chunks cost, written however long after: the pool full again
    let refilling = Instant::now();
    if let Some(pool) = &pool {
        pool.refill();
        while !pool.full() {
            glommio::timer::sleep(Duration::from_millis(1)).await;
        }
    }
    let refill_ms = refilling.elapsed().as_secs_f64() * 1e3;
    let exec_ns = sys::thread_cpu_ns() - thread_before;
    let drain_ms = drain(&dir);
    let delta = before.delta(&devices.snap());
    // the refill and the flush stopped once counted
    if let Some(pool) = &pool {
        pool.kick.borrow_mut().take();
    }
    if let Some(refill) = refill {
        refill.await;
    }
    let syncs = flush.as_ref().map_or(0, |flush| flush.syncs.get());
    if let Some(flush) = &flush {
        flush.stop().await;
    }
    // the chunks a cap let through
    let count = steps.len();
    let mut samples: [Samples; 7] = Default::default();
    for step in &steps {
        for (slot, took) in samples.iter_mut().zip([
            step.take,
            step.allocate,
            step.write,
            step.durable,
            step.rename,
            step.dir_sync,
            step.cycle,
        ]) {
            slot.push(took);
        }
    }
    let cycle = samples[6].summary();
    let dev_kib = delta.kib_per(count);
    let out = SideOut::new(
        format!("size={} writers={writers}", size_name(size)),
        side.name,
        &[
            ("n", count as f64),
            ("wall_s", wall),
            ("chunks_s", count as f64 / wall),
            ("mib_s", (count as u64 * size) as f64 / wall / f64::from(1 << 20)),
            ("take_p50", samples[0].summary().p50),
            ("allocate_p50", samples[1].summary().p50),
            ("write_p50", samples[2].summary().p50),
            ("durable_p50", samples[3].summary().p50),
            ("rename_p50", samples[4].summary().p50),
            ("dir_sync_p50", samples[5].summary().p50),
            ("cycle_p50", cycle.p50),
            ("cycle_tail", cycle.tail()),
            ("cycle_max", cycle.max),
            ("dev_kib", dev_kib),
            ("dev_per_byte", dev_kib * 1024.0 / size as f64),
            ("flushes", delta.flushes_per(count)),
            ("busy", delta.busy()),
            ("held", pool.as_ref().map_or(0, |pool| pool.held.get()) as f64),
            ("shared_syncs", syncs as f64 / count.max(1) as f64),
            ("refill_ms", refill_ms),
            ("exec_cpu_us", exec_ns as f64 / 1e3 / count.max(1) as f64),
            ("cpu_us", delta.cpu_us_per(count)),
            ("drain_ms", drain_ms),
        ],
    );
    io::wipe(&dir);
    out
}

/// How many chunks a cell places: its byte budget's worth, within bounds
///
/// # Arguments
///
/// * `ctx` - The run
/// * `size` - The chunk's bytes
fn count(ctx: &Ctx, size: u64) -> usize {
    let full = ((ctx.budget / size) as usize).clamp(8, 600);
    ctx.count(full, 12)
}

/// Run X15 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let sizes: Vec<u64> = if ctx.quick { vec![64 << 10, 1 << 20] } else { SIZES.to_vec() };
    let mut table = Table::new(&[
        "size", "writers", "side", "n", "take", "alloc", "write", "durable", "rename", "dir syncs",
        "cycle p50", "cycle p99/max", "chunks/s", "dev KiB/chunk", "dev B/B", "flushes/chunk",
        "held", "refill ms", "÷today",
    ]);
    let mut records: Vec<Record> = Vec::new();
    for &size in &sizes {
        for &writers in WRITERS {
            let count = count(ctx, size);
            let mut outs = Vec::new();
            for side in ordered(SIDES, round) {
                let (dir, devices, form, cap) = (ctx.sub("short").join(side.name), ctx.facts.devices.clone(), ctx.dir_sync(), ctx.side_cap());
                outs.push(on_core(ctx.core, ctx.sibling, move || {
                    run_side(dir, devices, form, size, writers, count, side, cap)
                }));
            }
            // today's rate, which every side is read against
            let today = outs.iter().find(|out| out.side == "today").map_or(0.0, |out| out.get("chunks_s"));
            outs.sort_by_key(|out| SIDES.iter().position(|side| side.name == out.side));
            for out in outs {
                table.row(vec![
                    size_name(size),
                    writers.to_string(),
                    out.side.clone(),
                    fmt(out.get("n")),
                    fmt(out.get("take_p50")),
                    fmt(out.get("allocate_p50")),
                    fmt(out.get("write_p50")),
                    fmt(out.get("durable_p50")),
                    fmt(out.get("rename_p50")),
                    fmt(out.get("dir_sync_p50")),
                    fmt(out.get("cycle_p50")),
                    fmt(out.get("cycle_tail")),
                    fmt(out.get("chunks_s")),
                    fmt(out.get("dev_kib")),
                    fmt(out.get("dev_per_byte")),
                    fmt(out.get("flushes")),
                    fmt(out.get("held")),
                    fmt(out.get("refill_ms")),
                    if today > 0.0 { fmt(out.get("chunks_s") / today) } else { "-".into() },
                ]);
                records.push(ctx.record("short", round, out));
            }
        }
    }
    print!(
        "{}",
        table.render(
            &format!("X15. A short chunk's file, round {round} (latencies µs p50; directory sync {:?})", ctx.dir_sync()),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}

/// X15's verdicts on a leg: each side's rate and device bytes beside today's, and fresh beside a
/// class pool, every ratio taken round by round
///
/// # Arguments
///
/// * `groups` - The records, grouped
/// * `leg` - The leg
#[must_use]
pub fn report(groups: &Groups, leg: &str) -> String {
    let mut table = Table::new(&[
        "size", "writers", "today chunks/s", "class-pool ÷ today", "class-fresh ÷ today", "fresh ÷ pool",
        "dev B/B today", "dev B/B class-pool", "dev B/B class-fresh", "leads",
    ]);
    let mut any = false;
    for &size in SIZES {
        for &writers in WRITERS {
            let cell = format!("size={} writers={writers}", size_name(size));
            let Some(today) = interval(groups, leg, "short", &cell, "today", "chunks_s") else {
                continue;
            };
            any = true;
            let at = |side: &str| interval(groups, leg, "short", &cell, side, "chunks_s");
            let over = |top: &str, bottom: &str| ratio(groups, leg, "short", (&cell, top, "chunks_s"), (&cell, bottom, "chunks_s"));
            let bytes = |side: &str| interval(groups, leg, "short", &cell, side, "dev_per_byte");
            // a side leads only where its interval clears the other's
            let leads = match (at("class-fresh"), at("class-pool")) {
                (Some(fresh), Some(pool)) if fresh.min > pool.max => "class-fresh",
                (Some(fresh), Some(pool)) if pool.min > fresh.max => "class-pool",
                (Some(_), Some(_)) => "within",
                _ => "-",
            };
            table.row(vec![
                size_name(size),
                writers.to_string(),
                today.show(),
                show(over("class-pool", "today")),
                show(over("class-fresh", "today")),
                show(over("class-fresh", "class-pool")),
                show(bytes("today")),
                show(bytes("class-pool")),
                show(bytes("class-fresh")),
                leads.into(),
            ]);
        }
    }
    if any {
        table.render("X15. A short chunk's file: each side beside today, and fresh beside a class pool", leg)
    } else {
        String::new()
    }
}
