//! Measurement 3: a partial write, journalled and applied in place, against staged and cloned
//!
//! S6 stages part of a chunk as a journal record and applies it by writing the units and the
//! header in place, so the bytes are written twice. A clone could make it once: stage the units
//! block-aligned in a file of their own, then splice them into the chunk with `FICLONERANGE`.
//! This times both, stage and apply apart, and counts what the device was asked to write. It
//! is the device half of Q27, and the clone sides are judged by T3.
//!
//! Writes are whole units at unit-aligned offsets, so nothing is read and merged; a write into
//! part of a unit would add a read and a write of the unit to the apply. Writes in flight go to
//! different chunks, as S6 requires of a read and an apply of one chunk. Each side has chunks of
//! its own, so a clone side's shared blocks never reach the journal side's chunks; J′ is the
//! journal side on chunks that have been cloned into, which is what choosing the clone would
//! leave every in-place write with afterwards.

use std::cell::Cell;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::DmaFile;

use super::counters::Devices;
use super::io::{self, Payloads, HEADER};
use super::journal::{Committer, Ring};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{drain, on_core, ordered, settle, size_name, sys, Ctx, SideOut};

/// The write sizes measured, each one whole unit
const SIZES: &[u64] = &[4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20];

/// A chunk's units
const CHUNK: u64 = 4 << 20;

/// Chunks each side writes into
const CHUNKS: usize = 64;

/// A staging slot: a header block and the largest write
const SLOT: u64 = HEADER + (1 << 20);

/// Staging slots in the shared staging file
const SLOTS: usize = 64;

/// The journal ring
const RING: u64 = 1 << 30;

/// How a side stages and applies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Way {
    /// A journal record, then the units and header written in place: S6
    Journal,
    /// Staged into a shared staging file, cloned into the chunk, the slot punched after
    ClonePunch,
    /// The same, the slot left as it is, so the next stage into it copies on write
    CloneCow,
    /// Staged into a file of its own for each writer, cloned, punched
    ClonePerSlot,
    /// The journal's way, on chunks that have been cloned into
    JournalCloned,
}

impl Way {
    /// The side's name
    fn name(self) -> &'static str {
        match self {
            Way::Journal => "J",
            Way::ClonePunch => "C-punch",
            Way::CloneCow => "C-cow",
            Way::ClonePerSlot => "C-perslot",
            Way::JournalCloned => "J'",
        }
    }

    /// The directory of the side's chunks
    fn chunks(self) -> &'static str {
        match self {
            Way::Journal => "chunks-j",
            Way::ClonePunch => "chunks-cp",
            Way::CloneCow => "chunks-cc",
            Way::ClonePerSlot => "chunks-cs",
            Way::JournalCloned => "chunks-jc",
        }
    }
}

/// What one write's parts took
#[derive(Debug, Clone, Copy, Default)]
struct Parts {
    /// The stage, to durable
    stage: Duration,
    /// The apply, to durable
    apply: Duration,
    /// The apply's fdatasync alone
    apply_sync: Duration,
    /// The clone's ioctl
    clone: Duration,
    /// The clone's wait for the blocking thread
    clone_wait: Duration,
    /// Punching the staging range afterwards
    release: Duration,
}

/// What every writer of a side shares
struct Bench {
    /// The side's chunks, opened
    chunks: Vec<DmaFile>,
    /// The journal, for the journal's ways
    journal: Rc<DmaFile>,
    /// Where the journal's records go
    ring: Ring,
    /// Its committer
    journal_sync: Rc<Committer>,
    /// The shared staging file
    stage: Rc<DmaFile>,
    /// Its committer
    stage_sync: Rc<Committer>,
    /// The staging files of their own, one a writer
    own: Vec<DmaFile>,
    /// The payloads
    payloads: Payloads,
    /// The write size
    size: u64,
    /// The side
    way: Way,
}

/// Write chunk files of the units' length plus a header, fully, and sync them
///
/// # Arguments
///
/// * `dir` - Their directory
/// * `count` - How many
/// * `payloads` - The bytes
async fn make_chunks(dir: &Path, count: usize, payloads: &Payloads) -> Vec<DmaFile> {
    io::wipe(dir);
    std::fs::create_dir_all(dir).expect("made");
    let mut chunks = Vec::with_capacity(count);
    for index in 0..count {
        let file = io::open(&dir.join(format!("{index:06}.0")), true).await;
        io::write_chunk(&file, payloads, CHUNK, 0).await;
        file.fdatasync().await.expect("synced");
        chunks.push(file);
    }
    chunks
}

/// One write: staged, then applied
///
/// # Arguments
///
/// * `bench` - What the side's writers share
/// * `writer` - Which writer this is
/// * `writers` - How many there are
/// * `nth` - This writer's how manieth write
/// * `rng` - The writer's generator
async fn one_write(bench: &Bench, writer: usize, writers: usize, nth: usize, rng: &mut Rng) -> Parts {
    let mut parts = Parts::default();
    let size = bench.size;
    // a chunk only this writer touches, and a whole unit at a random place in it
    let chunks = bench.chunks.len();
    let chunk = &bench.chunks[writer + writers * (nth % (chunks / writers))];
    let offset = HEADER + rng.below(CHUNK / size) * size;
    let staged = Instant::now();
    match bench.way {
        Way::Journal | Way::JournalCloned => {
            // a record of header and units in the ring, durable through the group commit
            let at = bench.ring.take(HEADER + size).expect("the ring wraps");
            bench.journal.write_rc_at(bench.payloads.get(HEADER + size), at).await.expect("staged");
            bench.journal_sync.durable().await;
            let applying = Instant::now();
            parts.stage = applying - staged;
            // the units and header in place, then the chunk's sync
            futures::join!(
                io::write_body(chunk, &bench.payloads, size, offset),
                io::write_body(chunk, &bench.payloads, HEADER, 0)
            );
            let syncing = Instant::now();
            chunk.fdatasync().await.expect("applied");
            parts.apply_sync = syncing.elapsed();
            parts.apply = applying.elapsed();
        }
        Way::ClonePunch | Way::CloneCow | Way::ClonePerSlot => {
            // the staging slot: this writer's own file, or one of its slots in the shared one
            let (stage, slot) = if bench.way == Way::ClonePerSlot {
                (&bench.own[writer], 0)
            } else {
                (&*bench.stage, (writer + writers * (nth % (SLOTS / writers))) as u64 * SLOT)
            };
            // header and units staged, durable through the group commit or alone
            stage.write_rc_at(bench.payloads.get(HEADER + size), slot).await.expect("staged");
            if bench.way == Way::ClonePerSlot {
                stage.fdatasync().await.expect("staged");
            } else {
                bench.stage_sync.durable().await;
            }
            let applying = Instant::now();
            parts.stage = applying - staged;
            // the units cloned in, the header written in place, then the chunk's sync
            let took = io::clone(stage, slot + HEADER, size, chunk, offset).await.expect("cloned");
            parts.clone = took.took;
            parts.clone_wait = took.waited;
            io::write_body(chunk, &bench.payloads, HEADER, 0).await;
            let syncing = Instant::now();
            chunk.fdatasync().await.expect("applied");
            parts.apply_sync = syncing.elapsed();
            parts.apply = applying.elapsed();
            // the staged units dropped, so the slot holds no blocks the chunk shares
            if bench.way != Way::CloneCow {
                let releasing = Instant::now();
                stage.deallocate(slot + HEADER, size).await.expect("punched");
                parts.release = releasing.elapsed();
            }
        }
    }
    parts
}

/// Run `count` writes with `writers` in flight, returning each write's parts
///
/// # Arguments
///
/// * `bench` - What the side's writers share
/// * `writers` - Writes in flight
/// * `count` - How many writes
/// * `seed` - The seed of the offsets
async fn writes(bench: &Rc<Bench>, writers: usize, count: usize, seed: u64) -> Vec<Parts> {
    let next = Rc::new(Cell::new(0_usize));
    let tasks = (0..writers).map(|writer| {
        let (bench, next) = (bench.clone(), next.clone());
        glommio::spawn_local(async move {
            let mut rng = Rng::new(seed ^ writer as u64);
            let mut mine = Vec::new();
            let mut nth = 0;
            // take the next write until there are none
            loop {
                let index = next.get();
                if index >= count {
                    return mine;
                }
                next.set(index + 1);
                mine.push(one_write(&bench, writer, writers, nth, &mut rng).await);
                nth += 1;
            }
        })
    });
    join_all(tasks).await.into_iter().flatten().collect()
}

/// Run one side
///
/// # Arguments
///
/// * `bench` - What its writers share
/// * `dir` - The measurement's directory
/// * `devices` - The devices to count on
/// * `writers` - Writes in flight
/// * `count` - Writes timed
async fn run_side(bench: Rc<Bench>, dir: &Path, devices: &Devices, writers: usize, count: usize) -> SideOut {
    let size = bench.size;
    // one pass over every slot first, so each is in the state the side leaves it in
    let _ = writes(&bench, writers, SLOTS, 0xfeed).await;
    settle(dir).await;
    let before = devices.snap();
    let thread_before = sys::thread_cpu_ns();
    let start = Instant::now();
    let parts = writes(&bench, writers, count, size).await;
    let wall = start.elapsed().as_secs_f64();
    let exec_ns = sys::thread_cpu_ns() - thread_before;
    let drain_ms = drain(dir);
    let delta = before.delta(&devices.snap());
    let mut samples: [Samples; 7] = Default::default();
    for part in &parts {
        for (slot, took) in samples.iter_mut().zip([
            part.stage,
            part.apply,
            part.apply_sync,
            part.clone,
            part.clone_wait,
            part.release,
            part.stage + part.apply,
        ]) {
            slot.push(took);
        }
    }
    let [stage, apply, apply_sync, clone, clone_wait, release, total] = samples.map(|s| s.summary());
    SideOut::new(
        format!("size={} inflight={writers}", size_name(size)),
        bench.way.name(),
        &[
            ("n", count as f64),
            ("writes_s", count as f64 / wall),
            ("stage_p50", stage.p50),
            ("stage_p99", stage.tail()),
            ("apply_p50", apply.p50),
            ("apply_p99", apply.tail()),
            ("apply_sync_p50", apply_sync.p50),
            ("apply_sync_p99", apply_sync.tail()),
            ("clone_p50", clone.p50),
            ("clone_wait_p50", clone_wait.p50),
            ("release_p50", release.p50),
            ("total_p50", total.p50),
            ("total_p99", total.tail()),
            ("dev_kib", delta.kib_per(count)),
            ("dev_ratio", delta.written as f64 / (count as u64 * size) as f64),
            ("flushes", delta.flushes_per(count)),
            ("exec_cpu_us", exec_ns as f64 / 1e3 / count as f64),
            ("cpu_us", delta.cpu_us_per(count)),
            ("drain_ms", drain_ms),
        ],
    )
}

/// Run measurement 3 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let dir = ctx.sub("partial");
    let (devices, quick, budget, clones) = (ctx.facts.devices.clone(), ctx.quick, ctx.budget, ctx.clones());
    let sizes: Vec<u64> = if quick { vec![4 << 10, 64 << 10] } else { SIZES.to_vec() };
    let ways: Vec<Way> = if clones {
        vec![Way::Journal, Way::ClonePunch, Way::CloneCow, Way::ClonePerSlot, Way::JournalCloned]
    } else {
        vec![Way::Journal]
    };
    let outs = on_core(ctx.core, ctx.sibling, move || async move {
        io::wipe(&dir);
        std::fs::create_dir_all(&dir).expect("made");
        let payloads = Payloads::new(0x9a27);
        // the journal ring, the shared staging file and each writer's own, all written once
        let journal = Rc::new(io::open(&dir.join("journal"), true).await);
        io::zero_fill(&journal, if quick { 64 << 20 } else { RING }).await;
        let stage = Rc::new(io::open(&dir.join("stage"), true).await);
        io::write_body(&stage, &payloads, SLOT * SLOTS as u64, 0).await;
        stage.fdatasync().await.expect("synced");
        let mut own = Vec::new();
        for writer in 0..6 {
            let file = io::open(&dir.join(format!("stage-{writer}")), true).await;
            io::write_body(&file, &payloads, SLOT, 0).await;
            file.fdatasync().await.expect("synced");
            own.push(file);
        }
        let own = Rc::new(own);
        // each side's chunks; J′'s cloned into eight times each before anything is timed
        let mut sets = Vec::new();
        for way in &ways {
            let count = if quick { 12 } else { CHUNKS };
            let chunks = make_chunks(&dir.join(way.chunks()), count, &payloads).await;
            if *way == Way::JournalCloned {
                let mut rng = Rng::new(0xc10e);
                for chunk in &chunks {
                    for _ in 0..8 {
                        let offset = HEADER + rng.below(CHUNK / (64 << 10)) * (64 << 10);
                        io::clone(&stage, HEADER, 64 << 10, chunk, offset).await.expect("cloned");
                    }
                    chunk.fdatasync().await.expect("synced");
                }
            }
            sets.push(Rc::new(chunks));
        }
        let mut outs = Vec::new();
        for &size in &sizes {
            for writers in [1_usize, 6] {
                let count = if quick { 16 } else { ((budget / size) as usize).clamp(200, 1000) };
                for way in ordered(&ways, round) {
                    // J′ and the files of their own are six-writer sides only
                    if writers == 1 && matches!(way, Way::JournalCloned | Way::ClonePerSlot) {
                        continue;
                    }
                    let set = &sets[ways.iter().position(|known| *known == way).expect("a set")];
                    // the files a side shares, moved into its bench for the side and back
                    let bench = Rc::new(Bench {
                        chunks: set.iter().map(|chunk| chunk.dup().expect("duplicated")).collect(),
                        journal: journal.clone(),
                        ring: Ring::wrapping(if quick { 64 << 20 } else { RING }),
                        journal_sync: Committer::start(journal.clone()),
                        stage: stage.clone(),
                        stage_sync: Committer::start(stage.clone()),
                        own: own.iter().map(|file| file.dup().expect("duplicated")).collect(),
                        payloads: Payloads::new(size),
                        size,
                        way,
                    });
                    let out = run_side(bench.clone(), &dir, &devices, writers, count).await;
                    bench.journal_sync.stop().await;
                    bench.stage_sync.stop().await;
                    outs.push(out);
                }
            }
        }
        drop(sets);
        drop(own);
        io::wipe(&dir);
        outs
    });
    let mut table = Table::new(&[
        "size", "in flight", "side", "stage p50", "stage p99", "apply p50", "apply p99",
        "apply's fdatasync p50", "p99", "clone p50", "clone wait p50", "release p50",
        "total p50", "total p99", "dev KiB/write", "÷ payload", "flushes/write", "cpu µs/write",
    ]);
    let mut records = Vec::new();
    for out in outs {
        let (size, writers) = out.cell.split_once(' ').unwrap_or((&out.cell, ""));
        table.row(vec![
            size.trim_start_matches("size=").to_string(),
            writers.trim_start_matches("inflight=").to_string(),
            out.side.clone(),
            fmt(out.get("stage_p50")),
            fmt(out.get("stage_p99")),
            fmt(out.get("apply_p50")),
            fmt(out.get("apply_p99")),
            fmt(out.get("apply_sync_p50")),
            fmt(out.get("apply_sync_p99")),
            fmt(out.get("clone_p50")),
            fmt(out.get("clone_wait_p50")),
            fmt(out.get("release_p50")),
            fmt(out.get("total_p50")),
            fmt(out.get("total_p99")),
            fmt(out.get("dev_kib")),
            fmt(out.get("dev_ratio")),
            fmt(out.get("flushes")),
            fmt(out.get("cpu_us")),
        ]);
        records.push(ctx.record("partial", round, out));
    }
    let clone = match &ctx.probes.clone {
        Ok(()) => "the filesystem clones".to_string(),
        Err(error) => format!("the clone sides are not run: FICLONERANGE refused, {error}"),
    };
    print!(
        "{}",
        table.render(
            &format!("3. A partial write, round {round} (latencies µs; whole units, no read-modify-write; {clone})"),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
