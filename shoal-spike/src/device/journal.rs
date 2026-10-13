//! Measurement 2: the journal a slice stages partial updates in
//!
//! S6's journal is a few files written ahead and overwritten, so that a stage is the cheap sync
//! and not the dear one, with one `fdatasync` covering every record staged since the last. This
//! measures that against a journal that appends, and one allocated ahead and never written,
//! whose first write into each block converts it. Two writers: each stager writing its own
//! record while a committer syncs whatever has completed, and one writer that copies the
//! pending records into one buffer and writes and syncs them together, as the WAL does. Both
//! keep F60's rule: a record is durable only through a flush that began after its write
//! completed.
//!
//! Beside a rotational disk, X7 adds S6's journal on the host's SSD (`ssd-written-ahead`), which
//! is the choice Q23 weighs, and runs the written-ahead journal again with the disk's write cache
//! off (`-wt`).

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::channel::{mpsc, oneshot};
use futures::future::join_all;
use futures::StreamExt;
use glommio::io::DmaFile;
use glommio::Task;

use super::counters::Devices;
use super::io::{self, Payloads, HEADER};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{drain, on_core, ordered, settle, size_name, sys, Ctx, SideOut};

/// The payloads a record carries after its header block
const RECORDS: &[u64] = &[4 << 10, 16 << 10, 64 << 10];

/// Stagers in flight at once
const STAGERS: &[usize] = &[1, 6, 64];

/// The ring a written-ahead journal wraps around
const RING: u64 = 1 << 30;

/// The most an appended or allocated journal grows to in one cell
const CAP: u64 = 512 << 20;

/// Where a journal's offsets come from
pub struct Ring {
    /// The next offset
    next: Cell<u64>,
    /// The length it wraps at, if it wraps
    wrap: Option<u64>,
    /// The length it stops at, if it does not wrap
    cap: u64,
}

impl Ring {
    /// A journal that wraps at a length
    ///
    /// # Arguments
    ///
    /// * `len` - The length
    #[must_use]
    pub fn wrapping(len: u64) -> Self {
        Ring { next: Cell::new(0), wrap: Some(len), cap: u64::MAX }
    }

    /// A journal that grows to a length and stops
    ///
    /// # Arguments
    ///
    /// * `cap` - The length
    #[must_use]
    pub fn growing(cap: u64) -> Self {
        Ring { next: Cell::new(0), wrap: None, cap }
    }

    /// Take the next run of bytes, or `None` once a growing journal is full
    ///
    /// # Arguments
    ///
    /// * `len` - The run's length
    pub fn take(&self, len: u64) -> Option<u64> {
        let mut at = self.next.get();
        if let Some(wrap) = self.wrap {
            // a record never straddles the end; it starts again at the front
            if at + len > wrap {
                at = 0;
            }
        } else if at + len > self.cap {
            return None;
        }
        self.next.set(at + len);
        Some(at)
    }
}

/// A committer that syncs a file for every batch of writes that completed before it began
pub struct Committer {
    /// Where a write that completed asks to be made durable
    tx: RefCell<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
    /// The committer's task, returning its syncs and the records each covered
    task: RefCell<Option<Task<Vec<(Instant, usize)>>>>,
}

impl Committer {
    /// Start a committer for a file
    ///
    /// # Arguments
    ///
    /// * `file` - The file it syncs
    #[must_use]
    pub fn start(file: Rc<DmaFile>) -> Rc<Committer> {
        let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<()>>();
        let task = glommio::spawn_local(async move {
            let mut syncs = Vec::new();
            // one sync for everything that has completed, then the next batch
            while let Some(first) = rx.next().await {
                let mut batch = vec![first];
                while let Ok(Some(more)) = rx.try_next() {
                    batch.push(more);
                }
                file.fdatasync().await.expect("the journal is synced");
                syncs.push((Instant::now(), batch.len()));
                for waiter in batch {
                    let _ = waiter.send(());
                }
            }
            syncs
        });
        Rc::new(Committer {
            tx: RefCell::new(Some(tx)),
            task: RefCell::new(Some(task)),
        })
    }

    /// Wait until a write that has completed is durable
    pub async fn durable(&self) {
        let (waiter, done) = oneshot::channel();
        self.tx
            .borrow()
            .as_ref()
            .expect("the committer runs")
            .unbounded_send(waiter)
            .expect("the committer listens");
        done.await.expect("the committer answers");
    }

    /// Stop the committer and return each sync's end and how many records it covered
    pub async fn stop(&self) -> Vec<(Instant, usize)> {
        self.tx.borrow_mut().take();
        let task = self.task.borrow_mut().take().expect("stopped once");
        task.await
    }
}

/// How a journal file is prepared
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prep {
    /// A new file that grows with every record
    Appended,
    /// A new file allocated to its length and never written: every record converts blocks
    Allocated,
    /// A file filled with zeros once and overwritten as a ring: S6's journal
    Written,
    /// The same journal on the host's SSD, beside a disk being measured
    WrittenSsd,
}

/// Who writes a record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Each stager writes its own record; a committer syncs what completed
    Each,
    /// One writer copies the pending records into one buffer, writes it and syncs
    Coalesced,
}

/// The name of a side
///
/// # Arguments
///
/// * `prep` - How the file was prepared
/// * `mode` - Who writes
fn side_name(prep: Prep, mode: Mode) -> String {
    let prep = match prep {
        Prep::Appended => "appended",
        Prep::Allocated => "allocated",
        Prep::Written => "written-ahead",
        Prep::WrittenSsd => "ssd-written-ahead",
    };
    let mode = match mode {
        Mode::Each => "each",
        Mode::Coalesced => "coalesced",
    };
    format!("{prep}/{mode}")
}

/// What one stager saw
#[derive(Debug, Default)]
struct Seen {
    /// Latencies of records begun after the warm-up
    warm: Samples,
    /// Latencies of every record
    all: Samples,
}

/// Run one cell's side on the executor this is called on
///
/// # Arguments
///
/// * `dir` - The journal's directory
/// * `devices` - The devices to count on
/// * `ring` - The written-ahead ring, shared by the round's cells
/// * `record` - The payload a record carries
/// * `stagers` - Stagers in flight
/// * `prep` - How the file is prepared
/// * `mode` - Who writes
/// * `window` - How long the cell runs
/// * `cap` - How far an appended or allocated file may grow
#[allow(clippy::too_many_arguments)]
async fn run_side(
    dir: PathBuf,
    devices: Devices,
    ring: Option<Rc<DmaFile>>,
    record: u64,
    stagers: usize,
    prep: Prep,
    mode: Mode,
    window: Duration,
    cap: u64,
) -> SideOut {
    let len = HEADER + record;
    let payloads = Rc::new(Payloads::new(record));
    // the file, prepared as the side says
    let (file, offsets) = match prep {
        Prep::Written | Prep::WrittenSsd => (ring.expect("the ring is made"), Ring::wrapping(RING)),
        Prep::Appended => (Rc::new(io::open(&dir.join("appended"), true).await), Ring::growing(cap)),
        Prep::Allocated => {
            let file = io::open(&dir.join("allocated"), true).await;
            file.pre_allocate(cap, false).await.expect("allocated");
            (Rc::new(file), Ring::growing(cap))
        }
    };
    let offsets = Rc::new(offsets);
    let _ = payloads.get(len);
    settle(&dir).await;
    let before = devices.snap();
    let thread_before = sys::thread_cpu_ns();
    let start = Instant::now();
    let warm = start + window / 10;
    let deadline = start + window;
    let (syncs, seen) = match mode {
        Mode::Each => {
            let committer = Committer::start(file.clone());
            let tasks: Vec<_> = (0..stagers)
                .map(|_| {
                    let (file, offsets, payloads, committer) =
                        (file.clone(), offsets.clone(), payloads.clone(), committer.clone());
                    glommio::spawn_local(async move {
                        let mut seen = Seen::default();
                        while Instant::now() < deadline {
                            // a place, the record written there, then its sync
                            let Some(at) = offsets.take(len) else { break };
                            let begun = Instant::now();
                            file.write_rc_at(payloads.get(len), at).await.expect("written");
                            committer.durable().await;
                            let took = begun.elapsed();
                            seen.all.push(took);
                            if begun >= warm {
                                seen.warm.push(took);
                            }
                        }
                        seen
                    })
                })
                .collect();
            let seen = join_all(tasks).await;
            (committer.stop().await, seen)
        }
        Mode::Coalesced => {
            let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<bool>>();
            // the one writer
            let writer = {
                let (file, offsets, payloads) = (file.clone(), offsets.clone(), payloads.clone());
                glommio::spawn_local(async move {
                    let mut syncs = Vec::new();
                    while let Some(first) = rx.next().await {
                        let mut batch = vec![first];
                        while let Ok(Some(more)) = rx.try_next() {
                            batch.push(more);
                        }
                        // the batch copied into one buffer, which a growing journal may refuse
                        let total = len * batch.len() as u64;
                        let Some(at) = offsets.take(total) else {
                            for waiter in batch {
                                let _ = waiter.send(false);
                            }
                            continue;
                        };
                        let mut buffer = glommio::allocate_dma_buffer(total as usize);
                        let source = payloads.get(len);
                        for piece in buffer.as_bytes_mut().chunks_mut(len as usize) {
                            piece.copy_from_slice(source.as_bytes());
                        }
                        file.write_at(buffer, at).await.expect("written");
                        file.fdatasync().await.expect("synced");
                        syncs.push((Instant::now(), batch.len()));
                        for waiter in batch {
                            let _ = waiter.send(true);
                        }
                    }
                    syncs
                })
            };
            let tasks: Vec<_> = (0..stagers)
                .map(|_| {
                    let tx = tx.clone();
                    glommio::spawn_local(async move {
                        let mut seen = Seen::default();
                        while Instant::now() < deadline {
                            let begun = Instant::now();
                            let (waiter, done) = oneshot::channel();
                            tx.unbounded_send(waiter).expect("the writer listens");
                            if !done.await.unwrap_or(false) {
                                break;
                            }
                            let took = begun.elapsed();
                            seen.all.push(took);
                            if begun >= warm {
                                seen.warm.push(took);
                            }
                        }
                        seen
                    })
                })
                .collect();
            drop(tx);
            let seen = join_all(tasks).await;
            (writer.await, seen)
        }
    };
    finish(&dir, &devices, before, thread_before, start, warm, syncs, seen, record, stagers, prep, mode)
}

/// Turn what a side saw into its figures
///
/// # Arguments
///
/// * `dir` - The journal's directory
/// * `devices` - The devices counted on
/// * `before` - The counters at the start
/// * `thread_before` - The executor's CPU at the start
/// * `start` - When the cell started
/// * `warm` - When the warm-up ended
/// * `syncs` - Each sync's end and the records it covered
/// * `seen` - What each stager saw
/// * `record` - The payload
/// * `stagers` - Stagers in flight
/// * `prep` - How the file was prepared
/// * `mode` - Who wrote
#[allow(clippy::too_many_arguments)]
fn finish(
    dir: &std::path::Path,
    devices: &Devices,
    before: super::counters::Snap,
    thread_before: u64,
    start: Instant,
    warm: Instant,
    syncs: Vec<(Instant, usize)>,
    seen: Vec<Seen>,
    record: u64,
    stagers: usize,
    prep: Prep,
    mode: Mode,
) -> SideOut {
    let end = Instant::now();
    let exec_ns = sys::thread_cpu_ns() - thread_before;
    let drain_ms = drain(dir);
    let delta = before.delta(&devices.snap());
    // every stager's latencies together; the warm ones if there are enough
    let mut warm_samples = Samples::default();
    let mut all_samples = Samples::default();
    for stager in &seen {
        warm_samples.extend(&stager.warm);
        all_samples.extend(&stager.all);
    }
    let total = all_samples.0.len();
    let (samples, from) = if warm_samples.0.len() >= 100 { (warm_samples, warm) } else { (all_samples, start) };
    let summary = samples.summary();
    let window = end.duration_since(from).as_secs_f64();
    let warm_syncs: Vec<&(Instant, usize)> = syncs.iter().filter(|(at, _)| *at >= from).collect();
    let covered: usize = warm_syncs.iter().map(|(_, records)| records).sum();
    let mut sizes: Vec<usize> = warm_syncs.iter().map(|(_, records)| *records).collect();
    sizes.sort_unstable();
    SideOut::new(
        format!("record={} stagers={stagers}", size_name(record)),
        side_name(prep, mode),
        &[
            ("records", summary.n as f64),
            ("window_s", window),
            ("records_s", summary.n as f64 / window),
            ("syncs_s", warm_syncs.len() as f64 / window),
            ("per_sync_mean", covered as f64 / warm_syncs.len().max(1) as f64),
            ("per_sync_p50", sizes.get(sizes.len() / 2).copied().unwrap_or(0) as f64),
            ("lat_p50", summary.p50),
            ("lat_p99", summary.p99),
            ("lat_p999", summary.p999),
            ("lat_max", summary.max),
            ("dev_kib", delta.kib_per(total)),
            ("flushes", delta.flushes_per(total)),
            ("exec_cpu_us", exec_ns as f64 / 1e3 / total.max(1) as f64),
            ("cpu_us", delta.cpu_us_per(total)),
            ("drain_ms", drain_ms),
        ],
    )
}

/// Run measurement 2 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let quick = ctx.quick;
    let records: Vec<u64> = if quick { vec![4 << 10] } else { RECORDS.to_vec() };
    let stagers: Vec<usize> = if quick { vec![1, 6] } else { STAGERS.to_vec() };
    let mut sides: Vec<(Prep, Mode)> = [Prep::Written, Prep::Appended, Prep::Allocated]
        .into_iter()
        .flat_map(|prep| [(prep, Mode::Each), (prep, Mode::Coalesced)])
        .collect();
    // beside a disk, the same journal on the host's SSD, which is where Q23 would put it
    if ctx.rotational() && ctx.ssd.is_some() {
        sides.push((Prep::WrittenSsd, Mode::Each));
    }
    run_with(ctx, round, &records, &stagers, &sides, "");
}

/// Run the written-ahead journal with the disk's write cache off, its sides named `-wt`
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run_wcoff(ctx: &Ctx, round: u32) {
    let records: Vec<u64> = if ctx.quick { vec![4 << 10] } else { vec![4 << 10, 16 << 10] };
    run_with(ctx, round, &records, &[1, 6], &[(Prep::Written, Mode::Each)], "-wt");
}

/// Run a set of journal sides for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
/// * `records` - The payloads
/// * `stagers` - The stagers in flight
/// * `sides` - How each side's file is prepared and written
/// * `suffix` - What every side's name ends with
fn run_with(ctx: &Ctx, round: u32, records: &[u64], stagers: &[usize], sides: &[(Prep, Mode)], suffix: &str) {
    let dir = ctx.sub("journal");
    let (devices, quick, window) = (ctx.facts.devices.clone(), ctx.quick, ctx.window(Duration::from_secs(3)));
    let cap = if quick { 32 << 20 } else { CAP };
    let ring_len = if quick { 64 << 20 } else { RING };
    // the SSD's ring and devices, when a side journals there
    let ssd = sides
        .iter()
        .any(|(prep, _)| *prep == Prep::WrittenSsd)
        .then(|| ctx.ssd.as_ref().map(|ssd| (ssd.dir.join("journal"), ssd.facts.devices.clone())))
        .flatten();
    let (records, stagers, sides) = (records.to_vec(), stagers.to_vec(), sides.to_vec());
    // every cell runs on one executor, which keeps the rings
    let outs = on_core(ctx.core, ctx.sibling, move || async move {
        io::wipe(&dir);
        std::fs::create_dir_all(&dir).expect("made");
        let ring = Rc::new(io::open(&dir.join("ring"), true).await);
        io::zero_fill(&ring, ring_len).await;
        let ssd_ring = match &ssd {
            Some((ssd_dir, _)) => {
                io::wipe(ssd_dir);
                std::fs::create_dir_all(ssd_dir).expect("made");
                let file = Rc::new(io::open(&ssd_dir.join("ring"), true).await);
                io::zero_fill(&file, ring_len).await;
                Some(file)
            }
            None => None,
        };
        let mut outs = Vec::new();
        for &record in &records {
            for &count in &stagers {
                for (prep, mode) in ordered(&sides, round) {
                    // the ring and the devices the side writes to
                    let (side_ring, side_devices) = match prep {
                        Prep::Written => (Some(ring.clone()), devices.clone()),
                        Prep::WrittenSsd => (
                            ssd_ring.clone(),
                            ssd.as_ref().map_or_else(|| devices.clone(), |(_, ssd_devices)| ssd_devices.clone()),
                        ),
                        _ => (None, devices.clone()),
                    };
                    outs.push(run_side(dir.clone(), side_devices, side_ring, record, count, prep, mode, window, cap).await);
                    // the growing files go before the next side
                    for name in ["appended", "allocated"] {
                        let _ = std::fs::remove_file(dir.join(name));
                    }
                }
            }
        }
        drop(ring);
        drop(ssd_ring);
        io::wipe(&dir);
        if let Some((ssd_dir, _)) = &ssd {
            io::wipe(ssd_dir);
        }
        outs
    });
    let mut table = Table::new(&[
        "record", "stagers", "file/writer", "records/s", "syncs/s", "records/sync", "p50 µs",
        "p99 µs", "p99.9 µs", "dev KiB/record", "flushes/record", "cpu µs/record",
    ]);
    let mut records_out = Vec::new();
    let mut outs = outs;
    outs.sort_by(|a, b| (a.cell.clone(), a.side.clone()).cmp(&(b.cell.clone(), b.side.clone())));
    for mut out in outs {
        out.side.push_str(suffix);
        let (record, stagers) = out.cell.split_once(' ').unwrap_or((&out.cell, ""));
        table.row(vec![
            record.trim_start_matches("record=").to_string(),
            stagers.trim_start_matches("stagers=").to_string(),
            out.side.clone(),
            fmt(out.get("records_s")),
            fmt(out.get("syncs_s")),
            fmt(out.get("per_sync_mean")),
            fmt(out.get("lat_p50")),
            fmt(out.get("lat_p99")),
            fmt(out.get("lat_p999")),
            fmt(out.get("dev_kib")),
            fmt(out.get("flushes")),
            fmt(out.get("cpu_us")),
        ]);
        records_out.push(ctx.record("journal", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("2. The journal{suffix}, round {round} (a 4 KiB header block on every record; write cache {})", ctx.facts.write_cache),
            &ctx.label()
        )
    );
    ctx.emit(&records_out);
}
