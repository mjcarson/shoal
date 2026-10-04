//! Measurement 8: one device, driven by one, two and four executors
//!
//! A slice is one executor's part of a device. S4 gives a device one slice unless one core
//! cannot drive it, and then several, each owned by its own executor; the number found here is
//! what the inventory wizard offers. Each slice runs on its own physical core with its own
//! directory and population, and every slice starts and stops on one clock. Reads hold every
//! chunk open, so a read is the device's and the core's and nothing else's; writes are the whole
//! chunk cycle, six in flight, one directory sync a batch. `r64-T` and `r4-T` hold the total depth
//! at 32 however many slices there are, and `w1M-T` and `w4M-T` four batches of six, which is what
//! tells a core that runs out from a queue that is too shallow (T2).
//!
//! A slice's CPU is read three ways: its executor thread's CPU time, glommio's own runtime (tasks
//! run and rings polled, without the time spent waiting), and how often the thread slept. The
//! first is not a measure of need: when completions come back within tens of microseconds the
//! reactor finds one before it sleeps, so the thread stays on its cpu whatever the load. A
//! checksum's cost is added to the runtime by arithmetic from X5's rate, since adding the
//! checksum crate is M13's to do.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::{Directory, DmaFile};

use super::counters::{cpu_busy, cpu_jiffies};
use super::facts::Facts;
use super::io::{self, DirSync, Payloads, HEADER};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{on_core, spawn_on, sys, Ctx, SideOut};

/// A population chunk's units
const CHUNK: u64 = 4 << 20;

/// Chunks a slice's population holds: a gibibyte
const POPULATION: usize = 256;

/// The total depth `r64-T` holds
const TOTAL_DEPTH: usize = 32;

/// The most a write cell may put on the filesystem, across every slice: room for the
/// Optane's rate over the whole cell, so it bounds space and never cuts a window short
const WRITE_CAP: u64 = 24 << 30;

/// The batches of six the `-T` write controls hold in flight across every slice
const WRITE_LOOPS: usize = 4;

/// Object directories a writing slice rotates through
const OBJECTS: usize = 64;

/// What a slice does
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Work {
    /// Random reads of a unit, at a depth
    Read {
        /// The unit
        unit: u64,
        /// Reads in flight on the slice
        depth: usize,
    },
    /// Whole chunks written, six at a time, one directory sync a batch
    Write {
        /// The chunk's units
        size: u64,
        /// Batches of six in flight on the slice
        loops: usize,
    },
}

/// One slice's results
#[derive(Debug, Default)]
struct SliceOut {
    /// Operations completed in the window
    ops: usize,
    /// Bytes they moved
    bytes: u64,
    /// Their latencies
    samples: Samples,
    /// The executor thread's CPU over the window, as a fraction of it
    busy: f64,
    /// The executor's own runtime over the window, as a fraction of it: running tasks and
    /// polling the rings, without the time it spun or slept waiting for a completion
    run: f64,
    /// Times the executor thread gave up its cpu of its own accord in the window: a reactor
    /// that sleeps between completions does it about once an operation, one that spins never
    sleeps: u64,
}

/// The directory of a slice
///
/// # Arguments
///
/// * `root` - The measurement's directory
/// * `slice` - The slice
fn slice_dir(root: &Path, slice: usize) -> PathBuf {
    root.join(format!("slice-{slice}"))
}

/// Make a slice's population if it is not there
///
/// # Arguments
///
/// * `dir` - The slice's directory
/// * `count` - Chunks in it
async fn populate(dir: PathBuf, count: usize) {
    let marker = dir.join("complete");
    if marker.exists() {
        return;
    }
    let pop = dir.join("pop");
    io::wipe(&pop);
    std::fs::create_dir_all(&pop).expect("made");
    let payloads = Payloads::new(0x51ce);
    for index in 0..count {
        let file = io::open(&pop.join(format!("{index:06}.0")), true).await;
        io::write_chunk(&file, &payloads, CHUNK, 0).await;
        file.fdatasync().await.expect("synced");
        file.close().await.expect("closed");
    }
    std::fs::write(&marker, count.to_string()).expect("marked");
}

/// Sleep on the executor until an instant
///
/// # Arguments
///
/// * `at` - The instant
async fn until(at: Instant) {
    let now = Instant::now();
    if at > now {
        glommio::timer::sleep(at - now).await;
    }
}

/// One slice's run, on its own executor
///
/// # Arguments
///
/// * `dir` - The slice's directory
/// * `work` - What it does
/// * `count` - Chunks in its population
/// * `form` - The directory sync
/// * `start` - When every slice starts
/// * `warm` - When the window opens
/// * `end` - When it closes
/// * `cap` - The most a writing slice writes
#[allow(clippy::too_many_arguments)]
async fn slice(
    dir: PathBuf,
    work: Work,
    count: usize,
    form: DirSync,
    start: Instant,
    warm: Instant,
    end: Instant,
    cap: u64,
) -> SliceOut {
    // the executor's CPU at the window's edges, read on its own thread
    let cpu = glommio::spawn_local(async move {
        until(warm).await;
        let at_warm = sys::thread_cpu_ns();
        let sleeps_at_warm = sys::voluntary_switches();
        // taking the stats resets them, so the second take is the window's
        let _ = glommio::executor().executor_stats();
        until(end).await;
        let runtime = glommio::executor().executor_stats().total_runtime();
        (sys::thread_cpu_ns() - at_warm, runtime, sys::voluntary_switches() - sleeps_at_warm)
    });
    let mut out = match work {
        Work::Read { unit, depth } => {
            // every chunk held open, so a read is the device's
            let mut files = Vec::with_capacity(count);
            for index in 0..count {
                files.push(io::open_read(&dir.join("pop").join(format!("{index:06}.0"))).await);
            }
            let files = Rc::new(files);
            until(start).await;
            let tasks = (0..depth).map(|task| {
                let files = files.clone();
                glommio::spawn_local(async move {
                    let mut rng = Rng::new(unit ^ (task as u64) << 32);
                    let mut out = SliceOut::default();
                    while Instant::now() < end {
                        let file: &DmaFile = &files[rng.below(files.len() as u64) as usize];
                        let offset = HEADER + rng.below(CHUNK / unit) * unit;
                        let begun = Instant::now();
                        file.read_at_aligned(offset, unit as usize).await.expect("read");
                        let done = Instant::now();
                        if begun >= warm && done <= end {
                            out.ops += 1;
                            out.bytes += unit;
                            out.samples.push(done - begun);
                        }
                    }
                    out
                })
            });
            let outs = join_all(tasks).await;
            for file in Rc::try_unwrap(files).ok().expect("every reader is done") {
                file.close().await.expect("closed");
            }
            merge(outs)
        }
        Work::Write { size, loops } => {
            let w = dir.join("w");
            io::wipe(&w);
            let mut objects = Vec::with_capacity(OBJECTS);
            for object in 0..OBJECTS {
                let path = w.join(format!("{object:016x}"));
                std::fs::create_dir_all(&path).expect("made");
                objects.push(Rc::new(Directory::open(&path).await.expect("opened")));
            }
            let objects = Rc::new(objects);
            let payloads = Rc::new(Payloads::new(size));
            let _ = (payloads.get(HEADER), payloads.get(size.min(io::PIECE)));
            until(start).await;
            let written = Rc::new(Cell::new(0_u64));
            let next = Rc::new(Cell::new(0_usize));
            // each loop writes batches of six, one directory sync a batch, until the window closes
            let tasks = (0..loops).map(|_| {
                let (w, objects, payloads, written, next) =
                    (w.clone(), objects.clone(), payloads.clone(), written.clone(), next.clone());
                glommio::spawn_local(async move {
                    let mut out = SliceOut::default();
                    while Instant::now() < end && written.get() < cap {
                        let batch = next.get();
                        next.set(batch + 1);
                        let object = batch % OBJECTS;
                        let path = w.join(format!("{object:016x}"));
                        let begun = Instant::now();
                        // six chunks at once, then the one directory sync that acknowledges them
                        join_all((0..6).map(|position| {
                            let (path, payloads) = (path.clone(), payloads.clone());
                            async move {
                                let name = format!("{batch:06}.{position}");
                                let file = io::open(&path.join(format!("{name}.7")), true).await;
                                file.pre_allocate(size + HEADER, false).await.expect("allocated");
                                io::write_chunk(&file, &payloads, size, 0).await;
                                file.fdatasync().await.expect("synced");
                                file.rename(path.join(name)).await.expect("renamed");
                                file.close().await.expect("closed");
                            }
                        }))
                        .await;
                        io::sync_dir(&objects[object], form).await;
                        let done = Instant::now();
                        written.set(written.get() + 6 * size);
                        if begun >= warm && done <= end {
                            out.ops += 6;
                            out.bytes += 6 * size;
                            for _ in 0..6 {
                                out.samples.push(done - begun);
                            }
                        }
                    }
                    out
                })
            });
            let outs = join_all(tasks).await;
            drop(objects);
            merge(outs)
        }
    };
    let window = end.duration_since(warm).as_secs_f64();
    let (busy, runtime, sleeps) = cpu.await;
    out.sleeps = sleeps;
    out.busy = busy as f64 / 1e9 / window;
    out.run = runtime.as_secs_f64() / window;
    if let Work::Write { .. } = work {
        io::wipe(&dir.join("w"));
    }
    out
}

/// Merge the reader tasks' results of one slice
///
/// # Arguments
///
/// * `outs` - Each task's
fn merge(outs: Vec<SliceOut>) -> SliceOut {
    let mut all = SliceOut::default();
    for out in outs {
        all.ops += out.ops;
        all.bytes += out.bytes;
        all.samples.extend(&out.samples);
    }
    all
}

/// Run one cell: `slices` executors doing one kind of work, together
///
/// # Arguments
///
/// * `ctx` - The run
/// * `name` - The workload's name
/// * `work` - What each slice does
/// * `slices` - How many slices
/// * `count` - Chunks in each population
fn cell(ctx: &Ctx, name: &str, work: Work, slices: usize, count: usize) -> SideOut {
    // a write cell is shorter, since it writes for as long as it runs: a byte cap would stop a
    // fast device inside its warm-up and leave the window empty
    let (warm_for, window) = match work {
        Work::Read { .. } => (Duration::from_secs(2), Duration::from_secs(8)),
        Work::Write { .. } => (Duration::from_secs(1), Duration::from_secs(4)),
    };
    let (warm_for, window) = (ctx.window(warm_for), ctx.window(window));
    let start = Instant::now() + Duration::from_millis(1500);
    let (warm, end) = (start + warm_for, start + warm_for + window);
    // a bound on the space a cell can fill, never on its time
    let cap = WRITE_CAP / slices as u64;
    let root = ctx.sub("slices");
    // every slice on its own core, its blocking thread on the sibling
    let handles: Vec<_> = (0..slices)
        .map(|index| {
            let (cpu, sibling) = ctx.order[index];
            let (dir, form) = (slice_dir(&root, index), ctx.dir_sync());
            spawn_on(cpu, sibling, move || slice(dir, work, count, form, start, warm, end, cap))
        })
        .collect();
    // the device and the cpus at the window's edges, read from here
    std::thread::sleep(warm.saturating_duration_since(Instant::now()));
    let (before, jiffies) = (ctx.facts.devices.snap(), cpu_jiffies());
    std::thread::sleep(end.saturating_duration_since(Instant::now()));
    let (after, jiffies_after) = (ctx.facts.devices.snap(), cpu_jiffies());
    let outs: Vec<SliceOut> = handles.into_iter().map(|handle| handle.join().expect("a slice finishes")).collect();
    let delta = before.delta(&after);
    let seconds = window.as_secs_f64();
    let mut samples = Samples::default();
    let (mut ops, mut bytes) = (0, 0);
    let (mut busy_sum, mut busy_max, mut projected_max, mut sustainable) = (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
    let mut run_max = 0.0_f64;
    let mut sleeps = 0;
    let mut core_max = 0.0_f64;
    for (index, out) in outs.iter().enumerate() {
        samples.extend(&out.samples);
        ops += out.ops;
        bytes += out.bytes;
        // the checksum's cost, projected onto the slice's own core, over the executor's runtime
        let rate = out.bytes as f64 / seconds;
        let projected = out.run + rate / (ctx.crc_gibs * f64::from(1 << 30));
        run_max = run_max.max(out.run);
        sleeps += out.sleeps;
        busy_sum += out.busy;
        busy_max = busy_max.max(out.busy);
        projected_max = projected_max.max(projected);
        sustainable += rate / projected.max(1.0);
        core_max = core_max.max(cpu_busy(&jiffies, &jiffies_after, ctx.order[index].0));
    }
    let summary = samples.summary();
    SideOut::new(
        format!("{name} slices={slices}"),
        name,
        &[
            ("slices", slices as f64),
            ("depth", match work { Work::Read { depth, .. } => depth as f64, Work::Write { loops, .. } => 6.0 * loops as f64 }),
            ("mib_s", bytes as f64 / seconds / f64::from(1 << 20)),
            ("ops_s", ops as f64 / seconds),
            ("p50", summary.p50),
            ("p99", summary.p99),
            ("p999", summary.p999),
            ("busy_mean", busy_sum / slices as f64),
            ("busy_max", busy_max),
            ("run_max", run_max),
            ("sleeps_per_op", sleeps as f64 / ops.max(1) as f64),
            ("core_max", core_max),
            ("process_cores", delta.cpu_ns as f64 / 1e9 / delta.secs),
            ("projected_max", projected_max),
            ("sustainable_mib_s", sustainable / f64::from(1 << 20)),
            ("dev_write_mib_s", delta.written as f64 / delta.secs / f64::from(1 << 20)),
            ("dev_read_mib_s", delta.read as f64 / delta.secs / f64::from(1 << 20)),
            ("flushes_s", delta.flushes as f64 / delta.secs),
        ],
    )
}

/// The device's ceiling by fio, for the same three shapes, if fio is installed
///
/// fio runs one job a physical core with io_uring and direct I/O on files of its own in the
/// same filesystem, and owes nothing to this harness.
///
/// # Arguments
///
/// * `ctx` - The run
fn fio(ctx: &Ctx) -> Vec<SideOut> {
    let dir = ctx.sub("fio");
    std::fs::create_dir_all(&dir).expect("made");
    let jobs = ctx.order.len().to_string();
    let runtime = if ctx.quick { "1" } else { "8" };
    let mut outs = Vec::new();
    for (name, rw, bs, depth, field) in [
        ("r64", "randread", "64k", "32", "read"),
        ("r4", "randread", "4k", "32", "read"),
        ("w-seq-1M", "write", "1m", "8", "write"),
    ] {
        let output = Command::new("fio")
            .args([
                "--name=x6", "--ioengine=io_uring", "--direct=1", "--time_based", "--group_reporting",
                "--output-format=json", "--size=1g", "--ramp_time=1",
            ])
            .arg(format!("--directory={}", dir.display()))
            .arg(format!("--rw={rw}"))
            .arg(format!("--bs={bs}"))
            .arg(format!("--iodepth={depth}"))
            .arg(format!("--numjobs={jobs}"))
            .arg(format!("--runtime={runtime}"))
            .output();
        let Ok(output) = output else {
            return Vec::new();
        };
        let json: shoal::serde_json::Value = match shoal::serde_json::from_slice(&output.stdout) {
            Ok(json) => json,
            Err(_) => continue,
        };
        let job = &json["jobs"][0][field];
        let bytes = job["bw_bytes"].as_f64().unwrap_or(0.0);
        let p99 = job["clat_ns"]["percentile"]["99.000000"].as_f64().unwrap_or(0.0) / 1e3;
        outs.push(SideOut::new(
            format!("fio {name}"),
            "fio",
            &[
                ("mib_s", bytes / f64::from(1 << 20)),
                ("ops_s", job["iops"].as_f64().unwrap_or(0.0)),
                ("p99", p99),
                ("jobs", ctx.order.len() as f64),
            ],
        ));
    }
    outs
}

/// Run measurement 8 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let count = ctx.count(POPULATION, 8);
    let most = ctx.slices.iter().copied().max().unwrap_or(1);
    // every slice's population, made once and kept
    for index in 0..most {
        let dir = slice_dir(&ctx.sub("slices"), index);
        on_core(ctx.core, ctx.sibling, move || populate(dir, count));
    }
    let gap = if ctx.quick { Duration::ZERO } else { Duration::from_secs(30) };
    let mut outs = Vec::new();
    // the device's own ceiling first
    outs.extend(fio(ctx));
    let workloads: Vec<(&str, Box<dyn Fn(usize) -> Work>)> = vec![
        ("r64", Box::new(|_| Work::Read { unit: 64 << 10, depth: 32 })),
        ("r64-T", Box::new(|slices| Work::Read { unit: 64 << 10, depth: (TOTAL_DEPTH / slices).max(1) })),
        ("r4", Box::new(|_| Work::Read { unit: 4 << 10, depth: 32 })),
        ("r4-T", Box::new(|slices| Work::Read { unit: 4 << 10, depth: (TOTAL_DEPTH / slices).max(1) })),
        ("w1M", Box::new(|_| Work::Write { size: 1 << 20, loops: 1 })),
        ("w1M-T", Box::new(|slices| Work::Write { size: 1 << 20, loops: (WRITE_LOOPS / slices).max(1) })),
        ("w4M", Box::new(|_| Work::Write { size: 4 << 20, loops: 1 })),
        ("w4M-T", Box::new(|slices| Work::Write { size: 4 << 20, loops: (WRITE_LOOPS / slices).max(1) })),
    ];
    let order: Vec<usize> = (0..workloads.len()).collect();
    for index in super::ordered(&order, round) {
        let (name, work) = &workloads[index];
        for &slices in &ctx.slices {
            let work = work(slices);
            outs.push(cell(ctx, name, work, slices, count));
            // a pause after writing, so the next cell does not meet the SSD's cache half full
            if let Work::Write { .. } = work {
                std::thread::sleep(gap);
            }
        }
    }
    // one slice at every depth
    for (name, unit) in [("depth-r64", 64 << 10), ("depth-r4", 4 << 10)] {
        let depths: Vec<usize> = if ctx.quick { vec![1, 32] } else { vec![1, 2, 4, 8, 16, 32, 64, 128] };
        for depth in depths {
            let mut out = cell(ctx, name, Work::Read { unit, depth }, 1, count);
            out.cell = format!("{name} depth={depth}");
            outs.push(out);
        }
    }
    // the sentinel: the first write cell again, to see whether the device has changed under the round
    let mut sentinel = cell(ctx, "w1M", Work::Write { size: 1 << 20, loops: 1 }, 1, count);
    sentinel.cell = "w1M slices=1 sentinel".to_string();
    sentinel.side = "w1M-sentinel".to_string();
    outs.push(sentinel);
    let mut table = Table::new(&[
        "cell", "MiB/s", "ops/s", "p50 µs", "p99 µs", "p99.9 µs", "thread CPU mean",
        "thread CPU max", "runtime max", "sleeps/op", "core busy max", "process cores", "runtime + checksum", "sustainable MiB/s",
        "device write MiB/s", "flushes/s",
    ]);
    let mut records = Vec::new();
    for out in outs {
        table.row(vec![
            out.cell.clone(),
            fmt(out.get("mib_s")),
            fmt(out.get("ops_s")),
            fmt(out.get("p50")),
            fmt(out.get("p99")),
            fmt(out.get("p999")),
            fmt(out.get("busy_mean")),
            fmt(out.get("busy_max")),
            fmt(out.get("run_max")),
            fmt(out.get("sleeps_per_op")),
            fmt(out.get("core_max")),
            fmt(out.get("process_cores")),
            fmt(out.get("projected_max")),
            fmt(out.get("sustainable_mib_s")),
            fmt(out.get("dev_write_mib_s")),
            fmt(out.get("flushes_s")),
        ]);
        records.push(ctx.record("slices", round, out));
    }
    let cpus: Vec<String> = ctx.order.iter().map(|(cpu, sibling)| format!("{cpu}/{}", sibling.map_or("-".into(), |s| s.to_string()))).collect();
    print!(
        "{}",
        table.render(
            &format!(
                "8. One device, several slices, round {round} (slices on cpus {}, blocking thread on the sibling; checksum at {} GiB/s a core)",
                cpus.join(", "),
                ctx.crc_gibs
            ),
            &ctx.label()
        )
    );
    ctx.emit(&records);
    if ctx.quick && !ctx.keep {
        io::wipe(&ctx.sub("slices"));
        io::wipe(&ctx.sub("fio"));
    }
}

/// Write one file sequentially until the device's write cache is spent, and print the rate of
/// every gibibyte
///
/// # Arguments
///
/// * `dir` - The scratch directory
/// * `core` - The cpu
/// * `sibling` - Its sibling
/// * `facts` - The label
pub fn slc_probe(dir: &Path, core: usize, sibling: Option<usize>, facts: &Facts) {
    let path = dir.join("slc-probe");
    let rates = on_core(core, sibling, move || async move {
        let file = io::open(&path, true).await;
        let payloads = Payloads::new(0x51c);
        let mut rates = Vec::new();
        for gib in 0..32_u64 {
            let start = Instant::now();
            io::write_body(&file, &payloads, 1 << 30, gib << 30).await;
            file.fdatasync().await.expect("synced");
            rates.push(1024.0 / start.elapsed().as_secs_f64());
        }
        file.close().await.expect("closed");
        let _ = std::fs::remove_file(&path);
        rates
    });
    let mut table = Table::new(&["GiB", "MiB/s"]);
    for (gib, rate) in rates.iter().enumerate() {
        table.row(vec![(gib + 1).to_string(), fmt(*rate)]);
    }
    print!("{}", table.render("The write cache: one file written sequentially, a GiB at a time", &facts.label()));
}
