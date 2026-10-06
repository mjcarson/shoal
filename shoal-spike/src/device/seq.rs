//! X7's first measurement: what the disk does with sequential and random direct I/O, by size and
//! by depth
//!
//! A rotational disk's rate depends on how far its arm moves between reads, and that is set by
//! the size of a read and how many are queued for the disk to reorder. Sequential writes and reads
//! give the ceiling, a file written and read back through glommio at three piece sizes and three
//! depths. Random reads over sixteen gibibytes spread across the filesystem give the cost of a
//! seek at each size and depth, and the same reads inside one gibibyte give the cost of a short
//! one. The size at which a random read reaches half and four fifths of the sequential rate is how
//! far a disk should read ahead (Q23). fio, one job on files of its own, is the harness-free
//! ceiling beside it.

use std::cell::Cell;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::io::DmaFile;

use super::arm::span_of_paths;
use super::counters::Devices;
use super::io::{self, Payloads};
use super::paced::{until, Window};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{on_core, ordered, size_name, sys, Ctx, SideOut};

/// The pieces a sequential cell writes and reads
const PIECES: &[u64] = &[128 << 10, 1 << 20, 4 << 20];

/// The depths every cell runs at
const DEPTHS: &[usize] = &[1, 4, 32];

/// The sizes a random read takes
const SIZES: &[u64] = &[4 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20];

/// Files in the random population, each in a directory of its own
const FILES: usize = 16;

/// Each file's length
const FILE: u64 = 1 << 30;

/// The most bytes a random cell's reads hold in flight, so a large read's depth stays in memory
const IN_FLIGHT: u64 = 128 << 20;

/// The most a sequential write cell writes
const SEQ_CAP: u64 = 4 << 30;

/// The random population's files
///
/// # Arguments
///
/// * `root` - The population's directory
fn population(root: &Path) -> Vec<PathBuf> {
    (0..FILES).map(|index| root.join(format!("pg{index:02}")).join("data")).collect()
}

/// Write the random population if it is not there: each file in a directory of its own, so a
/// filesystem that spreads directories spreads the files
///
/// # Arguments
///
/// * `root` - Its directory
/// * `len` - Each file's length
async fn populate(root: PathBuf, len: u64) {
    let marker = root.join("complete");
    if marker.exists() {
        return;
    }
    io::wipe(&root);
    let payloads = Payloads::new(0x5e9);
    for path in population(&root) {
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("made");
        let file = io::open(&path, true).await;
        io::write_body(&file, &payloads, len, 0).await;
        file.fdatasync().await.expect("synced");
        file.close().await.expect("closed");
    }
    std::fs::write(&marker, "done").expect("marked");
}

/// What one cell saw
#[derive(Debug, Default)]
struct Seen {
    /// Bytes moved in the counted window
    bytes: u64,
    /// Operations in it
    ops: usize,
    /// Their latencies
    samples: Samples,
}

/// Write a file sequentially, `depth` pieces in flight, from its start, until the window ends or
/// the cap is written, and sync it; or read it back the same way
///
/// # Arguments
///
/// * `path` - The file
/// * `piece` - Each operation's size
/// * `depth` - Operations in flight
/// * `write` - Whether it writes, or reads what a write left
/// * `count_for` - How long it runs
/// * `devices` - The devices
/// * `device_bytes` - The filesystem device's size, which the file's place is a fraction of
async fn sequential(path: PathBuf, piece: u64, depth: usize, write: bool, count_for: Duration, devices: Devices, device_bytes: u64) -> SideOut {
    let file = Rc::new(io::open(&path, write).await);
    // a read stops at what the write left
    let limit = if write { SEQ_CAP } else { file.file_size().await.expect("a size") };
    let payloads = Rc::new(Payloads::new(piece));
    let _ = payloads.get(piece);
    let next = Rc::new(Cell::new(0_u64));
    // the clock starts once the file is open, so a slow open is not in the window
    let window = Window::new(Duration::ZERO, count_for);
    until(window.start).await;
    let before = devices.snap();
    let started = Instant::now();
    // each task takes the next piece in order, so the disk sees one stream
    let tasks = (0..depth).map(|_| {
        let (file, payloads, next) = (file.clone(), payloads.clone(), next.clone());
        glommio::spawn_local(async move {
            let mut seen = Seen::default();
            while Instant::now() < window.end && next.get() + piece <= limit {
                let at = next.get();
                next.set(at + piece);
                let begun = Instant::now();
                if write {
                    file.write_rc_at(payloads.get(piece), at).await.expect("written");
                } else {
                    file.read_at_aligned(at, piece as usize).await.expect("read");
                }
                seen.samples.push(begun.elapsed());
                seen.bytes += piece;
                seen.ops += 1;
            }
            seen
        })
    });
    let seen = join_all(tasks).await;
    // a write is done when it is durable
    if write {
        file.fdatasync().await.expect("synced");
    }
    let secs = started.elapsed().as_secs_f64();
    let delta = before.delta(&devices.snap());
    let mut all = Seen::default();
    for part in seen {
        all.bytes += part.bytes;
        all.ops += part.ops;
        all.samples.extend(&part.samples);
    }
    // where the file starts on the disk, since a disk's outer tracks stream faster than its inner
    let at = sys::extent_map(file.as_raw_fd())
        .ok()
        .and_then(|extents| extents.first().map(|extent| extent.physical as f64 / device_bytes.max(1) as f64))
        .unwrap_or(0.0);
    Rc::try_unwrap(file).ok().expect("every task is done").close().await.expect("closed");
    let summary = all.samples.summary();
    SideOut::new(
        format!("{} piece={} depth={depth}", if write { "write" } else { "read" }, size_name(piece)),
        if write { "seq-write" } else { "seq-read" },
        &[
            ("mib_s", all.bytes as f64 / secs / f64::from(1 << 20)),
            ("ops_s", all.ops as f64 / secs),
            ("gib", all.bytes as f64 / f64::from(1 << 30)),
            ("p50", summary.p50),
            ("p99", summary.tail()),
            ("busy", delta.busy()),
            ("merges", delta.merges as f64 / all.ops.max(1) as f64),
            ("at", at),
        ],
    )
}

/// Read random pieces of a set of files, `depth` in flight, through a window
///
/// # Arguments
///
/// * `paths` - The files
/// * `len` - Each file's length
/// * `size` - Each read's size
/// * `depth` - Reads in flight
/// * `times` - The warm-up and how long it is counted
/// * `devices` - The devices
/// * `name` - The side's name: `random` over the population, `short` inside one file
async fn random(paths: Vec<PathBuf>, len: u64, size: u64, depth: usize, times: (Duration, Duration), devices: Devices, name: &'static str) -> SideOut {
    let mut files: Vec<DmaFile> = Vec::with_capacity(paths.len());
    for path in &paths {
        files.push(io::open_read(path).await);
    }
    let files = Rc::new(files);
    // the clock starts once every file is open
    let window = Window::new(times.0, times.1);
    until(window.start).await;
    // the device read at the window's edges, from a task of its own
    let edges = glommio::spawn_local({
        let devices = devices.clone();
        async move {
            until(window.warm).await;
            let before = devices.snap();
            until(window.end).await;
            before.delta(&devices.snap())
        }
    });
    let tasks = (0..depth).map(|task| {
        let files = files.clone();
        glommio::spawn_local(async move {
            let mut rng = Rng::new(size ^ (task as u64) << 40);
            let mut seen = Seen::default();
            while Instant::now() < window.end {
                // a file, then a place in it aligned to the read's own size
                let file = &files[rng.below(files.len() as u64) as usize];
                let at = rng.below(len / size) * size;
                let begun = Instant::now();
                file.read_at_aligned(at, size as usize).await.expect("read");
                let ended = Instant::now();
                if window.counts(begun, ended) {
                    seen.samples.push(ended - begun);
                    seen.bytes += size;
                    seen.ops += 1;
                }
            }
            seen
        })
    });
    let seen = join_all(tasks).await;
    let delta = edges.await;
    for file in Rc::try_unwrap(files).ok().expect("every reader is done") {
        file.close().await.expect("closed");
    }
    let mut all = Seen::default();
    for part in seen {
        all.bytes += part.bytes;
        all.ops += part.ops;
        all.samples.extend(&part.samples);
    }
    let secs = window.secs();
    let summary = all.samples.summary();
    SideOut::new(
        format!("{name} size={} depth={depth}", size_name(size)),
        name,
        &[
            ("mib_s", all.bytes as f64 / secs / f64::from(1 << 20)),
            ("ops_s", all.ops as f64 / secs),
            ("p50", summary.p50),
            ("p99", summary.tail()),
            ("p999", summary.p999),
            ("busy", delta.busy()),
            ("merges", delta.merges as f64 / all.ops.max(1) as f64),
        ],
    )
}

/// The disk's ceiling by fio, one job: a sequential write of a file of its own, and a
/// sequential read and random 64 KiB reads of the population, which is written whole
///
/// A read of a file fio laid out itself could read extents it allocated and never wrote, which
/// a filesystem answers with zeros from memory, so fio reads what this harness wrote.
///
/// # Arguments
///
/// * `ctx` - The run
/// * `paths` - The population's files
fn fio(ctx: &Ctx, paths: &[PathBuf]) -> Vec<SideOut> {
    let dir = ctx.sub("seq").join("fio");
    std::fs::create_dir_all(&dir).expect("made");
    let runtime = if ctx.quick { "1" } else { "8" };
    let every = paths.iter().map(|path| path.display().to_string()).collect::<Vec<_>>().join(":");
    let written = dir.join("x7-fio").display().to_string();
    let mut outs = Vec::new();
    // the sequential read takes the files one after another, so it never reads one twice
    for (name, rw, bs, depth, field, files, size, service) in [
        ("w-seq-1M", "write", "1m", "8", "write", written.as_str(), "64g", "random"),
        ("r-seq-1M", "read", "1m", "8", "read", every.as_str(), "0", "sequential"),
        ("r64-qd32", "randread", "64k", "32", "read", every.as_str(), "0", "random"),
    ] {
        let mut command = Command::new("fio");
        command
            .args([
                "--name=x7", "--ioengine=io_uring", "--direct=1", "--time_based", "--group_reporting",
                "--output-format=json", "--ramp_time=1", "--numjobs=1", "--fallocate=none",
            ])
            .arg(format!("--file_service_type={service}"))
            .arg(format!("--filename={files}"))
            .arg(format!("--rw={rw}"))
            .arg(format!("--bs={bs}"))
            .arg(format!("--iodepth={depth}"))
            .arg(format!("--runtime={runtime}"));
        // a write names its file's size; a read takes the files as they are
        if size != "0" {
            command.arg(format!("--size={size}"));
        }
        let Ok(output) = command.output() else {
            return outs;
        };
        let json: shoal::serde_json::Value = match shoal::serde_json::from_slice(&output.stdout) {
            Ok(json) => json,
            Err(_) => continue,
        };
        let job = &json["jobs"][0][field];
        outs.push(SideOut::new(
            format!("fio {name}"),
            "fio",
            &[
                ("mib_s", job["bw_bytes"].as_f64().unwrap_or(0.0) / f64::from(1 << 20)),
                ("ops_s", job["iops"].as_f64().unwrap_or(0.0)),
                ("p50", job["clat_ns"]["percentile"]["50.000000"].as_f64().unwrap_or(0.0) / 1e3),
                ("p99", job["clat_ns"]["percentile"]["99.000000"].as_f64().unwrap_or(0.0) / 1e3),
            ],
        ));
    }
    io::wipe(&dir);
    outs
}

/// Run X7's sequential and random measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let root = ctx.sub("seq");
    let pop = root.join("pop");
    let len = if ctx.quick { 64 << 20 } else { FILE };
    let (warm_for, count_for) = (ctx.window(Duration::from_secs(1)), ctx.window(Duration::from_secs(8)));
    // the random population first, written once a leg, and where it lies
    {
        let pop = pop.clone();
        on_core(ctx.core, ctx.sibling, move || populate(pop, len));
    }
    let span = span_of_paths(&population(&pop), ctx.facts.fs_bytes);
    let mut outs = Vec::new();
    // sequential: each piece and depth written to a file of its own, then read back
    let pieces: Vec<u64> = if ctx.quick { vec![1 << 20] } else { PIECES.to_vec() };
    let depths: Vec<usize> = if ctx.quick { vec![1, 32] } else { DEPTHS.to_vec() };
    let mut cells: Vec<(u64, usize)> = Vec::new();
    for &piece in &pieces {
        for &depth in &depths {
            cells.push((piece, depth));
        }
    }
    for (piece, depth) in ordered(&cells, round) {
        let path = root.join(format!("w-{}-{depth}", size_name(piece)));
        for write in [true, false] {
            let (path, devices, bytes) = (path.clone(), ctx.facts.devices.clone(), ctx.facts.fs_bytes);
            outs.push(on_core(ctx.core, ctx.sibling, move || sequential(path, piece, depth, write, count_for, devices, bytes)));
        }
        let _ = std::fs::remove_file(&path);
    }
    // random: every size at every depth over the population, holding at most IN_FLIGHT in flight
    let sizes: Vec<u64> = if ctx.quick { vec![4 << 10, 1 << 20] } else { SIZES.to_vec() };
    let mut cells: Vec<(u64, usize)> = Vec::new();
    for &size in &sizes {
        for &depth in &depths {
            cells.push((size, depth.min((IN_FLIGHT / size).max(1) as usize)));
        }
    }
    cells.dedup();
    for (size, depth) in ordered(&cells, round) {
        let (paths, devices) = (population(&pop), ctx.facts.devices.clone());
        outs.push(on_core(ctx.core, ctx.sibling, move || random(paths, len, size, depth, (warm_for, count_for), devices, "random")));
    }
    // short seeks: 64 KiB reads inside the first file alone, shallow, since a deep queue of them
    // soon reads from the disk's own cache
    for depth in [1, 4] {
        let (paths, devices) = (population(&pop)[..1].to_vec(), ctx.facts.devices.clone());
        outs.push(on_core(ctx.core, ctx.sibling, move || random(paths, len, 64 << 10, depth, (warm_for, count_for), devices, "short")));
    }
    outs.extend(fio(ctx, &population(&pop)));
    let mut table = Table::new(&["cell", "side", "MiB/s", "ops/s", "p50 µs", "p99 µs", "disk busy", "merges/op", "file at"]);
    let mut records = Vec::new();
    for mut out in outs {
        table.row(vec![
            out.cell.clone(),
            out.side.clone(),
            fmt(out.get("mib_s")),
            fmt(out.get("ops_s")),
            fmt(out.get("p50")),
            fmt(out.get("p99")),
            fmt(out.get("busy")),
            fmt(out.get("merges")),
            if out.side == "seq-write" || out.side == "seq-read" { format!("{:.1}%", out.get("at") * 100.0) } else { "-".into() },
        ]);
        for (name, value) in span.figures() {
            out.metrics.insert(name.to_string(), value);
        }
        records.push(ctx.record("seq", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X7 · Sequential and random direct I/O, round {round} ({}; {} files of {} MiB)", span.show(), FILES, len >> 20),
            &ctx.label()
        )
    );
    ctx.emit(&records);
    if ctx.quick && !ctx.keep {
        io::wipe(&root);
    }
    let _ = sys::syncfs(&ctx.dir);
}
