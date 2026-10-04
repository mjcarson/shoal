//! Measurement 6: a read of one chunk unit at a random offset
//!
//! A read names a chunk, a range and a label; S6 answers it by reading every unit the range
//! touches, and the header to check the label and the units' checksums against. Cold, nothing
//! of the chunk is cached: not its inode, not its directory entry, not its extent map, so the
//! open goes to the device too. Cold is what a read of data nobody has touched for a while
//! costs, which is most of what an object store holds. Warm, the file is closed but its inode
//! cached; open, the file is open and its header in memory, so one read. The population is
//! aged before it is read, so that an SSD's write cache does not serve it.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use glommio::io::DmaFile;

use super::counters::Devices;
use super::io::{self, Payloads, HEADER};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{on_core, size_name, sys, Ctx, SideOut};

/// The units read
const UNITS: &[u64] = &[4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20];

/// A chunk's units
const CHUNK: u64 = 4 << 20;

/// Chunks an object holds
const PER_OBJECT: usize = 64;

/// How long the population sits before it is read, so the SSD has moved it out of its cache
const AGE: Duration = Duration::from_secs(120);

/// How warm a read is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    /// Every cache dropped before each read
    Cold,
    /// The chunk's inode cached, the file closed
    Warm,
    /// The file open and its header in memory
    Open,
}

impl Level {
    /// Its name
    fn name(self) -> &'static str {
        match self {
            Level::Cold => "cold",
            Level::Warm => "dentry-warm",
            Level::Open => "open",
        }
    }
}

/// The path of chunk `index`
///
/// # Arguments
///
/// * `pg` - The placement group
/// * `index` - The chunk
fn chunk_path(pg: &Path, index: usize) -> PathBuf {
    pg.join(format!("{:016x}", index / PER_OBJECT))
        .join(format!("{:06}.{}", (index % PER_OBJECT) / 4, index % 4))
}

/// Make the population if it is not there, and wait until it has aged
///
/// # Arguments
///
/// * `root` - Its directory
/// * `count` - Chunks in it
/// * `age` - How long it sits before it is read
async fn populate(root: PathBuf, count: usize, age: Duration) {
    let marker = root.join("complete");
    if !marker.exists() {
        io::wipe(&root);
        let pg = root.join("pg0");
        let payloads = Payloads::new(0x7ead);
        for index in 0..count {
            let path = chunk_path(&pg, index);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("made");
            let file = io::open(&path, true).await;
            io::write_chunk(&file, &payloads, CHUNK, 0).await;
            file.fdatasync().await.expect("synced");
            file.close().await.expect("closed");
        }
        // the small file a cold read wakes the device with first
        let wake = io::open(&root.join("wake"), true).await;
        io::write_body(&wake, &payloads, 4096, 0).await;
        wake.fdatasync().await.expect("synced");
        let _ = sys::syncfs(&root);
        std::fs::write(&marker, count.to_string()).expect("marked");
    }
    // the population sits until it has aged
    let made = std::fs::metadata(&marker)
        .and_then(|meta| meta.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let aged = made.elapsed().unwrap_or(Duration::MAX);
    if aged < age {
        glommio::timer::sleep(age - aged).await;
    }
}

/// The samples of one level at one unit
///
/// # Arguments
///
/// * `root` - The population's directory
/// * `devices` - The devices
/// * `count` - Chunks in the population
/// * `unit` - The unit read
/// * `level` - How warm
/// * `samples` - How many reads
async fn sample(root: PathBuf, devices: Devices, count: usize, unit: u64, level: Level, samples: usize) -> SideOut {
    let pg = root.join("pg0");
    let mut rng = Rng::new(unit ^ samples as u64);
    let wake = io::open_read(&root.join("wake")).await;
    // the open level holds every chunk open; the warm level opens each once first
    let held: Vec<DmaFile> = if level == Level::Open {
        let mut held = Vec::with_capacity(count);
        for index in 0..count {
            held.push(io::open_read(&chunk_path(&pg, index)).await);
        }
        held
    } else {
        if level == Level::Warm {
            for index in 0..count {
                io::open_read(&chunk_path(&pg, index)).await.close().await.expect("closed");
            }
        }
        Vec::new()
    };
    let (mut opens, mut reads, mut closes, mut totals) =
        (Samples::default(), Samples::default(), Samples::default(), Samples::default());
    let mut cold = true;
    let mut read_bytes = 0;
    for _ in 0..samples {
        let index = rng.below(count as u64) as usize;
        let offset = HEADER + rng.below(CHUNK / unit) * unit;
        // cold: every cache dropped, then the device woken by a read of something else
        if level == Level::Cold {
            cold &= sys::drop_caches().is_ok();
            wake.read_at_aligned(0, 4096).await.expect("woken");
        }
        let before = devices.snap();
        let start = Instant::now();
        let opened;
        let read;
        match level {
            Level::Open => {
                opened = start;
                held[index].read_at_aligned(offset, unit as usize).await.expect("read");
                read = Instant::now();
                totals.push(read - start);
            }
            Level::Cold | Level::Warm => {
                let file = io::open_read(&chunk_path(&pg, index)).await;
                opened = Instant::now();
                // the header and the unit together, as a verified read issues them
                let (header, body) = futures::join!(
                    file.read_at_aligned(0, HEADER as usize),
                    file.read_at_aligned(offset, unit as usize)
                );
                header.expect("read");
                body.expect("read");
                read = Instant::now();
                totals.push(read - start);
                file.close().await.expect("closed");
                closes.push(read.elapsed());
            }
        }
        opens.push(opened - start);
        reads.push(read - opened);
        read_bytes += before.delta(&devices.snap()).read;
    }
    for file in held {
        file.close().await.expect("closed");
    }
    let (open, read, close, total) = (opens.summary(), reads.summary(), closes.summary(), totals.summary());
    SideOut::new(
        format!("unit={}", size_name(unit)),
        level.name(),
        &[
            ("n", samples as f64),
            ("cold", if cold { 1.0 } else { 0.0 }),
            ("open_p50", open.p50),
            ("open_p99", open.tail()),
            ("read_p50", read.p50),
            ("read_p99", read.tail()),
            ("close_p50", close.p50),
            ("total_p50", total.p50),
            ("total_p99", total.tail()),
            ("total_p999", total.p999),
            ("dev_read_kib", read_bytes as f64 / 1024.0 / samples as f64),
        ],
    )
}

/// Run measurement 6 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let root = ctx.sub("read");
    let count = ctx.count(2048, 32);
    let age = if ctx.quick { Duration::ZERO } else { AGE };
    {
        let root = root.clone();
        on_core(ctx.core, ctx.sibling, move || populate(root, count, age));
    }
    let mut table = Table::new(&[
        "unit", "level", "n", "open p50", "open p99", "read p50", "read p99", "close p50",
        "total p50", "total p99", "total p99.9", "dev KiB read/sample",
    ]);
    let mut records = Vec::new();
    let units: Vec<u64> = if ctx.quick { vec![4 << 10, 64 << 10] } else { UNITS.to_vec() };
    for &unit in &units {
        for level in super::ordered(&[Level::Cold, Level::Warm, Level::Open], round) {
            let samples = match level {
                Level::Cold => ctx.count(100, 5),
                _ => ctx.count(2000, 20),
            };
            let (root, devices) = (root.clone(), ctx.facts.devices.clone());
            let out = on_core(ctx.core, ctx.sibling, move || sample(root, devices, count, unit, level, samples));
            let cold = level != Level::Cold || out.get("cold") > 0.0;
            table.row(vec![
                size_name(unit),
                if cold { out.side.clone() } else { format!("{} (not cold: not root)", out.side) },
                fmt(out.get("n")),
                fmt(out.get("open_p50")),
                fmt(out.get("open_p99")),
                fmt(out.get("read_p50")),
                fmt(out.get("read_p99")),
                fmt(out.get("close_p50")),
                fmt(out.get("total_p50")),
                fmt(out.get("total_p99")),
                fmt(out.get("total_p999")),
                fmt(out.get("dev_read_kib")),
            ]);
            records.push(ctx.record("read", round, out));
        }
    }
    print!(
        "{}",
        table.render(&format!("6. One unit at a random offset, round {round} (µs; queue depth 1)"), &ctx.label())
    );
    ctx.emit(&records);
    if ctx.quick && !ctx.keep {
        io::wipe(&root);
    }
}
