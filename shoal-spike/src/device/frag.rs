//! Measurement 7: what a run of clones does to a chunk's extents, and to reading it
//!
//! A clone splices the staged blocks into the chunk, so every clone can split an extent in
//! three. A chunk that has taken a thousand small writes by clone may be a thousand extents,
//! and a sequential read of it a thousand seeks' worth of requests, and a cold read of one unit
//! a large extent map to load first. The same run of writes made in place is the control,
//! except on btrfs, which copies on write in place too. T3 (d) is judged on the 64 MiB chunk.

use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Instant;

use futures::stream::{self, StreamExt};
use glommio::io::DmaFile;

use super::io::{self, Payloads, HEADER};
use super::stats::{fmt, Rng, Samples};
use super::table::Table;
use super::{on_core, ordered, size_name, sys, Ctx, SideOut};

/// The chunk sizes
const SIZES: &[u64] = &[4 << 20, 64 << 20];

/// The largest write, which is the staging file's length
const LARGEST: u64 = 64 << 10;

/// How a side writes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Way {
    /// Staged, cloned in, the staging range punched
    Clone,
    /// Written in place
    Overwrite,
}

impl Way {
    /// Its name
    fn name(self) -> &'static str {
        match self {
            Way::Clone => "clone",
            Way::Overwrite => "overwrite",
        }
    }
}

/// A sequential direct read of a whole chunk, a mebibyte at a time with four in flight, five
/// times; MiB/s
///
/// # Arguments
///
/// * `file` - The chunk
/// * `len` - Its length
async fn sequential(file: &DmaFile, len: u64) -> f64 {
    let start = Instant::now();
    for _ in 0..5 {
        stream::iter((0..len.div_ceil(1 << 20)).map(|piece| piece << 20))
            .map(|at| async move {
                file.read_at_aligned(at, (1 << 20).min(len - at) as usize).await.expect("read");
            })
            .buffer_unordered(4)
            .collect::<Vec<()>>()
            .await;
    }
    (5 * len) as f64 / start.elapsed().as_secs_f64() / f64::from(1 << 20)
}

/// Cold reads of one 64 KiB unit of a chunk, every cache dropped before each; the median, µs
///
/// # Arguments
///
/// * `path` - The chunk
/// * `size` - Its units' length
/// * `samples` - How many reads
async fn cold_unit(path: &PathBuf, size: u64, samples: usize) -> f64 {
    let mut rng = Rng::new(size);
    let mut took = Samples::default();
    for _ in 0..samples {
        let _ = sys::drop_caches();
        let offset = HEADER + rng.below(size / LARGEST) * LARGEST;
        let start = Instant::now();
        let file = io::open_read(path).await;
        file.read_at_aligned(offset, LARGEST as usize).await.expect("read");
        took.push(start.elapsed());
        file.close().await.expect("closed");
    }
    took.summary().p50
}

/// Run one side at one chunk size
///
/// # Arguments
///
/// * `dir` - The side's directory
/// * `size` - The chunk's units' length
/// * `way` - How it writes
/// * `ops` - How many writes
/// * `cold` - Cold reads to sample
async fn run_side(dir: PathBuf, size: u64, way: Way, ops: usize, cold: usize) -> SideOut {
    io::wipe(&dir);
    std::fs::create_dir_all(&dir).expect("made");
    let payloads = Payloads::new(size);
    // a fresh chunk to write into, and another left alone as the control
    let (path, fresh_path) = (dir.join("chunk"), dir.join("fresh"));
    let chunk = io::open(&path, true).await;
    io::write_chunk(&chunk, &payloads, size, 0).await;
    chunk.fdatasync().await.expect("synced");
    let fresh = io::open(&fresh_path, true).await;
    io::write_chunk(&fresh, &payloads, size, 0).await;
    fresh.fdatasync().await.expect("synced");
    let stage = io::open(&dir.join("stage"), true).await;
    let mut extents = vec![sys::fiemap(chunk.as_raw_fd()).expect("mapped")];
    let checkpoints = [(ops / 100).max(1), (ops / 10).max(2), ops];
    let mut rng = Rng::new(0xf7a6 ^ size);
    let start = Instant::now();
    for op in 1..=ops {
        // a write of 4 to 64 KiB at a block-aligned place
        let len = (1 + rng.below(LARGEST / 4096)) * 4096;
        let offset = HEADER + rng.below((size - len) / 4096 + 1) * 4096;
        match way {
            Way::Clone => {
                io::write_body(&stage, &payloads, len, 0).await;
                stage.fdatasync().await.expect("staged");
                io::clone(&stage, 0, len, &chunk, offset).await.expect("cloned");
                chunk.fdatasync().await.expect("applied");
                stage.deallocate(0, len).await.expect("punched");
            }
            Way::Overwrite => {
                io::write_body(&chunk, &payloads, len, offset).await;
                chunk.fdatasync().await.expect("applied");
            }
        }
        if checkpoints.contains(&op) {
            extents.push(sys::fiemap(chunk.as_raw_fd()).expect("mapped"));
        }
    }
    let writes_s = ops as f64 / start.elapsed().as_secs_f64();
    // reading it after, against the control
    let seq = sequential(&chunk, HEADER + size).await;
    let fresh_seq = sequential(&fresh, HEADER + size).await;
    chunk.close().await.expect("closed");
    fresh.close().await.expect("closed");
    stage.close().await.expect("closed");
    let cold_p50 = cold_unit(&path, size, cold).await;
    let fresh_cold_p50 = cold_unit(&fresh_path, size, cold).await;
    io::wipe(&dir);
    let last = *extents.last().expect("mapped");
    SideOut::new(
        format!("chunk={}", size_name(size)),
        way.name(),
        &[
            ("ops", ops as f64),
            ("writes_s", writes_s),
            ("extents_0", f64::from(extents[0].total)),
            ("extents_1pc", f64::from(extents.get(1).map_or(0, |e| e.total))),
            ("extents_10pc", f64::from(extents.get(2).map_or(0, |e| e.total))),
            ("extents_end", f64::from(last.total)),
            ("shared_end", f64::from(last.shared)),
            ("unwritten_end", f64::from(last.unwritten)),
            ("seq_mib_s", seq),
            ("fresh_seq_mib_s", fresh_seq),
            ("seq_ratio", seq / fresh_seq),
            ("cold_p50", cold_p50),
            ("fresh_cold_p50", fresh_cold_p50),
            ("cold_ratio", cold_p50 / fresh_cold_p50),
        ],
    )
}

/// Run measurement 7 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    // a clone was rejected by X6, and a write in place leaves a chunk's extents as they were
    if ctx.rotational() {
        println!("### 7. Fragmentation, round {round}\n\nNot run on a rotational disk: X6 rejected the clone, and a write in place does not fragment.\n");
        return;
    }
    let ways: Vec<Way> = if ctx.clones() { vec![Way::Clone, Way::Overwrite] } else { vec![Way::Overwrite] };
    let (ops, cold) = (ctx.count(1000, 50), ctx.count(50, 5));
    let mut table = Table::new(&[
        "chunk", "side", "writes", "extents before", "after 1%", "after 10%", "after all",
        "shared", "unwritten", "sequential MiB/s", "fresh MiB/s", "÷ fresh", "cold 64 KiB µs",
        "fresh µs", "÷ fresh",
    ]);
    let mut records = Vec::new();
    for &size in SIZES {
        for way in ordered(&ways, round) {
            let dir = ctx.sub("frag").join(way.name());
            let out = on_core(ctx.core, ctx.sibling, move || run_side(dir, size, way, ops, cold));
            table.row(vec![
                size_name(size),
                out.side.clone(),
                fmt(out.get("ops")),
                fmt(out.get("extents_0")),
                fmt(out.get("extents_1pc")),
                fmt(out.get("extents_10pc")),
                fmt(out.get("extents_end")),
                fmt(out.get("shared_end")),
                fmt(out.get("unwritten_end")),
                fmt(out.get("seq_mib_s")),
                fmt(out.get("fresh_seq_mib_s")),
                fmt(out.get("seq_ratio")),
                fmt(out.get("cold_p50")),
                fmt(out.get("fresh_cold_p50")),
                fmt(out.get("cold_ratio")),
            ]);
            records.push(ctx.record("frag", round, out));
        }
    }
    let note = if ctx.clones() { "" } else { "; the filesystem does not clone, so only the control runs" };
    print!(
        "{}",
        table.render(&format!("7. Fragmentation after a run of writes, round {round}{note}"), &ctx.label())
    );
    ctx.emit(&records);
}
