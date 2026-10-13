//! X7: what a sync costs on the disk, and whether several at once are merged into one flush
//!
//! Each writer has a file of its own, written ahead with zeros, and loops: four kibibytes written
//! at its next place, then `fdatasync`. On a disk whose cache writes back every sync is a cache
//! flush, and the block layer may answer several syncs that arrive while a flush runs with the
//! next one: the flushes counted for each sync say whether it did. With the cache off (`-wt`, run
//! by `wcoff`) a write is on the platter when it completes and a sync issues no flush, so what is
//! left is the rotation a write waits for.

use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;

use super::counters::Devices;
use super::io::{self, Payloads, ALIGN};
use super::paced::{until, Window};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// The writers a cell runs
const WRITERS: &[usize] = &[1, 2, 4, 6];

/// Each writer's file, written ahead and overwritten as a ring
const FILE: u64 = 64 << 20;

/// Run one cell: `writers` files, each written and synced in a loop
///
/// # Arguments
///
/// * `dir` - The cell's directory
/// * `writers` - Writers in flight
/// * `times` - The warm-up and how long it is counted
/// * `devices` - The devices
async fn cell(dir: PathBuf, writers: usize, times: (Duration, Duration), devices: Devices) -> SideOut {
    io::wipe(&dir);
    std::fs::create_dir_all(&dir).expect("made");
    // every writer's file written ahead, so a write is an overwrite and a sync commits no size
    let mut files = Vec::with_capacity(writers);
    for writer in 0..writers {
        let file = io::open(&dir.join(format!("w{writer}")), true).await;
        io::zero_fill(&file, FILE).await;
        files.push(Rc::new(file));
    }
    let payloads = Rc::new(Payloads::new(0x5c));
    let _ = payloads.get(ALIGN);
    // the clock starts once every file is written ahead
    let window = Window::new(times.0, times.1);
    until(window.start).await;
    let edges = glommio::spawn_local({
        let devices = devices.clone();
        async move {
            until(window.warm).await;
            let before = devices.snap();
            until(window.end).await;
            before.delta(&devices.snap())
        }
    });
    let tasks = files.iter().map(|file| {
        let (file, payloads) = (file.clone(), payloads.clone());
        glommio::spawn_local(async move {
            let mut samples = Samples::default();
            let mut at = 0;
            while Instant::now() < window.end {
                // the next block, then its sync
                let begun = Instant::now();
                file.write_rc_at(payloads.get(ALIGN), at).await.expect("written");
                file.fdatasync().await.expect("synced");
                let ended = Instant::now();
                if window.counts(begun, ended) {
                    samples.push(ended - begun);
                }
                at = (at + ALIGN) % FILE;
            }
            samples
        })
    });
    let mut samples = Samples::default();
    for part in join_all(tasks).await {
        samples.extend(&part);
    }
    let delta = edges.await;
    drop(files);
    io::wipe(&dir);
    let summary = samples.summary();
    let syncs = summary.n;
    SideOut::new(
        format!("writers={writers}"),
        "overwrite",
        &[
            ("syncs_s", syncs as f64 / window.secs()),
            ("lat_p50", summary.p50),
            ("lat_p99", summary.tail()),
            ("lat_max", summary.max),
            ("flushes_per_sync", delta.flushes_per(syncs)),
            ("dev_kib", delta.kib_per(syncs)),
            ("busy", delta.busy()),
        ],
    )
}

/// Run the sync measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
/// * `suffix` - What every side's name ends with: `-wt` with the disk's write cache off
pub fn run(ctx: &Ctx, round: u32, suffix: &str) {
    let writers: Vec<usize> = if ctx.quick { vec![1, 6] } else { WRITERS.to_vec() };
    let (warm_for, count_for) = (ctx.window(Duration::from_millis(500)), ctx.window(Duration::from_secs(5)));
    let mut table = Table::new(&["writers", "side", "syncs/s", "p50 µs", "p99 µs", "max µs", "flushes/sync", "dev KiB/sync", "disk busy"]);
    let mut records = Vec::new();
    for count in ordered(&writers, round) {
        let (dir, devices) = (ctx.sub("sync"), ctx.facts.devices.clone());
        let mut out = on_core(ctx.core, ctx.sibling, move || cell(dir, count, (warm_for, count_for), devices));
        out.side.push_str(suffix);
        table.row(vec![
            count.to_string(),
            out.side.clone(),
            fmt(out.get("syncs_s")),
            fmt(out.get("lat_p50")),
            fmt(out.get("lat_p99")),
            fmt(out.get("lat_max")),
            fmt(out.get("flushes_per_sync")),
            fmt(out.get("dev_kib")),
            fmt(out.get("busy")),
        ]);
        records.push(ctx.record("sync", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round {round} (write cache {})", ctx.facts.write_cache),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
