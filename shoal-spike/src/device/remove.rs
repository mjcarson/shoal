//! Measurement 4: removing chunks, a thousand at a time
//!
//! Reclaiming a deleted object, or a stripe's old chunks after a move, removes its files. What
//! that costs is the filesystem's: inodes and extents freed, directories changed, and work that
//! XFS and btrfs defer past the unlink, which the drain after each side catches. The population
//! is cold when it is removed, as reclaimed chunks are. Its chunks are a header block written and
//! the rest allocated, since removing a chunk frees its extents and never reads its bytes.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use glommio::io::Directory;

use super::counters::Devices;
use super::io::{self, DirSync, Payloads, HEADER};
use super::stats::{fmt, Samples};
use super::table::Table;
use super::{drain, on_core, ordered, settle, size_name, sys, Ctx, SideOut};

/// The chunk sizes removed
const SIZES: &[u64] = &[1 << 20, 4 << 20];

/// Objects the `trees` side spreads its chunks over
const OBJECTS: usize = 16;

/// How a side removes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Way {
    /// Each file through glommio's remove, then one sync of their directory
    Chunks,
    /// Each object's files, then its directory, then one sync of the placement group
    Trees,
    /// `unlinkat` on the executor's own thread, then one sync: what a remove through the ring
    /// would cost, which the fork does not issue
    Raw,
}

impl Way {
    /// The side's name
    fn name(self) -> &'static str {
        match self {
            Way::Chunks => "chunks",
            Way::Trees => "trees",
            Way::Raw => "raw",
        }
    }
}

/// The directory a chunk is in, for a side
///
/// # Arguments
///
/// * `pg` - The placement group
/// * `way` - The side
/// * `index` - The chunk
/// * `count` - How many chunks there are
fn object_of(pg: &Path, way: Way, index: usize, count: usize) -> PathBuf {
    match way {
        Way::Trees => pg.join(format!("{:016x}", index / count.div_ceil(OBJECTS))),
        _ => pg.join("0000000000000000"),
    }
}

/// Run one side
///
/// # Arguments
///
/// * `dir` - The side's directory
/// * `devices` - The devices to count on
/// * `form` - The directory sync
/// * `size` - The chunk size
/// * `count` - How many chunks
/// * `way` - The side
async fn run_side(dir: PathBuf, devices: Devices, form: DirSync, size: u64, count: usize, way: Way) -> SideOut {
    io::wipe(&dir);
    let pg = dir.join("pg0");
    let payloads = Payloads::new(size);
    // the population: a header written and the rest allocated, every directory made first
    for index in 0..count {
        std::fs::create_dir_all(object_of(&pg, way, index, count)).expect("made");
    }
    for index in 0..count {
        let path = object_of(&pg, way, index, count).join(format!("{index:06}.0"));
        let file = io::open(&path, true).await;
        file.pre_allocate(size + HEADER, false).await.expect("allocated");
        io::write_body(&file, &payloads, HEADER, 0).await;
        file.close().await.expect("closed");
    }
    // cold, as reclaimed chunks are
    let _ = sys::syncfs(&dir);
    let cold = sys::drop_caches().is_ok();
    let pg_dir = Directory::open(&pg).await.expect("opened");
    settle(&dir).await;
    let before = devices.snap();
    let thread_before = sys::thread_cpu_ns();
    let start = Instant::now();
    let mut unlinks = Samples::default();
    match way {
        Way::Chunks | Way::Raw => {
            let object = object_of(&pg, way, 0, count);
            let handle = Directory::open(&object).await.expect("opened");
            for index in 0..count {
                let name = format!("{index:06}.0");
                let begun = Instant::now();
                if way == Way::Raw {
                    sys::unlink_at(handle.as_raw_fd(), &std::ffi::CString::new(name).expect("a name"), false)
                        .expect("unlinked");
                } else {
                    glommio::io::remove(object.join(name)).await.expect("removed");
                }
                unlinks.push(begun.elapsed());
            }
            let unlinked = start.elapsed();
            let syncing = Instant::now();
            io::sync_dir(&handle, form).await;
            return finish(&dir, &devices, before, thread_before, unlinked.as_secs_f64(), syncing.elapsed().as_secs_f64(), unlinks, size, count, way, cold);
        }
        Way::Trees => {
            for index in 0..count {
                let object = object_of(&pg, way, index, count);
                let begun = Instant::now();
                glommio::io::remove(object.join(format!("{index:06}.0"))).await.expect("removed");
                unlinks.push(begun.elapsed());
                // the object's directory goes after its last chunk
                let last = index + 1 == count || object_of(&pg, way, index + 1, count) != object;
                if last {
                    glommio::executor()
                        .spawn_blocking(move || std::fs::remove_dir(object))
                        .await
                        .expect("the object's directory is removed");
                }
            }
            let unlinked = start.elapsed();
            let syncing = Instant::now();
            io::sync_dir(&pg_dir, form).await;
            return finish(&dir, &devices, before, thread_before, unlinked.as_secs_f64(), syncing.elapsed().as_secs_f64(), unlinks, size, count, way, cold);
        }
    }
}

/// Turn a side's timings into its figures
///
/// # Arguments
///
/// * `dir` - The side's directory
/// * `devices` - The devices counted on
/// * `before` - The counters at the start
/// * `thread_before` - The executor's CPU at the start
/// * `unlinked` - Seconds the removes took
/// * `synced` - Seconds the directory sync took
/// * `unlinks` - Each remove's latency
/// * `size` - The chunk size
/// * `count` - How many chunks
/// * `way` - The side
/// * `cold` - Whether the caches were dropped first
#[allow(clippy::too_many_arguments)]
fn finish(
    dir: &Path,
    devices: &Devices,
    before: super::counters::Snap,
    thread_before: u64,
    unlinked: f64,
    synced: f64,
    unlinks: Samples,
    size: u64,
    count: usize,
    way: Way,
    cold: bool,
) -> SideOut {
    let exec_ns = sys::thread_cpu_ns() - thread_before;
    let drain_ms = drain(dir);
    let delta = before.delta(&devices.snap());
    let summary = unlinks.summary();
    io::wipe(dir);
    SideOut::new(
        format!("size={} n={count}", size_name(size)),
        way.name(),
        &[
            ("n", count as f64),
            ("cold", if cold { 1.0 } else { 0.0 }),
            ("unlink_p50", summary.p50),
            ("unlink_p99", summary.tail()),
            ("unlink_s", unlinked),
            ("dir_sync_ms", synced * 1e3),
            ("drain_ms", drain_ms),
            ("us_per_chunk", (unlinked + synced) * 1e6 / count as f64 + drain_ms * 1e3 / count as f64),
            ("dev_kib", delta.kib_per(count)),
            ("discard_kib", delta.discarded as f64 / 1024.0 / count as f64),
            ("flushes", delta.flushes as f64),
            ("exec_cpu_us", exec_ns as f64 / 1e3 / count as f64),
            ("cpu_us", delta.cpu_us_per(count)),
        ],
    )
}

/// Run measurement 4 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let count = ctx.count(1000, 50);
    let mut table = Table::new(&[
        "size", "side", "n", "cold", "unlink p50 µs", "unlink p99 µs", "removes s", "dir sync ms",
        "drain ms", "µs/chunk in all", "dev KiB/chunk", "discard KiB/chunk", "cpu µs/chunk",
    ]);
    let mut records = Vec::new();
    for &size in SIZES {
        for way in ordered(&[Way::Chunks, Way::Trees, Way::Raw], round) {
            let (dir, devices, form) = (ctx.sub("remove").join(way.name()), ctx.facts.devices.clone(), ctx.dir_sync());
            let out = on_core(ctx.core, ctx.sibling, move || run_side(dir, devices, form, size, count, way));
            table.row(vec![
                size_name(size),
                out.side.clone(),
                fmt(out.get("n")),
                if out.get("cold") > 0.0 { "yes".into() } else { "no (not root)".into() },
                fmt(out.get("unlink_p50")),
                fmt(out.get("unlink_p99")),
                fmt(out.get("unlink_s")),
                fmt(out.get("dir_sync_ms")),
                fmt(out.get("drain_ms")),
                fmt(out.get("us_per_chunk")),
                fmt(out.get("dev_kib")),
                fmt(out.get("discard_kib")),
                fmt(out.get("cpu_us")),
            ]);
            records.push(ctx.record("remove", round, out));
        }
    }
    print!("{}", table.render(&format!("4. Removing chunks, round {round}"), &ctx.label()));
    ctx.emit(&records);
}
